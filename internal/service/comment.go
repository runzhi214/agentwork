package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"

	"github.com/eushing/agentwork/internal/events"
	"github.com/eushing/agentwork/internal/logging"
	"github.com/eushing/agentwork/internal/store"
)

// Comment is a message under a goal. Authors are polymorphic (human/agent/
// system). content is Markdown and may carry structured mention URIs that the
// server parses AFTER persistence to trigger runs. See DESIGN.md §2.
type Comment struct {
	ID         string `json:"id"`
	GoalID     string `json:"goal_id"`
	AuthorType string `json:"author_type"` // human|agent|system
	AuthorID   string `json:"author_id"`
	ParentID   string `json:"parent_id"`
	Content    string `json:"content"`
	CreatedAt  string `json:"created_at"`
	// RunID is the run whose product this comment is (AGENTWORK_RUN_ID for
	// agent comments; insertRunResultComment stamps the run). Persisted —
	// the approval panel distinguishes worker reports from review opinions
	// by run ownership (DESIGN.md 决策 4-4). Also used to thread an agent
	// comment as a REPLY to its run's trigger comment.
	RunID string `json:"run_id,omitempty"`
	// AskHuman (决策 7-3): true when the agent posted this comment via
	// `goal comment --ask` — an explicit question to the goal creator. The
	// platform publishes comment:agent_question so notify pushes a Feishu
	// card; the flag persists so the web can render a "❓ question" style
	// after refresh. Reply routing is by parent_id→agent, NOT this flag.
	AskHuman bool `json:"ask_human,omitempty"`
}

type CommentService struct {
	st      *store.Store
	bus     *events.Bus
	runSvc  *RunService
	goalSvc *GoalService
}

func NewCommentService(st *store.Store, bus *events.Bus) *CommentService {
	return &CommentService{st: st, bus: bus}
}

func (s *CommentService) SetRunService(rs *RunService)   { s.runSvc = rs }
func (s *CommentService) SetGoalService(gs *GoalService) { s.goalSvc = gs }

// Mention is one parsed mention from a comment body.
type Mention struct {
	Type string `json:"type"` // agent | squad | human | all
	ID   string `json:"id"`
}

// Mention-cycle guard (two thresholds): a goal whose runs are repeatedly
// triggered by AGENT comments is likely an agent ping-pong (A mentions B,
// B mentions A, ...). Healthy work does not churn agent-triggered runs;
// repeated churn (including repeated human rejects) means the task itself is
// stuck. Over maxMentionHints the next run's prompt warns the agents to stop
// circular handoffs; over maxMentionCycle the next trigger is refused and
// the goal fails with the cycle count as the reason.
const (
	MaxMentionHints = 4
	MaxMentionCycle = 8
)

// MentionRe matches `[@Name](mention://(agent|squad|human|all)/(<uuid-ish>|all))`.
// Matches multica's parser shape (server/internal/util/mention.go): only
// structured Markdown URIs, only UUID-ish ids (or literal "all"). Bare `@handle`
// prose does NOT match — the agent must resolve a UUID and write the link.
// See DESIGN.md §2 ("only structured URIs, only UUID").
var MentionRe = regexp.MustCompile(`\[@?(.+?)\]\(mention://(agent|squad|human|all)/([0-9a-fA-F-]+|all)\)`)

// ParseMentions extracts deduplicated mentions from persisted comment body.
// NEVER called on agent stdout — only on already-stored comment content.
func ParseMentions(content string) []Mention {
	matches := MentionRe.FindAllStringSubmatch(content, -1)
	seen := map[string]struct{}{}
	out := []Mention{}
	for _, m := range matches {
		mention := Mention{Type: m[2], ID: m[3]}
		key := mention.Type + ":" + mention.ID
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, mention)
	}
	return out
}

// HasMentionAll reports whether the content mentions @all.
func HasMentionAll(content string) bool {
	for _, m := range ParseMentions(content) {
		if m.Type == "all" {
			return true
		}
	}
	return false
}

// Create persists a comment and dispatches any agent/squad mentions it carries.
// Per DESIGN.md:
//   - @all → suppress auto-trigger (no run enqueued); humans notified later.
//   - mention://agent/<id> → enqueue a new run on that agent (same goal,
//     different agent), does NOT cancel the current assignee's in-flight run.
//   - mention://squad/<id> → route to the squad's leader (leader run).
//   - mention://human/<id> → just renders a link.
func (s *CommentService) Create(ctx context.Context, c Comment) (*Comment, error) {
	return s.create(ctx, c, true)
}

// CreateNoDispatch is the pure-Comment path (comment_goal's contract,
// 决策 5-2): the comment is persisted and published, but mentions in it
// NEVER trigger runs — asking another agent is consult_agent's job, not the
// comment's. The mention-dispatch below is the Consult mechanism, and a
// comment that silently dispatched a guest run would turn "saying" into
// "asking" against the tool's own description.
func (s *CommentService) CreateNoDispatch(ctx context.Context, c Comment) (*Comment, error) {
	return s.create(ctx, c, false)
}

// create is the shared core: persist + publish, then (when dispatch is set)
// the mention dispatch — the comment-triggered reopen and the consult
// enqueue are both dispatch-side effects, so a pure comment on a terminal
// goal lands without reopening either.
func (s *CommentService) create(ctx context.Context, c Comment, dispatch bool) (*Comment, error) {
	if c.GoalID == "" {
		return nil, NewFieldRequiredError("goal_id")
	}
	if c.AuthorType == "" {
		c.AuthorType = "human"
	}
	// Platform threading: an agent comment made inside a run automatically
	// replies to the comment that triggered that run (mention → run →
	// reply). The agent never needs to know parent ids.
	if c.RunID != "" && c.ParentID == "" {
		var tid string
		if err := s.st.DB().QueryRowContext(ctx,
			`SELECT trigger_comment_id FROM run WHERE id=?`, c.RunID).Scan(&tid); err == nil && tid != "" {
			c.ParentID = tid
		}
	}
	g, err := s.goalSvc.Get(ctx, c.GoalID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, NewValidationError("goal does not exist")
		}
		return nil, err
	}
	c.ID = newID()
	c.CreatedAt = now()

	tx, err := s.st.DB().BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var parentID any
	if c.ParentID != "" {
		parentID = c.ParentID
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO comment (id,goal_id,author_type,author_id,parent_id,content,created_at,run_id,ask_human) VALUES (?,?,?,?,?,?,?,?,?)`,
		c.ID, c.GoalID, c.AuthorType, c.AuthorID, parentID, c.Content, c.CreatedAt, c.RunID, c.AskHuman); err != nil {
		return nil, fmt.Errorf("insert comment: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO activity_log (id,goal_id,actor_type,actor_id,action,detail,created_at) VALUES (?,?,?,?,'commented',?,?)`,
		newID(), c.GoalID, c.AuthorType, c.AuthorID, "{}", c.CreatedAt); err != nil {
		return nil, fmt.Errorf("insert activity: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.bus.Publish(ctx, events.Event{Topic: "comment:created", Payload: c})

	// 决策 7-3: an agent's --ask comment is a question to the goal creator.
	// Publish a dedicated event (not comment:created) so notify can push a
	// Feishu card without coupling to every comment. Only agent-authored
	// questions fire this — a human cannot ask themselves.
	if c.AskHuman && c.AuthorType == "agent" {
		s.bus.Publish(ctx, events.Event{Topic: "comment:agent_question", Payload: map[string]any{
			"goal_id":    c.GoalID,
			"comment_id": c.ID,
			"agent_id":   c.AuthorID,
			"question":   c.Content,
		}})
	}

	if !dispatch {
		return &c, nil // pure comment — no reopen, no mention-triggered runs
	}

	// Dispatch mentions AFTER the comment is durably stored. @all suppresses
	// auto-trigger entirely (no runs); other mentions enqueue runs.
	// Execution freeze (DESIGN.md §4, decision 2-3 REVISED, 决策 6-15①): the
	// freeze protects EXECUTION, not intent. An ACTIVE goal's mention
	// enqueues and runs; a REVIEW goal's mention enqueues a DURABLE INTENT
	// that the Claim gate holds queued until the human releases (the branch
	// state under the human's decision is never mutated — nobody executes);
	// terminal/backlog goals take no silent new work (terminal + human +
	// action mention = the reopen path above).
	//
	// COMMENT-TRIGGERED REOPEN (GitHub's reopen-and-comment): a HUMAN comment
	// on a TERMINAL goal (done/failed/cancelled) that carries an action
	// mention (agent/squad) reopens the goal — "this task is not over" — and
	// the mention then triggers normally. A plain comment without a mention
	// lands only (terminal goals take no silent new work; a stray remark must
	// not burn a run).
	isTerminal := g.Status == "done" || g.Status == "failed" || g.Status == "cancelled"
	hasActionMention := false
	for _, m := range ParseMentions(c.Content) {
		if m.Type == "agent" || m.Type == "squad" {
			hasActionMention = true
			break
		}
	}
	reopened := false
	if isTerminal && c.AuthorType == "human" && hasActionMention {
		if _, err := s.goalSvc.Reopen(ctx, c.GoalID, "", c.ID); err == nil {
			// Reopened → the goal is active now; the mention dispatch below
			// proceeds against the fresh state. The reopen's OWNER run already
			// carries THIS comment as its trigger (决策 4-1 修订) — a mention
			// to the assignee must not dispatch a second, consult-shaped run
			// on top of it.
			reopened = true
			if g2, err := s.goalSvc.Get(ctx, c.GoalID); err == nil {
				g = g2
			}
		}
	}
	// 决策 7-3: a HUMAN reply to an AGENT comment (parent_id → agent-authored
	// comment) continues the owner's work — it is NOT a consult. Without this
	// routing, a web reply (which auto-inserts a mention://agent link) would
	// fall into the consult dispatch below, enqueuing a guest run (fresh empty
	// workdir, no session, no owner wake) — the agent would see "working
	// directory is empty, no context" and the task would end. Instead, wake
	// the goal's current owner with role='owner' (persistent workdir + session
	// resume via priorSessionFor) and the reply comment as the trigger.
	if c.AuthorType == "human" && c.ParentID != "" && s.runSvc != nil && s.goalSvc != nil {
		var parentAuthor string
		if err := s.st.DB().QueryRowContext(ctx,
			`SELECT author_type FROM comment WHERE id=?`, c.ParentID).Scan(&parentAuthor); err == nil && parentAuthor == "agent" {
			// Terminal goal + a reply to an agent comment: reopen first (the
			// reopen above only fired for action-mention replies; a plain-text
			// reply to an agent comment must also reopen a terminal goal).
			if isTerminal && !reopened {
				if _, err := s.goalSvc.Reopen(ctx, c.GoalID, "", c.ID); err == nil {
					reopened = true
					if g2, err := s.goalSvc.Get(ctx, c.GoalID); err == nil {
						g = g2
					}
				}
			}
			// Resolve the goal's current owner agent (squad → leader).
			ownerAgentID := ""
			if g.AssigneeType == "agent" {
				ownerAgentID = g.AssigneeID
			} else if g.AssigneeType == "squad" {
				// resolveLeaderTx takes a *sql.Tx (nil panics on its tx.Query);
				// query the leader directly — the comment path holds no tx here
				// (the comment row was committed before dispatch).
				_ = s.st.DB().QueryRowContext(ctx,
					`SELECT leader_id FROM squad WHERE id=?`, g.AssigneeID).Scan(&ownerAgentID)
			}
			if ownerAgentID != "" && (g.Status == "active" || reopened) {
				if _, err := s.runSvc.EnqueueForMentionRole(ctx, c.GoalID, ownerAgentID, c.ID, "owner"); err != nil {
					logging.Errorf("comment: human→agent reply owner wake goal=%s: %v", c.GoalID, err)
				} else {
					logging.Infof("comment: human reply to agent → owner wake goal=%q (%s) agent=%s (trigger comment %s)",
						g.Title, c.GoalID, ownerAgentID, c.ID)
				}
				// Return BEFORE the status guard + mention dispatch — the owner
				// wake is done; a co-present mention link must NOT also enqueue
				// a consult run (double-trigger).
				return &c, nil
			}
			// ownerAgentID empty (human-owned goal, or squad leader unresolved):
			// fall through — the reply lands as a comment, no run (human owner
			// has no agent to wake; squad leader resolution failed gracefully).
		}
	}
	if HasMentionAll(c.Content) || (g.Status != "active" && g.Status != "review") {
		// @all: notify humans only (TBD: no inbox in MVP) and suppress triggers.
		// terminal/backlog: comment lands, triggers suppressed.
		// REVIEW passes through: the mention enqueues as a queued intent —
		// Claim's goal gate (P0-1) admits it only after the human releases.
		return &c, nil
	}
	// Consult permission (Collaboration.md §6, 决策 5-2): only the goal's
	// OWNER (agent run) may mention other agents from a comment — a guest run
	// is itself a consult and cannot fan out further. Human comments are the
	// human's consult and stay unrestricted.
	agentCanConsult := true
	if c.AuthorType == "agent" {
		owns, err := s.goalSvc.AgentOwnsGoal(ctx, g, c.AuthorID)
		if err != nil {
			return nil, err
		}
		agentCanConsult = owns
	}
	for _, m := range ParseMentions(c.Content) {
		if reopened && g.AssigneeType == m.Type && g.AssigneeID == m.ID {
			continue // the reopen's owner run answers THIS comment already
		}
		switch m.Type {
		case "agent":
			if s.runSvc == nil || !agentCanConsult {
				continue
			}
			// Mention-cycle guard: an agent-triggered run churn above the hard
			// threshold fails the goal (the task is stuck in a handoff loop).
			if exceeds, err := s.mentionCycleExceeds(ctx, c.GoalID, MaxMentionCycle); err == nil && exceeds {
				s.forceMentionCycleFailed(ctx, c.GoalID)
				continue
			}
			r, e := s.runSvc.EnqueueForMention(ctx, c.GoalID, m.ID, c.ID)
			if e != nil {
				// A bad/unknown agent UUID → drop, don't fail the whole comment.
				continue
			}
			logging.Infof("comment: mention dispatch goal=%q (%s) → consult run %s agent=%s (trigger comment %s)",
				g.Title, c.GoalID, r.ID, m.ID, c.ID)
			// Consult chain (决策 5-8): record the request — requester run /
			// target / trigger comment / guest run — so the platform can
			// auto-resume the requester once the guest answers.
			if c.AuthorType == "agent" {
				if err := s.recordConsult(ctx, c, m.ID, r.ID); err != nil {
					logging.Errorf("comment: record consult: %v", err)
				}
			}
		case "squad":
			if s.runSvc == nil || !agentCanConsult {
				continue
			}
			if exceeds, err := s.mentionCycleExceeds(ctx, c.GoalID, MaxMentionCycle); err == nil && exceeds {
				s.forceMentionCycleFailed(ctx, c.GoalID)
				continue
			}
			// Route to the squad's leader as a leader run.
			if e := s.enqueueLeaderRunForMention(ctx, m.ID, c.GoalID, c.ID); e != nil {
				continue
			}
		case "human", "all":
			// No run; just a rendered link.
		}
	}
	return &c, nil
}

// MentionCycleCount counts a goal's agent-triggered runs (trigger_comment_id
// pointing at an AGENT-authored comment). Platform triggers (system review
// requests) and human triggers are not agent churn. Sub-goal/verify runs are
// EXEMPT (P2-2, 决策 6-15⑩): their trigger is the owner's dispatch comment —
// workflow execution, not mention churn. Review runs too (决策 6-19):
// platform-enqueued, anchored on the parking run's agent-authored report —
// the review round is the approval window's evidence, never churn (the
// counter's semantic is the agent↔agent consult loop; this is the current
// approximation, not the final interaction-edge definition).
func (s *CommentService) MentionCycleCount(ctx context.Context, goalID string) (int, error) {
	var n int
	err := s.st.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM run r JOIN comment c ON c.id = r.trigger_comment_id
		 WHERE r.goal_id=? AND c.author_type='agent'
		   AND r.role NOT IN ('subgoal','verify','review')`, goalID).Scan(&n)
	return n, err
}

// mentionCycleExceeds reports whether the goal's agent-triggered churn is at
// or above the hard threshold (the NEXT trigger is the one refused).
func (s *CommentService) mentionCycleExceeds(ctx context.Context, goalID string, limit int) (bool, error) {
	n, err := s.MentionCycleCount(ctx, goalID)
	if err != nil {
		return false, err
	}
	return n >= limit, nil
}

// forceMentionCycleFailed fails a goal stuck in an agent handoff loop: the
// goal goes failed (the human take-over path, Reopen), the failure reason
// names the cycle count, queued runs are dropped, and the failure is
// recorded in the feed.
func (s *CommentService) forceMentionCycleFailed(ctx context.Context, goalID string) {
	n, err := s.MentionCycleCount(ctx, goalID)
	if err != nil {
		n = MaxMentionCycle
	}
	res, err := s.st.DB().ExecContext(ctx,
		`UPDATE goal SET status='failed' WHERE id=? AND status='active'`, goalID)
	if err != nil {
		return
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return // already moved (review/reject raced) — the loop is moot
	}
	// Platform text is English (决策 6-18) — the language lives in the
	// MATERIALS (agents' comments follow the goal's language via the
	// LANGUAGE rule); platform notifications stay fixed.
	reason := fmt.Sprintf("agent collaboration cycle reached %d (limit %d)", n, MaxMentionCycle)
	comment := fmt.Sprintf("Task failed: %s. Agents kept handing the task to each other — review and reopen.", reason)
	ts := now()
	_, _ = s.st.DB().ExecContext(ctx,
		`INSERT INTO activity_log (id,goal_id,actor_type,actor_id,action,detail,created_at) VALUES (?,?,?,'','mention_cycle_failed',?,?)`,
		newID(), goalID, "system", `{"reason":"`+reason+`"}`, ts)
	_, _ = s.st.DB().ExecContext(ctx,
		`INSERT INTO comment (id,goal_id,author_type,author_id,parent_id,content,created_at) VALUES (?,?,'system','',NULL,?,?)`,
		newID(), goalID, comment, ts)
	_, _ = s.st.DB().ExecContext(ctx,
		`UPDATE run SET status='cancelled', cancel_reason='goal_terminal' WHERE goal_id=? AND status='queued'`, goalID)
	_, _ = s.st.DB().ExecContext(ctx,
		`UPDATE sub_goal SET status='cancelled' WHERE goal_id=? AND status NOT IN ('verified','cancelled','failed')`, goalID)
	s.bus.Publish(ctx, events.Event{Topic: "goal:finished", Payload: map[string]any{
		"goal_id": goalID, "status": "failed", "summary": reason,
	}})
}

// recordConsult appends a consult_request row (决策 5-8, Collaboration.md
// §12): the full chain — requester (the owner run that asked) → trigger
// comment → guest run — so the reconcile step can auto-resume the requester
// when the guest's answer lands. response_comment_id is back-filled at guest
// run end.
func (s *CommentService) recordConsult(ctx context.Context, c Comment, targetAgentID, guestRunID string) error {
	ts := now()
	_, err := s.st.DB().ExecContext(ctx,
		`INSERT INTO consult_request (id,goal_id,requester_agent_id,requester_run_id,target_agent_id,trigger_comment_id,guest_run_id,response_comment_id,created_at)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		newID(), c.GoalID, c.AuthorID, c.RunID, targetAgentID, c.ID, guestRunID, "", ts)
	return err
}

// enqueueLeaderRunForMention resolves a squad's leader and enqueues a leader
// run on it for the goal, sourced from the triggering comment.
func (s *CommentService) enqueueLeaderRunForMention(ctx context.Context, squadID, goalID, triggerCommentID string) error {
	var leaderID string
	err := s.st.DB().QueryRowContext(ctx, `SELECT leader_id FROM squad WHERE id=?`, squadID).Scan(&leaderID)
	if errors.Is(err, sql.ErrNoRows) {
		return NewValidationError("squad not found")
	}
	if err != nil {
		return err
	}
	// Leader runs via mention: enqueue on the leader agent with isLeader + squadID.
	// We reuse enqueue directly (its own tx) — mention-triggered runs may run
	// concurrently with the current assignee's run, which is intended.
	_, err = s.runSvc.enqueue(ctx, goalID, leaderID, 1, true, squadID, triggerCommentID)
	return err
}

// ListAfter lists a goal's comments NEWER than the given comment id (the
// get_comments tool's incremental cursor — ” = from the start), capped at
// limit (0 = default 50, hard max 100). Read-only; the long-run dynamic
// context channel (决策 6-15⑪): the prompt snapshot is fixed at claim, this
// is the live view.
func (s *CommentService) ListAfter(ctx context.Context, goalID, afterID string, limit int) ([]Comment, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	var rows *sql.Rows
	var err error
	if afterID == "" {
		rows, err = s.st.DB().QueryContext(ctx,
			`SELECT id,goal_id,author_type,author_id,parent_id,content,created_at,run_id,ask_human FROM comment WHERE goal_id=? ORDER BY created_at LIMIT ?`,
			goalID, limit)
	} else {
		rows, err = s.st.DB().QueryContext(ctx,
			`SELECT id,goal_id,author_type,author_id,parent_id,content,created_at,run_id,ask_human FROM comment
			 WHERE goal_id=? AND created_at > (SELECT created_at FROM comment WHERE id=?)
			 ORDER BY created_at LIMIT ?`,
			goalID, afterID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Comment{}
	for rows.Next() {
		var c Comment
		var parentID sql.NullString
		if err := rows.Scan(&c.ID, &c.GoalID, &c.AuthorType, &c.AuthorID, &parentID, &c.Content, &c.CreatedAt, &c.RunID, &c.AskHuman); err != nil {
			return nil, err
		}
		c.ParentID = parentID.String
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *CommentService) List(ctx context.Context, goalID string) ([]Comment, error) {
	rows, err := s.st.DB().QueryContext(ctx,
		`SELECT id,goal_id,author_type,author_id,parent_id,content,created_at,run_id,ask_human FROM comment WHERE goal_id=? ORDER BY created_at`, goalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Comment{}
	for rows.Next() {
		var c Comment
		var parentID sql.NullString
		if err := rows.Scan(&c.ID, &c.GoalID, &c.AuthorType, &c.AuthorID, &parentID, &c.Content, &c.CreatedAt, &c.RunID, &c.AskHuman); err != nil {
			return nil, err
		}
		c.ParentID = parentID.String
		out = append(out, c)
	}
	return out, rows.Err()
}

// sanity check that the agent/squad we mention exists is intentionally NOT done
// here for agent: a stale agent UUID is dropped silently (matching multica's
// blockTarget behavior). Squad is checked in enqueueLeaderRunForMention.
