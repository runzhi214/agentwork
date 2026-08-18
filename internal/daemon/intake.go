package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/eushing/agentwork/internal/link"
	"github.com/eushing/agentwork/internal/logging"
	"github.com/eushing/agentwork/internal/notify"
	"github.com/eushing/agentwork/internal/service"
)

// runIntakeTask executes an inbound-message parse run (M3-4): the parser
// agent reads the owner's message (the run prompt) and writes intake.json —
// the parsed action — into its scratch workdir. The PLATFORM executes the
// action (goal create / review list / goal status — never the agent; the
// parser only understands intent and names ids) and replies over IM.
//
// Structured output is read from the file, never from agent stdout
// (DESIGN.md §5.3, §9): the parser is a processor agent, same as the
// policy compiler.
func (d *Daemon) runIntakeTask(ctx context.Context, q *service.ClaimedRow, prompt, agentID string) {
	// Scratch workdir (no repo): the parser works from the prompt alone and
	// writes its result file here.
	workdir := filepath.Join(runsRoot(), "proc", q.RunID)
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		d.failIntakeRun(ctx, q, "mkdir workdir: "+err.Error())
		return
	}
	// The artifact's ABSOLUTE path: the scratch dir is opaque to the agent —
	// told to "write intake.json in the current directory" it guessed (a
	// write_file call missing its required path arg, then a raw shell heredoc
	// terminal_create cannot run — command must be an executable). State the
	// full path so the write_file path argument is unambiguous.
	prompt += fmt.Sprintf("\n\nArtifact file ABSOLUTE path: %s\n(Write it there with your file tools; do NOT guess the working directory, do NOT use shell redirection)\n",
		filepath.Join(workdir, "intake.json"))
	var argsJSON, rtEnvJSON, intakeMachineID string
	var maxConcurrent int
	err := d.st.DB().QueryRowContext(ctx,
		`SELECT r.args, r.env, COALESCE(r.machine_id,''), a.max_concurrent
		 FROM agent a JOIN runtime r ON r.id = a.runtime_id WHERE a.id=?`, agentID).
		Scan(&argsJSON, &rtEnvJSON, &intakeMachineID, &maxConcurrent)
	if err != nil {
		d.failIntakeRun(ctx, q, "load agent runtime: "+err.Error())
		return
	}
	d.ensureWorker(agentID, maxConcurrent)

	var args []string
	_ = json.Unmarshal([]byte(argsJSON), &args)
	var rtEnv map[string]string
	_ = json.Unmarshal([]byte(rtEnvJSON), &rtEnv)
	agentEnv, _ := d.loadAgentEnv(ctx, agentID)

	// CLI 分支 Phase 2: machine-owned intake runtimes dispatch (same shape
	// as the compile processor — scratch proc dir + intake.json artifact).
	if intakeMachineID != "" {
		dispatchEnv := map[string]string{}
		for k, v := range rtEnv {
			dispatchEnv[k] = v
		}
		for k, v := range agentEnv {
			dispatchEnv[k] = v
		}
		d.dispatchToMachine(ctx, q, link.RunDispatchParams{
			RunID: q.RunID, AgentID: q.AgentID, Attempt: q.Attempt, Token: q.Token,
			Prompt: prompt, Proc: true, Scratch: true,
			ArtifactFiles: []string{"intake.json"},
			ACPSpawn: args, Env: dispatchEnv,
		}, intakeMachineID)
		return
	}

	// Legacy transports have no executor anymore (the unified model
	// dispatches everything to machines).
	d.failIntakeRun(ctx, q, "this runtime has no machine — run `agentwork connect` and point the agent at a machine-owned runtime")
}

// ingestIntakeArtifact completes an intake run from its FILE artifact
// (intake.json) — the shared path for local execution and the machine-
// dispatched upload (CLI 分支 Phase 2).
func (d *Daemon) ingestIntakeArtifact(ctx context.Context, q *service.ClaimedRow, artifactContent string) {
	var parsed intakeAction
	if err := json.Unmarshal([]byte(artifactContent), &parsed); err != nil {
		d.failIntakeRun(ctx, q, "parse intake.json: "+err.Error())
		return
	}
	d.replyIntake(ctx, q, parsed)
}

// failIntakeRun marks the parse run failed AND tells the owner — the inbound
// flow already acknowledged the message ("⏳ 收到"), so a silent failure
// would leave the user waiting for a result that never comes. The failure
// detail is sent IN FULL (no truncation): the user debugging a parse failure
// needs the whole reason, including the path that failed.
func (d *Daemon) failIntakeRun(ctx context.Context, q *service.ClaimedRow, summary string) {
	if n := d.imNotifier(); n != nil {
		if err := n.Send("⚠️ 消息解析失败：" + summary); err != nil {
			logging.Errorf("daemon: intake failure reply: %v", err)
		}
	}
	// Stamp the run failed directly (P0-5 conditional — a reaper stamp
	// wins) — NOT via failProcessorRun, whose domain:compile_failed event
	// is the compile path's signal and would mislabel an intake failure.
	res, err := d.st.DB().ExecContext(ctx,
		`UPDATE run SET status='failed', result_summary=?, finished_at=? WHERE id=? AND status IN ('queued','running')`,
		summary, nowStr(), q.RunID)
	if err != nil {
		logging.Infof("daemon: mark intake run %s failed: %v", q.RunID, err)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		logging.Infof("daemon: intake run %s already terminal — dropping late failure", q.RunID)
	}
}

// intakeAction is the parser's output contract (see notify/intake.go's
// BuildPrompt for the shape the parser is instructed to produce). The create
// sub-structs are NAMED types so the draft-merge helpers can take them as
// parameters (an anonymous struct can't be a func arg) and the draft can
// round-trip a sub-struct as JSON via a concrete type.
type intakeAction struct {
	Intent string `json:"intent"`
	Goal   goalAction `json:"goal"`
	GoalID string     `json:"goal_id"`
	// GoalQuery carries the goal_query fields (list/filter goals by status
	// and/or completion time). Kept as a named type for clarity.
	GoalQuery goalQueryAction `json:"goal_query"`
	// Schedule carries the parsed定时任务 fields (create_schedule / schedule_stop).
	// Kept anonymous — it does not participate in the create-draft/merge flow.
	Schedule struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Cron        string `json:"cron"`
		AssigneeID  string `json:"assignee_id"`
		DomainID    string `json:"domain_id"`
	} `json:"schedule"`
	Agent agentAction  `json:"agent"`
	Squad squadAction  `json:"squad"`
}

// goalAction is the create_goal sub-struct.
type goalAction struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	AssigneeID  string `json:"assignee_id"`
	DomainID    string `json:"domain_id"`
}

// goalQueryAction carries the goal_query fields: status filters by goal
// status (done/active/backlog/failed/review/cancelled; empty = all),
// last_n limits to the N most recent goals (0 = all, capped at 30),
// and from_date/to_date optionally filter by goal created_at
// (YYYY-MM-DD; empty = unbounded).
type goalQueryAction struct {
	Status   string `json:"status"`
	LastN    int    `json:"last_n"`
	FromDate string `json:"from_date"`
	ToDate   string `json:"to_date"`
}

// agentAction carries the create_agent fields. Technical config (env/model/
// mcp_servers/max_concurrent) is deliberately absent — NL builds a
// persona-bearing skeleton, the rest is filled in on the Web.
type agentAction struct {
	Name            string   `json:"name"`
	RuntimeID       string   `json:"runtime_id"`
	Description     string   `json:"description"`
	SystemPrompt    string   `json:"system_prompt"`
	Skills          []string `json:"skills"`
	SkillsSpecified bool     `json:"skills_specified"`
}

// squadAction carries the create_squad fields. Members are added after Create
// via AddMember (role="member"); the leader is held in LeaderID, never in
// MemberIDs.
type squadAction struct {
	Name         string   `json:"name"`
	LeaderID     string   `json:"leader_id"`
	Description  string   `json:"description"`
	Instructions string   `json:"instructions"`
	MemberIDs    []string `json:"member_ids"`
}

// replyIntake executes the parsed action and replies over IM. The run row is
// stamped completed here (the daemon owns processor-run finishing).
func (d *Daemon) replyIntake(ctx context.Context, q *service.ClaimedRow, parsed intakeAction) {
	var reply string
	switch parsed.Intent {
	case "create_goal":
		reply = d.intakeCreateGoal(ctx, parsed)
	case "review_list":
		reply = d.intakeReviewList(ctx)
	case "goal_status":
		reply = d.intakeGoalStatus(ctx, parsed.GoalID)
	case "goal_query":
		reply = d.intakeGoalQuery(ctx, parsed.GoalQuery)
	case "create_schedule":
		reply = d.intakeCreateSchedule(ctx, parsed)
	case "schedule_list":
		reply = d.intakeScheduleList(ctx)
	case "schedule_stop":
		reply = d.intakeScheduleStop(ctx, parsed)
	case "create_agent":
		reply = d.intakeCreateAgent(ctx, parsed)
	case "create_squad":
		reply = d.intakeCreateSquad(ctx, parsed)
	default:
		reply = "没听懂这条指令 😅 你可以这样问我：\n- “创建任务 <标题>，让 <agent> 在 <domain> 上做 <描述>”\n- “查看待审批”\n- “查询任务状态 <id>”\n- “查看任务列表”\n- “查看已完成的任务”\n- “最近一条已完成的任务”\n- “查看 2026-01-01 到 2026-08-01 已完成的任务”\n- “每 1 个小时做 <任务>”\n- “查看定时任务”\n- “停掉定时任务 <名字>”\n- “创建 agent <名字>，用 <运行时>，<人设描述>”\n- “创建 squad <名字>，leader 是 <agent>，成员有 <agent1> <agent2>”"
	}
	if _, err := d.st.DB().ExecContext(ctx,
		`UPDATE run SET status='completed', result_summary=?, finished_at=? WHERE id=?`,
		reply, nowStr(), q.RunID); err != nil {
		logging.Infof("daemon: finish intake run %s: %v", q.RunID, err)
	}
	if n := d.imNotifier(); n != nil {
		var err error
		if parsed.Intent == "goal_query" {
			err = n.SendMarkdown(reply)
		} else {
			err = n.Send(reply)
		}
		if err != nil {
			logging.Errorf("daemon: intake reply: %v", err)
		}
	}
	logging.Infof("daemon: intake %s → %s", q.RunID, parsed.Intent)
}

// intakeCreateGoal creates the goal (active → first run enqueued) through the
// service layer — the goal layer validates assignee/domain, so a parser
// hallucinating an id fails here with the validator's message, not a
// platform crash. Two-branch ask-once structure: merge-from-draft on the
// clarification turn, else collect ALL missing fields and ask once.
func (d *Daemon) intakeCreateGoal(ctx context.Context, parsed intakeAction) string {
	g := parsed.Goal
	hasAgents := d.platformHasAgents(ctx)
	if draft, ok := d.loadDraftOfKind(ctx, "goal"); ok {
		merged := mergeGoal(draft.Payload, g)
		if d.intakeSvc != nil {
			_ = d.intakeSvc.ClearDraft(ctx)
		}
		return d.doCreateGoal(ctx, merged, hasAgents)
	}
	if missing := goalMissingFields(g, hasAgents); len(missing) > 0 {
		// No agents at all → there is nothing to ask about (the assignee slot
		// is empty and the roster is empty); surface the setup hint, not an ask.
		if !hasAgents && strings.TrimSpace(g.AssigneeID) == "" {
			return "创建任务失败：没有可用的 agent（先在 Web 配置 agent）"
		}
		return d.collectAndAsk(ctx, "goal", mustMarshal(g), missing)
	}
	return d.doCreateGoal(ctx, g, hasAgents)
}

// doCreateGoal calls the service layer and returns the reply. P0-2
// (决策 6-15②): the active goal's first run is born in Create's transaction
// — no separate enqueue. hasAgents gates the assignee check: a merge that
// still lacks an assignee (vague reply) fails here only when agents exist
// (otherwise the goal layer would reject it; we surface the clearer message).
func (d *Daemon) doCreateGoal(ctx context.Context, g goalAction, hasAgents bool) string {
	if strings.TrimSpace(g.Title) == "" {
		return "创建任务失败：缺少标题"
	}
	if strings.TrimSpace(g.DomainID) == "" {
		return "创建任务失败：缺少项目/仓库"
	}
	if hasAgents && strings.TrimSpace(g.AssigneeID) == "" {
		return "创建任务失败：没有可用的 agent（先在 Web 配置 agent）"
	}
	created, err := d.goalSvc.Create(ctx, service.Goal{
		Title:         g.Title,
		Description:   g.Description,
		DomainID:      g.DomainID,
		AssigneeType:  "agent",
		AssigneeID:    g.AssigneeID,
		Status:        "active",
		CreatedByType: "human",
	})
	if err != nil {
		return "创建任务失败：" + err.Error()
	}
	return fmt.Sprintf("✅ 已创建任务：%s（goal %s），agent 开始执行", created.Title, shortID(created.ID))
}

// intakeDomainList lists the available domains for the clarification ask.
func (d *Daemon) intakeDomainList(ctx context.Context) string {
	var b strings.Builder
	rows, err := d.st.DB().QueryContext(ctx, `SELECT name FROM domain ORDER BY name`)
	if err != nil {
		return "（当前没有可用项目——先在 Web 建域）"
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		fmt.Fprintf(&b, "- %s\n", name)
		n++
	}
	if n == 0 {
		return "（当前没有可用项目——先在 Web 建域）"
	}
	return b.String()
}

// intakeReviewList answers "待审批" with the current checkpoint queue.
func (d *Daemon) intakeReviewList(ctx context.Context) string {
	if d.qs == nil {
		return "平台未就绪（store 未接线）"
	}
	goals, err := d.qs.ReviewGoals(ctx)
	if err != nil {
		return "查询失败：" + err.Error()
	}
	if len(goals) == 0 {
		return "✅ 当前没有待审批的卡点"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "🔔 待审批（%d 个）：\n", len(goals))
	for _, g := range goals {
		fmt.Fprintf(&b, "- %s（%s）\n  %s\n", g.Title, shortID(g.GoalID), firstLineIn(g.Reason))
	}
	b.WriteString("\n在飞书里点对应卡片的按钮，或打开 Web 审批队列处理。")
	return b.String()
}

// intakeGoalStatus answers "状态 <id>" (id or short id) with the goal's
// state and its last run's outcome.
func (d *Daemon) intakeGoalStatus(ctx context.Context, id string) string {
	if d.qs == nil {
		return "平台未就绪（store 未接线）"
	}
	if strings.TrimSpace(id) == "" {
		return "查询任务状态需要任务 id（如：查询任务状态 3f2a1b）"
	}
	v, err := d.qs.GoalStatus(ctx, strings.TrimSpace(id))
	if err != nil {
		return "查询失败：找不到该任务（" + err.Error() + "）"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "📌 %s（%s）\n状态：%s", v.Title, shortID(v.GoalID), v.Status)
	if v.ReviewRequest != "" {
		b.WriteString("\n待审：" + v.ReviewRequest)
	}
	if v.Summary != "" {
		b.WriteString("\n最近结果：" + truncateIn(v.Summary, 200))
	}
	return b.String()
}

// goalStatusLabels maps goal status values to Chinese display labels for the
// goal_query reply header.
var goalStatusLabels = map[string]string{
	"done":      "已完成的任务",
	"active":    "进行中的任务",
	"backlog":   "待办任务",
	"failed":    "失败的任务",
	"review":    "待审批的任务",
	"cancelled": "已取消的任务",
}

const maxGoalListSize = 30

// intakeGoalQuery answers "查看任务列表" / "查看已完成的任务" etc. — filters
// goals by status and/or created_at date range, limits to last_n (or 30 max),
// resolves assignee ids to names, and formats the reply as a markdown card.
func (d *Daemon) intakeGoalQuery(ctx context.Context, q goalQueryAction) string {
	all, err := d.goalSvc.List(ctx)
	if err != nil {
		return "查询失败：" + err.Error()
	}

	status := strings.TrimSpace(q.Status)
	fromTime, toTime := parseGoalQueryDateRange(q.FromDate, q.ToDate)
	filtered := filterGoals(all, status, fromTime, toTime)

	total := len(filtered)
	if total == 0 {
		if status != "" {
			return fmt.Sprintf("📋 没有状态为 %s 的任务", status)
		}
		return "📋 当前没有任务"
	}

	limit := maxGoalListSize
	if q.LastN > 0 && q.LastN < limit {
		limit = q.LastN
	}
	if limit > total {
		limit = total
	}

	nameMap := d.loadAssigneeNames(ctx)

	var b strings.Builder
	b.WriteString(formatGoalQueryHeader(status, q.FromDate, q.ToDate, total, limit))
	b.WriteString("\n\n")
	for i := 0; i < limit; i++ {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(formatGoalEntry(filtered[i], nameMap))
	}
	if limit < total {
		fmt.Fprintf(&b, "\n共 %d 个，当前显示前 %d 个。", total, limit)
	}
	return b.String()
}

// parseGoalQueryDateRange converts from_date/to_date (YYYY-MM-DD) into time
// bounds: fromTime at start-of-day, toTime at end-of-day. Empty strings →
// zero time (unbounded).
func parseGoalQueryDateRange(fromDate, toDate string) (fromTime, toTime time.Time) {
	fromDate = strings.TrimSpace(fromDate)
	toDate = strings.TrimSpace(toDate)
	if fromDate != "" {
		if t, err := time.ParseInLocation("2006-01-02", fromDate, time.Local); err == nil {
			fromTime = t
		}
	}
	if toDate != "" {
		if t, err := time.ParseInLocation("2006-01-02", toDate, time.Local); err == nil {
			toTime = t.Add(24*time.Hour - time.Second)
		}
	}
	return fromTime, toTime
}

// filterGoals returns goals matching the status and created_at date range.
// A zero status or zero time means no filter on that dimension.
func filterGoals(goals []service.Goal, status string, fromTime, toTime time.Time) []service.Goal {
	hasDateFilter := !fromTime.IsZero() || !toTime.IsZero()
	out := []service.Goal{}
	for _, g := range goals {
		if status != "" && g.Status != status {
			continue
		}
		if hasDateFilter {
			ct, err := time.Parse(time.RFC3339Nano, g.CreatedAt)
			if err != nil {
				continue
			}
			if !fromTime.IsZero() && ct.Before(fromTime) {
				continue
			}
			if !toTime.IsZero() && ct.After(toTime) {
				continue
			}
		}
		out = append(out, g)
	}
	return out
}

// loadAssigneeNames queries agent and squad tables, returning a map of
// id→name. Squad entries are keyed "squad:<id>" to avoid collision with
// agent ids.
func (d *Daemon) loadAssigneeNames(ctx context.Context) map[string]string {
	nameMap := map[string]string{}
	queryNames := func(table, prefix string) {
		rows, err := d.st.DB().QueryContext(ctx, "SELECT id, name FROM "+table)
		if err != nil {
			return
		}
		defer rows.Close()
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err == nil {
				nameMap[prefix+id] = name
			}
		}
	}
	queryNames("agent", "")
	queryNames("squad", "squad:")
	return nameMap
}

// resolveAssigneeName maps a goal's assignee_id to a display name using the
// nameMap from loadAssigneeNames. Falls back to short id (or "未分配"/"人工"
// for empty ids).
func resolveAssigneeName(g service.Goal, nameMap map[string]string) string {
	if g.AssigneeID == "" {
		if g.AssigneeType == "human" {
			return "人工"
		}
		return "未分配"
	}
	prefix := ""
	if g.AssigneeType == "squad" {
		prefix = "squad:"
	}
	if name, ok := nameMap[prefix+g.AssigneeID]; ok {
		return name
	}
	return shortID(g.AssigneeID)
}

// formatGoalQueryHeader builds the header line: emoji + label + optional date
// range + count.
func formatGoalQueryHeader(status, fromDate, toDate string, total, shown int) string {
	header := "📋 "
	if status != "" {
		if label, ok := goalStatusLabels[status]; ok {
			header += label
		} else {
			header += "状态：" + status + " 的任务"
		}
	} else {
		header += "任务列表"
	}
	fromDate = strings.TrimSpace(fromDate)
	toDate = strings.TrimSpace(toDate)
	if fromDate != "" || toDate != "" {
		header += fmt.Sprintf(" %s ~ %s", fromDate, toDate)
	}
	header += fmt.Sprintf("（共 %d 个）", total)
	return header
}

// formatGoalEntry renders one goal as a markdown block (title bold, then
// description/assignee/status/created-at lines).
func formatGoalEntry(g service.Goal, nameMap map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "**- %s（%s）**\n", g.Title, shortID(g.ID))
	if g.Description != "" {
		fmt.Fprintf(&b, "  **描述**：%s\n", firstLineIn(g.Description))
	}
	fmt.Fprintf(&b, "  **分配给**：%s（%s）｜**状态**：%s", resolveAssigneeName(g, nameMap), g.AssigneeType, g.Status)
	if ct, err := time.Parse(time.RFC3339Nano, g.CreatedAt); err == nil {
		fmt.Fprintf(&b, "｜**创建时间**：%s", ct.Local().Format("2006-01-02"))
	}
	b.WriteString("\n")
	return b.String()
}

// intakeCreateSchedule creates a cron schedule through the service layer —
// the parser converts natural-language frequency to cron, the platform
// validates (cron syntax, assignee/domain existence) and computes the first
// next_run_at.
func (d *Daemon) intakeCreateSchedule(ctx context.Context, parsed intakeAction) string {
	sch := parsed.Schedule
	if strings.TrimSpace(sch.Name) == "" || strings.TrimSpace(sch.Title) == "" {
		return "创建定时任务失败：缺少任务名或任务标题"
	}
	if strings.TrimSpace(sch.Cron) == "" {
		return "创建定时任务失败：缺少 cron 表达式（没听懂频率？）"
	}
	if strings.TrimSpace(sch.DomainID) == "" {
		return "创建定时任务失败：没有可用的 domain（先在 Web 建域并配置验收策略）"
	}
	if strings.TrimSpace(sch.AssigneeID) == "" {
		return "创建定时任务失败：没有可用的 agent（先在 Web 配置 agent）"
	}
	// The schedule runs on the daemon machine's OWN local time — the owner
	// speaks in their local hours ("每天 9 点"), and on a single-user machine
	// that IS the daemon's zone. Hardcoding a zone (e.g. Asia/Shanghai)
	// silently mis-times every schedule on a machine in another zone.
	timezone := time.Local.String()
	if timezone == "" {
		timezone = "UTC"
	}
	s, err := d.schedSvc.Create(ctx, service.Schedule{
		Name:           sch.Name,
		TitleTemplate:  sch.Title,
		Description:    sch.Description,
		AssigneeType:   "agent",
		AssigneeID:     sch.AssigneeID,
		DomainID:       sch.DomainID,
		CronExpression: sch.Cron,
		Timezone:       timezone,
		Enabled:        true,
	})
	if err != nil {
		return "创建定时任务失败：" + err.Error()
	}
	next := s.NextRunAt
	if next != "" {
		if t, err := time.Parse(time.RFC3339Nano, next); err == nil {
			next = t.Local().Format("01-02 15:04")
		}
	}
	return fmt.Sprintf("✅ 已创建定时任务：%s（%s），下次执行 %s（本地时间）", s.Name, s.CronExpression, next)
}

// intakeScheduleList answers "查看定时任务" with the enabled schedules.
func (d *Daemon) intakeScheduleList(ctx context.Context) string {
	all, err := d.schedSvc.List(ctx)
	if err != nil {
		return "查询失败：" + err.Error()
	}
	enabled := []service.Schedule{}
	for _, s := range all {
		if s.Enabled {
			enabled = append(enabled, s)
		}
	}
	if len(enabled) == 0 {
		return "📭 当前没有启用的定时任务"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "📅 启用的定时任务（%d 个）：\n", len(enabled))
	for _, s := range enabled {
		fmt.Fprintf(&b, "- %s（%s）\n", s.Name, s.CronExpression)
		if s.Description != "" {
			b.WriteString("  " + firstLineIn(s.Description) + "\n")
		}
	}
	b.WriteString("\n停掉某个：发送“停掉定时任务 <名字>”")
	return b.String()
}

// intakeScheduleStop disables a schedule by name (the row and firing history
// stay; dispatchSchedules only fires enabled rows).
func (d *Daemon) intakeScheduleStop(ctx context.Context, parsed intakeAction) string {
	name := strings.TrimSpace(parsed.Schedule.Name)
	if name == "" {
		return "停掉定时任务需要名字（如：停掉定时任务 每小时巡检）"
	}
	all, err := d.schedSvc.List(ctx)
	if err != nil {
		return "查询失败：" + err.Error()
	}
	var target *service.Schedule
	for i := range all {
		if all[i].Enabled && all[i].Name == name {
			target = &all[i]
			break
		}
	}
	if target == nil {
		return fmt.Sprintf("没找到启用的定时任务 %q——先“查看定时任务”确认名字", name)
	}
	if _, err := d.schedSvc.SetEnabled(ctx, target.ID, false); err != nil {
		return "停用失败：" + err.Error()
	}
	return fmt.Sprintf("⏹ 已停用定时任务：%s（%s）", target.Name, target.CronExpression)
}

// intakeCreateAgent creates an agent (persona + runtime + optional skills)
// through the service layer. Two branches:
//   - Clarification turn (agent-kind draft exists): merge draft + this turn's
//     parsed fields, clear the draft, and commit — the "ask at most once"
//     guarantee means a vague reply still goes to Create (which validates);
//     it never re-asks.
//   - Fresh create: collect ALL missing required fields at once, save the
//     parser's partial output as a draft, and ask in ONE message.
func (d *Daemon) intakeCreateAgent(ctx context.Context, parsed intakeAction) string {
	a := parsed.Agent
	if draft, ok := d.loadDraftOfKind(ctx, "agent"); ok {
		merged := mergeAgent(draft.Payload, a)
		if d.intakeSvc != nil {
			_ = d.intakeSvc.ClearDraft(ctx)
		}
		return d.doCreateAgent(ctx, merged)
	}
	if missing := agentMissingFields(a, d.platformHasSkills(ctx)); len(missing) > 0 {
		return d.collectAndAsk(ctx, "agent", mustMarshal(a), missing)
	}
	return d.doCreateAgent(ctx, a)
}

// doCreateAgent calls the service layer and returns the reply — no
// validation, no draft logic. The service validates name/runtime_id and the
// runtime's existence; a hallucinated or still-empty id fails here with the
// validator's message (a terminal error, not a re-ask).
func (d *Daemon) doCreateAgent(ctx context.Context, a agentAction) string {
	created, err := d.agentSvc.Create(ctx, service.Agent{
		Name:         a.Name,
		RuntimeID:    a.RuntimeID,
		Description:  a.Description,
		SystemPrompt: a.SystemPrompt,
		Skills:       a.Skills,
	})
	if err != nil {
		return "创建 agent 失败：" + err.Error()
	}
	return fmt.Sprintf("✅ 已创建 agent：%s（%s）", created.Name, shortID(created.ID))
}

// intakeCreateSquad creates a squad (leader + optional members). Same
// two-branch ask-once structure as the agent handler.
func (d *Daemon) intakeCreateSquad(ctx context.Context, parsed intakeAction) string {
	sq := parsed.Squad
	if draft, ok := d.loadDraftOfKind(ctx, "squad"); ok {
		merged := mergeSquad(draft.Payload, sq)
		if d.intakeSvc != nil {
			_ = d.intakeSvc.ClearDraft(ctx)
		}
		return d.doCreateSquad(ctx, merged)
	}
	if missing := squadMissingFields(sq); len(missing) > 0 {
		return d.collectAndAsk(ctx, "squad", mustMarshal(sq), missing)
	}
	return d.doCreateSquad(ctx, sq)
}

// doCreateSquad creates the squad then attaches members (skipping the leader,
// which is already squad.leader_id). Hallucinated members surface as a
// partial-success reply.
func (d *Daemon) doCreateSquad(ctx context.Context, sq squadAction) string {
	created, err := d.squadSvc.Create(ctx, service.Squad{
		Name:         sq.Name,
		LeaderID:     sq.LeaderID,
		Description:  sq.Description,
		Instructions: sq.Instructions,
	})
	if err != nil {
		return "创建 squad 失败：" + err.Error()
	}
	var failed []string
	for _, mid := range sq.MemberIDs {
		mid = strings.TrimSpace(mid)
		if mid == "" || mid == sq.LeaderID {
			continue
		}
		if _, err := d.squadSvc.AddMember(ctx, created.ID, "agent", mid, "member"); err != nil {
			failed = append(failed, mid)
		}
	}
	if len(failed) > 0 {
		return fmt.Sprintf("⚠️ 已创建 squad：%s（%s），但以下成员添加失败：%s", created.Name, shortID(created.ID), strings.Join(failed, "、"))
	}
	return fmt.Sprintf("✅ 已创建 squad：%s（%s）", created.Name, shortID(created.ID))
}

// collectAndAsk builds ONE clarification message listing all missing required
// fields + the relevant rosters, saves the parser's partial output as a draft,
// and returns the message. The rosters included depend on kind and which
// fields are missing.
func (d *Daemon) collectAndAsk(ctx context.Context, kind, payloadJSON string, missing []string) string {
	var b strings.Builder
	switch kind {
	case "goal":
		b.WriteString("创建任务还需要以下信息：\n")
	case "agent":
		b.WriteString("创建 agent 还需要以下信息：\n")
	case "squad":
		b.WriteString("创建 squad 还需要以下信息：\n")
	}
	for _, f := range missing {
		b.WriteString("- " + f + "\n")
	}
	b.WriteString("\n")
	// Rosters relevant to the kind.
	switch kind {
	case "goal":
		b.WriteString("当前可用项目：\n" + d.intakeDomainList(ctx))
		if d.platformHasAgents(ctx) {
			b.WriteString("当前可用 agent：\n" + d.intakeAgentList(ctx))
		}
	case "agent":
		b.WriteString("当前可用运行时：\n" + d.intakeRuntimeList(ctx))
		if d.platformHasSkills(ctx) {
			b.WriteString("\n平台 skill（选配，回复里带上要配的 skill 名，或明确回复“不要”）：\n" + d.intakeSkillList(ctx))
		}
	case "squad":
		b.WriteString("当前可用 agent：\n" + d.intakeAgentList(ctx))
	}
	b.WriteString("\n请一条消息回复所有信息。")
	if d.intakeSvc != nil {
		_ = d.intakeSvc.SaveDraft(ctx, notify.IntakeDraft{
			Kind: kind, Payload: payloadJSON, CreatedAt: nowStr(),
		})
	}
	return b.String()
}

// loadDraftOfKind returns the pending draft if its Kind matches. Other kinds
// (or none) are treated as absent — the flows do not interfere.
func (d *Daemon) loadDraftOfKind(ctx context.Context, kind string) (*notify.IntakeDraft, bool) {
	if d.intakeSvc == nil {
		return nil, false
	}
	draft, ok := d.intakeSvc.LoadDraft(ctx)
	if !ok || draft.Kind != kind {
		return nil, false
	}
	return draft, true
}

// mergeAgent merges a clarification reply onto a draft: the reply's non-empty
// fields override the draft's. Skills is taken from the reply only when
// SkillsSpecified is true (the reply explicitly selected or declined skills);
// otherwise the draft's skills/specified state is preserved.
func mergeAgent(draftPayload string, reply agentAction) agentAction {
	var draft agentAction
	_ = json.Unmarshal([]byte(draftPayload), &draft)
	if strings.TrimSpace(reply.Name) != "" {
		draft.Name = reply.Name
	}
	if strings.TrimSpace(reply.RuntimeID) != "" {
		draft.RuntimeID = reply.RuntimeID
	}
	if strings.TrimSpace(reply.Description) != "" {
		draft.Description = reply.Description
	}
	if strings.TrimSpace(reply.SystemPrompt) != "" {
		draft.SystemPrompt = reply.SystemPrompt
	}
	if reply.SkillsSpecified {
		draft.Skills = reply.Skills
		draft.SkillsSpecified = true
	}
	return draft
}

// mergeSquad merges a clarification reply onto a draft.
func mergeSquad(draftPayload string, reply squadAction) squadAction {
	var draft squadAction
	_ = json.Unmarshal([]byte(draftPayload), &draft)
	if strings.TrimSpace(reply.Name) != "" {
		draft.Name = reply.Name
	}
	if strings.TrimSpace(reply.LeaderID) != "" {
		draft.LeaderID = reply.LeaderID
	}
	if strings.TrimSpace(reply.Description) != "" {
		draft.Description = reply.Description
	}
	if strings.TrimSpace(reply.Instructions) != "" {
		draft.Instructions = reply.Instructions
	}
	if len(reply.MemberIDs) > 0 {
		draft.MemberIDs = reply.MemberIDs
	}
	return draft
}

// mergeGoal merges a clarification reply onto a draft.
func mergeGoal(draftPayload string, reply goalAction) goalAction {
	var draft goalAction
	_ = json.Unmarshal([]byte(draftPayload), &draft)
	if strings.TrimSpace(reply.Title) != "" {
		draft.Title = reply.Title
	}
	if strings.TrimSpace(reply.Description) != "" {
		draft.Description = reply.Description
	}
	if strings.TrimSpace(reply.AssigneeID) != "" {
		draft.AssigneeID = reply.AssigneeID
	}
	if strings.TrimSpace(reply.DomainID) != "" {
		draft.DomainID = reply.DomainID
	}
	return draft
}

// agentMissingFields returns human names of required agent fields that are
// empty, plus skills when the library is non-empty and the owner did not
// mention skills (SkillsSpecified=false).
func agentMissingFields(a agentAction, hasSkills bool) []string {
	var out []string
	if strings.TrimSpace(a.Name) == "" {
		out = append(out, "名称")
	}
	if strings.TrimSpace(a.RuntimeID) == "" {
		out = append(out, "运行时")
	}
	if hasSkills && !a.SkillsSpecified {
		out = append(out, "skills（选配，或明确回复不要）")
	}
	return out
}

// squadMissingFields returns human names of required squad fields that are
// empty.
func squadMissingFields(sq squadAction) []string {
	var out []string
	if strings.TrimSpace(sq.Name) == "" {
		out = append(out, "名称")
	}
	if strings.TrimSpace(sq.LeaderID) == "" {
		out = append(out, "leader agent")
	}
	return out
}

// goalMissingFields returns human names of required goal fields that are
// empty. assignee_id is only required when the platform has at least one
// agent (otherwise there is nothing to ask — the platform is not configured,
// a hard error).
func goalMissingFields(g goalAction, hasAgents bool) []string {
	var out []string
	if strings.TrimSpace(g.Title) == "" {
		out = append(out, "标题")
	}
	if strings.TrimSpace(g.DomainID) == "" {
		out = append(out, "项目/仓库")
	}
	if hasAgents && strings.TrimSpace(g.AssigneeID) == "" {
		out = append(out, "执行的 agent")
	}
	return out
}

// mustMarshal serializes a value to JSON, returning "{}" on error (should not
// happen for these struct types, but a draft must never fail to save).
func mustMarshal(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(raw)
}

// platformHasSkills reports whether the skill library is non-empty.
func (d *Daemon) platformHasSkills(ctx context.Context) bool {
	var n int
	if err := d.st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM skill`).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// platformHasAgents reports whether at least one agent exists — a goal needs
// an assignee, and if none exist the platform is not configured (hard error,
// not an ask).
func (d *Daemon) platformHasAgents(ctx context.Context) bool {
	var n int
	if err := d.st.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM agent`).Scan(&n); err != nil {
		return false
	}
	return n > 0
}

// intakeRuntimeList lists the active runtimes for the clarification ask.
func (d *Daemon) intakeRuntimeList(ctx context.Context) string {
	rows, err := d.st.DB().QueryContext(ctx, `SELECT name FROM runtime WHERE status='active' ORDER BY name`)
	if err != nil {
		return "（当前没有可用运行时——先在 Web 配置 runtime）"
	}
	defer rows.Close()
	var b strings.Builder
	n := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		fmt.Fprintf(&b, "- %s\n", name)
		n++
	}
	if n == 0 {
		return "（当前没有可用运行时——先在 Web 配置 runtime）"
	}
	return b.String()
}

// intakeSkillList lists the platform skill library for the clarification ask.
func (d *Daemon) intakeSkillList(ctx context.Context) string {
	rows, err := d.st.DB().QueryContext(ctx, `SELECT name FROM skill ORDER BY name`)
	if err != nil {
		return "（查询 skill 失败）"
	}
	defer rows.Close()
	var b strings.Builder
	n := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		fmt.Fprintf(&b, "- %s\n", name)
		n++
	}
	if n == 0 {
		return "（当前没有 skill）"
	}
	return b.String()
}

// intakeAgentList lists the agents for the squad-leader / goal-assignee ask.
func (d *Daemon) intakeAgentList(ctx context.Context) string {
	rows, err := d.st.DB().QueryContext(ctx, `SELECT name FROM agent ORDER BY name`)
	if err != nil {
		return "（当前没有可用 agent——先在 Web 配置 agent）"
	}
	defer rows.Close()
	var b strings.Builder
	n := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			continue
		}
		fmt.Fprintf(&b, "- %s\n", name)
		n++
	}
	if n == 0 {
		return "（当前没有可用 agent——先在 Web 配置 agent）"
	}
	return b.String()
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func firstLineIn(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return strings.TrimSpace(s)
}

func truncateIn(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
