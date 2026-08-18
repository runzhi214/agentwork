package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/eushing/agentwork/internal/events"
	"github.com/eushing/agentwork/internal/notify"
	"github.com/eushing/agentwork/internal/service"
	"github.com/eushing/agentwork/internal/store"
)

// newIntakeDaemon builds the daemon surfaces the intake executor needs: a
// real store with an agent + a gated domain, and the goal/run services.
func newIntakeDaemon(t *testing.T) (*Daemon, *store.Store) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	bus := events.NewBus()
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO runtime (id,name,created_at) VALUES ('rt1','rt1',?)`,
		time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx,
		`INSERT INTO agent (id,name,runtime_id,max_concurrent,created_at) VALUES ('a1','worker1','rt1',1,?)`,
		time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	goalSvc := service.NewGoalService(st, bus)
	runSvc := service.NewRunService(st, bus)
	goalSvc.SetRunService(runSvc)
	runSvc.SetGoalService(goalSvc)
	ds := service.NewDomainService(st, bus)
	if _, err := ds.Create(ctx, service.Domain{Name: "d1", GitURL: "https://e.com/d1.git"}); err != nil {
		t.Fatal(err)
	}
	schedSvc := service.NewScheduleService(st, bus)
	agentSvc := service.NewAgentService(st, bus)
	squadSvc := service.NewSquadService(st, bus)
	intakeSvc := notify.NewIntakeService(notify.NewSQLQueryStore(st), &mapSettings{vals: map[string]string{}}, runSvc)
	d := &Daemon{
		st: st, bus: bus, goalSvc: goalSvc, runSvc: runSvc, schedSvc: schedSvc,
		agentSvc: agentSvc, squadSvc: squadSvc, qs: notify.NewSQLQueryStore(st),
		intakeSvc: intakeSvc,
	}
	return d, st
}

// mapSettings is a SettingsStore fake for tests (mirrors notify/card_test.go).
type mapSettings struct {
	vals map[string]string
}

func (m *mapSettings) Get(_ context.Context, key string) (string, error) {
	return m.vals[key], nil
}
func (m *mapSettings) Set(_ context.Context, key, value string) error {
	m.vals[key] = value
	return nil
}
func (m *mapSettings) Delete(_ context.Context, key string) error {
	delete(m.vals, key)
	return nil
}

// TestIntakeCreateGoal: the platform executes the parsed create action
// through the goal layer — active goal created, first run enqueued. Missing
// required fields produce user-facing messages, not crashes.
func TestIntakeCreateGoal(t *testing.T) {
	d, _ := newIntakeDaemon(t)
	ctx := context.Background()
	domID := firstID(t, ctx, d, `SELECT id FROM domain`)

	reply := d.intakeCreateGoal(ctx, intakeAction{Goal: goalSub("从飞书建的任务", "", "a1", domID)})
	if !strings.Contains(reply, "已创建任务") {
		t.Fatalf("expected creation reply, got %q", reply)
	}
	var runStatus string
	if err := d.st.DB().QueryRowContext(ctx,
		`SELECT r.status FROM run r JOIN goal g ON g.id=r.goal_id WHERE g.title=?`, "从飞书建的任务").Scan(&runStatus); err != nil {
		t.Fatalf("created goal must have a run: %v", err)
	}
	if runStatus != "queued" {
		t.Fatalf("first run must be queued for execution, got %q", runStatus)
	}

	// Missing title → the ask lists 标题 (the first call saved a goal draft
	// for "从飞书建的任务"; clear it so this case asks fresh).
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateGoal(ctx, intakeAction{Goal: goalSub("", "", "a1", domID)}); !strings.Contains(r, "还需要以下信息") || !strings.Contains(r, "标题") {
		t.Fatalf("missing title must ask with 标题, got %q", r)
	}
	// Hallucinated domain → service-layer validator message (all required
	// fields present, so it goes straight to Create).
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateGoal(ctx, intakeAction{Goal: goalSub("x", "", "a1", "nonexistent")}); !strings.Contains(r, "创建任务失败") {
		t.Fatalf("hallucinated domain must fail via the validator, got %q", r)
	}
	// Missing domain (title present) → the ask lists 项目/仓库 + the domain roster.
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateGoal(ctx, intakeAction{Goal: goalSub("修一下", "", "a1", "")}); !strings.Contains(r, "项目/仓库") {
		t.Fatalf("missing domain must ask with 项目/仓库, got %q", r)
	}
}

// TestIntakeReviewListAndStatus: the review queue and the status query are
// answered from the store, short ids accepted.
func TestIntakeReviewListAndStatus(t *testing.T) {
	d, st := newIntakeDaemon(t)
	ctx := context.Background()
	domID := firstID(t, ctx, d, `SELECT id FROM domain`)
	g, err := d.goalSvc.Create(ctx, service.Goal{
		Title: "待审", DomainID: domID, AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB().ExecContext(ctx,
		`UPDATE goal SET status='review', review_request='merge: 必审' WHERE id=?`, g.ID); err != nil {
		t.Fatal(err)
	}

	reply := d.intakeReviewList(ctx)
	if !strings.Contains(reply, "待审") || !strings.Contains(reply, "待审批") {
		t.Fatalf("review list must carry the pending goal: %q", reply)
	}

	status := d.intakeGoalStatus(ctx, g.ID[:8])
	if !strings.Contains(status, "review") || !strings.Contains(status, "待审") {
		t.Fatalf("status query must resolve the short id: %q", status)
	}
	if r := d.intakeGoalStatus(ctx, "zzzzzzzz"); !strings.Contains(r, "查询失败") {
		t.Fatalf("unknown id must fail cleanly: %q", r)
	}
}

// TestIntakeCreateSchedule: the platform executes the parsed schedule
// action through the service layer (cron validated, next_run computed);
// schedule_list and schedule_stop round-trip.
func TestIntakeCreateSchedule(t *testing.T) {
	d, _ := newIntakeDaemon(t)
	ctx := context.Background()
	domID := firstID(t, ctx, d, `SELECT id FROM domain`)

	reply := d.intakeCreateSchedule(ctx, intakeAction{Intent: "create_schedule", Schedule: struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Cron        string `json:"cron"`
		AssigneeID  string `json:"assignee_id"`
		DomainID    string `json:"domain_id"`
	}{Name: "每小时巡检", Title: "定时巡检", Cron: "0 * * * *", AssigneeID: "a1", DomainID: domID}})
	if !strings.Contains(reply, "已创建定时任务") {
		t.Fatalf("expected creation reply, got %q", reply)
	}
	// Bad cron → the validator's message, not a crash.
	if r := d.intakeCreateSchedule(ctx, intakeAction{Intent: "create_schedule", Schedule: struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Cron        string `json:"cron"`
		AssigneeID  string `json:"assignee_id"`
		DomainID    string `json:"domain_id"`
	}{Name: "坏cron", Title: "x", Cron: "not-a-cron", AssigneeID: "a1", DomainID: domID}}); !strings.Contains(r, "创建定时任务失败") {
		t.Fatalf("bad cron must surface the validator message, got %q", r)
	}

	list := d.intakeScheduleList(ctx)
	if !strings.Contains(list, "每小时巡检") || !strings.Contains(list, "0 * * * *") {
		t.Fatalf("schedule list must carry the created schedule: %q", list)
	}

	stop := d.intakeScheduleStop(ctx, intakeAction{Intent: "schedule_stop", Schedule: struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Cron        string `json:"cron"`
		AssigneeID  string `json:"assignee_id"`
		DomainID    string `json:"domain_id"`
	}{Name: "每小时巡检"}})
	if !strings.Contains(stop, "已停用") {
		t.Fatalf("expected stop reply, got %q", stop)
	}
	if r := d.intakeScheduleStop(ctx, intakeAction{Intent: "schedule_stop", Schedule: struct {
		Name        string `json:"name"`
		Title       string `json:"title"`
		Description string `json:"description"`
		Cron        string `json:"cron"`
		AssigneeID  string `json:"assignee_id"`
		DomainID    string `json:"domain_id"`
	}{Name: "不存在"}}); !strings.Contains(r, "没找到") {
		t.Fatalf("stopping an unknown schedule must say so, got %q", r)
	}
	// Disabled schedules drop out of the list.
	if r := d.intakeScheduleList(ctx); strings.Contains(r, "每小时巡检") {
		t.Fatalf("disabled schedule must leave the list: %q", r)
	}
}

// TestIntakeGoalQuery: the goal query filters by status, resolves assignee
// names, paginates (10 per page), filters by created_at date range, and
// always shows "创建时间" (stable — no longer depends on run.finished_at).
func TestIntakeGoalQuery(t *testing.T) {
	d, st := newIntakeDaemon(t)
	ctx := context.Background()
	domID := firstID(t, ctx, d, `SELECT id FROM domain`)

	// Two done goals + one active goal.
	g1, err := d.goalSvc.Create(ctx, service.Goal{
		Title: "已完成任务A", Description: "修复登录bug", DomainID: domID,
		AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	g2, err := d.goalSvc.Create(ctx, service.Goal{
		Title: "已完成任务B", Description: "添加导出功能", DomainID: domID,
		AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	g3, err := d.goalSvc.Create(ctx, service.Goal{
		Title: "进行中的任务", Description: "优化性能", DomainID: domID,
		AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	_ = g3
	for _, gid := range []string{g1.ID, g2.ID} {
		if _, err := st.DB().ExecContext(ctx,
			`UPDATE goal SET status='done' WHERE id=?`, gid); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Query all done goals → both g1 and g2, with "创建时间" always shown.
	reply := d.intakeGoalQuery(ctx, goalQueryAction{Status: "done"})
	if !strings.Contains(reply, "已完成任务A") || !strings.Contains(reply, "已完成任务B") {
		t.Fatalf("done query must list both done goals: %q", reply)
	}
	if strings.Contains(reply, "进行中的任务") {
		t.Fatalf("done query must not list active goals: %q", reply)
	}
	if !strings.Contains(reply, "worker1") {
		t.Fatalf("done query must resolve assignee name: %q", reply)
	}
	if !strings.Contains(reply, "done") {
		t.Fatalf("done query must show status: %q", reply)
	}
	if !strings.Contains(reply, "创建时间") {
		t.Fatalf("done query must always show 创建时间 (no longer depends on run.finished_at): %q", reply)
	}
	if !strings.Contains(reply, "已完成的任务") {
		t.Fatalf("done query header must say 已完成的任务: %q", reply)
	}

	// 2. Query all goals (no status filter) → all three.
	reply = d.intakeGoalQuery(ctx, goalQueryAction{})
	if !strings.Contains(reply, "已完成任务A") || !strings.Contains(reply, "进行中的任务") {
		t.Fatalf("unfiltered query must list all goals: %q", reply)
	}
	if !strings.Contains(reply, "任务列表") {
		t.Fatalf("unfiltered query header must say 任务列表: %q", reply)
	}

	// 3. Query active goals → only g3.
	reply = d.intakeGoalQuery(ctx, goalQueryAction{Status: "active"})
	if !strings.Contains(reply, "进行中的任务") {
		t.Fatalf("active query must list active goal: %q", reply)
	}
	if strings.Contains(reply, "已完成任务A") {
		t.Fatalf("active query must not list done goals: %q", reply)
	}

	// 4. Empty result → friendly message.
	reply = d.intakeGoalQuery(ctx, goalQueryAction{Status: "failed"})
	if !strings.Contains(reply, "没有") {
		t.Fatalf("empty result must say so: %q", reply)
	}

	// 5. Date range filter by created_at: set g1's created_at to old, g2's to
	// today — a range covering only recent dates excludes g1.
	oldDate := time.Now().AddDate(0, 0, -30).Format(time.RFC3339Nano)
	if _, err := st.DB().ExecContext(ctx,
		`UPDATE goal SET created_at=? WHERE id=?`, oldDate, g1.ID); err != nil {
		t.Fatal(err)
	}
	todayStr := time.Now().Format("2006-01-02")
	reply = d.intakeGoalQuery(ctx, goalQueryAction{
		Status: "done", FromDate: todayStr, ToDate: todayStr})
	if strings.Contains(reply, "已完成任务A") {
		t.Fatalf("date range must exclude 30-day-old goal: %q", reply)
	}
	if !strings.Contains(reply, "已完成任务B") {
		t.Fatalf("date range must include today's goal: %q", reply)
	}
	if !strings.Contains(reply, todayStr+" ~ "+todayStr) {
		t.Fatalf("header must show date range: %q", reply)
	}

	// 6. LastN: create 25 done goals, verify last_n limits the shown count.
	d2, st2 := newIntakeDaemon(t)
	domID2 := firstID(t, ctx, d2, `SELECT id FROM domain`)
	for i := 0; i < 25; i++ {
		g, err := d2.goalSvc.Create(ctx, service.Goal{
			Title: fmt.Sprintf("批量任务%02d", i), DomainID: domID2,
			AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st2.DB().ExecContext(ctx,
			`UPDATE goal SET status='done' WHERE id=?`, g.ID); err != nil {
			t.Fatal(err)
		}
	}
	// LastN=1: only 1 entry shown, header says "共 25 个".
	reply = d2.intakeGoalQuery(ctx, goalQueryAction{Status: "done", LastN: 1})
	if !strings.Contains(reply, "共 25 个") {
		t.Fatalf("last_n=1 must still show total count: %q", reply)
	}
	if strings.Count(reply, "**- ") != 1 {
		t.Fatalf("last_n=1 must show exactly 1 goal entry, got %d: %q", strings.Count(reply, "**- "), reply)
	}
	// LastN=3: exactly 3 entries.
	reply = d2.intakeGoalQuery(ctx, goalQueryAction{Status: "done", LastN: 3})
	if strings.Count(reply, "**- ") != 3 {
		t.Fatalf("last_n=3 must show exactly 3 goal entries, got %d: %q", strings.Count(reply, "**- "), reply)
	}
	// No LastN: all 25 shown (under 30 cap), no truncation notice.
	reply = d2.intakeGoalQuery(ctx, goalQueryAction{Status: "done"})
	if strings.Count(reply, "**- ") != 25 {
		t.Fatalf("no last_n must show all 25 goals, got %d: %q", strings.Count(reply, "**- "), reply)
	}
	if strings.Contains(reply, "当前显示前") {
		t.Fatalf("under-30 list must not show truncation notice: %q", reply)
	}

	// 7. Max cap: 35 goals, no last_n → capped at 30 + truncation notice.
	d3, st3 := newIntakeDaemon(t)
	domID3 := firstID(t, ctx, d3, `SELECT id FROM domain`)
	for i := 0; i < 35; i++ {
		g, err := d3.goalSvc.Create(ctx, service.Goal{
			Title: fmt.Sprintf("超限任务%02d", i), DomainID: domID3,
			AssigneeType: "agent", AssigneeID: "a1", Status: "active"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st3.DB().ExecContext(ctx,
			`UPDATE goal SET status='done' WHERE id=?`, g.ID); err != nil {
			t.Fatal(err)
		}
	}
	reply = d3.intakeGoalQuery(ctx, goalQueryAction{Status: "done"})
	if strings.Count(reply, "**- ") != 30 {
		t.Fatalf("35 goals with no last_n must cap at 30, got %d: %q", strings.Count(reply, "**- "), reply)
	}
	if !strings.Contains(reply, "共 35 个") || !strings.Contains(reply, "当前显示前 30 个") {
		t.Fatalf("truncated list must show notice: %q", reply)
	}
}

func firstID(t *testing.T, ctx context.Context, d *Daemon, q string) string {
	t.Helper()
	var id string
	if err := d.st.DB().QueryRowContext(ctx, q).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// agentSub builds an agentAction for test call sites (so each test does not
// re-declare the full struct literal).
func agentSub(Name, RuntimeID, Description, SystemPrompt string, Skills []string, SkillsSpecified bool) agentAction {
	return agentAction{
		Name: Name, RuntimeID: RuntimeID, Description: Description,
		SystemPrompt: SystemPrompt, Skills: Skills, SkillsSpecified: SkillsSpecified,
	}
}

// goalSub builds a goalAction for test call sites.
func goalSub(Title, Description, AssigneeID, DomainID string) goalAction {
	return goalAction{Title: Title, Description: Description, AssigneeID: AssigneeID, DomainID: DomainID}
}

// squadSub builds a squadAction for test call sites.
func squadSub(Name, LeaderID, Description, Instructions string, MemberIDs []string) squadAction {
	return squadAction{
		Name: Name, LeaderID: LeaderID, Description: Description,
		Instructions: Instructions, MemberIDs: MemberIDs,
	}
}

// TestIntakeCreateAgent: the platform executes create_agent through the
// agent service — persona + runtime + optional skills. Skills clarification
// fires only when the library is non-empty AND the owner did not mention
// skills (skills_specified=false). The clarification draft completes the
// agent on the owner's next reply.
func TestIntakeCreateAgent(t *testing.T) {
	ctx := context.Background()

	// --- No skills on the platform: created directly, no clarification. ---
	d, st := newIntakeDaemon(t)
	reply := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"代码审查", "rt1", "审查 PR 代码质量", "你是代码审查员", nil, false)})
	if !strings.Contains(reply, "已创建 agent") {
		t.Fatalf("expected creation reply, got %q", reply)
	}
	var agentName, agentSkills string
	if err := st.DB().QueryRowContext(ctx,
		`SELECT name, skills FROM agent WHERE name=?`, "代码审查").Scan(&agentName, &agentSkills); err != nil {
		t.Fatalf("created agent must exist: %v", err)
	}
	if agentSkills != "[]" {
		t.Fatalf("agent with no skills must store []: got %q", agentSkills)
	}

	// Each missing-field case saves a draft; the daemon is shared across
	// these cases, so clear the draft between them (or a later case would
	// merge onto the earlier case's draft instead of asking fresh).
	clearIntakeDraft := func() { _ = d.intakeSvc.ClearDraft(ctx) }

	// Missing name → the ask lists 名称 (no hard "缺少名称" anymore).
	clearIntakeDraft()
	if r := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"", "rt1", "x", "y", nil, true)}); !strings.Contains(r, "还需要以下信息") || !strings.Contains(r, "名称") {
		t.Fatalf("missing name must ask with 名称, got %q", r)
	}
	// Missing runtime_id → the ask lists 运行时 + the runtime roster.
	clearIntakeDraft()
	if r := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"无运行时", "", "x", "y", nil, true)}); !strings.Contains(r, "运行时") || !strings.Contains(r, "rt1") {
		t.Fatalf("missing runtime must ask with the runtime roster, got %q", r)
	}
	// Missing BOTH name and runtime → one ask listing both (not two round-trips).
	clearIntakeDraft()
	if r := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"", "", "", "", nil, true)}); !strings.Contains(r, "名称") || !strings.Contains(r, "运行时") {
		t.Fatalf("missing name+runtime must ask for both at once, got %q", r)
	}
	// Hallucinated runtime_id → service-layer validator message (all fields
	// present, so it goes straight to Create, which rejects the bad id).
	clearIntakeDraft()
	if r := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"坏runtime", "nope", "x", "y", nil, true)}); !strings.Contains(r, "创建 agent 失败") {
		t.Fatalf("hallucinated runtime must fail via the validator, got %q", r)
	}

	// Ask-at-most-once: after a draft is saved, a VAGUE reply that still
	// misses required fields must NOT re-ask — it merges, clears the draft,
	// and fails at the service layer (terminal error). No second ask.
	clearIntakeDraft()
	if r := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"", "", "", "", nil, true)}); !strings.Contains(r, "还需要以下信息") {
		t.Fatalf("setup: missing name+runtime must ask, got %q", r)
	}
	// Draft is now saved. Vague reply supplies nothing useful.
	vague := d.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"", "", "", "", nil, true)})
	if strings.Contains(vague, "还需要以下信息") {
		t.Fatalf("vague reply must NOT re-ask (ask-at-most-once), got %q", vague)
	}
	if !strings.Contains(vague, "创建 agent 失败") {
		t.Fatalf("vague reply must fail at the service layer, got %q", vague)
	}
	if _, ok := d.loadDraftOfKind(ctx, "agent"); ok {
		t.Fatal("draft must be cleared after the clarification turn (even on failure)")
	}

	// --- Skills on the platform + owner did not mention skills → clarify. ---
	d2, st2 := newIntakeDaemon(t)
	if _, err := st2.DB().ExecContext(ctx,
		`INSERT INTO skill (id,name,description,created_at) VALUES ('sk1','git-helper','git helper',?)`,
		time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	reply = d2.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"带skill的agent", "rt1", "x", "y", nil, false)})
	if !strings.Contains(reply, "还需要以下信息") || !strings.Contains(reply, "skills") || !strings.Contains(reply, "git-helper") {
		t.Fatalf("must ask for skills (listed as missing) and show the skill, got %q", reply)
	}
	// The draft is saved — the clarification turn must build from it.
	if _, ok := d2.loadDraftOfKind(ctx, "agent"); !ok {
		t.Fatal("agent-kind draft must be saved for the clarification")
	}
	// Clarification turn: owner picks skills → agent created from the draft
	// (the draft carried name/runtime/description/system_prompt; the reply
	// supplies only skills).
	reply = d2.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"", "", "", "", []string{"sk1"}, true)})
	if !strings.Contains(reply, "已创建 agent") {
		t.Fatalf("clarification turn must create the agent, got %q", reply)
	}
	if _, ok := d2.loadDraftOfKind(ctx, "agent"); ok {
		t.Fatal("draft must be cleared after the agent is created")
	}
	var skillsJSON string
	if err := st2.DB().QueryRowContext(ctx,
		`SELECT skills FROM agent WHERE name=?`, "带skill的agent").Scan(&skillsJSON); err != nil {
		t.Fatalf("agent must exist: %v", err)
	}
	if !strings.Contains(skillsJSON, "sk1") {
		t.Fatalf("agent skills must carry sk1, got %q", skillsJSON)
	}

	// --- Skills on the platform + owner explicitly declined → no ask. ---
	d3, _ := newIntakeDaemon(t)
	if _, err := d3.st.DB().ExecContext(ctx,
		`INSERT INTO skill (id,name,description,created_at) VALUES ('sk2','x','x',?)`,
		time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	reply = d3.intakeCreateAgent(ctx, intakeAction{Intent: "create_agent", Agent: agentSub(
		"不要skill的agent", "rt1", "x", "y", nil, true)})
	if !strings.Contains(reply, "已创建 agent") {
		t.Fatalf("explicit decline must create directly, got %q", reply)
	}
}

// TestIntakeCreateSquad: the platform executes create_squad through the
// squad service (leader + optional members). Hallucinated leader/member ids
// surface as failures, not crashes.
func TestIntakeCreateSquad(t *testing.T) {
	ctx := context.Background()

	// Bare squad (leader only) — the minimal viable squad.
	d, st := newIntakeDaemon(t)
	reply := d.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"审查组", "a1", "PR 审查小组", "leader 拆分并委派子目标", nil)})
	if !strings.Contains(reply, "已创建 squad") {
		t.Fatalf("expected creation reply, got %q", reply)
	}
	var squadName string
	if err := st.DB().QueryRowContext(ctx,
		`SELECT name FROM squad WHERE name=?`, "审查组").Scan(&squadName); err != nil {
		t.Fatalf("created squad must exist: %v", err)
	}

	// Missing name → the ask lists 名称 (clear the draft between cases —
	// each missing case saves a squad draft on the shared daemon).
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"", "a1", "x", "y", nil)}); !strings.Contains(r, "还需要以下信息") || !strings.Contains(r, "名称") {
		t.Fatalf("missing name must ask with 名称, got %q", r)
	}
	// Missing leader_id → the ask lists leader agent + the agent roster.
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"无leader", "", "x", "y", nil)}); !strings.Contains(r, "leader agent") || !strings.Contains(r, "worker1") {
		t.Fatalf("missing leader must ask with the agent roster, got %q", r)
	}
	// Missing BOTH name and leader → one ask listing both.
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"", "", "x", "y", nil)}); !strings.Contains(r, "名称") || !strings.Contains(r, "leader agent") {
		t.Fatalf("missing name+leader must ask for both at once, got %q", r)
	}
	// Hallucinated leader_id → service-layer validator message.
	_ = d.intakeSvc.ClearDraft(ctx)
	if r := d.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"坏leader", "nope", "x", "y", nil)}); !strings.Contains(r, "创建 squad 失败") {
		t.Fatalf("hallucinated leader must fail via the validator, got %q", r)
	}

	// Squad with a member — the member row is attached after Create.
	d2, st2 := newIntakeDaemon(t)
	if _, err := st2.DB().ExecContext(ctx,
		`INSERT INTO agent (id,name,runtime_id,max_concurrent,created_at) VALUES ('a2','worker2','rt1',1,?)`,
		time.Now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	reply = d2.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"带成员的组", "a1", "x", "y", []string{"a2"})})
	if !strings.Contains(reply, "已创建 squad") {
		t.Fatalf("squad with member must create, got %q", reply)
	}
	var memberCount int
	if err := st2.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM squad_member WHERE squad_id=(SELECT id FROM squad WHERE name=?)`, "带成员的组").Scan(&memberCount); err != nil {
		t.Fatalf("query members: %v", err)
	}
	if memberCount != 1 {
		t.Fatalf("squad must have 1 member, got %d", memberCount)
	}

	// Hallucinated member → partial success (squad created, member failed).
	d3, _ := newIntakeDaemon(t)
	reply = d3.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"部分成功", "a1", "x", "y", []string{"ghost"})})
	if !strings.Contains(reply, "已创建 squad") || !strings.Contains(reply, "添加失败") {
		t.Fatalf("hallucinated member must be partial success, got %q", reply)
	}

	// The leader id in member_ids is skipped (it is already squad.leader_id).
	d4, st4 := newIntakeDaemon(t)
	reply = d4.intakeCreateSquad(ctx, intakeAction{Intent: "create_squad", Squad: squadSub(
		"leader重复", "a1", "x", "y", []string{"a1"})})
	if !strings.Contains(reply, "已创建 squad") {
		t.Fatalf("leader-as-member must still create, got %q", reply)
	}
	if err := st4.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM squad_member WHERE squad_id=(SELECT id FROM squad WHERE name=?)`, "leader重复").Scan(&memberCount); err != nil {
		t.Fatalf("query members: %v", err)
	}
	if memberCount != 0 {
		t.Fatalf("leader must not be attached as a member, got %d", memberCount)
	}
}