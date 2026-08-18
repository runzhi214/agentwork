package notify

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/eushing/agentwork/internal/card"
	"github.com/eushing/agentwork/internal/card/feishu"
)

// Card construction (M3): Feishu interactive cards (schema 2.0). Cards are the
// M3 upgrade over text pushes — the approval card carries the evidence and
// approve/reject buttons whose callbacks arrive over the long connection
// (card.action.trigger). The card content is emitted as a JSON string; the
// IM API is a content passthrough, so no SDK model dependency.

// buildReviewCard is the approval card (M3-1): header + gate reason +
// evidence summary + optional reject-reason input + approve/reject buttons.
// The buttons carry {action, goal_id, run_id} — the run id is the evidence
// run the card displays, recorded on the gate_decision (audit chain).
//
// pendingReviewers drives the Option B hint: while reviewers are still
// working, the card names them and tells the human they MAY wait; the
// review_ready patch rebuilds this card with the pending set empty, so the
// hint is replaced by the actual 审查意见.
//
// Schema 2.0 is built directly (not via feishu.BuildCard) because the card
// carries custom elements: a markdown body and a column_set button row —
// more than the neutral Card model's single body + optional approval section.
func buildReviewCard(g ReviewGoal, pendingReviewers []string) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "**%s**", g.Title)
	if g.Reason != "" {
		b.WriteString("\n**卡点**：" + g.Reason)
	}
	if ev := evidenceSummary(g.Evidence); ev != "" {
		b.WriteString("\n\n" + ev)
	}
	if len(pendingReviewers) > 0 {
		b.WriteString("\n\n🔎 审查中：" + strings.Join(pendingReviewers, "、") + "——可等待他们的意见后再决定")
	}
	// The squad review opinions — the approval decision reads the review,
	// not just the worker's claim. A reviewer that dumped an evidence-style
	// JSON blob as its final message gets its agent field extracted and
	// rendered as markdown (the raw JSON in a > quote was unreadable).
	if len(g.Comments) > 0 {
		b.WriteString("\n\n**审查意见**")
		for _, c := range g.Comments {
			b.WriteString("\n" + renderReviewComment(c))
		}
	}
	reviewCard := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"update_multi": true},
		"header": map[string]any{
			"template": "orange",
			"title":    map[string]any{"tag": "plain_text", "content": "🔔 待审批"},
		},
		"body": map[string]any{
			"direction": "vertical",
			"elements": []map[string]any{
			{"tag": "markdown", "content": b.String()},
			{"tag": "hr"},
			reviewButtonRow(g.GoalID, g.RunID),
			},
		},
	}
	raw, err := json.Marshal(reviewCard)
	return string(raw), err
}

// reviewButtonRow builds the column_set element containing the two approval
// buttons (批准 / 驳回). Button values carry {action, goal_id, run_id} so
// onCardAction can correlate the click to the goal + evidence run.
func reviewButtonRow(goalID, runID string) map[string]any {
	btn := func(label, action, btnType string) map[string]any {
		return map[string]any{
			"tag":   "button",
			"text":  map[string]any{"tag": "plain_text", "content": label},
			"type":  btnType,
			"width": "default",
			"name":  action,
			"value": map[string]any{"action": action, "goal_id": goalID, "run_id": runID},
		}
	}
	return map[string]any{
		"tag":                "column_set",
		"flex_mode":          "flow",
		"horizontal_spacing": "8px",
		"columns": []map[string]any{
			{"tag": "column", "width": "auto", "elements": []map[string]any{btn("✅ 批准", "approve", "primary_filled")}},
			{"tag": "column", "width": "auto", "elements": []map[string]any{btn("❌ 驳回", "reject", "danger_filled")}},
		},
	}
}

// buildMilestoneCard is the generic milestone card (M3-2): a colored header
// (done green / failed red / merged blue) + title + body markdown.
func buildMilestoneCard(emoji, template, title, body string) (string, error) {
	return feishu.BuildCard(&card.Card{
		Header:  card.CardHeader{Title: emoji + " " + title},
		Content: body,
		Color:   card.CardColor(template),
	})
}

// SendMarkdown sends a markdown-body card (no header) so **bold** and other
// CommonMark syntax render in Feishu. Falls back to plain-text Send when
// card building fails. Used by intake replies that carry markdown formatting
// (e.g. goal_query list output).
func (n *Notifier) SendMarkdown(text string) error {
	cardJSON, err := feishu.BuildCard(&card.Card{Content: text})
	if err != nil {
		return n.Send(text)
	}
	_, err = n.SendCard(cardJSON)
	return err
}

// buildAskCard is the agent-question card (决策 7-3): an agent asked the human
// a question via `goal comment --ask`. The card carries a structured body
// (the goal title + the question) and a FORM the human fills inline — the
// reply submits over the long connection as a card.action.trigger with
// form_value, and onCardAction turns it into a human comment threading under
// the ask (parent_id routing wakes the owner). No web hop: the whole round-
// trip stays in Feishu.
//
// The title names the questioning agent (❓ {agentName}) so a glance tells the
// human WHO is asking before they open the card. comment_id is the ask
// comment — the reply's parent_id (the owner-wake routing key).
func buildAskCard(agentName, goalTitle, question, goalID, commentID string) (string, error) {
	if agentName == "" {
		agentName = "Agent"
	}
	body := fmt.Sprintf("**任务：**\n%s\n\n**%s 询问：**\n%s", goalTitle, agentName, question)
	askCard := map[string]any{
		"schema": "2.0",
		"config": map[string]any{"update_multi": true},
		"header": map[string]any{
			"template": "blue",
			"title":    map[string]any{"tag": "plain_text", "content": "❓ " + agentName},
		},
		"body": map[string]any{
			"direction": "vertical",
			"elements": []map[string]any{
				{"tag": "markdown", "content": body},
				{"tag": "hr"},
				// The reply form: an input the human fills + a submit button. On
				// submit the card.action.trigger callback carries form_value
				// {reply_text: "..."} alongside the button's value (action/
				// goal_id/comment_id) — onCardAction reads both, posts the reply
				// as a human comment threading under the ask. The container tag
				// is "form" (NOT "form_container" — Feishu's im/v1 interactive
				// API rejects the latter as "not support tag").
				{
					"tag":  "form",
					"name": "reply_form",
					"elements": []map[string]any{
						{
							"tag":         "input",
							"name":        "reply_text",
							"placeholder": map[string]any{"tag": "plain_text", "content": "输入回复…"},
							"width":       "fill",
						},
						{
							"tag":    "button",
							"text":   map[string]any{"tag": "plain_text", "content": "回复"},
							"type":   "primary",
							"name":   "submit_reply",
							"action_type": "form_submit",
							"value":  map[string]any{"action": "reply_ask", "goal_id": goalID, "comment_id": commentID},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(askCard)
	return string(raw), err
}

// buildProcessedCard replaces the approval card after a button decision: the
// buttons are gone, the outcome is stamped (M3-1, the Message.Update path).
func buildProcessedCard(goalID, decision string, scratch bool) (string, error) {
	ok := decision == "approve"
	header, body := "❌ 已驳回", "该卡点已驳回，goal 将带决策意见重跑。"
	if ok {
		header = "✅ 已批准"
		if scratch {
			body = "任务完成——按汇报验收（产物在项目目录，无合入步骤）。"
		} else {
			body = "平台正在自动合入（merge + 复验 + push）。"
		}
	}
	template := card.CardColorRed
	if ok {
		template = card.CardColorGreen
	}
	return feishu.BuildCard(&card.Card{
		Header:  card.CardHeader{Title: header},
		Content: body + "  \n`goal " + short(goalID) + "`",
		Color:   template,
	})
}

// evidenceSummary renders the run.evidence JSON bundle into the approval
// card's markdown body: diff stat + verify outcome + guards + agent summary.
// Unknown shapes degrade to an empty string (the card then shows only the gate
// reason — never a broken card).
func evidenceSummary(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	var ev struct {
		DiffStat string   `json:"diff_stat"`
		Changed  []string `json:"changed"`
		Verify   string   `json:"verify"`
		Guards   string   `json:"guards"`
		Agent    string   `json:"agent"`
	}
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		return ""
	}
	var parts []string
	if s := strings.TrimSpace(ev.DiffStat); s != "" {
		// The diff stat is multi-line; keep the total line and drop the file
		// rows (card space).
		lines := strings.Split(s, "\n")
		parts = append(parts, "改动："+strings.TrimSpace(lines[len(lines)-1]))
	} else if n := len(ev.Changed); n > 0 {
		parts = append(parts, fmt.Sprintf("改动：%d 个文件", n))
	}
	if v := strings.TrimSpace(ev.Verify); v != "" {
		// Keep the command lines and the last output line (the outcome).
		lines := strings.Split(v, "\n")
		cmds, tail := []string{}, ""
		for i, l := range lines {
			l = strings.TrimSpace(l)
			if strings.HasPrefix(l, "$ ") {
				cmds = append(cmds, strings.TrimPrefix(l, "$ "))
			}
			if i == len(lines)-1 && l != "" {
				tail = l
			}
		}
		if len(cmds) > 0 {
			parts = append(parts, "verify: "+strings.Join(cmds, " | "))
		}
		if tail != "" {
			parts = append(parts, "结果："+tail)
		}
	}
	if g := strings.TrimSpace(ev.Guards); g != "" {
		parts = append(parts, "约束："+truncate(g, 200))
	}
	// The agent's report is markdown (lark_md renders it). It gets its own
	// paragraph (separated by \n\n from the objective evidence above) so the
	// card visually distinguishes "machine-verified facts" from "agent's
	// self-report". truncateParagraphs cuts on paragraph boundaries (\n\n)
	// so bullet lists and code blocks are not split mid-way (a byte-level
	// cut left broken markdown that rendered as raw text).
	var agentSection string
	if a := strings.TrimSpace(ev.Agent); a != "" {
		agentSection = "\n\n—— agent 自述 ——\n" + truncateParagraphs(a, 1200)
	}
	return strings.Join(parts, "  \n") + agentSection
}

// truncateParagraphs truncates s to at most maxBytes, cutting on paragraph
// boundaries (\n\n) so markdown structures (bullet lists, code blocks) stay
// intact. A single paragraph exceeding maxBytes falls back to a byte-level
// cut. The result gets a "…" suffix when truncation occurred.
func truncateParagraphs(s string, maxBytes int) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxBytes {
		return s
	}
	paras := strings.Split(s, "\n\n")
	var b strings.Builder
	for _, p := range paras {
		if b.Len()+len(p) > maxBytes {
			break
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(p)
	}
	if b.Len() == 0 {
		// Single paragraph larger than maxBytes — rune-level cut avoids
		// splitting a multi-byte UTF-8 character (Chinese is 3 bytes/char).
		runes := []rune(s)
		if len(runes) > maxBytes {
			runes = runes[:maxBytes]
		}
		b.WriteString(string(runes))
	}
	return b.String() + "\n\n…"
}

// renderReviewComment renders one reviewer comment for the approval card's
// 审查意见 section. A reviewer agent may dump an evidence-style JSON blob
// ({"agent":"…","diff_stat":"…",…}) as its final message — the raw JSON in
// a > quote was unreadable. When the comment parses as evidence JSON with a
// non-empty agent field, the agent text is extracted and rendered as markdown
// (paragraph-truncated). Plain-text comments keep the > quote.
func renderReviewComment(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var ev struct {
		Agent string `json:"agent"`
	}
	if json.Unmarshal([]byte(raw), &ev) == nil {
		if a := strings.TrimSpace(ev.Agent); a != "" {
			return truncateParagraphs(a, 1200)
		}
	}
	return "> " + raw
}
