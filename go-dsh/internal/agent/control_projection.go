package agent

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

const (
	EventHarnessConfigured              = "agent/configured"
	EventCancelRequested                = "agent/cancel-requested"
	EventRetry                          = "llm/retry"
	EventRetryStarted                   = "llm/retry-started"
	EventToolStarted                    = "tool/started"
	AttemptPhaseRetry      AttemptPhase = "retry-scheduled"
)

type harnessConfig struct {
	Title        string `json:"title"`
	Model        string `json:"model"`
	SystemPrompt string `json:"system_prompt"`
}
type RetryState struct {
	ID      string        `json:"retry_id"`
	Attempt uint32        `json:"attempt"`
	Delay   time.Duration `json:"delay"`
	Elapsed time.Duration `json:"elapsed"`
}
type ToolCallState struct {
	Call             tool.Call `json:"call"`
	ProviderCallID   string    `json:"provider_call_id"`
	RequiresApproval bool      `json:"requires_approval"`
	ApprovalID       string    `json:"approval_id,omitempty"`
	Requested        bool      `json:"requested"`
	Decision         *bool     `json:"decision,omitempty"`
	Started          bool      `json:"started"`
	Done             bool      `json:"done"`
	CallSeq          uint64    `json:"call_seq"`
}
type ApprovalState struct {
	ID     string `json:"approval_id"`
	CallID string `json:"call_id"`
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`
}
type controlProjection struct {
	Configured      bool
	Config          harnessConfig
	CancelCause     *CancelCause
	LastCancelCause *CancelCause
	Retry           *RetryState
	RetryElapsed    time.Duration
	Request         json.RawMessage
	ToolCalls       map[string]ToolCallState
	ToolOrder       []string
	NeedsToolStep   bool
}

func (p controlProjection) clone() controlProjection {
	c := p
	if p.CancelCause != nil {
		v := *p.CancelCause
		c.CancelCause = &v
	}
	if p.LastCancelCause != nil {
		v := *p.LastCancelCause
		c.LastCancelCause = &v
	}
	if p.Retry != nil {
		v := *p.Retry
		c.Retry = &v
	}
	c.Request = append(json.RawMessage(nil), p.Request...)
	c.ToolOrder = append([]string(nil), p.ToolOrder...)
	c.ToolCalls = make(map[string]ToolCallState, len(p.ToolCalls))
	for id, v := range p.ToolCalls {
		v.Call.Arguments = append(json.RawMessage(nil), v.Call.Arguments...)
		if v.Decision != nil {
			b := *v.Decision
			v.Decision = &b
		}
		c.ToolCalls[id] = v
	}
	return c
}
func (p *AgentProjection) applyControl(e session.Event) error {
	switch e.EventType {
	case EventHarnessConfigured:
		if p.Configured {
			return ErrInvalidDriverTransition
		}
		if err := json.Unmarshal(e.Data, &p.Config); err != nil {
			return err
		}
		if p.Config.Model == "" {
			return ErrInvalidDriverTransition
		}
		p.Configured = true
	case "turn/start":
		p.CancelCause = nil
		p.NeedsToolStep = false
	case "step/start":
		p.Retry = nil
		p.RetryElapsed = 0
		p.Request = nil
		p.NeedsToolStep = false
	case "turn/end":
		p.CancelCause = nil
		p.Retry = nil
		p.NeedsToolStep = false
	case EventCancelRequested:
		var v struct {
			Cause CancelCause `json:"cause"`
			Scope string      `json:"scope"`
		}
		if err := json.Unmarshal(e.Data, &v); err != nil {
			return err
		}
		if v.Cause.Source == "" {
			return ErrInvalidDriverTransition
		}
		if v.Scope != "pending-only" {
			if p.ActiveTurnID == "" || e.TurnID != p.ActiveTurnID || p.CancelCause != nil {
				return ErrInvalidDriverTransition
			}
			p.CancelCause = &v.Cause
		}
		p.LastCancelCause = &v.Cause
	case EventRetry:
		var r RetryState
		if err := json.Unmarshal(e.Data, &r); err != nil {
			return err
		}
		if p.CancelCause != nil || p.ActiveStepID != e.StepID || p.ActiveTurnID != e.TurnID || p.AttemptPhase != AttemptPhaseRequested || r.Attempt != p.ActiveAttempt || r.ID == "" || r.Delay < 0 || r.Delay > 10*time.Second || r.Elapsed != p.RetryElapsed+r.Delay || r.Elapsed > 60*time.Second {
			return ErrInvalidDriverTransition
		}
		p.Retry = &r
		p.RetryElapsed = r.Elapsed
		p.AttemptPhase = AttemptPhaseRetry
	case EventRetryStarted:
		var r RetryState
		if err := json.Unmarshal(e.Data, &r); err != nil {
			return err
		}
		if p.CancelCause != nil || p.Retry == nil || p.Retry.ID != r.ID || p.AttemptPhase != AttemptPhaseRetry || e.StepID != p.ActiveStepID || e.TurnID != p.ActiveTurnID {
			return ErrStaleDriverActivity
		}
		p.AttemptPhase = AttemptPhaseNone
	case EventModelRequested:
		var r ModelRequestedPayload
		if err := json.Unmarshal(e.Data, &r); err != nil {
			return err
		}
		p.Request = append(json.RawMessage(nil), r.RequestSummary...)
	case "tool/call":
		var v ToolCallState
		if err := json.Unmarshal(e.Data, &v); err != nil {
			return err
		}
		if v.Call.ID == "" || v.Call.ID != e.CallID || v.Call.TurnID != p.ActiveTurnID || v.Call.StepID != e.StepID || v.Started || v.Done || v.Requested || v.Decision != nil {
			return ErrInvalidDriverTransition
		}
		if p.ToolCalls == nil {
			p.ToolCalls = map[string]ToolCallState{}
		}
		if _, ok := p.ToolCalls[v.Call.ID]; ok {
			return fmt.Errorf("%w: duplicate tool call", ErrInvalidDriverTransition)
		}
		if v.ProviderCallID == "" {
			// 旧持久化事件尚未分离内部 ID 与供应商 ID。
			v.ProviderCallID = v.Call.ID
		}
		v.CallSeq = e.Seq
		p.ToolCalls[v.Call.ID] = v
		p.ToolOrder = append(p.ToolOrder, v.Call.ID)
	case "approval/requested", "approval/resolved":
		var v ApprovalState
		if err := json.Unmarshal(e.Data, &v); err != nil {
			return err
		}
		call, ok := p.ToolCalls[v.CallID]
		if !ok || !call.RequiresApproval || call.ApprovalID != v.ID || call.Done {
			return ErrApprovalNotPending
		}
		if e.EventType == "approval/requested" {
			if call.Requested {
				return ErrApprovalResolved
			}
			call.Requested = true
		} else {
			if !call.Requested || call.Decision != nil {
				return ErrApprovalResolved
			}
			call.Decision = &v.Allow
		}
		p.ToolCalls[v.CallID] = call
	case EventToolStarted:
		c, ok := p.ToolCalls[e.CallID]
		if !ok || c.Started || c.Done || p.CancelCause != nil || c.Call.TurnID != p.ActiveTurnID {
			return ErrStaleDriverActivity
		}
		if c.RequiresApproval && (!c.Requested || c.Decision == nil || !*c.Decision) {
			return ErrApprovalNotPending
		}
		c.Started = true
		p.ToolCalls[e.CallID] = c
	case "tool/result":
		c, ok := p.ToolCalls[e.CallID]
		if !ok {
			if !p.Configured {
				return nil
			}
			return ErrInvalidDriverTransition
		}
		if c.Done {
			return ErrInvalidDriverTransition
		}
		c.Done = true
		p.ToolCalls[e.CallID] = c
		p.NeedsToolStep = true
	}
	return nil
}
func statusFrom(s session.Snapshot) Status {
	p, _ := ProjectionFrom(s)
	v := Status{SessionID: s.SessionID, State: StateIdle, Phase: DriverIdle, ActiveTurnID: p.ActiveTurnID, ActiveStepID: p.ActiveStepID, StepIndex: p.ActiveStepIndex, ActiveAttempt: p.ActiveAttempt, Queued: len(p.NextStep) + len(p.NextTurn), Cancellation: p.CancelCause}
	if p.ActiveTurnID != "" {
		v.State = StateRunning
		v.Phase = DriverRunning
	}
	for _, c := range p.ToolCalls {
		if c.Call.TurnID == p.ActiveTurnID && !c.Done && c.RequiresApproval && c.Decision == nil {
			v.State = StateWaitingApproval
		}
	}
	if p.CancelCause != nil {
		v.State = StateCancelling
	}
	return v
}
func toolResultEvent(c ToolCallState, content string, structured json.RawMessage, code string) session.NewEvent {
	if len(structured) == 0 {
		structured = nil
	}
	envelope, err := json.Marshal(struct {
		Content    string          `json:"content"`
		Structured json.RawMessage `json:"structured"`
		Error      string          `json:"error"`
	}{Content: content, Structured: structured, Error: code})
	if err != nil {
		panic(err)
	}
	m := llm.Message{Role: llm.RoleTool, Source: llm.MessageSourceSurface, Content: []llm.ContentBlock{{Type: llm.ContentBlockToolResult, ToolCallID: c.ProviderCallID, ToolName: c.Call.Name, Text: content, ToolResult: envelope, Complete: true}}}
	e := surfaceFact("tool/result", c.Call.TurnID, c.Call.StepID, map[string]any{"message": m, "error": code}, []uint64{c.CallSeq})
	e.CallID = c.Call.ID
	return e
}
