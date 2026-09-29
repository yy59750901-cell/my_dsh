package agent

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

type cancelCommand struct {
	Cause        CancelCause
	ExpectedTurn string
	BindTurn     bool
}

func (c cancelCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	if (c.BindTurn || c.ExpectedTurn != "") && c.ExpectedTurn != p.ActiveTurnID {
		return nil, ErrStaleDriverActivity
	}
	if p.CancelCause != nil {
		return nil, nil
	}
	if p.ActiveTurnID == "" && len(p.NextTurn)+len(p.NextStep) == 0 {
		return nil, nil
	}
	scope := "turn"
	if p.ActiveTurnID == "" {
		scope = "pending-only"
	}
	events := []session.NewEvent{fact(EventCancelRequested, p.ActiveTurnID, p.ActiveStepID, map[string]any{"cause": c.Cause, "scope": scope})}
	for _, target := range []InboxTarget{InboxNextStep, InboxNextTurn} {
		if n := len(p.Queue(target)); n > 0 {
			events = append(events, fact(EventInboxSpliced, p.ActiveTurnID, "", InboxSplicedPayload{Target: target, DeleteCount: n, Reason: InboxReasonCancel}))
		}
	}
	return events, nil
}

type resolveApprovalCommand struct {
	ID    string
	Allow bool
}

func (c resolveApprovalCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	for _, v := range p.ToolCalls {
		if v.ApprovalID != c.ID {
			continue
		}
		if v.Decision != nil {
			if *v.Decision == c.Allow {
				return nil, nil
			}
			return nil, ErrApprovalResolved
		}
		if v.Done || !v.Requested || p.CancelCause != nil || v.Call.TurnID != p.ActiveTurnID {
			return nil, ErrApprovalNotPending
		}
		return []session.NewEvent{fact("approval/resolved", v.Call.TurnID, v.Call.StepID, ApprovalState{ID: c.ID, CallID: v.Call.ID, Allow: c.Allow})}, nil
	}
	return nil, ErrApprovalNotFound
}

type closeActivityCommand struct {
	Turn      string
	Reason    string
	Synthetic bool
	Quiescent bool
}

func (c closeActivityCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	if p.ActiveTurnID == "" {
		return nil, nil
	}
	if p.ActiveTurnID != c.Turn {
		return nil, ErrStaleDriverActivity
	}
	if c.Quiescent && (p.ActiveStepID != "" || len(p.NextStep) > 0 || p.NeedsToolStep) {
		return nil, ErrNoClaimableInput
	}
	reason := c.Reason
	if p.CancelCause != nil {
		reason = "aborted"
	}
	events := []session.NewEvent{}
	for _, id := range p.ToolOrder {
		v := p.ToolCalls[id]
		if v.Call.TurnID != c.Turn || v.Done {
			continue
		}
		if v.RequiresApproval && v.Decision == nil {
			events = append(events, fact("approval/resolved", v.Call.TurnID, v.Call.StepID, ApprovalState{ID: v.ApprovalID, CallID: id, Reason: reason}))
		}
		events = append(events, toolResultEvent(v, "工具未完成，未重放执行", nil, reason))
	}
	if p.ActiveStepID != "" {
		if p.CancelCause != nil && !c.Synthetic {
			prefix, err := cancelledPrefixEvent(p)
			if err != nil {
				return nil, err
			}
			if prefix != nil {
				events = append(events, *prefix)
			}
		}
		events = append(events, fact("step/end", c.Turn, p.ActiveStepID, StepEndPayload{Reason: StepEndReason(reason), Attempt: p.ActiveAttempt, Synthetic: c.Synthetic, UsageMissing: true}))
	}
	end := TurnEndPayload{Reason: reason, Synthetic: c.Synthetic}
	if p.PendingClaimID != "" {
		claim, _ := p.Claim(p.PendingClaimID)
		end.ClaimID = claim.ClaimID
		end.ConsumedInputIDs = claim.OrderedInputIDs
	}
	events = append(events, fact("turn/end", c.Turn, "", end))
	return events, nil
}

type harnessRepairInitializer struct{}

func (harnessRepairInitializer) Initialize(ctx context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, ok := ProjectionFrom(s)
	if !ok || !p.Configured || p.ActiveTurnID == "" {
		return nil, nil
	}
	return (closeActivityCommand{Turn: p.ActiveTurnID, Reason: "interrupted", Synthetic: true}).Decide(ctx, s)
}

type startStepWithInputCommand struct{ StartClaimedStepCommand }

func (c startStepWithInputCommand) Decide(ctx context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	events, err := c.StartClaimedStepCommand.Decide(ctx, s)
	if err != nil {
		return nil, err
	}
	p, _ := ProjectionFrom(s)
	claim, _ := p.Claim(c.ExpectedClaimID)
	for _, item := range claim.Items {
		m := llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceStep}
		for i, b := range item.Content {
			if b.Type != "text" {
				return nil, ErrInvalidInboxRequest
			}
			m.Content = append(m.Content, llm.ContentBlock{Type: llm.ContentBlockText, Text: b.Text, Index: uint32(i), Complete: true})
		}
		events = append(events, surfaceFact("user/message", c.ExpectedTurnID, c.StepID, m, []uint64{item.AcceptedSeq}))
	}
	return events, nil
}

type continuationClaimCommand struct {
	Turn  string
	Index uint32
	ID    string
}

func (c continuationClaimCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	if p.CancelCause != nil || p.ActiveTurnID != c.Turn || p.ActiveStepID != "" || p.PendingClaimID != "" || !p.NeedsToolStep || len(p.NextStep) > 0 || nextClaimStepIndex(p, c.Turn) != c.Index {
		return nil, ErrStaleDriverActivity
	}
	return []session.NewEvent{fact(EventInputClaimed, c.Turn, "", InputClaimedPayload{ClaimID: c.ID, TurnID: c.Turn, ProposedStepIndex: c.Index, Continuation: true})}, nil
}

func retryDelay(f *llm.LlmFailure, attempt uint32, elapsed time.Duration, maxAttempts int) (time.Duration, bool) {
	if f == nil || int(attempt) >= maxAttempts {
		return 0, false
	}
	retry := false
	switch f.Code {
	case llm.FailureRateLimit, llm.FailureTransport, llm.FailureTimeout, llm.FailureEmptyResponse:
		retry = true
	case llm.FailureUnknown:
		retry = f.Status != nil && *f.Status >= 500 && *f.Status <= 599
	}
	if !retry {
		return 0, false
	}
	delay := 500 * time.Millisecond
	for n := uint32(1); n < attempt && delay < 10*time.Second; n++ {
		delay = nextBackoff(delay, 10*time.Second)
	}
	if f.ProviderRetryAfterMs != nil && *f.ProviderRetryAfterMs >= 0 && *f.ProviderRetryAfterMs <= 10000 {
		delay = time.Duration(*f.ProviderRetryAfterMs) * time.Millisecond
	} else {
		delay = time.Duration(float64(delay) * (0.9 + rand.Float64()*0.2))
		if delay > 10*time.Second {
			delay = 10 * time.Second
		}
	}
	return delay, elapsed+delay <= 60*time.Second
}

type retryAttemptCommand struct {
	Turn, Step string
	Attempt    uint32
	Delay      time.Duration
	ID         string
}

func (c retryAttemptCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	if p.CancelCause != nil || p.ActiveTurnID != c.Turn || p.ActiveStepID != c.Step || p.ActiveAttempt != c.Attempt || p.AttemptPhase != AttemptPhaseRequested {
		return nil, ErrStaleDriverActivity
	}
	terminal, err := rebuildTerminalAssembly(p, c.Turn, c.Step, c.Attempt)
	if err != nil {
		return nil, err
	}
	if terminal.Finish.Kind != llm.FinishError {
		return nil, ErrInvalidDriverTransition
	}
	events := []session.NewEvent{}
	if terminal.Usage != nil {
		e := fact(EventModelUsage, c.Turn, c.Step, ModelUsagePayload{Attempt: c.Attempt, Usage: *terminal.Usage})
		e.ReplayPolicy = session.ReplayIgnorable
		events = append(events, e)
	}
	events = append(events, fact("model/error", c.Turn, c.Step, ModelErrorPayload{Attempt: c.Attempt, Failure: *terminal.Finish.Failure}), fact(EventRetry, c.Turn, c.Step, RetryState{ID: c.ID, Attempt: c.Attempt, Delay: c.Delay, Elapsed: p.RetryElapsed + c.Delay}))
	return events, nil
}

type startRetryCommand struct {
	Turn, Step, RetryID string
	Request             llm.ModelRequest
}

func (c startRetryCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, _ := ProjectionFrom(s)
	if p.CancelCause != nil || p.ActiveTurnID != c.Turn || p.ActiveStepID != c.Step || p.Retry == nil || p.Retry.ID != c.RetryID || p.AttemptPhase != AttemptPhaseRetry || p.ActiveAttempt+1 != c.Request.Attempt {
		return nil, ErrStaleDriverActivity
	}
	data, err := summarizeModelRequest(c.Request)
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{fact(EventRetryStarted, c.Turn, c.Step, *p.Retry), fact(EventModelRequested, c.Turn, c.Step, ModelRequestedPayload{Attempt: c.Request.Attempt, Model: c.Request.Model, RequestSummary: data})}, nil
}
