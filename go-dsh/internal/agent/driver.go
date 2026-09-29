package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

type sessionDriver struct {
	h        *Harness
	id       string
	mu       sync.Mutex
	actor    *session.Actor
	turn     string
	cancel   context.CancelFunc
	stopping bool
}

func (d *sessionDriver) Status(ctx context.Context) (Status, error) { return d.h.GetStatus(ctx, d.id) }
func (d *sessionDriver) Cancel(cause CancelCause) bool {
	d.mu.Lock()
	a, turn, cancel := d.actor, d.turn, d.cancel
	if cause.Source == CancelSourceServerShutdown || cause.Source == CancelSourceSessionDispose {
		d.stopping = true
	}
	d.mu.Unlock()
	if a == nil || turn == "" {
		return false
	}
	if cause.Source != CancelSourceUser {
		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := submitCommitted(ctx, a, cancelCommand{Cause: cause, ExpectedTurn: turn})
		done()
		if err != nil {
			if cancel != nil {
				cancel()
			}
			return false
		}
	}
	p, _ := ProjectionFrom(a.Snapshot())
	if p.ActiveTurnID != turn || p.CancelCause == nil {
		return false
	}
	if cancel != nil {
		cancel()
	}
	return true
}
func (d *sessionDriver) interruptActivity(actor *session.Actor, turn string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if turn == "" || d.actor != actor || d.turn != turn || d.cancel == nil {
		return false
	}
	d.cancel()
	return true
}

func (d *sessionDriver) Run(ctx context.Context, latch *WakeLatch) (runErr error) {
	lease, err := d.h.sessions.GetOrLoad(ctx, d.id)
	if err != nil {
		return err
	}
	defer lease.Release()
	a := lease.Actor()
	d.mu.Lock()
	d.actor = a
	d.mu.Unlock()
	defer func() {
		if v := recover(); v != nil {
			runErr = fmt.Errorf("agent driver panic: %v", v)
		}
		p, _ := ProjectionFrom(a.Snapshot())
		if p.ActiveTurnID != "" {
			_, err := d.commit(a, closeActivityCommand{Turn: p.ActiveTurnID, Reason: "error"})
			runErr = errors.Join(runErr, err)
		}
		d.mu.Lock()
		if d.cancel != nil {
			d.cancel()
		}
		d.actor = nil
		d.turn = ""
		d.cancel = nil
		d.mu.Unlock()
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		observed := latch.Snapshot()
		p, _ := ProjectionFrom(a.Snapshot())
		if !projectionShouldWake(p) {
			latch.Acknowledge(observed)
			if !latch.Requested() {
				return nil
			}
			continue
		}
		if !p.Configured {
			if _, err = d.commit(a, session.CommandFunc(func(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
				p, _ := ProjectionFrom(s)
				if p.Configured {
					return nil, nil
				}
				return []session.NewEvent{fact(EventHarnessConfigured, "", "", harnessConfig{Model: d.h.options.Model, SystemPrompt: d.h.options.SystemPrompt})}, nil
			})); err != nil {
				return err
			}
		}
		claim, err := d.h.inbox.StartTurnAndClaim(ctx, d.id)
		if errors.Is(err, ErrNoClaimableInput) {
			continue
		}
		if err != nil {
			return err
		}
		turnCtx, cancel := context.WithCancel(ctx)
		d.mu.Lock()
		d.turn = claim.TurnID
		d.cancel = cancel
		stopping := d.stopping
		d.mu.Unlock()
		if stopping {
			d.Cancel(LifecycleDisposedCause)
		}
		p, _ = ProjectionFrom(a.Snapshot())
		if p.CancelCause != nil {
			cancel()
		}
		err = d.runTurn(turnCtx, a, latch, claim)
		cancel()
		if err != nil {
			p, _ = ProjectionFrom(a.Snapshot())
			if p.ActiveTurnID == claim.TurnID {
				_, closeErr := d.commit(a, closeActivityCommand{Turn: claim.TurnID, Reason: "error"})
				if closeErr != nil {
					return errors.Join(err, closeErr)
				}
			}
			// 取消与旧活动拒绝是正常收敛；存储故障交还 Registry。
			if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrStaleDriverActivity) {
				return err
			}
		}
		d.mu.Lock()
		d.turn = ""
		d.cancel = nil
		d.mu.Unlock()
	}
}
func (d *sessionDriver) commit(a *session.Actor, c session.Command) ([]session.Event, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return submitCommitted(ctx, a, c)
}

func (d *sessionDriver) runTurn(ctx context.Context, a *session.Actor, latch *WakeLatch, claim InputClaim) error {
	// 每个 Turn 固定定义和 Resolve 返回的 generation handle。
	handles := map[string]tool.Tool{}
	definitions := []llm.ToolDefinition{}
	for _, def := range d.h.tools.Definitions() {
		handle, err := d.h.tools.Resolve(def.Name)
		if err != nil {
			return err
		}
		pinned := handle.Definition()
		handles[pinned.Name] = handle
		definitions = append(definitions, llm.ToolDefinition{Name: pinned.Name, Description: pinned.Description, InputSchema: pinned.InputSchema})
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		p, _ := ProjectionFrom(a.Snapshot())
		if p.CancelCause != nil {
			return context.Canceled
		}
		if int(claim.ProposedStepIndex) > d.h.options.MaxSteps {
			_, err := d.commit(a, closeActivityCommand{Turn: claim.TurnID, Reason: "blocked"})
			return err
		}
		stepID, err := (cryptoInboxIDGenerator{}).NewID("step")
		if err != nil {
			return err
		}
		eventID, err := (cryptoInboxIDGenerator{}).NewID("event")
		if err != nil {
			return err
		}
		start := StartClaimedStepCommand{ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: claim.ProposedStepIndex, StepID: stepID, EventID: eventID, OccurredAt: time.Now().UTC()}
		if _, err = d.commit(a, startStepWithInputCommand{start}); err != nil {
			return err
		}
		requestSnapshot := a.Snapshot()
		request, err := d.buildRequest(requestSnapshot, definitions)
		if err != nil {
			var exceeded *ContextWindowExceededError
			if errors.As(err, &exceeded) {
				_, err = d.commit(a, contextOverflowCommand{Turn: claim.TurnID, Step: stepID, StepIndex: claim.ProposedStepIndex, SourceHeadSeq: requestSnapshot.HeadSeq, Budget: exceeded.Budget})
			}
			return err
		}
		terminal, err := d.runAttempts(ctx, a, request, claim.ProposedStepIndex, handles)
		if err != nil {
			return err
		}
		if terminal.Finish.Kind == llm.FinishError || terminal.Finish.Kind == llm.FinishAborted {
			reason, _ := stepEndReasonForFinish(terminal.Finish.Kind)
			_, err = d.commit(a, closeActivityCommand{Turn: claim.TurnID, Reason: string(reason)})
			return err
		}
		if err = d.runTools(ctx, a, latch, claim.TurnID, handles); err != nil {
			return err
		}
		for {
			p, _ = ProjectionFrom(a.Snapshot())
			if p.CancelCause != nil {
				return context.Canceled
			}
			if len(p.NextStep) > 0 {
				claim, err = d.h.inbox.ClaimForProposedStep(ctx, d.id, claim.TurnID, nextClaimStepIndex(p, claim.TurnID))
				if err != nil {
					return err
				}
				break
			}
			if p.NeedsToolStep {
				id, err := (cryptoInboxIDGenerator{}).NewID("claim")
				if err != nil {
					return err
				}
				_, err = d.commit(a, continuationClaimCommand{Turn: claim.TurnID, Index: nextClaimStepIndex(p, claim.TurnID), ID: id})
				if errors.Is(err, ErrStaleDriverActivity) {
					continue
				}
				if err != nil {
					return err
				}
				updated, _ := ProjectionFrom(a.Snapshot())
				claim, _ = updated.Claim(id)
				break
			}
			_, err = d.commit(a, closeActivityCommand{Turn: claim.TurnID, Reason: "completed", Quiescent: true})
			if errors.Is(err, ErrNoClaimableInput) {
				continue
			}
			return err
		}
	}
}

func (d *sessionDriver) buildRequest(s session.Snapshot, defs []llm.ToolDefinition) (llm.ModelRequest, error) {
	p, _ := ProjectionFrom(s)
	id, err := (cryptoInboxIDGenerator{}).NewID("request")
	if err != nil {
		return llm.ModelRequest{}, err
	}
	v, err := deriveContext(s, defs, d.h.options)
	if err != nil {
		return llm.ModelRequest{}, err
	}
	reserved := v.Budget.ReservedOutputTokens
	r := llm.ModelRequest{RequestID: id, SessionID: s.SessionID, TurnID: p.ActiveTurnID, StepID: p.ActiveStepID, Attempt: 1, Model: p.Config.Model, Tools: v.Tools, Messages: v.Messages, MaxTokens: &reserved}
	if v.Budget.Exceeded {
		return r, &ContextWindowExceededError{Budget: v.Budget}
	}
	return r, nil
}

func (d *sessionDriver) runAttempts(ctx context.Context, a *session.Actor, r llm.ModelRequest, index uint32, handles map[string]tool.Tool) (llm.TerminalAssembly, error) {
	runner, err := NewActiveAttemptRunner(a, llm.NewRuntime(d.h.provider, nil), AttemptRunnerOptions{})
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	data, err := summarizeModelRequest(r)
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	id, err := (cryptoInboxIDGenerator{}).NewID("event")
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	_, err = d.commit(a, StartInitialAttemptCommand{ExpectedTurnID: r.TurnID, ExpectedStepID: r.StepID, ExpectedStepIndex: index, Model: r.Model, RequestSummary: data, EventID: id, OccurredAt: time.Now().UTC()})
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	for {
		if err = ctx.Err(); err != nil {
			return llm.TerminalAssembly{}, err
		}
		terminal, err := runner.Run(ctx, AttemptActivity{TurnID: r.TurnID, StepID: r.StepID, StepIndex: index, Attempt: r.Attempt, Phase: AttemptPhaseRequested}, r)
		if err != nil {
			return terminal, err
		}
		p, _ := ProjectionFrom(a.Snapshot())
		if p.CancelCause != nil {
			return terminal, context.Canceled
		}
		terminal, err = rebuildTerminalAssembly(p, r.TurnID, r.StepID, r.Attempt)
		if err != nil {
			return terminal, err
		}
		if delay, retry := retryDelay(terminal.Finish.Failure, r.Attempt, p.RetryElapsed, d.h.options.MaxAttempts); terminal.Finish.Kind == llm.FinishError && retry {
			retryID, err := (cryptoInboxIDGenerator{}).NewID("retry")
			if err != nil {
				return terminal, err
			}
			if _, err = d.commit(a, retryAttemptCommand{Turn: r.TurnID, Step: r.StepID, Attempt: r.Attempt, Delay: delay, ID: retryID}); err != nil {
				return terminal, err
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return terminal, ctx.Err()
			case <-timer.C:
			}
			r.Attempt++
			r.RequestID, err = (cryptoInboxIDGenerator{}).NewID("request")
			if err != nil {
				return terminal, err
			}
			if _, err = d.commit(a, startRetryCommand{Turn: r.TurnID, Step: r.StepID, RetryID: retryID, Request: r}); err != nil {
				return terminal, err
			}
			continue
		}
		ids, err := runner.newTerminalEventIDs(terminal)
		if err != nil {
			return terminal, err
		}
		base := CommitAttemptTerminalCommand{ExpectedTurnID: r.TurnID, ExpectedStepID: r.StepID, ExpectedStepIndex: index, ExpectedAttempt: r.Attempt, ExpectedPhase: AttemptPhaseRequested, ExpectedChunkCount: p.ActiveChunkCount, EventIDs: ids, OccurredAt: time.Now().UTC()}
		_, err = d.commit(a, terminalWithToolsCommand{CommitAttemptTerminalCommand: base, Handles: handles})
		return terminal, err
	}
}

type terminalWithToolsCommand struct {
	CommitAttemptTerminalCommand
	Handles map[string]tool.Tool
}

func (c terminalWithToolsCommand) Decide(ctx context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	events, err := c.CommitAttemptTerminalCommand.Decide(ctx, s)
	if err != nil {
		return nil, err
	}
	p, _ := ProjectionFrom(s)
	terminal, err := rebuildTerminalAssembly(p, c.ExpectedTurnID, c.ExpectedStepID, c.ExpectedAttempt)
	if err != nil {
		return nil, err
	}
	if terminal.Message == nil {
		return events, nil
	}
	calls := []session.NewEvent{}
	for _, b := range terminal.Message.Content {
		if b.Type != llm.ContentBlockToolCall {
			continue
		}
		// 编码完整命名空间，避免拼接歧义，并保证审批 ID 可用于单段 URL。
		namespace, _ := json.Marshal([]string{c.ExpectedTurnID, c.ExpectedStepID, b.ToolCallID})
		id := "call-" + base64.RawURLEncoding.EncodeToString(namespace)
		call := ToolCallState{ProviderCallID: b.ToolCallID, Call: tool.Call{ID: id, SessionID: s.SessionID, TurnID: c.ExpectedTurnID, StepID: c.ExpectedStepID, Name: b.ToolName, Arguments: json.RawMessage(b.ArgumentsJSONRaw), IdempotencyKey: id, Timeout: 30 * time.Second}}
		if handle := c.Handles[b.ToolName]; handle != nil {
			def := handle.Definition()
			call.Call.DefinitionVersion = def.Version
			call.Call.Generation = def.Generation
			call.RequiresApproval = def.RequiresApproval
		}
		if call.RequiresApproval {
			call.ApprovalID = "approval-" + call.Call.ID
		}
		e := fact("tool/call", call.Call.TurnID, call.Call.StepID, call)
		e.CallID = call.Call.ID
		calls = append(calls, e)
		if call.RequiresApproval {
			calls = append(calls, fact("approval/requested", call.Call.TurnID, call.Call.StepID, ApprovalState{ID: call.ApprovalID, CallID: call.Call.ID}))
		}
	}
	// 模型消息、调用意图、审批请求与 Step 终态同批提交。
	end := events[len(events)-1]
	events = append(events[:len(events)-1], calls...)
	return append(events, end), nil
}

func (d *sessionDriver) runTools(ctx context.Context, a *session.Actor, latch *WakeLatch, turn string, handles map[string]tool.Tool) error {
	p, _ := ProjectionFrom(a.Snapshot())
	for _, id := range p.ToolOrder {
		call := p.ToolCalls[id]
		if call.Call.TurnID != turn || call.Done {
			continue
		}
		for call.RequiresApproval && call.Decision == nil {
			current, _ := ProjectionFrom(a.Snapshot())
			call = current.ToolCalls[id]
			if current.CancelCause != nil {
				return context.Canceled
			}
			if call.Decision != nil {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-latch.Signal():
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		content, code := "", ""
		var structured json.RawMessage
		if call.RequiresApproval && !*call.Decision {
			content = "人工审批拒绝执行"
			code = "APPROVAL_DENIED"
		} else if handle := handles[call.Call.Name]; handle == nil {
			content = "工具不存在"
			code = "TOOL_NOT_FOUND"
		} else {
			_, err := d.commit(a, session.CommandFunc(func(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
				p, _ := ProjectionFrom(s)
				v, ok := p.ToolCalls[id]
				if !ok || v.Done || v.Started || p.CancelCause != nil || p.ActiveTurnID != turn {
					return nil, ErrStaleDriverActivity
				}
				if v.RequiresApproval && (v.Decision == nil || !*v.Decision) {
					return nil, ErrApprovalNotPending
				}
				e := fact(EventToolStarted, turn, v.Call.StepID, map[string]string{"call_id": id})
				e.CallID = id
				return []session.NewEvent{e}, nil
			}))
			if err != nil {
				return err
			}
			result, err := handle.Execute(ctx, call.Call)
			content = result.Content
			structured = result.Structured
			if err == nil {
				err = result.Err
			}
			if err != nil {
				if content != "" {
					content += "\n"
				}
				content += err.Error()
				code = "TOOL_ERROR"
			}
		}
		_, err := d.commit(a, session.CommandFunc(func(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
			p, _ := ProjectionFrom(s)
			v := p.ToolCalls[id]
			if v.Done {
				return nil, nil
			}
			if p.ActiveTurnID != turn {
				return nil, ErrStaleDriverActivity
			}
			return []session.NewEvent{toolResultEvent(v, content, structured, code)}, nil
		}))
		if err != nil {
			return err
		}
	}
	return nil
}
