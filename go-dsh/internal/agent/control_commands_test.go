package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

func TestModelRequestSummaryContainsOnlyAuditFields(t *testing.T) {
	r := llm.ModelRequest{
		RequestID: "request-1", Model: "test-model", Attempt: 1,
		Messages: []llm.Message{{Role: llm.RoleSystem, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: "secret-prompt"}}}},
		Tools:    []llm.ToolDefinition{{Name: "tool", Description: "secret-description", InputSchema: json.RawMessage(`{"b":2,"a":1}`)}},
		Metadata: map[string]json.RawMessage{"api_key": json.RawMessage(`"secret-key"`), "Authorization": json.RawMessage(`"secret-header"`)},
	}
	data, err := summarizeModelRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-", "messages", "api_key", "Authorization", "input_schema", "description"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("摘要泄露内容: %s", data)
		}
	}
	var summary modelRequestSummary
	if err = json.Unmarshal(data, &summary); err != nil || summary.MessageCount != 1 || summary.RoleCounts[llm.RoleSystem] != 1 || summary.ToolCount != 1 || !strings.HasPrefix(summary.RequestHash, "sha256:") {
		t.Fatalf("摘要=%s err=%v", data, err)
	}
	r.RequestID, r.SessionID, r.TurnID, r.StepID, r.Attempt = "retry", "other", "turn", "step", 2
	r.Metadata = map[string]json.RawMessage{"trace": json.RawMessage(`"different"`)}
	r.Tools[0].InputSchema = json.RawMessage(`{ "a": 1.0, "b": 2 }`)
	if hash, err := modelRequestHash(r); err != nil || hash != summary.RequestHash {
		t.Fatalf("重试与诊断信息改变了语义哈希: %s %v", hash, err)
	}
	base, _ := json.Marshal(r)
	for name, mutate := range map[string]func(*llm.ModelRequest){
		"prompt":      func(r *llm.ModelRequest) { r.Messages[0].Content[0].Text = "changed" },
		"model":       func(r *llm.ModelRequest) { r.Model = "changed" },
		"tools":       func(r *llm.ModelRequest) { r.Tools[0].InputSchema = json.RawMessage(`{"type":"object"}`) },
		"temperature": func(r *llm.ModelRequest) { v := 0.2; r.Temperature = &v },
		"max-tokens":  func(r *llm.ModelRequest) { v := 20; r.MaxTokens = &v },
		"reasoning":   func(r *llm.ModelRequest) { r.Reasoning = &llm.ReasoningOptions{Enabled: true} },
	} {
		t.Run(name, func(t *testing.T) {
			var changed llm.ModelRequest
			if err := json.Unmarshal(base, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if hash, err := modelRequestHash(changed); err != nil || hash == summary.RequestHash {
				t.Fatalf("语义修改未反映到哈希: %s %v", hash, err)
			}
		})
	}
}

func TestToolResultEnvelopeWithoutStructuredData(t *testing.T) {
	for _, structured := range []json.RawMessage{nil, {}} {
		e := toolResultEvent(ToolCallState{Call: tool.Call{ID: "internal", Name: "write"}, ProviderCallID: "provider"}, "denied", structured, "APPROVAL_DENIED")
		var payload struct {
			Message llm.Message `json:"message"`
		}
		if err := json.Unmarshal(e.Data, &payload); err != nil {
			t.Fatal(err)
		}
		block := payload.Message.Content[0]
		if block.ToolCallID != "provider" || string(block.ToolResult) != `{"content":"denied","structured":null,"error":"APPROVAL_DENIED"}` {
			t.Fatalf("结果=%s", block.ToolResult)
		}
	}
}

func TestDefaultRetryPolicyBounds(t *testing.T) {
	for _, code := range []llm.FailureCode{llm.FailureRateLimit, llm.FailureTransport, llm.FailureTimeout, llm.FailureEmptyResponse} {
		for i := uint32(1); i <= 5; i++ {
			d, ok := retryDelay(&llm.LlmFailure{Code: code}, i, 0, 6)
			if !ok || d < 0 || d > 10*time.Second {
				t.Fatalf("code=%s attempt=%d delay=%v", code, i, d)
			}
		}
		if _, ok := retryDelay(&llm.LlmFailure{Code: code}, 6, 0, 6); ok {
			t.Fatal("超过五次重试")
		}
	}
	for _, code := range []llm.FailureCode{llm.FailureInvalidCredential, llm.FailureQuota, llm.FailureProtocol, llm.FailureStreamClosed, llm.FailureAborted, llm.FailureNoAdapter, llm.FailureModelNotFound} {
		if _, ok := retryDelay(&llm.LlmFailure{Code: code}, 1, 0, 6); ok {
			t.Fatalf("unexpected retry %s", code)
		}
	}
	retryAfter := int64(10000)
	f := &llm.LlmFailure{Code: llm.FailureRateLimit, ProviderRetryAfterMs: &retryAfter}
	if d, ok := retryDelay(f, 1, 50*time.Second, 6); !ok || d != 10*time.Second {
		t.Fatalf("Retry-After delay=%v retry=%v", d, ok)
	}
	if _, ok := retryDelay(f, 1, 51*time.Second, 6); ok {
		t.Fatal("累计等待超过60秒")
	}
	retryAfter = -1
	for range 30 {
		d, ok := retryDelay(f, 1, 0, 6)
		if !ok || d < 450*time.Millisecond || d > 550*time.Millisecond {
			t.Fatalf("jitter=%v", d)
		}
	}
}

type delayedCancelReply struct {
	*session.Actor
	ready   chan struct{}
	release chan struct{}
}

func (s *delayedCancelReply) Submit(ctx context.Context, command session.Command) ([]session.Event, error) {
	events, err := s.Actor.Submit(ctx, command)
	close(s.ready)
	<-s.release
	return events, err
}

func TestBoundCancelNeverTouchesLaterActivity(t *testing.T) {
	for _, mode := range []string{"before-decide", "failure-same-turn", "failure-next-turn", "committed-next-turn", "different-actor"} {
		t.Run(mode, func(t *testing.T) {
			writer, store, _, dispose := newTestInboxWriter(t)
			defer dispose()
			lease, activity := startActiveAttemptForRunner(t, writer)
			defer lease.Release()
			a := lease.Actor()
			originalCtx, originalCancel := context.WithCancel(context.Background())
			defer originalCancel()
			d := &sessionDriver{actor: a, turn: activity.TurnID, cancel: originalCancel}
			target := cancelTarget{actor: a, turn: activity.TurnID, driver: d}
			ctx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			failure := errors.New("cancel append failed")
			if mode == "before-decide" {
				cancelRequest()
			} else {
				store.mu.Lock()
				if mode == "committed-next-turn" {
					store.appendErrorAfterWrite = failure
				} else {
					store.appendErrorBeforeWrite = failure
				}
				store.mu.Unlock()
			}
			s := &delayedCancelReply{Actor: a, ready: make(chan struct{}), release: make(chan struct{})}
			defer func() {
				select {
				case <-s.release:
				default:
					close(s.release)
				}
			}()
			done := make(chan error, 1)
			go func() { done <- target.submit(ctx, s, "original-only") }()
			select {
			case <-s.ready:
			case <-time.After(3 * time.Second):
				t.Fatal("未收到取消提交结果")
			}
			currentCtx := originalCtx
			if mode != "failure-same-turn" {
				if _, err := a.Submit(context.Background(), closeActivityCommand{Turn: activity.TurnID, Reason: "completed"}); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.FollowUp(context.Background(), InboxRequest{SessionID: "session-1", RequestID: "fresh", Content: []ContentBlock{{Type: "text", Text: "fresh"}}}); err != nil {
					t.Fatal(err)
				}
				claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
				if err != nil {
					t.Fatal(err)
				}
				var nextCancel context.CancelFunc
				currentCtx, nextCancel = context.WithCancel(context.Background())
				defer nextCancel()
				d.mu.Lock()
				d.turn, d.cancel = claim.TurnID, nextCancel
				if mode == "different-actor" {
					d.actor, d.turn = &session.Actor{}, activity.TurnID
				}
				d.mu.Unlock()
			}
			close(s.release)
			select {
			case err := <-done:
				if mode == "before-decide" && !errors.Is(err, context.Canceled) {
					t.Fatalf("err=%v", err)
				}
				if mode == "committed-next-turn" && err != nil {
					t.Fatal(err)
				}
				if mode != "before-decide" && mode != "committed-next-turn" && !errors.Is(err, failure) {
					t.Fatalf("err=%v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("取消未返回")
			}
			if mode == "failure-same-turn" {
				if originalCtx.Err() == nil {
					t.Fatal("持久化失败未停止原执行")
				}
			} else if currentCtx.Err() != nil || originalCtx.Err() != nil {
				t.Fatal("迟到取消触碰了不匹配活动")
			}
			p, _ := ProjectionFrom(a.Snapshot())
			if p.CancelCause != nil {
				t.Fatalf("新增了取消原因: %+v", p.CancelCause)
			}
			for _, e := range store.snapshot() {
				if e.EventType == EventCancelRequested && (mode != "committed-next-turn" || e.TurnID != activity.TurnID) {
					t.Fatalf("产生了额外取消事件: %s", e.Data)
				}
			}
		})
	}
}

func TestBoundCancelRejectsTurnStartedAfterCapture(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &sessionDriver{actor: lease.Actor(), turn: activity.TurnID, cancel: cancel}
	for _, turn := range []string{"", "older-turn"} {
		err := (cancelTarget{actor: lease.Actor(), turn: turn, driver: d}).submit(context.Background(), lease.Actor(), "late")
		if !errors.Is(err, ErrStaleDriverActivity) || ctx.Err() != nil {
			t.Fatalf("迟到取消=%v ctx=%v", err, ctx.Err())
		}
	}
}

func TestCancelCommittedCauseWinsAndPreservesNewSteering(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	a := lease.Actor()
	store.mu.Lock()
	store.appendErrorAfterWrite = errors.New("response lost")
	store.mu.Unlock()
	first := CancelCause{Source: CancelSourceUser, Reason: "first"}
	es, err := submitCommitted(context.Background(), a, cancelCommand{Cause: first})
	if err != nil || len(es) == 0 {
		t.Fatalf("commit=%v err=%v", es, err)
	}
	_, err = submitCommitted(context.Background(), a, cancelCommand{Cause: CancelCause{Source: CancelSourceDeadline, Reason: "second"}})
	if err != nil {
		t.Fatal(err)
	}
	p, _ := ProjectionFrom(a.Snapshot())
	if p.CancelCause == nil || *p.CancelCause != first {
		t.Fatalf("cause=%+v", p.CancelCause)
	}
	r, err := writer.Steer(context.Background(), InboxRequest{SessionID: "session-1", RequestID: "new", Content: []ContentBlock{{Type: "text", Text: "new work"}}})
	if err != nil || r.Target != InboxNextTurn {
		t.Fatalf("steer=%+v err=%v", r, err)
	}
	_, err = a.Submit(context.Background(), AppendAssistantChunkCommand{ExpectedTurnID: activity.TurnID, ExpectedStepID: activity.StepID, ExpectedStepIndex: 1, ExpectedAttempt: 1, ExpectedPhase: AttemptPhaseRequested, Chunk: llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "late"}, EventID: "late", OccurredAt: time.Now()})
	if !errors.Is(err, ErrStaleDriverActivity) {
		t.Fatalf("late chunk=%v", err)
	}
	if _, err = a.Submit(context.Background(), closeActivityCommand{Turn: activity.TurnID, Reason: "error"}); err != nil {
		t.Fatal(err)
	}
	p, _ = ProjectionFrom(a.Snapshot())
	if p.ActiveTurnID != "" || len(p.NextTurn) != 1 || p.NextTurn[0].Content[0].Text != "new work" {
		t.Fatalf("after cancel=%+v", p)
	}
}

func TestCancelVersusTerminalCommitsExactlyOneOutcome(t *testing.T) {
	for _, cancelFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "cancel-first", false: "terminal-first"}[cancelFirst], func(t *testing.T) {
			writer, store, _, dispose := newTestInboxWriter(t)
			defer dispose()
			lease, activity := startActiveAttemptForRunner(t, writer)
			defer lease.Release()
			runner, _ := NewActiveAttemptRunner(lease.Actor(), llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
				return &agentScriptedStream{results: []agentStreamResult{{chunk: llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "ok"}}, {chunk: llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}}}}, nil
			}), nil), AttemptRunnerOptions{})
			terminal, err := runner.Run(context.Background(), activity, llm.ModelRequest{SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 1, Model: "test-model"})
			if err != nil {
				t.Fatal(err)
			}
			ids, _ := runner.newTerminalEventIDs(terminal)
			p, _ := ProjectionFrom(lease.Actor().Snapshot())
			c := CommitAttemptTerminalCommand{ExpectedTurnID: activity.TurnID, ExpectedStepID: activity.StepID, ExpectedStepIndex: 1, ExpectedAttempt: 1, ExpectedPhase: AttemptPhaseRequested, ExpectedChunkCount: p.ActiveChunkCount, EventIDs: ids, OccurredAt: time.Now()}
			cancel := func() {
				if _, err := lease.Actor().Submit(context.Background(), cancelCommand{Cause: CancelCause{Source: CancelSourceUser}}); err != nil {
					t.Fatal(err)
				}
			}
			if cancelFirst {
				cancel()
			}
			_, err = lease.Actor().Submit(context.Background(), c)
			if cancelFirst && !errors.Is(err, ErrStaleDriverActivity) {
				t.Fatal(err)
			}
			if !cancelFirst && err != nil {
				t.Fatal(err)
			}
			if !cancelFirst {
				cancel()
			}
			if _, err = lease.Actor().Submit(context.Background(), closeActivityCommand{Turn: activity.TurnID, Reason: "aborted"}); err != nil {
				t.Fatal(err)
			}
			messages, steps := 0, 0
			for _, e := range store.snapshot() {
				if e.EventType == "assistant/message" {
					messages++
				}
				if e.EventType == "step/end" {
					steps++
				}
			}
			if messages != 1 || steps != 1 {
				t.Fatalf("messages=%d steps=%d", messages, steps)
			}
		})
	}
}

func TestCancelPersistsOnlyCurrentSafePrefixAtomically(t *testing.T) {
	for _, mode := range []string{"mixed", "tool-only", "error", "backoff", "retry-current"} {
		t.Run(mode, func(t *testing.T) {
			writer, store, _, dispose := newTestInboxWriter(t)
			defer dispose()
			lease, activity := startActiveAttemptForRunner(t, writer)
			defer lease.Release()
			a := lease.Actor()
			assembler, _ := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 1})
			seqs := []uint64{}
			n := 0
			persist := func(chunk llm.StreamChunk) {
				t.Helper()
				n++
				e, err := persistAttemptChunk(context.Background(), a, assembler, activity, chunk, fmt.Sprintf("prefix-%d", n), time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if chunk.Kind == llm.StreamChunkTextDelta || chunk.Kind == llm.StreamChunkReasoningDelta {
					seqs = append(seqs, e.Seq)
				}
			}
			if mode != "tool-only" {
				persist(llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "safe text"})
				persist(llm.StreamChunk{Kind: llm.StreamChunkReasoningDelta, Index: 1, Text: "safe reasoning"})
			}
			persist(llm.StreamChunk{Kind: llm.StreamChunkToolCallDelta, Index: 2, ToolCallID: "never-execute", ToolName: "write", ArgumentsDelta: `{`})
			if mode == "error" || mode == "backoff" || mode == "retry-current" {
				persist(llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureTransport}}})
				if mode != "error" {
					if _, err := a.Submit(context.Background(), retryAttemptCommand{Turn: activity.TurnID, Step: activity.StepID, Attempt: 1, ID: "retry"}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "retry-current" {
					r := llm.ModelRequest{SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 2, Model: "test-model"}
					if _, err := a.Submit(context.Background(), startRetryCommand{Turn: activity.TurnID, Step: activity.StepID, RetryID: "retry", Request: r}); err != nil {
						t.Fatal(err)
					}
					activity.Attempt = 2
					assembler, _ = llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 2})
					seqs = nil
					persist(llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "new attempt"})
				}
			}
			if _, err := a.Submit(context.Background(), cancelCommand{Cause: CancelCause{Source: CancelSourceUser}}); err != nil {
				t.Fatal(err)
			}
			batch, err := a.Submit(context.Background(), closeActivityCommand{Turn: activity.TurnID, Reason: "error"})
			if err != nil {
				t.Fatal(err)
			}
			wantPrefix := mode == "mixed" || mode == "retry-current"
			if wantPrefix {
				if len(batch) != 3 || batch[0].EventType != "assistant/message" || batch[1].EventType != "step/end" || batch[2].EventType != "turn/end" {
					t.Fatalf("非原子关闭批次: %+v", batch)
				}
				var payload AssistantMessagePayload
				if err := json.Unmarshal(batch[0].Data, &payload); err != nil {
					t.Fatal(err)
				}
				if !payload.Interrupted || !slices.Equal(batch[0].SourceEventSeqs, seqs) || len(payload.Message.Content) != len(seqs) {
					t.Fatalf("prefix=%s sources=%v want=%v", batch[0].Data, batch[0].SourceEventSeqs, seqs)
				}
				for _, block := range payload.Message.Content {
					if block.Type == llm.ContentBlockToolCall || block.Complete {
						t.Fatalf("错误前缀块: %+v", block)
					}
				}
				if mode == "retry-current" && strings.Contains(string(batch[0].Data), "safe text") {
					t.Fatal("保留了失败 attempt 内容")
				}
			} else if len(batch) != 2 || batch[0].EventType != "step/end" {
				t.Fatalf("错误/工具前缀进入 Surface: %+v", batch)
			}
			var end StepEndPayload
			if err := json.Unmarshal(batch[len(batch)-2].Data, &end); err != nil || end.Reason != StepEndAborted {
				t.Fatalf("end=%+v err=%v", end, err)
			}
			again, err := a.Submit(context.Background(), closeActivityCommand{Turn: activity.TurnID, Reason: "error"})
			if err != nil || len(again) != 0 {
				t.Fatalf("重复关闭=%v %v", again, err)
			}
			schema, _ := NewSessionSchema()
			head, _ := store.Head(context.Background(), "session-1")
			s, err := session.Replay(context.Background(), store, "session-1", 1, head, session.ReplayOptions{Schema: schema})
			if err != nil {
				t.Fatal(err)
			}
			messages, err := s.DeriveMessages()
			if err != nil || (len(messages) == 1) != wantPrefix {
				t.Fatalf("replay messages=%s err=%v", messages, err)
			}
		})
	}
}

func TestCancelAndRetryStartRace(t *testing.T) {
	for range 20 {
		writer, _, _, dispose := newTestInboxWriter(t)
		lease, activity := startActiveAttemptForRunner(t, writer)
		a := lease.Actor()
		runner, _ := NewActiveAttemptRunner(a, llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
			return &agentScriptedStream{results: []agentStreamResult{{chunk: llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureTransport}}}}}}, nil
		}), nil), AttemptRunnerOptions{})
		r := llm.ModelRequest{SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 1, Model: "test-model"}
		if _, err := runner.Run(context.Background(), activity, r); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Submit(context.Background(), retryAttemptCommand{Turn: r.TurnID, Step: r.StepID, Attempt: 1, ID: "retry"}); err != nil {
			t.Fatal(err)
		}
		r.Attempt = 2
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make(chan error, 2)
		go func() {
			defer wg.Done()
			_, err := a.Submit(context.Background(), startRetryCommand{Turn: r.TurnID, Step: r.StepID, RetryID: "retry", Request: r})
			errs <- err
		}()
		go func() {
			defer wg.Done()
			_, err := a.Submit(context.Background(), cancelCommand{Cause: CancelCause{Source: CancelSourceUser, Reason: "cancel"}})
			errs <- err
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil && !errors.Is(err, ErrStaleDriverActivity) {
				t.Fatal(err)
			}
		}
		p, _ := ProjectionFrom(a.Snapshot())
		if p.CancelCause == nil {
			t.Fatal("cancel lost")
		}
		if _, err := a.Submit(context.Background(), closeActivityCommand{Turn: r.TurnID, Reason: "aborted"}); err != nil {
			t.Fatal(err)
		}
		lease.Release()
		dispose()
	}
}

func TestHarnessRequestMustMatchPersistedPrompt(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	s := lease.Actor().Snapshot()
	p, _ := session.ProjectionAs[*AgentProjection](s, AgentProjectionKey)
	p.Configured = true
	r := llm.ModelRequest{SessionID: s.SessionID, TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 1, Model: "test-model", Messages: []llm.Message{{Role: llm.RoleSystem, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: "durable"}}}}}
	p.Request, _ = summarizeModelRequest(r)
	if err := validateActiveAttemptRequest(s, activity, r); err != nil {
		t.Fatal(err)
	}
	r.Messages[0].Content[0].Text = "mutated"
	if err := validateActiveAttemptRequest(s, activity, r); !errors.Is(err, ErrStaleDriverActivity) {
		t.Fatalf("request mutation=%v", err)
	}
}
