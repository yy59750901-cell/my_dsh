package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

func TestActiveAttemptRunnerPersistsChunksBeforeAssemblyCommit(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()

	adapter := &agentScriptedStream{results: []agentStreamResult{
		{chunk: llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Index: 0, Text: "hello", ProviderRaw: json.RawMessage(`{"secret":"must-not-persist"}`)}},
		{chunk: llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}},
	}}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	ids := &sequenceInboxIDGenerator{values: []string{"event-chunk-0", "event-chunk-1"}}
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: ids,
		Now:         func() time.Time { return time.Date(2026, 8, 26, 17, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}

	terminal, err := runner.Run(context.Background(), activity, llm.ModelRequest{
		RequestID: "request-1", SessionID: "session-1", TurnID: activity.TurnID,
		StepID: activity.StepID, Attempt: activity.Attempt, Model: "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 1 || terminal.Message.Content[0].Text != "hello" {
		t.Fatalf("terminal message = %+v", terminal.Message)
	}
	if len(terminal.SourceEventSeqs) != 1 {
		t.Fatalf("source event seqs = %v", terminal.SourceEventSeqs)
	}

	events := store.snapshot()
	var chunks []session.Event
	for _, event := range events {
		if event.EventType == EventAssistantChunk {
			chunks = append(chunks, event)
		}
	}
	if len(chunks) != 2 {
		t.Fatalf("assistant chunk events = %d, want 2", len(chunks))
	}
	if terminal.SourceEventSeqs[0] != chunks[0].Seq {
		t.Fatalf("source event seq = %d, want first chunk seq %d", terminal.SourceEventSeqs[0], chunks[0].Seq)
	}
	if strings.Contains(string(chunks[0].Data), "must-not-persist") || strings.Contains(string(chunks[0].Data), "provider_raw") {
		t.Fatalf("provider raw leaked into durable event: %s", chunks[0].Data)
	}
	var first AssistantChunkPayload
	if err := json.Unmarshal(chunks[0].Data, &first); err != nil {
		t.Fatal(err)
	}
	if first.Attempt != 1 || first.ChunkIndex != 0 || first.Chunk.Text != "hello" {
		t.Fatalf("first chunk payload = %+v", first)
	}
	projection, _ := ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveChunkCount != 2 || projection.LastChunkIndex != 1 || projection.LastChunkEventID != "event-chunk-1" || projection.LastChunkEventSeq != chunks[1].Seq {
		t.Fatalf("chunk cursor = %+v", projection)
	}
	if adapter.closeCalls != 1 {
		t.Fatalf("stream close calls = %d, want 1", adapter.closeCalls)
	}
}

func TestPersistAttemptChunkAppendFailureDoesNotAdvanceAssembler(t *testing.T) {
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: "turn-1", StepID: "step-1", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	submitErr := errors.New("append failed")
	submitter := failingAttemptSubmitter{err: submitErr}
	_, err = persistAttemptChunk(context.Background(), submitter, assembler, AttemptActivity{
		TurnID: "turn-1", StepID: "step-1", StepIndex: 1, Attempt: 1, Phase: AttemptPhaseRequested,
	}, llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "not committed"}, "event-1", time.Now())
	if !errors.Is(err, submitErr) {
		t.Fatalf("error = %v, want %v", err, submitErr)
	}
	if snapshot := assembler.Snapshot(); snapshot.Revision != 0 || len(snapshot.Blocks) != 0 {
		t.Fatalf("assembler advanced after failed append: %+v", snapshot)
	}
}

func TestPersistAttemptChunkRejectsStaleAttemptWithoutAppend(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: 2})
	if err != nil {
		t.Fatal(err)
	}
	before := len(store.snapshot())
	activity.Attempt = 2
	_, err = persistAttemptChunk(context.Background(), lease.Actor(), assembler, activity,
		llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "stale"}, "event-stale", time.Now())
	if !errors.Is(err, ErrStaleDriverActivity) {
		t.Fatalf("error = %v, want ErrStaleDriverActivity", err)
	}
	if got := len(store.snapshot()); got != before {
		t.Fatalf("stale chunk appended events: got %d, want %d", got, before)
	}
	if assembler.Snapshot().Revision != 0 {
		t.Fatalf("stale chunk advanced assembler")
	}
}

func TestPersistAttemptChunkRecoversCommittedAppendUncertainty(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.appendErrorAfterWrite = errors.New("transport lost after commit")
	store.mu.Unlock()

	submitter := &closingAfterUncertainSubmitter{actor: lease.Actor(), activity: activity}
	committed, err := persistAttemptChunk(context.Background(), submitter, assembler, activity,
		llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "durable"}, "event-uncertain", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if committed.EventID != "event-uncertain" || committed.Seq == 0 {
		t.Fatalf("recovered receipt = %+v", committed)
	}
	snapshot := assembler.Snapshot()
	if snapshot.Revision != 1 || len(snapshot.Blocks) != 1 || snapshot.Blocks[0].Text != "durable" || len(snapshot.SourceEventSeqs) != 1 || snapshot.SourceEventSeqs[0] != committed.Seq {
		t.Fatalf("assembler after recovery = %+v", snapshot)
	}
	count := 0
	for _, event := range store.snapshot() {
		if event.EventID == "event-uncertain" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("uncertain chunk copies = %d, want 1", count)
	}
}

func TestAssistantChunkReplaysBeforeActiveAttemptRepair(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	lease, activity := startActiveAttemptForRunner(t, writer)
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistAttemptChunk(context.Background(), lease.Actor(), assembler, activity,
		llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "before crash"}, "event-before-crash", time.Now()); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	dispose()

	repaired, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer repaired.Dispose(context.Background())
	projection, _ := ProjectionFrom(repaired.Snapshot())
	if repaired.Snapshot().Core().Status != session.StatusIdle || projection.ActiveTurnID != "" || projection.ActiveStepID != "" || projection.ActiveChunkCount != 0 || projection.LastChunkEventID != "" {
		t.Fatalf("repaired state: core=%+v projection=%+v", repaired.Snapshot().Core(), projection)
	}
	foundChunk := false
	for _, event := range store.snapshot() {
		if event.EventID == "event-before-crash" && event.EventType == EventAssistantChunk {
			foundChunk = true
		}
	}
	if !foundChunk {
		t.Fatal("committed assistant chunk disappeared during replay/repair")
	}
}

func TestDurableFinishIsRecoveredIntoTerminalBeforeCoreRepair(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	lease, activity := startActiveAttemptForRunner(t, writer)
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	var chunkSeqs []uint64
	for index, chunk := range []llm.StreamChunk{
		{Kind: llm.StreamChunkTextDelta, Text: "recover me"},
		{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}},
	} {
		committed, persistErr := persistAttemptChunk(context.Background(), lease.Actor(), assembler, activity, chunk, fmt.Sprintf("event-repair-%d", index), time.Now())
		if persistErr != nil {
			t.Fatal(persistErr)
		}
		chunkSeqs = append(chunkSeqs, committed.Seq)
	}
	lease.Release()
	dispose()
	store.mu.Lock()
	store.appendErrorAfterWrite = errors.New("repair terminal response lost after commit")
	store.mu.Unlock()

	repaired, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	projection, _ := ProjectionFrom(repaired.Snapshot())
	if repaired.Snapshot().Core().Status != session.StatusIdle || projection.ActiveTurnID != "" || projection.ActiveStepID != "" {
		t.Fatalf("repaired state: core=%+v projection=%+v", repaired.Snapshot().Core(), projection)
	}
	messages, err := repaired.Snapshot().Surface().DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || !strings.Contains(string(messages[0]), "recover me") {
		t.Fatalf("recovered surface messages = %s", messages)
	}
	var terminalStepEnd StepEndPayload
	foundTerminalStepEnd := false
	foundAssistant := false
	for _, event := range store.snapshot() {
		switch event.EventType {
		case session.SurfaceEventAssistantMessage:
			foundAssistant = true
			if len(event.SourceEventSeqs) != 1 || event.SourceEventSeqs[0] != chunkSeqs[0] || event.SourceEventSeqs[0] == chunkSeqs[1] {
				t.Fatalf("recovered assistant sources = %v, chunks = %v", event.SourceEventSeqs, chunkSeqs)
			}
		case "step/end":
			if !foundTerminalStepEnd {
				if err := json.Unmarshal(event.Data, &terminalStepEnd); err != nil {
					t.Fatal(err)
				}
				foundTerminalStepEnd = true
			}
		}
	}
	if !foundAssistant || !foundTerminalStepEnd || terminalStepEnd.Reason != StepEndCompleted || terminalStepEnd.Synthetic {
		t.Fatalf("recovered assistant=%v terminal step end=%+v", foundAssistant, terminalStepEnd)
	}
	eventCount := len(store.snapshot())
	if err := repaired.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Dispose(context.Background())
	if got := len(store.snapshot()); got != eventCount {
		t.Fatalf("second load appended duplicate repair events: got %d, want %d", got, eventCount)
	}
}

func TestActiveAttemptRunnerPersistsCancelledTerminalWithDurableContext(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()

	adapter := &agentScriptedStream{}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: &sequenceInboxIDGenerator{values: []string{"event-aborted-finish"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	terminal, err := runner.Run(ctx, activity, llm.ModelRequest{
		SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID,
		Attempt: activity.Attempt, Model: "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Finish.Kind != llm.FinishAborted || terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != llm.FailureAborted {
		t.Fatalf("terminal finish = %+v", terminal.Finish)
	}
	events := store.snapshot()
	if events[len(events)-1].EventID != "event-aborted-finish" || events[len(events)-1].EventType != EventAssistantChunk {
		t.Fatalf("last event = %+v", events[len(events)-1])
	}
}

func TestStartAndRunInitialAttemptCommitsSuccessfulTerminalBatch(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, step := startClaimedStepForRunner(t, writer)
	defer lease.Release()

	adapter := &agentScriptedStream{results: []agentStreamResult{
		{chunk: llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Index: 0, Text: "hello"}},
		{chunk: llm.StreamChunk{Kind: llm.StreamChunkUsage, Usage: &llm.TokenUsage{InputTokens: 4, OutputTokens: 2}}},
		{chunk: llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}},
	}}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: &sequenceInboxIDGenerator{values: []string{
			"event-requested", "event-text", "event-usage-chunk", "event-finish",
			"event-usage", "event-message", "event-step-end",
		}},
		Now: func() time.Time { return time.Date(2026, 8, 26, 18, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := runner.StartAndRunInitialAttempt(context.Background(), step, llm.ModelRequest{
		RequestID: "request-1", SessionID: "session-1", TurnID: step.TurnID,
		StepID: step.StepID, Attempt: 1, Model: "test-model",
	}, json.RawMessage(`{"message_count":1,"tool_count":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Finish.Kind != llm.FinishStop || terminal.Message == nil || terminal.Usage == nil {
		t.Fatalf("terminal = %+v", terminal)
	}

	events := store.snapshot()
	wantTail := []string{EventModelUsage, session.SurfaceEventAssistantMessage, "step/end"}
	if len(events) < len(wantTail) {
		t.Fatalf("events = %d", len(events))
	}
	tail := events[len(events)-len(wantTail):]
	for index, want := range wantTail {
		if tail[index].EventType != want {
			t.Fatalf("tail[%d] = %s, want %s", index, tail[index].EventType, want)
		}
	}
	if tail[1].EventID != "event-message" || len(tail[1].SourceEventSeqs) != 1 {
		t.Fatalf("assistant message event = %+v", tail[1])
	}
	var textChunkSeq uint64
	for _, event := range events {
		if event.EventID == "event-text" {
			textChunkSeq = event.Seq
		}
	}
	if textChunkSeq == 0 || tail[1].SourceEventSeqs[0] != textChunkSeq {
		t.Fatalf("assistant sources = %v, text chunk seq = %d", tail[1].SourceEventSeqs, textChunkSeq)
	}
	var stepEnd StepEndPayload
	if err := json.Unmarshal(tail[2].Data, &stepEnd); err != nil {
		t.Fatal(err)
	}
	if stepEnd.Reason != StepEndCompleted || stepEnd.Attempt != 1 || stepEnd.UsageMissing {
		t.Fatalf("step end = %+v", stepEnd)
	}
	projection, _ := ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveStepID != "" || projection.ActiveAttempt != 0 || len(projection.ActiveChunks) != 0 {
		t.Fatalf("active attempt was not closed: %+v", projection)
	}
	messages, err := lease.Actor().Snapshot().Surface().DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || !strings.Contains(string(messages[0]), "hello") {
		t.Fatalf("surface messages = %s", messages)
	}
}

func TestStartAndRunInitialAttemptCommitsModelErrorAndStepEndAtomically(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, step := startClaimedStepForRunner(t, writer)
	defer lease.Release()

	adapter := &agentScriptedStream{results: []agentStreamResult{
		{chunk: llm.StreamChunk{
			Kind: llm.StreamChunkFinish,
			Finish: &llm.FinishReason{
				Kind:    llm.FinishError,
				Failure: &llm.LlmFailure{Code: llm.FailureRateLimit, Message: "limited"},
			},
		}},
	}}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: &sequenceInboxIDGenerator{values: []string{
			"event-requested", "event-error-finish", "event-model-error", "event-step-end",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := runner.StartAndRunInitialAttempt(context.Background(), step, llm.ModelRequest{
		SessionID: "session-1", TurnID: step.TurnID, StepID: step.StepID,
		Attempt: 1, Model: "test-model",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Finish.Kind != llm.FinishError || terminal.Message != nil {
		t.Fatalf("terminal = %+v", terminal)
	}
	events := store.snapshot()
	if len(events) < 2 || events[len(events)-2].EventType != "model/error" || events[len(events)-1].EventType != "step/end" {
		t.Fatalf("terminal tail = %+v", events)
	}
	if events[len(events)-2].Seq+1 != events[len(events)-1].Seq {
		t.Fatalf("model/error and step/end are not contiguous")
	}
	var failure ModelErrorPayload
	if err := json.Unmarshal(events[len(events)-2].Data, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Attempt != 1 || failure.Failure.Code != llm.FailureRateLimit {
		t.Fatalf("model error = %+v", failure)
	}
	var stepEnd StepEndPayload
	if err := json.Unmarshal(events[len(events)-1].Data, &stepEnd); err != nil {
		t.Fatal(err)
	}
	if stepEnd.Reason != StepEndError || !stepEnd.UsageMissing {
		t.Fatalf("step end = %+v", stepEnd)
	}
}

func TestCommitAttemptTerminalRejectsStaleChunkCount(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()
	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := persistAttemptChunk(context.Background(), lease.Actor(), assembler, activity,
		llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}, "event-finish", time.Now()); err != nil {
		t.Fatal(err)
	}
	before := len(store.snapshot())
	_, err = lease.Actor().Submit(context.Background(), CommitAttemptTerminalCommand{
		ExpectedTurnID: activity.TurnID, ExpectedStepID: activity.StepID,
		ExpectedStepIndex: activity.StepIndex, ExpectedAttempt: activity.Attempt,
		ExpectedPhase: activity.Phase, ExpectedChunkCount: 0,
		EventIDs: AttemptTerminalEventIDs{StepEnd: "event-step-end"}, OccurredAt: time.Now(),
	})
	if !errors.Is(err, ErrStaleDriverActivity) {
		t.Fatalf("error = %v, want ErrStaleDriverActivity", err)
	}
	if got := len(store.snapshot()); got != before {
		t.Fatalf("stale terminal appended events: got %d, want %d", got, before)
	}
}

func TestStartAndRunInitialAttemptCommitsTerminalDespiteCloseError(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, step := startClaimedStepForRunner(t, writer)
	defer lease.Release()

	adapter := &agentScriptedStream{
		results: []agentStreamResult{
			{chunk: llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "durable"}},
			{chunk: llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}},
		},
		closeErr: errors.New("close failed after terminal"),
	}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: &sequenceInboxIDGenerator{values: []string{
			"event-requested", "event-text", "event-finish", "event-message", "event-step-end",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.StartAndRunInitialAttempt(context.Background(), step, llm.ModelRequest{
		SessionID: "session-1", TurnID: step.TurnID, StepID: step.StepID,
		Attempt: 1, Model: "test-model",
	}, nil); err != nil {
		t.Fatal(err)
	}
	events := store.snapshot()
	if events[len(events)-1].EventType != "step/end" {
		t.Fatalf("terminal batch was not committed: last=%s", events[len(events)-1].EventType)
	}
}

func TestActiveAttemptRunnerRejectsRequestThatDiffersFromDurableAttempt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*AttemptActivity, *llm.ModelRequest)
		prime  bool
	}{
		{name: "session", mutate: func(_ *AttemptActivity, request *llm.ModelRequest) { request.SessionID = "wrong-session" }},
		{name: "turn", mutate: func(activity *AttemptActivity, request *llm.ModelRequest) {
			activity.TurnID = "wrong-turn"
			request.TurnID = activity.TurnID
		}},
		{name: "step", mutate: func(activity *AttemptActivity, request *llm.ModelRequest) {
			activity.StepID = "wrong-step"
			request.StepID = activity.StepID
		}},
		{name: "step index", mutate: func(activity *AttemptActivity, _ *llm.ModelRequest) { activity.StepIndex++ }},
		{name: "attempt", mutate: func(activity *AttemptActivity, request *llm.ModelRequest) {
			activity.Attempt++
			request.Attempt = activity.Attempt
		}},
		{name: "phase", mutate: func(activity *AttemptActivity, _ *llm.ModelRequest) { activity.Phase = AttemptPhase("streaming") }},
		{name: "model", mutate: func(_ *AttemptActivity, request *llm.ModelRequest) { request.Model = "other-model" }},
		{name: "existing chunk cursor", prime: true, mutate: func(_ *AttemptActivity, _ *llm.ModelRequest) {}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, _, _, dispose := newTestInboxWriter(t)
			defer dispose()
			lease, activity := startActiveAttemptForRunner(t, writer)
			defer lease.Release()
			if test.prime {
				assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := persistAttemptChunk(context.Background(), lease.Actor(), assembler, activity,
					llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "already started"}, "event-existing", time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			providerCalled := false
			runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
				providerCalled = true
				return &agentScriptedStream{}, nil
			}), nil)
			runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			request := llm.ModelRequest{
				SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID,
				Attempt: activity.Attempt, Model: "test-model",
			}
			test.mutate(&activity, &request)
			_, err = runner.Run(context.Background(), activity, request)
			if !errors.Is(err, ErrStaleDriverActivity) {
				t.Fatalf("error = %v, want ErrStaleDriverActivity", err)
			}
			if providerCalled {
				t.Fatal("provider was called for request that differed from durable attempt")
			}
		})
	}
}

func TestCommitAssemblyMutationRejectsMismatchedDurableReceipt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*session.Event)
	}{
		{name: "event id", mutate: func(event *session.Event) { event.EventID = "event-wrong" }},
		{name: "replay policy", mutate: func(event *session.Event) { event.ReplayPolicy = session.ReplayRequired }},
		{name: "turn id", mutate: func(event *session.Event) { event.TurnID = "turn-wrong" }},
		{name: "step id", mutate: func(event *session.Event) { event.StepID = "step-wrong" }},
		{name: "payload", mutate: func(event *session.Event) {
			event.Data = json.RawMessage(`{"attempt":1,"chunk_index":0,"chunk":{"kind":"text-delta","text":"wrong"}}`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: "turn-1", StepID: "step-1", Attempt: 1})
			if err != nil {
				t.Fatal(err)
			}
			chunk := llm.StreamChunk{Kind: llm.StreamChunkTextDelta, Text: "hello"}
			mutation, err := assembler.PlanPush(chunk)
			if err != nil {
				t.Fatal(err)
			}
			command := AppendAssistantChunkCommand{
				ExpectedTurnID: "turn-1", ExpectedStepID: "step-1", ExpectedStepIndex: 1,
				ExpectedAttempt: 1, ExpectedPhase: AttemptPhaseRequested, ExpectedChunkIndex: 0,
				Chunk: chunk, EventID: "event-right", OccurredAt: time.Now(),
			}
			data, err := command.payloadData()
			if err != nil {
				t.Fatal(err)
			}
			event := session.Event{
				SchemaVersion: session.SchemaVersion{Major: 1}, EventType: EventAssistantChunk,
				EventID: "event-right", Seq: 1, ReplayPolicy: session.ReplayIgnorable,
				Data: data, TurnID: "turn-1", StepID: "step-1",
			}
			test.mutate(&event)
			_, err = commitAssemblyMutation(mutation, command, event)
			if !errors.Is(err, session.ErrConflict) {
				t.Fatalf("error = %v, want session.ErrConflict", err)
			}
			if assembler.Snapshot().Revision != 0 {
				t.Fatal("mismatched receipt advanced assembler")
			}
		})
	}
}

func TestActiveAttemptRunnerDoesNotBlockActorMailbox(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
	lease, activity := startActiveAttemptForRunner(t, writer)
	defer lease.Release()

	adapter := &agentBlockingFinishStream{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := llm.NewRuntime(agentProviderFunc(func(context.Context, llm.ModelRequest) (llm.Stream, error) {
		return adapter, nil
	}), nil)
	runner, err := NewActiveAttemptRunner(lease.Actor(), runtime, AttemptRunnerOptions{
		IDGenerator: &sequenceInboxIDGenerator{values: []string{"event-finish"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(context.Background(), activity, llm.ModelRequest{
			SessionID: "session-1", TurnID: activity.TurnID, StepID: activity.StepID,
			Attempt: activity.Attempt, Model: "test-model",
		})
		runDone <- runErr
	}()
	<-adapter.entered

	submitDone := make(chan error, 1)
	go func() {
		_, submitErr := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			return nil, nil
		}))
		submitDone <- submitErr
	}()
	select {
	case submitErr := <-submitDone:
		if submitErr != nil {
			t.Fatal(submitErr)
		}
	case <-time.After(time.Second):
		t.Fatal("actor mailbox was blocked by stream read")
	}
	close(adapter.release)
	if runErr := <-runDone; runErr != nil {
		t.Fatal(runErr)
	}
}

func startClaimedStepForRunner(t *testing.T, writer *InboxWriter) (*session.ActorLease, StepActivity) {
	t.Helper()
	claim := startDriverTestTurn(t, writer)
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 26, 16, 30, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartClaimedStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: 1,
		StepID: "step-1", EventID: "event-step-start", OccurredAt: at,
	}); err != nil {
		lease.Release()
		t.Fatal(err)
	}
	return lease, StepActivity{TurnID: claim.TurnID, StepID: "step-1", StepIndex: 1}
}

func startActiveAttemptForRunner(t *testing.T, writer *InboxWriter) (*session.ActorLease, AttemptActivity) {
	t.Helper()
	lease, step := startClaimedStepForRunner(t, writer)
	at := time.Date(2026, 8, 26, 16, 30, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartInitialAttemptCommand{
		ExpectedTurnID: step.TurnID, ExpectedStepID: step.StepID, ExpectedStepIndex: step.StepIndex,
		ExpectedAttempt: 0, ExpectedPhase: AttemptPhaseNone, Model: "test-model",
		EventID: "event-model-requested", OccurredAt: at.Add(time.Second),
	}); err != nil {
		lease.Release()
		t.Fatal(err)
	}
	return lease, AttemptActivity{TurnID: step.TurnID, StepID: step.StepID, StepIndex: step.StepIndex, Attempt: 1, Phase: AttemptPhaseRequested}
}

type sequenceInboxIDGenerator struct {
	mu     sync.Mutex
	values []string
}

func (generator *sequenceInboxIDGenerator) NewID(string) (string, error) {
	generator.mu.Lock()
	defer generator.mu.Unlock()
	if len(generator.values) == 0 {
		return "", errors.New("no test ids remaining")
	}
	value := generator.values[0]
	generator.values = generator.values[1:]
	return value, nil
}

type closingAfterUncertainSubmitter struct {
	actor    *session.Actor
	activity AttemptActivity
}

func (submitter *closingAfterUncertainSubmitter) Submit(ctx context.Context, command session.Command) ([]session.Event, error) {
	events, err := submitter.actor.Submit(ctx, command)
	if err != nil && len(events) != 0 {
		_, closeErr := submitter.actor.Submit(context.Background(), FinishActiveStepCommand{
			ExpectedTurnID: submitter.activity.TurnID, ExpectedStepID: submitter.activity.StepID,
			ExpectedStepIndex: submitter.activity.StepIndex, ExpectedAttempt: submitter.activity.Attempt,
			ExpectedPhase: submitter.activity.Phase, Reason: StepEndInterrupted,
			EventID: "event-close-after-uncertain", OccurredAt: time.Now(),
		})
		if closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
	}
	return events, err
}

func (submitter *closingAfterUncertainSubmitter) Snapshot() session.Snapshot {
	return submitter.actor.Snapshot()
}

type failingAttemptSubmitter struct {
	err error
}

func (submitter failingAttemptSubmitter) Submit(context.Context, session.Command) ([]session.Event, error) {
	return nil, submitter.err
}

func (failingAttemptSubmitter) Snapshot() session.Snapshot {
	return session.Snapshot{}
}

type agentProviderFunc func(context.Context, llm.ModelRequest) (llm.Stream, error)

func (function agentProviderFunc) Stream(ctx context.Context, request llm.ModelRequest) (llm.Stream, error) {
	return function(ctx, request)
}

type agentStreamResult struct {
	chunk llm.StreamChunk
	err   error
}

type agentScriptedStream struct {
	mu         sync.Mutex
	results    []agentStreamResult
	closeCalls int
	closeErr   error
}

func (stream *agentScriptedStream) Next(context.Context) (llm.StreamChunk, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.results) == 0 {
		return llm.StreamChunk{}, io.EOF
	}
	result := stream.results[0]
	stream.results = stream.results[1:]
	return result.chunk, result.err
}

func (stream *agentScriptedStream) Close() error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.closeCalls++
	return stream.closeErr
}

type agentBlockingFinishStream struct {
	mu      sync.Mutex
	entered chan struct{}
	release chan struct{}
	read    bool
}

func (stream *agentBlockingFinishStream) Next(context.Context) (llm.StreamChunk, error) {
	stream.mu.Lock()
	if stream.read {
		stream.mu.Unlock()
		return llm.StreamChunk{}, io.EOF
	}
	stream.read = true
	close(stream.entered)
	stream.mu.Unlock()
	<-stream.release
	return llm.StreamChunk{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}, nil
}

func (stream *agentBlockingFinishStream) Close() error {
	return nil
}
