package agent

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

func TestDriverStateCommandsPersistStepAttemptAndTerminal(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	claim := startDriverTestTurn(t, writer)

	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	at := time.Date(2026, 8, 26, 16, 0, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartClaimedStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: 1,
		StepID: "step-1", EventID: "event-step-start", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	projection, _ := ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveStepID != "step-1" || projection.ActiveStepIndex != 1 || projection.ActiveStepClaimID != claim.ClaimID || projection.ActiveAttempt != 0 || projection.AttemptPhase != AttemptPhaseNone {
		t.Fatalf("projection after step start = %+v", projection)
	}

	requestSummary := json.RawMessage(`{"message_count":1,"tool_count":0}`)
	if _, err := lease.Actor().Submit(context.Background(), StartInitialAttemptCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 0, ExpectedPhase: AttemptPhaseNone, Model: "test-model",
		RequestSummary: requestSummary, EventID: "event-model-requested", OccurredAt: at.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	requestSummary[0] = '['
	projection, _ = ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveAttempt != 1 || projection.AttemptPhase != AttemptPhaseRequested || projection.ActiveModel != "test-model" {
		t.Fatalf("projection after model request = %+v", projection)
	}

	if _, err := lease.Actor().Submit(context.Background(), FinishActiveStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 1, ExpectedPhase: AttemptPhaseRequested, Reason: StepEndInterrupted,
		EventID: "event-step-end", OccurredAt: at.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	projection, _ = ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveStepID != "" || projection.ActiveStepIndex != 0 || projection.ActiveStepClaimID != "" || projection.ActiveAttempt != 0 || projection.AttemptPhase != AttemptPhaseNone || projection.ActiveModel != "" {
		t.Fatalf("projection after step end = %+v", projection)
	}

	events := store.snapshot()
	if len(events) < 3 {
		t.Fatalf("events = %d", len(events))
	}
	tail := events[len(events)-3:]
	wantTypes := []string{"step/start", EventModelRequested, "step/end"}
	for index, want := range wantTypes {
		if tail[index].EventType != want {
			t.Fatalf("tail[%d] = %s, want %s", index, tail[index].EventType, want)
		}
	}
}

func TestFinishActiveStepCommandRejectsNormalModelTerminalBypass(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	claim := startDriverTestTurn(t, writer)
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	at := time.Date(2026, 8, 26, 16, 0, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartClaimedStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: 1,
		StepID: "step-1", EventID: "event-step-start", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Actor().Submit(context.Background(), StartInitialAttemptCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 0, ExpectedPhase: AttemptPhaseNone, Model: "test-model",
		EventID: "event-model-requested", OccurredAt: at.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	before := len(store.snapshot())
	_, err = lease.Actor().Submit(context.Background(), FinishActiveStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 1, ExpectedPhase: AttemptPhaseRequested, Reason: StepEndCompleted,
		EventID: "event-illegal-completed", OccurredAt: at.Add(2 * time.Second),
	})
	if !errors.Is(err, ErrInvalidDriverTransition) {
		t.Fatalf("error = %v, want ErrInvalidDriverTransition", err)
	}
	if got := len(store.snapshot()); got != before {
		t.Fatalf("normal terminal bypass appended events: got %d, want %d", got, before)
	}
}

func TestDriverStateCommandsRejectStaleAttemptWithoutAppending(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	claim := startDriverTestTurn(t, writer)
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	at := time.Date(2026, 8, 26, 16, 0, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartClaimedStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: 1,
		StepID: "step-1", EventID: "event-step-start", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Actor().Submit(context.Background(), StartInitialAttemptCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 0, ExpectedPhase: AttemptPhaseNone, Model: "test-model",
		EventID: "event-model-requested", OccurredAt: at.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	before := len(store.snapshot())

	_, err = lease.Actor().Submit(context.Background(), FinishActiveStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 2, ExpectedPhase: AttemptPhaseRequested, Reason: StepEndInterrupted,
		EventID: "event-stale-step-end", OccurredAt: at.Add(2 * time.Second),
	})
	if !errors.Is(err, ErrStaleDriverActivity) {
		t.Fatalf("error = %v, want ErrStaleDriverActivity", err)
	}
	if got := len(store.snapshot()); got != before {
		t.Fatalf("stale command appended events: got %d, want %d", got, before)
	}
	projection, _ := ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveStepID != "step-1" || projection.ActiveAttempt != 1 || projection.AttemptPhase != AttemptPhaseRequested {
		t.Fatalf("stale command changed projection = %+v", projection)
	}
}

func TestDriverStateReplaysAndRepairsActiveAttemptIdempotently(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	claim := startDriverTestTurn(t, writer)
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 26, 16, 0, 0, 0, time.UTC)
	if _, err := lease.Actor().Submit(context.Background(), StartClaimedStepCommand{
		ExpectedTurnID: claim.TurnID, ExpectedClaimID: claim.ClaimID, ExpectedStepIndex: 1,
		StepID: "step-1", EventID: "event-step-start", OccurredAt: at,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := lease.Actor().Submit(context.Background(), StartInitialAttemptCommand{
		ExpectedTurnID: claim.TurnID, ExpectedStepID: "step-1", ExpectedStepIndex: 1,
		ExpectedAttempt: 0, ExpectedPhase: AttemptPhaseNone, Model: "test-model",
		EventID: "event-model-requested", OccurredAt: at.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	lease.Release()

	schema, err := NewSessionSchema()
	if err != nil {
		t.Fatal(err)
	}
	head, err := store.Head(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := session.Replay(context.Background(), store, "session-1", 1, head, session.ReplayOptions{Schema: schema, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	projection, _ := ProjectionFrom(replayed)
	if projection.ActiveStepID != "step-1" || projection.ActiveStepIndex != 1 || projection.ActiveAttempt != 1 || projection.AttemptPhase != AttemptPhaseRequested || projection.ActiveModel != "test-model" {
		t.Fatalf("replayed active attempt = %+v", projection)
	}

	dispose()
	repaired, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	projection, _ = ProjectionFrom(repaired.Snapshot())
	if repaired.Snapshot().Core().Status != session.StatusIdle || projection.ActiveTurnID != "" || projection.ActiveStepID != "" || projection.ActiveAttempt != 0 || projection.AttemptPhase != AttemptPhaseNone {
		t.Fatalf("repaired state: core=%+v projection=%+v", repaired.Snapshot().Core(), projection)
	}
	events := store.snapshot()
	if len(events) < 3 || events[len(events)-3].EventType != "step/end" || events[len(events)-2].EventType != "turn/end" || events[len(events)-1].EventType != session.EventSessionRepaired {
		t.Fatalf("repair tail = %+v", events)
	}
	var repairedStepEnd StepEndPayload
	if err := json.Unmarshal(events[len(events)-3].Data, &repairedStepEnd); err != nil {
		t.Fatal(err)
	}
	if repairedStepEnd.Reason != StepEndInterrupted || !repairedStepEnd.Synthetic || repairedStepEnd.Attempt != 0 {
		t.Fatalf("synthetic step end = %+v", repairedStepEnd)
	}
	repairedEventCount := len(events)
	if err := repaired.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloaded, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Dispose(context.Background())
	if got := len(store.snapshot()); got != repairedEventCount {
		t.Fatalf("repair was not idempotent: event count = %d, want %d", got, repairedEventCount)
	}
}

func startDriverTestTurn(t *testing.T, writer *InboxWriter) InputClaim {
	t.Helper()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-user",
		Content: []ContentBlock{{Type: "text", Text: "question"}},
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	return claim
}
