package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

func TestStartTurnAndClaimCommitsAtomicOrderedBatch(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()

	injected, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-context", Source: InputSourceSystem,
		Content:  []ContentBlock{{Type: "text", Text: "context", Data: json.RawMessage(`{"scope":"test"}`)}},
		Metadata: map[string]json.RawMessage{"trace": json.RawMessage(`{"enabled":true}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	followUp, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-user",
		Content: []ContentBlock{{Type: "text", Text: "question"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim.TurnID == "" || claim.ClaimID == "" || claim.ProposedStepIndex != 1 {
		t.Fatalf("claim identity = %+v", claim)
	}
	if !reflect.DeepEqual(claim.OrderedInputIDs, []string{injected.InputID, followUp.InputID}) || !reflect.DeepEqual(claim.SourceTargets, []InboxTarget{InboxNextStep, InboxNextTurn}) {
		t.Fatalf("claim order = %+v", claim)
	}

	events := store.snapshot()
	if len(events) != 6 {
		t.Fatalf("events = %d, want 6", len(events))
	}
	wantTypes := []string{EventInboxSpliced, EventInboxSpliced, "turn/start", EventInboxSpliced, EventInboxSpliced, EventInputClaimed}
	for index, want := range wantTypes {
		if events[index].EventType != want {
			t.Fatalf("event[%d] = %s, want %s", index, events[index].EventType, want)
		}
	}
	if claim.ClaimedAtSeq != 6 || !claim.ClaimedAt.Equal(events[5].CommittedAt) {
		t.Fatalf("claim committed fields = %+v", claim)
	}

	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	projection, ok := ProjectionFrom(lease.Actor().Snapshot())
	if !ok || len(projection.NextStep) != 0 || len(projection.NextTurn) != 0 || projection.ActiveTurnID != claim.TurnID || projection.PendingClaimID != claim.ClaimID {
		t.Fatalf("projection after claim = %+v", projection)
	}
	claim.Items[0].Content[0].Text = "mutated"
	claim.Items[0].Content[0].Data[0] = '['
	claim.Items[0].Metadata["trace"][0] = '['
	persisted, _ := projection.Claim(claim.ClaimID)
	if persisted.Items[0].Content[0].Text == "mutated" || !json.Valid(persisted.Items[0].Content[0].Data) || !json.Valid(persisted.Items[0].Metadata["trace"]) {
		t.Fatal("returned claim mutated persisted projection")
	}
}

func TestStartTurnAndClaimRequiresNextTurnInput(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	if _, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-context", Source: InputSourceSystem,
		Content: []ContentBlock{{Type: "text", Text: "context"}},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if !errors.Is(err, ErrNoClaimableInput) {
		t.Fatalf("error = %v, want ErrNoClaimableInput", err)
	}
	if len(store.snapshot()) != 1 {
		t.Fatal("failed claim changed the event log")
	}
}

func TestClaimForProposedStepRequiresPriorClaimConsumption(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-user",
		Content: []ContentBlock{{Type: "text", Text: "question"}},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-context", Source: InputSourceSystem,
		Content: []ContentBlock{{Type: "text", Text: "next context"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ClaimForProposedStep(context.Background(), "session-1", first.TurnID, 2); !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("unconsumed claim error = %v, want ErrInvalidClaimRequest", err)
	}

	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	stepID := "step-1"
	stepData, _ := json.Marshal(StepStartPayload{ClaimID: first.ClaimID, StepIndex: 1})
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{
			claimTestEvent("step-start", "step/start", first.TurnID, stepID, stepData),
			claimTestEvent("step-end", "step/end", first.TurnID, stepID, json.RawMessage(`{"reason":"completed"}`)),
		}, nil
	})); err != nil {
		t.Fatal(err)
	}
	lease.Release()

	second, err := writer.ClaimForProposedStep(context.Background(), "session-1", first.TurnID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.ProposedStepIndex != 2 || !reflect.DeepEqual(second.SourceTargets, []InboxTarget{InboxNextStep}) || len(second.Items) != 1 || second.Items[0].Content[0].Text != "next context" {
		t.Fatalf("second claim = %+v", second)
	}
	if got := len(store.snapshot()); got != 9 {
		t.Fatalf("events = %d, want 9", got)
	}
}

func TestClaimedStepAndPreStepTurnEndMustReferenceClaim(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
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
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	_, err = lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("bad-step", "step/start", claim.TurnID, "step-1", json.RawMessage(`{"claim_id":"wrong","step_index":1}`))}, nil
	}))
	if !errors.Is(err, ErrInvalidInputClaim) {
		t.Fatalf("step/start error = %v, want ErrInvalidInputClaim", err)
	}
	_, err = lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("bad-turn-end", "turn/end", claim.TurnID, "", json.RawMessage(`{"reason":"blocked"}`))}, nil
	}))
	if !errors.Is(err, ErrInvalidInputClaim) {
		t.Fatalf("turn/end error = %v, want ErrInvalidInputClaim", err)
	}

	endData, _ := json.Marshal(TurnEndPayload{Reason: "blocked", ClaimID: claim.ClaimID, ConsumedInputIDs: claim.OrderedInputIDs})
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("turn-end", "turn/end", claim.TurnID, "", endData)}, nil
	})); err != nil {
		t.Fatal(err)
	}
	projection, _ := ProjectionFrom(lease.Actor().Snapshot())
	if projection.ActiveTurnID != "" || projection.PendingClaimID != "" {
		t.Fatalf("claim was not consumed by turn/end: %+v", projection)
	}
}

func TestTurnEndRejectsClaimCredentialsAfterClaimWasConsumedByStep(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
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
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	stepData, _ := json.Marshal(StepStartPayload{ClaimID: claim.ClaimID, StepIndex: 1})
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{
			claimTestEvent("step-start", "step/start", claim.TurnID, "step-1", stepData),
			claimTestEvent("step-end", "step/end", claim.TurnID, "step-1", json.RawMessage(`{"reason":"completed"}`)),
		}, nil
	})); err != nil {
		t.Fatal(err)
	}

	historicalClaimData, _ := json.Marshal(TurnEndPayload{
		Reason:           "completed",
		ClaimID:          claim.ClaimID,
		ConsumedInputIDs: claim.OrderedInputIDs,
	})
	_, err = lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("bad-turn-end", "turn/end", claim.TurnID, "", historicalClaimData)}, nil
	}))
	if !errors.Is(err, ErrInvalidInputClaim) {
		t.Fatalf("turn/end error = %v, want ErrInvalidInputClaim", err)
	}
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("turn-end", "turn/end", claim.TurnID, "", json.RawMessage(`{"reason":"completed"}`))}, nil
	})); err != nil {
		t.Fatal(err)
	}
}

func TestStartTurnAndClaimRecoversCommittedAppendResult(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-user",
		Content: []ContentBlock{{Type: "text", Text: "question"}},
	}); err != nil {
		t.Fatal(err)
	}
	appendErr := errors.New("append response lost after commit")
	store.mu.Lock()
	store.appendErrorAfterWrite = appendErr
	store.mu.Unlock()

	claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("recovered claim returned error: %v", err)
	}
	if claim.ClaimID == "" || claim.ClaimedAtSeq != 4 || len(claim.Items) != 1 {
		t.Fatalf("recovered claim = %+v", claim)
	}
	if len(store.snapshot()) != 4 {
		t.Fatalf("events = %d, want one committed claim batch", len(store.snapshot()))
	}
}

func TestStartTurnAndClaimSerializesConcurrentCallers(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-user",
		Content: []ContentBlock{{Type: "text", Text: "question"}},
	}); err != nil {
		t.Fatal(err)
	}

	const callers = 2
	claims := make(chan InputClaim, callers)
	errorsCh := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
			claims <- claim
			errorsCh <- err
		}()
	}
	group.Wait()
	close(claims)
	close(errorsCh)

	successes := 0
	failures := 0
	for err := range errorsCh {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrInvalidClaimRequest) {
			failures++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("successes=%d failures=%d", successes, failures)
	}
	if len(store.snapshot()) != 4 {
		t.Fatalf("events = %d, want one accepted claim batch", len(store.snapshot()))
	}
}

func TestSubmitClaimDoesNotTreatClaimIDCollisionAsRecovery(t *testing.T) {
	writer, _, _, dispose := newTestInboxWriter(t)
	defer dispose()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1",
		Content: []ContentBlock{{Type: "text", Text: "first"}},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	endData, _ := json.Marshal(TurnEndPayload{Reason: "blocked", ClaimID: first.ClaimID, ConsumedInputIDs: first.OrderedInputIDs})
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("turn-end", "turn/end", first.TurnID, "", endData)}, nil
	})); err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if _, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-2",
		Content: []ContentBlock{{Type: "text", Text: "second"}},
	}); err != nil {
		t.Fatal(err)
	}

	collision := &claimInboxCommand{
		mode: claimModeStartTurn, turnID: "turn-collision", claimID: first.ClaimID,
		proposedStepIndex: 1, eventIDs: []string{"event-a", "event-b", "event-c", "event-d"},
		occurredAt: time.Date(2026, 8, 26, 16, 0, 0, 0, time.UTC),
	}
	_, err = writer.submitClaim(context.Background(), "session-1", collision)
	if !errors.Is(err, ErrInvalidClaimRequest) {
		t.Fatalf("error = %v, want ErrInvalidClaimRequest", err)
	}
}

func TestLoadActorRepairsTurnWithPendingClaimWithoutRequeue(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
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
	dispose()

	replayed, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	core := replayed.Snapshot().Core()
	projection, ok := ProjectionFrom(replayed.Snapshot())
	if !ok || core.Status != session.StatusIdle || projection.ActiveTurnID != "" || projection.PendingClaimID != "" || len(projection.NextTurn) != 0 {
		t.Fatalf("repaired state core=%+v projection=%+v", core, projection)
	}
	if persisted, exists := projection.Claim(claim.ClaimID); !exists || !reflect.DeepEqual(persisted.OrderedInputIDs, claim.OrderedInputIDs) {
		t.Fatalf("claim ownership was lost during repair: %+v", persisted)
	}
	events := store.snapshot()
	if len(events) < 6 || events[len(events)-2].EventType != "turn/end" || events[len(events)-1].EventType != session.EventSessionRepaired {
		t.Fatalf("repair tail = %+v", events)
	}
	repairedEventCount := len(events)
	if err := replayed.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloaded, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Dispose(context.Background())
	if got := len(store.snapshot()); got != repairedEventCount {
		t.Fatalf("claim repair was not idempotent: event count = %d, want %d", got, repairedEventCount)
	}
}

func TestLoadSessionActorRepairsActiveStepAndPreservesUnclaimedQueues(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	initial, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-initial",
		Content: []ContentBlock{{Type: "text", Text: "initial"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := writer.StartTurnAndClaim(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := writer.sessions.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	stepData, _ := json.Marshal(StepStartPayload{ClaimID: claim.ClaimID, StepIndex: 1})
	if _, err := lease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{claimTestEvent("step-start", "step/start", claim.TurnID, "step-1", stepData)}, nil
	})); err != nil {
		t.Fatal(err)
	}
	lease.Release()

	injected, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-context", Source: InputSourceSystem,
		Content: []ContentBlock{{Type: "text", Text: "context"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-next-turn",
		Content: []ContentBlock{{Type: "text", Text: "next turn"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	dispose()

	repaired, err := LoadSessionActor(context.Background(), store, "session-1", SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	core := repaired.Snapshot().Core()
	projection, ok := ProjectionFrom(repaired.Snapshot())
	if !ok || core.Status != session.StatusIdle || projection.ActiveTurnID != "" || projection.ActiveStepClaimID != "" {
		t.Fatalf("active step was not repaired: core=%+v projection=%+v", core, projection)
	}
	if got := projection.Queue(InboxNextStep); len(got) != 1 || got[0].InputID != injected.InputID {
		t.Fatalf("next-step queue changed during repair: %+v", got)
	}
	if got := projection.Queue(InboxNextTurn); len(got) != 1 || got[0].InputID != queued.InputID {
		t.Fatalf("next-turn queue changed during repair: %+v", got)
	}
	if persisted, exists := projection.Claim(claim.ClaimID); !exists || !reflect.DeepEqual(persisted.OrderedInputIDs, []string{initial.InputID}) {
		t.Fatalf("active step claim history changed during repair: %+v", persisted)
	}
	events := store.snapshot()
	if len(events) < 3 || events[len(events)-3].EventType != "step/end" || events[len(events)-2].EventType != "turn/end" || events[len(events)-1].EventType != session.EventSessionRepaired {
		t.Fatalf("active step repair tail = %+v", events)
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
		t.Fatalf("active step repair was not idempotent: event count = %d, want %d", got, repairedEventCount)
	}
}

func claimTestEvent(eventID, eventType, turnID, stepID string, data json.RawMessage) session.NewEvent {
	return session.NewEvent{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     eventType,
		EventID:       eventID,
		OccurredAt:    time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC),
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
		TurnID:        turnID,
		StepID:        stepID,
	}
}
