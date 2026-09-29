package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

const (
	testPayloadHashA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testPayloadHashB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestAgentProjectionReplaysInsertionAndDetachesCopies(t *testing.T) {
	event := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Start:          0,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	snapshot := replayAgentEvents(t, event)
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		t.Fatal("agent projection missing")
	}
	if len(projection.NextTurn) != 1 || len(projection.NextStep) != 0 {
		t.Fatalf("queues = next-turn:%d next-step:%d", len(projection.NextTurn), len(projection.NextStep))
	}
	item := projection.NextTurn[0]
	if item.RequestID != "request-1" || item.IdempotencyKey != "key-1" || item.AcceptedSeq != 1 || !item.AcceptedAt.Equal(event.CommittedAt) {
		t.Fatalf("committed acceptance fields = %+v", item)
	}
	accepted, ok := projection.AcceptedByIdempotency(CommandFollowUp, "key-1")
	if !ok {
		t.Fatal("idempotency index missing")
	}
	if accepted.Item.InputID != "input-1" || accepted.Receipt.Placement != "queued" || accepted.Receipt.CommandID != event.EventID {
		t.Fatalf("accepted input = %+v", accepted)
	}

	projection.NextTurn[0].Content[0].Text = "mutated"
	projection.NextTurn[0].Content[0].Data[0] = '['
	projection.NextTurn[0].Metadata["trace"][0] = '['
	fresh, _ := ProjectionFrom(snapshot)
	if fresh.NextTurn[0].Content[0].Text != "hello" || string(fresh.NextTurn[0].Content[0].Data) != `{"language":"zh"}` || string(fresh.NextTurn[0].Metadata["trace"]) != `{"enabled":true}` {
		t.Fatalf("projection copy mutation leaked into snapshot: %+v", fresh.NextTurn[0])
	}
}

func TestAgentProjectionRejectsOutOfBoundsSplice(t *testing.T) {
	event := testAgentEvent(t, EventInboxSpliced, 1, InboxSplicedPayload{
		Target:      InboxNextTurn,
		Start:       1,
		DeleteCount: 1,
		Reason:      InboxReasonCancel,
	})
	_, err := replayAgentEventsWithError(event)
	if !errors.Is(err, ErrInvalidInboxEvent) {
		t.Fatalf("error = %v, want ErrInvalidInboxEvent", err)
	}
}

func TestAgentProjectionPreservesHistoricalInputIDsAfterRemoval(t *testing.T) {
	insert := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	remove := testAgentEvent(t, EventInboxSpliced, 2, InboxSplicedPayload{
		Target:      InboxNextTurn,
		Start:       0,
		DeleteCount: 1,
		Reason:      InboxReasonCancel,
	})
	reinsert := testInboxInsertionEvent(t, 3, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-2",
		IdempotencyKey: "key-2",
		PayloadHash:    testPayloadHashB,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	_, err := replayAgentEventsWithError(insert, remove, reinsert)
	if !errors.Is(err, ErrDuplicateInput) {
		t.Fatalf("error = %v, want ErrDuplicateInput", err)
	}
}

func TestAgentProjectionRejectsIdempotencyConflictAndDuplicateAppend(t *testing.T) {
	first := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		IdempotencyKey: "same-key",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})

	t.Run("different payload hash", func(t *testing.T) {
		conflicting := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
			Target:         InboxNextTurn,
			Start:          1,
			Items:          []InboxItem{testInboxItem("input-2", InputSourceUser)},
			Reason:         InboxReasonFollowUp,
			CommandKind:    CommandFollowUp,
			RequestID:      "request-2",
			IdempotencyKey: "same-key",
			PayloadHash:    testPayloadHashB,
			OriginalTarget: InboxNextTurn,
			ActualTarget:   InboxNextTurn,
		})
		_, err := replayAgentEventsWithError(first, conflicting)
		if !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
		}
	})

	t.Run("same payload hash", func(t *testing.T) {
		duplicate := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
			Target:         InboxNextTurn,
			Start:          1,
			Items:          []InboxItem{testInboxItem("input-2", InputSourceUser)},
			Reason:         InboxReasonFollowUp,
			CommandKind:    CommandFollowUp,
			RequestID:      "request-2",
			IdempotencyKey: "same-key",
			PayloadHash:    testPayloadHashA,
			OriginalTarget: InboxNextTurn,
			ActualTarget:   InboxNextTurn,
		})
		_, err := replayAgentEventsWithError(first, duplicate)
		if !errors.Is(err, ErrDuplicateInput) {
			t.Fatalf("error = %v, want ErrDuplicateInput", err)
		}
	})
}

func TestAgentProjectionAllowsIndependentInputsWithoutIdempotencyKey(t *testing.T) {
	first := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	second := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Start:          1,
		Items:          []InboxItem{testInboxItem("input-2", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-2",
		PayloadHash:    testPayloadHashB,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	projection, _ := ProjectionFrom(replayAgentEvents(t, first, second))
	if len(projection.NextTurn) != 2 || len(projection.InputIDByIdemKey) != 0 {
		t.Fatalf("projection = %+v", projection)
	}
}

func TestAgentProjectionRejectsCommandSourceMismatch(t *testing.T) {
	event := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextStep,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceSystem)},
		Reason:         InboxReasonSteer,
		CommandKind:    CommandSteer,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextStep,
		ActualTarget:   InboxNextStep,
	})
	_, err := replayAgentEventsWithError(event)
	if !errors.Is(err, ErrInvalidInboxEvent) {
		t.Fatalf("error = %v, want ErrInvalidInboxEvent", err)
	}
}

func TestAgentProjectionRejectsNonTailInsertion(t *testing.T) {
	first := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	prepend := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Start:          0,
		Items:          []InboxItem{testInboxItem("input-2", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-2",
		IdempotencyKey: "key-2",
		PayloadHash:    testPayloadHashB,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	_, err := replayAgentEventsWithError(first, prepend)
	if !errors.Is(err, ErrInvalidInboxEvent) {
		t.Fatalf("error = %v, want ErrInvalidInboxEvent", err)
	}
}

func TestAgentProjectionClaimsDeletedInputsAtomically(t *testing.T) {
	nextStep := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextStep,
		Items:          []InboxItem{testInboxItem("input-step", InputSourceUser)},
		Reason:         InboxReasonSteer,
		CommandKind:    CommandSteer,
		RequestID:      "request-step",
		IdempotencyKey: "key-step",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextStep,
		ActualTarget:   InboxNextStep,
	})
	nextTurn := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{testInboxItem("input-turn", InputSourceUser)},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-turn",
		IdempotencyKey: "key-turn",
		PayloadHash:    testPayloadHashB,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	turnStart := testTurnEvent(t, 3, "turn/start", "turn-1")
	deleteStep := testAgentEvent(t, EventInboxSpliced, 4, InboxSplicedPayload{
		Target:      InboxNextStep,
		Start:       0,
		DeleteCount: 1,
		Reason:      InboxReasonClaim,
		ClaimID:     "claim-1",
	})
	deleteTurn := testAgentEvent(t, EventInboxSpliced, 5, InboxSplicedPayload{
		Target:      InboxNextTurn,
		Start:       0,
		DeleteCount: 1,
		Reason:      InboxReasonClaim,
		ClaimID:     "claim-1",
	})
	marker := testAgentEvent(t, EventInputClaimed, 6, InputClaimedPayload{
		ClaimID:           "claim-1",
		TurnID:            "turn-1",
		ProposedStepIndex: 1,
		OrderedInputIDs:   []string{"input-step", "input-turn"},
		SourceTargets:     []InboxTarget{InboxNextStep, InboxNextTurn},
	})
	marker.TurnID = "turn-1"

	snapshot := replayAgentEvents(t, nextStep, nextTurn, turnStart, deleteStep, deleteTurn, marker)
	projection, _ := ProjectionFrom(snapshot)
	if len(projection.NextStep) != 0 || len(projection.NextTurn) != 0 {
		t.Fatalf("claimed queues are not empty: next-step=%d next-turn=%d", len(projection.NextStep), len(projection.NextTurn))
	}
	claim, ok := projection.Claim("claim-1")
	if !ok {
		t.Fatal("claim missing")
	}
	if !reflect.DeepEqual(claim.OrderedInputIDs, []string{"input-step", "input-turn"}) || !reflect.DeepEqual(claim.SourceTargets, []InboxTarget{InboxNextStep, InboxNextTurn}) {
		t.Fatalf("claim ownership = %+v", claim)
	}
	if claim.ClaimedAtSeq != 6 || len(claim.Items) != 2 || claim.Items[0].InputID != "input-step" || claim.Items[1].InputID != "input-turn" {
		t.Fatalf("resolved claim = %+v", claim)
	}
	if _, ok := projection.Accepted("input-step"); !ok {
		t.Fatal("claim removed accepted input history")
	}
}

func TestAgentProjectionRejectsIncompleteOrMismatchedClaim(t *testing.T) {
	insert := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextStep,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceSystem)},
		Reason:         InboxReasonInject,
		CommandKind:    CommandInject,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextStep,
		ActualTarget:   InboxNextStep,
	})
	turnStart := testTurnEvent(t, 2, "turn/start", "turn-1")
	deletion := testAgentEvent(t, EventInboxSpliced, 3, InboxSplicedPayload{
		Target:      InboxNextStep,
		Start:       0,
		DeleteCount: 1,
		Reason:      InboxReasonClaim,
		ClaimID:     "claim-1",
	})

	t.Run("missing marker", func(t *testing.T) {
		_, err := replayAgentEventsWithError(insert, turnStart, deletion)
		if !errors.Is(err, ErrIncompleteInputClaim) {
			t.Fatalf("error = %v, want ErrIncompleteInputClaim", err)
		}
	})

	t.Run("marker order differs", func(t *testing.T) {
		marker := testAgentEvent(t, EventInputClaimed, 4, InputClaimedPayload{
			ClaimID:           "claim-1",
			TurnID:            "turn-1",
			ProposedStepIndex: 1,
			OrderedInputIDs:   []string{"another-input"},
			SourceTargets:     []InboxTarget{InboxNextStep},
		})
		marker.TurnID = "turn-1"
		_, err := replayAgentEventsWithError(insert, turnStart, deletion, marker)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})
}

func TestAgentProjectionRequiresInitialClaimInCommittedBatchButAllowsReplayRepair(t *testing.T) {
	spec := AgentSchemaContribution().Projections[0]
	state := spec.New()
	turnStart := testTurnEvent(t, 1, "turn/start", "turn-1")
	if err := spec.Apply(state, turnStart); err != nil {
		t.Fatal(err)
	}
	if err := spec.ValidateBoundary(state, session.BoundaryCandidateBatch); !errors.Is(err, ErrIncompleteInputClaim) {
		t.Fatalf("candidate boundary error = %v, want ErrIncompleteInputClaim", err)
	}
	if err := spec.ValidateBoundary(state, session.BoundaryReplayEnd); err != nil {
		t.Fatalf("replay boundary should leave incomplete turn for repair: %v", err)
	}

	intervening := testAgentEvent(t, "extension/ignorable", 2, struct{}{})
	intervening.ReplayPolicy = session.ReplayIgnorable
	if err := spec.Apply(state, intervening); !errors.Is(err, ErrInvalidInputClaim) {
		t.Fatalf("intervening event error = %v, want ErrInvalidInputClaim", err)
	}

	repairEnd := testTurnEvent(t, 2, "turn/end", "turn-1")
	repairEnd.Data = json.RawMessage(`{"reason":"interrupted","synthetic":true}`)
	if err := spec.Apply(state, repairEnd); err != nil {
		t.Fatalf("synthetic repair turn/end failed: %v", err)
	}
	if err := spec.ValidateBoundary(state, session.BoundaryCommittedBatch); err != nil {
		t.Fatalf("repaired boundary failed: %v", err)
	}
}

func TestAgentProjectionRejectsOrphanAndInterruptedClaims(t *testing.T) {
	insert := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextStep,
		Items:          []InboxItem{testInboxItem("input-1", InputSourceSystem)},
		Reason:         InboxReasonInject,
		CommandKind:    CommandInject,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextStep,
		ActualTarget:   InboxNextStep,
	})

	t.Run("orphan turn", func(t *testing.T) {
		deletion := testAgentEvent(t, EventInboxSpliced, 2, InboxSplicedPayload{
			Target: InboxNextStep, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
		})
		_, err := replayAgentEventsWithError(insert, deletion)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})

	t.Run("intervening event", func(t *testing.T) {
		turnStart := testTurnEvent(t, 2, "turn/start", "turn-1")
		deletion := testAgentEvent(t, EventInboxSpliced, 3, InboxSplicedPayload{
			Target: InboxNextStep, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
		})
		intervening := testAgentEvent(t, "extension/ignorable", 4, struct{}{})
		intervening.ReplayPolicy = session.ReplayIgnorable
		_, err := replayAgentEventsWithError(insert, turnStart, deletion, intervening)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})
}

func TestAgentProjectionRejectsPartialOrReorderedClaim(t *testing.T) {
	firstStep := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target: InboxNextStep, Items: []InboxItem{testInboxItem("input-step-1", InputSourceSystem)},
		Reason: InboxReasonInject, CommandKind: CommandInject, RequestID: "request-1", IdempotencyKey: "key-1",
		PayloadHash: testPayloadHashA, OriginalTarget: InboxNextStep, ActualTarget: InboxNextStep,
	})
	secondStep := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
		Target: InboxNextStep, Start: 1, Items: []InboxItem{testInboxItem("input-step-2", InputSourceSystem)},
		Reason: InboxReasonInject, CommandKind: CommandInject, RequestID: "request-2", IdempotencyKey: "key-2",
		PayloadHash: testPayloadHashB, OriginalTarget: InboxNextStep, ActualTarget: InboxNextStep,
	})
	turnStart := testTurnEvent(t, 3, "turn/start", "turn-1")
	partial := testAgentEvent(t, EventInboxSpliced, 4, InboxSplicedPayload{
		Target: InboxNextStep, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
	})
	_, err := replayAgentEventsWithError(firstStep, secondStep, turnStart, partial)
	if !errors.Is(err, ErrInvalidInputClaim) {
		t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
	}
}

func TestAgentProjectionEnforcesInitialAndSubsequentClaimShapes(t *testing.T) {
	t.Run("initial claim requires next-turn", func(t *testing.T) {
		nextStep := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
			Target: InboxNextStep, Items: []InboxItem{testInboxItem("input-step", InputSourceSystem)},
			Reason: InboxReasonInject, CommandKind: CommandInject, RequestID: "request-step",
			PayloadHash: testPayloadHashA, OriginalTarget: InboxNextStep, ActualTarget: InboxNextStep,
		})
		turnStart := testTurnEvent(t, 2, "turn/start", "turn-1")
		deletion := testAgentEvent(t, EventInboxSpliced, 3, InboxSplicedPayload{
			Target: InboxNextStep, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
		})
		marker := testAgentEvent(t, EventInputClaimed, 4, InputClaimedPayload{
			ClaimID: "claim-1", TurnID: "turn-1", ProposedStepIndex: 1,
			OrderedInputIDs: []string{"input-step"}, SourceTargets: []InboxTarget{InboxNextStep},
		})
		marker.TurnID = "turn-1"
		_, err := replayAgentEventsWithError(nextStep, turnStart, deletion, marker)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})

	t.Run("subsequent claim cannot consume next-turn", func(t *testing.T) {
		first := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
			Target: InboxNextTurn, Items: []InboxItem{testInboxItem("input-turn-1", InputSourceUser)},
			Reason: InboxReasonFollowUp, CommandKind: CommandFollowUp, RequestID: "request-1",
			PayloadHash: testPayloadHashA, OriginalTarget: InboxNextTurn, ActualTarget: InboxNextTurn,
		})
		second := testInboxInsertionEvent(t, 2, InboxSplicedPayload{
			Target: InboxNextTurn, Start: 1, Items: []InboxItem{testInboxItem("input-turn-2", InputSourceUser)},
			Reason: InboxReasonFollowUp, CommandKind: CommandFollowUp, RequestID: "request-2",
			PayloadHash: testPayloadHashB, OriginalTarget: InboxNextTurn, ActualTarget: InboxNextTurn,
		})
		turnStart := testTurnEvent(t, 3, "turn/start", "turn-1")
		deletion := testAgentEvent(t, EventInboxSpliced, 4, InboxSplicedPayload{
			Target: InboxNextTurn, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
		})
		marker := testAgentEvent(t, EventInputClaimed, 5, InputClaimedPayload{
			ClaimID: "claim-1", TurnID: "turn-1", ProposedStepIndex: 1,
			OrderedInputIDs: []string{"input-turn-1"}, SourceTargets: []InboxTarget{InboxNextTurn},
		})
		marker.TurnID = "turn-1"
		stepStart := testAgentEvent(t, "step/start", 6, StepStartPayload{ClaimID: "claim-1", StepIndex: 1})
		stepStart.TurnID, stepStart.StepID = "turn-1", "step-1"
		stepEnd := testAgentEvent(t, "step/end", 7, StepEndPayload{Reason: StepEndCompleted})
		stepEnd.TurnID, stepEnd.StepID = "turn-1", "step-1"
		invalidDeletion := testAgentEvent(t, EventInboxSpliced, 8, InboxSplicedPayload{
			Target: InboxNextTurn, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-2",
		})
		_, err := replayAgentEventsWithError(first, second, turnStart, deletion, marker, stepStart, stepEnd, invalidDeletion)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})
}

func TestAgentProjectionRequiresClaimCredentialsForRepairAndCancel(t *testing.T) {
	first := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target: InboxNextTurn, Items: []InboxItem{testInboxItem("input-turn", InputSourceUser)},
		Reason: InboxReasonFollowUp, CommandKind: CommandFollowUp, RequestID: "request-1",
		PayloadHash: testPayloadHashA, OriginalTarget: InboxNextTurn, ActualTarget: InboxNextTurn,
	})
	turnStart := testTurnEvent(t, 2, "turn/start", "turn-1")
	deletion := testAgentEvent(t, EventInboxSpliced, 3, InboxSplicedPayload{
		Target: InboxNextTurn, Start: 0, DeleteCount: 1, Reason: InboxReasonClaim, ClaimID: "claim-1",
	})
	marker := testAgentEvent(t, EventInputClaimed, 4, InputClaimedPayload{
		ClaimID: "claim-1", TurnID: "turn-1", ProposedStepIndex: 1,
		OrderedInputIDs: []string{"input-turn"}, SourceTargets: []InboxTarget{InboxNextTurn},
	})
	marker.TurnID = "turn-1"

	t.Run("synthetic turn end", func(t *testing.T) {
		turnEnd := testTurnEvent(t, 5, "turn/end", "turn-1")
		turnEnd.Data = json.RawMessage(`{"reason":"interrupted","synthetic":true}`)
		_, err := replayAgentEventsWithError(first, turnStart, deletion, marker, turnEnd)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})

	t.Run("session cancelled", func(t *testing.T) {
		cancelled := testAgentEvent(t, "session/cancelled", 5, struct{}{})
		_, err := replayAgentEventsWithError(first, turnStart, deletion, marker, cancelled)
		if !errors.Is(err, ErrInvalidInputClaim) {
			t.Fatalf("error = %v, want ErrInvalidInputClaim", err)
		}
	})
}

func TestAgentProjectionRejectsCommitOwnedAcceptanceFields(t *testing.T) {
	item := testInboxItem("input-1", InputSourceUser)
	item.AcceptedSeq = 99
	event := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
		Target:         InboxNextTurn,
		Items:          []InboxItem{item},
		Reason:         InboxReasonFollowUp,
		CommandKind:    CommandFollowUp,
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		PayloadHash:    testPayloadHashA,
		OriginalTarget: InboxNextTurn,
		ActualTarget:   InboxNextTurn,
	})
	_, err := replayAgentEventsWithError(event)
	if !errors.Is(err, ErrInvalidInboxEvent) {
		t.Fatalf("error = %v, want ErrInvalidInboxEvent", err)
	}
}

func TestAgentProjectionEnforcesJSONNumberRangeOnReplay(t *testing.T) {
	tests := []struct {
		name     string
		data     json.RawMessage
		metadata json.RawMessage
		wantErr  bool
	}{
		{name: "safe boundaries", data: json.RawMessage(`{"values":[9007199254740991,-9007199254740991]}`), metadata: json.RawMessage(`0.1`)},
		{name: "unsafe nested content", data: json.RawMessage(`{"values":[{"id":9007199254740992.1}]}`), metadata: json.RawMessage(`true`), wantErr: true},
		{name: "unsafe metadata", data: json.RawMessage(`{"ok":true}`), metadata: json.RawMessage(`-9007199254740992`), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			item := testInboxItem("input-1", InputSourceSystem)
			item.Content[0].Data = test.data
			item.Metadata["trace"] = test.metadata
			event := testInboxInsertionEvent(t, 1, InboxSplicedPayload{
				Target:         InboxNextStep,
				Items:          []InboxItem{item},
				Reason:         InboxReasonInject,
				CommandKind:    CommandInject,
				RequestID:      "request-1",
				PayloadHash:    testPayloadHashA,
				OriginalTarget: InboxNextStep,
				ActualTarget:   InboxNextStep,
			})
			_, err := replayAgentEventsWithError(event)
			if test.wantErr && !errors.Is(err, ErrInvalidInboxEvent) {
				t.Fatalf("error = %v, want ErrInvalidInboxEvent", err)
			}
			if !test.wantErr && err != nil {
				t.Fatal(err)
			}
		})
	}
}

func replayAgentEvents(t *testing.T, events ...session.Event) session.Snapshot {
	t.Helper()
	snapshot, err := replayAgentEventsWithError(events...)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func replayAgentEventsWithError(events ...session.Event) (session.Snapshot, error) {
	schema, err := session.NewSessionSchema(1, session.CoreSchemaContribution(), session.SurfaceSchemaContribution(), AgentSchemaContribution())
	if err != nil {
		return session.Snapshot{}, err
	}
	return session.Replay(context.Background(), staticAgentEventStore{events: events}, "session-1", 1, uint64(len(events)), session.ReplayOptions{Schema: schema, PageSize: 1})
}

func testInboxInsertionEvent(t *testing.T, seq uint64, payload InboxSplicedPayload) session.Event {
	t.Helper()
	payload.DeleteCount = 0
	return testAgentEvent(t, EventInboxSpliced, seq, payload)
}

func testTurnEvent(t *testing.T, seq uint64, eventType, turnID string) session.Event {
	t.Helper()
	event := testAgentEvent(t, eventType, seq, struct{}{})
	event.TurnID = turnID
	return event
}

func testAgentEvent(t *testing.T, eventType string, seq uint64, payload any) session.Event {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 26, 14, 0, int(seq), 0, time.UTC)
	return session.Event{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     eventType,
		EventID:       "event-" + time.Unix(int64(seq), 0).UTC().Format("150405"),
		SessionID:     "session-1",
		Seq:           seq,
		OccurredAt:    at,
		CommittedAt:   at.Add(time.Second),
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
	}
}

func testInboxItem(inputID string, source InputSource) InboxItem {
	return InboxItem{
		InputID: inputID,
		Source:  source,
		Content: []ContentBlock{{
			Type: "text",
			Text: "hello",
			Data: json.RawMessage(`{"language":"zh"}`),
		}},
		Metadata: map[string]json.RawMessage{
			"trace": json.RawMessage(`{"enabled":true}`),
		},
	}
}

type staticAgentEventStore struct {
	events []session.Event
}

func (store staticAgentEventStore) Create(context.Context, session.NewSession) error {
	return nil
}

func (store staticAgentEventStore) ClaimWriter(context.Context, string) (uint64, uint64, error) {
	return 1, uint64(len(store.events)), nil
}

func (store staticAgentEventStore) Append(context.Context, string, uint64, uint64, []session.NewEvent) ([]session.Event, error) {
	return nil, errors.New("append is not supported by staticAgentEventStore")
}

func (store staticAgentEventStore) Load(_ context.Context, _ string, afterSeq uint64, limit int) ([]session.Event, error) {
	if afterSeq >= uint64(len(store.events)) {
		return nil, nil
	}
	end := int(afterSeq) + limit
	if end > len(store.events) {
		end = len(store.events)
	}
	return append([]session.Event(nil), store.events[int(afterSeq):end]...), nil
}

func (store staticAgentEventStore) Head(context.Context, string) (uint64, error) {
	return uint64(len(store.events)), nil
}
