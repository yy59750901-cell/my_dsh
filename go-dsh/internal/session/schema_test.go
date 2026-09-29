package session

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

type sequenceProjection struct {
	applied    []uint64
	boundaries []ProjectionBoundary
}

func sequenceProjectionSpec(key ProjectionKey, order int, trace *[]string, boundaryTrace ...*[]ProjectionBoundary) ProjectionSpec {
	return ProjectionSpec{
		Key:   key,
		Order: order,
		New:   func() any { return new(sequenceProjection) },
		Clone: func(state any) any {
			current := state.(*sequenceProjection)
			return &sequenceProjection{
				applied:    append([]uint64(nil), current.applied...),
				boundaries: append([]ProjectionBoundary(nil), current.boundaries...),
			}
		},
		Apply: func(state any, event Event) error {
			projection := state.(*sequenceProjection)
			projection.applied = append(projection.applied, event.Seq)
			if trace != nil {
				*trace = append(*trace, string(key))
			}
			return nil
		},
		ValidateBoundary: func(state any, boundary ProjectionBoundary) error {
			projection := state.(*sequenceProjection)
			projection.boundaries = append(projection.boundaries, boundary)
			if len(boundaryTrace) != 0 && boundaryTrace[0] != nil {
				*boundaryTrace[0] = append(*boundaryTrace[0], boundary)
			}
			return nil
		},
	}
}

func TestNewSessionSchemaRejectsDuplicateDefinitions(t *testing.T) {
	projection := sequenceProjectionSpec("one", 1, nil)
	_, err := NewSessionSchema(1,
		SchemaContribution{Name: "first", Events: []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}}, Projections: []ProjectionSpec{projection}},
		SchemaContribution{Name: "second", Events: []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}}, Projections: []ProjectionSpec{sequenceProjectionSpec("two", 2, nil)}},
	)
	if !errors.Is(err, ErrInvalidSessionSchema) {
		t.Fatalf("error = %v, want ErrInvalidSessionSchema", err)
	}
}

func TestSessionSchemaUsesStableProjectionOrder(t *testing.T) {
	var trace []string
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name:   "ordered",
		Events: []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}},
		Projections: []ProjectionSpec{
			sequenceProjectionSpec("z-last", 20, &trace),
			sequenceProjectionSpec("b-second", 10, &trace),
			sequenceProjectionSpec("a-first", 10, &trace),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := schema.newSnapshot("session-1", 1)
	_, err = schema.foldBatch(snapshot, []Event{testSchemaEvent("event/one", ReplayRequired, 1)}, BoundaryCommittedBatch, phaseCommitted)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"a-first", "b-second", "z-last"}; !reflect.DeepEqual(trace, want) {
		t.Fatalf("projection order = %v, want %v", trace, want)
	}
}

func TestReplayValidatesBoundaryOnlyAtReplayEnd(t *testing.T) {
	var boundaries []ProjectionBoundary
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name:        "replay",
		Events:      []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}},
		Projections: []ProjectionSpec{sequenceProjectionSpec("sequence", 1, nil, &boundaries)},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := replaySurfaceStore{events: []Event{
		testSchemaEvent("event/one", ReplayRequired, 1),
		testSchemaEvent("event/one", ReplayRequired, 2),
	}}
	snapshot, err := Replay(context.Background(), store, "session-1", 1, 2, ReplayOptions{Schema: schema, PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	projection, ok := ProjectionAs[*sequenceProjection](snapshot, "sequence")
	if !ok {
		t.Fatal("sequence projection missing")
	}
	if want := []uint64{1, 2}; !reflect.DeepEqual(projection.applied, want) {
		t.Fatalf("applied = %v, want %v", projection.applied, want)
	}
	if want := []ProjectionBoundary{BoundaryReplayEnd}; !reflect.DeepEqual(boundaries, want) {
		t.Fatalf("boundaries = %v, want %v", boundaries, want)
	}
	if len(projection.boundaries) != 0 {
		t.Fatalf("boundary validation mutated installed projection: %v", projection.boundaries)
	}
}

func TestUnknownIgnorableReachesEveryProjection(t *testing.T) {
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name:        "extension-aware",
		Events:      []EventDefinition{{EventType: "event/known", ReplayPolicy: ReplayRequired}},
		Projections: []ProjectionSpec{sequenceProjectionSpec("sequence", 1, nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := replaySurfaceStore{events: []Event{testSchemaEvent("extension/unknown", ReplayIgnorable, 1)}}
	snapshot, err := Replay(context.Background(), store, "session-1", 1, 1, ReplayOptions{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	projection, _ := ProjectionAs[*sequenceProjection](snapshot, "sequence")
	if !reflect.DeepEqual(projection.applied, []uint64{1}) {
		t.Fatalf("unknown ignorable event was not projected: %v", projection.applied)
	}
}

func TestKnownEventReplayPolicyMustMatchDefinition(t *testing.T) {
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name:        "strict-policy",
		Events:      []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}},
		Projections: []ProjectionSpec{sequenceProjectionSpec("sequence", 1, nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := replaySurfaceStore{events: []Event{testSchemaEvent("event/one", ReplayIgnorable, 1)}}
	if _, err := Replay(context.Background(), store, "session-1", 1, 1, ReplayOptions{Schema: schema}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("error = %v, want ErrInvalidEvent", err)
	}
}

func TestSnapshotCloneDetachesProjectionState(t *testing.T) {
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name:        "clone",
		Events:      []EventDefinition{{EventType: "event/one", ReplayPolicy: ReplayRequired}},
		Projections: []ProjectionSpec{sequenceProjectionSpec("sequence", 1, nil)},
	})
	if err != nil {
		t.Fatal(err)
	}
	original, err := schema.foldBatch(schema.newSnapshot("session-1", 1), []Event{testSchemaEvent("event/one", ReplayRequired, 1)}, BoundaryCommittedBatch, phaseCommitted)
	if err != nil {
		t.Fatal(err)
	}
	cloned := original.Clone()
	clonedProjection, _ := ProjectionAs[*sequenceProjection](cloned, "sequence")
	clonedProjection.applied[0] = 99
	originalProjection, _ := ProjectionAs[*sequenceProjection](original, "sequence")
	if originalProjection.applied[0] != 1 {
		t.Fatalf("clone mutated original projection: %v", originalProjection.applied)
	}
}

func TestSchemaCallbacksReceiveIsolatedEvents(t *testing.T) {
	validator := CandidateEventValidatorFunc(func(event Event) error {
		event.Data[0] = '['
		return nil
	})
	type payloadProjection struct{ payload string }
	projection := func(key ProjectionKey, mutate bool) ProjectionSpec {
		return ProjectionSpec{
			Key:   key,
			Order: 1,
			New:   func() any { return new(payloadProjection) },
			Clone: func(state any) any {
				cloned := *state.(*payloadProjection)
				return &cloned
			},
			Apply: func(state any, event Event) error {
				state.(*payloadProjection).payload = string(event.Data)
				if mutate {
					event.Data[0] = '['
				}
				return nil
			},
			ValidateBoundary: func(any, ProjectionBoundary) error { return nil },
		}
	}
	schema, err := NewSessionSchema(1, SchemaContribution{
		Name: "isolated-callbacks",
		Events: []EventDefinition{{
			EventType: "event/one", ReplayPolicy: ReplayRequired, CandidateValidators: []CandidateEventValidator{validator},
		}},
		Projections: []ProjectionSpec{projection("first", true), projection("second", false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	event := testSchemaEvent("event/one", ReplayRequired, 1)
	event.Data = json.RawMessage(`{"value":1}`)
	folded, err := schema.foldBatch(schema.newSnapshot("session-1", 1), []Event{event}, BoundaryCandidateBatch, phaseCandidate)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := ProjectionAs[*payloadProjection](folded, "second")
	if second.payload != `{"value":1}` {
		t.Fatalf("callback mutation leaked to later projector: %q", second.payload)
	}
	if string(event.Data) != `{"value":1}` {
		t.Fatalf("callback mutation leaked to caller: %q", event.Data)
	}
}

func TestCoreProjectionRejectsInvalidTransitions(t *testing.T) {
	schema := DefaultSessionSchema()
	invalidTurn := testSchemaEvent("turn/start", ReplayRequired, 1)
	if _, err := schema.foldBatch(schema.newSnapshot("session-1", 1), []Event{invalidTurn}, BoundaryCandidateBatch, phaseCandidate); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("empty turn id error = %v, want ErrInvalidEvent", err)
	}

	turn := testSchemaEvent("turn/start", ReplayRequired, 1)
	turn.TurnID = "turn-1"
	duplicate := testSchemaEvent("turn/start", ReplayRequired, 2)
	duplicate.TurnID = "turn-2"
	if _, err := schema.foldBatch(schema.newSnapshot("session-1", 1), []Event{turn, duplicate}, BoundaryCandidateBatch, phaseCandidate); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("duplicate turn error = %v, want ErrInvalidEvent", err)
	}
}

func testSchemaEvent(eventType string, policy ReplayPolicy, seq uint64) Event {
	return Event{
		SchemaVersion: SchemaVersion{Major: 1},
		EventType:     eventType,
		EventID:       "event-id",
		SessionID:     "session-1",
		Seq:           seq,
		ReplayPolicy:  policy,
	}
}
