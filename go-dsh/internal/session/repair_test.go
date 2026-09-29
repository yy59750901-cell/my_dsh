package session_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yy59750901/go-dsh/internal/session"
)

func TestLoadActorRepairsIncompleteCoreBoundariesBeforeAdmission(t *testing.T) {
	store := &memoryEventStore{events: []session.Event{
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "turn/start",
			EventID:       "turn-start",
			SessionID:     "session-1",
			Seq:           1,
			ReplayPolicy:  session.ReplayRequired,
			TurnID:        "turn-1",
		},
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "step/start",
			EventID:       "step-start",
			SessionID:     "session-1",
			Seq:           2,
			ReplayPolicy:  session.ReplayRequired,
			TurnID:        "turn-1",
			StepID:        "step-1",
		},
	}}

	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if actor.IsClosed() {
		t.Fatal("actor was not admitted after synchronous repair")
	}
	core := actor.Snapshot().Core()
	if core.Status != session.StatusIdle || core.TurnID != "" || core.StepID != "" {
		t.Fatalf("core was not repaired: %#v", core)
	}
	events := store.snapshotEvents()
	if len(events) != 5 {
		t.Fatalf("event count = %d, want 5", len(events))
	}
	if events[2].EventType != "step/end" || events[3].EventType != "turn/end" || events[4].EventType != session.EventSessionRepaired {
		t.Fatalf("unexpected repair batch: %s, %s, %s", events[2].EventType, events[3].EventType, events[4].EventType)
	}
	for _, event := range events[2:4] {
		var payload struct {
			Reason    string `json:"reason"`
			Synthetic bool   `json:"synthetic"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Reason != "interrupted" || !payload.Synthetic {
			t.Fatalf("invalid repair payload: %s", event.Data)
		}
	}
	if err := actor.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}

	reloaded, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reloaded.Dispose(context.Background())
	if got := len(store.snapshotEvents()); got != 5 {
		t.Fatalf("repair was not idempotent: event count = %d, want 5", got)
	}
}

func TestLoadActorDoesNotPublishBeforeInitializerSucceeds(t *testing.T) {
	store := new(memoryEventStore)
	initializerErr := context.Canceled
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{
		Initializers: []session.ActorInitializer{session.ActorInitializerFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			return nil, initializerErr
		})},
	})
	if actor != nil || err != initializerErr {
		t.Fatalf("actor=%v err=%v, want nil and initializer error", actor, err)
	}
}
