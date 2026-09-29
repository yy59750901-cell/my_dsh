package agent_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yy59750901/go-dsh/internal/agent"
)

type recordingWakeRequester struct {
	mu       sync.Mutex
	sessions []string
	errors   map[string]error
}

func (requester *recordingWakeRequester) RequestWake(sessionID string) error {
	requester.mu.Lock()
	defer requester.mu.Unlock()
	requester.sessions = append(requester.sessions, sessionID)
	return requester.errors[sessionID]
}

func (requester *recordingWakeRequester) requestedSessions() []string {
	requester.mu.Lock()
	defer requester.mu.Unlock()
	return append([]string(nil), requester.sessions...)
}

func TestWakeReconcilerPaginatesAndWakesOnlyRunnableInbox(t *testing.T) {
	var cursors []string
	catalog := agent.SessionCatalogFunc(func(_ context.Context, cursor string, limit int) (agent.ResumableSessionPage, error) {
		if limit != 2 {
			t.Fatalf("catalog limit = %d, want 2", limit)
		}
		cursors = append(cursors, cursor)
		switch cursor {
		case "":
			return agent.ResumableSessionPage{SessionIDs: []string{"followup", "inject-only"}, NextCursor: "inject-only"}, nil
		case "inject-only":
			return agent.ResumableSessionPage{SessionIDs: []string{"steer", "empty"}}, nil
		default:
			t.Fatalf("unexpected cursor %q", cursor)
			return agent.ResumableSessionPage{}, nil
		}
	})
	requirements := map[string]agent.WakeRequirement{
		"followup":    {NextTurnItems: 1},
		"inject-only": {NextStepItems: 2},
		"steer":       {NextStepItems: 1, UserSteeringItems: 1},
		"empty":       {},
	}
	inspector := agent.WakeInspectorFunc(func(_ context.Context, sessionID string) (agent.WakeRequirement, error) {
		return requirements[sessionID], nil
	})
	requester := &recordingWakeRequester{}
	reconciler := agent.NewWakeReconciler(catalog, inspector, requester, agent.WakeReconcilerOptions{PageSize: 2})

	if err := reconciler.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !reflect.DeepEqual(cursors, []string{"", "inject-only"}) {
		t.Fatalf("catalog cursors = %v", cursors)
	}
	if got := requester.requestedSessions(); !reflect.DeepEqual(got, []string{"followup", "steer"}) {
		t.Fatalf("requested Sessions = %v, want followup and steer", got)
	}
}

func TestWakeReconcilerContinuesAfterPerSessionFailures(t *testing.T) {
	inspectErr := errors.New("replay failed")
	wakeErr := errors.New("runtime closed")
	catalog := agent.SessionCatalogFunc(func(context.Context, string, int) (agent.ResumableSessionPage, error) {
		return agent.ResumableSessionPage{SessionIDs: []string{"bad-replay", "bad-wake", "good"}}, nil
	})
	inspector := agent.WakeInspectorFunc(func(_ context.Context, sessionID string) (agent.WakeRequirement, error) {
		if sessionID == "bad-replay" {
			return agent.WakeRequirement{}, inspectErr
		}
		return agent.WakeRequirement{NextTurnItems: 1}, nil
	})
	requester := &recordingWakeRequester{errors: map[string]error{"bad-wake": wakeErr}}
	reconciler := agent.NewWakeReconciler(catalog, inspector, requester, agent.WakeReconcilerOptions{})

	err := reconciler.Run(context.Background())
	if !errors.Is(err, inspectErr) || !errors.Is(err, wakeErr) {
		t.Fatalf("Run() error = %v, want both per-Session errors", err)
	}
	if got := requester.requestedSessions(); !reflect.DeepEqual(got, []string{"bad-wake", "good"}) {
		t.Fatalf("requested Sessions = %v", got)
	}
}

func TestWakeReconcilerRejectsNonAdvancingCursor(t *testing.T) {
	catalog := agent.SessionCatalogFunc(func(context.Context, string, int) (agent.ResumableSessionPage, error) {
		return agent.ResumableSessionPage{NextCursor: "same"}, nil
	})
	inspector := agent.WakeInspectorFunc(func(context.Context, string) (agent.WakeRequirement, error) {
		return agent.WakeRequirement{}, nil
	})
	reconciler := agent.NewWakeReconciler(catalog, inspector, &recordingWakeRequester{}, agent.WakeReconcilerOptions{})

	err := reconciler.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cursor did not advance") {
		t.Fatalf("Run() error = %v, want non-advancing cursor error", err)
	}
}

func TestWakeReconcilerStopsOnCatalogFailure(t *testing.T) {
	catalogErr := errors.New("catalog unavailable")
	catalog := agent.SessionCatalogFunc(func(context.Context, string, int) (agent.ResumableSessionPage, error) {
		return agent.ResumableSessionPage{}, catalogErr
	})
	inspector := agent.WakeInspectorFunc(func(context.Context, string) (agent.WakeRequirement, error) {
		return agent.WakeRequirement{}, nil
	})
	reconciler := agent.NewWakeReconciler(catalog, inspector, &recordingWakeRequester{}, agent.WakeReconcilerOptions{})

	if err := reconciler.Run(context.Background()); !errors.Is(err, catalogErr) {
		t.Fatalf("Run() error = %v, want %v", err, catalogErr)
	}
}
