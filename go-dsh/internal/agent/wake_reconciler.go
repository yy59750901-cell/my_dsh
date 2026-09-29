package agent

import (
	"context"
	"errors"
	"fmt"
)

const defaultWakeReconcilePageSize = 100

type ResumableSessionPage struct {
	SessionIDs []string
	NextCursor string
}

type SessionCatalog interface {
	ListResumableSessionIDs(context.Context, string, int) (ResumableSessionPage, error)
}

type SessionCatalogFunc func(context.Context, string, int) (ResumableSessionPage, error)

func (function SessionCatalogFunc) ListResumableSessionIDs(ctx context.Context, cursor string, limit int) (ResumableSessionPage, error) {
	return function(ctx, cursor, limit)
}

type WakeRequirement struct {
	NextTurnItems     int
	NextStepItems     int
	UserSteeringItems int
}

func (requirement WakeRequirement) ShouldWake() bool {
	return requirement.NextTurnItems > 0 || requirement.UserSteeringItems > 0
}

type WakeInspector interface {
	InspectWakeRequirement(context.Context, string) (WakeRequirement, error)
}

type WakeInspectorFunc func(context.Context, string) (WakeRequirement, error)

func (function WakeInspectorFunc) InspectWakeRequirement(ctx context.Context, sessionID string) (WakeRequirement, error) {
	return function(ctx, sessionID)
}

type WakeRequester interface {
	RequestWake(string) error
}

type WakeReconcilerOptions struct {
	PageSize int
}

type WakeReconciler struct {
	catalog   SessionCatalog
	inspector WakeInspector
	runtime   WakeRequester
	pageSize  int
}

func NewWakeReconciler(catalog SessionCatalog, inspector WakeInspector, runtime WakeRequester, options WakeReconcilerOptions) *WakeReconciler {
	if catalog == nil {
		panic("agent SessionCatalog is required")
	}
	if inspector == nil {
		panic("agent WakeInspector is required")
	}
	if runtime == nil {
		panic("agent WakeRequester is required")
	}
	pageSize := options.PageSize
	if pageSize <= 0 {
		pageSize = defaultWakeReconcilePageSize
	}
	return &WakeReconciler{
		catalog:   catalog,
		inspector: inspector,
		runtime:   runtime,
		pageSize:  pageSize,
	}
}

// Run performs a one-shot, read-only scan of resumable Sessions. Inspectors
// must use read-only Replay and must not claim a Session writer or create an
// Actor. A failure for one Session is aggregated while remaining Sessions are
// still inspected.
func (reconciler *WakeReconciler) Run(ctx context.Context) error {
	cursor := ""
	var reconcileErr error
	for {
		page, err := reconciler.catalog.ListResumableSessionIDs(ctx, cursor, reconciler.pageSize)
		if err != nil {
			return errors.Join(reconcileErr, fmt.Errorf("list resumable Sessions after %q: %w", cursor, err))
		}
		for _, sessionID := range page.SessionIDs {
			if err := ctx.Err(); err != nil {
				return errors.Join(reconcileErr, err)
			}
			if sessionID == "" {
				reconcileErr = errors.Join(reconcileErr, errors.New("inspect resumable Session: empty session id"))
				continue
			}
			requirement, err := reconciler.inspector.InspectWakeRequirement(ctx, sessionID)
			if err != nil {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("inspect resumable Session %q: %w", sessionID, err))
				continue
			}
			if !requirement.ShouldWake() {
				continue
			}
			if err := reconciler.runtime.RequestWake(sessionID); err != nil {
				reconcileErr = errors.Join(reconcileErr, fmt.Errorf("wake resumable Session %q: %w", sessionID, err))
			}
		}
		if page.NextCursor == "" {
			return reconcileErr
		}
		if page.NextCursor == cursor {
			return errors.Join(reconcileErr, fmt.Errorf("list resumable Sessions: cursor did not advance from %q", cursor))
		}
		cursor = page.NextCursor
	}
}
