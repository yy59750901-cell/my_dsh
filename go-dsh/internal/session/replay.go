package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	ErrUnsupportedSchema  = errors.New("unsupported session event schema")
	ErrReplayHeadMismatch = errors.New("replay head mismatch")
	ErrInvalidEvent       = errors.New("invalid session event")
)

const (
	StatusIdle      = "idle"
	StatusRunning   = "running"
	StatusCancelled = "cancelled"
)

func KnownEventTypes() map[string]struct{} {
	return DefaultSessionSchema().KnownEventTypes()
}

type Snapshot struct {
	SessionID string
	Epoch     uint64
	HeadSeq   uint64

	schema      *SessionSchema
	projections map[ProjectionKey]any
}

func (snapshot Snapshot) Clone() Snapshot {
	if snapshot.schema == nil {
		return snapshot
	}
	return snapshot.schema.cloneSnapshot(snapshot)
}

func (snapshot Snapshot) Core() CoreProjection {
	projection, ok := ProjectionAs[*CoreProjection](snapshot, CoreProjectionKey)
	if !ok || projection == nil {
		return CoreProjection{}
	}
	return *projection
}

func (snapshot Snapshot) Surface() Surface {
	projection, ok := ProjectionAs[*Surface](snapshot, SurfaceProjectionKey)
	if !ok || projection == nil {
		return Surface{}
	}
	return projection.Clone()
}

func (snapshot Snapshot) DeriveMessages() ([]json.RawMessage, error) {
	return snapshot.Surface().DeriveMessages()
}

// CandidateEventValidator can veto an event before persistence. Commit-owned
// fields such as CommittedAt are not populated at this boundary.
type CandidateEventValidator interface {
	ValidateCandidate(Event) error
}

type CandidateEventValidatorFunc func(Event) error

func (f CandidateEventValidatorFunc) ValidateCandidate(event Event) error {
	return f(event)
}

// EventValidator validates the exact event returned by durable storage. It runs
// both immediately after Append and during Replay.
type EventValidator interface {
	Validate(Event) error
}

type EventValidatorFunc func(Event) error

func (f EventValidatorFunc) Validate(event Event) error {
	return f(event)
}

type ReplayOptions struct {
	Schema   *SessionSchema
	PageSize int
}

func normalizeReplayOptions(options ReplayOptions) ReplayOptions {
	if options.Schema == nil {
		options.Schema = DefaultSessionSchema()
	}
	if options.PageSize <= 0 {
		options.PageSize = 256
	}
	return options
}

func Replay(ctx context.Context, store EventStore, sessionID string, epoch uint64, expectedHead uint64, options ReplayOptions) (Snapshot, error) {
	options = normalizeReplayOptions(options)
	snapshot := options.Schema.newSnapshot(sessionID, epoch)
	for snapshot.HeadSeq < expectedHead {
		events, err := store.Load(ctx, sessionID, snapshot.HeadSeq, options.PageSize)
		if err != nil {
			return Snapshot{}, err
		}
		if len(events) == 0 {
			return Snapshot{}, fmt.Errorf("%w: expected %d, reached %d", ErrReplayHeadMismatch, expectedHead, snapshot.HeadSeq)
		}
		if err := ValidateSequence(events, snapshot.HeadSeq); err != nil {
			return Snapshot{}, err
		}
		for _, event := range events {
			if event.SessionID != sessionID || event.Seq > expectedHead {
				return Snapshot{}, fmt.Errorf("%w: unexpected event session or seq", ErrConflict)
			}
		}
		snapshot, err = options.Schema.foldBatch(snapshot, events, "", phaseCommitted)
		if err != nil {
			return Snapshot{}, err
		}
	}
	if snapshot.HeadSeq != expectedHead {
		return Snapshot{}, fmt.Errorf("%w: expected %d, got %d", ErrReplayHeadMismatch, expectedHead, snapshot.HeadSeq)
	}
	if err := options.Schema.validateBoundary(snapshot, BoundaryReplayEnd); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}
