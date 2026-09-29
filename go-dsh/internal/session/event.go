package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrNotFound        = errors.New("session not found")
	ErrInvalidSession  = errors.New("invalid session")
	ErrConflict        = errors.New("session event conflict")
	ErrWriterFenced    = errors.New("session writer fenced")
	ErrUnknownRequired = errors.New("unknown required session event")
)

type ReplayPolicy string

const (
	ReplayRequired  ReplayPolicy = "required"
	ReplayIgnorable ReplayPolicy = "ignorable"
)

type SchemaVersion struct {
	Major uint32 `json:"major"`
	Minor uint32 `json:"minor"`
}

type TraceContext struct {
	TraceID      string `json:"traceId,omitempty"`
	SpanID       string `json:"spanId,omitempty"`
	ParentSpanID string `json:"parentSpanId,omitempty"`
}

type Event struct {
	SchemaVersion    SchemaVersion   `json:"schemaVersion"`
	EventType        string          `json:"eventType"`
	EventID          string          `json:"eventId"`
	SessionID        string          `json:"sessionId"`
	Seq              uint64          `json:"seq,string"`
	OccurredAt       time.Time       `json:"occurredAt"`
	CommittedAt      time.Time       `json:"committedAt"`
	ReplayPolicy     ReplayPolicy    `json:"replayPolicy"`
	Data             json.RawMessage `json:"data"`
	TurnID           string          `json:"turnId,omitempty"`
	StepID           string          `json:"stepId,omitempty"`
	CallID           string          `json:"callId,omitempty"`
	Trace            TraceContext    `json:"trace,omitempty"`
	CausationEventID string          `json:"causationEventId,omitempty"`
	SourceEventSeqs  []uint64        `json:"sourceEventSeqs,omitempty"`
	SurfaceOp        *SurfaceOp      `json:"surfaceOp,omitempty"`
	Extensions       json.RawMessage `json:"extensions,omitempty"`
}

type NewEvent struct {
	SchemaVersion    SchemaVersion
	EventType        string
	EventID          string
	OccurredAt       time.Time
	ReplayPolicy     ReplayPolicy
	Data             json.RawMessage
	TurnID           string
	StepID           string
	CallID           string
	Trace            TraceContext
	CausationEventID string
	SourceEventSeqs  []uint64
	SurfaceOp        *SurfaceOp
	Extensions       json.RawMessage
}

func cloneEvent(event Event) Event {
	event.Data = cloneRawMessage(event.Data)
	event.SourceEventSeqs = cloneEventSeqs(event.SourceEventSeqs)
	event.SurfaceOp = cloneSurfaceOp(event.SurfaceOp)
	event.Extensions = cloneRawMessage(event.Extensions)
	return event
}

func cloneEvents(events []Event) []Event {
	if events == nil {
		return nil
	}
	cloned := make([]Event, len(events))
	for index, event := range events {
		cloned[index] = cloneEvent(event)
	}
	return cloned
}

type NewSession struct {
	ID          string
	TenantID    string
	WorkspaceID string
	ParentID    string
	ForkSeq     *uint64
	CreatedAt   time.Time
}

type EventStore interface {
	Create(ctx context.Context, newSession NewSession) error
	ClaimWriter(ctx context.Context, sessionID string) (epoch uint64, head uint64, err error)
	Append(ctx context.Context, sessionID string, epoch uint64, expectedSeq uint64, events []NewEvent) ([]Event, error)
	Load(ctx context.Context, sessionID string, afterSeq uint64, limit int) ([]Event, error)
	Head(ctx context.Context, sessionID string) (uint64, error)
}

func ValidateSequence(events []Event, afterSeq uint64) error {
	expected := afterSeq + 1
	for _, event := range events {
		if event.Seq != expected {
			return fmt.Errorf("%w: expected seq %d, got %d", ErrConflict, expected, event.Seq)
		}
		expected++
	}
	return nil
}

func ValidateReplayability(events []Event, knownEventTypes map[string]struct{}) error {
	for _, event := range events {
		if _, known := knownEventTypes[event.EventType]; known {
			continue
		}
		if event.ReplayPolicy == ReplayRequired {
			return fmt.Errorf("%w: %s at seq %d", ErrUnknownRequired, event.EventType, event.Seq)
		}
	}
	return nil
}
