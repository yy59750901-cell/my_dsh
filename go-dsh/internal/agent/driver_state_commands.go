package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

// StartClaimedStepCommand atomically consumes the pending claim into a concrete
// Step boundary. Expected fields fence commands created from stale snapshots.
type StartClaimedStepCommand struct {
	ExpectedTurnID    string
	ExpectedClaimID   string
	ExpectedStepIndex uint32
	StepID            string
	EventID           string
	OccurredAt        time.Time
}

func (command StartClaimedStepCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	if command.ExpectedTurnID == "" || command.ExpectedClaimID == "" || command.ExpectedStepIndex == 0 || command.StepID == "" || command.EventID == "" || command.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: complete step identity and event metadata are required", ErrInvalidDriverTransition)
	}
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, fmt.Errorf("%w: agent projection is missing", ErrInvalidDriverTransition)
	}
	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.ExpectedTurnID || core.StepID != "" || projection.ActiveTurnID != command.ExpectedTurnID || projection.PendingClaimID != command.ExpectedClaimID || projection.ActiveStepID != "" || projection.ActiveStepClaimID != "" {
		return nil, fmt.Errorf("%w: claimed step boundary changed", ErrStaleDriverActivity)
	}
	claim, exists := projection.Claim(command.ExpectedClaimID)
	if !exists || claim.TurnID != command.ExpectedTurnID || claim.ProposedStepIndex != command.ExpectedStepIndex {
		return nil, fmt.Errorf("%w: pending claim no longer matches step proposal", ErrStaleDriverActivity)
	}
	data, err := json.Marshal(StepStartPayload{ClaimID: command.ExpectedClaimID, StepIndex: command.ExpectedStepIndex})
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "step/start",
		EventID:       command.EventID,
		OccurredAt:    command.OccurredAt.UTC(),
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
		TurnID:        command.ExpectedTurnID,
		StepID:        command.StepID,
	}}, nil
}

// StartInitialAttemptCommand opens attempt 1 for the current Step. Retry
// transitions are intentionally separate and will be added with RetryPolicy.
type StartInitialAttemptCommand struct {
	ExpectedTurnID    string
	ExpectedStepID    string
	ExpectedStepIndex uint32
	ExpectedAttempt   uint32
	ExpectedPhase     AttemptPhase
	Model             string
	RequestSummary    json.RawMessage
	EventID           string
	OccurredAt        time.Time
}

func (command StartInitialAttemptCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	if command.ExpectedTurnID == "" || command.ExpectedStepID == "" || command.ExpectedStepIndex == 0 || command.ExpectedAttempt != 0 || command.ExpectedPhase != AttemptPhaseNone || strings.TrimSpace(command.Model) == "" || command.EventID == "" || command.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: invalid initial attempt proposal", ErrInvalidDriverTransition)
	}
	if len(command.RequestSummary) != 0 {
		if err := validateCanonicalJSONValue(command.RequestSummary); err != nil {
			return nil, fmt.Errorf("%w: invalid model request summary: %v", ErrInvalidDriverTransition, err)
		}
	}
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, fmt.Errorf("%w: agent projection is missing", ErrInvalidDriverTransition)
	}
	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.ExpectedTurnID || core.StepID != command.ExpectedStepID || projection.ActiveTurnID != command.ExpectedTurnID || projection.ActiveStepID != command.ExpectedStepID || projection.ActiveStepIndex != command.ExpectedStepIndex || projection.ActiveAttempt != command.ExpectedAttempt || projection.AttemptPhase != command.ExpectedPhase {
		return nil, fmt.Errorf("%w: active step or attempt boundary changed", ErrStaleDriverActivity)
	}
	data, err := json.Marshal(ModelRequestedPayload{
		Attempt:        1,
		Model:          command.Model,
		RequestSummary: append(json.RawMessage(nil), command.RequestSummary...),
	})
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     EventModelRequested,
		EventID:       command.EventID,
		OccurredAt:    command.OccurredAt.UTC(),
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
		TurnID:        command.ExpectedTurnID,
		StepID:        command.ExpectedStepID,
	}}, nil
}

// FinishActiveStepCommand is reserved for explicit synthetic interruption.
// Normal model terminals must use CommitAttemptTerminalCommand so durable
// chunks, message/error facts, usage, and step/end remain one checked closure.
type FinishActiveStepCommand struct {
	ExpectedTurnID    string
	ExpectedStepID    string
	ExpectedStepIndex uint32
	ExpectedAttempt   uint32
	ExpectedPhase     AttemptPhase
	Reason            StepEndReason
	EventID           string
	OccurredAt        time.Time
}

func (command FinishActiveStepCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	if command.ExpectedTurnID == "" || command.ExpectedStepID == "" || command.ExpectedStepIndex == 0 || command.ExpectedAttempt == 0 || command.ExpectedPhase == AttemptPhaseNone || command.Reason != StepEndInterrupted || command.EventID == "" || command.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: only synthetic interrupted step closure is allowed", ErrInvalidDriverTransition)
	}
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, fmt.Errorf("%w: agent projection is missing", ErrInvalidDriverTransition)
	}
	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.ExpectedTurnID || core.StepID != command.ExpectedStepID || projection.ActiveTurnID != command.ExpectedTurnID || projection.ActiveStepID != command.ExpectedStepID || projection.ActiveStepIndex != command.ExpectedStepIndex || projection.ActiveAttempt != command.ExpectedAttempt || projection.AttemptPhase != command.ExpectedPhase {
		return nil, fmt.Errorf("%w: active step or attempt boundary changed", ErrStaleDriverActivity)
	}
	data, err := json.Marshal(StepEndPayload{Reason: command.Reason, Synthetic: true})
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "step/end",
		EventID:       command.EventID,
		OccurredAt:    command.OccurredAt.UTC(),
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
		TurnID:        command.ExpectedTurnID,
		StepID:        command.ExpectedStepID,
	}}, nil
}

// AssistantChunkPayload is the durable, provider-neutral audit record for one
// canonical LLM stream chunk. ProviderRaw is deliberately excluded by the LLM
// JSON contract so unredacted provider payloads never enter Session history.
type AssistantChunkPayload struct {
	Attempt    uint32          `json:"attempt"`
	ChunkIndex uint64          `json:"chunk_index"`
	Chunk      llm.StreamChunk `json:"chunk"`
}

func (payload AssistantChunkPayload) validate() error {
	if payload.Attempt == 0 {
		return errors.New("positive attempt is required")
	}
	if err := llm.ValidateStreamChunk(payload.Chunk); err != nil {
		return err
	}
	return nil
}

// AppendAssistantChunkCommand fences one chunk against the active logical
// attempt and advances its durable chunk cursor exactly once.
type AppendAssistantChunkCommand struct {
	ExpectedTurnID     string
	ExpectedStepID     string
	ExpectedStepIndex  uint32
	ExpectedAttempt    uint32
	ExpectedPhase      AttemptPhase
	ExpectedChunkIndex uint64
	Chunk              llm.StreamChunk
	EventID            string
	OccurredAt         time.Time
}

func (command AppendAssistantChunkCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	if command.ExpectedTurnID == "" || command.ExpectedStepID == "" || command.ExpectedStepIndex == 0 || command.ExpectedAttempt == 0 || command.ExpectedPhase == AttemptPhaseNone || command.EventID == "" || command.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: invalid assistant chunk proposal", ErrInvalidDriverTransition)
	}
	data, err := command.payloadData()
	if err != nil {
		return nil, err
	}
	payloadHash, err := canonicalPayloadHash(data)
	if err != nil {
		return nil, fmt.Errorf("hash assistant chunk: %w", err)
	}

	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, fmt.Errorf("%w: agent projection is missing", ErrInvalidDriverTransition)
	}
	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.ExpectedTurnID || core.StepID != command.ExpectedStepID || projection.ActiveTurnID != command.ExpectedTurnID || projection.ActiveStepID != command.ExpectedStepID || projection.ActiveStepIndex != command.ExpectedStepIndex || projection.ActiveAttempt != command.ExpectedAttempt || projection.AttemptPhase != command.ExpectedPhase {
		return nil, fmt.Errorf("%w: active step or attempt boundary changed", ErrStaleDriverActivity)
	}
	if projection.ActiveChunkCount != command.ExpectedChunkIndex {
		if projection.LastChunkEventID == command.EventID && projection.LastChunkIndex == command.ExpectedChunkIndex && projection.LastChunkPayloadHash == payloadHash {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: active chunk cursor changed", ErrStaleDriverActivity)
	}
	return []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     EventAssistantChunk,
		EventID:       command.EventID,
		OccurredAt:    command.OccurredAt.UTC(),
		ReplayPolicy:  session.ReplayIgnorable,
		Data:          data,
		TurnID:        command.ExpectedTurnID,
		StepID:        command.ExpectedStepID,
	}}, nil
}

func (command AppendAssistantChunkCommand) payloadData() (json.RawMessage, error) {
	payload := AssistantChunkPayload{Attempt: command.ExpectedAttempt, ChunkIndex: command.ExpectedChunkIndex, Chunk: command.Chunk}
	if err := payload.validate(); err != nil {
		return nil, fmt.Errorf("%w: invalid assistant chunk: %v", ErrInvalidDriverTransition, err)
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal assistant chunk: %w", err)
	}
	return data, nil
}

func canonicalPayloadHash(data json.RawMessage) (string, error) {
	canonical, err := canonicalizeJSON(data)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
