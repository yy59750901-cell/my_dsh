package agent

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsoncanonicalizer "github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
	"github.com/yy59750901/go-dsh/internal/session"
)

var (
	ErrInvalidInboxRequest            = errors.New("invalid agent inbox request")
	ErrInputPersistedSchedulingFailed = errors.New("agent input persisted but scheduling failed")
)

type SessionActorProvider interface {
	GetOrLoad(context.Context, string) (*session.ActorLease, error)
}

type InboxIDGenerator interface {
	NewID(string) (string, error)
}

type InboxIDGeneratorFunc func(string) (string, error)

func (generator InboxIDGeneratorFunc) NewID(prefix string) (string, error) {
	return generator(prefix)
}

type InboxWriterOptions struct {
	IDGenerator InboxIDGenerator
	Now         func() time.Time
}

type InboxWriter struct {
	sessions SessionActorProvider
	runtime  WakeRequester
	ids      InboxIDGenerator
	now      func() time.Time
}

type InboxRequest struct {
	SessionID      string
	RequestID      string
	IdempotencyKey string
	InputID        string
	Source         InputSource
	Content        []ContentBlock
	Metadata       map[string]json.RawMessage
}

type SchedulingError struct {
	Receipt Receipt
	Cause   error
}

func (failure *SchedulingError) Error() string {
	if failure == nil {
		return ErrInputPersistedSchedulingFailed.Error()
	}
	return fmt.Sprintf("%s: session %q input %q: %v", ErrInputPersistedSchedulingFailed, failure.Receipt.SessionID, failure.Receipt.InputID, failure.Cause)
}

func (failure *SchedulingError) Unwrap() error {
	if failure == nil {
		return nil
	}
	return failure.Cause
}

func (failure *SchedulingError) Is(target error) bool {
	return target == ErrInputPersistedSchedulingFailed
}

func NewInboxWriter(sessions SessionActorProvider, runtime WakeRequester, options InboxWriterOptions) (*InboxWriter, error) {
	if sessions == nil || runtime == nil {
		return nil, fmt.Errorf("%w: session actor provider and runtime are required", ErrInvalidInboxRequest)
	}
	if options.IDGenerator == nil {
		options.IDGenerator = cryptoInboxIDGenerator{}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &InboxWriter{sessions: sessions, runtime: runtime, ids: options.IDGenerator, now: options.Now}, nil
}

func (writer *InboxWriter) FollowUp(ctx context.Context, request InboxRequest) (Receipt, error) {
	request.Source = InputSourceUser
	return writer.accept(ctx, CommandFollowUp, InboxReasonFollowUp, InboxNextTurn, request, true)
}

func (writer *InboxWriter) Steer(ctx context.Context, request InboxRequest) (Receipt, error) {
	request.Source = InputSourceUser
	return writer.accept(ctx, CommandSteer, InboxReasonSteer, InboxNextStep, request, true)
}

func (writer *InboxWriter) Inject(ctx context.Context, request InboxRequest) (Receipt, error) {
	if request.Source != InputSourceExtension && request.Source != InputSourceSystem {
		return Receipt{}, fmt.Errorf("%w: inject source must be extension or system", ErrInvalidInboxRequest)
	}
	return writer.accept(ctx, CommandInject, InboxReasonInject, InboxNextStep, request, false)
}

func (writer *InboxWriter) accept(ctx context.Context, commandKind CommandKind, reason InboxReason, originalTarget InboxTarget, request InboxRequest, wake bool) (Receipt, error) {
	if writer == nil || writer.sessions == nil || writer.runtime == nil {
		return Receipt{}, fmt.Errorf("%w: inbox writer is not initialized", ErrInvalidInboxRequest)
	}
	if request.SessionID == "" || request.RequestID == "" || len(request.Content) == 0 {
		return Receipt{}, fmt.Errorf("%w: session id, request id, and content are required", ErrInvalidInboxRequest)
	}
	if !validSource(request.Source) {
		return Receipt{}, fmt.Errorf("%w: unsupported source %q", ErrInvalidInboxRequest, request.Source)
	}
	request.Content = cloneContentBlocks(request.Content)
	request.Metadata = cloneMetadata(request.Metadata)
	payloadHash, err := canonicalInputPayloadHash(commandKind, request.Source, request.Content, request.Metadata)
	if err != nil {
		return Receipt{}, fmt.Errorf("%w: canonicalize payload: %v", ErrInvalidInboxRequest, err)
	}
	if request.InputID == "" {
		request.InputID, err = writer.ids.NewID("input")
		if err != nil {
			return Receipt{}, fmt.Errorf("generate input id: %w", err)
		}
	}
	eventID, err := writer.ids.NewID("event")
	if err != nil {
		return Receipt{}, fmt.Errorf("generate event id: %w", err)
	}

	lease, err := writer.sessions.GetOrLoad(ctx, request.SessionID)
	if err != nil {
		return Receipt{}, err
	}
	defer lease.Release()

	command := &acceptInboxCommand{
		commandKind:    commandKind,
		reason:         reason,
		originalTarget: originalTarget,
		request:        request,
		payloadHash:    payloadHash,
		eventID:        eventID,
		occurredAt:     writer.now().UTC(),
	}
	if _, err := lease.Actor().Submit(ctx, command); err != nil {
		return Receipt{}, err
	}

	projection, ok := ProjectionFrom(lease.Actor().Snapshot())
	if !ok {
		return Receipt{}, errors.New("agent projection missing after inbox append")
	}
	accepted, ok := projection.Accepted(command.acceptedInputID)
	if !ok {
		return Receipt{}, fmt.Errorf("accepted input %q missing after inbox append", command.acceptedInputID)
	}
	receipt := accepted.Receipt
	receipt.Duplicate = command.duplicate
	if wake && projectionShouldWake(projection) {
		if err := writer.runtime.RequestWake(request.SessionID); err != nil {
			return receipt, &SchedulingError{Receipt: receipt, Cause: err}
		}
	}
	return receipt, nil
}

type acceptInboxCommand struct {
	commandKind    CommandKind
	reason         InboxReason
	originalTarget InboxTarget
	request        InboxRequest
	payloadHash    string
	eventID        string
	occurredAt     time.Time

	acceptedInputID string
	duplicate       bool
}

func (command *acceptInboxCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, errors.New("agent projection missing")
	}
	if command.request.IdempotencyKey != "" {
		if accepted, exists := projection.AcceptedByIdempotency(command.commandKind, command.request.IdempotencyKey); exists {
			if accepted.PayloadHash != command.payloadHash {
				return nil, fmt.Errorf("%w: command %q key %q", ErrIdempotencyConflict, command.commandKind, command.request.IdempotencyKey)
			}
			command.acceptedInputID = accepted.Item.InputID
			command.duplicate = true
			return nil, nil
		}
	}

	actualTarget := command.originalTarget
	if command.commandKind == CommandSteer && projection.CancelCause != nil {
		actualTarget = InboxNextTurn
	}
	payload := InboxSplicedPayload{
		Target:         actualTarget,
		Start:          len(projection.Queue(actualTarget)),
		Items:          []InboxItem{{InputID: command.request.InputID, Source: command.request.Source, Content: cloneContentBlocks(command.request.Content), Metadata: cloneMetadata(command.request.Metadata)}},
		Reason:         command.reason,
		CommandKind:    command.commandKind,
		RequestID:      command.request.RequestID,
		IdempotencyKey: command.request.IdempotencyKey,
		PayloadHash:    command.payloadHash,
		OriginalTarget: command.originalTarget,
		ActualTarget:   actualTarget,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal inbox splice: %w", err)
	}
	command.acceptedInputID = command.request.InputID
	return []session.NewEvent{{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     EventInboxSpliced,
		EventID:       command.eventID,
		OccurredAt:    command.occurredAt,
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
	}}, nil
}

func canonicalInputPayloadHash(commandKind CommandKind, source InputSource, content []ContentBlock, metadata map[string]json.RawMessage) (string, error) {
	for index, block := range content {
		if len(block.Data) == 0 {
			continue
		}
		if err := validateCanonicalJSONValue(block.Data); err != nil {
			return "", fmt.Errorf("content block %d data: %w", index, err)
		}
	}
	for key, value := range metadata {
		if key == "" {
			return "", errors.New("metadata key must not be empty")
		}
		if err := validateCanonicalJSONValue(value); err != nil {
			return "", fmt.Errorf("metadata %q: %w", key, err)
		}
	}
	normalized := struct {
		CommandKind CommandKind                `json:"command_kind"`
		Source      InputSource                `json:"source"`
		Content     []ContentBlock             `json:"content"`
		Metadata    map[string]json.RawMessage `json:"metadata,omitempty"`
	}{
		CommandKind: commandKind,
		Source:      source,
		Content:     cloneContentBlocks(content),
		Metadata:    cloneMetadata(metadata),
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	canonical, err := canonicalizeJSON(encoded)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func canonicalizeJSON(encoded []byte) ([]byte, error) {
	return jsoncanonicalizer.Transform(encoded)
}

func projectionShouldWake(projection *AgentProjection) bool {
	if projection == nil || len(projection.NextTurn) != 0 {
		return projection != nil && len(projection.NextTurn) != 0
	}
	for _, item := range projection.NextStep {
		if item.Source == InputSourceUser {
			return true
		}
	}
	return false
}

func cloneContentBlocks(content []ContentBlock) []ContentBlock {
	if content == nil {
		return nil
	}
	cloned := make([]ContentBlock, len(content))
	for index, block := range content {
		block.Data = append(json.RawMessage(nil), block.Data...)
		cloned[index] = block
	}
	return cloned
}

func cloneMetadata(metadata map[string]json.RawMessage) map[string]json.RawMessage {
	if metadata == nil {
		return nil
	}
	cloned := make(map[string]json.RawMessage, len(metadata))
	for key, value := range metadata {
		cloned[key] = append(json.RawMessage(nil), value...)
	}
	return cloned
}

type cryptoInboxIDGenerator struct{}

func (cryptoInboxIDGenerator) NewID(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return prefix + "-" + hex.EncodeToString(value[:]), nil
}
