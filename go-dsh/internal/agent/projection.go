package agent

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

const (
	EventInboxSpliced   = "agent/inbox/spliced"
	EventInputClaimed   = "agent/input-claimed"
	EventModelRequested = "model/requested"
	EventAssistantChunk = "assistant/chunk"

	AgentProjectionKey session.ProjectionKey = "agent"
)

var (
	ErrInvalidInboxEvent       = errors.New("invalid agent inbox event")
	ErrDuplicateInput          = errors.New("duplicate agent input")
	ErrIdempotencyConflict     = errors.New("agent input idempotency conflict")
	ErrInvalidInputClaim       = errors.New("invalid agent input claim")
	ErrIncompleteInputClaim    = errors.New("incomplete agent input claim batch")
	ErrInvalidDriverTransition = errors.New("invalid agent driver transition")
	ErrStaleDriverActivity     = errors.New("stale agent driver activity")
)

type InboxTarget string

const (
	InboxNextTurn InboxTarget = "next-turn"
	InboxNextStep InboxTarget = "next-step"
)

type InputSource string

const (
	InputSourceUser      InputSource = "user"
	InputSourceExtension InputSource = "extension"
	InputSourceSystem    InputSource = "system"
)

type InboxReason string

const (
	InboxReasonFollowUp InboxReason = "followup"
	InboxReasonSteer    InboxReason = "steer"
	InboxReasonInject   InboxReason = "inject"
	InboxReasonClaim    InboxReason = "claim"
	InboxReasonCancel   InboxReason = "cancel"
)

type CommandKind string

const (
	CommandFollowUp CommandKind = "followup"
	CommandSteer    CommandKind = "steer"
	CommandInject   CommandKind = "inject"
)

type ContentBlock struct {
	Type string          `json:"type"`
	Text string          `json:"text,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type InboxItem struct {
	InputID        string                     `json:"input_id"`
	RequestID      string                     `json:"request_id"`
	IdempotencyKey string                     `json:"idempotency_key"`
	Source         InputSource                `json:"source"`
	Content        []ContentBlock             `json:"content"`
	AcceptedSeq    uint64                     `json:"accepted_seq,omitempty"`
	AcceptedAt     time.Time                  `json:"accepted_at,omitempty"`
	Metadata       map[string]json.RawMessage `json:"metadata,omitempty"`
}

type InboxSplicedPayload struct {
	Target         InboxTarget `json:"target"`
	Start          int         `json:"start"`
	DeleteCount    int         `json:"delete_count"`
	Items          []InboxItem `json:"items"`
	Reason         InboxReason `json:"reason"`
	CommandKind    CommandKind `json:"command_kind,omitempty"`
	RequestID      string      `json:"request_id,omitempty"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	PayloadHash    string      `json:"payload_hash,omitempty"`
	OriginalTarget InboxTarget `json:"original_target,omitempty"`
	ActualTarget   InboxTarget `json:"actual_target,omitempty"`
	ClaimID        string      `json:"claim_id,omitempty"`
}

type InputClaimedPayload struct {
	Continuation      bool          `json:"continuation,omitempty"`
	ClaimID           string        `json:"claim_id"`
	TurnID            string        `json:"turn_id"`
	ProposedStepIndex uint32        `json:"proposed_step_index"`
	OrderedInputIDs   []string      `json:"ordered_input_ids"`
	SourceTargets     []InboxTarget `json:"source_targets"`
}

type StepStartPayload struct {
	ClaimID   string `json:"claim_id"`
	StepIndex uint32 `json:"step_index"`
}

type AttemptPhase string

const (
	AttemptPhaseNone      AttemptPhase = ""
	AttemptPhaseRequested AttemptPhase = "requested"
)

type ModelRequestedPayload struct {
	Attempt        uint32          `json:"attempt"`
	Model          string          `json:"model"`
	RequestSummary json.RawMessage `json:"request_summary,omitempty"`
}

type StepEndReason string

const (
	StepEndCompleted   StepEndReason = "completed"
	StepEndMaxTokens   StepEndReason = "max-tokens"
	StepEndBlocked     StepEndReason = "blocked"
	StepEndAborted     StepEndReason = "aborted"
	StepEndError       StepEndReason = "error"
	StepEndInterrupted StepEndReason = "interrupted"
	StepEndToolCalls   StepEndReason = "tool-calls"
)

type StepEndPayload struct {
	Reason       StepEndReason `json:"reason"`
	Attempt      uint32        `json:"attempt,omitempty"`
	UsageMissing bool          `json:"usage_missing,omitempty"`
	Synthetic    bool          `json:"synthetic,omitempty"`
}

type TurnEndPayload struct {
	Reason           string   `json:"reason,omitempty"`
	ClaimID          string   `json:"claim_id,omitempty"`
	ConsumedInputIDs []string `json:"consumed_input_ids,omitempty"`
	Synthetic        bool     `json:"synthetic,omitempty"`
}

type AcceptedInput struct {
	Item           InboxItem
	CommandKind    CommandKind
	PayloadHash    string
	OriginalTarget InboxTarget
	ActualTarget   InboxTarget
	Receipt        Receipt
}

type InputClaim struct {
	ClaimID           string
	TurnID            string
	ProposedStepIndex uint32
	OrderedInputIDs   []string
	SourceTargets     []InboxTarget
	Items             []InboxItem
	ClaimedAtSeq      uint64
	ClaimedAt         time.Time
}

type AttemptChunkReceipt struct {
	ChunkIndex  uint64
	EventID     string
	EventSeq    uint64
	PayloadHash string
	Chunk       llm.StreamChunk
}

type AgentProjection struct {
	controlProjection
	NextTurn []InboxItem
	NextStep []InboxItem

	ActiveTurnID         string
	PendingClaimID       string
	ActiveStepClaimID    string
	ActiveStepID         string
	ActiveStepIndex      uint32
	ActiveAttempt        uint32
	AttemptPhase         AttemptPhase
	ActiveModel          string
	ActiveChunkCount     uint64
	LastChunkIndex       uint64
	LastChunkEventID     string
	LastChunkEventSeq    uint64
	LastChunkPayloadHash string
	ActiveChunks         []AttemptChunkReceipt

	AcceptedByInputID map[string]AcceptedInput
	InputIDByIdemKey  map[string]string
	ClaimsByID        map[string]InputClaim
	ClaimIDByStep     map[string]string

	pendingClaim               *pendingInputClaim
	awaitingInitialClaimTurnID string
}

type pendingInputClaim struct {
	ClaimID       string
	Initial       bool
	InputIDs      []string
	SourceTargets []InboxTarget
}

func NewAgentProjection() *AgentProjection {
	return &AgentProjection{
		AcceptedByInputID: make(map[string]AcceptedInput),
		InputIDByIdemKey:  make(map[string]string),
		ClaimsByID:        make(map[string]InputClaim),
		ClaimIDByStep:     make(map[string]string),
	}
}

func (projection *AgentProjection) Clone() *AgentProjection {
	if projection == nil {
		return NewAgentProjection()
	}
	cloned := NewAgentProjection()
	cloned.controlProjection = projection.controlProjection.clone()
	cloned.NextTurn = cloneInboxItems(projection.NextTurn)
	cloned.NextStep = cloneInboxItems(projection.NextStep)
	cloned.ActiveTurnID = projection.ActiveTurnID
	cloned.PendingClaimID = projection.PendingClaimID
	cloned.ActiveStepClaimID = projection.ActiveStepClaimID
	cloned.ActiveStepID = projection.ActiveStepID
	cloned.ActiveStepIndex = projection.ActiveStepIndex
	cloned.ActiveAttempt = projection.ActiveAttempt
	cloned.AttemptPhase = projection.AttemptPhase
	cloned.ActiveModel = projection.ActiveModel
	cloned.ActiveChunkCount = projection.ActiveChunkCount
	cloned.LastChunkIndex = projection.LastChunkIndex
	cloned.LastChunkEventID = projection.LastChunkEventID
	cloned.LastChunkEventSeq = projection.LastChunkEventSeq
	cloned.LastChunkPayloadHash = projection.LastChunkPayloadHash
	cloned.ActiveChunks = cloneAttemptChunkReceipts(projection.ActiveChunks)
	cloned.awaitingInitialClaimTurnID = projection.awaitingInitialClaimTurnID
	for inputID, accepted := range projection.AcceptedByInputID {
		accepted.Item = cloneInboxItem(accepted.Item)
		cloned.AcceptedByInputID[inputID] = accepted
	}
	for key, inputID := range projection.InputIDByIdemKey {
		cloned.InputIDByIdemKey[key] = inputID
	}
	for claimID, claim := range projection.ClaimsByID {
		claim.OrderedInputIDs = append([]string(nil), claim.OrderedInputIDs...)
		claim.SourceTargets = append([]InboxTarget(nil), claim.SourceTargets...)
		claim.Items = cloneInboxItems(claim.Items)
		cloned.ClaimsByID[claimID] = claim
	}
	for step, claimID := range projection.ClaimIDByStep {
		cloned.ClaimIDByStep[step] = claimID
	}
	if projection.pendingClaim != nil {
		cloned.pendingClaim = &pendingInputClaim{
			ClaimID:       projection.pendingClaim.ClaimID,
			Initial:       projection.pendingClaim.Initial,
			InputIDs:      append([]string(nil), projection.pendingClaim.InputIDs...),
			SourceTargets: append([]InboxTarget(nil), projection.pendingClaim.SourceTargets...),
		}
	}
	return cloned
}

func (projection *AgentProjection) Queue(target InboxTarget) []InboxItem {
	if projection == nil {
		return nil
	}
	switch target {
	case InboxNextTurn:
		return cloneInboxItems(projection.NextTurn)
	case InboxNextStep:
		return cloneInboxItems(projection.NextStep)
	default:
		return nil
	}
}

func (projection *AgentProjection) Accepted(inputID string) (AcceptedInput, bool) {
	if projection == nil {
		return AcceptedInput{}, false
	}
	accepted, ok := projection.AcceptedByInputID[inputID]
	if !ok {
		return AcceptedInput{}, false
	}
	accepted.Item = cloneInboxItem(accepted.Item)
	return accepted, true
}

func (projection *AgentProjection) AcceptedByIdempotency(commandKind CommandKind, key string) (AcceptedInput, bool) {
	if projection == nil || key == "" {
		return AcceptedInput{}, false
	}
	inputID, ok := projection.InputIDByIdemKey[idempotencyIndexKey(commandKind, key)]
	if !ok {
		return AcceptedInput{}, false
	}
	return projection.Accepted(inputID)
}

func (projection *AgentProjection) Claim(claimID string) (InputClaim, bool) {
	if projection == nil {
		return InputClaim{}, false
	}
	claim, ok := projection.ClaimsByID[claimID]
	if !ok {
		return InputClaim{}, false
	}
	claim.OrderedInputIDs = append([]string(nil), claim.OrderedInputIDs...)
	claim.SourceTargets = append([]InboxTarget(nil), claim.SourceTargets...)
	claim.Items = cloneInboxItems(claim.Items)
	return claim, true
}

func ProjectionFrom(snapshot session.Snapshot) (*AgentProjection, bool) {
	projection, ok := session.ProjectionAs[*AgentProjection](snapshot, AgentProjectionKey)
	if !ok || projection == nil {
		return nil, false
	}
	return projection.Clone(), true
}

func AgentSchemaContribution() session.SchemaContribution {
	return session.SchemaContribution{
		Name: "agent",
		Events: []session.EventDefinition{
			{EventType: EventInboxSpliced, ReplayPolicy: session.ReplayRequired},
			{EventType: EventInputClaimed, ReplayPolicy: session.ReplayRequired},
			{EventType: EventModelRequested, ReplayPolicy: session.ReplayRequired},
			{EventType: EventHarnessConfigured, ReplayPolicy: session.ReplayRequired},
			{EventType: EventCancelRequested, ReplayPolicy: session.ReplayRequired},
			{EventType: EventRetry, ReplayPolicy: session.ReplayRequired},
			{EventType: EventRetryStarted, ReplayPolicy: session.ReplayRequired},
			{EventType: EventToolStarted, ReplayPolicy: session.ReplayRequired},
		},
		Projections: []session.ProjectionSpec{{
			Key:   AgentProjectionKey,
			Order: 300,
			New:   func() any { return NewAgentProjection() },
			Clone: func(state any) any { return state.(*AgentProjection).Clone() },
			Apply: func(state any, event session.Event) error {
				return state.(*AgentProjection).apply(event)
			},
			ValidateBoundary: func(state any, boundary session.ProjectionBoundary) error {
				projection := state.(*AgentProjection)
				if projection.pendingClaim != nil {
					return fmt.Errorf("%w: claim %q has deletions without marker", ErrIncompleteInputClaim, projection.pendingClaim.ClaimID)
				}
				if projection.awaitingInitialClaimTurnID != "" && boundary != session.BoundaryReplayEnd {
					return fmt.Errorf("%w: turn %q started without its initial claim", ErrIncompleteInputClaim, projection.awaitingInitialClaimTurnID)
				}
				return nil
			},
		}},
	}
}

func (projection *AgentProjection) apply(event session.Event) error {
	if projection.CancelCause != nil {
		switch event.EventType {
		case EventModelRequested, EventAssistantChunk, EventRetry, EventRetryStarted, "step/start", EventToolStarted:
			return ErrStaleDriverActivity
		}
	}
	if err := projection.applyControl(event); err != nil {
		return err
	}
	switch event.EventType {
	case EventInboxSpliced:
		var payload InboxSplicedPayload
		if err := decodeEventPayload(event.Data, &payload); err != nil {
			return fmt.Errorf("%w: decode inbox splice: %v", ErrInvalidInboxEvent, err)
		}
		if projection.pendingClaim != nil && (payload.Reason != InboxReasonClaim || payload.ClaimID != projection.pendingClaim.ClaimID) {
			return fmt.Errorf("%w: claim %q must be closed before another inbox operation", ErrInvalidInputClaim, projection.pendingClaim.ClaimID)
		}
		if projection.awaitingInitialClaimTurnID != "" && payload.Reason != InboxReasonClaim {
			return fmt.Errorf("%w: turn %q must claim its initial inputs before another inbox operation", ErrInvalidInputClaim, projection.awaitingInitialClaimTurnID)
		}
		return projection.applySplice(event, payload)
	case EventInputClaimed:
		var payload InputClaimedPayload
		if err := decodeEventPayload(event.Data, &payload); err != nil {
			return fmt.Errorf("%w: decode input claim: %v", ErrInvalidInputClaim, err)
		}
		return projection.applyClaim(event, payload)
	default:
		if projection.pendingClaim != nil {
			return fmt.Errorf("%w: claim %q must be closed immediately after its deletion splices", ErrInvalidInputClaim, projection.pendingClaim.ClaimID)
		}
		if projection.awaitingInitialClaimTurnID != "" && (event.EventType != "turn/end" || !isSyntheticEvent(event.Data)) {
			return fmt.Errorf("%w: turn %q must claim its initial inputs before another event", ErrInvalidInputClaim, projection.awaitingInitialClaimTurnID)
		}
	}

	switch event.EventType {
	case "turn/start":
		if event.TurnID == "" || projection.ActiveTurnID != "" || projection.PendingClaimID != "" || projection.ActiveStepClaimID != "" {
			return fmt.Errorf("%w: invalid active turn transition", ErrInvalidInputClaim)
		}
		projection.ActiveTurnID = event.TurnID
		projection.awaitingInitialClaimTurnID = event.TurnID
	case "step/start":
		if err := projection.applyStepStart(event); err != nil {
			return err
		}
	case EventModelRequested:
		if err := projection.applyModelRequested(event); err != nil {
			return err
		}
	case EventAssistantChunk:
		if err := projection.applyAssistantChunk(event); err != nil {
			return err
		}
	case "step/end":
		if err := projection.applyStepEnd(event); err != nil {
			return err
		}
	case "turn/end":
		if err := projection.applyTurnEnd(event); err != nil {
			return err
		}
	case "session/cancelled":
		if projection.PendingClaimID != "" || projection.ActiveStepClaimID != "" {
			return fmt.Errorf("%w: cancellation cannot discard owned claim inputs", ErrInvalidInputClaim)
		}
		projection.ActiveTurnID = ""
		projection.clearActiveStep()
		projection.awaitingInitialClaimTurnID = ""
	}
	return nil
}

func (projection *AgentProjection) applySplice(event session.Event, payload InboxSplicedPayload) error {
	queue, err := projection.queue(payload.Target)
	if err != nil {
		return err
	}
	if payload.Start < 0 || payload.DeleteCount < 0 || payload.Start > len(*queue) || payload.DeleteCount > len(*queue)-payload.Start {
		return fmt.Errorf("%w: splice [%d:%d] exceeds %s length %d", ErrInvalidInboxEvent, payload.Start, payload.Start+payload.DeleteCount, payload.Target, len(*queue))
	}
	if len(payload.Items) > 0 && payload.DeleteCount != 0 {
		return fmt.Errorf("%w: insertion cannot delete existing items", ErrInvalidInboxEvent)
	}

	switch payload.Reason {
	case InboxReasonFollowUp, InboxReasonSteer, InboxReasonInject:
		if err := projection.validateInsertion(payload); err != nil {
			return err
		}
	case InboxReasonClaim:
		if err := projection.recordClaimDeletion(payload, (*queue)[payload.Start:payload.Start+payload.DeleteCount]); err != nil {
			return err
		}
	case InboxReasonCancel:
		if err := validateRemovalSplice(payload, false); err != nil {
			return err
		}
		if payload.Start != 0 || payload.DeleteCount != len(*queue) {
			return fmt.Errorf("%w: cancel splice must clear the target queue", ErrInvalidInboxEvent)
		}
	default:
		return fmt.Errorf("%w: unsupported splice reason %q", ErrInvalidInboxEvent, payload.Reason)
	}

	inserted := cloneInboxItems(payload.Items)
	for index := range inserted {
		item := &inserted[index]
		item.RequestID = payload.RequestID
		item.IdempotencyKey = payload.IdempotencyKey
		item.AcceptedSeq = event.Seq
		item.AcceptedAt = event.CommittedAt
	}
	updated := make([]InboxItem, 0, len(*queue)-payload.DeleteCount+len(inserted))
	updated = append(updated, (*queue)[:payload.Start]...)
	updated = append(updated, inserted...)
	updated = append(updated, (*queue)[payload.Start+payload.DeleteCount:]...)
	*queue = updated

	for _, item := range inserted {
		receipt := Receipt{
			RequestID:      payload.RequestID,
			SessionID:      event.SessionID,
			CommandID:      event.EventID,
			AcceptedSeq:    event.Seq,
			Placement:      placementFor(payload.Target, item.Source),
			AcceptedAt:     event.CommittedAt,
			InputID:        item.InputID,
			Target:         payload.Target,
			IdempotencyKey: payload.IdempotencyKey,
		}
		projection.AcceptedByInputID[item.InputID] = AcceptedInput{
			Item:           cloneInboxItem(item),
			CommandKind:    payload.CommandKind,
			PayloadHash:    payload.PayloadHash,
			OriginalTarget: payload.OriginalTarget,
			ActualTarget:   payload.ActualTarget,
			Receipt:        receipt,
		}
		if payload.IdempotencyKey != "" {
			projection.InputIDByIdemKey[idempotencyIndexKey(payload.CommandKind, payload.IdempotencyKey)] = item.InputID
		}
	}
	return nil
}

func (projection *AgentProjection) validateInsertion(payload InboxSplicedPayload) error {
	if payload.DeleteCount != 0 || payload.ClaimID != "" || len(payload.Items) != 1 {
		return fmt.Errorf("%w: an external command insertion must contain exactly one item", ErrInvalidInboxEvent)
	}
	queue, err := projection.queue(payload.Target)
	if err != nil {
		return err
	}
	if payload.Start != len(*queue) {
		return fmt.Errorf("%w: insertion must append at queue tail", ErrInvalidInboxEvent)
	}
	if payload.CommandKind == "" || payload.RequestID == "" || !validPayloadHash(payload.PayloadHash) {
		return fmt.Errorf("%w: insertion identity fields are incomplete", ErrInvalidInboxEvent)
	}
	if !validTarget(payload.OriginalTarget) || !validTarget(payload.ActualTarget) || payload.Target != payload.ActualTarget {
		return fmt.Errorf("%w: invalid insertion targets", ErrInvalidInboxEvent)
	}
	if err := validateCommandPlacement(payload, payload.Items[0].Source); err != nil {
		return err
	}
	if payload.IdempotencyKey != "" {
		idempotencyKey := idempotencyIndexKey(payload.CommandKind, payload.IdempotencyKey)
		if inputID, exists := projection.InputIDByIdemKey[idempotencyKey]; exists {
			accepted := projection.AcceptedByInputID[inputID]
			if accepted.PayloadHash != payload.PayloadHash {
				return fmt.Errorf("%w: command %q key %q", ErrIdempotencyConflict, payload.CommandKind, payload.IdempotencyKey)
			}
			return fmt.Errorf("%w: command %q key %q was already accepted", ErrDuplicateInput, payload.CommandKind, payload.IdempotencyKey)
		}
	}
	item := payload.Items[0]
	if err := validateInboxItem(item, payload); err != nil {
		return err
	}
	if _, exists := projection.AcceptedByInputID[item.InputID]; exists {
		return fmt.Errorf("%w: input id %q", ErrDuplicateInput, item.InputID)
	}
	return nil
}

func validateCommandPlacement(payload InboxSplicedPayload, source InputSource) error {
	switch payload.CommandKind {
	case CommandFollowUp:
		if payload.Reason != InboxReasonFollowUp || payload.OriginalTarget != InboxNextTurn || payload.ActualTarget != InboxNextTurn || source != InputSourceUser {
			return fmt.Errorf("%w: invalid followup placement or source", ErrInvalidInboxEvent)
		}
	case CommandSteer:
		if payload.Reason != InboxReasonSteer || payload.OriginalTarget != InboxNextStep || (payload.ActualTarget != InboxNextStep && payload.ActualTarget != InboxNextTurn) || source != InputSourceUser {
			return fmt.Errorf("%w: invalid steer placement or source", ErrInvalidInboxEvent)
		}
	case CommandInject:
		if payload.Reason != InboxReasonInject || payload.OriginalTarget != InboxNextStep || payload.ActualTarget != InboxNextStep || (source != InputSourceExtension && source != InputSourceSystem) {
			return fmt.Errorf("%w: invalid inject placement or source", ErrInvalidInboxEvent)
		}
	default:
		return fmt.Errorf("%w: unsupported insertion command %q", ErrInvalidInboxEvent, payload.CommandKind)
	}
	return nil
}

func validateInboxItem(item InboxItem, payload InboxSplicedPayload) error {
	if item.InputID == "" || item.Source == "" || len(item.Content) == 0 {
		return fmt.Errorf("%w: input identity, source, and content are required", ErrInvalidInboxEvent)
	}
	if item.RequestID != "" && item.RequestID != payload.RequestID {
		return fmt.Errorf("%w: item request id differs from splice", ErrInvalidInboxEvent)
	}
	if item.IdempotencyKey != "" && item.IdempotencyKey != payload.IdempotencyKey {
		return fmt.Errorf("%w: item idempotency key differs from splice", ErrInvalidInboxEvent)
	}
	if item.AcceptedSeq != 0 || !item.AcceptedAt.IsZero() {
		return fmt.Errorf("%w: acceptance fields are commit-owned", ErrInvalidInboxEvent)
	}
	if !validSource(item.Source) {
		return fmt.Errorf("%w: unsupported input source %q", ErrInvalidInboxEvent, item.Source)
	}
	for _, block := range item.Content {
		if block.Type == "" {
			return fmt.Errorf("%w: content block type is required", ErrInvalidInboxEvent)
		}
		if len(block.Data) != 0 {
			if err := validateCanonicalJSONValue(block.Data); err != nil {
				return fmt.Errorf("%w: invalid content block data: %v", ErrInvalidInboxEvent, err)
			}
		}
	}
	for key, value := range item.Metadata {
		if key == "" {
			return fmt.Errorf("%w: metadata keys must not be empty", ErrInvalidInboxEvent)
		}
		if err := validateCanonicalJSONValue(value); err != nil {
			return fmt.Errorf("%w: invalid metadata value for %q: %v", ErrInvalidInboxEvent, key, err)
		}
	}
	return nil
}

func (projection *AgentProjection) recordClaimDeletion(payload InboxSplicedPayload, deleted []InboxItem) error {
	if err := validateRemovalSplice(payload, true); err != nil {
		return err
	}
	if projection.ActiveTurnID == "" {
		return fmt.Errorf("%w: claim requires an active turn", ErrInvalidInputClaim)
	}
	if payload.Start != 0 {
		return fmt.Errorf("%w: claim must consume from the queue head", ErrInvalidInputClaim)
	}
	initial := projection.awaitingInitialClaimTurnID != ""
	if projection.pendingClaim != nil {
		if projection.pendingClaim.ClaimID != payload.ClaimID {
			return fmt.Errorf("%w: claim %q started before claim %q was completed", ErrInvalidInputClaim, payload.ClaimID, projection.pendingClaim.ClaimID)
		}
		if projection.pendingClaim.Initial != initial {
			return fmt.Errorf("%w: claim mode changed within one batch", ErrInvalidInputClaim)
		}
	}
	if !initial && payload.Target != InboxNextStep {
		return fmt.Errorf("%w: a subsequent claim may only consume next-step", ErrInvalidInputClaim)
	}
	switch payload.Target {
	case InboxNextStep:
		if payload.DeleteCount != len(projection.NextStep) || pendingContainsTarget(projection.pendingClaim, InboxNextTurn) {
			return fmt.Errorf("%w: next-step claim must consume the full queue before next-turn", ErrInvalidInputClaim)
		}
	case InboxNextTurn:
		if payload.DeleteCount != 1 || len(projection.NextStep) != 0 || pendingContainsTarget(projection.pendingClaim, InboxNextTurn) {
			return fmt.Errorf("%w: next-turn claim must consume exactly the first item after next-step", ErrInvalidInputClaim)
		}
	default:
		return fmt.Errorf("%w: unsupported claim target %q", ErrInvalidInputClaim, payload.Target)
	}
	if projection.pendingClaim == nil {
		projection.pendingClaim = &pendingInputClaim{ClaimID: payload.ClaimID, Initial: initial}
	}
	for _, item := range deleted {
		if _, accepted := projection.AcceptedByInputID[item.InputID]; !accepted {
			return fmt.Errorf("%w: claimed input %q is absent from acceptance history", ErrInvalidInputClaim, item.InputID)
		}
		projection.pendingClaim.InputIDs = append(projection.pendingClaim.InputIDs, item.InputID)
		projection.pendingClaim.SourceTargets = append(projection.pendingClaim.SourceTargets, payload.Target)
	}
	return nil
}

func validateRemovalSplice(payload InboxSplicedPayload, claim bool) error {
	if len(payload.Items) != 0 || payload.DeleteCount <= 0 {
		return fmt.Errorf("%w: removal splice must delete at least one item and cannot insert", ErrInvalidInboxEvent)
	}
	if payload.CommandKind != "" || payload.RequestID != "" || payload.IdempotencyKey != "" || payload.PayloadHash != "" || payload.OriginalTarget != "" || payload.ActualTarget != "" {
		return fmt.Errorf("%w: removal splice contains insertion identity", ErrInvalidInboxEvent)
	}
	if claim && payload.ClaimID == "" {
		return fmt.Errorf("%w: claim splice requires claim id", ErrInvalidInputClaim)
	}
	if !claim && payload.ClaimID != "" {
		return fmt.Errorf("%w: non-claim splice cannot carry claim id", ErrInvalidInboxEvent)
	}
	return nil
}

func (projection *AgentProjection) applyClaim(event session.Event, payload InputClaimedPayload) error {
	pending := projection.pendingClaim
	if payload.Continuation {
		if pending != nil || !projection.NeedsToolStep || len(projection.NextStep) != 0 || len(payload.OrderedInputIDs) != 0 || len(payload.SourceTargets) != 0 {
			return ErrInvalidInputClaim
		}
		for _, call := range projection.ToolCalls {
			if call.Call.TurnID == projection.ActiveTurnID && !call.Done {
				return ErrInvalidInputClaim
			}
		}
		pending = &pendingInputClaim{ClaimID: payload.ClaimID}
	}
	if pending == nil || payload.ClaimID == "" || pending.ClaimID != payload.ClaimID {
		return fmt.Errorf("%w: claim marker has no matching deletions", ErrInvalidInputClaim)
	}
	if projection.PendingClaimID != "" || projection.ActiveStepClaimID != "" {
		return fmt.Errorf("%w: claim %q has not been consumed", ErrInvalidInputClaim, projection.PendingClaimID)
	}
	if payload.TurnID == "" || event.TurnID != payload.TurnID || payload.TurnID != projection.ActiveTurnID || payload.ProposedStepIndex == 0 {
		return fmt.Errorf("%w: claim turn is not active or proposed step is invalid", ErrInvalidInputClaim)
	}
	if pending.Initial {
		steerOnly := len(projection.NextTurn) == 0 && countClaimTarget(pending.SourceTargets, InboxNextTurn) == 0
		if steerOnly {
			steerOnly = false
			for _, id := range pending.InputIDs {
				if projection.AcceptedByInputID[id].Item.Source == InputSourceUser {
					steerOnly = true
				}
			}
		}
		if payload.ProposedStepIndex != 1 || len(pending.SourceTargets) == 0 || (!steerOnly && (pending.SourceTargets[len(pending.SourceTargets)-1] != InboxNextTurn || countClaimTarget(pending.SourceTargets, InboxNextTurn) != 1)) {
			return fmt.Errorf("%w: initial claim must consume next-step followed by one next-turn input", ErrInvalidInputClaim)
		}
	} else {
		if payload.ProposedStepIndex <= 1 || countClaimTarget(pending.SourceTargets, InboxNextStep) != len(pending.SourceTargets) {
			return fmt.Errorf("%w: subsequent claim must consume only next-step inputs", ErrInvalidInputClaim)
		}
	}
	if payload.ProposedStepIndex != nextClaimStepIndex(projection, payload.TurnID) {
		return fmt.Errorf("%w: proposed step index %d is not next for turn %q", ErrInvalidInputClaim, payload.ProposedStepIndex, payload.TurnID)
	}
	if _, exists := projection.ClaimsByID[payload.ClaimID]; exists {
		return fmt.Errorf("%w: duplicate claim id %q", ErrInvalidInputClaim, payload.ClaimID)
	}
	stepKey := claimStepKey(payload.TurnID, payload.ProposedStepIndex)
	if existing, exists := projection.ClaimIDByStep[stepKey]; exists {
		return fmt.Errorf("%w: turn %q step %d already belongs to claim %q", ErrInvalidInputClaim, payload.TurnID, payload.ProposedStepIndex, existing)
	}
	if !slices.Equal(payload.OrderedInputIDs, pending.InputIDs) || !slices.Equal(payload.SourceTargets, pending.SourceTargets) {
		return fmt.Errorf("%w: marker does not match deleted inputs", ErrInvalidInputClaim)
	}
	if len(payload.OrderedInputIDs) == 0 && !payload.Continuation {
		return fmt.Errorf("%w: claim cannot be empty", ErrInvalidInputClaim)
	}
	items := make([]InboxItem, 0, len(payload.OrderedInputIDs))
	for _, inputID := range payload.OrderedInputIDs {
		accepted, exists := projection.AcceptedByInputID[inputID]
		if !exists {
			return fmt.Errorf("%w: accepted input %q is missing", ErrInvalidInputClaim, inputID)
		}
		items = append(items, cloneInboxItem(accepted.Item))
	}
	projection.ClaimsByID[payload.ClaimID] = InputClaim{
		ClaimID:           payload.ClaimID,
		TurnID:            payload.TurnID,
		ProposedStepIndex: payload.ProposedStepIndex,
		OrderedInputIDs:   append([]string(nil), payload.OrderedInputIDs...),
		SourceTargets:     append([]InboxTarget(nil), payload.SourceTargets...),
		Items:             items,
		ClaimedAtSeq:      event.Seq,
		ClaimedAt:         event.CommittedAt,
	}
	projection.ClaimIDByStep[stepKey] = payload.ClaimID
	projection.PendingClaimID = payload.ClaimID
	projection.pendingClaim = nil
	if projection.awaitingInitialClaimTurnID == payload.TurnID {
		projection.awaitingInitialClaimTurnID = ""
	}
	return nil
}

func (projection *AgentProjection) applyStepStart(event session.Event) error {
	if projection.ActiveTurnID == "" || event.TurnID != projection.ActiveTurnID || event.StepID == "" || projection.PendingClaimID == "" || projection.ActiveStepClaimID != "" || projection.ActiveStepID != "" {
		return fmt.Errorf("%w: step/start requires one pending claim in the active turn", ErrInvalidInputClaim)
	}
	var payload StepStartPayload
	if err := decodeEventPayload(event.Data, &payload); err != nil {
		return fmt.Errorf("%w: decode step/start: %v", ErrInvalidInputClaim, err)
	}
	claim, exists := projection.ClaimsByID[projection.PendingClaimID]
	if !exists || payload.ClaimID != claim.ClaimID || payload.StepIndex != claim.ProposedStepIndex {
		return fmt.Errorf("%w: step/start does not match pending claim %q", ErrInvalidInputClaim, projection.PendingClaimID)
	}
	projection.ActiveStepClaimID = projection.PendingClaimID
	projection.PendingClaimID = ""
	projection.ActiveStepID = event.StepID
	projection.ActiveStepIndex = payload.StepIndex
	projection.ActiveAttempt = 0
	projection.AttemptPhase = AttemptPhaseNone
	projection.ActiveModel = ""
	return nil
}

func (projection *AgentProjection) applyModelRequested(event session.Event) error {
	if projection.ActiveTurnID == "" || projection.ActiveStepID == "" || projection.ActiveStepClaimID == "" || event.TurnID != projection.ActiveTurnID || event.StepID != projection.ActiveStepID {
		return fmt.Errorf("%w: model/requested requires the active claimed step", ErrInvalidDriverTransition)
	}
	var payload ModelRequestedPayload
	if err := decodeEventPayload(event.Data, &payload); err != nil {
		return fmt.Errorf("%w: decode model/requested: %v", ErrInvalidDriverTransition, err)
	}
	if payload.Attempt != projection.ActiveAttempt+1 || (projection.ActiveAttempt != 0 && projection.Retry == nil) || projection.AttemptPhase != AttemptPhaseNone || strings.TrimSpace(payload.Model) == "" {
		return fmt.Errorf("%w: initial model request does not match the active step", ErrInvalidDriverTransition)
	}
	if len(payload.RequestSummary) != 0 {
		if err := validateCanonicalJSONValue(payload.RequestSummary); err != nil {
			return fmt.Errorf("%w: invalid model request summary: %v", ErrInvalidDriverTransition, err)
		}
	}
	projection.ActiveAttempt = payload.Attempt
	projection.AttemptPhase = AttemptPhaseRequested
	projection.ActiveModel = payload.Model
	projection.ActiveChunkCount = 0
	projection.LastChunkIndex = 0
	projection.LastChunkEventID = ""
	projection.LastChunkEventSeq = 0
	projection.LastChunkPayloadHash = ""
	projection.ActiveChunks = nil
	return nil
}

func (projection *AgentProjection) applyAssistantChunk(event session.Event) error {
	if projection.ActiveTurnID == "" || projection.ActiveStepID == "" || projection.ActiveAttempt == 0 || projection.AttemptPhase == AttemptPhaseNone || event.TurnID != projection.ActiveTurnID || event.StepID != projection.ActiveStepID {
		return fmt.Errorf("%w: assistant/chunk requires the active attempt", ErrInvalidDriverTransition)
	}
	var payload AssistantChunkPayload
	if err := decodeEventPayload(event.Data, &payload); err != nil {
		return fmt.Errorf("%w: decode assistant/chunk: %v", ErrInvalidDriverTransition, err)
	}
	if err := payload.validate(); err != nil {
		return fmt.Errorf("%w: invalid assistant/chunk: %v", ErrInvalidDriverTransition, err)
	}
	if payload.Attempt != projection.ActiveAttempt || payload.ChunkIndex != projection.ActiveChunkCount {
		return fmt.Errorf("%w: assistant/chunk does not match active attempt cursor", ErrInvalidDriverTransition)
	}
	payloadHash, err := canonicalPayloadHash(event.Data)
	if err != nil {
		return fmt.Errorf("%w: hash assistant/chunk: %v", ErrInvalidDriverTransition, err)
	}
	receipt := AttemptChunkReceipt{
		ChunkIndex:  payload.ChunkIndex,
		EventID:     event.EventID,
		EventSeq:    event.Seq,
		PayloadHash: payloadHash,
		Chunk:       cloneAttemptStreamChunk(payload.Chunk),
	}
	projection.LastChunkIndex = payload.ChunkIndex
	projection.LastChunkEventID = event.EventID
	projection.LastChunkEventSeq = event.Seq
	projection.LastChunkPayloadHash = payloadHash
	projection.ActiveChunks = append(projection.ActiveChunks, receipt)
	projection.ActiveChunkCount++
	return nil
}

func (projection *AgentProjection) applyStepEnd(event session.Event) error {
	if projection.ActiveStepClaimID == "" || projection.ActiveStepID == "" || event.StepID != projection.ActiveStepID || event.TurnID != projection.ActiveTurnID {
		return fmt.Errorf("%w: step/end does not match the active claimed step", ErrInvalidDriverTransition)
	}
	var payload StepEndPayload
	if err := decodeEventPayload(event.Data, &payload); err != nil {
		return fmt.Errorf("%w: decode step/end: %v", ErrInvalidDriverTransition, err)
	}
	if !validStepEndReason(payload.Reason) {
		return fmt.Errorf("%w: unsupported step end reason %q", ErrInvalidDriverTransition, payload.Reason)
	}
	if projection.ActiveAttempt == 0 {
		if payload.Attempt != 0 {
			return fmt.Errorf("%w: step without an attempt cannot end attempt %d", ErrInvalidDriverTransition, payload.Attempt)
		}
	} else if payload.Attempt != projection.ActiveAttempt && !(payload.Synthetic && payload.Attempt == 0) {
		return fmt.Errorf("%w: step/end attempt %d does not match active attempt %d", ErrInvalidDriverTransition, payload.Attempt, projection.ActiveAttempt)
	}
	projection.ActiveStepClaimID = ""
	projection.clearActiveStep()
	return nil
}

func (projection *AgentProjection) clearActiveStep() {
	projection.ActiveStepID = ""
	projection.ActiveStepIndex = 0
	projection.ActiveAttempt = 0
	projection.AttemptPhase = AttemptPhaseNone
	projection.ActiveModel = ""
	projection.ActiveChunkCount = 0
	projection.LastChunkIndex = 0
	projection.LastChunkEventID = ""
	projection.LastChunkEventSeq = 0
	projection.LastChunkPayloadHash = ""
	projection.ActiveChunks = nil
}

func validStepEndReason(reason StepEndReason) bool {
	switch reason {
	case StepEndCompleted, StepEndMaxTokens, StepEndBlocked, StepEndAborted, StepEndError, StepEndInterrupted, StepEndToolCalls:
		return true
	default:
		return false
	}
}

func (projection *AgentProjection) applyTurnEnd(event session.Event) error {
	if projection.ActiveTurnID == "" || (event.TurnID != "" && event.TurnID != projection.ActiveTurnID) || projection.ActiveStepClaimID != "" {
		return fmt.Errorf("%w: turn/end does not match active turn state", ErrInvalidInputClaim)
	}
	var payload TurnEndPayload
	if err := decodeEventPayload(event.Data, &payload); err != nil {
		return fmt.Errorf("%w: decode turn/end: %v", ErrInvalidInputClaim, err)
	}
	if projection.PendingClaimID != "" {
		claim, exists := projection.ClaimsByID[projection.PendingClaimID]
		if !exists {
			return fmt.Errorf("%w: pending claim %q is missing", ErrInvalidInputClaim, projection.PendingClaimID)
		}
		if payload.ClaimID != claim.ClaimID || !slices.Equal(payload.ConsumedInputIDs, claim.OrderedInputIDs) {
			return fmt.Errorf("%w: turn/end does not consume pending claim %q", ErrInvalidInputClaim, claim.ClaimID)
		}
		projection.PendingClaimID = ""
	} else if payload.ClaimID != "" || len(payload.ConsumedInputIDs) != 0 {
		return fmt.Errorf("%w: turn/end cannot carry claim credentials without a pending claim", ErrInvalidInputClaim)
	}
	projection.ActiveTurnID = ""
	projection.awaitingInitialClaimTurnID = ""
	return nil
}

func (projection *AgentProjection) queue(target InboxTarget) (*[]InboxItem, error) {
	switch target {
	case InboxNextTurn:
		return &projection.NextTurn, nil
	case InboxNextStep:
		return &projection.NextStep, nil
	default:
		return nil, fmt.Errorf("%w: unsupported inbox target %q", ErrInvalidInboxEvent, target)
	}
}

func isSyntheticEvent(data json.RawMessage) bool {
	var payload struct {
		Synthetic bool `json:"synthetic"`
	}
	return json.Unmarshal(data, &payload) == nil && payload.Synthetic
}

func pendingContainsTarget(pending *pendingInputClaim, target InboxTarget) bool {
	if pending == nil {
		return false
	}
	return slices.Contains(pending.SourceTargets, target)
}

func countClaimTarget(targets []InboxTarget, target InboxTarget) int {
	count := 0
	for _, candidate := range targets {
		if candidate == target {
			count++
		}
	}
	return count
}

func nextClaimStepIndex(projection *AgentProjection, turnID string) uint32 {
	var highest uint32
	for _, claim := range projection.ClaimsByID {
		if claim.TurnID == turnID && claim.ProposedStepIndex > highest {
			highest = claim.ProposedStepIndex
		}
	}
	return highest + 1
}

func validTarget(target InboxTarget) bool {
	return target == InboxNextTurn || target == InboxNextStep
}

func validSource(source InputSource) bool {
	return source == InputSourceUser || source == InputSourceExtension || source == InputSourceSystem
}

func validPayloadHash(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func placementFor(target InboxTarget, source InputSource) string {
	if target == InboxNextTurn {
		return "queued"
	}
	if source == InputSourceUser {
		return "steering"
	}
	return "context"
}

func idempotencyIndexKey(commandKind CommandKind, key string) string {
	return string(commandKind) + "\x00" + key
}

func claimStepKey(turnID string, proposedStepIndex uint32) string {
	return fmt.Sprintf("%s\x00%d", turnID, proposedStepIndex)
}

func decodeEventPayload(data json.RawMessage, target any) error {
	if len(data) == 0 || !json.Valid(data) {
		return errors.New("payload is not valid json")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return err
	}
	return nil
}

func validateCanonicalJSONValue(data json.RawMessage) error {
	if len(data) == 0 || !json.Valid(data) {
		return errors.New("value is not valid json")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	return validateJSONNumberSafety(value)
}

func validateJSONNumberSafety(value any) error {
	switch typed := value.(type) {
	case json.Number:
		rational, ok := new(big.Rat).SetString(typed.String())
		if !ok {
			return fmt.Errorf("number %q is invalid", typed)
		}
		absolute := new(big.Rat).Abs(rational)
		if absolute.Cmp(new(big.Rat).SetInt64(9007199254740991)) > 0 {
			return fmt.Errorf("number %q exceeds the accepted I-JSON range", typed)
		}
	case []any:
		for _, item := range typed {
			if err := validateJSONNumberSafety(item); err != nil {
				return err
			}
		}
	case map[string]any:
		for _, item := range typed {
			if err := validateJSONNumberSafety(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func cloneInboxItems(items []InboxItem) []InboxItem {
	if items == nil {
		return nil
	}
	cloned := make([]InboxItem, len(items))
	for index, item := range items {
		cloned[index] = cloneInboxItem(item)
	}
	return cloned
}

func cloneInboxItem(item InboxItem) InboxItem {
	content := item.Content
	item.Content = make([]ContentBlock, len(content))
	for index, block := range content {
		block.Data = append(json.RawMessage(nil), block.Data...)
		item.Content[index] = block
	}
	if item.Metadata != nil {
		metadata := make(map[string]json.RawMessage, len(item.Metadata))
		for key, value := range item.Metadata {
			metadata[key] = append(json.RawMessage(nil), value...)
		}
		item.Metadata = metadata
	}
	return item
}

func cloneAttemptChunkReceipts(receipts []AttemptChunkReceipt) []AttemptChunkReceipt {
	if receipts == nil {
		return nil
	}
	cloned := make([]AttemptChunkReceipt, len(receipts))
	for index, receipt := range receipts {
		receipt.Chunk = cloneAttemptStreamChunk(receipt.Chunk)
		cloned[index] = receipt
	}
	return cloned
}

func cloneAttemptStreamChunk(chunk llm.StreamChunk) llm.StreamChunk {
	chunk.ProviderRaw = append(json.RawMessage(nil), chunk.ProviderRaw...)
	if chunk.Block != nil {
		block := *chunk.Block
		block.ToolResult = append(json.RawMessage(nil), block.ToolResult...)
		chunk.Block = &block
	}
	if chunk.Usage != nil {
		usage := *chunk.Usage
		usage.CacheReadTokens = cloneInt64(usage.CacheReadTokens)
		usage.CacheWriteTokens = cloneInt64(usage.CacheWriteTokens)
		usage.ReasoningTokens = cloneInt64(usage.ReasoningTokens)
		chunk.Usage = &usage
	}
	if chunk.Finish != nil {
		finish := *chunk.Finish
		if finish.Failure != nil {
			failure := *finish.Failure
			failure.Status = cloneInt(failure.Status)
			failure.ProviderRetryAfterMs = cloneInt64(failure.ProviderRetryAfterMs)
			failure.RetryableHint = cloneBool(failure.RetryableHint)
			failure.CauseChain = append([]llm.FailureCause(nil), failure.CauseChain...)
			finish.Failure = &failure
		}
		chunk.Finish = &finish
	}
	return chunk
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
