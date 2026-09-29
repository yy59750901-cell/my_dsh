package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

var (
	ErrInvalidClaimRequest = errors.New("invalid agent claim request")
	ErrNoClaimableInput    = errors.New("no claimable agent input")
)

type AgentRepairInitializer struct {
	IDGenerator InboxIDGenerator
	Now         func() time.Time
}

func AgentActorInitializers() []session.ActorInitializer {
	return []session.ActorInitializer{harnessRepairInitializer{}, AgentRepairInitializer{}, session.CoreRepairInitializer{}}
}

func (initializer AgentRepairInitializer) Initialize(ctx context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, nil
	}
	if projection.PendingClaimID == "" {
		return initializer.recoverDurableAttemptTerminal(ctx, snapshot, projection)
	}
	core := snapshot.Core()
	claim, exists := projection.Claim(projection.PendingClaimID)
	if !exists || core.TurnID == "" || core.TurnID != claim.TurnID || core.StepID != "" || projection.ActiveTurnID != claim.TurnID {
		return nil, fmt.Errorf("%w: pending claim %q does not match the incomplete core turn", ErrInvalidInputClaim, projection.PendingClaimID)
	}
	generator := initializer.IDGenerator
	if generator == nil {
		generator = cryptoInboxIDGenerator{}
	}
	turnEndID, err := generator.NewID("repair")
	if err != nil {
		return nil, fmt.Errorf("generate repair turn event id: %w", err)
	}
	repairedID, err := generator.NewID("repair")
	if err != nil {
		return nil, fmt.Errorf("generate repair marker event id: %w", err)
	}
	turnEndData, err := json.Marshal(TurnEndPayload{
		Reason:           "interrupted",
		ClaimID:          claim.ClaimID,
		ConsumedInputIDs: append([]string(nil), claim.OrderedInputIDs...),
		Synthetic:        true,
	})
	if err != nil {
		return nil, err
	}
	repairedData, err := json.Marshal(map[string]any{
		"reason":          "incomplete-claimed-turn",
		"synthetic":       true,
		"closed":          []string{"turn"},
		"previousHeadSeq": snapshot.HeadSeq,
		"claim_id":        claim.ClaimID,
	})
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "turn/end",
			EventID:       turnEndID,
			ReplayPolicy:  session.ReplayRequired,
			Data:          turnEndData,
			TurnID:        claim.TurnID,
		},
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     session.EventSessionRepaired,
			EventID:       repairedID,
			ReplayPolicy:  session.ReplayIgnorable,
			Data:          repairedData,
		},
	}, nil
}

func (initializer AgentRepairInitializer) recoverDurableAttemptTerminal(ctx context.Context, snapshot session.Snapshot, projection *AgentProjection) ([]session.NewEvent, error) {
	if projection == nil || projection.ActiveStepID == "" || projection.ActiveAttempt == 0 || len(projection.ActiveChunks) == 0 || projection.ActiveChunks[len(projection.ActiveChunks)-1].Chunk.Kind != llm.StreamChunkFinish {
		return nil, nil
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != projection.ActiveTurnID || core.StepID != projection.ActiveStepID {
		return nil, fmt.Errorf("%w: durable terminal does not match active core step", ErrInvalidDriverTransition)
	}
	terminal, err := rebuildTerminalAssembly(projection, projection.ActiveTurnID, projection.ActiveStepID, projection.ActiveAttempt)
	if err != nil {
		return nil, fmt.Errorf("rebuild durable terminal during repair: %w", err)
	}
	generator := initializer.IDGenerator
	if generator == nil {
		generator = cryptoInboxIDGenerator{}
	}
	ids, err := newAttemptTerminalEventIDs(generator, "repair", terminal)
	if err != nil {
		return nil, err
	}
	now := initializer.Now
	if now == nil {
		now = time.Now
	}
	command := CommitAttemptTerminalCommand{
		ExpectedTurnID:     projection.ActiveTurnID,
		ExpectedStepID:     projection.ActiveStepID,
		ExpectedStepIndex:  projection.ActiveStepIndex,
		ExpectedAttempt:    projection.ActiveAttempt,
		ExpectedPhase:      projection.AttemptPhase,
		ExpectedChunkCount: projection.ActiveChunkCount,
		EventIDs:           ids,
		OccurredAt:         now().UTC(),
	}
	return command.Decide(ctx, snapshot)
}

func (writer *InboxWriter) StartTurnAndClaim(ctx context.Context, sessionID string) (InputClaim, error) {
	if writer == nil || writer.sessions == nil || writer.ids == nil || writer.now == nil || sessionID == "" {
		return InputClaim{}, fmt.Errorf("%w: initialized writer and session id are required", ErrInvalidClaimRequest)
	}
	turnID, err := writer.ids.NewID("turn")
	if err != nil {
		return InputClaim{}, fmt.Errorf("generate turn id: %w", err)
	}
	claimID, err := writer.ids.NewID("claim")
	if err != nil {
		return InputClaim{}, fmt.Errorf("generate claim id: %w", err)
	}
	eventIDs, err := writer.newEventIDs(4)
	if err != nil {
		return InputClaim{}, err
	}
	command := &claimInboxCommand{
		mode:              claimModeStartTurn,
		turnID:            turnID,
		claimID:           claimID,
		proposedStepIndex: 1,
		eventIDs:          eventIDs,
		occurredAt:        writer.now().UTC(),
	}
	return writer.submitClaim(ctx, sessionID, command)
}

func (writer *InboxWriter) ClaimForProposedStep(ctx context.Context, sessionID, turnID string, proposedStepIndex uint32) (InputClaim, error) {
	if writer == nil || writer.sessions == nil || writer.ids == nil || writer.now == nil || sessionID == "" || turnID == "" || proposedStepIndex == 0 {
		return InputClaim{}, fmt.Errorf("%w: initialized writer, session id, turn id, and proposed step index are required", ErrInvalidClaimRequest)
	}
	claimID, err := writer.ids.NewID("claim")
	if err != nil {
		return InputClaim{}, fmt.Errorf("generate claim id: %w", err)
	}
	eventIDs, err := writer.newEventIDs(2)
	if err != nil {
		return InputClaim{}, err
	}
	command := &claimInboxCommand{
		mode:              claimModeNextStep,
		turnID:            turnID,
		claimID:           claimID,
		proposedStepIndex: proposedStepIndex,
		eventIDs:          eventIDs,
		occurredAt:        writer.now().UTC(),
	}
	return writer.submitClaim(ctx, sessionID, command)
}

func (writer *InboxWriter) submitClaim(ctx context.Context, sessionID string, command *claimInboxCommand) (InputClaim, error) {
	lease, err := writer.sessions.GetOrLoad(ctx, sessionID)
	if err != nil {
		return InputClaim{}, err
	}
	defer lease.Release()

	_, submitErr := lease.Actor().Submit(ctx, command)
	projection, ok := ProjectionFrom(lease.Actor().Snapshot())
	if submitErr != nil {
		if ok {
			if claim, exists := projection.Claim(command.claimID); exists && command.matchesCommittedClaim(claim) {
				return claim, nil
			}
		}
		return InputClaim{}, submitErr
	}
	if ok {
		if claim, exists := projection.Claim(command.claimID); exists && command.matchesCommittedClaim(claim) {
			return claim, nil
		}
	}
	if !ok {
		return InputClaim{}, errors.New("agent projection missing after input claim")
	}
	return InputClaim{}, fmt.Errorf("claim %q missing after append", command.claimID)
}

func (writer *InboxWriter) newEventIDs(count int) ([]string, error) {
	ids := make([]string, count)
	for index := range ids {
		value, err := writer.ids.NewID("event")
		if err != nil {
			return nil, fmt.Errorf("generate event id: %w", err)
		}
		ids[index] = value
	}
	return ids, nil
}

type claimMode string

const (
	claimModeStartTurn claimMode = "start-turn"
	claimModeNextStep  claimMode = "next-step"
)

type claimInboxCommand struct {
	mode              claimMode
	turnID            string
	claimID           string
	proposedStepIndex uint32
	eventIDs          []string
	occurredAt        time.Time

	expectedInputIDs []string
	expectedTargets  []InboxTarget
}

func (command *claimInboxCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, errors.New("agent projection missing")
	}
	if _, exists := projection.Claim(command.claimID); exists {
		return nil, fmt.Errorf("%w: duplicate claim id %q", ErrInvalidClaimRequest, command.claimID)
	}

	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	switch command.mode {
	case claimModeStartTurn:
		return command.decideStartTurn(snapshot, projection)
	case claimModeNextStep:
		return command.decideNextStep(snapshot, projection)
	default:
		return nil, fmt.Errorf("%w: unsupported claim mode %q", ErrInvalidClaimRequest, command.mode)
	}
}

func (command *claimInboxCommand) decideStartTurn(snapshot session.Snapshot, projection *AgentProjection) ([]session.NewEvent, error) {
	core := snapshot.Core()
	if core.Status != session.StatusIdle || core.TurnID != "" || core.StepID != "" || projection.ActiveTurnID != "" {
		return nil, fmt.Errorf("%w: session already has active work", ErrInvalidClaimRequest)
	}
	if command.proposedStepIndex != 1 || len(command.eventIDs) != 4 {
		return nil, fmt.Errorf("%w: initial claim must target step 1", ErrInvalidClaimRequest)
	}
	if !projectionShouldWake(projection) {
		return nil, ErrNoClaimableInput
	}

	orderedItems := make([]InboxItem, 0, len(projection.NextStep)+1)
	targets := make([]InboxTarget, 0, len(projection.NextStep)+1)
	events := make([]session.NewEvent, 0, 4)
	turnData, err := json.Marshal(struct {
		ClaimID           string `json:"claim_id"`
		ProposedStepIndex uint32 `json:"proposed_step_index"`
	}{ClaimID: command.claimID, ProposedStepIndex: command.proposedStepIndex})
	if err != nil {
		return nil, err
	}
	events = append(events, command.newEvent(command.eventIDs[0], "turn/start", turnData, command.turnID))
	if len(projection.NextStep) != 0 {
		orderedItems = append(orderedItems, projection.NextStep...)
		for range projection.NextStep {
			targets = append(targets, InboxNextStep)
		}
		splice, err := command.claimSpliceEvent(command.eventIDs[1], InboxNextStep, len(projection.NextStep))
		if err != nil {
			return nil, err
		}
		events = append(events, splice)
	}
	if len(projection.NextTurn) != 0 {
		orderedItems = append(orderedItems, projection.NextTurn[0])
		targets = append(targets, InboxNextTurn)
		turnSplice, err := command.claimSpliceEvent(command.eventIDs[2], InboxNextTurn, 1)
		if err != nil {
			return nil, err
		}
		events = append(events, turnSplice)
	}
	marker, err := command.claimMarkerEvent(command.eventIDs[3], orderedItems, targets)
	if err != nil {
		return nil, err
	}
	events = append(events, marker)
	return events, nil
}

func (command *claimInboxCommand) decideNextStep(snapshot session.Snapshot, projection *AgentProjection) ([]session.NewEvent, error) {
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.turnID || core.StepID != "" || projection.ActiveTurnID != command.turnID {
		return nil, fmt.Errorf("%w: turn %q is not ready to claim a step", ErrInvalidClaimRequest, command.turnID)
	}
	if len(command.eventIDs) != 2 || command.proposedStepIndex != nextClaimStepIndex(projection, command.turnID) || command.proposedStepIndex <= 1 {
		return nil, fmt.Errorf("%w: subsequent claim must use the next step index", ErrInvalidClaimRequest)
	}
	if projection.PendingClaimID != "" || projection.ActiveStepClaimID != "" {
		return nil, fmt.Errorf("%w: claim %q has not been consumed", ErrInvalidClaimRequest, projection.PendingClaimID)
	}
	if len(projection.NextStep) == 0 {
		return nil, ErrNoClaimableInput
	}
	if _, exists := projection.ClaimIDByStep[claimStepKey(command.turnID, command.proposedStepIndex)]; exists {
		return nil, fmt.Errorf("%w: turn %q step %d was already claimed", ErrInvalidClaimRequest, command.turnID, command.proposedStepIndex)
	}

	items := cloneInboxItems(projection.NextStep)
	targets := make([]InboxTarget, len(items))
	for index := range targets {
		targets[index] = InboxNextStep
	}
	splice, err := command.claimSpliceEvent(command.eventIDs[0], InboxNextStep, len(items))
	if err != nil {
		return nil, err
	}
	marker, err := command.claimMarkerEvent(command.eventIDs[1], items, targets)
	if err != nil {
		return nil, err
	}
	return []session.NewEvent{splice, marker}, nil
}

func (command *claimInboxCommand) claimSpliceEvent(eventID string, target InboxTarget, count int) (session.NewEvent, error) {
	data, err := json.Marshal(InboxSplicedPayload{
		Target:      target,
		Start:       0,
		DeleteCount: count,
		Reason:      InboxReasonClaim,
		ClaimID:     command.claimID,
	})
	if err != nil {
		return session.NewEvent{}, fmt.Errorf("marshal claim splice: %w", err)
	}
	return command.newEvent(eventID, EventInboxSpliced, data, command.turnID), nil
}

func (command *claimInboxCommand) claimMarkerEvent(eventID string, items []InboxItem, targets []InboxTarget) (session.NewEvent, error) {
	inputIDs := make([]string, len(items))
	for index, item := range items {
		inputIDs[index] = item.InputID
	}
	command.expectedInputIDs = append([]string(nil), inputIDs...)
	command.expectedTargets = append([]InboxTarget(nil), targets...)
	data, err := json.Marshal(InputClaimedPayload{
		ClaimID:           command.claimID,
		TurnID:            command.turnID,
		ProposedStepIndex: command.proposedStepIndex,
		OrderedInputIDs:   inputIDs,
		SourceTargets:     append([]InboxTarget(nil), targets...),
	})
	if err != nil {
		return session.NewEvent{}, fmt.Errorf("marshal input claim: %w", err)
	}
	return command.newEvent(eventID, EventInputClaimed, data, command.turnID), nil
}

func (command *claimInboxCommand) matchesCommittedClaim(claim InputClaim) bool {
	return len(command.expectedInputIDs) != 0 &&
		claim.ClaimID == command.claimID &&
		claim.TurnID == command.turnID &&
		claim.ProposedStepIndex == command.proposedStepIndex &&
		slices.Equal(claim.OrderedInputIDs, command.expectedInputIDs) &&
		slices.Equal(claim.SourceTargets, command.expectedTargets)
}

func (command *claimInboxCommand) newEvent(eventID, eventType string, data json.RawMessage, turnID string) session.NewEvent {
	return session.NewEvent{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     eventType,
		EventID:       eventID,
		OccurredAt:    command.occurredAt,
		ReplayPolicy:  session.ReplayRequired,
		Data:          data,
		TurnID:        turnID,
	}
}
