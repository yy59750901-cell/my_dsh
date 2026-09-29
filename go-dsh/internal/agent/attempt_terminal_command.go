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

const EventModelUsage = "model/usage"

type ModelUsagePayload struct {
	Attempt uint32         `json:"attempt"`
	Usage   llm.TokenUsage `json:"usage"`
}

type AssistantMessagePayload struct {
	Message     llm.Message `json:"message"`
	Interrupted bool        `json:"interrupted,omitempty"`
}

type ModelErrorPayload struct {
	Attempt uint32         `json:"attempt"`
	Failure llm.LlmFailure `json:"failure"`
}

type AttemptTerminalEventIDs struct {
	Usage      string
	Message    string
	ModelError string
	StepEnd    string
}

// CommitAttemptTerminalCommand rebuilds the terminal view exclusively from
// durable assistant/chunk receipts and atomically closes the active Step.
type CommitAttemptTerminalCommand struct {
	ExpectedTurnID     string
	ExpectedStepID     string
	ExpectedStepIndex  uint32
	ExpectedAttempt    uint32
	ExpectedPhase      AttemptPhase
	ExpectedChunkCount uint64
	EventIDs           AttemptTerminalEventIDs
	OccurredAt         time.Time
}

func (command CommitAttemptTerminalCommand) Decide(_ context.Context, snapshot session.Snapshot) ([]session.NewEvent, error) {
	if command.ExpectedTurnID == "" || command.ExpectedStepID == "" || command.ExpectedStepIndex == 0 || command.ExpectedAttempt == 0 || command.ExpectedPhase == AttemptPhaseNone || command.OccurredAt.IsZero() || command.EventIDs.StepEnd == "" {
		return nil, fmt.Errorf("%w: invalid attempt terminal proposal", ErrInvalidDriverTransition)
	}
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return nil, fmt.Errorf("%w: agent projection is missing", ErrInvalidDriverTransition)
	}
	if projection.CancelCause != nil {
		return nil, ErrStaleDriverActivity
	}
	core := snapshot.Core()
	if core.Status != session.StatusRunning || core.TurnID != command.ExpectedTurnID || core.StepID != command.ExpectedStepID || projection.ActiveTurnID != command.ExpectedTurnID || projection.ActiveStepID != command.ExpectedStepID || projection.ActiveStepIndex != command.ExpectedStepIndex || projection.ActiveAttempt != command.ExpectedAttempt || projection.AttemptPhase != command.ExpectedPhase || projection.ActiveChunkCount != command.ExpectedChunkCount {
		return nil, fmt.Errorf("%w: active attempt terminal boundary changed", ErrStaleDriverActivity)
	}

	terminal, err := rebuildTerminalAssembly(projection, command.ExpectedTurnID, command.ExpectedStepID, command.ExpectedAttempt)
	if err != nil {
		return nil, err
	}
	reason, err := stepEndReasonForFinish(terminal.Finish.Kind)
	if err != nil {
		return nil, err
	}
	if err := validateTerminalEventIDs(command.EventIDs, terminal); err != nil {
		return nil, err
	}

	events := make([]session.NewEvent, 0, 3)
	if terminal.Usage != nil {
		data, marshalErr := json.Marshal(ModelUsagePayload{Attempt: command.ExpectedAttempt, Usage: *terminal.Usage})
		if marshalErr != nil {
			return nil, marshalErr
		}
		events = append(events, session.NewEvent{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     EventModelUsage,
			EventID:       command.EventIDs.Usage,
			OccurredAt:    command.OccurredAt.UTC(),
			ReplayPolicy:  session.ReplayIgnorable,
			Data:          data,
			TurnID:        command.ExpectedTurnID,
			StepID:        command.ExpectedStepID,
		})
	}
	if terminal.Message != nil {
		data, marshalErr := json.Marshal(AssistantMessagePayload{Message: *terminal.Message, Interrupted: terminal.Finish.Kind == llm.FinishAborted})
		if marshalErr != nil {
			return nil, marshalErr
		}
		events = append(events, session.NewEvent{
			SchemaVersion:   session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor},
			EventType:       session.SurfaceEventAssistantMessage,
			EventID:         command.EventIDs.Message,
			OccurredAt:      command.OccurredAt.UTC(),
			ReplayPolicy:    session.ReplayRequired,
			Data:            data,
			TurnID:          command.ExpectedTurnID,
			StepID:          command.ExpectedStepID,
			SourceEventSeqs: append([]uint64(nil), terminal.SourceEventSeqs...),
			SurfaceOp:       session.AppendSurfaceOp(),
		})
	}
	if terminal.Finish.Kind == llm.FinishError {
		data, marshalErr := json.Marshal(ModelErrorPayload{Attempt: command.ExpectedAttempt, Failure: *terminal.Finish.Failure})
		if marshalErr != nil {
			return nil, marshalErr
		}
		events = append(events, session.NewEvent{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "model/error",
			EventID:       command.EventIDs.ModelError,
			OccurredAt:    command.OccurredAt.UTC(),
			ReplayPolicy:  session.ReplayRequired,
			Data:          data,
			TurnID:        command.ExpectedTurnID,
			StepID:        command.ExpectedStepID,
		})
	}
	stepEndData, err := json.Marshal(StepEndPayload{
		Reason:       reason,
		Attempt:      command.ExpectedAttempt,
		UsageMissing: terminal.Usage == nil,
	})
	if err != nil {
		return nil, err
	}
	events = append(events, session.NewEvent{
		SchemaVersion: session.SchemaVersion{Major: 1},
		EventType:     "step/end",
		EventID:       command.EventIDs.StepEnd,
		OccurredAt:    command.OccurredAt.UTC(),
		ReplayPolicy:  session.ReplayRequired,
		Data:          stepEndData,
		TurnID:        command.ExpectedTurnID,
		StepID:        command.ExpectedStepID,
	})
	return events, nil
}

func rebuildAttemptAssembler(projection *AgentProjection, turnID, stepID string, attempt uint32) (*llm.BlockAssembler, error) {
	if projection == nil || uint64(len(projection.ActiveChunks)) != projection.ActiveChunkCount {
		return nil, fmt.Errorf("%w: durable chunk receipts are incomplete", ErrInvalidDriverTransition)
	}
	identity := llm.AttemptIdentity{TurnID: turnID, StepID: stepID, Attempt: attempt}
	assembler, err := llm.NewBlockAssembler(identity)
	if err != nil {
		return nil, err
	}
	for position, receipt := range projection.ActiveChunks {
		if receipt.ChunkIndex != uint64(position) || receipt.EventID == "" || receipt.EventSeq == 0 || receipt.PayloadHash == "" {
			return nil, fmt.Errorf("%w: invalid durable chunk receipt at position %d", ErrInvalidDriverTransition, position)
		}
		mutation, planErr := assembler.PlanPush(receipt.Chunk)
		if planErr != nil {
			return nil, planErr
		}
		if mutation.ChunkIndex() != receipt.ChunkIndex {
			return nil, fmt.Errorf("%w: chunk cursor mismatch at position %d", ErrInvalidDriverTransition, position)
		}
		if commitErr := mutation.Commit(llm.CommittedChunk{
			Identity:       identity,
			ChunkIndex:     receipt.ChunkIndex,
			SourceEventSeq: receipt.EventSeq,
			Chunk:          receipt.Chunk,
		}); commitErr != nil {
			return nil, commitErr
		}
	}
	return assembler, nil
}

func rebuildTerminalAssembly(projection *AgentProjection, turnID, stepID string, attempt uint32) (llm.TerminalAssembly, error) {
	assembler, err := rebuildAttemptAssembler(projection, turnID, stepID, attempt)
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	terminal, err := assembler.Terminal()
	if err == nil && projection.Configured && terminal.Message == nil && (terminal.Finish.Kind == llm.FinishStop || terminal.Finish.Kind == llm.FinishMaxTokens) {
		terminal.Finish = llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureEmptyResponse, Message: "模型返回空内容"}}
	}
	return terminal, err
}

func cancelledPrefixEvent(p *AgentProjection) (*session.NewEvent, error) {
	if p.ActiveAttempt == 0 || p.AttemptPhase != AttemptPhaseRequested {
		return nil, nil
	}
	assembler, err := rebuildAttemptAssembler(p, p.ActiveTurnID, p.ActiveStepID, p.ActiveAttempt)
	if err != nil {
		return nil, err
	}
	view := assembler.Snapshot()
	if view.Finish != nil && view.Finish.Kind == llm.FinishError {
		return nil, nil
	}
	message := llm.Message{Role: llm.RoleAssistant, Source: llm.MessageSourceStep}
	kept := make(map[uint32]bool)
	for _, block := range view.Blocks {
		if (block.Type == llm.ContentBlockText || block.Type == llm.ContentBlockReasoning) && block.Text != "" {
			message.Content = append(message.Content, block)
			kept[block.Index] = true
		}
	}
	if len(message.Content) == 0 {
		return nil, nil
	}
	// 仅引用保留文本块的真实贡献事件，不引用工具块、usage 或 finish。
	sources := make([]uint64, 0)
	for _, receipt := range p.ActiveChunks {
		if kept[receipt.Chunk.Index] && slices.Contains(view.SourceEventSeqs, receipt.EventSeq) {
			sources = append(sources, receipt.EventSeq)
		}
	}
	e := surfaceFact("assistant/message", p.ActiveTurnID, p.ActiveStepID, AssistantMessagePayload{Message: message, Interrupted: true}, sources)
	return &e, nil
}

func stepEndReasonForFinish(kind llm.FinishKind) (StepEndReason, error) {
	switch kind {
	case llm.FinishStop:
		return StepEndCompleted, nil
	case llm.FinishToolCalls:
		return StepEndToolCalls, nil
	case llm.FinishMaxTokens:
		return StepEndMaxTokens, nil
	case llm.FinishAborted:
		return StepEndAborted, nil
	case llm.FinishError:
		return StepEndError, nil
	default:
		return "", fmt.Errorf("%w: unsupported finish kind %q", ErrInvalidDriverTransition, kind)
	}
}

func validateTerminalEventIDs(ids AttemptTerminalEventIDs, terminal llm.TerminalAssembly) error {
	required := []string{ids.StepEnd}
	if terminal.Usage != nil {
		if ids.Usage == "" {
			return fmt.Errorf("%w: usage event id is required", ErrInvalidDriverTransition)
		}
		required = append(required, ids.Usage)
	}
	if terminal.Message != nil {
		if ids.Message == "" {
			return fmt.Errorf("%w: assistant message event id is required", ErrInvalidDriverTransition)
		}
		required = append(required, ids.Message)
	}
	if terminal.Finish.Kind == llm.FinishError {
		if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code == "" || ids.ModelError == "" {
			return fmt.Errorf("%w: model error requires failure and event id", ErrInvalidDriverTransition)
		}
		required = append(required, ids.ModelError)
	}
	seen := make(map[string]struct{}, len(required))
	for _, eventID := range required {
		if _, duplicate := seen[eventID]; duplicate {
			return fmt.Errorf("%w: terminal event ids must be unique", ErrInvalidDriverTransition)
		}
		seen[eventID] = struct{}{}
	}
	if terminal.Message != nil && !slices.IsSorted(terminal.SourceEventSeqs) {
		return fmt.Errorf("%w: terminal source event sequences must be sorted", ErrInvalidDriverTransition)
	}
	return nil
}

func terminalBatchCommitted(events []session.Event, ids AttemptTerminalEventIDs) bool {
	if len(events) == 0 || events[len(events)-1].EventType != "step/end" || events[len(events)-1].EventID != ids.StepEnd {
		return false
	}
	for _, event := range events {
		if event.EventID == ids.StepEnd {
			return true
		}
	}
	return false
}

var errAttemptTerminalNotCommitted = errors.New("attempt terminal batch was not committed")
