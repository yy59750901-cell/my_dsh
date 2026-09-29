package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

var (
	ErrInvalidAttemptRun        = errors.New("invalid active attempt run")
	ErrAttemptStreamClosedEarly = errors.New("active attempt stream closed before finish")
)

type StepActivity struct {
	TurnID    string
	StepID    string
	StepIndex uint32
}

type AttemptActivity struct {
	TurnID    string
	StepID    string
	StepIndex uint32
	Attempt   uint32
	Phase     AttemptPhase
}

type AttemptRunnerOptions struct {
	IDGenerator   InboxIDGenerator
	Now           func() time.Time
	CommitTimeout time.Duration
}

type attemptEventSubmitter interface {
	Submit(context.Context, session.Command) ([]session.Event, error)
	Snapshot() session.Snapshot
}

// ActiveAttemptRunner consumes a canonical Runtime stream outside the Session
// Actor mailbox and persists every accepted chunk before advancing the live
// BlockAssembler.
type ActiveAttemptRunner struct {
	submitter     attemptEventSubmitter
	runtime       *llm.Runtime
	ids           InboxIDGenerator
	now           func() time.Time
	commitTimeout time.Duration
}

func NewActiveAttemptRunner(submitter attemptEventSubmitter, runtime *llm.Runtime, options AttemptRunnerOptions) (*ActiveAttemptRunner, error) {
	if submitter == nil || runtime == nil {
		return nil, fmt.Errorf("%w: submitter and LLM runtime are required", ErrInvalidAttemptRun)
	}
	if options.IDGenerator == nil {
		options.IDGenerator = cryptoInboxIDGenerator{}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.CommitTimeout <= 0 {
		options.CommitTimeout = 30 * time.Second
	}
	return &ActiveAttemptRunner{
		submitter:     submitter,
		runtime:       runtime,
		ids:           options.IDGenerator,
		now:           options.Now,
		commitTimeout: options.CommitTimeout,
	}, nil
}

// StartAndRunInitialAttempt persists model/requested, consumes the canonical
// stream, and atomically commits the terminal facts with step/end.
func (runner *ActiveAttemptRunner) StartAndRunInitialAttempt(ctx context.Context, step StepActivity, request llm.ModelRequest, requestSummary json.RawMessage) (llm.TerminalAssembly, error) {
	if runner == nil || step.TurnID == "" || step.StepID == "" || step.StepIndex == 0 || request.Attempt != 1 || request.TurnID != step.TurnID || request.StepID != step.StepID || request.Model == "" {
		return llm.TerminalAssembly{}, fmt.Errorf("%w: invalid initial attempt request", ErrInvalidAttemptRun)
	}
	requestEventID, err := runner.ids.NewID("event")
	if err != nil {
		return llm.TerminalAssembly{}, fmt.Errorf("generate model requested event id: %w", err)
	}
	startCommand := StartInitialAttemptCommand{
		ExpectedTurnID:    step.TurnID,
		ExpectedStepID:    step.StepID,
		ExpectedStepIndex: step.StepIndex,
		ExpectedAttempt:   0,
		ExpectedPhase:     AttemptPhaseNone,
		Model:             request.Model,
		RequestSummary:    append(json.RawMessage(nil), requestSummary...),
		EventID:           requestEventID,
		OccurredAt:        runner.now().UTC(),
	}
	commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), runner.commitTimeout)
	committed, submitErr := runner.submitter.Submit(commitCtx, startCommand)
	cancelCommit()
	if !singleCommittedEvent(committed, EventModelRequested, requestEventID) {
		if submitErr != nil {
			return llm.TerminalAssembly{}, submitErr
		}
		return llm.TerminalAssembly{}, fmt.Errorf("%w: model/requested was not committed", ErrInvalidAttemptRun)
	}

	activity := AttemptActivity{
		TurnID: step.TurnID, StepID: step.StepID, StepIndex: step.StepIndex,
		Attempt: 1, Phase: AttemptPhaseRequested,
	}
	terminal, chunkCount, err := runner.runStream(ctx, activity, request)
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	ids, err := runner.newTerminalEventIDs(terminal)
	if err != nil {
		return llm.TerminalAssembly{}, err
	}
	terminalCommand := CommitAttemptTerminalCommand{
		ExpectedTurnID:     activity.TurnID,
		ExpectedStepID:     activity.StepID,
		ExpectedStepIndex:  activity.StepIndex,
		ExpectedAttempt:    activity.Attempt,
		ExpectedPhase:      activity.Phase,
		ExpectedChunkCount: chunkCount,
		EventIDs:           ids,
		OccurredAt:         runner.now().UTC(),
	}
	commitCtx, cancelCommit = context.WithTimeout(context.WithoutCancel(ctx), runner.commitTimeout)
	committed, submitErr = runner.submitter.Submit(commitCtx, terminalCommand)
	cancelCommit()
	if !terminalBatchCommitted(committed, ids) {
		if submitErr != nil {
			return llm.TerminalAssembly{}, submitErr
		}
		return llm.TerminalAssembly{}, errAttemptTerminalNotCommitted
	}
	return terminal, nil
}

func (runner *ActiveAttemptRunner) Run(ctx context.Context, activity AttemptActivity, request llm.ModelRequest) (llm.TerminalAssembly, error) {
	terminal, _, err := runner.runStream(ctx, activity, request)
	return terminal, err
}

func (runner *ActiveAttemptRunner) runStream(ctx context.Context, activity AttemptActivity, request llm.ModelRequest) (terminal llm.TerminalAssembly, chunkCount uint64, err error) {
	if runner == nil || runner.submitter == nil || runner.runtime == nil || activity.TurnID == "" || activity.StepID == "" || activity.StepIndex == 0 || activity.Attempt == 0 || activity.Phase == AttemptPhaseNone {
		return llm.TerminalAssembly{}, 0, fmt.Errorf("%w: complete active attempt identity is required", ErrInvalidAttemptRun)
	}
	if request.TurnID != activity.TurnID || request.StepID != activity.StepID || request.Attempt != activity.Attempt || request.SessionID == "" || request.Model == "" {
		return llm.TerminalAssembly{}, 0, fmt.Errorf("%w: model request does not match active attempt", ErrInvalidAttemptRun)
	}
	if err := validateActiveAttemptRequest(runner.submitter.Snapshot(), activity, request); err != nil {
		return llm.TerminalAssembly{}, 0, err
	}

	assembler, err := llm.NewBlockAssembler(llm.AttemptIdentity{TurnID: activity.TurnID, StepID: activity.StepID, Attempt: activity.Attempt})
	if err != nil {
		return llm.TerminalAssembly{}, 0, err
	}
	stream, err := runner.runtime.Stream(ctx, request)
	if err != nil {
		return llm.TerminalAssembly{}, 0, err
	}
	terminalReady := false
	defer func() {
		closeErr := stream.Close()
		if !terminalReady {
			err = errors.Join(err, closeErr)
		}
	}()

	for {
		chunk, nextErr := stream.Next(ctx)
		if nextErr != nil {
			if errors.Is(nextErr, io.EOF) {
				return llm.TerminalAssembly{}, chunkCount, ErrAttemptStreamClosedEarly
			}
			return llm.TerminalAssembly{}, chunkCount, nextErr
		}
		eventID, idErr := runner.ids.NewID("event")
		if idErr != nil {
			return llm.TerminalAssembly{}, chunkCount, fmt.Errorf("generate assistant chunk event id: %w", idErr)
		}
		commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), runner.commitTimeout)
		_, persistErr := persistAttemptChunk(commitCtx, runner.submitter, assembler, activity, chunk, eventID, runner.now().UTC())
		cancelCommit()
		if persistErr != nil {
			return llm.TerminalAssembly{}, chunkCount, persistErr
		}
		chunkCount++
		if chunk.Kind == llm.StreamChunkFinish {
			terminal, terminalErr := assembler.Terminal()
			if terminalErr == nil {
				terminalReady = true
			}
			return terminal, chunkCount, terminalErr
		}
	}
}

func validateActiveAttemptRequest(snapshot session.Snapshot, activity AttemptActivity, request llm.ModelRequest) error {
	projection, ok := ProjectionFrom(snapshot)
	if !ok {
		return fmt.Errorf("%w: agent projection is missing", ErrInvalidAttemptRun)
	}
	var summary modelRequestSummary
	if len(projection.Request) > 0 && json.Unmarshal(projection.Request, &summary) != nil {
		return fmt.Errorf("%w: invalid durable request summary", ErrStaleDriverActivity)
	}
	// 未配置的基础命令允许旧摘要；Harness 请求必须提供冻结的语义哈希。
	if projection.Configured || summary.RequestHash != "" {
		actual, err := modelRequestHash(request)
		if err != nil {
			return err
		}
		if actual != summary.RequestHash {
			return fmt.Errorf("%w: request differs from durable hash", ErrStaleDriverActivity)
		}
	}
	core := snapshot.Core()
	if snapshot.SessionID != request.SessionID || core.Status != session.StatusRunning || core.TurnID != activity.TurnID || core.StepID != activity.StepID || projection.ActiveTurnID != activity.TurnID || projection.ActiveStepID != activity.StepID || projection.ActiveStepIndex != activity.StepIndex || projection.ActiveAttempt != activity.Attempt || projection.AttemptPhase != activity.Phase || projection.ActiveModel != request.Model || projection.ActiveChunkCount != 0 || len(projection.ActiveChunks) != 0 {
		return fmt.Errorf("%w: model request does not match durable active attempt", ErrStaleDriverActivity)
	}
	return nil
}

type modelRequestSummary struct {
	RequestID    string           `json:"request_id"`
	Model        string           `json:"model"`
	Attempt      uint32           `json:"attempt"`
	MessageCount int              `json:"message_count"`
	RoleCounts   map[llm.Role]int `json:"role_counts"`
	ToolCount    int              `json:"tool_count"`
	RequestHash  string           `json:"request_hash"`
}

func modelRequestHash(request llm.ModelRequest) (string, error) {
	// 仅包含影响模型语义的字段；不纳入身份、attempt 或诊断/凭据 metadata。
	payload := struct {
		Model       string                `json:"model"`
		Messages    []llm.Message         `json:"messages"`
		Tools       []llm.ToolDefinition  `json:"tools"`
		Temperature *float64              `json:"temperature,omitempty"`
		MaxTokens   *int                  `json:"max_tokens,omitempty"`
		Reasoning   *llm.ReasoningOptions `json:"reasoning,omitempty"`
	}{request.Model, request.Messages, request.Tools, request.Temperature, request.MaxTokens, request.Reasoning}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return canonicalPayloadHash(data)
}

func summarizeModelRequest(request llm.ModelRequest) (json.RawMessage, error) {
	hash, err := modelRequestHash(request)
	if err != nil {
		return nil, err
	}
	roles := make(map[llm.Role]int)
	for _, message := range request.Messages {
		roles[message.Role]++
	}
	return json.Marshal(modelRequestSummary{
		RequestID: request.RequestID, Model: request.Model, Attempt: request.Attempt,
		MessageCount: len(request.Messages), RoleCounts: roles, ToolCount: len(request.Tools), RequestHash: hash,
	})
}

func (runner *ActiveAttemptRunner) newTerminalEventIDs(terminal llm.TerminalAssembly) (AttemptTerminalEventIDs, error) {
	return newAttemptTerminalEventIDs(runner.ids, "event", terminal)
}

func newAttemptTerminalEventIDs(generator InboxIDGenerator, prefix string, terminal llm.TerminalAssembly) (AttemptTerminalEventIDs, error) {
	if generator == nil {
		return AttemptTerminalEventIDs{}, fmt.Errorf("%w: terminal event id generator is required", ErrInvalidAttemptRun)
	}
	ids := AttemptTerminalEventIDs{}
	var err error
	if terminal.Usage != nil {
		ids.Usage, err = generator.NewID(prefix)
		if err != nil {
			return AttemptTerminalEventIDs{}, fmt.Errorf("generate model usage event id: %w", err)
		}
	}
	if terminal.Message != nil {
		ids.Message, err = generator.NewID(prefix)
		if err != nil {
			return AttemptTerminalEventIDs{}, fmt.Errorf("generate assistant message event id: %w", err)
		}
	}
	if terminal.Finish.Kind == llm.FinishError {
		ids.ModelError, err = generator.NewID(prefix)
		if err != nil {
			return AttemptTerminalEventIDs{}, fmt.Errorf("generate model error event id: %w", err)
		}
	}
	ids.StepEnd, err = generator.NewID(prefix)
	if err != nil {
		return AttemptTerminalEventIDs{}, fmt.Errorf("generate step end event id: %w", err)
	}
	return ids, nil
}

func singleCommittedEvent(events []session.Event, eventType, eventID string) bool {
	return len(events) == 1 && events[0].EventType == eventType && events[0].EventID == eventID && events[0].Seq != 0
}

func persistAttemptChunk(ctx context.Context, submitter attemptEventSubmitter, assembler *llm.BlockAssembler, activity AttemptActivity, chunk llm.StreamChunk, eventID string, occurredAt time.Time) (session.Event, error) {
	if submitter == nil || assembler == nil {
		return session.Event{}, fmt.Errorf("%w: submitter and assembler are required", ErrInvalidAttemptRun)
	}
	mutation, err := assembler.PlanPush(chunk)
	if err != nil {
		return session.Event{}, err
	}
	command := AppendAssistantChunkCommand{
		ExpectedTurnID:     activity.TurnID,
		ExpectedStepID:     activity.StepID,
		ExpectedStepIndex:  activity.StepIndex,
		ExpectedAttempt:    activity.Attempt,
		ExpectedPhase:      activity.Phase,
		ExpectedChunkIndex: mutation.ChunkIndex(),
		Chunk:              chunk,
		EventID:            eventID,
		OccurredAt:         occurredAt,
	}
	committed, submitErr := submitter.Submit(ctx, command)
	if len(committed) == 1 {
		return commitAssemblyMutation(mutation, command, committed[0])
	}
	if submitErr != nil {
		return session.Event{}, submitErr
	}
	return session.Event{}, fmt.Errorf("%w: assistant chunk append returned %d events", session.ErrConflict, len(committed))
}

func commitAssemblyMutation(mutation *llm.AssemblyMutation, command AppendAssistantChunkCommand, event session.Event) (session.Event, error) {
	expectedData, err := command.payloadData()
	if err != nil {
		return session.Event{}, err
	}
	expectedHash, err := canonicalPayloadHash(expectedData)
	if err != nil {
		return session.Event{}, err
	}
	committedHash, err := canonicalPayloadHash(event.Data)
	if err != nil {
		return session.Event{}, fmt.Errorf("%w: committed assistant chunk payload is invalid", session.ErrConflict)
	}
	if event.EventType != EventAssistantChunk || event.EventID != command.EventID || event.Seq == 0 || event.TurnID != command.ExpectedTurnID || event.StepID != command.ExpectedStepID || event.ReplayPolicy != session.ReplayIgnorable || expectedHash != committedHash {
		return session.Event{}, fmt.Errorf("%w: committed assistant chunk receipt is invalid", session.ErrConflict)
	}
	if err := mutation.Commit(llm.CommittedChunk{
		Identity:       llm.AttemptIdentity{TurnID: command.ExpectedTurnID, StepID: command.ExpectedStepID, Attempt: command.ExpectedAttempt},
		ChunkIndex:     command.ExpectedChunkIndex,
		SourceEventSeq: event.Seq,
		Chunk:          command.Chunk,
	}); err != nil {
		return session.Event{}, err
	}
	return event, nil
}
