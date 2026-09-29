package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

type ErrorNormalizer func(error) LlmFailure

type Runtime struct {
	provider   Provider
	normalizer ErrorNormalizer
}

func NewRuntime(provider Provider, normalizer ErrorNormalizer) *Runtime {
	if normalizer == nil {
		normalizer = DefaultErrorNormalizer
	}
	return &Runtime{provider: provider, normalizer: normalizer}
}

// Stream converts provider creation failures into a canonical one-terminal
// stream, so callers use one terminal handling path for all model failures.
func (runtime *Runtime) Stream(ctx context.Context, request ModelRequest) (Stream, error) {
	if runtime == nil || runtime.provider == nil {
		return newTerminalStream(errorFinish(LlmFailure{Code: FailureNoAdapter, Message: "LLM provider is not configured"})), nil
	}
	adapterStream, err := runtime.provider.Stream(ctx, cloneModelRequest(request))
	if err != nil {
		cause := err
		if ctxErr := ctx.Err(); ctxErr != nil {
			cause = ctxErr
		}
		failure := runtime.normalize(cause)
		if errors.Is(cause, context.Canceled) {
			return newTerminalStream(abortedFinish(failure)), nil
		}
		return newTerminalStream(errorFinish(failure)), nil
	}
	if adapterStream == nil {
		return newTerminalStream(errorFinish(LlmFailure{Code: FailureProtocol, Message: "LLM provider returned a nil stream"})), nil
	}
	return &runtimeStream{adapter: adapterStream, normalize: runtime.normalize}, nil
}

func (runtime *Runtime) normalize(err error) LlmFailure {
	normalizer := DefaultErrorNormalizer
	if runtime != nil && runtime.normalizer != nil {
		normalizer = runtime.normalizer
	}
	failure := normalizer(err)
	if failure.Code == "" {
		failure.Code = FailureUnknown
	}
	if failure.Message == "" && err != nil {
		failure.Message = err.Error()
	}
	return cloneFailure(failure)
}

func DefaultErrorNormalizer(err error) LlmFailure {
	switch {
	case errors.Is(err, context.Canceled):
		return LlmFailure{Code: FailureAborted, Message: "model request was cancelled"}
	case errors.Is(err, context.DeadlineExceeded):
		return LlmFailure{Code: FailureTimeout, Message: "model request deadline exceeded"}
	case err == nil:
		return LlmFailure{Code: FailureUnknown, Message: "unknown model failure"}
	default:
		return LlmFailure{Code: FailureUnknown, Message: err.Error(), CauseChain: []FailureCause{{Type: fmt.Sprintf("%T", err), Message: err.Error()}}}
	}
}

type runtimeStream struct {
	adapter           Stream
	normalize         ErrorNormalizer
	prefrozenTerminal bool

	nextActive atomic.Bool
	closeOnce  sync.Once
	closeErr   error

	stateMu sync.Mutex
	done    bool
	closed  bool
}

func (stream *runtimeStream) Next(ctx context.Context) (StreamChunk, error) {
	if !stream.nextActive.CompareAndSwap(false, true) {
		finish, err := stream.publishFinish(errorFinish(LlmFailure{Code: FailureProtocol, Message: "concurrent Stream.Next calls are forbidden"}))
		_ = stream.closeAdapter()
		return finish, err
	}
	defer stream.nextActive.Store(false)

	if stream.isDoneOrClosed() {
		return StreamChunk{}, io.EOF
	}
	if !stream.prefrozenTerminal {
		if err := ctx.Err(); err != nil {
			return stream.publishFinish(stream.contextFinish(err))
		}
	}

	chunk, err := stream.adapter.Next(ctx)
	if err != nil {
		return stream.finishForReadError(ctx, err)
	}
	chunk = cloneStreamChunk(chunk)
	if err := validateStreamChunk(chunk); err != nil {
		return stream.publishFinish(errorFinish(LlmFailure{Code: FailureProtocol, Message: err.Error()}))
	}
	if chunk.Kind != StreamChunkFinish {
		return stream.publishData(chunk)
	}

	// Provider adapters must freeze a protocol terminal locally. The lookahead
	// therefore confirms io.EOF without another network read.
	lookahead, lookaheadErr := stream.adapter.Next(ctx)
	if lookaheadErr == nil {
		return stream.publishFinish(errorFinish(LlmFailure{
			Code:    FailureProtocol,
			Message: fmt.Sprintf("provider emitted %q after terminal finish", lookahead.Kind),
		}))
	}
	if !errors.Is(lookaheadErr, io.EOF) {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return stream.publishFinish(stream.contextFinish(ctxErr))
		}
		return stream.publishFinish(errorFinish(LlmFailure{Code: FailureProtocol, Message: "provider terminal was not followed by local EOF"}))
	}
	if !isZeroStreamChunk(lookahead) {
		return stream.publishFinish(errorFinish(LlmFailure{Code: FailureProtocol, Message: "provider returned data together with terminal EOF"}))
	}
	return stream.publishFinish(chunk)
}

func (stream *runtimeStream) Close() error {
	stream.stateMu.Lock()
	stream.closed = true
	stream.stateMu.Unlock()
	return stream.closeAdapter()
}

func (stream *runtimeStream) closeAdapter() error {
	stream.closeOnce.Do(func() {
		stream.closeErr = stream.adapter.Close()
	})
	return stream.closeErr
}

func (stream *runtimeStream) finishForReadError(ctx context.Context, err error) (StreamChunk, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return stream.publishFinish(stream.contextFinish(ctxErr))
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return stream.publishFinish(stream.contextFinish(err))
	}
	if errors.Is(err, io.EOF) {
		return stream.publishFinish(errorFinish(LlmFailure{Code: FailureStreamClosed, Message: "provider stream closed before terminal finish"}))
	}
	return stream.publishFinish(errorFinish(stream.normalizeFailure(err)))
}

func (stream *runtimeStream) normalizeFailure(err error) LlmFailure {
	failure := stream.normalize(err)
	if failure.Code == "" {
		failure.Code = FailureUnknown
	}
	if failure.Message == "" && err != nil {
		failure.Message = err.Error()
	}
	return cloneFailure(failure)
}

func (stream *runtimeStream) contextFinish(err error) StreamChunk {
	failure := stream.normalizeFailure(err)
	if errors.Is(err, context.Canceled) {
		return abortedFinish(failure)
	}
	return errorFinish(failure)
}

func (stream *runtimeStream) isDoneOrClosed() bool {
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	return stream.done || stream.closed
}

func (stream *runtimeStream) publishData(chunk StreamChunk) (StreamChunk, error) {
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	if stream.done || stream.closed {
		return StreamChunk{}, io.EOF
	}
	return cloneStreamChunk(chunk), nil
}

func (stream *runtimeStream) publishFinish(chunk StreamChunk) (StreamChunk, error) {
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	if stream.done || stream.closed {
		return StreamChunk{}, io.EOF
	}
	stream.done = true
	return cloneStreamChunk(chunk), nil
}

type frozenTerminalAdapter struct {
	finish StreamChunk
	mu     sync.Mutex
	read   bool
	closed bool
}

func newTerminalStream(finish StreamChunk) Stream {
	adapter := &frozenTerminalAdapter{finish: cloneStreamChunk(finish)}
	return &runtimeStream{adapter: adapter, normalize: DefaultErrorNormalizer, prefrozenTerminal: true}
}

func (stream *frozenTerminalAdapter) Next(context.Context) (StreamChunk, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.closed || stream.read {
		return StreamChunk{}, io.EOF
	}
	stream.read = true
	return cloneStreamChunk(stream.finish), nil
}

func (stream *frozenTerminalAdapter) Close() error {
	stream.mu.Lock()
	stream.closed = true
	stream.mu.Unlock()
	return nil
}

func errorFinish(failure LlmFailure) StreamChunk {
	cloned := cloneFailure(failure)
	return StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishError, Failure: &cloned}}
}

func abortedFinish(failure LlmFailure) StreamChunk {
	failure.Code = FailureAborted
	cloned := cloneFailure(failure)
	return StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishAborted, Failure: &cloned}}
}

func isZeroStreamChunk(chunk StreamChunk) bool {
	return chunk.Kind == "" &&
		chunk.Index == 0 &&
		chunk.BlockType == "" &&
		chunk.Text == "" &&
		chunk.ToolCallID == "" &&
		chunk.ToolName == "" &&
		chunk.ArgumentsDelta == "" &&
		chunk.Block == nil &&
		chunk.Usage == nil &&
		chunk.Finish == nil &&
		chunk.ProviderRaw == nil
}

// ValidateStreamChunk verifies the provider-neutral chunk contract before the
// chunk crosses a package boundary or is persisted.
func ValidateStreamChunk(chunk StreamChunk) error {
	return validateStreamChunk(chunk)
}

func validateStreamChunk(chunk StreamChunk) error {
	switch chunk.Kind {
	case StreamChunkBlockStart, StreamChunkTextDelta, StreamChunkReasoningDelta, StreamChunkToolCallDelta, StreamChunkBlockEnd:
		if chunk.Finish != nil || chunk.Usage != nil {
			return fmt.Errorf("chunk %q carries terminal or usage payload", chunk.Kind)
		}
	case StreamChunkUsage:
		if chunk.Usage == nil || chunk.Finish != nil || hasBlockPayload(chunk) {
			return errors.New("usage chunk must carry only usage")
		}
		if err := validateUsage(*chunk.Usage); err != nil {
			return err
		}
	case StreamChunkFinish:
		if chunk.Finish == nil || chunk.Usage != nil || hasBlockPayload(chunk) {
			return errors.New("finish chunk must carry only finish reason")
		}
		if err := validateFinish(*chunk.Finish); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported stream chunk kind %q", chunk.Kind)
	}
	return nil
}

func hasBlockPayload(chunk StreamChunk) bool {
	return chunk.Index != 0 ||
		chunk.BlockType != "" ||
		chunk.Text != "" ||
		chunk.ToolCallID != "" ||
		chunk.ToolName != "" ||
		chunk.ArgumentsDelta != "" ||
		chunk.Block != nil
}

func validateFinish(finish FinishReason) error {
	switch finish.Kind {
	case FinishStop, FinishToolCalls, FinishMaxTokens:
		if finish.Failure != nil {
			return fmt.Errorf("successful finish %q cannot carry a failure", finish.Kind)
		}
	case FinishAborted, FinishError:
		if finish.Failure == nil || finish.Failure.Code == "" {
			return fmt.Errorf("finish %q requires a coded failure", finish.Kind)
		}
	default:
		return fmt.Errorf("unsupported finish kind %q", finish.Kind)
	}
	return nil
}

func validateUsage(usage TokenUsage) error {
	if usage.InputTokens < 0 || usage.OutputTokens < 0 || negativeOptional(usage.CacheReadTokens) || negativeOptional(usage.CacheWriteTokens) || negativeOptional(usage.ReasoningTokens) {
		return errors.New("token usage values must be non-negative")
	}
	return nil
}

func negativeOptional(value *int64) bool {
	return value != nil && *value < 0
}

func cloneStreamChunk(chunk StreamChunk) StreamChunk {
	chunk.ProviderRaw = cloneRawMessage(chunk.ProviderRaw)
	if chunk.Block != nil {
		block := *chunk.Block
		block.ToolResult = cloneRawMessage(block.ToolResult)
		chunk.Block = &block
	}
	if chunk.Usage != nil {
		usage := *chunk.Usage
		usage.CacheReadTokens = cloneInt64Pointer(usage.CacheReadTokens)
		usage.CacheWriteTokens = cloneInt64Pointer(usage.CacheWriteTokens)
		usage.ReasoningTokens = cloneInt64Pointer(usage.ReasoningTokens)
		chunk.Usage = &usage
	}
	if chunk.Finish != nil {
		finish := *chunk.Finish
		if finish.Failure != nil {
			failure := cloneFailure(*finish.Failure)
			finish.Failure = &failure
		}
		chunk.Finish = &finish
	}
	return chunk
}

func cloneModelRequest(request ModelRequest) ModelRequest {
	request.Temperature = cloneFloat64Pointer(request.Temperature)
	request.MaxTokens = cloneIntPointer(request.MaxTokens)
	if request.Reasoning != nil {
		reasoning := *request.Reasoning
		reasoning.MaxTokens = cloneIntPointer(reasoning.MaxTokens)
		request.Reasoning = &reasoning
	}
	if request.Messages != nil {
		messages := make([]Message, len(request.Messages))
		for messageIndex, message := range request.Messages {
			if message.Content != nil {
				message.Content = make([]ContentBlock, len(message.Content))
				copy(message.Content, request.Messages[messageIndex].Content)
			}
			for blockIndex := range message.Content {
				message.Content[blockIndex].ToolResult = cloneRawMessage(message.Content[blockIndex].ToolResult)
			}
			messages[messageIndex] = message
		}
		request.Messages = messages
	}
	if request.Tools != nil {
		tools := make([]ToolDefinition, len(request.Tools))
		copy(tools, request.Tools)
		request.Tools = tools
		for index := range request.Tools {
			request.Tools[index].InputSchema = cloneRawMessage(request.Tools[index].InputSchema)
		}
	}
	if request.Metadata != nil {
		metadata := make(map[string]json.RawMessage, len(request.Metadata))
		for key, value := range request.Metadata {
			metadata[key] = cloneRawMessage(value)
		}
		request.Metadata = metadata
	}
	return request
}

func cloneFailure(failure LlmFailure) LlmFailure {
	failure.Status = cloneIntPointer(failure.Status)
	failure.ProviderRetryAfterMs = cloneInt64Pointer(failure.ProviderRetryAfterMs)
	failure.RetryableHint = cloneBoolPointer(failure.RetryableHint)
	if failure.CauseChain != nil {
		failure.CauseChain = append(make([]FailureCause, 0, len(failure.CauseChain)), failure.CauseChain...)
	}
	return failure
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(value))
	copy(cloned, value)
	return cloned
}

func cloneFloat64Pointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneBoolPointer(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
