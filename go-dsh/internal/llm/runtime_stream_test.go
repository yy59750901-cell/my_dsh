package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestRuntimeStreamConfirmsProviderTerminalWithLocalEOF(t *testing.T) {
	adapter := &scriptedStream{results: []streamResult{
		{chunk: StreamChunk{Kind: StreamChunkTextDelta, Index: 0, BlockType: ContentBlockText, Text: "hello"}},
		{chunk: StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}},
		{err: io.EOF},
	}}
	stream := runtimeStreamForTest(t, adapter)

	chunk, err := stream.Next(context.Background())
	if err != nil || chunk.Kind != StreamChunkTextDelta || chunk.Text != "hello" {
		t.Fatalf("first Next = %+v, %v", chunk, err)
	}
	finish, err := stream.Next(context.Background())
	if err != nil || finish.Kind != StreamChunkFinish || finish.Finish == nil || finish.Finish.Kind != FinishStop {
		t.Fatalf("finish Next = %+v, %v", finish, err)
	}
	if adapter.nextCalls() != 3 {
		t.Fatalf("adapter Next calls = %d, want terminal lookahead call", adapter.nextCalls())
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("post-terminal error = %v, want io.EOF", err)
	}
	if adapter.nextCalls() != 3 {
		t.Fatal("runtime accessed provider after publishing terminal")
	}
}

func TestRuntimeStreamConvertsPrematureEOFToStreamClosed(t *testing.T) {
	adapter := &scriptedStream{results: []streamResult{{err: io.EOF}}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureStreamClosed)
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("post-terminal error = %v, want io.EOF", err)
	}
}

func TestRuntimeStreamConvertsCreationErrorToSingleTerminal(t *testing.T) {
	providerErr := errors.New("provider unavailable")
	runtime := NewRuntime(providerFunc(func(context.Context, ModelRequest) (Stream, error) {
		return nil, providerErr
	}), func(err error) LlmFailure {
		return LlmFailure{Code: FailureTransport, Message: err.Error()}
	})
	stream, err := runtime.Stream(context.Background(), ModelRequest{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureTransport)
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("post-terminal error = %v, want io.EOF", err)
	}
}

func TestRuntimeStreamPreservesFrozenCreationFailureWhenConsumerContextIsCancelled(t *testing.T) {
	runtime := NewRuntime(providerFunc(func(context.Context, ModelRequest) (Stream, error) {
		return nil, errors.New("provider unavailable")
	}), func(err error) LlmFailure {
		return LlmFailure{Code: FailureTransport, Message: err.Error()}
	})
	stream, err := runtime.Stream(context.Background(), ModelRequest{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	finish, err := stream.Next(ctx)
	assertFailureFinish(t, finish, err, FinishError, FailureTransport)
}

func TestRuntimeStreamConvertsCreationCancellationToAbortedFinish(t *testing.T) {
	runtime := NewRuntime(providerFunc(func(context.Context, ModelRequest) (Stream, error) {
		return nil, context.Canceled
	}), nil)
	stream, err := runtime.Stream(context.Background(), ModelRequest{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishAborted, FailureAborted)
}

func TestRuntimeStreamUsesRequestDeadlineAsCreationFailureCause(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	runtime := NewRuntime(providerFunc(func(context.Context, ModelRequest) (Stream, error) {
		return nil, context.Canceled
	}), nil)
	stream, err := runtime.Stream(ctx, ModelRequest{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureTimeout)
}

func TestRuntimeStreamConvertsCancellationToAbortedFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := &scriptedStream{results: []streamResult{{chunk: StreamChunk{Kind: StreamChunkTextDelta, Text: "unreachable"}}}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(ctx)
	assertFailureFinish(t, finish, err, FinishAborted, FailureAborted)
	if adapter.nextCalls() != 0 {
		t.Fatal("cancelled context reached provider stream")
	}
}

func TestRuntimeStreamConvertsDeadlineToTimeoutFinish(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	adapter := &scriptedStream{results: []streamResult{{chunk: StreamChunk{Kind: StreamChunkTextDelta, Text: "unreachable"}}}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(ctx)
	assertFailureFinish(t, finish, err, FinishError, FailureTimeout)
	if adapter.nextCalls() != 0 {
		t.Fatal("expired context reached provider stream")
	}
}

func TestRuntimeStreamRejectsConcurrentNextWithOneProtocolTerminal(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	adapter := &blockingStream{entered: entered, release: release}
	stream := runtimeStreamForTest(t, adapter)

	firstResult := make(chan streamResult, 1)
	go func() {
		chunk, err := stream.Next(context.Background())
		firstResult <- streamResult{chunk: chunk, err: err}
	}()
	<-entered

	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureProtocol)
	var first streamResult
	select {
	case first = <-firstResult:
	case <-time.After(time.Second):
		t.Fatal("protocol terminal did not close the adapter and unblock the original Next")
	}
	if !errors.Is(first.err, io.EOF) {
		t.Fatalf("original Next error = %v, want io.EOF after protocol terminal", first.err)
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("post-terminal error = %v, want io.EOF", err)
	}
}

func TestRuntimeStreamPublishPathsDoNotReleaseNextOwnership(t *testing.T) {
	tests := []struct {
		name    string
		publish func(*runtimeStream) (StreamChunk, error)
	}{
		{
			name: "data",
			publish: func(stream *runtimeStream) (StreamChunk, error) {
				return stream.publishData(StreamChunk{Kind: StreamChunkTextDelta, Text: "chunk"})
			},
		},
		{
			name: "finish",
			publish: func(stream *runtimeStream) (StreamChunk, error) {
				return stream.publishFinish(StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stream := &runtimeStream{}
			stream.nextActive.Store(true)
			if _, err := test.publish(stream); err != nil {
				t.Fatal(err)
			}
			if !stream.nextActive.Load() {
				t.Fatal("publication released Next ownership before the caller returned")
			}
		})
	}
}

func TestRuntimeStreamRejectsDataAfterProviderTerminal(t *testing.T) {
	adapter := &scriptedStream{results: []streamResult{
		{chunk: StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}},
		{chunk: StreamChunk{Kind: StreamChunkUsage, Usage: &TokenUsage{InputTokens: 1}}},
	}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureProtocol)
}

func TestRuntimeStreamRejectsDataReturnedWithTerminalEOF(t *testing.T) {
	adapter := &scriptedStream{results: []streamResult{
		{chunk: StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}},
		{chunk: StreamChunk{Kind: StreamChunkUsage, Usage: &TokenUsage{InputTokens: 1}}, err: io.EOF},
	}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureProtocol)
}

func TestRuntimeStreamRejectsInvalidUsageAsProtocolFailure(t *testing.T) {
	adapter := &scriptedStream{results: []streamResult{{chunk: StreamChunk{
		Kind: StreamChunkUsage, Usage: &TokenUsage{InputTokens: -1},
	}}}}
	stream := runtimeStreamForTest(t, adapter)

	finish, err := stream.Next(context.Background())
	assertFailureFinish(t, finish, err, FinishError, FailureProtocol)
}

func TestRuntimeStreamRejectsUsageOrFinishWithBlockPayload(t *testing.T) {
	tests := []StreamChunk{
		{Kind: StreamChunkUsage, Usage: &TokenUsage{}, Text: "unexpected"},
		{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}, BlockType: ContentBlockText},
	}
	for _, providerChunk := range tests {
		adapter := &scriptedStream{results: []streamResult{{chunk: providerChunk}}}
		stream := runtimeStreamForTest(t, adapter)
		finish, err := stream.Next(context.Background())
		assertFailureFinish(t, finish, err, FinishError, FailureProtocol)
	}
}

func TestRuntimeStreamCloseIsIdempotentAndStopsNext(t *testing.T) {
	adapter := &scriptedStream{}
	stream := runtimeStreamForTest(t, adapter)
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if adapter.closeCalls() != 1 {
		t.Fatalf("adapter Close calls = %d, want 1", adapter.closeCalls())
	}
	if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("Next after Close = %v, want io.EOF", err)
	}
}

func TestRuntimeStreamCloseSuppressesInflightProviderData(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	adapter := &blockingStream{entered: entered, release: release}
	stream := runtimeStreamForTest(t, adapter)
	result := make(chan streamResult, 1)
	go func() {
		chunk, err := stream.Next(context.Background())
		result <- streamResult{chunk: chunk, err: err}
	}()
	<-entered
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case next := <-result:
		if !errors.Is(next.err, io.EOF) {
			t.Fatalf("in-flight Next = %+v, want io.EOF after Close", next)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter Close did not unblock the in-flight Next")
	}
}

func TestRuntimePassesDetachedModelRequestToProvider(t *testing.T) {
	metadata := []byte(`{"trace":true}`)
	toolSchema := []byte(`{"type":"object"}`)
	toolResult := []byte(`{"value":1}`)
	request := ModelRequest{
		Model: "test-model",
		Messages: []Message{
			{Role: RoleTool, Content: []ContentBlock{{Type: ContentBlockToolResult, ToolResult: toolResult}}},
			{Role: RoleAssistant, Content: []ContentBlock{}},
		},
		Tools:    []ToolDefinition{{Name: "lookup", InputSchema: toolSchema}},
		Metadata: map[string]json.RawMessage{"trace": metadata, "empty": {}},
	}
	captured := make(chan ModelRequest, 1)
	runtime := NewRuntime(providerFunc(func(_ context.Context, providerRequest ModelRequest) (Stream, error) {
		captured <- providerRequest
		return &scriptedStream{results: []streamResult{
			{chunk: StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}},
			{err: io.EOF},
		}}, nil
	}), nil)
	stream, err := runtime.Stream(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	providerRequest := <-captured
	metadata[0], toolSchema[0], toolResult[0] = '[', '[', '['
	request.Messages[0].Content[0].Text = "mutated"
	if string(providerRequest.Metadata["trace"]) != `{"trace":true}` || string(providerRequest.Tools[0].InputSchema) != `{"type":"object"}` || string(providerRequest.Messages[0].Content[0].ToolResult) != `{"value":1}` || providerRequest.Messages[0].Content[0].Text != "" {
		t.Fatalf("caller mutation leaked into provider request: %+v", providerRequest)
	}
	if providerRequest.Messages[1].Content == nil || providerRequest.Metadata["empty"] == nil {
		t.Fatal("request clone collapsed non-nil empty values")
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStreamClonesProviderOwnedPayloads(t *testing.T) {
	raw := []byte(`{"secret":"redacted"}`)
	blockResult := []byte(`{"ok":true}`)
	adapter := &scriptedStream{results: []streamResult{
		{chunk: StreamChunk{Kind: StreamChunkBlockEnd, Block: &ContentBlock{Type: ContentBlockToolResult, ToolResult: blockResult}, ProviderRaw: raw}},
	}}
	stream := runtimeStreamForTest(t, adapter)
	chunk, err := stream.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = '['
	blockResult[0] = '['
	if string(chunk.ProviderRaw) != `{"secret":"redacted"}` || string(chunk.Block.ToolResult) != `{"ok":true}` {
		t.Fatalf("provider mutation leaked into canonical chunk: %+v", chunk)
	}
}

func runtimeStreamForTest(t *testing.T, adapter Stream) Stream {
	t.Helper()
	runtime := NewRuntime(providerFunc(func(context.Context, ModelRequest) (Stream, error) {
		return adapter, nil
	}), nil)
	stream, err := runtime.Stream(context.Background(), ModelRequest{Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func assertFailureFinish(t *testing.T, chunk StreamChunk, err error, kind FinishKind, code FailureCode) {
	t.Helper()
	if err != nil || chunk.Kind != StreamChunkFinish || chunk.Finish == nil || chunk.Finish.Kind != kind || chunk.Finish.Failure == nil || chunk.Finish.Failure.Code != code {
		t.Fatalf("finish = %+v, error = %v, want kind=%s code=%s", chunk, err, kind, code)
	}
}

type providerFunc func(context.Context, ModelRequest) (Stream, error)

func (function providerFunc) Stream(ctx context.Context, request ModelRequest) (Stream, error) {
	return function(ctx, request)
}

type streamResult struct {
	chunk StreamChunk
	err   error
}

type scriptedStream struct {
	mu         sync.Mutex
	results    []streamResult
	nextCount  int
	closeCount int
}

func (stream *scriptedStream) Next(context.Context) (StreamChunk, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.nextCount++
	if len(stream.results) == 0 {
		return StreamChunk{}, io.EOF
	}
	result := stream.results[0]
	stream.results = stream.results[1:]
	return result.chunk, result.err
}

func (stream *scriptedStream) Close() error {
	stream.mu.Lock()
	stream.closeCount++
	stream.mu.Unlock()
	return nil
}

func (stream *scriptedStream) nextCalls() int {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.nextCount
}

func (stream *scriptedStream) closeCalls() int {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	return stream.closeCount
}

type blockingStream struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (stream *blockingStream) Next(context.Context) (StreamChunk, error) {
	stream.enteredOnce.Do(func() { close(stream.entered) })
	select {
	case <-stream.release:
		return StreamChunk{Kind: StreamChunkTextDelta, Text: "late"}, nil
	case <-time.After(5 * time.Second):
		return StreamChunk{}, errors.New("test stream did not release")
	}
}

func (stream *blockingStream) Close() error {
	stream.unblock()
	return nil
}

func (stream *blockingStream) unblock() {
	stream.releaseOnce.Do(func() { close(stream.release) })
}
