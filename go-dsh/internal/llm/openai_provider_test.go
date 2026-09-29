package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func openAITestRequest() ModelRequest {
	return ModelRequest{Model: "test-model", Messages: []Message{{Role: RoleUser, Content: []ContentBlock{{Type: ContentBlockText, Text: "你好"}}}}}
}

func openAITestEvent(delta, finish string) string {
	reason := "null"
	if finish != "" {
		data, _ := json.Marshal(finish)
		reason = string(data)
	}
	return `data: {"choices":[{"index":0,"delta":` + delta + `,"finish_reason":` + reason + "}]}\n\n"
}

func openAITestProvider(t *testing.T, handler http.HandlerFunc, timeout time.Duration) *OpenAIProvider {
	t.Helper()
	server := httptest.NewServer(handler)
	client := server.Client()
	client.Timeout = timeout
	t.Cleanup(server.Close)
	t.Cleanup(client.CloseIdleConnections)
	provider, err := NewOpenAIProvider(OpenAIOptions{BaseURL: server.URL + "/v1/", APIKey: "test-secret-key", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func openAITestBody(t *testing.T, body string) *OpenAIProvider {
	t.Helper()
	return openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = io.WriteString(w, body)
	}, 3*time.Second)
}

// 用真实 Runtime 和 Assembler 的两阶段提交路径消费，不用替代实现放宽契约。
func consumeOpenAITest(t *testing.T, provider Provider, request ModelRequest) (TerminalAssembly, []StreamChunk) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := NewRuntime(provider, nil).Stream(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	identity := AttemptIdentity{TurnID: "test-turn", StepID: "test-step", Attempt: 1}
	assembler, err := NewBlockAssembler(identity)
	if err != nil {
		t.Fatal(err)
	}
	var chunks []StreamChunk
	for {
		chunk, err := stream.Next(ctx)
		if err != nil {
			t.Fatalf("终态前发生读取错误：%v", err)
		}
		if chunk.ProviderRaw != nil {
			t.Fatal("原始上游数据进入 canonical chunk")
		}
		mutation, err := assembler.PlanPush(chunk)
		if err != nil {
			t.Fatalf("Assembler 不接受 chunk %+v：%v", chunk, err)
		}
		if err := mutation.Commit(CommittedChunk{Identity: identity, ChunkIndex: mutation.ChunkIndex(), SourceEventSeq: uint64(len(chunks) + 1), Chunk: chunk}); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, chunk)
		if chunk.Kind == StreamChunkFinish {
			break
		}
		if len(chunks) > 10000 {
			t.Fatal("流没有终止")
		}
	}
	cancelled, cancelNext := context.WithCancel(context.Background())
	cancelNext()
	for range 2 {
		chunk, err := stream.Next(cancelled)
		if !errors.Is(err, io.EOF) || !isZeroStreamChunk(chunk) {
			t.Fatalf("终态后不是本地零值 EOF：%+v, %v", chunk, err)
		}
	}
	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	return terminal, chunks
}

func TestOpenAIRequestAuthAndRuntimeAssembly(t *testing.T) {
	captured := make(chan map[string]json.RawMessage, 1)
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer test-secret-key" || r.Header.Get("Accept") != "text/event-stream" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("请求方法、路径或固定请求头不匹配")
		}
		var payload map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		captured <- payload
		w.Header().Set("Content-Type", "text/event-stream")
		body := ": keepalive\n\n" + openAITestEvent(`{"role":"assistant","content":null}`, "") +
			openAITestEvent(`{"reasoning_content":"先思考"}`, "") +
			openAITestEvent(`{"content":"你好"}`, "") + openAITestEvent(`{"content":"，世界"}`, "stop") +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":8,\"prompt_tokens_details\":{\"cached_tokens\":3},\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n" + "data: [DONE]\n\n"
		_, _ = io.WriteString(w, body)
	}, 3*time.Second)
	request := openAITestRequest()
	temperature, maxTokens := 0.5, 100
	request.Temperature, request.MaxTokens = &temperature, &maxTokens
	request.Reasoning = &ReasoningOptions{Enabled: true, Effort: "low"}
	request.RequestID, request.SessionID = "private-request", "private-session"
	request.Metadata = map[string]json.RawMessage{"endpoint": json.RawMessage(`"https://untrusted.invalid/"`)}
	request.Tools = []ToolDefinition{{Name: "echo", Description: "回显", InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`)}}
	request.Messages = append([]Message{{Role: RoleSystem, Content: []ContentBlock{{Type: ContentBlockText, Text: "系统提示"}}}}, request.Messages...)
	terminal, chunks := consumeOpenAITest(t, provider, request)
	if terminal.Finish.Kind != FinishStop || terminal.Message == nil || len(terminal.Message.Content) != 2 || terminal.Message.Content[0].Text != "先思考" || terminal.Message.Content[1].Text != "你好，世界" {
		t.Fatalf("内容组装错误：%+v", terminal)
	}
	usage := terminal.Usage
	if usage == nil || usage.InputTokens != 20 || usage.OutputTokens != 8 || usage.CacheReadTokens == nil || *usage.CacheReadTokens != 3 || usage.ReasoningTokens == nil || *usage.ReasoningTokens != 2 {
		t.Fatalf("usage 错误：%+v", usage)
	}
	if chunks[len(chunks)-2].Kind != StreamChunkUsage {
		t.Fatal("独立 usage 必须先于 canonical finish")
	}
	payload := <-captured
	if string(payload["stream"]) != "true" || string(payload["stream_options"]) != `{"include_usage":true}` || string(payload["reasoning_effort"]) != `"low"` || string(payload["max_tokens"]) != "100" || string(payload["temperature"]) != "0.5" {
		t.Fatalf("请求字段映射错误：%s", mustOpenAIJSON(t, payload))
	}
	for _, field := range []string{"metadata", "session_id", "request_id", "turn_id", "step_id", "api_key"} {
		if _, exists := payload[field]; exists {
			t.Fatalf("不应透传内部字段 %s", field)
		}
	}
	var tools []openAIWireTool
	if json.Unmarshal(payload["tools"], &tools) != nil || len(tools) != 1 || tools[0].Type != "function" || tools[0].Function.Name != "echo" || string(tools[0].Function.Parameters) != string(request.Tools[0].InputSchema) {
		t.Fatalf("工具 schema 映射错误：%s", payload["tools"])
	}
}

func TestOpenAIFragmentedMultiToolAndToolHistory(t *testing.T) {
	body := openAITestEvent(`{"reasoning":"准备工具"}`, "") +
		openAITestEvent(`{"tool_calls":[{"index":7,"id":"call_","type":"function","function":{"name":"ec","arguments":"{\"text\":"}},{"index":2,"id":"other_","function":{"name":"su","arguments":"{\"n\":"}}]}`, "") +
		openAITestEvent(`{"tool_calls":[{"index":2,"id":"b","function":{"name":"m","arguments":"2}"}},{"index":7,"id":"a","function":{"name":"ho","arguments":"\"中文\"}"}}]}`, "tool_calls") +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n"
	terminal, chunks := consumeOpenAITest(t, openAITestBody(t, body), openAITestRequest())
	if terminal.Finish.Kind != FinishToolCalls || terminal.Message == nil || len(terminal.Message.Content) != 3 {
		t.Fatalf("工具终态错误：%+v", terminal)
	}
	blocks := terminal.Message.Content
	if blocks[1].ToolCallID != "call_a" || blocks[1].ToolName != "echo" || blocks[1].ArgumentsJSONRaw != `{"text":"中文"}` || !blocks[1].Complete || blocks[2].ToolCallID != "other_b" || blocks[2].ToolName != "sum" || blocks[2].ArgumentsJSONRaw != `{"n":2}` {
		t.Fatalf("工具分片拼接错误：%+v", blocks)
	}
	ids := 0
	for _, chunk := range chunks {
		if chunk.ToolCallID != "" {
			ids++
		}
	}
	if ids != 2 {
		t.Fatal("每个工具只能发布一次完整 ID")
	}
	request := openAITestRequest()
	request.Messages = append(request.Messages, *terminal.Message, Message{Role: RoleTool, Content: []ContentBlock{
		{Type: ContentBlockToolResult, ToolCallID: "call_a", ToolResult: json.RawMessage(`{"text":"中文"}`)},
		{Type: ContentBlockToolResult, ToolCallID: "other_b", ToolResult: json.RawMessage(`2`)},
	}})
	captured := make(chan []openAIMessage, 1)
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			Messages []openAIMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		captured <- payload.Messages
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, openAITestEvent(`{"content":"工具已完成"}`, "stop")+"data: [DONE]\n\n")
	}, 3*time.Second)
	answer, _ := consumeOpenAITest(t, provider, request)
	if answer.Finish.Kind != FinishStop {
		t.Fatalf("工具往返失败：%+v", answer)
	}
	messages := <-captured
	if len(messages) != 4 || messages[1].Role != RoleAssistant || len(messages[1].ToolCalls) != 2 || messages[1].ToolCalls[0].ID != "call_a" || messages[1].ToolCalls[0].Function.Arguments == nil || *messages[1].ToolCalls[0].Function.Arguments != `{"text":"中文"}` || messages[2].Role != RoleTool || messages[2].ToolCallID != "call_a" || messages[2].Content != `{"text":"中文"}` || messages[3].ToolCallID != "other_b" {
		t.Fatalf("工具历史映射错误：%+v", messages)
	}
}

func TestOpenAIHTTPFailuresAreStructuredAndPrivate(t *testing.T) {
	tests := []struct {
		status int
		code   FailureCode
		retry  bool
	}{
		{400, FailureInvalidRequest, false}, {401, FailureInvalidCredential, false},
		{403, FailureInvalidCredential, false}, {404, FailureModelNotFound, false},
		{429, FailureRateLimit, true}, {500, FailureTransport, true},
		{503, FailureTransport, true}, {504, FailureTimeout, true},
		{408, FailureTimeout, true}, {302, FailureProtocol, false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprint(test.status), func(t *testing.T) {
			provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "7")
				w.Header().Set("X-Request-ID", "upstream-private-body")
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, `{"error":{"message":"test-secret-key upstream-private-body"}}`)
			}, 3*time.Second)
			terminal, chunks := consumeOpenAITest(t, provider, openAITestRequest())
			failure := terminal.Finish.Failure
			if terminal.Finish.Kind != FinishError || failure == nil || failure.Code != test.code || failure.Status == nil || *failure.Status != test.status || failure.RetryableHint == nil || *failure.RetryableHint != test.retry {
				t.Fatalf("HTTP 错误归一化失败：%+v", failure)
			}
			if test.retry && (failure.ProviderRetryAfterMs == nil || *failure.ProviderRetryAfterMs != 7000) {
				t.Fatalf("Retry-After 归一化错误：%+v", failure)
			}
			assertOpenAIPrivate(t, mustOpenAIJSON(t, chunks))
		})
	}
}

func TestOpenAIRejectsAllRedirectsWithoutMutatingClient(t *testing.T) {
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Add(1) }))
	defer target.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var callbacks atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, status) }))
			defer server.Close()
			client := server.Client()
			client.Timeout = time.Second
			client.CheckRedirect = func(*http.Request, []*http.Request) error { callbacks.Add(1); return nil }
			provider, err := NewOpenAIProvider(OpenAIOptions{BaseURL: server.URL, APIKey: "test-secret-key", HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			terminal, _ := consumeOpenAITest(t, provider, openAITestRequest())
			if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureProtocol || reached.Load() != 0 || callbacks.Load() != 0 {
				t.Fatal("发生重定向或调用了外部回调")
			}
			_ = client.CheckRedirect(nil, nil)
			if callbacks.Load() != 1 || client.Timeout != time.Second {
				t.Fatal("调用方 client 被改写")
			}
		})
	}
}

func TestOpenAIEndpointConfiguration(t *testing.T) {
	for _, endpoint := range []string{"", "not-a-url", "ftp://localhost/v1", "http://example.invalid/v1", "https://user:test-secret-key@example.invalid", "https://example.invalid/v1?key=test-secret-key", "https://example.invalid/#test-secret-key", "https://example.invalid:99999/v1", "https://example.invalid:bad/v1", "https://example.invalid/v1?"} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := NewOpenAIProvider(OpenAIOptions{BaseURL: endpoint})
			if err == nil {
				t.Fatal("接受了无效 endpoint")
			}
			assertOpenAIPrivate(t, err.Error())
		})
	}
	for _, endpoint := range []string{"https://example.invalid/v1", "http://localhost:1234/v1/", "http://127.0.0.1/v1", "http://[::1]:1234/v1"} {
		provider, err := NewOpenAIProvider(OpenAIOptions{BaseURL: endpoint})
		if err != nil || provider.client.Timeout != openAIDefaultTimeout {
			t.Fatalf("有效配置失败：%v", err)
		}
	}
	if _, err := NewOpenAIProvider(OpenAIOptions{BaseURL: "https://example.invalid", APIKey: "secret\r\nX-Test: injected"}); err == nil {
		t.Fatal("接受了请求头注入")
	}
}

func TestOpenAISSETerminationAndValidation(t *testing.T) {
	text := openAITestEvent(`{"content":"内容"}`, "")
	finish := openAITestEvent(`{}`, "stop")
	tool := func(args string) string {
		encoded, _ := json.Marshal(args)
		return openAITestEvent(`{"tool_calls":[{"index":0,"id":"call_a","function":{"name":"echo","arguments":`+string(encoded)+`}}]}`, "tool_calls")
	}
	tests := []struct {
		name, body string
		code       FailureCode
		kind       FinishKind
	}{
		{"done-without-finish-reason", text + "data: [DONE]\n\n", "", FinishStop},
		{"finish-clean-eof", text + finish, "", FinishStop},
		{"standard-done", text + finish + "data: [DONE]\n\n", "", FinishStop},
		{"empty", "data: [DONE]\n\n", FailureEmptyResponse, FinishError},
		{"unexpected-eof", text, FailureStreamClosed, FinishError},
		{"empty-eof", "", FailureStreamClosed, FinishError},
		{"partial-frame", text + `data: {"choices":`, FailureStreamClosed, FinishError},
		{"partial-frame-after-finish", text + finish + `data: {`, FailureStreamClosed, FinishError},
		{"malformed-json", "data: test-secret-key upstream-private-body\n\n", FailureProtocol, FinishError},
		{"null-json", "data: null\n\n", FailureProtocol, FinishError},
		{"missing-choices", "data: {}\n\n", FailureProtocol, FinishError},
		{"error-event", "event: error\ndata: test-secret-key upstream-private-body\n\n", FailureProtocol, FinishError},
		{"error-envelope", "data: {\"error\":{\"message\":\"upstream-private-body\"}}\n\n", FailureProtocol, FinishError},
		{"invalid-utf8", "data: \xff\n\n", FailureProtocol, FinishError},
		{"wrong-choice", "data: {\"choices\":[{\"index\":1,\"delta\":{\"content\":\"x\"}}]}\n\n", FailureProtocol, FinishError},
		{"missing-index", "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n", FailureProtocol, FinishError},
		{"multiple-choices", "data: {\"choices\":[{\"index\":0},{\"index\":1}]}\n\n", FailureProtocol, FinishError},
		{"unknown-finish", text + openAITestEvent(`{}`, "upstream-private-body"), FailureProtocol, FinishError},
		{"duplicate-finish", text + finish + finish, FailureProtocol, FinishError},
		{"delta-after-finish", text + finish + text, FailureProtocol, FinishError},
		{"negative-usage", text + "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":-1}}\n\n", FailureProtocol, FinishError},
		{"negative-cached-usage", text + "data: {\"choices\":[],\"usage\":{\"prompt_tokens_details\":{\"cached_tokens\":-1}}}\n\n", FailureProtocol, FinishError},
		{"no-tool-for-tool-finish", text + openAITestEvent(`{}`, "tool_calls") + "data: [DONE]\n\n", FailureProtocol, FinishError},
		{"invalid-tool-json", tool(`{"text":`) + "data: [DONE]\n\n", FailureProtocol, FinishError},
		{"duplicate-tool-argument", tool(`{"text":1,"text":2}`) + "data: [DONE]\n\n", FailureProtocol, FinishError},
		{"array-tool-argument", tool(`[]`) + "data: [DONE]\n\n", FailureProtocol, FinishError},
		{"negative-tool-index", openAITestEvent(`{"tool_calls":[{"index":-1}]}`, ""), FailureProtocol, FinishError},
		{"missing-tool-index", openAITestEvent(`{"tool_calls":[{"id":"a"}]}`, ""), FailureProtocol, FinishError},
		{"oversized-line", "data: " + strings.Repeat("x", openAIMaxFrameBytes) + "\n\n", FailureProtocol, FinishError},
		{"oversized-multiline", strings.Repeat("data: "+strings.Repeat("x", 1024)+"\n", 1024) + "\n", FailureProtocol, FinishError},
		{"oversized-comments", strings.Repeat(":"+strings.Repeat("x", 1024)+"\n", 1024) + "\n", FailureProtocol, FinishError},
		{"max-tokens", text + openAITestEvent(`{}`, "length") + "data: [DONE]\n\n", "", FinishMaxTokens},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			terminal, chunks := consumeOpenAITest(t, openAITestBody(t, test.body), openAITestRequest())
			if terminal.Finish.Kind != test.kind {
				t.Fatalf("finish = %+v", terminal.Finish)
			}
			if test.code != "" && (terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != test.code) {
				t.Fatalf("failure = %+v", terminal.Finish.Failure)
			}
			if test.kind == FinishError && terminal.Message != nil {
				t.Fatal("失败响应不应产生终态 message")
			}
			if test.kind == FinishMaxTokens && terminal.Message.Content[0].Complete {
				t.Fatal("截断文本被标为完整")
			}
			assertOpenAIPrivate(t, mustOpenAIJSON(t, chunks))
		})
	}
}

func TestOpenAISSELineEndingsMultilineAndNetworkFragments(t *testing.T) {
	for _, ending := range []string{"\n", "\r\n", "\r"} {
		t.Run(fmt.Sprintf("%q", ending), func(t *testing.T) {
			body := "\xef\xbb\xbf: comment\n\nevent: message\nid: ignored\ndata: {\"choices\":[\ndata: {\"index\":0,\"delta\":{\"content\":\"中文\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
			body = strings.ReplaceAll(body, "\n", ending)
			provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				for i := range len(body) {
					_, _ = w.Write([]byte{body[i]})
					w.(http.Flusher).Flush()
				}
			}, 3*time.Second)
			terminal, _ := consumeOpenAITest(t, provider, openAITestRequest())
			if terminal.Finish.Kind != FinishStop || terminal.Message == nil || terminal.Message.Content[0].Text != "中文" {
				t.Fatalf("SSE 分片解析失败：%+v", terminal)
			}
		})
	}
}

func TestOpenAIFinishClosesNetworkBeforeLocalEOF(t *testing.T) {
	left := make(chan struct{})
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		defer close(left)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, openAITestEvent(`{"content":"完成"}`, "stop")+"data: [DONE]\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, 3*time.Second)
	stream, err := provider.Stream(context.Background(), openAITestRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for {
		chunk, err := stream.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Kind == StreamChunkFinish {
			break
		}
	}
	openAITestWait(t, left)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	chunk, err := stream.Next(ctx)
	if !errors.Is(err, io.EOF) || !isZeroStreamChunk(chunk) {
		t.Fatalf("不是本地 EOF：%+v, %v", chunk, err)
	}
}

func TestOpenAICloseAndCancellationUnblockInflightNext(t *testing.T) {
	for _, action := range []string{"close", "request-cancel", "next-cancel", "next-timeout", "client-timeout", "overlap"} {
		t.Run(action, func(t *testing.T) {
			left := make(chan struct{})
			timeout := 3 * time.Second
			if action == "client-timeout" {
				timeout = 100 * time.Millisecond
			}
			provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
				defer close(left)
				w.Header().Set("Content-Type", "text/event-stream")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}, timeout)
			requestCtx, cancelRequest := context.WithCancel(context.Background())
			defer cancelRequest()
			stream, err := provider.Stream(requestCtx, openAITestRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			nextCtx, cancelNext := context.WithCancel(context.Background())
			defer cancelNext()
			if action == "next-timeout" {
				var cancel context.CancelFunc
				nextCtx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
			}
			// 通过 body.Read 的信号确定已进入阻塞读取，不依赖 sleep 或调度猜测。
			adapter := stream.(*openAIStream)
			entered := make(chan struct{})
			adapter.body = &openAITestReadSignal{ReadCloser: adapter.body, entered: entered}
			adapter.decoder.scanner = newOpenAIStream(adapter.ctx, adapter.cancel, adapter.body).decoder.scanner
			result := make(chan streamResult, 1)
			go func() { chunk, err := stream.Next(nextCtx); result <- streamResult{chunk: chunk, err: err} }()
			openAITestWait(t, entered)
			switch action {
			case "close":
				closed := make(chan struct{})
				go func() { _ = stream.Close(); _ = stream.Close(); close(closed) }()
				openAITestWait(t, closed)
			case "request-cancel":
				cancelRequest()
			case "next-cancel":
				cancelNext()
			case "overlap":
				chunk, err := stream.Next(context.Background())
				assertFailureFinish(t, chunk, err, FinishError, FailureProtocol)
			}
			var got streamResult
			select {
			case got = <-result:
			case <-time.After(2 * time.Second):
				t.Fatal("inflight Next 未解除阻塞")
			}
			if action == "close" || action == "overlap" {
				if !errors.Is(got.err, io.EOF) || !isZeroStreamChunk(got.chunk) {
					t.Fatalf("关闭后泄露 inflight 结果：%+v", got)
				}
			} else if action == "next-timeout" || action == "client-timeout" {
				assertFailureFinish(t, got.chunk, got.err, FinishError, FailureTimeout)
			} else {
				assertFailureFinish(t, got.chunk, got.err, FinishAborted, FailureAborted)
			}
			openAITestWait(t, left)
			chunk, err := stream.Next(context.Background())
			if !errors.Is(err, io.EOF) || !isZeroStreamChunk(chunk) {
				t.Fatal("关闭或终止后不是本地 EOF")
			}
		})
	}
}

type openAITestReadSignal struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (reader *openAITestReadSignal) Read(p []byte) (int, error) {
	reader.once.Do(func() { close(reader.entered) })
	return reader.ReadCloser.Read(p)
}

func openAITestWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("等待取消/清理信号超时")
	}
}

func TestOpenAIHeaderTimeoutAndTransportPrivacy(t *testing.T) {
	left := make(chan struct{})
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		defer close(left)
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}, 50*time.Millisecond)
	terminal, _ := consumeOpenAITest(t, provider, openAITestRequest())
	if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureTimeout {
		t.Fatalf("响应头超时归一化失败：%+v", terminal)
	}
	openAITestWait(t, left)
	client := &http.Client{Transport: openAITestRoundTrip(func(r *http.Request) (*http.Response, error) {
		return nil, errors.New("test-secret-key upstream-private-body")
	})}
	private, err := NewOpenAIProvider(OpenAIOptions{BaseURL: "https://example.invalid", APIKey: "test-secret-key", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	terminal, chunks := consumeOpenAITest(t, private, openAITestRequest())
	if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureTransport {
		t.Fatalf("传输错误归一化失败：%+v", terminal)
	}
	assertOpenAIPrivate(t, mustOpenAIJSON(t, chunks))
}

type openAITestRoundTrip func(*http.Request) (*http.Response, error)

func (transport openAITestRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) {
	return transport(r)
}

func TestOpenAIInvalidRequestNeverReachesNetwork(t *testing.T) {
	var calls atomic.Int32
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }, time.Second)
	tests := []ModelRequest{
		{}, {Model: "m", Messages: []Message{{Role: "bad"}}},
		{Model: "m", Messages: []Message{{Role: RoleTool, Content: []ContentBlock{{Type: ContentBlockToolResult, ToolResult: json.RawMessage(`{}`)}}}}},
	}
	badSchema := openAITestRequest()
	badSchema.Tools = []ToolDefinition{{Name: "echo", InputSchema: json.RawMessage(`upstream-private-body`)}}
	tests = append(tests, badSchema)
	for _, request := range tests {
		terminal, chunks := consumeOpenAITest(t, provider, request)
		if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureInvalidRequest {
			t.Fatalf("无效请求分类错误：%+v", terminal)
		}
		assertOpenAIPrivate(t, mustOpenAIJSON(t, chunks))
	}
	if calls.Load() != 0 {
		t.Fatal("无效请求访问了网络")
	}
}

func TestOpenAIUsageSnapshotsAndTruncatedTool(t *testing.T) {
	body := "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n" +
		openAITestEvent(`{"content":"部分","tool_calls":[{"index":0,"id":"call_","function":{"name":"echo","arguments":"{\"text\":"}}]}`, "length") +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n"
	terminal, chunks := consumeOpenAITest(t, openAITestBody(t, body), openAITestRequest())
	if terminal.Finish.Kind != FinishMaxTokens || terminal.Usage == nil || terminal.Usage.InputTokens != 2 || len(terminal.Message.Content) != 1 || terminal.Message.Content[0].Complete || len(terminal.DroppedBlockIndexes) != 1 {
		t.Fatalf("截断语义错误：%+v", terminal)
	}
	usages := 0
	for _, chunk := range chunks {
		if chunk.Kind == StreamChunkUsage {
			usages++
		}
	}
	if usages != 1 {
		t.Fatal("usage 不是最终单快照")
	}
}

func TestOpenAIRetryAfterBounds(t *testing.T) {
	for _, value := range []string{"9223372036854775807", time.Now().Add(48 * time.Hour).UTC().Format(http.TimeFormat)} {
		failure := openAIHTTPFailure(429, value)
		if failure.ProviderRetryAfterMs == nil || *failure.ProviderRetryAfterMs != int64((24*time.Hour)/time.Millisecond) {
			t.Fatalf("Retry-After 未封顶：%+v", failure)
		}
	}
	for _, value := range []string{"-1", "test-secret-key", "99999999999999999999999999999999"} {
		failure := openAIHTTPFailure(429, value)
		if failure.ProviderRetryAfterMs != nil {
			t.Fatalf("无效 Retry-After 被接受：%+v", failure)
		}
		assertOpenAIPrivate(t, mustOpenAIJSON(t, failure))
	}
}

func TestOpenAIResourceLimitsAndDuplicateToolIDs(t *testing.T) {
	var tooMany strings.Builder
	for index := range openAIMaxTools + 1 {
		tooMany.WriteString(openAITestEvent(fmt.Sprintf(`{"tool_calls":[{"index":%d}]}`, index*1000), ""))
	}
	fragment := strings.Repeat("a", openAIMaxFrameBytes/2)
	argumentFrame := openAITestEvent(`{"tool_calls":[{"index":0,"function":{"arguments":"`+fragment+`"}}]}`, "")
	bodies := map[string]string{
		"too-many-sparse-tools":             tooMany.String(),
		"oversized-arguments-across-frames": strings.Repeat(argumentFrame, 3),
		"oversized-id":                      openAITestEvent(`{"tool_calls":[{"index":0,"id":"`+strings.Repeat("a", 4097)+`"}]}`, ""),
		"duplicate-ids":                     openAITestEvent(`{"tool_calls":[{"index":1,"id":"same","function":{"name":"echo","arguments":"{}"}},{"index":0,"id":"same","function":{"name":"echo","arguments":"{}"}}]}`, "tool_calls") + "data: [DONE]\n\n",
		"missing-tool-id":                   openAITestEvent(`{"tool_calls":[{"index":0,"function":{"name":"echo","arguments":"{}"}}]}`, "tool_calls") + "data: [DONE]\n\n",
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			terminal, _ := consumeOpenAITest(t, openAITestBody(t, body), openAITestRequest())
			if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureProtocol {
				t.Fatalf("资源/工具校验失败：%+v", terminal)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newOpenAIStream(ctx, cancel, io.NopCloser(strings.NewReader(": heartbeat\n\n")))
	stream.decoder.total = openAIMaxStreamBytes - 1
	chunk, err := stream.Next(ctx)
	assertFailureFinish(t, chunk, err, FinishError, FailureProtocol)
}

func TestOpenAIFinishOnCRPersistentConnection(t *testing.T) {
	for _, ending := range []string{"\r", "\r\n"} {
		t.Run(fmt.Sprintf("%q", ending), func(t *testing.T) {
			left := make(chan struct{})
			provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
				defer close(left)
				w.Header().Set("Content-Type", "text/event-stream")
				body := openAITestEvent(`{"content":"完成"}`, "stop") + "data: [DONE]\n\n"
				_, _ = io.WriteString(w, strings.ReplaceAll(body, "\n", ending))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}, time.Second)
			terminal, _ := consumeOpenAITest(t, provider, openAITestRequest())
			if terminal.Finish.Kind != FinishStop {
				t.Fatalf("CR 终态仍在等待网络：%+v", terminal)
			}
			openAITestWait(t, left)
		})
	}
}

func TestOpenAIOnlyConfiguredEndpointIsUsed(t *testing.T) {
	var untrustedRequests atomic.Int32
	untrusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { untrustedRequests.Add(1) }))
	defer untrusted.Close()
	var configuredRequests atomic.Int32
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		configuredRequests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, openAITestEvent(`{"content":"配置地址"}`, "stop")+"data: [DONE]\n\n")
	}, time.Second)
	request := openAITestRequest()
	request.Model = untrusted.URL
	encoded, _ := json.Marshal(untrusted.URL)
	request.Metadata = map[string]json.RawMessage{"base_url": encoded, "endpoint": encoded}
	request.Messages[0].Content[0].Text = untrusted.URL
	terminal, _ := consumeOpenAITest(t, provider, request)
	if terminal.Finish.Kind != FinishStop || configuredRequests.Load() != 1 || untrustedRequests.Load() != 0 {
		t.Fatal("模型、prompt 或 metadata 改写了 endpoint")
	}
}

func TestOpenAIWrongContentTypeAndUnreadErrorBody(t *testing.T) {
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"content":"test-secret-key upstream-private-body"}`)
	}, time.Second)
	terminal, chunks := consumeOpenAITest(t, provider, openAITestRequest())
	if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureProtocol {
		t.Fatal("接受了非 SSE 响应")
	}
	assertOpenAIPrivate(t, mustOpenAIJSON(t, chunks))
	left := make(chan struct{})
	provider = openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		defer close(left)
		w.WriteHeader(http.StatusUnauthorized)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, time.Second)
	terminal, _ = consumeOpenAITest(t, provider, openAITestRequest())
	if terminal.Finish.Failure == nil || terminal.Finish.Failure.Code != FailureInvalidCredential {
		t.Fatal("错误响应体阻塞了状态归一化")
	}
	openAITestWait(t, left)
}

func TestOpenAICancellationThroughRuntimeReleasesEveryRequest(t *testing.T) {
	var active atomic.Int32
	left := make(chan struct{}, 16)
	provider := openAITestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		active.Add(1)
		defer func() { active.Add(-1); left <- struct{}{} }()
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, time.Second)
	for i := range 16 {
		stream, err := NewRuntime(provider, nil).Stream(context.Background(), openAITestRequest())
		if err != nil {
			t.Fatal(err)
		}
		adapter := stream.(*runtimeStream).adapter.(*openAIStream)
		entered := make(chan struct{})
		adapter.body = &openAITestReadSignal{ReadCloser: adapter.body, entered: entered}
		adapter.decoder.scanner = newOpenAIStream(adapter.ctx, adapter.cancel, adapter.body).decoder.scanner
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan streamResult, 1)
		go func() { chunk, err := stream.Next(ctx); result <- streamResult{chunk: chunk, err: err} }()
		openAITestWait(t, entered)
		if i%2 == 0 {
			_ = stream.Close()
		} else {
			cancel()
		}
		select {
		case got := <-result:
			if i%2 == 0 {
				if !errors.Is(got.err, io.EOF) || !isZeroStreamChunk(got.chunk) {
					t.Fatal("Runtime Close 后发布了 inflight 结果")
				}
			} else {
				assertFailureFinish(t, got.chunk, got.err, FinishAborted, FailureAborted)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Runtime 读取协程未退出")
		}
		cancel()
		_ = stream.Close()
		openAITestWait(t, left)
	}
	if active.Load() != 0 {
		t.Fatal("有未退出的 HTTP 请求")
	}
}

func assertOpenAIPrivate(t *testing.T, text string) {
	t.Helper()
	for _, secret := range []string{"test-secret-key", "upstream-private-body"} {
		if strings.Contains(text, secret) {
			t.Fatal("密钥或原始上游错误被泄露")
		}
	}
}

func mustOpenAIJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestOpenAIProviderConcurrentRequests(t *testing.T) {
	provider := openAITestBody(t, openAITestEvent(`{"content":"隔离"}`, "stop")+"data: [DONE]\n\n")
	var group sync.WaitGroup
	for range 12 {
		group.Add(1)
		go func() {
			defer group.Done()
			stream, err := provider.Stream(context.Background(), openAITestRequest())
			if err != nil {
				t.Error(err)
				return
			}
			defer stream.Close()
			var kinds []StreamChunkKind
			for {
				chunk, err := stream.Next(context.Background())
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				kinds = append(kinds, chunk.Kind)
			}
			want := []StreamChunkKind{StreamChunkBlockStart, StreamChunkTextDelta, StreamChunkBlockEnd, StreamChunkFinish}
			if !reflect.DeepEqual(kinds, want) {
				t.Errorf("请求间状态污染：%v", kinds)
			}
		}()
	}
	group.Wait()
}
