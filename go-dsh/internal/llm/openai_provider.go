package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	openAIDefaultTimeout = 2 * time.Minute
	openAIMaxFrameBytes  = 1 << 20
	openAIMaxStreamBytes = 32 << 20
	openAIMaxTools       = 128
)

type OpenAIOptions struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

type OpenAIProvider struct {
	endpoint string
	apiKey   string
	client   http.Client
}

var _ Provider = (*OpenAIProvider)(nil)

// NewOpenAIProvider 只接受受信配置中的 endpoint；请求内容不能改变目标地址。
// HTTP 仅允许 localhost 或字面量回环地址，其他地址必须使用 HTTPS。
func NewOpenAIProvider(options OpenAIOptions) (*OpenAIProvider, error) {
	u, err := url.Parse(options.BaseURL)
	if err != nil || u == nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("OpenAI endpoint 配置无效")
	}
	ip := net.ParseIP(u.Hostname())
	loopback := strings.EqualFold(u.Hostname(), "localhost") || (ip != nil && ip.IsLoopback())
	if u.Scheme != "https" && (u.Scheme != "http" || !loopback) {
		return nil, errors.New("OpenAI endpoint 必须使用 HTTPS，回环测试地址除外")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, errors.New("OpenAI endpoint 端口无效")
		}
	}
	for _, c := range options.APIKey {
		if c <= ' ' || c >= 127 {
			return nil, errors.New("OpenAI APIKey 格式无效")
		}
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/chat/completions"
	u.RawPath = ""
	client := http.Client{}
	if options.HTTPClient != nil {
		client = *options.HTTPClient
	}
	if client.Timeout <= 0 {
		client.Timeout = openAIDefaultTimeout
	}
	// 无论重定向是否同源都拒绝；不修改调用方的 client，也不携带其 cookie jar。
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client.Jar = nil
	return &OpenAIProvider{endpoint: u.String(), apiKey: options.APIKey, client: client}, nil
}

func (provider *OpenAIProvider) Stream(ctx context.Context, request ModelRequest) (Stream, error) {
	if err := ctx.Err(); err != nil {
		return openAITerminal(openAIReadFailure(err)), nil
	}
	if provider == nil || provider.endpoint == "" {
		return openAITerminal(openAIFailure(FailureNoAdapter)), nil
	}
	payload, err := marshalOpenAIRequest(request)
	if err != nil {
		return openAITerminal(openAIFailure(FailureInvalidRequest)), nil
	}
	requestCtx, cancel := context.WithTimeout(ctx, provider.client.Timeout)
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, provider.endpoint, bytes.NewReader(payload))
	if err != nil {
		cancel()
		return openAITerminal(openAIFailure(FailureInvalidRequest)), nil
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "text/event-stream")
	if provider.apiKey != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+provider.apiKey)
	}
	response, err := provider.client.Do(httpRequest)
	if err != nil {
		failure := openAIReadFailure(err)
		if requestCtx.Err() != nil {
			failure = openAIReadFailure(requestCtx.Err())
		}
		cancel()
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return openAITerminal(failure), nil
	}
	if response.StatusCode != http.StatusOK {
		failure := openAIHTTPFailure(response.StatusCode, response.Header.Get("Retry-After"))
		cancel()
		_ = response.Body.Close()
		return openAITerminal(failure), nil
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || mediaType != "text/event-stream" {
		cancel()
		_ = response.Body.Close()
		return openAITerminal(openAIFailure(FailureProtocol)), nil
	}
	return newOpenAIStream(requestCtx, cancel, response.Body), nil
}

// 直接发布结构化终态，避免默认 normalizer 将适配器错误降为 UNKNOWN。
// 不读取错误响应体，不保留原始错误、URL、请求 ID 或任意上游错误字符串。
func openAITerminal(failure LlmFailure) Stream {
	return &frozenTerminalAdapter{finish: openAIFailureChunk(failure)}
}

func openAIFailureChunk(failure LlmFailure) StreamChunk {
	if failure.Code == FailureAborted {
		return abortedFinish(failure)
	}
	return errorFinish(failure)
}

func openAIFailure(code FailureCode) LlmFailure {
	messages := map[FailureCode]string{
		FailureNoAdapter:         "OpenAI provider 未配置",
		FailureInvalidRequest:    "模型请求参数无效",
		FailureInvalidCredential: "模型服务拒绝凭证或访问权限",
		FailureModelNotFound:     "模型或接口不存在",
		FailureRateLimit:         "模型服务请求限流",
		FailureTransport:         "模型服务传输失败",
		FailureTimeout:           "模型请求超时",
		FailureAborted:           "模型请求已取消",
		FailureProtocol:          "模型服务流协议无效",
		FailureStreamClosed:      "模型流在终止标记之前关闭",
		FailureEmptyResponse:     "模型返回空内容",
	}
	return LlmFailure{Code: code, Message: messages[code]}
}

func openAIReadFailure(err error) LlmFailure {
	var timeout net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return openAIFailure(FailureTimeout)
	case errors.Is(err, context.Canceled):
		return openAIFailure(FailureAborted)
	case errors.As(err, &timeout) && timeout.Timeout():
		return openAIFailure(FailureTimeout)
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return openAIFailure(FailureStreamClosed)
	default:
		return openAIFailure(FailureTransport)
	}
}

func openAIHTTPFailure(status int, retryAfter string) LlmFailure {
	code, retryable := FailureInvalidRequest, false
	switch {
	case status >= 300 && status < 400:
		code = FailureProtocol
	case status == 401 || status == 403:
		code = FailureInvalidCredential
	case status == 404:
		code = FailureModelNotFound
	case status == 429:
		code, retryable = FailureRateLimit, true
	case status == 408 || status == 504:
		code, retryable = FailureTimeout, true
	case status >= 500:
		code, retryable = FailureTransport, true
	}
	failure := openAIFailure(code)
	failure.Status, failure.RetryableHint = &status, &retryable
	if retryable {
		var delay time.Duration
		if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && seconds >= 0 {
			if seconds > 86400 {
				seconds = 86400
			}
			delay = time.Duration(seconds) * time.Second
		} else if date, err := http.ParseTime(retryAfter); err == nil {
			delay = time.Until(date)
		}
		if delay > 24*time.Hour {
			delay = 24 * time.Hour
		}
		if delay > 0 {
			ms := delay.Milliseconds()
			failure.ProviderRetryAfterMs = &ms
		}
	}
	return failure
}

type openAIMessage struct {
	Role             Role             `json:"role"`
	Content          string           `json:"content"`
	Name             string           `json:"name,omitempty"`
	ReasoningContent string           `json:"reasoning_content,omitempty"`
	ToolCalls        []openAIWireTool `json:"tool_calls,omitempty"`
	ToolCallID       string           `json:"tool_call_id,omitempty"`
}

type openAIWireTool struct {
	ID       string             `json:"id,omitempty"`
	Type     string             `json:"type"`
	Function openAIWireFunction `json:"function"`
}

type openAIWireFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Arguments   *string         `json:"arguments,omitempty"`
}

func marshalOpenAIRequest(request ModelRequest) ([]byte, error) {
	invalid := errors.New("模型请求参数无效")
	if strings.TrimSpace(request.Model) == "" || len(request.Messages) == 0 || len(request.Tools) > openAIMaxTools {
		return nil, invalid
	}
	payload := struct {
		Model         string           `json:"model"`
		Messages      []openAIMessage  `json:"messages"`
		Tools         []openAIWireTool `json:"tools,omitempty"`
		Stream        bool             `json:"stream"`
		StreamOptions struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
		Temperature     *float64 `json:"temperature,omitempty"`
		MaxTokens       *int     `json:"max_tokens,omitempty"`
		ReasoningEffort string   `json:"reasoning_effort,omitempty"`
	}{Model: request.Model, Stream: true, Temperature: request.Temperature, MaxTokens: request.MaxTokens}
	payload.StreamOptions.IncludeUsage = true
	if request.MaxTokens != nil && *request.MaxTokens <= 0 {
		return nil, invalid
	}
	if request.Reasoning != nil && request.Reasoning.Enabled {
		if request.Reasoning.MaxTokens != nil || request.Reasoning.Preset != "" {
			return nil, invalid
		}
		payload.ReasoningEffort = request.Reasoning.Effort
	}
	for _, message := range request.Messages {
		if message.Role == RoleTool {
			if len(message.Content) == 0 {
				return nil, invalid
			}
			for _, block := range message.Content {
				if block.Type != ContentBlockToolResult || block.ToolCallID == "" || !json.Valid(block.ToolResult) {
					return nil, invalid
				}
				payload.Messages = append(payload.Messages, openAIMessage{Role: RoleTool, ToolCallID: block.ToolCallID, Content: string(block.ToolResult)})
			}
			continue
		}
		if message.Role != RoleSystem && message.Role != RoleUser && message.Role != RoleAssistant {
			return nil, invalid
		}
		wire := openAIMessage{Role: message.Role, Name: message.Name}
		for _, block := range message.Content {
			switch block.Type {
			case ContentBlockText:
				wire.Content += block.Text
			case ContentBlockReasoning:
				if message.Role != RoleAssistant {
					return nil, invalid
				}
				wire.ReasoningContent += block.Text
			case ContentBlockToolCall:
				if message.Role != RoleAssistant || block.ToolCallID == "" || block.ToolName == "" || validateToolArguments(block.ArgumentsJSONRaw) != nil {
					return nil, invalid
				}
				args := block.ArgumentsJSONRaw
				wire.ToolCalls = append(wire.ToolCalls, openAIWireTool{ID: block.ToolCallID, Type: "function", Function: openAIWireFunction{Name: block.ToolName, Arguments: &args}})
			default:
				return nil, invalid
			}
		}
		payload.Messages = append(payload.Messages, wire)
	}
	for _, tool := range request.Tools {
		if tool.Name == "" || validateToolArguments(string(tool.InputSchema)) != nil {
			return nil, invalid
		}
		payload.Tools = append(payload.Tools, openAIWireTool{Type: "function", Function: openAIWireFunction{Name: tool.Name, Description: tool.Description, Parameters: tool.InputSchema}})
	}
	data, err := json.Marshal(payload)
	if err != nil || len(data) > openAIMaxStreamBytes {
		return nil, invalid
	}
	return data, nil
}

type openAIStream struct {
	ctx         context.Context
	cancel      context.CancelFunc
	body        io.ReadCloser
	releaseOnce sync.Once
	nextActive  atomic.Bool
	stateMu     sync.Mutex
	closed      bool
	done        bool

	// 以下字段仅由持有 nextActive 的调用方访问，Close 不触碰解析状态。
	decoder                   *openAISSEDecoder
	queue                     []StreamChunk
	final                     bool
	finish                    *FinishReason
	usage                     *TokenUsage
	blocks                    []openAIBlock
	tools                     map[int]int
	textIndex, reasoningIndex int
}

func (stream *openAIStream) Close() error {
	stream.stateMu.Lock()
	stream.closed = true
	stream.stateMu.Unlock()
	stream.release()
	return nil
}

func (stream *openAIStream) release() {
	stream.releaseOnce.Do(func() {
		stream.cancel()
		_ = stream.body.Close()
	})
}

func (stream *openAIStream) stopped() bool {
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	return stream.closed || stream.done
}

func (stream *openAIStream) publish(chunk StreamChunk) (StreamChunk, error) {
	stream.stateMu.Lock()
	defer stream.stateMu.Unlock()
	if stream.closed || stream.done {
		return StreamChunk{}, io.EOF
	}
	if chunk.Kind == StreamChunkFinish {
		stream.done = true
	}
	return chunk, nil
}

func (stream *openAIStream) fail(failure LlmFailure) {
	stream.queue = []StreamChunk{openAIFailureChunk(failure)}
	stream.final = true
	stream.release()
}

func (stream *openAIStream) Next(ctx context.Context) (StreamChunk, error) {
	if !stream.nextActive.CompareAndSwap(false, true) {
		chunk, err := stream.publish(openAIFailureChunk(openAIFailure(FailureProtocol)))
		stream.release()
		return chunk, err
	}
	defer stream.nextActive.Store(false)
	if stream.stopped() {
		return StreamChunk{}, io.EOF
	}
	// 使用请求级取消中断标准库读操作，不创建常驻 reader goroutine。
	stop := context.AfterFunc(ctx, stream.cancel)
	defer stop()
	if ctx.Err() != nil {
		stream.fail(openAIReadFailure(ctx.Err()))
	} else if !stream.final && stream.ctx.Err() != nil {
		stream.fail(openAIReadFailure(stream.ctx.Err()))
	}
	for len(stream.queue) == 0 {
		data, err := stream.decoder.next()
		if ctx.Err() != nil {
			stream.fail(openAIReadFailure(ctx.Err()))
		} else if stream.ctx.Err() != nil {
			stream.fail(openAIReadFailure(stream.ctx.Err()))
		} else if errors.Is(err, io.EOF) && stream.finish != nil {
			stream.complete()
		} else if errors.Is(err, errOpenAISSE) {
			stream.fail(openAIFailure(FailureProtocol))
		} else if err != nil {
			stream.fail(openAIReadFailure(err))
		} else if err := stream.accept(data); err != nil {
			stream.fail(openAIFailure(FailureProtocol))
		}
	}
	chunk := stream.queue[0]
	stream.queue[0] = StreamChunk{}
	stream.queue = stream.queue[1:]
	return stream.publish(chunk)
}
