package llm

import (
	"context"
	"encoding/json"
)

type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type MessageSource string

const (
	MessageSourceSurface MessageSource = "surface"
	MessageSourcePrompt  MessageSource = "prompt"
	MessageSourceStep    MessageSource = "step"
)

type ContentBlockType string

const (
	ContentBlockText       ContentBlockType = "text"
	ContentBlockReasoning  ContentBlockType = "reasoning"
	ContentBlockToolCall   ContentBlockType = "tool-call"
	ContentBlockToolResult ContentBlockType = "tool-result"
)

type ContentBlock struct {
	Type             ContentBlockType `json:"type"`
	Text             string           `json:"text,omitempty"`
	Index            uint32           `json:"index"`
	ToolCallID       string           `json:"tool_call_id,omitempty"`
	ToolName         string           `json:"tool_name,omitempty"`
	ArgumentsJSONRaw string           `json:"arguments_json_raw,omitempty"`
	ToolResult       json.RawMessage  `json:"tool_result,omitempty"`
	Complete         bool             `json:"complete"`
}

type Message struct {
	Role    Role           `json:"role"`
	Name    string         `json:"name,omitempty"`
	Source  MessageSource  `json:"source,omitempty"`
	Content []ContentBlock `json:"content"`
}

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type ReasoningOptions struct {
	Enabled   bool   `json:"enabled"`
	Effort    string `json:"effort,omitempty"`
	MaxTokens *int   `json:"max_tokens,omitempty"`
	Preset    string `json:"preset,omitempty"`
}

type ModelRequest struct {
	RequestID   string                     `json:"request_id"`
	SessionID   string                     `json:"session_id"`
	TurnID      string                     `json:"turn_id"`
	StepID      string                     `json:"step_id"`
	Attempt     uint32                     `json:"attempt"`
	Model       string                     `json:"model"`
	Messages    []Message                  `json:"messages"`
	Tools       []ToolDefinition           `json:"tools"`
	Temperature *float64                   `json:"temperature,omitempty"`
	MaxTokens   *int                       `json:"max_tokens,omitempty"`
	Reasoning   *ReasoningOptions          `json:"reasoning,omitempty"`
	Metadata    map[string]json.RawMessage `json:"metadata,omitempty"`
}

type StreamChunkKind string

const (
	StreamChunkBlockStart     StreamChunkKind = "block-start"
	StreamChunkTextDelta      StreamChunkKind = "text-delta"
	StreamChunkReasoningDelta StreamChunkKind = "reasoning-delta"
	StreamChunkToolCallDelta  StreamChunkKind = "tool-call-delta"
	StreamChunkBlockEnd       StreamChunkKind = "block-end"
	StreamChunkUsage          StreamChunkKind = "usage"
	StreamChunkFinish         StreamChunkKind = "finish"
)

type FinishKind string

const (
	FinishStop      FinishKind = "stop"
	FinishToolCalls FinishKind = "tool-calls"
	FinishMaxTokens FinishKind = "max-tokens"
	FinishAborted   FinishKind = "aborted"
	FinishError     FinishKind = "error"
)

type FailureCode string

const (
	FailureContextWindowExceeded    FailureCode = "CONTEXT_WINDOW_EXCEEDED"
	FailureQuota                    FailureCode = "QUOTA"
	FailureRateLimit                FailureCode = "RATE_LIMIT"
	FailureEmptyResponse            FailureCode = "EMPTY_RESPONSE"
	FailureInvalidCredential        FailureCode = "INVALID_CREDENTIAL"
	FailureInvalidRequest           FailureCode = "INVALID_REQUEST"
	FailureModelNotFound            FailureCode = "MODEL_NOT_FOUND"
	FailureToolExecutionUnavailable FailureCode = "TOOL_EXECUTION_UNAVAILABLE"
	FailureTransport                FailureCode = "TRANSPORT"
	FailureTimeout                  FailureCode = "TIMEOUT"
	FailureStreamClosed             FailureCode = "STREAM_CLOSED"
	FailureProtocol                 FailureCode = "PROTOCOL"
	FailureNoAdapter                FailureCode = "NO_ADAPTER"
	FailureAborted                  FailureCode = "ABORTED"
	FailureUnknown                  FailureCode = "UNKNOWN"
)

type FailureCause struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type LlmFailure struct {
	Message              string         `json:"message"`
	Code                 FailureCode    `json:"code"`
	Status               *int           `json:"status,omitempty"`
	ProviderRetryAfterMs *int64         `json:"provider_retry_after_ms,omitempty"`
	ProviderRequestID    string         `json:"provider_request_id,omitempty"`
	RetryableHint        *bool          `json:"retryable_hint,omitempty"`
	CauseChain           []FailureCause `json:"cause_chain,omitempty"`
}

type FinishReason struct {
	Kind    FinishKind  `json:"kind"`
	Failure *LlmFailure `json:"failure,omitempty"`
}

type TokenUsage struct {
	InputTokens      int64  `json:"input_tokens"`
	OutputTokens     int64  `json:"output_tokens"`
	CacheReadTokens  *int64 `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens *int64 `json:"cache_write_tokens,omitempty"`
	ReasoningTokens  *int64 `json:"reasoning_tokens,omitempty"`
}

type StreamChunk struct {
	Kind           StreamChunkKind  `json:"kind"`
	Index          uint32           `json:"index,omitempty"`
	BlockType      ContentBlockType `json:"block_type,omitempty"`
	Text           string           `json:"text,omitempty"`
	ToolCallID     string           `json:"tool_call_id,omitempty"`
	ToolName       string           `json:"tool_name,omitempty"`
	ArgumentsDelta string           `json:"arguments_delta,omitempty"`
	Block          *ContentBlock    `json:"block,omitempty"`
	Usage          *TokenUsage      `json:"usage,omitempty"`
	Finish         *FinishReason    `json:"finish,omitempty"`
	ProviderRaw    json.RawMessage  `json:"-"`
}

// Stream is pull-based. Implementations returned by a Provider are adapter
// streams and must obey all of these lifecycle requirements:
//   - after returning a finish chunk, the next Next call returns a zero
//     StreamChunk and io.EOF immediately without network I/O;
//   - Next calls for the same stream must not overlap; Runtime treats overlap
//     as one FinishError/PROTOCOL terminal, suppresses the in-flight result,
//     and returns io.EOF thereafter;
//   - Close is idempotent, bounded, safe to call concurrently with Next, and
//     unblocks an in-flight Next;
//   - after Close returns, later Next calls return io.EOF immediately.
//
// Runtime relies on these adapter guarantees to validate a provider terminal
// and to expose the same guarantees on its canonical stream without spawning
// an unbounded cleanup goroutine.
type Stream interface {
	Next(context.Context) (StreamChunk, error)
	Close() error
}

type Provider interface {
	Stream(context.Context, ModelRequest) (Stream, error)
}
