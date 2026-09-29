package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	protocolv1 "github.com/yy59750901/go-dsh/gen/go/dsh/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	MaxGRPCRemoteTools   = 128
	MaxGRPCRemoteEvents  = 256
	maxGRPCRemoteMessage = 2 << 20
	maxGRPCLocalIDBytes  = 4 << 10
)

var (
	ErrGRPCRemoteClosed   = errors.New("remote tool client closed")
	ErrGRPCRemoteProtocol = errors.New("invalid remote tool response")
	ErrGRPCRemoteLimit    = errors.New("remote tool size or event limit exceeded")
	ErrGRPCRemoteFailure  = errors.New("remote tool reported failure")
	ErrGRPCRemoteDenied   = errors.New("remote tool denied")
)

// GRPCRemoteConfig 仅由可信操作员提供；连接及 TLS/认证策略由调用方固定配置。
// 此客户端不提供执行服务器，RequiresApproval 不能代替 Harness 的审批判定。
type GRPCRemoteConfig struct {
	SessionID string
	Timeout   time.Duration
}
type GRPCRemoteClient struct {
	rpc     protocolv1.ToolServiceClient
	session string
	timeout time.Duration
	ctx     context.Context
	cancel  context.CancelFunc
	once    sync.Once
}

// NewGRPCRemoteClient 借用连接；Close 只取消本客户端请求，不关闭共享连接。
func NewGRPCRemoteClient(conn grpc.ClientConnInterface, config GRPCRemoteConfig) (*GRPCRemoteClient, error) {
	if conn == nil || !remoteID(config.SessionID, true, 256) || config.Timeout < 0 || config.Timeout > MaxTimeout {
		return nil, errors.New("invalid remote tool configuration")
	}
	if config.Timeout == 0 {
		config.Timeout = DefaultTimeout
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &GRPCRemoteClient{rpc: protocolv1.NewToolServiceClient(conn), session: config.SessionID, timeout: config.Timeout, ctx: ctx, cancel: cancel}, nil
}
func (c *GRPCRemoteClient) Close() error { c.once.Do(c.cancel); return nil }

func remoteID(id string, optional bool, limit int) bool {
	if id == "" {
		return optional
	}
	if len(id) > limit || !utf8.ValidString(id) {
		return false
	}
	for _, r := range id {
		if r <= 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// 本地 namespace 可能超过协议端的 ID 限额；对完整值做 SHA-256，不截断，
// 不依赖重试次数、连接或时间。空幂等键仍为空，避免改变未指定幂等键的语义。
func grpcRemoteWireID(local string) string {
	if local == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(local))
	return hex.EncodeToString(digest[:])
}

func (c *GRPCRemoteClient) requestContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("nil context")
	}
	if c.ctx.Err() != nil {
		return nil, nil, ErrGRPCRemoteClosed
	}
	if timeout <= 0 || timeout > c.timeout {
		timeout = c.timeout
	}
	child, cancel := context.WithTimeout(ctx, timeout)
	stop := context.AfterFunc(c.ctx, cancel)
	return child, func() { stop(); cancel() }, nil
}
func (c *GRPCRemoteClient) safeError(ctx context.Context, err error) error {
	if c.ctx.Err() != nil {
		return ErrGRPCRemoteClosed
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	switch status.Code(err) {
	case codes.Canceled:
		return context.Canceled
	case codes.DeadlineExceeded:
		return context.DeadlineExceeded
	case codes.ResourceExhausted:
		return ErrGRPCRemoteLimit
	default:
		return status.Error(status.Code(err), "remote tool RPC failed")
	}
}

// Tools 冻结服务器定义，返回通过 Registry 同一 schema 子集保护的 handle。
// 注册表 generation 是本地值，发往服务器时使用固定的远程 generation。
func (c *GRPCRemoteClient) Tools(ctx context.Context) ([]Tool, error) {
	ctx, cancel, err := c.requestContext(ctx, 0)
	if err != nil {
		return nil, err
	}
	defer cancel()
	response, err := c.rpc.ListTools(ctx, &protocolv1.ListToolsRequest{SessionId: c.session}, grpc.MaxCallRecvMsgSize(maxGRPCRemoteMessage))
	if err != nil {
		return nil, c.safeError(ctx, err)
	}
	if response == nil || len(response.Tools) > MaxGRPCRemoteTools || proto.Size(response) > maxGRPCRemoteMessage {
		return nil, ErrGRPCRemoteLimit
	}
	registry := NewRegistry()
	seen := make(map[string]bool)
	out := make([]Tool, 0, len(response.Tools))
	for _, wire := range response.Tools {
		if wire == nil || seen[wire.Name] || !namePattern.MatchString(wire.Name) || !versionPattern.MatchString(wire.Version) || wire.Generation == 0 || len(wire.Description) > 16<<10 || len(wire.RequiredCapabilities) > 128 {
			return nil, ErrGRPCRemoteProtocol
		}
		seen[wire.Name] = true
		for _, capability := range wire.RequiredCapabilities {
			if !namePattern.MatchString(capability) {
				return nil, ErrGRPCRemoteProtocol
			}
		}
		if wire.InputSchema == nil {
			return nil, ErrGRPCRemoteProtocol
		}
		input, err := protojson.Marshal(wire.InputSchema)
		if err != nil {
			return nil, ErrGRPCRemoteProtocol
		}
		var output []byte
		if wire.OutputSchema != nil {
			output, err = protojson.Marshal(wire.OutputSchema)
			if err != nil {
				return nil, ErrGRPCRemoteProtocol
			}
		}
		d := Definition{Name: wire.Name, Description: wire.Description, InputSchema: input, OutputSchema: output, Version: wire.Version, Generation: wire.Generation, RequiredCapabilities: append([]string(nil), wire.RequiredCapabilities...), RequiresApproval: true}
		remote := &grpcRemoteTool{client: c, definition: d}
		if err := registry.Register(remote); err != nil {
			return nil, ErrGRPCRemoteProtocol
		}
		handle, err := registry.Resolve(d.Name)
		if err != nil {
			return nil, ErrGRPCRemoteProtocol
		}
		out = append(out, handle)
	}
	return out, nil
}

type grpcRemoteTool struct {
	client     *GRPCRemoteClient
	definition Definition
}

func (t *grpcRemoteTool) Definition() Definition { return cloneDefinition(t.definition) }
func (t *grpcRemoteTool) Execute(ctx context.Context, call Call) (result Result, err error) {
	start := time.Now()
	defer func() { result.CallID = call.ID; result.Err = err; result.Duration = time.Since(start) }()
	if !remoteID(call.ID, false, maxGRPCLocalIDBytes) || !remoteID(call.SessionID, true, 256) || !remoteID(call.TurnID, true, 256) || !remoteID(call.StepID, true, 256) || !remoteID(call.IdempotencyKey, true, maxGRPCLocalIDBytes) || call.Timeout < 0 {
		return Result{}, ErrInvalidArguments
	}
	if call.Name != "" && call.Name != t.definition.Name || call.DefinitionVersion != "" && call.DefinitionVersion != t.definition.Version {
		return Result{}, ErrStaleDefinition
	}
	session := t.client.session
	if session != "" && call.SessionID != "" && call.SessionID != session {
		return Result{}, ErrInvalidArguments
	}
	if session == "" {
		session = call.SessionID
	}
	if len(call.Arguments) > MaxArgumentBytes {
		return Result{}, ErrGRPCRemoteLimit
	}
	original, e := decodeJSON(call.Arguments)
	if e != nil {
		return Result{}, ErrInvalidArguments
	}
	if _, ok := original.(map[string]any); !ok {
		return Result{}, ErrInvalidArguments
	}
	args := &structpb.Struct{}
	if protojson.Unmarshal(call.Arguments, args) != nil {
		return Result{}, ErrInvalidArguments
	}
	normalized, e := protojson.Marshal(args)
	if e != nil {
		return Result{}, ErrInvalidArguments
	}
	roundtrip, e := decodeJSON(normalized)
	// protobuf Struct 使用 double，拒绝会改变实参数值的有损转换。
	if e != nil || !jsonEqual(original, roundtrip) {
		return Result{}, ErrInvalidArguments
	}
	ctx, cancel, e := t.client.requestContext(ctx, call.Timeout)
	if e != nil {
		return Result{}, e
	}
	defer cancel()
	deadline, _ := ctx.Deadline()
	wireID := grpcRemoteWireID(call.ID)
	request := &protocolv1.ExecuteToolRequest{Call: &protocolv1.ToolCall{CallId: wireID, SessionId: session, TurnId: call.TurnID, StepId: call.StepID, ToolName: t.definition.Name, Arguments: args, DefinitionVersion: t.definition.Version, Generation: t.definition.Generation, IdempotencyKey: grpcRemoteWireID(call.IdempotencyKey), Timeout: durationpb.New(max(time.Until(deadline), 0))}}
	if proto.Size(request) > maxGRPCRemoteMessage {
		return Result{}, ErrGRPCRemoteLimit
	}
	stream, e := t.client.rpc.Execute(ctx, request, grpc.MaxCallRecvMsgSize(maxGRPCRemoteMessage), grpc.MaxCallSendMsgSize(maxGRPCRemoteMessage))
	if e != nil {
		return Result{}, t.client.safeError(ctx, e)
	}
	var final *protocolv1.ToolResult
	total := 0
	for count := 0; ; count++ {
		event, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			return Result{}, t.client.safeError(ctx, e)
		}
		if count >= MaxGRPCRemoteEvents || event == nil {
			return Result{}, ErrGRPCRemoteLimit
		}
		total += proto.Size(event)
		if total > MaxOutputBytes {
			return Result{}, ErrGRPCRemoteLimit
		}
		if final != nil || event.CallId != wireID || len(event.Phase) > 64 {
			return Result{}, ErrGRPCRemoteProtocol
		}
		if event.FinalResult != nil {
			if event.FinalResult.CallId != wireID {
				return Result{}, ErrGRPCRemoteProtocol
			}
			final = event.FinalResult
		}
	}
	if final == nil {
		return Result{}, ErrGRPCRemoteProtocol
	}
	if final.Duration != nil && (final.Duration.CheckValid() != nil || final.Duration.AsDuration() < 0) {
		return Result{}, ErrGRPCRemoteProtocol
	}
	switch final.Status {
	case protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_SUCCEEDED:
		if final.Error != nil {
			return Result{}, ErrGRPCRemoteProtocol
		}
	case protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_FAILED:
		return Result{}, ErrGRPCRemoteFailure
	case protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_CANCELLED:
		return Result{}, context.Canceled
	case protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_DENIED:
		return Result{}, ErrGRPCRemoteDenied
	default:
		return Result{}, ErrGRPCRemoteProtocol
	}
	if len(final.ArtifactRefs) != 0 {
		return Result{}, ErrGRPCRemoteProtocol
	}
	var texts []string
	for _, part := range final.Content {
		if part == nil || part.Type != "text" || part.Data != nil {
			return Result{}, ErrGRPCRemoteProtocol
		}
		texts = append(texts, part.Text)
	}
	result = Result{Content: strings.Join(texts, "\n"), Truncated: final.Truncated, Redacted: final.Redacted}
	if final.StructuredResult != nil {
		result.Structured, e = protojson.Marshal(final.StructuredResult)
		if e != nil {
			return Result{}, ErrGRPCRemoteProtocol
		}
	}
	if len(result.Content)+len(result.Structured) > MaxOutputBytes {
		return Result{}, ErrGRPCRemoteLimit
	}
	return result, nil
}

var _ Tool = (*grpcRemoteTool)(nil)
