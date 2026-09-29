package tool

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	protocolv1 "github.com/yy59750901/go-dsh/gen/go/dsh/protocol/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

type grpcRemoteFake struct {
	protocolv1.UnimplementedToolServiceServer
	list  func(context.Context, *protocolv1.ListToolsRequest) (*protocolv1.ListToolsResponse, error)
	run   func(*protocolv1.ExecuteToolRequest, grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error
	calls atomic.Int32
}

func remoteSchema(raw string) *structpb.Struct {
	s := &structpb.Struct{}
	_ = s.UnmarshalJSON([]byte(raw))
	return s
}
func remoteDefinition() *protocolv1.ToolDefinition {
	return &protocolv1.ToolDefinition{Name: "remote", Version: "v2", Generation: 42, InputSchema: remoteSchema(`{"type":"object"}`), OutputSchema: remoteSchema(`{"type":"object"}`)}
}
func (f *grpcRemoteFake) ListTools(ctx context.Context, r *protocolv1.ListToolsRequest) (*protocolv1.ListToolsResponse, error) {
	if f.list != nil {
		return f.list(ctx, r)
	}
	return &protocolv1.ListToolsResponse{Tools: []*protocolv1.ToolDefinition{remoteDefinition()}}, nil
}
func remoteFinal(id string) *protocolv1.ToolExecutionEvent {
	return &protocolv1.ToolExecutionEvent{CallId: id, FinalResult: &protocolv1.ToolResult{CallId: id, Status: protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_SUCCEEDED, Content: []*protocolv1.ContentPart{{Type: "text", Text: "ok"}}, StructuredResult: remoteSchema(`{"ok":true}`)}}
}
func (f *grpcRemoteFake) Execute(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
	f.calls.Add(1)
	if f.run != nil {
		return f.run(r, s)
	}
	return s.Send(remoteFinal(r.Call.CallId))
}
func newGRPCFakeClient(t *testing.T, f *grpcRemoteFake) *GRPCRemoteClient {
	t.Helper()
	listener := bufconn.Listen(2 << 20)
	server := grpc.NewServer()
	protocolv1.RegisterToolServiceServer(server, f)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewGRPCRemoteClient(conn, GRPCRemoteConfig{SessionID: "session", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return c
}
func remoteHandle(t *testing.T, c *GRPCRemoteClient) Tool {
	t.Helper()
	tools, err := c.Tools(context.Background())
	if err != nil || len(tools) != 1 {
		t.Fatal(tools, err)
	}
	return tools[0]
}

func TestGRPCRemoteBindingApprovalAndReRegistration(t *testing.T) {
	fake := &grpcRemoteFake{list: func(ctx context.Context, r *protocolv1.ListToolsRequest) (*protocolv1.ListToolsResponse, error) {
		if r.SessionId != "session" {
			t.Error("未绑定列举会话")
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("没有截止时间")
		}
		return &protocolv1.ListToolsResponse{Tools: []*protocolv1.ToolDefinition{remoteDefinition()}}, nil
	}, run: func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
		c := r.Call
		if c.ToolName != "remote" || c.DefinitionVersion != "v2" || c.Generation != 42 || c.SessionId != "session" || c.TurnId != "turn" || c.StepId != "step" || c.IdempotencyKey != fmt.Sprintf("%x", sha256.Sum256([]byte("idem"))) || c.Arguments.Fields["n"].GetNumberValue() != 1 || c.Timeout.AsDuration() > time.Second {
			t.Error("请求绑定错误", c)
		}
		if err := s.Send(&protocolv1.ToolExecutionEvent{CallId: c.CallId, Phase: "running"}); err != nil {
			return err
		}
		return s.Send(remoteFinal(c.CallId))
	}}
	client := newGRPCFakeClient(t, fake)
	handle := remoteHandle(t, client)
	d := handle.Definition()
	if !d.RequiresApproval || d.Generation == 42 {
		t.Fatal("审批或 generation 边界错误", d)
	}
	d.InputSchema[0] = 'x'
	registry := NewRegistry()
	if err := registry.Register(handle); err != nil {
		t.Fatal(err)
	}
	result, err := registry.Execute(context.Background(), Call{ID: "id", Name: "remote", Arguments: json.RawMessage(`{"n":1}`), TurnID: "turn", StepID: "step", IdempotencyKey: "idem"})
	if err != nil || result.CallID != "id" || result.Content != "ok" || !json.Valid(result.Structured) {
		t.Fatal(result, err)
	}
}

func TestGRPCRemoteLongNamespaceWireIdentity(t *testing.T) {
	turn, step := strings.Repeat("t", 37), strings.Repeat("s", 37)
	namespace, err := json.Marshal([]string{turn, step, strings.Repeat("p", 128)})
	if err != nil {
		t.Fatal(err)
	}
	localID := "call-" + base64.RawURLEncoding.EncodeToString(namespace)
	if len(localID) <= 256 {
		t.Fatal("未覆盖 driver 长 namespace")
	}
	seen := make(chan *protocolv1.ToolCall, 8)
	client := newGRPCFakeClient(t, &grpcRemoteFake{run: func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
		seen <- r.Call
		return s.Send(remoteFinal(r.Call.CallId))
	}})
	// 直接检查客户端结果，避免 Registry 再次设置 CallID 掩盖身份漂移。
	remote := &grpcRemoteTool{client: client, definition: Definition{Name: "remote", Version: "v2", Generation: 42}}
	localKey := "idem-" + localID
	calls := []Call{
		{ID: localID, IdempotencyKey: localKey},
		{ID: localID, IdempotencyKey: localKey},
		{ID: localID + "a", IdempotencyKey: localKey},
		{ID: localID + "b", IdempotencyKey: localKey + "b"},
		{ID: strings.Repeat("x", maxGRPCLocalIDBytes), IdempotencyKey: strings.Repeat("y", maxGRPCLocalIDBytes)},
		{ID: "short"},
	}
	var previous *protocolv1.ToolCall
	for i, call := range calls {
		call.TurnID, call.StepID, call.Arguments = turn, step, json.RawMessage(`{}`)
		result, err := remote.Execute(context.Background(), call)
		if err != nil || result.CallID != call.ID || result.Content != "ok" {
			t.Fatal("本地结果身份漂移", result, err)
		}
		wire := <-seen
		wantID := fmt.Sprintf("%x", sha256.Sum256([]byte(call.ID)))
		wantKey := ""
		if call.IdempotencyKey != "" {
			wantKey = fmt.Sprintf("%x", sha256.Sum256([]byte(call.IdempotencyKey)))
		}
		if wire.CallId != wantID || wire.IdempotencyKey != wantKey || wire.TurnId != turn || wire.StepId != step {
			t.Fatal("未使用完整输入的 SHA-256 或篡改了其他身份", wire)
		}
		if i == 1 && (wire.CallId != previous.CallId || wire.IdempotencyKey != previous.IdempotencyKey) {
			t.Fatal("重试标识不稳定")
		}
		if i == 2 && (wire.CallId == previous.CallId || wire.IdempotencyKey != previous.IdempotencyKey) {
			t.Fatal("完整调用 ID 或独立幂等键未正确映射")
		}
		if i == 3 && (wire.CallId == previous.CallId || wire.IdempotencyKey == previous.IdempotencyKey) {
			t.Fatal("不同末尾被截断为相同标识")
		}
		previous = wire
	}
}

func TestGRPCRemoteRejectsWrongWireIdentityWithoutLocalDrift(t *testing.T) {
	localID := "call-" + strings.Repeat("x", 288)
	for _, mode := range []string{"local_echo", "wrong_event_wire", "wrong_final_wire"} {
		t.Run(mode, func(t *testing.T) {
			client := newGRPCFakeClient(t, &grpcRemoteFake{run: func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
				event := remoteFinal(r.Call.CallId)
				switch mode {
				case "local_echo":
					event.CallId, event.FinalResult.CallId = localID, localID
				case "wrong_event_wire":
					event.CallId = fmt.Sprintf("%x", sha256.Sum256([]byte(localID+"other")))
				case "wrong_final_wire":
					event.FinalResult.CallId = fmt.Sprintf("%x", sha256.Sum256([]byte(localID+"other")))
				}
				return s.Send(event)
			}})
			remote := &grpcRemoteTool{client: client, definition: Definition{Name: "remote", Version: "v2", Generation: 42}}
			result, err := remote.Execute(context.Background(), Call{ID: localID, Arguments: json.RawMessage(`{}`)})
			if !errors.Is(err, ErrGRPCRemoteProtocol) || result.CallID != localID || result.Err != err || result.Content != "" {
				t.Fatal("恶意 wire 身份未拒绝或污染本地结果", result, err)
			}
		})
	}
}

func TestGRPCRemoteRejectsCallsBeforeDispatch(t *testing.T) {
	f := &grpcRemoteFake{}
	h := remoteHandle(t, newGRPCFakeClient(t, f))
	for _, c := range []Call{
		{Arguments: json.RawMessage(`{}`)},
		{ID: "id", SessionID: "other", Arguments: json.RawMessage(`{}`)},
		{ID: "id", Name: "other", Arguments: json.RawMessage(`{}`)},
		{ID: "id", DefinitionVersion: "old", Arguments: json.RawMessage(`{}`)},
		{ID: "id", Generation: 999, Arguments: json.RawMessage(`{}`)},
		{ID: strings.Repeat("x", maxGRPCLocalIDBytes+1), Arguments: json.RawMessage(`{}`)},
		{ID: "id", IdempotencyKey: strings.Repeat("x", maxGRPCLocalIDBytes+1), Arguments: json.RawMessage(`{}`)},
		{ID: "id", IdempotencyKey: "bad\nkey", Arguments: json.RawMessage(`{}`)},
		{ID: "bad\x00id", Arguments: json.RawMessage(`{}`)},
		{ID: "bad\xffid", Arguments: json.RawMessage(`{}`)},
		{ID: "id", Arguments: json.RawMessage(`{"n":1,"n":2}`)},
		{ID: "id", Arguments: json.RawMessage(`{"n":9007199254740993}`)},
		{ID: "id", Arguments: json.RawMessage(`[]`)},
		{ID: "id", Arguments: json.RawMessage(`{} {}`)},
		{ID: "id", Arguments: json.RawMessage(strings.Repeat(" ", MaxArgumentBytes+1))},
	} {
		if _, err := h.Execute(context.Background(), c); err == nil {
			t.Fatal("非法调用被执行", c)
		}
	}
	if f.calls.Load() != 0 {
		t.Fatal("非法参数已发送到远程")
	}
}

func TestGRPCRemoteResultGuardsAndSafeErrors(t *testing.T) {
	cases := []struct {
		name string
		run  func(*protocolv1.ExecuteToolRequest, grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error
		want error
	}{
		{"event_id", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			return s.Send(remoteFinal("wrong"))
		}, ErrGRPCRemoteProtocol},
		{"result_id", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			e := remoteFinal(r.Call.CallId)
			e.FinalResult.CallId = "wrong"
			return s.Send(e)
		}, ErrGRPCRemoteProtocol},
		{"no_final", func(*protocolv1.ExecuteToolRequest, grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			return nil
		}, ErrGRPCRemoteProtocol},
		{"duplicate_final", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			if err := s.Send(remoteFinal(r.Call.CallId)); err != nil {
				return err
			}
			return s.Send(remoteFinal(r.Call.CallId))
		}, ErrGRPCRemoteProtocol},
		{"oversize", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			e := remoteFinal(r.Call.CallId)
			e.FinalResult.Content[0].Text = strings.Repeat("x", MaxOutputBytes+1)
			return s.Send(e)
		}, ErrGRPCRemoteLimit},
		{"many_events", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			for range MaxGRPCRemoteEvents + 1 {
				if err := s.Send(&protocolv1.ToolExecutionEvent{CallId: r.Call.CallId}); err != nil {
					return err
				}
			}
			return nil
		}, ErrGRPCRemoteLimit},
		{"failed", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			e := remoteFinal(r.Call.CallId)
			e.FinalResult.Status = protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_FAILED
			e.FinalResult.Error = &protocolv1.Error{Message: "SECRET"}
			e.FinalResult.Content[0].Text = "SECRET"
			return s.Send(e)
		}, ErrGRPCRemoteFailure},
		{"denied", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			e := remoteFinal(r.Call.CallId)
			e.FinalResult.Status = protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_DENIED
			return s.Send(e)
		}, ErrGRPCRemoteDenied},
		{"cancelled", func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			e := remoteFinal(r.Call.CallId)
			e.FinalResult.Status = protocolv1.ToolResultStatus_TOOL_RESULT_STATUS_CANCELLED
			return s.Send(e)
		}, context.Canceled},
		{"rpc_secret", func(*protocolv1.ExecuteToolRequest, grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
			return status.Error(codes.PermissionDenied, "SECRET")
		}, nil},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			h := remoteHandle(t, newGRPCFakeClient(t, &grpcRemoteFake{run: test.run}))
			result, err := h.Execute(context.Background(), Call{ID: "id", Arguments: json.RawMessage(`{}`)})
			if err == nil || test.want != nil && !errors.Is(err, test.want) || strings.Contains(err.Error(), "SECRET") || strings.Contains(result.Content, "SECRET") || result.CallID != "id" {
				t.Fatal(result, err)
			}
			if test.name == "rpc_secret" && status.Code(err) != codes.PermissionDenied {
				t.Fatal(err)
			}
		})
	}
}

func TestGRPCRemoteConcurrentDeadlineCancelAndClose(t *testing.T) {
	t.Run("concurrent", func(t *testing.T) {
		h := remoteHandle(t, newGRPCFakeClient(t, &grpcRemoteFake{}))
		var wg sync.WaitGroup
		for i := 0; i < 24; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				id := fmt.Sprintf("id%d", i)
				r, e := h.Execute(context.Background(), Call{ID: id, Arguments: json.RawMessage(`{}`)})
				if e != nil || r.CallID != id {
					t.Error(r, e)
				}
			}(i)
		}
		wg.Wait()
	})
	for _, mode := range []string{"deadline", "cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			f := &grpcRemoteFake{run: func(r *protocolv1.ExecuteToolRequest, s grpc.ServerStreamingServer[protocolv1.ToolExecutionEvent]) error {
				close(entered)
				<-s.Context().Done()
				close(stopped)
				return s.Context().Err()
			}}
			c := newGRPCFakeClient(t, f)
			h := remoteHandle(t, c)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			call := Call{ID: "id", Arguments: json.RawMessage(`{}`)}
			if mode == "deadline" {
				call.Timeout = 80 * time.Millisecond
			}
			done := make(chan error, 1)
			go func() { _, e := h.Execute(ctx, call); done <- e }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("服务端未收到请求")
			}
			want := context.DeadlineExceeded
			if mode == "cancel" {
				cancel()
				want = context.Canceled
			}
			if mode == "close" {
				_ = c.Close()
				want = ErrGRPCRemoteClosed
			}
			select {
			case err := <-done:
				if !errors.Is(err, want) {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("取消未结束")
			}
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("服务端未收到取消")
			}
		})
	}
}

func TestGRPCRemoteDefinitionValidation(t *testing.T) {
	for _, mode := range []string{"duplicate", "schema", "generation", "list_error"} {
		t.Run(mode, func(t *testing.T) {
			c := newGRPCFakeClient(t, &grpcRemoteFake{list: func(context.Context, *protocolv1.ListToolsRequest) (*protocolv1.ListToolsResponse, error) {
				d := remoteDefinition()
				r := &protocolv1.ListToolsResponse{Tools: []*protocolv1.ToolDefinition{d}}
				switch mode {
				case "duplicate":
					r.Tools = append(r.Tools, d)
				case "schema":
					d.InputSchema = remoteSchema(`{"pattern":"SECRET"}`)
				case "generation":
					d.Generation = 0
				case "list_error":
					return nil, status.Error(codes.Unavailable, "SECRET")
				}
				return r, nil
			}})
			if _, err := c.Tools(context.Background()); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatal(err)
			}
		})
	}
}
