package grpcapi

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/yy59750901/go-dsh/gen/go/dsh/protocol/v1"
	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const grpcToken = "grpc-secret-2809c425"

type guardedEcho struct{ calls atomic.Int32 }

func (e *guardedEcho) Definition() tool.Definition {
	d := tool.NewEchoTool().Definition()
	d.RequiresApproval = true
	return d
}
func (e *guardedEcho) Execute(ctx context.Context, c tool.Call) (tool.Result, error) {
	e.calls.Add(1)
	return tool.NewEchoTool().Execute(ctx, c)
}
func bufClient(t *testing.T, s *grpc.Server) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	go func() { _ = s.Serve(listener) }()
	t.Cleanup(func() { s.Stop(); _ = listener.Close() })
	c, err := grpc.NewClient("passthrough:///bufconn", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func grpcFixture(t *testing.T, guard *guardedEcho) (*agent.Harness, *grpc.ClientConn, context.Context) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "grpc.sqlite")+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sql.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sql.Close() })
	if err = db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	var echo tool.Tool = tool.NewEchoTool()
	if guard != nil {
		echo = guard
	}
	if err = tools.Register(echo); err != nil {
		t.Fatal(err)
	}
	h, err := agent.NewHarness(gormrepo.NewEventStore(db), llm.NewDemoProvider(), tools, agent.HarnessOptions{Model: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	conn := bufClient(t, NewServerWithHarness(h, tools, grpcToken))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return h, conn, metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+grpcToken))
}
func createRPC(t *testing.T, ctx context.Context, c pb.SessionServiceClient) string {
	t.Helper()
	r, err := c.CreateSession(ctx, &pb.CreateSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Session.TenantId != "local" || r.Session.WorkspaceId != "default" || r.Session.LastSeq != 2 {
		t.Fatalf("会话映射=%+v", r.Session)
	}
	return r.Session.Id
}
func waitRPCEvents(t *testing.T, h *agent.Harness, id, kind string) []session.Event {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		es, err := h.ListEvents(context.Background(), id, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range es {
			if e.EventType == kind {
				return es
			}
		}
		select {
		case <-tick.C:
		case <-timeout.C:
			t.Fatalf("等待 %s 超时", kind)
		}
	}
}
func TestGRPCHarnessTextCursorAndMapping(t *testing.T) {
	h, conn, ctx := grpcFixture(t, nil)
	sessions := pb.NewSessionServiceClient(conn)
	agents := pb.NewAgentServiceClient(conn)
	id := createRPC(t, ctx, sessions)
	input := &pb.SubmitPromptRequest{SessionId: id, Message: &pb.UserMessage{Content: []*pb.ContentPart{{Type: "text", Text: "你好"}, {Type: "text", Text: "世界"}}}, IdempotencyKey: "stable"}
	receipt, err := agents.SubmitPrompt(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Receipt.AcceptedSeq == 0 || receipt.Receipt.InputId == "" || receipt.Receipt.IdempotencyKey != "stable" {
		t.Fatalf("回执=%+v", receipt)
	}
	events := waitRPCEvents(t, h, id, "turn/end")
	duplicate, err := agents.SubmitPrompt(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.NoOp || !duplicate.Receipt.Duplicate || duplicate.Receipt.AcceptedSeq != receipt.Receipt.AcceptedSeq {
		t.Fatal("幂等回执不一致")
	}
	response, err := sessions.ListEvents(ctx, &pb.ListEventsRequest{SessionId: id, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Events) != 1 || !response.HasMore || response.HeadSeq != events[len(events)-1].Seq {
		t.Fatalf("事件分页=%+v", response)
	}
	full, err := sessions.ListEvents(ctx, &pb.ListEventsRequest{SessionId: id})
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Events) != len(events) {
		t.Fatal("事件数不符")
	}
	found := false
	for i, e := range full.Events {
		expected, err := eventProto(events[i])
		if err != nil {
			t.Fatal(err)
		}
		if events[i].EventType == agent.EventHarnessConfigured {
			expected.Data.Fields["system_prompt"] = structpb.NewStringValue("[REDACTED]")
		}
		if !proto.Equal(e, expected) {
			t.Fatalf("第 %d 个事件映射不符", i)
		}
		raw, _ := protojson.Marshal(e)
		if strings.Contains(string(raw), "你好世界") {
			found = true
		}
		if e.EventType == "assistant/message" && e.GetSurfaceOperation().GetKind() != pb.SurfaceOperationKind_SURFACE_OPERATION_KIND_APPEND {
			t.Fatal("surface 映射丢失")
		}
	}
	if !found {
		t.Fatal("多个文本块没有完整传入")
	}
	current, err := agents.GetStatus(ctx, &pb.GetAgentStatusRequest{SessionId: id})
	if err != nil {
		t.Fatal(err)
	}
	if current.State != pb.AgentState_AGENT_STATE_IDLE || current.LastEventSeq != response.HeadSeq {
		t.Fatalf("状态=%+v", current)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := sessions.SubscribeEvents(streamCtx, &pb.SubscribeEventsRequest{SessionId: id, AfterSeq: 1})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 2 {
		t.Fatal("订阅游标不正确")
	}
	cancel()
	streamCtx, cancel = context.WithCancel(ctx)
	defer cancel()
	stream, err = sessions.SubscribeEvents(streamCtx, &pb.SubscribeEventsRequest{SessionId: id, AfterSeq: first.Seq})
	if err != nil {
		t.Fatal(err)
	}
	next, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if next.Seq != 3 {
		t.Fatal("断线重连丢失事件")
	}
	list, err := pb.NewToolServiceClient(conn).ListTools(ctx, &pb.ListToolsRequest{SessionId: id})
	if err != nil || len(list.GetTools()) != 1 || list.Tools[0].Name != "echo" || list.Tools[0].InputSchema == nil {
		t.Fatalf("工具映射=%+v %v", list, err)
	}
}
func TestGRPCAuthValidationAndUnimplemented(t *testing.T) {
	_, conn, ctx := grpcFixture(t, nil)
	sessions := pb.NewSessionServiceClient(conn)
	agents := pb.NewAgentServiceClient(conn)
	id := createRPC(t, ctx, sessions)
	if _, err := sessions.GetSession(context.Background(), &pb.GetSessionRequest{SessionId: id}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("鉴权=%v", err)
	}
	duplicateAuth := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+grpcToken, "authorization", "Bearer "+grpcToken))
	if _, err := agents.GetStatus(duplicateAuth, &pb.GetAgentStatusRequest{SessionId: id}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("重复鉴权=%v", err)
	}
	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	data, _ := structpb.NewStruct(map[string]any{"url": "local-image"})
	for _, part := range []*pb.ContentPart{{Type: "image", Data: data}, {Type: "unknown", Text: "x"}, {Type: "text", Text: "x", Data: data}, {Text: "x"}} {
		message := &pb.UserMessage{Content: []*pb.ContentPart{{Type: "text", Text: "不能只发送这段"}, part}}
		if _, err := agents.SubmitPrompt(ctx, &pb.SubmitPromptRequest{SessionId: id, Message: message}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Prompt 内容校验=%v", err)
		}
		if _, err := agents.Steer(ctx, &pb.SteerRequest{SessionId: id, Message: message}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Steer 内容校验=%v", err)
		}
		if _, err := agents.Inject(ctx, &pb.InjectRequest{SessionId: id, Content: message.Content}); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Inject 内容校验=%v", err)
		}
	}
	if _, err := sessions.ListEvents(ctx, &pb.ListEventsRequest{SessionId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("未知会话=%v", err)
	}
	sub, err := sessions.SubscribeEvents(ctx, &pb.SubscribeEventsRequest{SessionId: "missing"})
	if err == nil {
		_, err = sub.Recv()
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("未知订阅=%v", err)
	}
	if _, err := sessions.ForkSession(ctx, &pb.ForkSessionRequest{SourceSessionId: id}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Fork=%v", err)
	}
	execute, err := pb.NewToolServiceClient(conn).Execute(ctx, &pb.ExecuteToolRequest{Call: &pb.ToolCall{SessionId: id, ToolName: "echo"}})
	if err == nil {
		_, err = execute.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("直接工具执行=%v", err)
	}
	if _, err := agents.Cancel(ctx, &pb.CancelAgentRequest{SessionId: id, KeepInbox: true}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("KeepInbox=%v", err)
	}
	if _, err := agents.Inject(ctx, &pb.InjectRequest{SessionId: id, Content: []*pb.ContentPart{{Type: "text", Text: "背景"}}, IdempotencyKey: "unsupported"}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Inject 幂等=%v", err)
	}
	if _, err := sessions.CreateSession(ctx, &pb.CreateSessionRequest{TenantId: "other"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("多租户=%v", err)
	}
	tooLarge := &pb.SubmitPromptRequest{SessionId: id, Message: &pb.UserMessage{Content: []*pb.ContentPart{{Type: "text", Text: strings.Repeat("a", 1<<20)}}}}
	if _, err := agents.SubmitPrompt(ctx, tooLarge); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("请求上限=%v", err)
	}
}
func TestGRPCApprovalScopeAndCancel(t *testing.T) {
	guard := &guardedEcho{}
	h, conn, ctx := grpcFixture(t, guard)
	sessions := pb.NewSessionServiceClient(conn)
	agents := pb.NewAgentServiceClient(conn)
	approvals := pb.NewApprovalServiceClient(conn)
	id := createRPC(t, ctx, sessions)
	other := createRPC(t, ctx, sessions)
	if _, err := agents.Inject(ctx, &pb.InjectRequest{SessionId: id, Content: []*pb.ContentPart{{Type: "text", Text: "背景"}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := agents.Steer(ctx, &pb.SteerRequest{SessionId: id, Message: &pb.UserMessage{Content: []*pb.ContentPart{{Type: "text", Text: "使用工具"}}}, IdempotencyKey: "tool"}); err != nil {
		t.Fatal(err)
	}
	es := waitRPCEvents(t, h, id, "approval/requested")
	approvalID := ""
	for _, e := range es {
		if e.EventType == "approval/requested" {
			var a agent.ApprovalState
			_ = json.Unmarshal(e.Data, &a)
			approvalID = a.ID
		}
	}
	req := &pb.ResolveApprovalRequest{ApprovalId: approvalID, Decision: pb.ApprovalStatus_APPROVAL_STATUS_APPROVED}
	if _, err := approvals.Resolve(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺少会话 metadata=%v", err)
	}
	wrong := metadata.AppendToOutgoingContext(ctx, "x-dsh-session-id", other)
	if _, err := approvals.Resolve(wrong, req); status.Code(err) != codes.NotFound {
		t.Fatalf("跨会话审批=%v", err)
	}
	scoped := metadata.AppendToOutgoingContext(ctx, "x-dsh-session-id", id)
	if _, err := approvals.Resolve(scoped, &pb.ResolveApprovalRequest{ApprovalId: approvalID}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("无效 decision=%v", err)
	}
	if guard.calls.Load() != 0 {
		t.Fatal("审批前执行了工具")
	}
	result, err := approvals.Resolve(scoped, req)
	if err != nil {
		t.Fatal(err)
	}
	if result.Approval.SessionId != id || result.Approval.Status != req.Decision || result.Approval.CallId == "" {
		t.Fatalf("审批映射=%+v", result)
	}
	waitRPCEvents(t, h, id, "turn/end")
	if guard.calls.Load() != 1 {
		t.Fatalf("执行次数=%d", guard.calls.Load())
	}
	if _, err := approvals.Resolve(scoped, req); err != nil {
		t.Fatalf("相同审批重试=%v", err)
	}
	req.Decision = pb.ApprovalStatus_APPROVAL_STATUS_REJECTED
	if _, err := approvals.Resolve(scoped, req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("反向审批=%v", err)
	}
	if guard.calls.Load() != 1 {
		t.Fatal("重复审批重新执行了工具")
	}
	if _, err := agents.Cancel(ctx, &pb.CancelAgentRequest{SessionId: id, Cancellation: &pb.Cancellation{Source: pb.CancelSource_CANCEL_SOURCE_USER, Reason: "用户取消"}}); err != nil {
		t.Fatal(err)
	}
}
func TestEventProtoMappingAndProtoJSON(t *testing.T) {
	now := time.Now().UTC()
	e := session.Event{SchemaVersion: session.SchemaVersion{Major: 1, Minor: 1}, EventType: "assistant/message", EventID: "event", SessionID: "session", Seq: 9007199254740993, OccurredAt: now, CommittedAt: now, ReplayPolicy: session.ReplayRequired, Data: json.RawMessage(`{"message":{"role":"assistant","content":[]}}`), TurnID: "turn", StepID: "step", CallID: "call", Trace: session.TraceContext{TraceID: "trace", SpanID: "span", ParentSpanID: "parent"}, CausationEventID: "cause", SourceEventSeqs: []uint64{9007199254740992}, SurfaceOp: session.ReplaceSurfaceOp(1, 2), Extensions: json.RawMessage(`{"custom":true}`)}
	mapped, err := eventProto(e)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := protojson.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	var restored pb.EventEnvelope
	if err = protojson.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Seq != e.Seq || restored.SourceEventSeqs[0] != e.SourceEventSeqs[0] || restored.Trace.ParentSpanId != "parent" || restored.CausationEventId != "cause" || restored.SurfaceOperation.ReplaceEndSeq != 2 || restored.Extensions.Fields["custom"].GetBoolValue() != true {
		t.Fatalf("映射丢字段=%s", raw)
	}
}
func TestGRPCHealthCompatibility(t *testing.T) {
	conn := bufClient(t, NewServer())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || r.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health=%+v %v", r, err)
	}
}
