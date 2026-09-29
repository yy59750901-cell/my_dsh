package grpcapi

import (
	"context"
	"strings"
	"time"

	pb "github.com/yy59750901/go-dsh/gen/go/dsh/protocol/v1"
	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type service struct {
	pb.UnimplementedAgentServiceServer
	pb.UnimplementedSessionServiceServer
	pb.UnimplementedToolServiceServer
	pb.UnimplementedApprovalServiceServer
	h     *agent.Harness
	tools *tool.Registry
}

func NewServerWithHarness(h *agent.Harness, tools *tool.Registry, token string) *grpc.Server {
	auth := func(ctx context.Context, method string) error {
		if strings.HasPrefix(method, "/grpc.health.v1.Health/") {
			return nil
		}
		md, _ := metadata.FromIncomingContext(ctx)
		if !httpapi.BearerAuthorized(md.Get("authorization"), token) {
			return status.Error(codes.Unauthenticated, "unauthenticated")
		}
		if h == nil {
			return status.Error(codes.Unavailable, "unavailable")
		}
		return nil
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if err := auth(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		result, err := next(ctx, req)
		if err != nil {
			return nil, safeError(err)
		}
		return redactMessage(result, token)
	}), grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if err := auth(stream.Context(), info.FullMethod); err != nil {
			return err
		}
		return safeError(next(srv, &safeStream{ServerStream: stream, token: token}))
	}))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthServer)
	svc := &service{h: h, tools: tools}
	pb.RegisterAgentServiceServer(server, svc)
	pb.RegisterSessionServiceServer(server, svc)
	pb.RegisterToolServiceServer(server, svc)
	pb.RegisterApprovalServiceServer(server, svc)
	return server
}
func redactMessage(value any, token string) (any, error) {
	message, ok := value.(proto.Message)
	if !ok {
		return nil, status.Error(codes.Internal, "internal")
	}
	raw, err := protojson.Marshal(message)
	if err == nil {
		raw, err = httpapi.RedactJSON(raw, token)
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "internal")
	}
	result := message.ProtoReflect().Type().New().Interface()
	if err = protojson.Unmarshal(raw, result); err != nil {
		return nil, status.Error(codes.Internal, "internal")
	}
	return result, nil
}

type safeStream struct {
	grpc.ServerStream
	token string
}

func (s *safeStream) SendMsg(value any) error {
	result, err := redactMessage(value, s.token)
	if err != nil {
		return err
	}
	return s.ServerStream.SendMsg(result)
}

func contentText(parts []*pb.ContentPart) (string, error) {
	var text strings.Builder
	if len(parts) == 0 {
		return "", status.Error(codes.InvalidArgument, "content_required")
	}
	for _, part := range parts {
		if part == nil || part.Type != "text" || len(part.GetData().GetFields()) != 0 || len(part.ProtoReflect().GetUnknown()) != 0 {
			return "", status.Error(codes.InvalidArgument, "unsupported_content_block")
		}
		text.WriteString(part.Text)
	}
	if strings.TrimSpace(text.String()) == "" {
		return "", status.Error(codes.InvalidArgument, "content_required")
	}
	return text.String(), nil
}
func messageText(message *pb.UserMessage) (string, error) {
	if message == nil {
		return "", status.Error(codes.InvalidArgument, "message_required")
	}
	if len(message.GetMetadata().GetFields()) != 0 {
		return "", status.Error(codes.Unimplemented, "message_metadata_unimplemented")
	}
	return contentText(message.Content)
}
func (s *service) submit(ctx context.Context, id, text, key string, steer bool) (*pb.SubmitPromptResponse, error) {
	if len(key) > 256 {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key_limit")
	}
	if _, err := httpapi.FindSession(ctx, s.h, id); err != nil {
		return nil, err
	}
	var r agent.Receipt
	var err error
	if steer {
		r, err = s.h.Steer(ctx, id, text, key)
	} else {
		r, err = s.h.Prompt(ctx, id, text, key)
	}
	if err != nil {
		return nil, err
	}
	current, err := s.GetStatus(ctx, &pb.GetAgentStatusRequest{SessionId: id})
	if err != nil {
		return nil, err
	}
	return &pb.SubmitPromptResponse{Receipt: receiptProto(r), Status: current, NoOp: r.Duplicate}, nil
}
func (s *service) SubmitPrompt(ctx context.Context, r *pb.SubmitPromptRequest) (*pb.SubmitPromptResponse, error) {
	text, err := messageText(r.Message)
	if err != nil {
		return nil, err
	}
	return s.submit(ctx, r.SessionId, text, r.IdempotencyKey, false)
}
func (s *service) Steer(ctx context.Context, r *pb.SteerRequest) (*pb.SubmitPromptResponse, error) {
	text, err := messageText(r.Message)
	if err != nil {
		return nil, err
	}
	return s.submit(ctx, r.SessionId, text, r.IdempotencyKey, true)
}
func (s *service) Inject(ctx context.Context, r *pb.InjectRequest) (*pb.SubmitPromptResponse, error) {
	text, err := contentText(r.Content)
	if err != nil {
		return nil, err
	}
	if r.IdempotencyKey != "" || (r.Source != "" && r.Source != "system") || len(r.GetMetadata().GetFields()) != 0 {
		return nil, status.Error(codes.Unimplemented, "inject_options_unimplemented")
	}
	if _, err = httpapi.FindSession(ctx, s.h, r.SessionId); err != nil {
		return nil, err
	}
	if err = s.h.Inject(ctx, r.SessionId, text); err != nil {
		return nil, err
	}
	current, err := s.GetStatus(ctx, &pb.GetAgentStatusRequest{SessionId: r.SessionId})
	return &pb.SubmitPromptResponse{Status: current}, err
}
func (s *service) Cancel(ctx context.Context, r *pb.CancelAgentRequest) (*pb.SubmitPromptResponse, error) {
	if r.KeepInbox || r.IdempotencyKey != "" || (r.GetCancellation().GetSource() != pb.CancelSource_CANCEL_SOURCE_UNSPECIFIED && r.GetCancellation().GetSource() != pb.CancelSource_CANCEL_SOURCE_USER) || r.GetCancellation().GetCancelledAt() != nil {
		return nil, status.Error(codes.Unimplemented, "cancel_options_unimplemented")
	}
	if _, err := httpapi.FindSession(ctx, s.h, r.SessionId); err != nil {
		return nil, err
	}
	if err := s.h.Cancel(ctx, r.SessionId, r.GetCancellation().GetReason()); err != nil {
		return nil, err
	}
	current, err := s.GetStatus(ctx, &pb.GetAgentStatusRequest{SessionId: r.SessionId})
	return &pb.SubmitPromptResponse{Status: current}, err
}
func (s *service) GetStatus(ctx context.Context, r *pb.GetAgentStatusRequest) (*pb.AgentStatus, error) {
	snapshot, _, err := httpapi.ReadSnapshot(ctx, s.h, r.SessionId)
	if err != nil {
		return nil, err
	}
	return statusProto(snapshot), nil
}
func (s *service) CreateSession(ctx context.Context, r *pb.CreateSessionRequest) (*pb.CreateSessionResponse, error) {
	if r.ParentId != "" || r.ForkSeq != nil {
		return nil, status.Error(codes.Unimplemented, "fork_unimplemented")
	}
	if (r.TenantId != "" && r.TenantId != "local") || (r.WorkspaceId != "" && r.WorkspaceId != "default") {
		return nil, status.Error(codes.InvalidArgument, "local_single_user_only")
	}
	id, err := s.h.CreateSession(ctx, "")
	if err != nil {
		return nil, err
	}
	result, err := s.GetSession(ctx, &pb.GetSessionRequest{SessionId: id})
	if err != nil {
		return nil, err
	}
	return &pb.CreateSessionResponse{Session: result.Session}, nil
}
func (s *service) GetSession(ctx context.Context, r *pb.GetSessionRequest) (*pb.GetSessionResponse, error) {
	snapshot, events, err := httpapi.ReadSnapshot(ctx, s.h, r.SessionId)
	if err != nil {
		return nil, err
	}
	return &pb.GetSessionResponse{Session: sessionProto(snapshot, events)}, nil
}
func (s *service) ListEvents(ctx context.Context, r *pb.ListEventsRequest) (*pb.ListEventsResponse, error) {
	if r.Limit > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit")
	}
	limit := int(r.Limit)
	if limit == 0 {
		limit = 1000
	}
	snapshot, events, err := httpapi.ReadSnapshot(ctx, s.h, r.SessionId)
	if err != nil {
		return nil, err
	}
	result := &pb.ListEventsResponse{HeadSeq: snapshot.HeadSeq}
	for _, e := range events {
		if e.Seq <= r.AfterSeq {
			continue
		}
		if len(result.Events) == limit {
			result.HasMore = true
			break
		}
		mapped, err := eventProto(e)
		if err != nil {
			return nil, err
		}
		result.Events = append(result.Events, mapped)
	}
	return result, nil
}
func (s *service) SubscribeEvents(r *pb.SubscribeEventsRequest, stream grpc.ServerStreamingServer[pb.EventEnvelope]) error {
	ctx := stream.Context()
	if _, err := httpapi.FindSession(ctx, s.h, r.SessionId); err != nil {
		return err
	}
	after := r.AfterSeq
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		events, err := s.h.ListEvents(ctx, r.SessionId, after, 1000)
		if err != nil {
			return err
		}
		for _, e := range events {
			if e.Seq <= after {
				continue
			}
			mapped, err := eventProto(e)
			if err != nil {
				return err
			}
			if err = stream.Send(mapped); err != nil {
				return err
			}
			after = e.Seq
		}
		if len(events) == 1000 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}
func (s *service) ForkSession(context.Context, *pb.ForkSessionRequest) (*pb.ForkSessionResponse, error) {
	return nil, status.Error(codes.Unimplemented, "fork_unimplemented")
}
func (s *service) ListTools(ctx context.Context, r *pb.ListToolsRequest) (*pb.ListToolsResponse, error) {
	if r.SessionId != "" {
		if _, err := httpapi.FindSession(ctx, s.h, r.SessionId); err != nil {
			return nil, err
		}
	}
	result := &pb.ListToolsResponse{}
	if s.tools != nil {
		for _, d := range s.tools.Definitions() {
			mapped, err := toolProto(d)
			if err != nil {
				return nil, err
			}
			result.Tools = append(result.Tools, mapped)
		}
	}
	return result, nil
}

// Execute 刻意不可用：工具执行只能由 Harness 在审批校验后驱动。
func (s *service) Execute(*pb.ExecuteToolRequest, grpc.ServerStreamingServer[pb.ToolExecutionEvent]) error {
	return status.Error(codes.Unimplemented, "direct_tool_execution_disabled")
}
func (s *service) Resolve(ctx context.Context, r *pb.ResolveApprovalRequest) (*pb.ResolveApprovalResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	ids := md.Get("x-dsh-session-id")
	if len(ids) != 1 || ids[0] == "" || r.ApprovalId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_metadata_and_approval_id_required")
	}
	if r.Decision != pb.ApprovalStatus_APPROVAL_STATUS_APPROVED && r.Decision != pb.ApprovalStatus_APPROVAL_STATUS_REJECTED {
		return nil, status.Error(codes.InvalidArgument, "approval_decision")
	}
	if r.DecidedBy != "" || r.Reason != "" {
		return nil, status.Error(codes.Unimplemented, "approval_audit_fields_unimplemented")
	}
	snapshot, _, err := httpapi.ReadSnapshot(ctx, s.h, ids[0])
	if err != nil {
		return nil, err
	}
	p, _ := agent.ProjectionFrom(snapshot)
	callID := ""
	for _, call := range p.ToolCalls {
		if call.ApprovalID == r.ApprovalId {
			callID = call.Call.ID
			break
		}
	}
	if callID == "" {
		return nil, agent.ErrApprovalNotFound
	}
	if err = s.h.ResolveApproval(ctx, ids[0], r.ApprovalId, r.Decision == pb.ApprovalStatus_APPROVAL_STATUS_APPROVED); err != nil {
		return nil, err
	}
	return &pb.ResolveApprovalResponse{Approval: &pb.Approval{ApprovalId: r.ApprovalId, SessionId: ids[0], CallId: callID, Status: r.Decision}}, nil
}
