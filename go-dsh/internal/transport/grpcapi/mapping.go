package grpcapi

import (
	"context"
	"encoding/json"
	"errors"

	pb "github.com/yy59750901/go-dsh/gen/go/dsh/protocol/v1"
	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func safeError(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		return status.Error(s.Code(), s.Code().String())
	}
	code := codes.Internal
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, session.ErrNotFound), errors.Is(err, agent.ErrApprovalNotFound):
		code = codes.NotFound
	case errors.Is(err, session.ErrInvalidSession), errors.Is(err, agent.ErrInvalidInboxRequest):
		code = codes.InvalidArgument
	case errors.Is(err, agent.ErrIdempotencyConflict), errors.Is(err, session.ErrConflict):
		code = codes.AlreadyExists
	case errors.Is(err, agent.ErrApprovalResolved), errors.Is(err, agent.ErrApprovalNotPending):
		code = codes.FailedPrecondition
	case errors.Is(err, agent.ErrHarnessClosed), errors.Is(err, session.ErrRegistryClosed):
		code = codes.Unavailable
	}
	return status.Error(code, code.String())
}
func receiptProto(r agent.Receipt) *pb.Receipt {
	return &pb.Receipt{RequestId: r.RequestID, SessionId: r.SessionID, CommandId: r.CommandID, AcceptedSeq: r.AcceptedSeq, Placement: r.Placement, Duplicate: r.Duplicate, AcceptedAt: timestamppb.New(r.AcceptedAt), InputId: r.InputID, Target: string(r.Target), IdempotencyKey: r.IdempotencyKey}
}
func statusProto(snapshot session.Snapshot) *pb.AgentStatus {
	v := httpapi.SnapshotStatus(snapshot)
	states := map[agent.State]pb.AgentState{agent.StateIdle: pb.AgentState_AGENT_STATE_IDLE, agent.StateRunning: pb.AgentState_AGENT_STATE_RUNNING, agent.StateWaitingApproval: pb.AgentState_AGENT_STATE_WAITING_APPROVAL, agent.StateCancelling: pb.AgentState_AGENT_STATE_CANCELLING, agent.StateFailed: pb.AgentState_AGENT_STATE_FAILED, agent.StateDisposed: pb.AgentState_AGENT_STATE_DISPOSED}
	result := &pb.AgentStatus{SessionId: v.SessionID, State: states[v.State], ActiveTurnId: v.ActiveTurnID, ActiveStepId: v.ActiveStepID, ActiveAttempt: v.ActiveAttempt, QueuedMessages: uint32(v.NextTurnMessages + v.NextStepMessages), NextTurnMessages: uint32(v.NextTurnMessages), NextStepMessages: uint32(v.NextStepMessages), LastEventSeq: v.LastEventSeq}
	if v.ActiveAttempt > 0 {
		result.RetryCount = v.ActiveAttempt - 1
	}
	p, _ := agent.ProjectionFrom(snapshot)
	if c := p.CancelCause; c != nil {
		sources := map[string]pb.CancelSource{"user": pb.CancelSource_CANCEL_SOURCE_USER, "client-disconnect": pb.CancelSource_CANCEL_SOURCE_CLIENT_DISCONNECT, "deadline": pb.CancelSource_CANCEL_SOURCE_DEADLINE, "parent": pb.CancelSource_CANCEL_SOURCE_PARENT, "policy": pb.CancelSource_CANCEL_SOURCE_POLICY, "session-dispose": pb.CancelSource_CANCEL_SOURCE_SESSION_DISPOSE, "server-shutdown": pb.CancelSource_CANCEL_SOURCE_SERVER_SHUTDOWN, "tool-provider": pb.CancelSource_CANCEL_SOURCE_TOOL_PROVIDER, "model-provider": pb.CancelSource_CANCEL_SOURCE_MODEL_PROVIDER, "system": pb.CancelSource_CANCEL_SOURCE_SYSTEM}
		result.Cancellation = &pb.Cancellation{Source: sources[string(c.Source)], Reason: c.Reason}
	}
	return result
}
func sessionProto(snapshot session.Snapshot, events []session.Event) *pb.Session {
	result := &pb.Session{Id: snapshot.SessionID, TenantId: "local", WorkspaceId: "default", Status: string(httpapi.SnapshotStatus(snapshot).State), LastSeq: snapshot.HeadSeq}
	if len(events) > 0 {
		result.CreatedAt = timestamppb.New(events[0].OccurredAt)
		result.UpdatedAt = timestamppb.New(events[len(events)-1].CommittedAt)
	}
	return result
}
func object(raw json.RawMessage) (*structpb.Struct, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	v := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, v); err != nil {
		return nil, err
	}
	return v, nil
}
func eventProto(e session.Event) (*pb.EventEnvelope, error) {
	data, err := object(e.Data)
	if err != nil {
		return nil, err
	}
	extensions, err := object(e.Extensions)
	if err != nil {
		return nil, err
	}
	policy := pb.ReplayPolicy_REPLAY_POLICY_UNSPECIFIED
	switch e.ReplayPolicy {
	case session.ReplayRequired:
		policy = pb.ReplayPolicy_REPLAY_POLICY_REQUIRED
	case session.ReplayIgnorable:
		policy = pb.ReplayPolicy_REPLAY_POLICY_IGNORABLE
	default:
		return nil, session.ErrInvalidEvent
	}
	result := &pb.EventEnvelope{SchemaVersion: &pb.SchemaVersion{Major: e.SchemaVersion.Major, Minor: e.SchemaVersion.Minor}, EventType: e.EventType, EventId: e.EventID, SessionId: e.SessionID, Seq: e.Seq, OccurredAt: timestamppb.New(e.OccurredAt), CommittedAt: timestamppb.New(e.CommittedAt), ReplayPolicy: policy, Data: data, TurnId: e.TurnID, StepId: e.StepID, CallId: e.CallID, Trace: &pb.TraceContext{TraceId: e.Trace.TraceID, SpanId: e.Trace.SpanID, ParentSpanId: e.Trace.ParentSpanID}, CausationEventId: e.CausationEventID, SourceEventSeqs: append([]uint64(nil), e.SourceEventSeqs...), Extensions: extensions}
	if e.SurfaceOp != nil {
		op := &pb.SurfaceOperation{}
		switch e.SurfaceOp.Kind {
		case session.SurfaceOpAppend:
			op.Kind = pb.SurfaceOperationKind_SURFACE_OPERATION_KIND_APPEND
		case session.SurfaceOpReplace:
			op.Kind = pb.SurfaceOperationKind_SURFACE_OPERATION_KIND_REPLACE
			op.ReplaceStartSeq = e.SurfaceOp.ReplaceStart
			op.ReplaceEndSeq = e.SurfaceOp.ReplaceEnd
		default:
			return nil, session.ErrInvalidSurface
		}
		result.SurfaceOperation = op
	}
	return result, nil
}
func toolProto(d tool.Definition) (*pb.ToolDefinition, error) {
	input, err := object(d.InputSchema)
	if err != nil {
		return nil, err
	}
	output, err := object(d.OutputSchema)
	if err != nil {
		return nil, err
	}
	return &pb.ToolDefinition{Name: d.Name, Description: d.Description, InputSchema: input, OutputSchema: output, Version: d.Version, Generation: d.Generation, RequiredCapabilities: append([]string(nil), d.RequiredCapabilities...)}, nil
}
