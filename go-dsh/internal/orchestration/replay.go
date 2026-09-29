package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/gorm"
)

func promptKey(jobID string) string { return "orchestration:" + jobID }

// 固定 head 的只读 Replay 不 ClaimWriter、不修复、不读取 Actor 内存，
// 也不会让追加中的事件跨过本次读取边界。
type replayStore struct {
	session.EventStore
	head   uint64
	events []session.Event
}

func (s *replayStore) Load(ctx context.Context, id string, after uint64, limit int) ([]session.Event, error) {
	if after >= s.head {
		return nil, nil
	}
	if uint64(limit) > s.head-after {
		limit = int(s.head - after)
	}
	es, err := s.EventStore.Load(ctx, id, after, limit)
	if err == nil {
		s.events = append(s.events, es...)
	}
	return es, err
}

func (m *Manager) jobResult(ctx context.Context, row jobRecord) (Job, error) {
	job := row.Job
	job.State, job.Outcome, job.TurnID, job.AcceptedSeq, job.EndSeq = Pending, "", "", 0, 0
	if job.CancelRequested {
		job.State = Cancelled
		job.Outcome = "cancelled-before-input"
	}
	var child gormrepo.SessionModel
	err := m.db.WithContext(ctx).Where("id = ?", job.ChildSessionID).First(&child).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return job, nil
	}
	if err != nil {
		return Job{}, storageError(ctx)
	}
	if child.ParentID == nil || *child.ParentID != job.ParentID || child.ForkSeq == nil || *child.ForkSeq != 0 || child.TenantID != "local" || child.WorkspaceID != "default" {
		return Job{}, ErrConflict
	}
	head, err := m.store.Head(ctx, job.ChildSessionID)
	if err != nil {
		return Job{}, ErrStorage
	}
	schema, err := agent.NewSessionSchema()
	if err != nil {
		return Job{}, ErrExecution
	}
	store := &replayStore{EventStore: m.store, head: head}
	snapshot, err := session.Replay(ctx, store, job.ChildSessionID, 0, head, session.ReplayOptions{Schema: schema})
	if err != nil {
		return Job{}, ErrExecution
	}
	projection, ok := agent.ProjectionFrom(snapshot)
	if !ok {
		return Job{}, ErrExecution
	}
	accepted, ok := projection.AcceptedByIdempotency(agent.CommandFollowUp, promptKey(job.ID))
	if !ok {
		return job, nil
	}
	// key 命中还必须验证完整输入，防止其他入口误用保留 key。
	if len(accepted.Item.Content) != 1 || accepted.Item.Content[0].Type != "text" || accepted.Item.Content[0].Text != job.Prompt {
		return Job{}, ErrConflict
	}
	job.AcceptedSeq, job.State, job.Outcome = accepted.Item.AcceptedSeq, Running, ""
	for _, claim := range projection.ClaimsByID {
		if slices.Contains(claim.OrderedInputIDs, accepted.Item.InputID) {
			job.TurnID = claim.TurnID
			break
		}
	}
	if job.TurnID == "" {
		queued := false
		for _, item := range append(projection.Queue(agent.InboxNextTurn), projection.Queue(agent.InboxNextStep)...) {
			if item.InputID == accepted.Item.InputID {
				queued = true
			}
		}
		if !queued {
			job.State, job.Outcome = Cancelled, "cancelled-before-turn"
		}
	}
	for _, event := range store.events {
		if job.TurnID == "" || event.TurnID != job.TurnID || event.EventType != "turn/end" {
			continue
		}
		var end agent.TurnEndPayload
		if json.Unmarshal(event.Data, &end) != nil {
			return Job{}, ErrExecution
		}
		job.EndSeq = event.Seq
		switch {
		case end.Synthetic:
			job.State, job.Outcome = Failed, "interrupted"
		case end.Reason == "completed":
			job.State, job.Outcome = Succeeded, "completed"
		case end.Reason == "aborted":
			job.State, job.Outcome = Cancelled, "aborted"
		default:
			job.State, job.Outcome = Failed, "execution-failed"
		}
	}
	if job.CancelRequested && !terminal(job.State) {
		job.State = Cancelling
	}
	return job, nil
}
