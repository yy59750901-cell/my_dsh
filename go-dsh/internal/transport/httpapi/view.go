package httpapi

import (
	"context"
	"errors"
	"sort"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/session"
)

// FindSession 只查目录，不加载 Actor，不领取 writer epoch。
func FindSession(ctx context.Context, h *agent.Harness, id string) (agent.SessionInfo, error) {
	if h == nil {
		return agent.SessionInfo{}, agent.ErrHarnessClosed
	}
	if id == "" {
		return agent.SessionInfo{}, session.ErrInvalidSession
	}
	list, err := h.ListSessions(ctx)
	if err != nil {
		return agent.SessionInfo{}, err
	}
	for _, info := range list {
		if info.ID == id {
			return info, nil
		}
	}
	return agent.SessionInfo{}, session.ErrNotFound
}

// ReadSnapshot 通过只读 facade 重放已提交事实，避免 Snapshot 的加载修复副作用。
func ReadSnapshot(ctx context.Context, h *agent.Harness, id string) (session.Snapshot, []session.Event, error) {
	if _, err := FindSession(ctx, h, id); err != nil {
		return session.Snapshot{}, nil, err
	}
	events := []session.Event{}
	var after uint64
	for {
		page, err := h.ListEvents(ctx, id, after, 1000)
		if err != nil {
			return session.Snapshot{}, nil, err
		}
		if err = session.ValidateSequence(page, after); err != nil {
			return session.Snapshot{}, nil, err
		}
		events = append(events, page...)
		if len(page) > 0 {
			after = page[len(page)-1].Seq
		}
		if len(page) < 1000 {
			break
		}
	}
	schema, err := agent.NewSessionSchema()
	if err != nil {
		return session.Snapshot{}, nil, err
	}
	s, err := session.Replay(ctx, readonlyEvents{events}, id, 0, after, session.ReplayOptions{Schema: schema})
	return s, events, err
}

type readonlyEvents struct{ events []session.Event }

var errReadOnly = errors.New("只读视图不允许写入")

func (readonlyEvents) Create(context.Context, session.NewSession) error { return errReadOnly }
func (readonlyEvents) ClaimWriter(context.Context, string) (uint64, uint64, error) {
	return 0, 0, errReadOnly
}
func (readonlyEvents) Append(context.Context, string, uint64, uint64, []session.NewEvent) ([]session.Event, error) {
	return nil, errReadOnly
}
func (r readonlyEvents) Head(context.Context, string) (uint64, error) {
	if len(r.events) == 0 {
		return 0, nil
	}
	return r.events[len(r.events)-1].Seq, nil
}
func (r readonlyEvents) Load(ctx context.Context, id string, after uint64, limit int) ([]session.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	start := sort.Search(len(r.events), func(i int) bool { return r.events[i].Seq > after })
	end := min(start+limit, len(r.events))
	return r.events[start:end], nil
}

type StatusView struct {
	SessionID        string      `json:"sessionId"`
	State            agent.State `json:"state"`
	ActiveTurnID     string      `json:"activeTurnId,omitempty"`
	ActiveStepID     string      `json:"activeStepId,omitempty"`
	ActiveAttempt    uint32      `json:"activeAttempt"`
	NextTurnMessages int         `json:"nextTurnMessages"`
	NextStepMessages int         `json:"nextStepMessages"`
	LastEventSeq     uint64      `json:"lastEventSeq,string"`
	CanCompact       bool        `json:"canCompact"`
}

func SnapshotStatus(s session.Snapshot) StatusView {
	p, _ := agent.ProjectionFrom(s)
	v := StatusView{SessionID: s.SessionID, State: agent.StateIdle, ActiveTurnID: p.ActiveTurnID, ActiveStepID: p.ActiveStepID, ActiveAttempt: p.ActiveAttempt, NextTurnMessages: len(p.NextTurn), NextStepMessages: len(p.NextStep), LastEventSeq: s.HeadSeq}
	if p.ActiveTurnID != "" {
		v.State = agent.StateRunning
	}
	for _, c := range p.ToolCalls {
		if c.Call.TurnID == p.ActiveTurnID && !c.Done && c.RequiresApproval && c.Decision == nil {
			v.State = agent.StateWaitingApproval
		}
	}
	if p.CancelCause != nil {
		v.State = agent.StateCancelling
	}
	// 仅用于 UI 提示，提交时仍由 Harness 原子检查是否允许压缩。
	core := s.Core()
	v.CanCompact = core.Status == session.StatusIdle && core.TurnID == "" && core.StepID == "" && p.ActiveTurnID == "" && p.ActiveStepID == "" && p.PendingClaimID == "" && p.ActiveStepClaimID == "" && p.CancelCause == nil && len(p.NextTurn)+len(p.NextStep) == 0 && len(s.Surface().Nodes()) > 0
	for _, c := range p.ToolCalls {
		if !c.Done {
			v.CanCompact = false
		}
	}
	return v
}
