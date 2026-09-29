package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

var (
	ErrHarnessClosed             = errors.New("agent harness closed")
	ErrInvalidHarnessOptions     = errors.New("invalid harness options")
	ErrSessionCatalogUnavailable = errors.New("durable session catalog required")
	ErrApprovalNotFound          = errors.New("approval not found")
	ErrApprovalResolved          = errors.New("approval already resolved")
	ErrApprovalNotPending        = errors.New("approval no longer pending")
)

type HarnessOptions struct {
	Model        string
	SystemPrompt string
	MaxSteps     int
	MaxAttempts  int
	// ContextTokenBudget 是输入估算与输出预留的总预算，不是精确 tokenizer 窗口。
	ContextTokenBudget          int
	ContextOutputReservedTokens int
}
type SessionInfo struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type Harness struct {
	store    session.EventStore
	catalog  SessionCatalog
	sessions *session.Registry
	runtime  *Registry
	inbox    *InboxWriter
	provider llm.Provider
	tools    *tool.Registry
	options  HarnessOptions
	mu       sync.RWMutex
	closed   bool
}

func NewHarness(store session.EventStore, provider llm.Provider, tools *tool.Registry, options HarnessOptions) (*Harness, error) {
	if store == nil || provider == nil || strings.TrimSpace(options.Model) == "" || options.MaxSteps < 0 || options.MaxAttempts < 0 {
		return nil, ErrInvalidHarnessOptions
	}
	if err := normalizeContextOptions(&options); err != nil {
		return nil, err
	}
	catalog, ok := store.(SessionCatalog)
	if !ok {
		return nil, ErrSessionCatalogUnavailable
	}
	if tools == nil {
		tools = tool.NewRegistry()
	}
	if options.MaxSteps == 0 {
		options.MaxSteps = 32
	}
	// MaxAttempts 表示总请求次数；默认首次请求加五次重试。
	if options.MaxAttempts == 0 {
		options.MaxAttempts = 6
	}
	sessions, err := NewSessionActorRegistry(store, SessionActorOptions{})
	if err != nil {
		return nil, err
	}
	h := &Harness{store: store, catalog: catalog, sessions: sessions, provider: provider, tools: tools, options: options}
	h.runtime = NewRuntimeRegistry(DriverFactoryFunc(func(_ context.Context, id string) (Driver, error) { return &sessionDriver{h: h, id: id}, nil }), RuntimeRegistryOptions{})
	h.inbox, err = NewInboxWriter(sessions, h.runtime, InboxWriterOptions{})
	if err != nil {
		return nil, err
	}
	schema, err := NewSessionSchema()
	if err != nil {
		return nil, err
	}
	inspector := WakeInspectorFunc(func(ctx context.Context, id string) (WakeRequirement, error) {
		head, err := store.Head(ctx, id)
		if err != nil {
			return WakeRequirement{}, err
		}
		snap, err := session.Replay(ctx, store, id, 0, head, session.ReplayOptions{Schema: schema})
		if err != nil {
			return WakeRequirement{}, err
		}
		p, _ := ProjectionFrom(snap)
		result := WakeRequirement{NextTurnItems: len(p.NextTurn), NextStepItems: len(p.NextStep)}
		for _, item := range p.NextStep {
			if item.Source == InputSourceUser {
				result.UserSteeringItems++
			}
		}
		// 不完整活动也需要加载并修复，但绝不续跑原请求或副作用。
		if p.ActiveTurnID != "" {
			result.NextTurnItems++
		}
		return result, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = NewWakeReconciler(catalog, inspector, h.runtime, WakeReconcilerOptions{}).Run(ctx); err != nil {
		_ = h.Close(context.Background())
		return nil, err
	}
	return h, nil
}

func (h *Harness) available() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return ErrHarnessClosed
	}
	return nil
}
func (h *Harness) CreateSession(ctx context.Context, title string) (string, error) {
	if err := h.available(); err != nil {
		return "", err
	}
	id, err := (cryptoInboxIDGenerator{}).NewID("session")
	if err != nil {
		return "", err
	}
	if err = h.store.Create(ctx, session.NewSession{ID: id, TenantID: "local", WorkspaceID: "default"}); err != nil {
		return "", err
	}
	lease, err := h.sessions.GetOrLoad(ctx, id)
	if err != nil {
		return "", err
	}
	defer lease.Release()
	_, err = submitCommitted(ctx, lease.Actor(), session.CommandFunc(func(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{fact("session/created", "", "", map[string]any{"title": title}), fact(EventHarnessConfigured, "", "", harnessConfig{Title: title, Model: h.options.Model, SystemPrompt: h.options.SystemPrompt})}, nil
	}))
	return id, err
}
func (h *Harness) input(ctx context.Context, id, text, key string, kind CommandKind) (Receipt, error) {
	if err := h.available(); err != nil {
		return Receipt{}, err
	}
	if strings.TrimSpace(text) == "" {
		return Receipt{}, ErrInvalidInboxRequest
	}
	reqID, err := (cryptoInboxIDGenerator{}).NewID("request")
	if err != nil {
		return Receipt{}, err
	}
	req := InboxRequest{SessionID: id, RequestID: reqID, IdempotencyKey: key, Content: []ContentBlock{{Type: "text", Text: text}}}
	switch kind {
	case CommandFollowUp:
		return h.inbox.FollowUp(ctx, req)
	case CommandSteer:
		return h.inbox.Steer(ctx, req)
	default:
		req.Source = InputSourceSystem
		return h.inbox.Inject(ctx, req)
	}
}
func (h *Harness) Prompt(ctx context.Context, id, text, key string) (Receipt, error) {
	return h.input(ctx, id, text, key, CommandFollowUp)
}
func (h *Harness) Steer(ctx context.Context, id, text, key string) (Receipt, error) {
	return h.input(ctx, id, text, key, CommandSteer)
}
func (h *Harness) Inject(ctx context.Context, id, text string) error {
	_, err := h.input(ctx, id, text, "", CommandInject)
	return err
}
func (h *Harness) Snapshot(ctx context.Context, id string) (session.Snapshot, error) {
	if err := h.available(); err != nil {
		return session.Snapshot{}, err
	}
	lease, err := h.sessions.GetOrLoad(ctx, id)
	if err != nil {
		return session.Snapshot{}, err
	}
	defer lease.Release()
	return lease.Actor().Snapshot(), nil
}
func (h *Harness) ListEvents(ctx context.Context, id string, after uint64, limit int) ([]session.Event, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	return h.store.Load(ctx, id, after, limit)
}
func (h *Harness) ListSessions(ctx context.Context) ([]SessionInfo, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	result := []SessionInfo{}
	cursor := ""
	for {
		page, err := h.catalog.ListResumableSessionIDs(ctx, cursor, 100)
		if err != nil {
			return nil, err
		}
		for _, id := range page.SessionIDs {
			title := id
			events, err := h.store.Load(ctx, id, 0, 10)
			if err != nil {
				return nil, err
			}
			for _, e := range events {
				if e.EventType == "session/created" || e.EventType == EventHarnessConfigured {
					var data struct {
						Title string `json:"title"`
					}
					if err = json.Unmarshal(e.Data, &data); err != nil {
						return nil, err
					}
					if data.Title != "" {
						title = data.Title
					}
				}
			}
			result = append(result, SessionInfo{ID: id, Title: title})
		}
		if page.NextCursor == "" {
			return result, nil
		}
		if page.NextCursor == cursor {
			return nil, fmt.Errorf("session catalog cursor did not advance")
		}
		cursor = page.NextCursor
	}
}
func (h *Harness) Cancel(ctx context.Context, id, reason string) error {
	if err := h.available(); err != nil {
		return err
	}
	lease, err := h.sessions.GetOrLoad(ctx, id)
	if err != nil {
		return err
	}
	defer lease.Release()
	a := lease.Actor()
	p, _ := ProjectionFrom(a.Snapshot())
	target := cancelTarget{actor: a, turn: p.ActiveTurnID}
	h.runtime.mu.Lock()
	if entry := h.runtime.entries[id]; entry != nil {
		target.driver, _ = entry.driver.(*sessionDriver)
	}
	h.runtime.mu.Unlock()
	return target.submit(ctx, a, reason)
}

// 固定本次取消的 Actor/Turn/Driver；迟到的提交响应不能重新定位当前活动。
type cancelTarget struct {
	actor  *session.Actor
	turn   string
	driver *sessionDriver
}

func (target cancelTarget) submit(ctx context.Context, submitter attemptEventSubmitter, reason string) error {
	var decided atomic.Bool
	committed, err := submitter.Submit(ctx, session.CommandFunc(func(ctx context.Context, s session.Snapshot) ([]session.NewEvent, error) {
		es, err := (cancelCommand{Cause: CancelCause{Source: CancelSourceUser, Reason: reason}, ExpectedTurn: target.turn, BindTurn: true}).Decide(ctx, s)
		if err == nil {
			p, _ := ProjectionFrom(s)
			decided.Store(len(es) > 0 || p.CancelCause != nil)
		}
		return es, err
	}))
	if decided.Load() && target.driver != nil {
		// 即使持久化失败，也仅停止原活动的执行 context，不再提交系统取消。
		target.driver.interruptActivity(target.actor, target.turn)
	}
	if len(committed) > 0 {
		return nil
	}
	return err
}

func (h *Harness) ResolveApproval(ctx context.Context, id, approvalID string, allow bool) error {
	if err := h.available(); err != nil {
		return err
	}
	lease, err := h.sessions.GetOrLoad(ctx, id)
	if err != nil {
		return err
	}
	defer lease.Release()
	_, err = submitCommitted(ctx, lease.Actor(), resolveApprovalCommand{ID: approvalID, Allow: allow})
	if err == nil {
		err = h.runtime.RequestWake(id)
	}
	return err
}
func (h *Harness) GetStatus(ctx context.Context, id string) (Status, error) {
	s, err := h.Snapshot(ctx, id)
	if err != nil {
		return Status{}, err
	}
	return statusFrom(s), nil
}
func (h *Harness) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	h.mu.Unlock()
	err := h.runtime.Dispose(ctx)
	if ctx.Err() != nil {
		return errors.Join(err, ctx.Err())
	}
	return errors.Join(err, h.sessions.Dispose(ctx))
}

func submitCommitted(ctx context.Context, actor *session.Actor, command session.Command) ([]session.Event, error) {
	events, err := actor.Submit(ctx, command)
	// Actor 已用数据库回读确认提交；不将响应丢失误当成尚未执行。
	if len(events) > 0 {
		return events, nil
	}
	return events, err
}

func fact(kind, turn, step string, payload any) session.NewEvent {
	data, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	id, err := (cryptoInboxIDGenerator{}).NewID("event")
	if err != nil {
		panic(err)
	}
	return session.NewEvent{SchemaVersion: session.SchemaVersion{Major: 1}, EventType: kind, EventID: id, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data, TurnID: turn, StepID: step}
}
func surfaceFact(kind, turn, step string, payload any, sources []uint64) session.NewEvent {
	e := fact(kind, turn, step, payload)
	e.SchemaVersion.Minor = session.SurfaceSchemaMinor
	e.SurfaceOp = session.AppendSurfaceOp()
	e.SourceEventSeqs = sources
	return e
}
