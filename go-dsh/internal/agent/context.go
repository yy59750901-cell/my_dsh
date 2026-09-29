package agent

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/session"
)

const (
	DefaultContextTokenBudget                                = 128000
	DefaultContextOutputReservedTokens                       = 4096
	ContextTokenEstimator                                    = "utf8-json-bytes-v1"
	EventContextCompacted                                    = "context/compacted"
	contextProjectionKey               session.ProjectionKey = "agent-context"
)

var (
	ErrContextWindowExceeded = errors.New("CONTEXT_WINDOW_EXCEEDED")
	ErrContextBusy           = errors.New("context compaction requires idle session without queued inputs")
	ErrContextChanged        = errors.New("context changed; provide a new summary of the current history")
	ErrContextEmpty          = errors.New("no context history to compact")
	ErrInvalidContextSummary = errors.New("context summary must be non-empty UTF-8 text")
)

// ContextBudget 是本地保守估算，不是供应商 tokenizer、实际 usage 或窗口保证。
// 每条消息按完整 JSON 的 UTF-8 字节数 +16、每个工具定义 +32 估算，
// 整个请求另加 64；输出预算单独预留。所有数值均为估算，不写入 usage。
type ContextBudget struct {
	Estimator              string `json:"estimator"`
	Exact                  bool   `json:"exact"`
	LimitTokens            int    `json:"limit_tokens"`
	ReservedOutputTokens   int    `json:"reserved_output_tokens"`
	EstimatedSystemTokens  int    `json:"estimated_system_tokens"`
	EstimatedHistoryTokens int    `json:"estimated_history_tokens"`
	EstimatedToolTokens    int    `json:"estimated_tool_tokens"`
	RequestOverheadTokens  int    `json:"request_overhead_tokens"`
	EstimatedInputTokens   int    `json:"estimated_input_tokens"`
	RemainingTokens        int    `json:"remaining_tokens"`
	Exceeded               bool   `json:"exceeded"`
}

type ContextWindowExceededError struct{ Budget ContextBudget }

func (e *ContextWindowExceededError) Error() string {
	return fmt.Sprintf("%s: estimated input %d + reserved output %d > budget %d (%s; not an exact tokenizer)", ErrContextWindowExceeded, e.Budget.EstimatedInputTokens, e.Budget.ReservedOutputTokens, e.Budget.LimitTokens, e.Budget.Estimator)
}
func (e *ContextWindowExceededError) Unwrap() error { return ErrContextWindowExceeded }

type ContextView struct {
	SessionID         string               `json:"session_id"`
	HeadSeq           uint64               `json:"head_seq"`
	ReplaceGeneration uint64               `json:"replace_generation"`
	SourceEventSeqs   []uint64             `json:"source_event_seqs"`
	Messages          []llm.Message        `json:"messages"`
	Tools             []llm.ToolDefinition `json:"tools"`
	Budget            ContextBudget        `json:"budget"`
}

func normalizeContextOptions(o *HarnessOptions) error {
	if o.ContextTokenBudget < 0 || o.ContextOutputReservedTokens < 0 {
		return ErrInvalidHarnessOptions
	}
	if o.ContextTokenBudget == 0 {
		o.ContextTokenBudget = DefaultContextTokenBudget
	}
	if o.ContextOutputReservedTokens == 0 {
		o.ContextOutputReservedTokens = DefaultContextOutputReservedTokens
	}
	if o.ContextOutputReservedTokens >= o.ContextTokenBudget {
		return ErrInvalidHarnessOptions
	}
	return nil
}

// ContextPreview 固定一次 durable head 后只读 Replay，不加载 Actor、不抢 writer、
// 不做 repair。并发追加不进入本次预览；CompactAt 必须携带返回的 HeadSeq。
func (h *Harness) ContextPreview(ctx context.Context, id string) (ContextView, error) {
	if err := h.available(); err != nil {
		return ContextView{}, err
	}
	if err := ctx.Err(); err != nil {
		return ContextView{}, err
	}
	head, err := h.store.Head(ctx, id)
	if err != nil {
		return ContextView{}, err
	}
	schema, err := NewSessionSchema()
	if err != nil {
		return ContextView{}, err
	}
	s, err := session.Replay(ctx, contextPreviewStore{EventStore: h.store, head: head}, id, 0, head, session.ReplayOptions{Schema: schema})
	if err != nil {
		return ContextView{}, err
	}
	return h.ContextPreviewFromSnapshot(s)
}

// ContextPreviewFromSnapshot 复用当前 Harness 的预算和工具配置，只派生给定快照。
// Inbox 不进入模型上下文；超限仍返回完整预览和 Budget.Exceeded，不截断、不写入。
func (h *Harness) ContextPreviewFromSnapshot(s session.Snapshot) (ContextView, error) {
	if err := h.available(); err != nil {
		return ContextView{}, err
	}
	defs := []llm.ToolDefinition{}
	for _, def := range h.tools.Definitions() {
		defs = append(defs, llm.ToolDefinition{Name: def.Name, Description: def.Description, InputSchema: def.InputSchema})
	}
	return deriveContext(s, defs, h.options)
}

// Replay 的读取上界必须固定，否则最后一页可能读入刚追加的后续事件。
type contextPreviewStore struct {
	session.EventStore
	head uint64
}

func (s contextPreviewStore) Load(ctx context.Context, id string, after uint64, limit int) ([]session.Event, error) {
	if after >= s.head {
		return nil, nil
	}
	if remaining := s.head - after; uint64(limit) > remaining {
		limit = int(remaining)
	}
	return s.EventStore.Load(ctx, id, after, limit)
}

func deriveContext(s session.Snapshot, defs []llm.ToolDefinition, options HarnessOptions) (ContextView, error) {
	if err := normalizeContextOptions(&options); err != nil {
		return ContextView{}, err
	}
	p, ok := ProjectionFrom(s)
	if !ok {
		return ContextView{}, ErrInvalidDriverTransition
	}
	surface := s.Surface()
	v := ContextView{SessionID: s.SessionID, HeadSeq: s.HeadSeq, ReplaceGeneration: surface.ReplaceGeneration(), SourceEventSeqs: surface.Nodes(), Messages: []llm.Message{}, Tools: defs}
	v.Budget = ContextBudget{Estimator: ContextTokenEstimator, LimitTokens: options.ContextTokenBudget, ReservedOutputTokens: options.ContextOutputReservedTokens, RequestOverheadTokens: 64}
	if p.Config.SystemPrompt != "" {
		v.Messages = append(v.Messages, llm.Message{Role: llm.RoleSystem, Source: llm.MessageSourcePrompt, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: p.Config.SystemPrompt, Complete: true}}})
		data, err := json.Marshal(v.Messages[0])
		if err != nil {
			return ContextView{}, err
		}
		v.Budget.EstimatedSystemTokens = len(data) + 16
	}
	raw, err := surface.DeriveMessages()
	if err != nil {
		return ContextView{}, err
	}
	for _, data := range raw {
		var m llm.Message
		if err := json.Unmarshal(data, &m); err != nil {
			return ContextView{}, err
		}
		encoded, err := json.Marshal(m)
		if err != nil {
			return ContextView{}, err
		}
		v.Budget.EstimatedHistoryTokens += len(encoded) + 16
		v.Messages = append(v.Messages, m)
	}
	for _, def := range defs {
		data, err := json.Marshal(def)
		if err != nil {
			return ContextView{}, err
		}
		v.Budget.EstimatedToolTokens += len(data) + 32
	}
	b := &v.Budget
	b.EstimatedInputTokens = b.EstimatedSystemTokens + b.EstimatedHistoryTokens + b.EstimatedToolTokens + b.RequestOverheadTokens
	b.RemainingTokens = b.LimitTokens - b.ReservedOutputTokens - b.EstimatedInputTokens
	b.Exceeded = b.RemainingTokens < 0
	return v, nil
}

// Compact 要求调用方提供恰好一个预览 head；保留可变参数仅用于旧调用编译兼容，
// 无版本调用明确拒绝，不允许服务端补读当前版本。
func (h *Harness) Compact(ctx context.Context, sessionID, summary string, expectedHead ...uint64) error {
	if len(expectedHead) != 1 {
		return ErrContextChanged
	}
	return h.CompactAt(ctx, sessionID, summary, expectedHead[0])
}

// CompactAt 仅接受人工摘要和调用方预览时的 head，不调用模型、不删除审计。
// 重试必须保留原 source head 与 summary，不能换用压缩后的 head。
func (h *Harness) CompactAt(ctx context.Context, sessionID, summary string, expectedHead uint64) error {
	if err := h.available(); err != nil {
		return err
	}
	if strings.TrimSpace(summary) == "" || !utf8.ValidString(summary) {
		return ErrInvalidContextSummary
	}
	lease, err := h.sessions.GetOrLoad(ctx, sessionID)
	if err != nil {
		return err
	}
	defer lease.Release()
	a := lease.Actor()
	_, err = submitCommitted(ctx, a, compactContextCommand{ExpectedHead: expectedHead, Summary: summary})
	return err
}

type ContextCompactedPayload struct {
	Summary         string   `json:"summary"`
	SourceHeadSeq   uint64   `json:"source_head_seq"`
	SourceEventSeqs []uint64 `json:"source_event_seqs"`
	Manual          bool     `json:"manual"`
}

type compactContextCommand struct {
	ExpectedHead uint64
	Summary      string
}

func (c compactContextCommand) Decide(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	if strings.TrimSpace(c.Summary) == "" || !utf8.ValidString(c.Summary) {
		return nil, ErrInvalidContextSummary
	}
	p, ok := ProjectionFrom(s)
	if !ok {
		return nil, ErrInvalidDriverTransition
	}
	core := s.Core()
	if core.Status != session.StatusIdle || core.TurnID != "" || core.StepID != "" || p.ActiveTurnID != "" || p.ActiveStepID != "" || p.PendingClaimID != "" || p.ActiveStepClaimID != "" || p.CancelCause != nil || len(p.NextTurn)+len(p.NextStep) != 0 {
		return nil, ErrContextBusy
	}
	for _, call := range p.ToolCalls {
		if !call.Done {
			return nil, ErrContextBusy
		}
	}
	state, ok := session.ProjectionAs[*contextProjection](s, contextProjectionKey)
	if !ok {
		return nil, ErrInvalidDriverTransition
	}
	nodes := s.Surface().Nodes()
	if receipt, exists := state.Summaries[sha256.Sum256([]byte(c.Summary))]; exists {
		if c.ExpectedHead == receipt.SourceHeadSeq && len(nodes) == 1 && nodes[0] == receipt.ReplacementSeq {
			return nil, nil
		}
		return nil, ErrContextChanged
	}
	// 不只绑定 Surface：Prompt 随后被 Cancel 清队列，也必须使旧提案失效。
	if s.HeadSeq != c.ExpectedHead {
		return nil, ErrContextChanged
	}
	if len(nodes) == 0 {
		return nil, ErrContextEmpty
	}
	payload := ContextCompactedPayload{Summary: c.Summary, SourceHeadSeq: s.HeadSeq, SourceEventSeqs: nodes, Manual: true}
	marker := fact(EventContextCompacted, "", "", payload)
	sources := append(slices.Clone(nodes), s.HeadSeq+1)
	replacement := surfaceFact(session.SurfaceEventUserMessage, "", "", compactMessage(c.Summary), sources)
	replacement.SurfaceOp = session.ReplaceSurfaceOp(nodes[0], nodes[len(nodes)-1])
	return []session.NewEvent{marker, replacement}, nil
}

func compactMessage(summary string) llm.Message {
	return llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceSurface, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: "以下为用户手工提供的历史摘要（非模型自动总结）：\n" + summary, Complete: true}}}
}

type contextCompactionReceipt struct {
	SourceHeadSeq  uint64
	ReplacementSeq uint64
}

// 独立投影维护完整 Surface 序号镜像，在 marker 前校验全量有序来源。
// candidate、committed、Replay 共用此校验，不改变 Session 或取消/工具状态机。
type contextProjection struct {
	Summaries    map[[32]byte]contextCompactionReceipt
	Pending      *ContextCompactedPayload
	SurfaceNodes []uint64
}

func contextSchemaContribution() session.SchemaContribution {
	return session.SchemaContribution{
		Name:   "agent-context",
		Events: []session.EventDefinition{{EventType: EventContextCompacted, ReplayPolicy: session.ReplayRequired}},
		Projections: []session.ProjectionSpec{{
			Key: contextProjectionKey, Order: 400,
			New: func() any { return &contextProjection{Summaries: map[[32]byte]contextCompactionReceipt{}} },
			Clone: func(state any) any {
				p := state.(*contextProjection)
				c := &contextProjection{Summaries: make(map[[32]byte]contextCompactionReceipt, len(p.Summaries)), SurfaceNodes: slices.Clone(p.SurfaceNodes)}
				for hash, seq := range p.Summaries {
					c.Summaries[hash] = seq
				}
				if p.Pending != nil {
					v := *p.Pending
					v.SourceEventSeqs = slices.Clone(v.SourceEventSeqs)
					c.Pending = &v
				}
				return c
			},
			Apply: func(state any, e session.Event) error { return state.(*contextProjection).apply(e) },
			ValidateBoundary: func(state any, boundary session.ProjectionBoundary) error {
				if state.(*contextProjection).Pending != nil && boundary != session.BoundaryReplayEnd {
					return fmt.Errorf("%w: compaction fact requires its surface replacement in the same batch", session.ErrInvalidSurface)
				}
				return nil
			},
		}},
	}
}

func (p *contextProjection) apply(e session.Event) error {
	if p.Pending == nil && e.EventType != EventContextCompacted {
		return p.applySurfaceNodes(e)
	}
	invalid := fmt.Errorf("%w: invalid manual context compaction at seq %d", session.ErrInvalidSurface, e.Seq)
	if p.Pending != nil {
		v := p.Pending
		nodes := v.SourceEventSeqs
		sources := append(slices.Clone(nodes), v.SourceHeadSeq+1)
		var m llm.Message
		if e.Seq != v.SourceHeadSeq+2 || e.EventType != session.SurfaceEventUserMessage || e.TurnID != "" || e.StepID != "" || e.SurfaceOp == nil || *e.SurfaceOp != *session.ReplaceSurfaceOp(nodes[0], nodes[len(nodes)-1]) || !slices.Equal(e.SourceEventSeqs, sources) || json.Unmarshal(e.Data, &m) != nil || !reflect.DeepEqual(m, compactMessage(v.Summary)) {
			return invalid
		}
		p.Summaries[sha256.Sum256([]byte(v.Summary))] = contextCompactionReceipt{SourceHeadSeq: v.SourceHeadSeq, ReplacementSeq: e.Seq}
		p.Pending = nil
		return p.applySurfaceNodes(e)
	}
	var v ContextCompactedPayload
	if json.Unmarshal(e.Data, &v) != nil || !v.Manual || strings.TrimSpace(v.Summary) == "" || e.TurnID != "" || e.StepID != "" || v.SourceHeadSeq != e.Seq-1 || len(v.SourceEventSeqs) == 0 {
		return invalid
	}
	if _, exists := p.Summaries[sha256.Sum256([]byte(v.Summary))]; exists {
		return invalid
	}
	if !slices.Equal(v.SourceEventSeqs, p.SurfaceNodes) {
		return invalid
	}
	p.Pending = &v
	return nil
}

func (p *contextProjection) applySurfaceNodes(e session.Event) error {
	if !session.IsSurfaceEligibleType(e.EventType) {
		return nil
	}
	// 顺序更早的 Session Surface 投影已经校验事件；legacy 无 op 等价于 append。
	if e.SurfaceOp == nil || e.SurfaceOp.Kind == session.SurfaceOpAppend {
		p.SurfaceNodes = append(p.SurfaceNodes, e.Seq)
		return nil
	}
	start := slices.Index(p.SurfaceNodes, e.SurfaceOp.ReplaceStart)
	end := slices.Index(p.SurfaceNodes, e.SurfaceOp.ReplaceEnd)
	if e.SurfaceOp.Kind != session.SurfaceOpReplace || start < 0 || end < start {
		return session.ErrInvalidSurface
	}
	nodes := append(slices.Clone(p.SurfaceNodes[:start]), e.Seq)
	p.SurfaceNodes = append(nodes, p.SurfaceNodes[end+1:]...)
	return nil
}

type contextOverflowCommand struct {
	Turn, Step    string
	StepIndex     uint32
	SourceHeadSeq uint64
	Budget        ContextBudget
}

func (c contextOverflowCommand) Decide(ctx context.Context, s session.Snapshot) ([]session.NewEvent, error) {
	p, ok := ProjectionFrom(s)
	if !ok || p.CancelCause != nil || p.ActiveTurnID != c.Turn || p.ActiveStepID != c.Step || p.ActiveStepIndex != c.StepIndex || p.ActiveAttempt != 0 || p.AttemptPhase != AttemptPhaseNone {
		return nil, ErrStaleDriverActivity
	}
	if !c.Budget.Exceeded || c.Turn == "" || c.Step == "" || c.SourceHeadSeq > s.HeadSeq {
		return nil, ErrInvalidDriverTransition
	}
	closure, err := (closeActivityCommand{Turn: c.Turn, Reason: "error"}).Decide(ctx, s)
	if err != nil {
		return nil, err
	}
	// Attempt=0 明确表示本地准入失败：没有发出模型请求，也不伪造 usage。
	payload := struct {
		ModelErrorPayload
		Budget        ContextBudget `json:"context_budget"`
		SourceHeadSeq uint64        `json:"source_head_seq"`
	}{ModelErrorPayload: ModelErrorPayload{Failure: llm.LlmFailure{Code: llm.FailureContextWindowExceeded, Message: (&ContextWindowExceededError{Budget: c.Budget}).Error()}}, Budget: c.Budget, SourceHeadSeq: c.SourceHeadSeq}
	return append([]session.NewEvent{fact("model/error", c.Turn, c.Step, payload)}, closure...), nil
}
