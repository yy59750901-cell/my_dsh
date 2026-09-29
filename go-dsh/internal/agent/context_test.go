package agent_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

func contextIdle(t *testing.T, h *agent.Harness, id string, head uint64) session.Snapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s, err := h.Snapshot(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		p, _ := agent.ProjectionFrom(s)
		if s.HeadSeq >= head && s.Core().Status == session.StatusIdle && p.ActiveTurnID == "" && len(p.NextTurn)+len(p.NextStep) == 0 {
			return s
		}
		select {
		case <-ctx.Done():
			t.Fatal("等待已提交 idle 快照超时")
		case <-tick.C:
		}
	}
}

func contextPreviewHead(t *testing.T, h *agent.Harness, id string) uint64 {
	t.Helper()
	view, err := h.ContextPreview(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return view.HeadSeq
}

func contextReplay(t *testing.T, store session.EventStore, id string, live session.Snapshot) {
	t.Helper()
	schema, err := agent.NewSessionSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []int{1, 2, 1000} {
		r, err := session.Replay(context.Background(), store, id, 0, live.HeadSeq, session.ReplayOptions{Schema: schema, PageSize: page})
		if err != nil {
			t.Fatal(err)
		}
		want, err := live.DeriveMessages()
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.DeriveMessages()
		if err != nil || !reflect.DeepEqual(want, got) || !reflect.DeepEqual(live.Surface(), r.Surface()) || live.Core() != r.Core() {
			t.Fatalf("Replay 不一致: page=%d err=%v", page, err)
		}
		lp, _ := agent.ProjectionFrom(live)
		rp, _ := agent.ProjectionFrom(r)
		if !reflect.DeepEqual(lp, rp) {
			t.Fatalf("Agent Replay 不一致: page=%d", page)
		}
	}
}

func TestCompactDurableReplayNoAuditDeletion(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint("uncertain=", uncertain), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "context.sqlite")
			base := openStore(t, path)
			var store session.EventStore = base
			if uncertain {
				store = &uncertainStore{EventStore: base, kind: agent.EventContextCompacted}
			}
			tools := tool.NewRegistry()
			if err := tools.Register(&structuredResultTool{}); err != nil {
				t.Fatal(err)
			}
			p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
				if n == 0 {
					return callStream("structured", "context-call"), nil
				}
				return textStream("任务已完成"), nil
			}}
			h := newHarness(t, store, p, tools)
			id := sessionID(t, h)
			prompt(t, h, id, "读取结构化结果", "first")
			before := waitTurns(t, h, id, 1)
			s := contextIdle(t, h, id, before[len(before)-1].Seq)
			if count(before, "tool/result") != 1 || len(s.Surface().Nodes()) != 4 {
				t.Fatalf("闭合工具历史不完整: %s", summarizeEvents(before))
			}
			sourceHead := contextPreviewHead(t, h, id)
			summary := "用户要求读取结构化结果；工具已返回 value=42；任务完成。"
			if err := h.Compact(ctx, id, summary, sourceHead); err != nil {
				t.Fatal(err)
			}
			after := events(t, h, id)
			if len(after) != len(before)+2 || !reflect.DeepEqual(before, after[:len(before)]) {
				t.Fatal("压缩删除或改写了审计历史，或未同批追加两个事实")
			}
			marker, replacement := after[len(before)], after[len(before)+1]
			var payload agent.ContextCompactedPayload
			if err := json.Unmarshal(marker.Data, &payload); err != nil {
				t.Fatal(err)
			}
			if marker.EventType != agent.EventContextCompacted || marker.ReplayPolicy != session.ReplayRequired || marker.SurfaceOp != nil || !payload.Manual || payload.Summary != summary || payload.SourceHeadSeq != s.HeadSeq || !slices.Equal(payload.SourceEventSeqs, s.Surface().Nodes()) {
				t.Fatalf("人工摘要事实不完整: %+v", marker)
			}
			nodes := s.Surface().Nodes()
			if replacement.SurfaceOp == nil || *replacement.SurfaceOp != *session.ReplaceSurfaceOp(nodes[0], nodes[len(nodes)-1]) || !slices.Equal(replacement.SourceEventSeqs, append(nodes, marker.Seq)) {
				t.Fatalf("替换未绑定所有来源: %+v", replacement)
			}
			view, err := h.ContextPreview(ctx, id)
			if err != nil || len(view.Messages) != 2 || view.Messages[0].Content[0].Text != "durable system prompt" || view.Messages[1].Role != llm.RoleUser || !strings.Contains(view.Messages[1].Content[0].Text, summary) || !strings.Contains(view.Messages[1].Content[0].Text, "非模型自动总结") || view.ReplaceGeneration != 1 {
				t.Fatalf("压缩上下文=%+v err=%v", view, err)
			}
			for _, m := range view.Messages {
				for _, b := range m.Content {
					if b.Type == llm.ContentBlockToolResult || b.Type == llm.ContentBlockToolCall {
						t.Fatal("压缩后残留工具调用或孤立工具结果")
					}
				}
			}
			live, _ := h.Snapshot(ctx, id)
			contextReplay(t, store, id, live)
			if err := h.Compact(ctx, id, summary, sourceHead); err != nil || len(events(t, h, id)) != len(after) || len(p.got()) != 2 {
				t.Fatalf("重复压缩非幂等或调用了模型: %v", err)
			}
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			// 两次真正重新打开 SQLite/Harness，证明幂等索引不是内存标记。
			for range 2 {
				restarted := newHarness(t, openStore(t, path), p, tools)
				got, err := restarted.ContextPreview(ctx, id)
				if err != nil || !reflect.DeepEqual(got, view) {
					t.Fatalf("重启预览改变: %v", err)
				}
				if err := restarted.CompactAt(ctx, id, summary, sourceHead); err != nil || len(events(t, restarted, id)) != len(after) {
					t.Fatalf("重启重试非幂等: %v", err)
				}
				if err := restarted.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
			h = newHarness(t, openStore(t, path), p, tools)
			prompt(t, h, id, "继续处理", "second")
			es := waitTurns(t, h, id, 2)
			contextIdle(t, h, id, es[len(es)-1].Seq)
			r := p.got()[2]
			if len(r.Messages) != 3 || !reflect.DeepEqual(r.Messages[:2], view.Messages) || r.MaxTokens == nil || *r.MaxTokens != agent.DefaultContextOutputReservedTokens {
				t.Fatalf("buildRequest 未使用压缩 Surface/输出预留: %+v", r)
			}
			if err := h.Compact(ctx, id, summary, sourceHead); !errors.Is(err, agent.ErrContextChanged) || len(events(t, h, id)) != len(es) {
				t.Fatalf("旧摘要被重新绑定新历史: %v", err)
			}
			if err := h.CompactAt(ctx, id, summary+" 用户继续处理，也已完成。", contextPreviewHead(t, h, id)); err != nil {
				t.Fatal(err)
			}
			newEvents := events(t, h, id)
			if !reflect.DeepEqual(es, newEvents[:len(es)]) {
				t.Fatal("二次压缩改写审计")
			}
			live, _ = h.Snapshot(ctx, id)
			contextReplay(t, store, id, live)
			if err := h.Compact(ctx, id, summary, sourceHead); !errors.Is(err, agent.ErrContextChanged) {
				t.Fatalf("被后续摘要覆盖的旧摘要重试未拒绝: %v", err)
			}
		})
	}
}

func TestCompactAtRejectsFirstLateSummaryAndRequiresOriginalVersion(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, filepath.Join(t.TempDir(), "version.sqlite"))
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "预览中的历史", "first")
	es := waitTurns(t, h, id, 1)
	contextIdle(t, h, id, es[len(es)-1].Seq)
	previewHead := contextPreviewHead(t, h, id)
	prompt(t, h, id, "用户预览后完成的新历史", "second")
	es = waitTurns(t, h, id, 2)
	contextIdle(t, h, id, es[len(es)-1].Seq)
	// 不是重复压缩，也不是已排队命令：这是旧摘要第一次到达服务端。
	if err := h.CompactAt(ctx, id, "只概括预览时的旧历史", previewHead); !errors.Is(err, agent.ErrContextChanged) {
		t.Fatalf("首次迟到摘要覆盖新历史: %v", err)
	}
	if err := h.Compact(ctx, id, "无版本摘要"); !errors.Is(err, agent.ErrContextChanged) {
		t.Fatalf("无版本调用未拒绝: %v", err)
	}
	if err := h.Compact(ctx, id, "多个版本", previewHead, previewHead); !errors.Is(err, agent.ErrContextChanged) {
		t.Fatalf("多个版本未拒绝: %v", err)
	}
	if !reflect.DeepEqual(es, events(t, h, id)) {
		t.Fatal("拒绝操作写入了事件")
	}
	sourceHead := contextPreviewHead(t, h, id)
	summary := "两次输入都已完成。"
	if err := h.CompactAt(ctx, id, summary, sourceHead); err != nil {
		t.Fatal(err)
	}
	compacted := events(t, h, id)
	if err := h.CompactAt(ctx, id, summary, sourceHead); err != nil {
		t.Fatalf("原版本重试失败: %v", err)
	}
	for _, wrongHead := range []uint64{previewHead, contextPreviewHead(t, h, id), sourceHead + 1} {
		if err := h.CompactAt(ctx, id, summary, wrongHead); !errors.Is(err, agent.ErrContextChanged) {
			t.Fatalf("仅凭相同摘要接受不同 source head %d: %v", wrongHead, err)
		}
	}
	if err := h.CompactAt(ctx, id, "不同摘要", sourceHead); !errors.Is(err, agent.ErrContextChanged) {
		t.Fatalf("旧版本搭配不同摘要被接受: %v", err)
	}
	if !reflect.DeepEqual(compacted, events(t, h, id)) {
		t.Fatal("幂等验证追加了事件")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := newHarness(t, store, p, nil)
	if err := restarted.CompactAt(ctx, id, summary, sourceHead); err != nil {
		t.Fatal(err)
	}
	if err := restarted.CompactAt(ctx, id, summary, contextPreviewHead(t, restarted, id)); !errors.Is(err, agent.ErrContextChanged) {
		t.Fatalf("Replay 丢失原 source head 绑定: %v", err)
	}
}

func TestCompactRejectsActiveQueuedAndCancelledRequest(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, filepath.Join(t.TempDir(), "context.sqlite"))
	gate := make(chan struct{})
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return &testStream{gate: gate}, nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	if err := h.CompactAt(ctx, id, " ", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrInvalidContextSummary) {
		t.Fatalf("空摘要: %v", err)
	}
	if err := h.CompactAt(ctx, id, "摘要", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrContextEmpty) {
		t.Fatalf("空历史: %v", err)
	}
	if err := h.Inject(ctx, id, "尚未消费的注入"); err != nil {
		t.Fatal(err)
	}
	before := events(t, h, id)
	if err := h.CompactAt(ctx, id, "摘要", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrContextBusy) || len(events(t, h, id)) != len(before) {
		t.Fatalf("idle 但有 next-step 队列未拒绝: %v", err)
	}
	if err := h.Cancel(ctx, id, "清理待处理输入"); err != nil {
		t.Fatal(err)
	}
	prompt(t, h, id, "开始", "first")
	waitEvents(t, h, id, func(es []session.Event) bool { return count(es, agent.EventModelRequested) == 1 })
	if err := h.CompactAt(ctx, id, "活动摘要", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrContextBusy) {
		t.Fatalf("活动 Turn 未拒绝: %v", err)
	}
	prompt(t, h, id, "排队输入", "queued")
	if err := h.CompactAt(ctx, id, "活动摘要", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrContextBusy) {
		t.Fatalf("活动并有 next-turn 队列未拒绝: %v", err)
	}
	if err := h.Cancel(ctx, id, "取消活动及队列"); err != nil {
		t.Fatal(err)
	}
	es := waitTurns(t, h, id, 1)
	contextIdle(t, h, id, es[len(es)-1].Seq)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := h.CompactAt(cancelled, id, "取消摘要", contextPreviewHead(t, h, id)); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消请求写入: %v", err)
	}
	if count(events(t, h, id), agent.EventContextCompacted) != 0 {
		t.Fatal("拒绝的压缩留下事实")
	}
	if err := h.CompactAt(ctx, id, "活动已取消，没有待执行工具。", contextPreviewHead(t, h, id)); err != nil {
		t.Fatal(err)
	}
	live, _ := h.Snapshot(ctx, id)
	contextReplay(t, store, id, live)
}

func TestCompactRejectsPendingApproval(t *testing.T) {
	ctx := context.Background()
	tools := tool.NewRegistry()
	w := &writeTool{}
	if err := tools.Register(w); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return callStream("write", "approval"), nil }}
	h := newHarness(t, openStore(t, filepath.Join(t.TempDir(), "approval.sqlite")), p, tools)
	id := sessionID(t, h)
	prompt(t, h, id, "写入", "approval")
	waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == 1 })
	if err := h.CompactAt(ctx, id, "尚未获批", contextPreviewHead(t, h, id)); !errors.Is(err, agent.ErrContextBusy) || w.executed.Load() != 0 {
		t.Fatalf("待审批活动被压缩: %v", err)
	}
	if err := h.Cancel(ctx, id, "不执行"); err != nil {
		t.Fatal(err)
	}
	es := waitTurns(t, h, id, 1)
	contextIdle(t, h, id, es[len(es)-1].Seq)
	if err := h.CompactAt(ctx, id, "审批已取消，工具未执行。", contextPreviewHead(t, h, id)); err != nil {
		t.Fatal(err)
	}
}

type contextGateStore struct {
	*gormrepo.EventStore
	kind    string
	entered chan struct{}
	release chan struct{}
	fired   atomic.Bool
	failure error
}

func (s *contextGateStore) Append(ctx context.Context, id string, epoch, seq uint64, es []session.NewEvent) ([]session.Event, error) {
	for _, e := range es {
		if e.EventType == s.kind && s.fired.CompareAndSwap(false, true) {
			close(s.entered)
			select {
			case <-s.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if s.failure != nil {
				return nil, s.failure
			}
		}
	}
	return s.EventStore.Append(ctx, id, epoch, seq, es)
}

func awaitContextGate(t *testing.T, gate <-chan struct{}) {
	t.Helper()
	select {
	case <-gate:
	case <-time.After(5 * time.Second):
		t.Fatal("等待并发屏障超时")
	}
}

func TestCompactConcurrentDuplicateAndPromptFencing(t *testing.T) {
	ctx := context.Background()
	store := &contextGateStore{EventStore: openStore(t, filepath.Join(t.TempDir(), "fencing.sqlite")), kind: agent.EventContextCompacted, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(store.release) })
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "旧输入", "old")
	es := waitTurns(t, h, id, 1)
	contextIdle(t, h, id, es[len(es)-1].Seq)
	sourceHead := contextPreviewHead(t, h, id)
	summary := "旧输入已经处理。"
	requestCtx, cancel := context.WithCancel(ctx)
	first := make(chan error, 1)
	go func() { first <- h.CompactAt(requestCtx, id, summary, sourceHead) }()
	awaitContextGate(t, store.entered)
	cancel()
	select {
	case err := <-first:
		t.Fatalf("Append 完成前因取消提前返回: %v", err)
	default:
	}
	const callers = 8
	duplicates := make(chan error, callers)
	for range callers {
		go func() { duplicates <- h.Compact(ctx, id, summary, sourceHead) }()
	}
	release.Do(func() { close(store.release) })
	if err := <-first; err != nil {
		t.Fatalf("已准入压缩未等待权威提交: %v", err)
	}
	for range callers {
		if err := <-duplicates; err != nil {
			t.Fatalf("并发重复压缩: %v", err)
		}
	}
	if count(events(t, h, id), agent.EventContextCompacted) != 1 {
		t.Fatal("并发重复压缩重复追加")
	}
	// 新 Prompt 与旧摘要重试并发：只允许无改动幂等、忙或上下文变化。
	var wg sync.WaitGroup
	wg.Add(2)
	var compactErr, promptErr error
	go func() { defer wg.Done(); compactErr = h.Compact(ctx, id, summary, sourceHead) }()
	go func() { defer wg.Done(); _, promptErr = h.Prompt(ctx, id, "新输入必须保留", "new") }()
	wg.Wait()
	if promptErr != nil || (compactErr != nil && !errors.Is(compactErr, agent.ErrContextBusy) && !errors.Is(compactErr, agent.ErrContextChanged)) {
		t.Fatalf("并发结果: compact=%v prompt=%v", compactErr, promptErr)
	}
	es = waitTurns(t, h, id, 2)
	live := contextIdle(t, h, id, es[len(es)-1].Seq)
	if count(es, agent.EventContextCompacted) != 1 || len(live.Surface().Nodes()) != 3 || len(p.got()) != 2 || p.got()[1].Messages[2].Content[0].Text != "新输入必须保留" {
		t.Fatal("并发 Prompt 被旧摘要吞掉")
	}
	contextReplay(t, store, id, live)
}

// 暂停已加载 Actor 的 Submit，验证调用方版本在排队期间也不会被补读替换。
// 在此暂停只控制测试调度，不依赖 sleep 或生产代码中的测试钩子。
type contextAdmissionGate struct {
	context.Context
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *contextAdmissionGate) Err() error {
	c.once.Do(func() {
		close(c.entered)
		<-c.release
	})
	return c.Context.Err()
}

func TestCompactStaleProposalFencedAfterPromptOrPendingCancel(t *testing.T) {
	for _, activity := range []string{"prompt", "pending-cancel"} {
		t.Run(activity, func(t *testing.T) {
			ctx := context.Background()
			store := openStore(t, filepath.Join(t.TempDir(), "stale.sqlite"))
			p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
			h := newHarness(t, store, p, nil)
			id := sessionID(t, h)
			prompt(t, h, id, "旧历史", "old")
			es := waitTurns(t, h, id, 1)
			before := contextIdle(t, h, id, es[len(es)-1].Seq)
			sourceHead := contextPreviewHead(t, h, id)
			paused := &contextAdmissionGate{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			var release sync.Once
			defer release.Do(func() { close(paused.release) })
			result := make(chan error, 1)
			go func() { result <- h.CompactAt(paused, id, "只根据旧历史提供的摘要", sourceHead) }()
			awaitContextGate(t, paused.entered)
			if activity == "prompt" {
				prompt(t, h, id, "后来完成的新输入", "new")
				es = waitTurns(t, h, id, 2)
				contextIdle(t, h, id, es[len(es)-1].Seq)
			} else {
				if err := h.Inject(ctx, id, "随后取消的待处理输入"); err != nil {
					t.Fatal(err)
				}
				if err := h.Cancel(ctx, id, "取消待处理输入"); err != nil {
					t.Fatal(err)
				}
				after, err := h.Snapshot(ctx, id)
				if err != nil || !reflect.DeepEqual(before.Surface().Nodes(), after.Surface().Nodes()) || before.HeadSeq == after.HeadSeq {
					t.Fatalf("pending cancel 测试前提不成立: %v", err)
				}
			}
			release.Do(func() { close(paused.release) })
			if err := <-result; !errors.Is(err, agent.ErrContextChanged) {
				t.Fatalf("旧提案未被完整 Head fencing: %v", err)
			}
			if count(events(t, h, id), agent.EventContextCompacted) != 0 {
				t.Fatal("旧提案留下压缩事实")
			}
			if err := h.CompactAt(ctx, id, "重新阅读最新历史后提供的新摘要", contextPreviewHead(t, h, id)); err != nil {
				t.Fatal(err)
			}
			live, _ := h.Snapshot(ctx, id)
			contextReplay(t, store, id, live)
		})
	}
}

func TestCompactFailedAppendLeavesNoOrphanFact(t *testing.T) {
	ctx := context.Background()
	store := &contextGateStore{EventStore: openStore(t, filepath.Join(t.TempDir(), "failed.sqlite")), kind: agent.EventContextCompacted, entered: make(chan struct{}), release: make(chan struct{}), failure: errors.New("写入前失败")}
	close(store.release)
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "历史", "old")
	es := waitTurns(t, h, id, 1)
	live := contextIdle(t, h, id, es[len(es)-1].Seq)
	sourceHead := contextPreviewHead(t, h, id)
	if err := h.CompactAt(ctx, id, "人工摘要", sourceHead); err == nil {
		t.Fatal("存储失败被吞掉")
	}
	if !reflect.DeepEqual(es, events(t, h, id)) {
		t.Fatal("失败压缩留下孤立摘要或替换")
	}
	contextReplay(t, store, id, live)
	if err := h.CompactAt(ctx, id, "人工摘要", sourceHead); err != nil {
		t.Fatalf("失败后不能安全重试: %v", err)
	}
}

type contextSizedTool struct{ size int }

func (t contextSizedTool) Definition() tool.Definition {
	return tool.Definition{Name: "sized", Version: "1", Description: strings.Repeat("工具说明", t.size), InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}}}`)}
}
func (contextSizedTool) Execute(context.Context, tool.Call) (tool.Result, error) {
	return tool.Result{}, errors.New("测试不应执行工具")
}

func TestContextOverflowConsistentDurableAndReplay(t *testing.T) {
	for _, part := range []string{"history", "system", "tools", "default-budget"} {
		t.Run(part, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "overflow.sqlite")
			base := openStore(t, path)
			store := &uncertainStore{EventStore: base, kind: "model/error"}
			options := agent.HarnessOptions{Model: "test-model", SystemPrompt: "系统提示", ContextTokenBudget: 900, ContextOutputReservedTokens: 100}
			text := "简短输入"
			tools := tool.NewRegistry()
			switch part {
			case "history":
				text = strings.Repeat("中文输入", 200)
			case "system":
				options.SystemPrompt = strings.Repeat("系统提示", 200)
			case "tools":
				if err := tools.Register(contextSizedTool{size: 200}); err != nil {
					t.Fatal(err)
				}
			case "default-budget":
				options.ContextTokenBudget, options.ContextOutputReservedTokens = 0, 0
				text = strings.Repeat("x", agent.DefaultContextTokenBudget)
			}
			p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("不应调用"), nil }}
			h, err := agent.NewHarness(store, p, tools, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = h.Close(ctx) })
			id := sessionID(t, h)
			prompt(t, h, id, text, "overflow")
			es := waitTurns(t, h, id, 1)
			live := contextIdle(t, h, id, es[len(es)-1].Seq)
			view, err := h.ContextPreview(ctx, id)
			if err != nil || !view.Budget.Exceeded || view.Budget.Exact || view.Budget.Estimator != agent.ContextTokenEstimator || len(view.Messages) != 2 || view.Messages[1].Content[0].Text != text {
				t.Fatalf("超限预览截断或未明确估算: %+v err=%v", view.Budget, err)
			}
			b := view.Budget
			if b.EstimatedInputTokens != b.EstimatedSystemTokens+b.EstimatedHistoryTokens+b.EstimatedToolTokens+b.RequestOverheadTokens || b.RemainingTokens != b.LimitTokens-b.ReservedOutputTokens-b.EstimatedInputTokens {
				t.Fatalf("预算不可解释: %+v", b)
			}
			if len(p.got()) != 0 || count(es, agent.EventModelRequested) != 0 || count(es, "model/usage") != 0 || count(es, "model/error") != 1 || count(es, agent.EventRetry) != 0 || !store.fired.Load() {
				t.Fatalf("超限调用模型/伪造 usage/重试: %s", summarizeEvents(es))
			}
			var payload struct {
				agent.ModelErrorPayload
				Budget        agent.ContextBudget `json:"context_budget"`
				SourceHeadSeq uint64              `json:"source_head_seq"`
			}
			for i, e := range es {
				if e.EventType == "model/error" {
					if err := json.Unmarshal(e.Data, &payload); err != nil {
						t.Fatal(err)
					}
					if payload.Failure.Code != llm.FailureContextWindowExceeded || payload.Attempt != 0 || payload.Budget != b || payload.SourceHeadSeq != e.Seq-1 || es[i+1].EventType != "step/end" || es[i+2].EventType != "turn/end" {
						t.Fatalf("错误和闭合不一致: %s", summarizeEvents(es))
					}
					var end agent.TurnEndPayload
					if err := json.Unmarshal(es[i+2].Data, &end); err != nil || end.Reason != "error" {
						t.Fatalf("Turn 终态: %+v %v", end, err)
					}
				}
			}
			contextReplay(t, store, id, live)
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			h2, err := agent.NewHarness(openStore(t, path), p, tools, options)
			if err != nil {
				t.Fatal(err)
			}
			defer h2.Close(ctx)
			got, err := h2.ContextPreview(ctx, id)
			if err != nil || !reflect.DeepEqual(view, got) || len(events(t, h2, id)) != len(es) {
				t.Fatalf("超限重启不一致: %v", err)
			}
			if part == "history" || part == "default-budget" {
				if err := h2.CompactAt(ctx, id, "输入过长，尚未处理。", got.HeadSeq); err != nil {
					t.Fatal(err)
				}
				prompt(t, h2, id, "继续", "after-compact")
				waitTurns(t, h2, id, 2)
				if len(p.got()) != 1 {
					t.Fatal("手工压缩后未恢复模型请求")
				}
			}
		})
	}
}

func TestContextBudgetExactEstimateBoundaryAndOutputReserve(t *testing.T) {
	text := "边界输入"
	m := llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceStep, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: text, Complete: true}}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	reserved := 80
	budget := len(data) + 16 + 64 + reserved
	for _, delta := range []int{0, -1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
			h, err := agent.NewHarness(openStore(t, filepath.Join(t.TempDir(), "boundary.sqlite")), p, nil, agent.HarnessOptions{Model: "test-model", ContextTokenBudget: budget + delta, ContextOutputReservedTokens: reserved})
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close(context.Background())
			id := sessionID(t, h)
			prompt(t, h, id, text, "boundary")
			es := waitTurns(t, h, id, 1)
			if delta == 0 {
				requests := p.got()
				if len(requests) != 1 || requests[0].MaxTokens == nil || *requests[0].MaxTokens != reserved || !reflect.DeepEqual(requests[0].Messages, []llm.Message{m}) || count(es, "model/error") != 0 {
					t.Fatal("估算等于预算时错误拒绝或输出预留未传递")
				}
			} else if len(p.got()) != 0 || count(es, "model/error") != 1 {
				t.Fatal("估算超过预算 1 时未拒绝")
			}
		})
	}
}

func TestCompactRequiredSchemaRejectsUnpairedOrMismatchedReplacement(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, filepath.Join(t.TempDir(), "schema.sqlite"))
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "历史输入", "first")
	before := waitTurns(t, h, id, 1)
	live := contextIdle(t, h, id, before[len(before)-1].Seq)
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := agent.LoadSessionActor(ctx, store, id, agent.SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Dispose(ctx)
	for _, mode := range []string{"unpaired", "missing-source", "different-summary"} {
		t.Run(mode, func(t *testing.T) {
			_, err := a.Submit(ctx, session.CommandFunc(func(_ context.Context, s session.Snapshot) ([]session.NewEvent, error) {
				nodes := s.Surface().Nodes()
				payload := agent.ContextCompactedPayload{Summary: "人工摘要", Manual: true, SourceHeadSeq: s.HeadSeq, SourceEventSeqs: nodes}
				data, err := json.Marshal(payload)
				if err != nil {
					return nil, err
				}
				marker := session.NewEvent{EventID: "marker-" + mode, EventType: agent.EventContextCompacted, SchemaVersion: session.SchemaVersion{Major: 1}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data}
				if mode == "unpaired" {
					return []session.NewEvent{marker}, nil
				}
				text := "以下为用户手工提供的历史摘要（非模型自动总结）：\n人工摘要"
				if mode == "different-summary" {
					text += "被篡改"
				}
				message := llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceSurface, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: text, Complete: true}}}
				data, err = json.Marshal(message)
				if err != nil {
					return nil, err
				}
				sources := append(slices.Clone(nodes), s.HeadSeq+1)
				if mode == "missing-source" {
					sources = nodes
				}
				replacement := session.NewEvent{EventID: "replacement-" + mode, EventType: session.SurfaceEventUserMessage, SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data, SurfaceOp: session.ReplaceSurfaceOp(nodes[0], nodes[len(nodes)-1]), SourceEventSeqs: sources}
				return []session.NewEvent{marker, replacement}, nil
			}))
			if !errors.Is(err, session.ErrInvalidSurface) {
				t.Fatalf("无效配对未拒绝: %v", err)
			}
			after, err := store.Load(ctx, id, 0, 1000)
			if err != nil || !reflect.DeepEqual(before, after) || a.Snapshot().HeadSeq != live.HeadSeq {
				t.Fatalf("拒绝候选污染了持久事实/投影: %v", err)
			}
		})
	}
}

func contextCompactionBatch(t *testing.T, head uint64, nodes []uint64, summary string) []session.NewEvent {
	t.Helper()
	payload := agent.ContextCompactedPayload{Summary: summary, SourceHeadSeq: head, SourceEventSeqs: nodes, Manual: true}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	marker := session.NewEvent{EventID: "marker-" + summary, EventType: agent.EventContextCompacted, SchemaVersion: session.SchemaVersion{Major: 1}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data}
	message := llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceSurface, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: "以下为用户手工提供的历史摘要（非模型自动总结）：\n" + summary, Complete: true}}}
	data, err = json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	replacement := session.NewEvent{EventID: "replacement-" + summary, EventType: session.SurfaceEventUserMessage, SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data, SurfaceOp: session.ReplaceSurfaceOp(nodes[0], nodes[len(nodes)-1]), SourceEventSeqs: append(slices.Clone(nodes), head+1)}
	return []session.NewEvent{marker, replacement}
}

func TestCompactRejectsJointlyForgedPartialSourcesInCandidateAndReplay(t *testing.T) {
	ctx := context.Background()
	base := openStore(t, filepath.Join(t.TempDir(), "forged.sqlite"))
	tools := tool.NewRegistry()
	if err := tools.Register(&structuredResultTool{}); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
		if n == 0 {
			return callStream("structured", "provenance-call"), nil
		}
		return textStream("工具完成"), nil
	}}
	h := newHarness(t, base, p, tools)
	id := sessionID(t, h)
	prompt(t, h, id, "调用工具", "first")
	before := waitTurns(t, h, id, 1)
	live := contextIdle(t, h, id, before[len(before)-1].Seq)
	nodes := live.Surface().Nodes()
	if len(nodes) != 4 || before[nodes[1]-1].EventType != session.SurfaceEventAssistantMessage || before[nodes[2]-1].EventType != session.SurfaceEventToolResult {
		t.Fatal("测试必须包含成对的 assistant tool call 和 tool result")
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	probe := &contextPreviewProbeStore{EventStore: base}
	a, err := agent.LoadSessionActor(ctx, probe, id, agent.SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Dispose(ctx)
	appendCount := probe.appends.Load()
	for _, mode := range []string{"only-tool-call", "reordered", "extra-source"} {
		t.Run(mode, func(t *testing.T) {
			forged := slices.Clone(nodes)
			switch mode {
			case "only-tool-call":
				forged = []uint64{nodes[1]}
			case "reordered":
				forged[0], forged[1] = forged[1], forged[0]
			case "extra-source":
				forged = append(forged[:len(forged)-1], 1, nodes[len(nodes)-1])
			}
			batch := contextCompactionBatch(t, live.HeadSeq, forged, mode)
			_, err := a.Submit(ctx, session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) { return batch, nil }))
			if !errors.Is(err, session.ErrInvalidSurface) {
				t.Fatalf("marker/replacement 联合伪造来源被接受: %v", err)
			}
			stored, err := base.Load(ctx, id, 0, 1000)
			if err != nil || !reflect.DeepEqual(before, stored) || a.Snapshot().HeadSeq != live.HeadSeq || probe.appends.Load() != appendCount {
				t.Fatalf("候选未在 Append 前拒绝或污染了投影: %v", err)
			}
		})
	}
	// 绕过 Actor 将恶意对写入临时 SQLite，检验 committed/Replay 的同一投影校验。
	bad := contextCompactionBatch(t, live.HeadSeq, []uint64{nodes[1]}, "persisted-partial")
	committed, err := base.Append(ctx, id, a.Snapshot().Epoch, live.HeadSeq, bad)
	if err != nil {
		t.Fatal(err)
	}
	ordinarySurface := live.Surface()
	for _, e := range committed {
		if err := ordinarySurface.Apply(e); err != nil {
			t.Fatalf("反例应满足原有局部 Surface 规则: %v", err)
		}
	}
	if !slices.Contains(ordinarySurface.Nodes(), nodes[2]) || slices.Contains(ordinarySurface.Nodes(), nodes[1]) {
		t.Fatal("反例未留下孤立 tool result")
	}
	schema, err := agent.NewSessionSchema()
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []int{1, 2, 1000} {
		_, err := session.Replay(ctx, base, id, 0, live.HeadSeq+2, session.ReplayOptions{Schema: schema, PageSize: page})
		if !errors.Is(err, session.ErrInvalidSurface) {
			t.Fatalf("Replay 接受联合缩减来源: page=%d err=%v", page, err)
		}
	}
}

func TestCompactSurfaceMirrorTracksLegacyPartialReplaceAndInvisibleNode(t *testing.T) {
	ctx := context.Background()
	base := openStore(t, filepath.Join(t.TempDir(), "mirror.sqlite"))
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("已完成"), nil }}
	h := newHarness(t, base, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "历史输入", "first")
	es := waitTurns(t, h, id, 1)
	before := contextIdle(t, h, id, es[len(es)-1].Seq)
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := agent.LoadSessionActor(ctx, base, id, agent.SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Dispose(ctx)
	message := llm.Message{Role: llm.RoleUser, Source: llm.MessageSourceSurface, Content: []llm.ContentBlock{{Type: llm.ContentBlockText, Text: "旧协议追加与局部替换", Complete: true}}}
	data, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	nodes := before.Surface().Nodes()
	// legacy 只能来自历史持久记录；当前 Actor 正确地禁止新写旧 schema。
	legacy := session.NewEvent{EventID: "legacy-message", EventType: session.SurfaceEventUserMessage, SchemaVersion: session.SchemaVersion{Major: 1}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data}
	replacement := session.NewEvent{EventID: "partial-replacement", EventType: session.SurfaceEventUserMessage, SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: data, SurfaceOp: session.ReplaceSurfaceOp(nodes[1], nodes[1]), SourceEventSeqs: []uint64{nodes[1]}}
	empty := session.NewEvent{EventID: "invisible-assistant", EventType: session.SurfaceEventAssistantMessage, SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor}, OccurredAt: time.Now().UTC(), ReplayPolicy: session.ReplayRequired, Data: json.RawMessage(`{"message":{"role":"assistant","content":[]}}`), SurfaceOp: session.AppendSurfaceOp()}
	epoch := a.Snapshot().Epoch
	if err := a.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = base.Append(ctx, id, epoch, before.HeadSeq, []session.NewEvent{legacy, replacement, empty}); err != nil {
		t.Fatal(err)
	}
	a, err = agent.LoadSessionActor(ctx, base, id, agent.SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Dispose(ctx)
	live := a.Snapshot()
	wantNodes := []uint64{nodes[0], before.HeadSeq + 2, before.HeadSeq + 1, before.HeadSeq + 3}
	if !slices.Equal(live.Surface().Nodes(), wantNodes) {
		t.Fatalf("反例应含非数字递增的 Surface 顺序: %v", live.Surface().Nodes())
	}
	contextReplay(t, base, id, live)
	if err := a.Dispose(ctx); err != nil {
		t.Fatal(err)
	}
	h = newHarness(t, base, p, nil)
	view, err := h.ContextPreview(ctx, id)
	if err != nil || !slices.Equal(view.SourceEventSeqs, wantNodes) || len(view.Messages) != 4 {
		t.Fatalf("镜像或不可见节点错误: %+v %v", view, err)
	}
	if err := h.CompactAt(ctx, id, "完整有序 Surface 的人工摘要", view.HeadSeq); err != nil {
		t.Fatal(err)
	}
	compacted := events(t, h, id)
	var payload agent.ContextCompactedPayload
	if err := json.Unmarshal(compacted[len(compacted)-2].Data, &payload); err != nil || !slices.Equal(payload.SourceEventSeqs, wantNodes) {
		t.Fatalf("镜像未覆盖全部实际节点: %+v %v", payload, err)
	}
}

type contextPreviewProbeStore struct {
	*gormrepo.EventStore
	claims   atomic.Int32
	appends  atomic.Int32
	headRead chan uint64
	release  chan struct{}
	gated    atomic.Bool
}

func (s *contextPreviewProbeStore) ClaimWriter(ctx context.Context, id string) (uint64, uint64, error) {
	s.claims.Add(1)
	return s.EventStore.ClaimWriter(ctx, id)
}

func (s *contextPreviewProbeStore) Append(ctx context.Context, id string, epoch, seq uint64, es []session.NewEvent) ([]session.Event, error) {
	s.appends.Add(1)
	return s.EventStore.Append(ctx, id, epoch, seq, es)
}

func (s *contextPreviewProbeStore) Head(ctx context.Context, id string) (uint64, error) {
	head, err := s.EventStore.Head(ctx, id)
	if err == nil && s.headRead != nil && s.gated.CompareAndSwap(false, true) {
		s.headRead <- head
		select {
		case <-s.release:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return head, err
}

func TestContextPreviewColdSessionDoesNotClaimWriterOrRepair(t *testing.T) {
	ctx := context.Background()
	base := openStore(t, filepath.Join(t.TempDir(), "readonly.sqlite"))
	probe := &contextPreviewProbeStore{EventStore: base}
	reader, err := agent.NewHarness(probe, &scriptProvider{}, nil, agent.HarnessOptions{Model: "reader", ContextTokenBudget: 9876, ContextOutputReservedTokens: 123})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close(ctx)
	gate := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(gate) })
	provider := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
		if n == 0 {
			return &testStream{gate: gate}, nil
		}
		return textStream("原 writer 继续执行"), nil
	}}
	writer := newHarness(t, base, provider, nil)
	id := sessionID(t, writer)
	prompt(t, writer, id, "活动输入", "first")
	waitEvents(t, writer, id, func(es []session.Event) bool { return count(es, agent.EventModelRequested) == 1 })
	before := events(t, writer, id)
	view, err := reader.ContextPreview(ctx, id)
	if err != nil || view.HeadSeq != before[len(before)-1].Seq || len(view.Messages) != 2 || view.Budget.LimitTokens != 9876 || view.Budget.ReservedOutputTokens != 123 {
		t.Fatalf("只读预览或 Harness options 错误: %+v %v", view, err)
	}
	if probe.claims.Load() != 0 || probe.appends.Load() != 0 || !reflect.DeepEqual(before, events(t, writer, id)) {
		t.Fatalf("冷会话预览抢占 writer 或进行了 repair: claims=%d appends=%d", probe.claims.Load(), probe.appends.Load())
	}
	snapshot, err := writer.Snapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	derived, err := reader.ContextPreviewFromSnapshot(snapshot)
	if err != nil || !reflect.DeepEqual(derived, view) || snapshot.Core().Status != session.StatusRunning {
		t.Fatalf("快照预览与只读 Replay 不一致: %v", err)
	}
	release.Do(func() { close(gate) })
	waitTurns(t, writer, id, 1)
	prompt(t, writer, id, "原 writer 的后续输入", "second")
	waitTurns(t, writer, id, 2)
	if len(provider.got()) != 2 || probe.claims.Load() != 0 || probe.appends.Load() != 0 {
		t.Fatal("只读预览影响了原 writer 的继续提交")
	}
}

func TestContextPreviewFencesConcurrentAppendAtCapturedHead(t *testing.T) {
	ctx := context.Background()
	base := openStore(t, filepath.Join(t.TempDir(), "fixed-head.sqlite"))
	probe := &contextPreviewProbeStore{EventStore: base, headRead: make(chan uint64, 1), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(probe.release) })
	reader := newHarness(t, probe, &scriptProvider{}, nil)
	provider := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("完成"), nil }}
	writer := newHarness(t, base, provider, nil)
	id := sessionID(t, writer)
	prompt(t, writer, id, "预览版本", "first")
	es := waitTurns(t, writer, id, 1)
	old := contextIdle(t, writer, id, es[len(es)-1].Seq)
	want, err := reader.ContextPreviewFromSnapshot(old)
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		view agent.ContextView
		err  error
	}
	done := make(chan result, 1)
	go func() { view, err := reader.ContextPreview(ctx, id); done <- result{view, err} }()
	select {
	case head := <-probe.headRead:
		if head != old.HeadSeq {
			t.Fatalf("捕获了错误 head: %d", head)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("等待只读 head 屏障超时")
	}
	prompt(t, writer, id, "不应混入当前预览的新历史", "second")
	es = waitTurns(t, writer, id, 2)
	contextIdle(t, writer, id, es[len(es)-1].Seq)
	release.Do(func() { close(probe.release) })
	got := <-done
	if got.err != nil || !reflect.DeepEqual(got.view, want) {
		t.Fatalf("Replay 最后一页跨越固定 head: %+v %v", got.view, got.err)
	}
	latest, err := reader.ContextPreview(ctx, id)
	if err != nil || latest.HeadSeq <= want.HeadSeq || len(latest.Messages) != 5 {
		t.Fatalf("后续预览未看到新版本: %+v %v", latest, err)
	}
	if probe.claims.Load() != 0 || probe.appends.Load() != 0 {
		t.Fatal("固定 head 预览执行了写入")
	}
}

func TestContextOptionsValidation(t *testing.T) {
	for _, options := range []agent.HarnessOptions{
		{Model: "test", ContextTokenBudget: -1},
		{Model: "test", ContextOutputReservedTokens: -1},
		{Model: "test", ContextTokenBudget: 100, ContextOutputReservedTokens: 100},
		{Model: "test", ContextTokenBudget: 100, ContextOutputReservedTokens: 101},
	} {
		p := &scriptProvider{}
		_, err := agent.NewHarness(openStore(t, filepath.Join(t.TempDir(), "invalid.sqlite")), p, nil, options)
		if !errors.Is(err, agent.ErrInvalidHarnessOptions) {
			t.Fatalf("非法预算未拒绝: %+v %v", options, err)
		}
	}
}
