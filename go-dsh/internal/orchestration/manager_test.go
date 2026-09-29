package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers map[*fakeTicker]bool
}
type fakeTicker struct {
	clock *fakeClock
	ch    chan time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), tickers: map[*fakeTicker]bool{}}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) NewTicker(time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTicker{clock: c, ch: make(chan time.Time, 1)}
	c.tickers[t] = true
	return t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for t := range c.tickers {
		select {
		case t.ch <- c.now:
		default:
		}
	}
}
func (t *fakeTicker) C() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop()               { t.clock.mu.Lock(); defer t.clock.mu.Unlock(); delete(t.clock.tickers, t) }

type scriptProvider struct {
	mu       sync.Mutex
	requests []llm.ModelRequest
	script   func(llm.ModelRequest) llm.Stream
}

func (p *scriptProvider) Stream(_ context.Context, request llm.ModelRequest) (llm.Stream, error) {
	p.mu.Lock()
	p.requests = append(p.requests, request)
	p.mu.Unlock()
	if p.script != nil {
		return p.script(request), nil
	}
	return textStream(nil), nil
}
func (p *scriptProvider) got() []llm.ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]llm.ModelRequest(nil), p.requests...)
}

type testStream struct {
	chunks []llm.StreamChunk
	gate   <-chan struct{}
	once   sync.Once
	closed chan struct{}
}

func (s *testStream) Next(ctx context.Context) (llm.StreamChunk, error) {
	if s.gate != nil {
		select {
		case <-ctx.Done():
			return llm.StreamChunk{}, ctx.Err()
		case <-s.closed:
			return llm.StreamChunk{}, io.EOF
		case <-s.gate:
		}
		s.gate = nil
	}
	select {
	case <-s.closed:
		return llm.StreamChunk{}, io.EOF
	default:
	}
	if len(s.chunks) == 0 {
		return llm.StreamChunk{}, io.EOF
	}
	chunk := s.chunks[0]
	s.chunks = s.chunks[1:]
	return chunk, nil
}
func (s *testStream) Close() error { s.once.Do(func() { close(s.closed) }); return nil }
func textStream(gate <-chan struct{}) llm.Stream {
	return &testStream{gate: gate, closed: make(chan struct{}), chunks: []llm.StreamChunk{{Kind: llm.StreamChunkTextDelta, Text: "脚本结果"}, {Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}}}
}

func openDB(t *testing.T, path string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err = db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	return db
}
func harness(t *testing.T, db *gorm.DB, p *scriptProvider) *agent.Harness {
	t.Helper()
	h, err := agent.NewHarness(gormrepo.NewEventStore(db), p, nil, agent.HarnessOptions{Model: "script", SystemPrompt: "本地固定系统提示", MaxAttempts: 1})
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
	return h
}
func manager(t *testing.T, db *gorm.DB, h *agent.Harness, clock *fakeClock) *Manager {
	t.Helper()
	m, err := NewWithOptions(db, h, Options{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return m
}
func fixture(t *testing.T, p *scriptProvider) (*gorm.DB, *agent.Harness, *Manager, *fakeClock, string) {
	t.Helper()
	db := openDB(t, filepath.Join(t.TempDir(), "orchestration.sqlite"))
	h := harness(t, db, p)
	clock := newClock()
	m := manager(t, db, h, clock)
	parent, err := h.CreateSession(context.Background(), "父会话")
	if err != nil {
		t.Fatal(err)
	}
	return db, h, m, clock, parent
}
func pump(t *testing.T, m *Manager) {
	t.Helper()
	if err := m.pump(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func await(t *testing.T, predicate func() bool) {
	t.Helper()
	timeout := time.NewTimer(8 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if predicate() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatal("等待异步状态超时")
		case <-tick.C:
		}
	}
}
func jobState(t *testing.T, m *Manager, parent, id string, state State) Job {
	t.Helper()
	var result Job
	await(t, func() bool {
		pump(t, m)
		var err error
		result, err = m.GetJob(context.Background(), parent, id)
		if err != nil {
			t.Fatal(err)
		}
		return result.State == state
	})
	return result
}
func workflowState(t *testing.T, m *Manager, parent, id string, state State) Workflow {
	t.Helper()
	var result Workflow
	await(t, func() bool {
		pump(t, m)
		var err error
		result, err = m.GetWorkflow(context.Background(), parent, id)
		if err != nil {
			t.Fatal(err)
		}
		return result.State == state
	})
	return result
}
func eventCount(t *testing.T, h *agent.Harness, id, kind string) int {
	t.Helper()
	n := 0
	after := uint64(0)
	for {
		es, err := h.ListEvents(context.Background(), id, after, 256)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range es {
			if e.EventType == kind {
				n++
			}
			after = e.Seq
		}
		if len(es) < 256 {
			return n
		}
	}
}

func TestChildIsolationOwnershipIdempotencyAndReadOnlyReplay(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, _, parent := fixture(t, p)
	if _, err := h.Prompt(ctx, parent, "父会话秘密历史", "parent-input"); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 1 })
	job, err := m.CreateChild(ctx, parent, "仅子任务", "child-key")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := m.CreateChild(ctx, parent, "仅子任务", "child-key")
	if err != nil || duplicate.ID != job.ID || duplicate.ChildSessionID != job.ChildSessionID {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	if _, err = m.CreateChild(ctx, parent, "不同输入", "child-key"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	other, err := h.CreateSession(ctx, "其他父会话")
	if err != nil {
		t.Fatal(err)
	}
	if err = m.CancelJob(ctx, other, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err = m.GetJob(ctx, other, job.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	result := jobState(t, m, parent, job.ID, Succeeded)
	if result.AcceptedSeq == 0 || result.EndSeq == 0 || result.TurnID == "" {
		t.Fatalf("没有权威终态凭据: %+v", result)
	}
	requests := p.got()
	if len(requests) != 2 {
		t.Fatalf("requests=%d", len(requests))
	}
	if len(requests[1].Messages) != 2 || requests[1].Messages[1].Content[0].Text != "仅子任务" {
		t.Fatalf("子会话继承了父历史: %+v", requests[1].Messages)
	}
	var before, after gormrepo.SessionModel
	if err = db.First(&before, "id = ?", job.ChildSessionID).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = m.Jobs(ctx, parent); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.First(&after, "id = ?", job.ChildSessionID).Error; err != nil {
		t.Fatal(err)
	}
	if before.ActorEpoch != after.ActorEpoch || before.LastSeq != after.LastSeq || after.ParentID == nil || *after.ParentID != parent || after.ForkSeq == nil || *after.ForkSeq != 0 {
		t.Fatalf("读取有副作用或来源错误: before=%+v after=%+v", before, after)
	}
}

func TestJobRecoveryFromCreatedSessionAndLostProjectionUpdate(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, clock, parent := fixture(t, p)
	job, err := m.CreateChild(ctx, parent, "恢复输入", "recover")
	if err != nil {
		t.Fatal(err)
	}
	// 模拟 Session 已完成，但编排进程尚未保存 receipt/state 就崩溃。
	if _, err = h.Prompt(ctx, job.ChildSessionID, job.Prompt, promptKey(job.ID)); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return eventCount(t, h, job.ChildSessionID, "turn/end") == 1 })
	if err = m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		recovered := manager(t, db, h, clock)
		result := jobState(t, recovered, parent, job.ID, Succeeded)
		if result.EndSeq == 0 {
			t.Fatal("恢复没有读取事件")
		}
		if err = recovered.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.got()) != 1 || eventCount(t, h, job.ChildSessionID, agent.EventModelRequested) != 1 {
		t.Fatal("恢复重跑已执行输入")
	}
	m2 := manager(t, db, h, clock)
	var partial jobRecord
	if err = m2.withLease(ctx, func(ctx context.Context) error {
		var e error
		partial, e = m2.createJob(ctx, stableID("job", parent, "before-session"), parent, "尚未建会话", "", 0)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	jobState(t, m2, parent, partial.ID, Succeeded)
	if len(p.got()) != 2 {
		t.Fatal("恢复未建会话的 job 失败")
	}
}

func TestSchedulesFakeClockRecoveryCoalescingAndValidation(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, clock, parent := fixture(t, p)
	due := clock.Now().Add(time.Minute)
	for _, interval := range []time.Duration{-1, time.Second, maxInterval + 1} {
		if _, err := m.CreateSchedule(ctx, parent, "输入", "invalid", due, interval); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := m.CreateSchedule(ctx, parent, "输入", "past", clock.Now(), 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	once, err := m.CreateSchedule(ctx, parent, "一次性", "once", due, 0)
	if err != nil {
		t.Fatal(err)
	}
	fixed, err := m.CreateSchedule(ctx, parent, "固定间隔", "fixed", due, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	pump(t, m)
	if len(p.got()) != 0 {
		t.Fatal("提前投递")
	}
	clock.Advance(time.Minute)
	// 模拟投递成功而 next_due 尚未推进。
	if _, err = h.Prompt(ctx, parent, once.Text, schedulePromptKey(scheduleRecord{Schedule: once})); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 1 })
	m2 := manager(t, db, h, clock)
	pump(t, m2)
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 2 })
	rows, err := m2.Schedules(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.FireCount != 1 {
			t.Fatalf("重复投递: %+v", row)
		}
		if row.ID == once.ID && row.State != Exhausted {
			t.Fatalf("未耗尽: %+v", row)
		}
	}
	if _, err = m2.CreateSchedule(ctx, parent, once.Text, "once", due, 0); err != nil {
		t.Fatalf("过期后的幂等重试被拒绝: %v", err)
	}
	clock.Advance(10 * time.Minute)
	pump(t, m2)
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 3 })
	rows, err = m2.Schedules(ctx, parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.ID == fixed.ID && (row.FireCount != 2 || !row.NextDue.Equal(due.Add(11*time.Minute))) {
			t.Fatalf("补偿风暴或间隔漂移: %+v", row)
		}
	}
	if err = m2.CancelSchedule(ctx, parent, fixed.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Hour)
	pump(t, m2)
	if len(p.got()) != 3 {
		t.Fatalf("取消计划仍触发或恢复重复: %d", len(p.got()))
	}
}

func TestWorkflowPauseResumeAcrossManagersAndIndependentNodes(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	p := &scriptProvider{script: func(r llm.ModelRequest) llm.Stream {
		if r.Messages[len(r.Messages)-1].Content[0].Text == "节点一" {
			return textStream(gate)
		}
		return textStream(nil)
	}}
	db, h, m, clock, parent := fixture(t, p)
	flow, err := m.CreateWorkflow(ctx, parent, []string{"节点一", "节点二"}, "flow")
	if err != nil {
		t.Fatal(err)
	}
	dup, err := m.CreateWorkflow(ctx, parent, []string{"节点一", "节点二"}, "flow")
	if err != nil || dup.ID != flow.ID {
		t.Fatal(err)
	}
	if _, err = m.CreateWorkflow(ctx, parent, []string{"不同节点"}, "flow"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	pump(t, m)
	pump(t, m)
	await(t, func() bool { return len(p.got()) == 1 })
	if err = m.PauseWorkflow(ctx, parent, flow.ID); err != nil {
		t.Fatal(err)
	}
	if err = m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	close(gate)
	m2 := manager(t, db, h, clock)
	await(t, func() bool {
		pump(t, m2)
		value, e := m2.GetWorkflow(ctx, parent, flow.ID)
		if e != nil {
			t.Fatal(e)
		}
		return len(value.Jobs) == 1 && value.Jobs[0].State == Succeeded
	})
	for i := 0; i < 3; i++ {
		pump(t, m2)
	}
	if len(p.got()) != 1 {
		t.Fatal("暂停后启动了后续节点")
	}
	if err = m2.ResumeWorkflow(ctx, parent, flow.ID); err != nil {
		t.Fatal(err)
	}
	value := workflowState(t, m2, parent, flow.ID, Succeeded)
	if len(value.Jobs) != 2 || value.Jobs[0].ChildSessionID == value.Jobs[1].ChildSessionID {
		t.Fatalf("节点未隔离: %+v", value)
	}
	if len(p.got()) != 2 || len(p.got()[1].Messages) != 2 {
		t.Fatal("节点重放或上下文未隔离")
	}
}

func TestWorkflowFailureDoesNotReplayNode(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{script: func(llm.ModelRequest) llm.Stream {
		return &testStream{closed: make(chan struct{}), chunks: []llm.StreamChunk{{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureInvalidRequest, Message: "secret-token"}}}}}
	}}
	db, h, m, clock, parent := fixture(t, p)
	flow, err := m.CreateWorkflow(ctx, parent, []string{"失败节点", "绝不可执行"}, "failure")
	if err != nil {
		t.Fatal(err)
	}
	result := workflowState(t, m, parent, flow.ID, Failed)
	if len(result.Jobs) != 1 || strings.Contains(result.Jobs[0].Outcome, "secret") {
		t.Fatal("失败推进或泄露错误")
	}
	pump(t, m)
	m2 := manager(t, db, h, clock)
	pump(t, m2)
	if err = m2.ResumeWorkflow(ctx, parent, flow.ID); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if len(p.got()) != 1 {
		t.Fatal("恢复重跑失败副作用节点")
	}
}

func TestCancelJobPropagatesAndParentAdmissionBarrier(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	p := &scriptProvider{script: func(llm.ModelRequest) llm.Stream { return textStream(gate) }}
	_, _, m, clock, parent := fixture(t, p)
	job, err := m.CreateChild(ctx, parent, "子任务", "job")
	if err != nil {
		t.Fatal(err)
	}
	grandchild, err := m.CreateChild(ctx, job.ChildSessionID, "孙任务", "grandchild")
	if err != nil {
		t.Fatal(err)
	}
	flow, err := m.CreateWorkflow(ctx, job.ChildSessionID, []string{"后代工作流"}, "descendant")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = m.CreateSchedule(ctx, job.ChildSessionID, "后代计划", "descendant", clock.Now().Add(time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	pump(t, m)
	await(t, func() bool { return len(p.got()) >= 2 })
	if err = m.CancelJob(ctx, parent, job.ID); err != nil {
		t.Fatal(err)
	}
	jobState(t, m, parent, job.ID, Cancelled)
	jobState(t, m, job.ChildSessionID, grandchild.ID, Cancelled)
	workflowState(t, m, job.ChildSessionID, flow.ID, Cancelled)
	if _, err = m.CreateChild(ctx, job.ChildSessionID, "取消后新增", "new"); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	if _, err = m.CreateChild(ctx, grandchild.ChildSessionID, "取消后新增", "new"); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
	rows, err := m.Schedules(ctx, job.ChildSessionID)
	if err != nil || len(rows) != 1 || rows[0].State != Cancelled {
		t.Fatalf("计划未传播: %+v %v", rows, err)
	}
	if err = m.CancelChildren(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if _, err = m.CreateWorkflow(ctx, parent, []string{"禁止"}, "new"); !errors.Is(err, ErrState) {
		t.Fatal(err)
	}
}

func TestConcurrentManagersClaimAndExpiredOwnerFencing(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, clock, parent := fixture(t, p)
	m2 := manager(t, db, h, clock)
	job, err := m.CreateChild(ctx, parent, "只执行一次", "shared")
	if err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- m.withLease(ctx, func(ctx context.Context) error {
			close(entered)
			<-release
			return m.transaction(ctx, func(*gorm.DB) error { return nil })
		})
	}()
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	waitGateSignal(t, entered)
	for i := 0; i < 2; i++ {
		waiting, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		err = m2.pump(waiting)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("共享门内的 owner 被抢占: %v", err)
		}
		if i == 0 {
			clock.Advance(leaseDuration + time.Second)
		}
	}
	unblock()
	if err = waitGateResult(t, finished); !errors.Is(err, ErrBusy) {
		t.Fatalf("过期 owner 可写入: %v", err)
	}
	pump(t, m2)
	jobState(t, m2, parent, job.ID, Succeeded)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, owner := range []*Manager{m, m2} {
		wg.Add(1)
		go func(owner *Manager) { defer wg.Done(); errs <- owner.pump(ctx) }(owner)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, ErrBusy) {
			t.Fatal(err)
		}
	}
	if len(p.got()) != 1 {
		t.Fatalf("双 manager 重复调用: %d", len(p.got()))
	}
}

func TestRunUsesFakeTickerAndCloseWaits(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	_, h, m, clock, parent := fixture(t, p)
	if _, err := m.CreateSchedule(ctx, parent, "时钟触发", "timer", clock.Now().Add(time.Minute), 0); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- m.Run(ctx) }()
	await(t, func() bool { clock.mu.Lock(); defer clock.mu.Unlock(); return len(clock.tickers) == 1 })
	if err := m.Run(ctx); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 1 })
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	clock.mu.Lock()
	n := len(clock.tickers)
	clock.mu.Unlock()
	if n != 0 {
		t.Fatal("ticker 未停止")
	}
	if _, err := m.CreateChild(ctx, parent, "已关闭", "closed"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := m.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSchemaVersionJSONAndSafeErrors(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, clock, parent := fixture(t, p)
	for _, value := range []any{Job{}, Schedule{}, Workflow{}, Options{}} {
		typeOf := reflect.TypeOf(value)
		for i := 0; i < typeOf.NumField(); i++ {
			if typeOf.Field(i).Tag.Get("json") == "" {
				t.Fatalf("缺少 JSON tag: %s.%s", typeOf.Name(), typeOf.Field(i).Name)
			}
		}
		if _, err := json.Marshal(value); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Model(&jobRecord{}).Where("id = ?", coordinatorID).Update("version", 999).Error; err != nil {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, m.gate, 1)
	if _, err := NewWithOptions(db, h, Options{Clock: clock}); !errors.Is(err, ErrVersion) {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, m.gate, 1)
	if _, err := m.CreateChild(ctx, "not-owned", "secret", "key"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := m.CreateChild(ctx, parent, "", "key"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if got := safeError(ctx, errors.New("SQL select secret password")); got != ErrStorage || strings.Contains(got.Error(), "password") {
		t.Fatal(got)
	}
}

var _ session.EventStore = (*replayStore)(nil)
