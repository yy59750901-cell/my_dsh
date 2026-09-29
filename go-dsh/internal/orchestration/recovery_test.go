package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
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
	"gorm.io/gorm"
)

type countedTool struct{ calls atomic.Int32 }

func (t *countedTool) Definition() tool.Definition {
	return tool.Definition{Name: "counted", InputSchema: []byte(`{"type":"object","additionalProperties":false}`)}
}
func (t *countedTool) Execute(context.Context, tool.Call) (tool.Result, error) {
	t.calls.Add(1)
	return tool.Result{Content: "本地计数副作用已执行"}, nil
}

func TestCrashRepairNeverReplaysSideEffectNode(t *testing.T) {
	ctx := context.Background()
	gate := make(chan struct{})
	counter := &countedTool{}
	registry := tool.NewRegistry()
	if err := registry.Register(counter); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(r llm.ModelRequest) llm.Stream {
		for _, message := range r.Messages {
			if message.Role == llm.RoleTool {
				return textStream(gate)
			}
		}
		return &testStream{closed: make(chan struct{}), chunks: []llm.StreamChunk{{Kind: llm.StreamChunkToolCallDelta, ToolCallID: "counted-call", ToolName: "counted", ArgumentsDelta: `{}`}, {Kind: llm.StreamChunkBlockEnd}, {Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishToolCalls}}}}
	}}
	db := openDB(t, filepath.Join(t.TempDir(), "original.sqlite"))
	h, err := agent.NewHarness(gormrepo.NewEventStore(db), p, registry, agent.HarnessOptions{Model: "script", MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.Close(context.Background()) })
	clock := newClock()
	m := manager(t, db, h, clock)
	parent, err := h.CreateSession(ctx, "父会话")
	if err != nil {
		t.Fatal(err)
	}
	flow, err := m.CreateWorkflow(ctx, parent, []string{"有副作用的节点", "不应执行的节点"}, "crash")
	if err != nil {
		t.Fatal(err)
	}
	pump(t, m)
	pump(t, m)
	await(t, func() bool { return counter.calls.Load() == 1 && len(p.got()) == 2 })
	value, err := m.GetWorkflow(ctx, parent, flow.ID)
	if err != nil || len(value.Jobs) != 1 {
		t.Fatalf("value=%+v err=%v", value, err)
	}
	child := value.Jobs[0].ChildSessionID
	// 在线一致性副本保留 model/requested 的未结束状态，模拟进程在工具成功后崩溃。
	crashPath := filepath.Join(t.TempDir(), "crash.sqlite")
	if err = db.Exec("VACUUM INTO ?", crashPath).Error; err != nil {
		t.Fatal(err)
	}
	if err = m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	copyDB := openDB(t, crashPath)
	recoveredProvider := &scriptProvider{}
	h2 := harness(t, copyDB, recoveredProvider)
	m2 := manager(t, copyDB, h2, clock)
	result := workflowState(t, m2, parent, flow.ID, Failed)
	if len(result.Jobs) != 1 || result.Jobs[0].Outcome != "interrupted" || result.Jobs[0].EndSeq == 0 {
		t.Fatalf("未从 repair 事件判失败: %+v", result)
	}
	if len(recoveredProvider.got()) != 0 || counter.calls.Load() != 1 {
		t.Fatal("恢复重放副作用")
	}
	count := eventCount(t, h2, child, "turn/end")
	if err = m2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = h2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	h3 := harness(t, copyDB, recoveredProvider)
	m3 := manager(t, copyDB, h3, clock)
	pump(t, m3)
	if eventCount(t, h3, child, "turn/end") != count || len(recoveredProvider.got()) != 0 {
		t.Fatal("第二次加载不幂等")
	}
}

func TestPauseBeforeDispatchAndCancelBeforeFirstNode(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	_, _, m, _, parent := fixture(t, p)
	flow, err := m.CreateWorkflow(ctx, parent, []string{"尚未投递"}, "pause-before")
	if err != nil {
		t.Fatal(err)
	}
	pump(t, m)
	if err = m.PauseWorkflow(ctx, parent, flow.ID); err != nil {
		t.Fatal(err)
	}
	pump(t, m)
	pump(t, m)
	if len(p.got()) != 0 {
		t.Fatal("暂停仍投递 pending 节点")
	}
	if err = m.ResumeWorkflow(ctx, parent, flow.ID); err != nil {
		t.Fatal(err)
	}
	workflowState(t, m, parent, flow.ID, Succeeded)
	cancelled, err := m.CreateWorkflow(ctx, parent, []string{"永不执行"}, "cancel-before")
	if err != nil {
		t.Fatal(err)
	}
	if err = m.CancelWorkflow(ctx, parent, cancelled.ID); err != nil {
		t.Fatal(err)
	}
	result := workflowState(t, m, parent, cancelled.ID, Cancelled)
	if len(result.Jobs) != 0 || len(p.got()) != 1 {
		t.Fatal("取消仍创建节点")
	}
}

func TestConcurrentCreateIdempotencyWithSeparateConnections(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	path := filepath.Join(t.TempDir(), "shared.sqlite")
	db := openDB(t, path)
	h := harness(t, db, p)
	clock := newClock()
	m1, m2 := manager(t, db, h, clock), manager(t, openDB(t, path), h, clock)
	parent, err := h.CreateSession(ctx, "父会话")
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan Job, 2)
	errs := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, m := range []*Manager{m1, m2} {
		wg.Add(1)
		go func(m *Manager) {
			defer wg.Done()
			<-start
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			for {
				value, err := m.CreateChild(deadline, parent, "相同并发输入", "same")
				if !errors.Is(err, ErrBusy) {
					results <- value
					errs <- err
					return
				}
				select {
				case <-deadline.Done():
					errs <- deadline.Err()
					return
				case <-tick.C:
				}
			}
		}(m)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var id string
	for value := range results {
		if id != "" && value.ID != id {
			t.Fatal("并发幂等分裂")
		}
		id = value.ID
	}
	jobState(t, m1, parent, id, Succeeded)
	pump(t, m2)
	if len(p.got()) != 1 {
		t.Fatal("并发创建重复执行")
	}
}

func TestChildIdentifierCollisionFailsClosed(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	_, h, m, _, parent := fixture(t, p)
	id := stableID("job", parent, "collision")
	other, err := h.CreateSession(ctx, "别的父会话")
	if err != nil {
		t.Fatal(err)
	}
	zero := uint64(0)
	if err = m.store.Create(ctx, session.NewSession{ID: stableID("child", id), ParentID: other, ForkSeq: &zero, TenantID: "local", WorkspaceID: "default"}); err != nil {
		t.Fatal(err)
	}
	if _, err = m.CreateChild(ctx, parent, "不得写到错误来源", "collision"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err = m.pump(ctx); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if len(p.got()) != 0 {
		t.Fatal("冲突来源仍执行")
	}
}

func TestLostScheduleUpdateRetriesOriginalInboxKey(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, h, m, clock, parent := fixture(t, p)
	value, err := m.CreateSchedule(ctx, parent, "测试丢失更新", "uncertain", clock.Now().Add(time.Minute), 0)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	var fail atomic.Bool
	fail.Store(true)
	name := "orchestration_test_fail_schedule_update"
	if err = db.Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "dsh_schedules" && fail.CompareAndSwap(true, false) {
			tx.AddError(errors.New("SQL secret schedule-write-failed"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Update().Remove(name) })
	if err = m.pump(ctx); !errors.Is(err, ErrStorage) || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
	await(t, func() bool { return eventCount(t, h, parent, "turn/end") == 1 })
	m2 := manager(t, db, h, clock)
	pump(t, m2)
	rows, err := m2.Schedules(ctx, parent)
	if err != nil || len(rows) != 1 || rows[0].ID != value.ID || rows[0].FireCount != 1 || rows[0].State != Exhausted {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if eventCount(t, h, parent, agent.EventModelRequested) != 1 {
		t.Fatal("更新丢失造成重复触发")
	}
}

func TestCloseWaitsForAdmittedRequest(t *testing.T) {
	ctx := context.Background()
	db, _, m, _, parent := fixture(t, &scriptProvider{})
	job, err := m.CreateChild(ctx, parent, "关闭等待", "close-wait")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	name := "orchestration_test_block_read"
	if err = db.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "dsh_jobs" {
			once.Do(func() { close(entered); <-release; <-tx.Statement.Context.Done() })
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	requestDone := make(chan error, 1)
	go func() { _, err := m.GetJob(ctx, parent, job.ID); requestDone <- err }()
	<-entered
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err = m.Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close 未等待已准入请求: %v", err)
	}
	select {
	case <-requestDone:
		t.Fatal("阻塞请求提前结束")
	default:
	}
	close(release)
	if err = m.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-requestDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Close 未取消已准入请求: %v", err)
	}
}

func TestReadOnlyResultIgnoresForgedCachedCompletion(t *testing.T) {
	ctx := context.Background()
	p := &scriptProvider{}
	db, _, m, _, parent := fixture(t, p)
	job, err := m.CreateChild(ctx, parent, "尚未开始", "projection")
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Model(&jobRecord{}).Where("id = ?", job.ID).Updates(map[string]any{"state": Succeeded, "outcome": "completed", "end_seq": 999}).Error; err != nil {
		t.Fatal(err)
	}
	result, err := m.GetJob(ctx, parent, job.ID)
	if err != nil || result.State != Pending || result.EndSeq != 0 {
		t.Fatalf("信任了缓存假完成: %+v %v", result, err)
	}
}

// 在真实 Commit 释放数据库锁后阻塞，精确命中续租检查与 Prompt 之间的窗口。
// 仅包装 A 的连接；B 使用独立连接，不能靠 SQLite 写锁获得假阳性。
type commitGatePool struct {
	*sql.DB
	afterCommit func()
}

func (p *commitGatePool) BeginTx(ctx context.Context, options *sql.TxOptions) (gorm.ConnPool, error) {
	tx, err := p.DB.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	owner, _ := ctx.Value(leaseKey{}).(string)
	return &commitGateTx{Tx: tx, owner: owner, afterCommit: p.afterCommit}, nil
}

type commitGateTx struct {
	*sql.Tx
	owner       string
	afterCommit func()
}

func (tx *commitGateTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.owner != "" {
		tx.afterCommit()
	}
	return nil
}

func blockNextLeaseCommit(t *testing.T, m *Manager) (<-chan struct{}, func()) {
	t.Helper()
	pool, err := m.db.DB()
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var once sync.Once
	m.db = m.db.Session(&gorm.Session{NewDB: true, Initialized: true})
	m.db.Statement.ConnPool = &commitGatePool{DB: pool, afterCommit: func() {
		once.Do(func() { close(entered); <-release })
	}}
	return entered, unblock
}

func waitGateSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("等待门锁交错信号超时")
	}
}

func waitGateResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("等待门锁操作退出超时")
		return nil
	}
}

func acceptedInputs(t *testing.T, h *agent.Harness, id string) int {
	t.Helper()
	events, err := h.ListEvents(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.EventType != agent.EventInboxSpliced {
			continue
		}
		var payload agent.InboxSplicedPayload
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.CommandKind == agent.CommandFollowUp {
			count += len(payload.Items)
		}
	}
	return count
}

func TestHarnessGateLinearizesDispatchAndControl(t *testing.T) {
	for _, kind := range []string{"schedule", "job", "workflow-pause", "workflow-cancel", "parent-cancel"} {
		for _, dispatchFirst := range []bool{true, false} {
			order := "control-first"
			if dispatchFirst {
				order = "dispatch-first"
			}
			t.Run(kind+"/"+order, func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "gate.sqlite")
				db := openDB(t, path)
				p := &scriptProvider{}
				h := harness(t, db, p)
				clock := newClock()
				a, b := manager(t, db, h, clock), manager(t, openDB(t, path), h, clock)
				if a.gate != b.gate {
					t.Fatal("同 Harness 的不同数据库连接未共享 gate")
				}
				parent, err := h.CreateSession(ctx, "交错父会话")
				if err != nil {
					t.Fatal(err)
				}
				var sessionID string
				var control func(context.Context) error
				var dispatch func(context.Context) error
				switch kind {
				case "schedule":
					value, err := a.CreateSchedule(ctx, parent, "计划输入", "gate", clock.Now().Add(time.Minute), time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					var stale scheduleRecord
					if err := db.First(&stale, "id = ?", value.ID).Error; err != nil {
						t.Fatal(err)
					}
					clock.Advance(time.Minute)
					sessionID = parent
					control = func(ctx context.Context) error { return b.CancelSchedule(ctx, parent, value.ID) }
					dispatch = func(ctx context.Context) error {
						return a.withLease(ctx, func(ctx context.Context) error { return a.fireSchedule(ctx, stale) })
					}
				default:
					var value Job
					if kind == "workflow-pause" || kind == "workflow-cancel" {
						flow, err := a.CreateWorkflow(ctx, parent, []string{"首节点", "后续节点不得投递"}, "gate")
						if err != nil {
							t.Fatal(err)
						}
						pump(t, a)
						jobs, err := a.Jobs(ctx, parent)
						if err != nil || len(jobs) != 1 {
							t.Fatalf("节点初始化错误: %+v %v", jobs, err)
						}
						value = jobs[0]
						if kind == "workflow-pause" {
							control = func(ctx context.Context) error { return b.PauseWorkflow(ctx, parent, flow.ID) }
						} else {
							control = func(ctx context.Context) error { return b.CancelWorkflow(ctx, parent, flow.ID) }
						}
					} else {
						value, err = a.CreateChild(ctx, parent, "子任务输入", "gate")
						if err != nil {
							t.Fatal(err)
						}
						if kind == "parent-cancel" {
							control = func(ctx context.Context) error { return b.CancelChildren(ctx, parent) }
						} else {
							control = func(ctx context.Context) error { return b.CancelJob(ctx, parent, value.ID) }
						}
					}
					var stale jobRecord
					if err := db.First(&stale, "id = ?", value.ID).Error; err != nil {
						t.Fatal(err)
					}
					// 预建 Workflow child，确保被阻塞的第一笔事务正是 Prompt 前续租。
					if err := a.withLease(ctx, func(ctx context.Context) error { return a.ensureChild(ctx, stale) }); err != nil {
						t.Fatal(err)
					}
					sessionID = value.ChildSessionID
					dispatch = func(ctx context.Context) error {
						return a.withLease(ctx, func(ctx context.Context) error { return a.reconcileJob(ctx, stale) })
					}
				}

				want := 0
				if dispatchFirst {
					entered, unblock := blockNextLeaseCommit(t, a)
					finished := make(chan error, 1)
					go func() { finished <- dispatch(ctx) }()
					waitGateSignal(t, entered)
					if acceptedInputs(t, h, sessionID) != 0 {
						t.Fatal("阻塞点已晚于首次投递")
					}
					clock.Advance(leaseDuration + time.Second)
					// A 的 SQLite 事务已结束，独立连接可以写；排除数据库锁的假保护。
					if err := b.db.Model(&jobRecord{}).Where("id = ?", coordinatorID).Update("version", SchemaVersion).Error; err != nil {
						t.Fatalf("阻塞点仍持有数据库写锁: %v", err)
					}
					waiting, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
					err = control(waiting)
					cancel()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("控制穿插了续租检查与首次投递: %v", err)
					}
					controlled := make(chan error, 1)
					go func() { controlled <- control(ctx) }()
					unblock()
					if err := waitGateResult(t, finished); !errors.Is(err, ErrBusy) {
						t.Fatalf("时钟前跳后投影写入没有被租约 fencing: %v", err)
					}
					if err := waitGateResult(t, controlled); err != nil {
						t.Fatal(err)
					}
					want = 1
				} else {
					clock.Advance(leaseDuration + time.Second)
					entered, unblock := blockNextLeaseCommit(t, b)
					controlled := make(chan error, 1)
					go func() { controlled <- control(ctx) }()
					waitGateSignal(t, entered)
					// 控制已提交但尚未出门，携带旧 row 的投递必须可取消地等待。
					waiting, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
					err = dispatch(waiting)
					cancel()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("投递越过尚未退出的控制 gate: %v", err)
					}
					finished := make(chan error, 1)
					go func() { finished <- dispatch(ctx) }()
					unblock()
					if err := waitGateResult(t, controlled); err != nil {
						t.Fatal(err)
					}
					// 使用控制前读取的旧 row，必须在门内重读后拒绝发送。
					if err := waitGateResult(t, finished); err != nil {
						t.Fatal(err)
					}
				}
				if got := acceptedInputs(t, h, sessionID); got != want {
					t.Fatalf("控制成功返回时投递数=%d，期望=%d", got, want)
				}
				clock.Advance(time.Hour)
				for i := 0; i < 3; i++ {
					pump(t, a)
					pump(t, b)
				}
				if got := acceptedInputs(t, h, sessionID); got != want {
					t.Fatalf("控制成功后新增投递: %d，原有=%d", got, want)
				}
				if !dispatchFirst && len(p.got()) != 0 {
					t.Fatal("控制先赢仍调用模型")
				}
				if kind == "workflow-pause" || kind == "workflow-cancel" {
					jobs, err := a.Jobs(ctx, parent)
					if err != nil || len(jobs) != 1 {
						t.Fatalf("控制后仍创建后续节点: %+v %v", jobs, err)
					}
				}
			})
		}
	}
}

func assertHarnessGate(t *testing.T, h *agent.Harness, want *harnessGate, refs int) {
	t.Helper()
	harnessGates.Lock()
	defer harnessGates.Unlock()
	got := harnessGates.entries[h]
	if got != want || (got != nil && got.refs != refs) {
		t.Fatalf("gate 引用错误: got=%p want=%p refs=%d", got, want, refs)
	}
}

func TestHarnessGateCloseAndReferenceLifecycle(t *testing.T) {
	ctx := context.Background()
	db, h, a, clock, _ := fixture(t, &scriptProvider{})
	b := manager(t, db.Session(&gorm.Session{}), h, clock)
	gate := a.gate
	assertHarnessGate(t, h, gate, 2)
	if _, err := NewWithOptions(db, h, Options{PollInterval: time.Millisecond}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, gate, 2)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	finished := make(chan error, 1)
	go func() {
		op, done, err := a.operation(ctx)
		if err != nil {
			finished <- err
			return
		}
		defer done()
		finished <- a.withLease(op, func(ctx context.Context) error {
			close(entered)
			<-release
			return ctx.Err()
		})
	}()
	waitGateSignal(t, entered)
	// 不同 Harness 独立数据库不应被无谓串行化。
	_, _, other, _, _ := fixture(t, &scriptProvider{})
	if other.gate == gate {
		t.Fatal("不同 Harness 共用了同一门")
	}
	if err := other.withLease(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := a.Close(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("持有 gate 的操作未退出却提前 Close: %v", err)
	}
	assertHarnessGate(t, h, gate, 2)
	// B 的 Run 正在等待 A；关闭 B 必须取消等待而非等 A 放行。
	runDone := make(chan error, 1)
	go func() { runDone <- b.Run(ctx) }()
	await(t, func() bool { clock.mu.Lock(); defer clock.mu.Unlock(); return len(clock.tickers) == 1 })
	bounded, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	if err := b.Close(bounded); err != nil {
		t.Fatalf("等待 gate 的 Run 无法关闭: %v", err)
	}
	if err := waitGateResult(t, runDone); err != nil {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, gate, 1)
	c := manager(t, db, h, clock)
	if c.gate != gate {
		t.Fatal("旧持有者尚未退出，新 Manager 分裂出另一扇门")
	}
	assertHarnessGate(t, h, gate, 2)
	if err := c.Close(bounded); err != nil {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, gate, 1)
	unblock()
	if err := waitGateResult(t, finished); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := a.Close(bounded); err != nil {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, nil, 0)
	for _, m := range []*Manager{a, b, c} {
		if err := m.Close(bounded); err != nil {
			t.Fatal(err)
		}
	}
	assertHarnessGate(t, h, nil, 0)
	reopened := manager(t, db, h, clock)
	if reopened.gate == gate {
		t.Fatal("最后 Close 后旧 gate 未回收")
	}
	assertHarnessGate(t, h, reopened.gate, 1)
	if err := reopened.gate.acquire(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("已取消 context 获得空闲 gate")
	}
	if err := reopened.withLease(bounded, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("取消等待丢失 gate token: %v", err)
	}
	if err := reopened.Close(bounded); err != nil {
		t.Fatal(err)
	}
	assertHarnessGate(t, h, nil, 0)
}
