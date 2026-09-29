package orchestration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const (
	coordinatorID    = "orchestration-coordinator-v1"
	leaseDuration    = 2 * time.Minute
	operationTimeout = 30 * time.Second
	maxInterval      = 365 * 24 * time.Hour
)

// 每个存活 Manager 持有一个引用；Close 等全部已准入操作退出后才释放。
// 不以 DB 指针为键，避免不同 GORM connection/session 访问同一 Harness 时分裂门锁。
var harnessGates = struct {
	sync.Mutex
	entries map[*agent.Harness]*harnessGate
}{entries: make(map[*agent.Harness]*harnessGate)}

type harnessGate struct {
	token chan struct{}
	refs  int // 由 harnessGates.Mutex 保护。
}

func retainHarnessGate(h *agent.Harness) *harnessGate {
	harnessGates.Lock()
	defer harnessGates.Unlock()
	gate := harnessGates.entries[h]
	if gate == nil {
		gate = &harnessGate{token: make(chan struct{}, 1)}
		gate.token <- struct{}{}
		harnessGates.entries[h] = gate
	}
	gate.refs++
	return gate
}

func releaseHarnessGate(h *agent.Harness, gate *harnessGate) {
	harnessGates.Lock()
	defer harnessGates.Unlock()
	gate.refs--
	if gate.refs == 0 {
		delete(harnessGates.entries, h)
	}
}

func (g *harnessGate) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		// token 和取消可能同时就绪，不能让已取消的等待者进入投递区间。
		if err := ctx.Err(); err != nil {
			g.release()
			return err
		}
		return nil
	}
}

func (g *harnessGate) release() { g.token <- struct{}{} }

type Manager struct {
	db             *gorm.DB
	h              *agent.Harness
	gate           *harnessGate
	store          *gormrepo.EventStore
	clock          Clock
	poll           time.Duration
	life           context.Context
	stop           context.CancelFunc
	mu             sync.Mutex
	closed         bool
	started        bool
	wg             sync.WaitGroup
	closeDone      chan struct{}
	jobCursor      string
	workflowCursor string
}

func New(database *gorm.DB, h *agent.Harness) (*Manager, error) {
	return NewWithOptions(database, h, Options{})
}

// NewWithOptions 不启动调度；调用 Run 才会投递。仅创建三个新的编排表。
// Version=1 是本包独立 schema 版本，不改变 Session schema；未知版本拒绝启动。
func NewWithOptions(database *gorm.DB, h *agent.Harness, options Options) (*Manager, error) {
	if database == nil || h == nil {
		return nil, ErrInvalid
	}
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.PollInterval == 0 {
		options.PollInterval = 250 * time.Millisecond
	}
	if options.PollInterval < 50*time.Millisecond || options.PollInterval > time.Minute {
		return nil, ErrInvalid
	}
	db := database.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)})
	ctx, cancel := context.WithTimeout(context.Background(), operationTimeout)
	defer cancel()
	db = db.WithContext(ctx)
	for _, model := range []any{&jobRecord{}, &scheduleRecord{}, &workflowRecord{}} {
		if db.Migrator().HasTable(model) {
			if !db.Migrator().HasColumn(model, "Version") {
				return nil, ErrVersion
			}
			var count int64
			if err := db.Model(model).Where("version <> ? OR version IS NULL", SchemaVersion).Count(&count).Error; err != nil {
				return nil, ErrStorage
			}
			if count != 0 {
				return nil, ErrVersion
			}
		} else if err := db.AutoMigrate(model); err != nil {
			return nil, ErrStorage
		}
	}
	row := jobRecord{Job: Job{ID: coordinatorID, State: Paused}, Kind: "coordinator", Version: SchemaVersion}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
		return nil, ErrStorage
	}
	life, stop := context.WithCancel(context.Background())
	return &Manager{db: database.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)}), h: h, gate: retainHarnessGate(h), store: gormrepo.NewEventStore(database.Session(&gorm.Session{Logger: logger.Default.LogMode(logger.Silent)})), clock: options.Clock, poll: options.PollInterval, life: life, stop: stop, closeDone: make(chan struct{})}, nil
}

func (m *Manager) operation(ctx context.Context) (context.Context, func(), error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, nil, ErrClosed
	}
	m.wg.Add(1)
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	stop := context.AfterFunc(m.life, cancel)
	return ctx, func() { stop(); cancel(); m.wg.Done() }, nil
}

// Run 阻塞运行唯一 pump；同 Harness 的共享 gate 串行化投递与控制，
// 持久租约保护调度记录，不提供跨进程 Prompt 与控制的原子性。
// 单次数据库/Session 故障安全返回，由宿主决定是否重启 Run；不隐瞒后台错误。
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.started {
		m.mu.Unlock()
		return ErrBusy
	}
	m.started = true
	m.wg.Add(1)
	m.mu.Unlock()
	defer func() { m.mu.Lock(); m.started = false; m.mu.Unlock(); m.wg.Done() }()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.life, cancel)
	defer stop()
	defer cancel()
	tick := m.clock.NewTicker(m.poll)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := m.pump(ctx)
		if err != nil && !errors.Is(err, ErrBusy) {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C():
		}
	}
}

// Close 仅停止并等待本 Manager 的请求与 pump，不关闭共享 Harness/DB，
// 不取消已投递的子任务。需要取消时先调用 CancelChildren/CancelJob。
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		m.stop()
		go func() {
			m.wg.Wait()
			releaseHarnessGate(m.h, m.gate)
			close(m.closeDone)
		}()
	}
	done := m.closeDone
	m.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) now() time.Time { return m.clock.Now().UTC().Truncate(time.Microsecond) }

type leaseKey struct{}

// gate 覆盖领取租约、读取控制状态、Prompt 返回以及释放租约的整个区间。
// 即使时钟前跳导致租约过期，同 Harness 的另一个 Manager 也不能在
// 检查后、投递前成功取消或暂停。这里只等待输入持久化，不等待 LLM/工具。
func (m *Manager) withLease(ctx context.Context, fn func(context.Context) error) error {
	if err := m.gate.acquire(ctx); err != nil {
		return err
	}
	defer m.gate.release()
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return ErrStorage
	}
	owner := hex.EncodeToString(bytes[:])
	now := m.now()
	result := m.db.WithContext(ctx).Model(&jobRecord{}).Where("id = ? AND (lease_owner = ? OR lease_until <= ?)", coordinatorID, "", now).Updates(map[string]any{"lease_owner": owner, "lease_until": now.Add(leaseDuration)})
	if result.Error != nil {
		return storageError(ctx)
	}
	if result.RowsAffected != 1 {
		return ErrBusy
	}
	defer func() {
		release, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.db.WithContext(release).Model(&jobRecord{}).Where("id = ? AND lease_owner = ?", coordinatorID, owner).Update("lease_owner", "")
	}()
	return fn(context.WithValue(ctx, leaseKey{}, owner))
}

func (m *Manager) transaction(ctx context.Context, fn func(*gorm.DB) error) error {
	owner, _ := ctx.Value(leaseKey{}).(string)
	if owner == "" {
		return ErrBusy
	}
	err := m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := m.now()
		r := tx.Model(&jobRecord{}).Where("id = ? AND lease_owner = ? AND lease_until > ?", coordinatorID, owner, now).Update("lease_until", now.Add(leaseDuration))
		if r.Error != nil {
			return ErrStorage
		}
		if r.RowsAffected != 1 {
			return ErrBusy
		}
		return fn(tx)
	})
	return safeError(ctx, err)
}

func storageError(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrStorage
}
func safeError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	for _, known := range []error{ErrInvalid, ErrNotFound, ErrConflict, ErrState, ErrBusy, ErrClosed, ErrStorage, ErrExecution, ErrVersion} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrStorage
}
func validText(s string) bool { return strings.TrimSpace(s) != "" && len(s) <= 65536 }
func validKey(s string) bool  { return strings.TrimSpace(s) != "" && len(s) <= 256 }
func stableID(kind string, values ...string) string {
	data, _ := json.Marshal(values)
	sum := sha256.Sum256(data)
	return kind + "-" + hex.EncodeToString(sum[:])
}
func digest(v any) string {
	data, _ := json.Marshal(v)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// 这里只验证本机来源归属，不声称 parentID 是身份凭证；HTTP 层仍需本地认证。
func (m *Manager) parent(ctx context.Context, id string) (gormrepo.SessionModel, error) {
	var s gormrepo.SessionModel
	if id == "" || len(id) > 256 {
		return s, ErrNotFound
	}
	err := m.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND workspace_id = ? AND archived_at IS NULL", id, "local", "default").First(&s).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return s, ErrNotFound
	}
	if err != nil {
		return s, storageError(ctx)
	}
	return s, nil
}

func (m *Manager) admission(ctx context.Context, parentID string) error {
	seen := map[string]bool{}
	for parentID != "" {
		if seen[parentID] {
			return ErrState
		}
		seen[parentID] = true
		s, err := m.parent(ctx, parentID)
		if err != nil {
			return err
		}
		var count int64
		if err = m.db.WithContext(ctx).Model(&jobRecord{}).Where("(kind = ? AND parent_id = ?) OR (kind = ? AND child_session_id = ? AND cancel_requested = ?)", "parent_cancel", parentID, "job", parentID, true).Count(&count).Error; err != nil {
			return storageError(ctx)
		}
		if count != 0 {
			return ErrState
		}
		parentID = ""
		if s.ParentID != nil {
			parentID = *s.ParentID
		}
	}
	return nil
}
