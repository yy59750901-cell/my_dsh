package profile

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrClosed     = errors.New("profile manager closed")
	ErrConflict   = errors.New("profile candidate version conflict")
	ErrCandidate  = errors.New("profile candidate unavailable")
	ErrValidation = errors.New("profile candidate validation failed")
	ErrCleanup    = errors.New("profile resource close failed")
)

// Snapshot 的配置不可原地修改；Bundle 每次返回独立副本。
type Snapshot struct {
	version uint64
	bundle  Bundle
}

func (s Snapshot) Version() uint64 { return s.version }
func (s Snapshot) Bundle() Bundle  { return cloneBundle(s.bundle) }

// Validator 在发布前构造资源。成功时所有权转移给 Manager；失败时即使返回
// 非 nil 资源也会关闭。回调须遵守 ctx，不得自行发布路由；错误只返回安全分类。
// nil Validator 表示只校验配置。凭据应由回调按需解析，不能写回快照。
type Validator func(context.Context, Snapshot) (io.Closer, error)

type generation struct {
	snapshot Snapshot
	resource io.Closer
	refs     int
}
type Manager struct {
	mu         sync.Mutex
	validate   Validator
	current    *generation
	pending    map[*Candidate]struct{}
	closed     bool
	cleanupErr error
}
type Candidate struct {
	manager *Manager
	base    uint64
	next    *generation
}
type Lease struct {
	manager    *Manager
	generation *generation
}

func NewManager(validate Validator) *Manager {
	return &Manager{validate: validate, pending: make(map[*Candidate]struct{}), current: &generation{snapshot: Snapshot{bundle: Bundle{Profiles: []Profile{}}}, refs: 1}}
}

func (m *Manager) ValidateCandidate(ctx context.Context, b Bundle) (*Candidate, error) {
	if ctx == nil {
		return nil, ErrValidation
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.Validate(); err != nil {
		return nil, err
	}
	b = cloneBundle(b)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	base := m.current.snapshot.version
	m.mu.Unlock()
	if base == ^uint64(0) {
		return nil, ErrConflict
	}
	next := &generation{snapshot: Snapshot{version: base + 1, bundle: b}, refs: 1}
	var err error
	if m.validate != nil {
		next.resource, err = prepare(m.validate, ctx, Snapshot{version: base + 1, bundle: cloneBundle(b)})
	}
	if err != nil || ctx.Err() != nil {
		m.dispose(next.resource)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrValidation
	}
	c := &Candidate{manager: m, base: base, next: next}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.dispose(next.resource)
		return nil, ErrClosed
	}
	m.pending[c] = struct{}{}
	m.mu.Unlock()
	return c, nil
}

func prepare(fn Validator, ctx context.Context, s Snapshot) (resource io.Closer, err error) {
	defer func() {
		if recover() != nil {
			err = ErrValidation
		}
	}()
	return fn(ctx, s)
}

// Commit 使用乐观版本检查；冲突、取消或已关闭时自动回滚候选。
// 提交成功后旧资源的清理错误通过 CleanupError 查询，不伪装成提交失败。
func (m *Manager) Commit(ctx context.Context, c *Candidate) (Snapshot, error) {
	if c == nil || c.manager != m {
		return Snapshot{}, ErrCandidate
	}
	m.mu.Lock()
	if c.next == nil {
		m.mu.Unlock()
		return Snapshot{}, ErrCandidate
	}
	next := c.next
	var err error
	switch {
	case ctx == nil:
		err = ErrValidation
	case ctx.Err() != nil:
		err = ctx.Err()
	case m.closed:
		err = ErrClosed
	case c.base != m.current.snapshot.version:
		err = ErrConflict
	}
	c.next = nil
	delete(m.pending, c)
	if err != nil {
		m.mu.Unlock()
		m.dispose(next.resource)
		return Snapshot{}, err
	}
	old := m.current
	m.current = next
	old.refs--
	var retired io.Closer
	if old.refs == 0 {
		retired = old.resource
	}
	snapshot := next.snapshot
	m.mu.Unlock()
	m.dispose(retired)
	return snapshot, nil
}

func (m *Manager) Reload(ctx context.Context, raw []byte) (Snapshot, error) {
	b, err := ParseBundle(raw)
	if err != nil {
		return Snapshot{}, err
	}
	c, err := m.ValidateCandidate(ctx, b)
	if err != nil {
		return Snapshot{}, err
	}
	return m.Commit(ctx, c)
}

// Close 丢弃尚未提交的候选，幂等且不会关闭已提交的资源。
func (c *Candidate) Close() error {
	if c == nil || c.manager == nil {
		return nil
	}
	m := c.manager
	m.mu.Lock()
	next := c.next
	c.next = nil
	delete(m.pending, c)
	m.mu.Unlock()
	if next != nil {
		return m.dispose(next.resource)
	}
	return nil
}

func (m *Manager) Acquire() (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	m.current.refs++
	return &Lease{manager: m, generation: m.current}, nil
}
func (l *Lease) Snapshot() (Snapshot, error) {
	if l == nil || l.manager == nil {
		return Snapshot{}, ErrClosed
	}
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	if l.generation == nil {
		return Snapshot{}, ErrClosed
	}
	return l.generation.snapshot, nil
}

// Resource 是借用值，调用方不得 Close；使用期间必须持有对应 lease。
// Manager 只保护生命周期，不使调用方资源本身变成线程安全。
func (l *Lease) Resource() (io.Closer, error) {
	if l == nil || l.manager == nil {
		return nil, ErrClosed
	}
	l.manager.mu.Lock()
	defer l.manager.mu.Unlock()
	if l.generation == nil {
		return nil, ErrClosed
	}
	return l.generation.resource, nil
}
func (l *Lease) Close() error {
	if l == nil || l.manager == nil {
		return nil
	}
	m := l.manager
	m.mu.Lock()
	var retired io.Closer
	if g := l.generation; g != nil {
		l.generation = nil
		g.refs--
		if g.refs == 0 {
			retired = g.resource
		}
	}
	m.mu.Unlock()
	return m.dispose(retired)
}

// Close 禁止新候选与租约；已有租约继续有效，最后一个 lease 释放后才关闭资源。
// 回调资源 Close 必须可结束；这里不启动不可控的后台清理 goroutine。
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		err := m.cleanupErr
		m.mu.Unlock()
		return err
	}
	m.closed = true
	resources := []io.Closer{}
	for c := range m.pending {
		resources = append(resources, c.next.resource)
		c.next = nil
		delete(m.pending, c)
	}
	m.current.refs--
	if m.current.refs == 0 {
		resources = append(resources, m.current.resource)
	}
	m.mu.Unlock()
	for _, r := range resources {
		m.dispose(r)
	}
	return m.CleanupError()
}
func (m *Manager) CleanupError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cleanupErr
}
func (m *Manager) dispose(resource io.Closer) (err error) {
	if resource == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrCleanup
		}
		if err != nil {
			err = ErrCleanup
			m.mu.Lock()
			m.cleanupErr = ErrCleanup
			m.mu.Unlock()
		}
	}()
	return resource.Close()
}
