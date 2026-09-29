package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrGenerationDisposed = errors.New("generation manager is disposed")
	ErrGenerationConflict = errors.New("generation changed before candidate commit")
	ErrCandidateClosed    = errors.New("generation candidate is closed")
	ErrInvalidGeneration  = errors.New("invalid generation")
)

type PostCommitCleanupError struct {
	Generation uint64
	Err        error
}

func (e *PostCommitCleanupError) Error() string {
	return fmt.Sprintf("generation %d committed, previous generation cleanup failed: %v", e.Generation, e.Err)
}

func (e *PostCommitCleanupError) Unwrap() error {
	return e.Err
}

type generationState struct {
	id        uint64
	scope     *Scope
	refs      int
	retired   bool
	disposing bool
	done      chan struct{}
	err       error
}

type GenerationManager struct {
	mu          sync.Mutex
	current     *generationState
	states      map[uint64]*generationState
	candidates  map[uint64]*generationState
	building    map[uint64]chan struct{}
	nextID      uint64
	disposed    bool
	disposeDone chan struct{}
	disposeErr  error
}

func NewGenerationManager(initial *Scope) (*GenerationManager, error) {
	if initial == nil || initial.Generation() == 0 {
		return nil, ErrInvalidGeneration
	}
	state := &generationState{
		id:    initial.Generation(),
		scope: initial,
		done:  make(chan struct{}),
	}
	return &GenerationManager{
		current:    state,
		states:     map[uint64]*generationState{state.id: state},
		candidates: make(map[uint64]*generationState),
		building:   make(map[uint64]chan struct{}),
		nextID:     state.id + 1,
	}, nil
}

func (m *GenerationManager) CurrentID() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.disposed {
		return 0, ErrGenerationDisposed
	}
	return m.current.id, nil
}

type GenerationBuilder func(context.Context, uint64) (*Scope, error)

func (m *GenerationManager) Begin(ctx context.Context, builder GenerationBuilder) (*Candidate, error) {
	if builder == nil {
		return nil, ErrInvalidGeneration
	}

	m.mu.Lock()
	if m.disposed {
		m.mu.Unlock()
		return nil, ErrGenerationDisposed
	}
	baseID := m.current.id
	generationID := m.nextID
	m.nextID++
	buildDone := make(chan struct{})
	m.building[generationID] = buildDone
	m.mu.Unlock()

	scope, buildErr := builder(ctx, generationID)
	if buildErr == nil && (scope == nil || scope.Generation() != generationID) {
		buildErr = fmt.Errorf("%w: candidate scope generation must be %d", ErrInvalidGeneration, generationID)
	}

	m.mu.Lock()
	delete(m.building, generationID)
	disposed := m.disposed
	var state *generationState
	if buildErr == nil && !disposed {
		state = &generationState{id: generationID, scope: scope, done: make(chan struct{})}
		m.candidates[generationID] = state
		close(buildDone)
		m.mu.Unlock()
		return &Candidate{manager: m, baseID: baseID, state: state}, nil
	}
	m.mu.Unlock()

	if scope != nil {
		buildErr = errors.Join(buildErr, scope.Dispose(ctx))
	}
	if disposed {
		buildErr = errors.Join(ErrGenerationDisposed, buildErr)
	}
	m.mu.Lock()
	close(buildDone)
	m.mu.Unlock()
	return nil, buildErr
}

func (m *GenerationManager) Acquire() (*GenerationLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.disposed {
		return nil, ErrGenerationDisposed
	}
	state := m.current
	state.refs++
	return &GenerationLease{manager: m, state: state}, nil
}

func (m *GenerationManager) release(ctx context.Context, state *generationState) error {
	m.mu.Lock()
	if state.refs <= 0 {
		m.mu.Unlock()
		return nil
	}
	state.refs--
	shouldDispose := state.retired && state.refs == 0 && !state.disposing
	if shouldDispose {
		state.disposing = true
	}
	m.mu.Unlock()

	if shouldDispose {
		return m.disposeState(ctx, state)
	}
	return nil
}

func (m *GenerationManager) disposeState(ctx context.Context, state *generationState) error {
	err := state.scope.Dispose(ctx)
	m.mu.Lock()
	state.err = err
	close(state.done)
	delete(m.states, state.id)
	delete(m.candidates, state.id)
	m.mu.Unlock()
	return err
}

func (m *GenerationManager) Dispose(ctx context.Context) error {
	m.mu.Lock()
	if m.disposed {
		done := m.disposeDone
		m.mu.Unlock()
		select {
		case <-done:
			m.mu.Lock()
			err := m.disposeErr
			m.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.disposed = true
	m.disposeDone = make(chan struct{})
	done := m.disposeDone
	building := make([]chan struct{}, 0, len(m.building))
	for _, buildDone := range m.building {
		building = append(building, buildDone)
	}
	for id, state := range m.candidates {
		delete(m.candidates, id)
		m.states[id] = state
	}
	states := make([]*generationState, 0, len(m.states))
	ready := make([]*generationState, 0, len(m.states))
	for _, state := range m.states {
		state.retired = true
		states = append(states, state)
		if state.refs == 0 && !state.disposing {
			state.disposing = true
			ready = append(ready, state)
		}
	}
	m.mu.Unlock()

	var disposeErr error
	for _, state := range ready {
		disposeErr = errors.Join(disposeErr, m.disposeState(ctx, state))
	}
	for _, buildDone := range building {
		select {
		case <-buildDone:
		case <-ctx.Done():
			disposeErr = errors.Join(disposeErr, ctx.Err())
		}
	}
	disposeErr = errors.Join(disposeErr, waitGenerationStates(ctx, states))

	m.mu.Lock()
	m.disposeErr = disposeErr
	close(done)
	m.mu.Unlock()
	return disposeErr
}

func waitGenerationStates(ctx context.Context, states []*generationState) error {
	var waitErr error
	for _, state := range states {
		select {
		case <-state.done:
			waitErr = errors.Join(waitErr, state.err)
		case <-ctx.Done():
			return errors.Join(waitErr, ctx.Err())
		}
	}
	return waitErr
}

type Candidate struct {
	mu      sync.Mutex
	manager *GenerationManager
	baseID  uint64
	state   *generationState
	closed  bool
}

func (c *Candidate) ID() uint64 {
	return c.state.id
}

func (c *Candidate) Scope() *Scope {
	return c.state.scope
}

func (c *Candidate) Commit(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCandidateClosed
	}

	m := c.manager
	m.mu.Lock()
	if m.disposed {
		m.mu.Unlock()
		return ErrGenerationDisposed
	}
	if m.current.id != c.baseID {
		m.mu.Unlock()
		return ErrGenerationConflict
	}
	previous := m.current
	previous.retired = true
	delete(m.candidates, c.state.id)
	m.current = c.state
	m.states[c.state.id] = c.state
	shouldDispose := previous.refs == 0 && !previous.disposing
	if shouldDispose {
		previous.disposing = true
	}
	m.mu.Unlock()
	c.closed = true

	if shouldDispose {
		if err := m.disposeState(ctx, previous); err != nil {
			return &PostCommitCleanupError{Generation: c.state.id, Err: err}
		}
	}
	return nil
}

func (c *Candidate) Rollback(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCandidateClosed
	}
	c.closed = true

	m := c.manager
	m.mu.Lock()
	_, owned := m.candidates[c.state.id]
	if owned {
		delete(m.candidates, c.state.id)
		m.states[c.state.id] = c.state
		c.state.retired = true
		c.state.disposing = true
	}
	disposing := c.state.disposing
	m.mu.Unlock()
	if owned {
		return m.disposeState(ctx, c.state)
	}
	if disposing {
		select {
		case <-c.state.done:
			return c.state.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.state.scope.Dispose(ctx)
}

type GenerationLease struct {
	mu      sync.Mutex
	manager *GenerationManager
	state   *generationState
	done    chan struct{}
	err     error
}

func (l *GenerationLease) ID() uint64 {
	return l.state.id
}

func (l *GenerationLease) Scope() *Scope {
	return l.state.scope
}

func (l *GenerationLease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.done != nil {
		done := l.done
		l.mu.Unlock()
		select {
		case <-done:
			l.mu.Lock()
			err := l.err
			l.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.done = make(chan struct{})
	done := l.done
	l.mu.Unlock()

	err := l.manager.release(ctx, l.state)
	l.mu.Lock()
	l.err = err
	close(done)
	l.mu.Unlock()
	return err
}
