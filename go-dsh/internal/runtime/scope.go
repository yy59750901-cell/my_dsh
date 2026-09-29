package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrScopeDisposed   = errors.New("scope is disposed")
	ErrBindingExists   = errors.New("binding already exists in scope")
	ErrBindingNotFound = errors.New("binding not found")
	ErrInvalidBinding  = errors.New("invalid binding")
)

type Binding struct {
	Name       string
	Value      any
	Generation uint64
}

type Scope struct {
	mu          sync.RWMutex
	name        string
	generation  uint64
	parent      *Scope
	children    []*Scope
	bindings    map[string]Binding
	lifecycle   Lifecycle
	closing     bool
	disposed    bool
	disposeDone chan struct{}
	disposeErr  error
}

func NewScope(name string, generation uint64) *Scope {
	return &Scope{
		name:       name,
		generation: generation,
		bindings:   make(map[string]Binding),
	}
}

func (s *Scope) Name() string {
	return s.name
}

func (s *Scope) Generation() uint64 {
	return s.generation
}

func (s *Scope) Parent() *Scope {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.parent
}

func (s *Scope) Fork(name string) (*Scope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.disposed {
		return nil, ErrScopeDisposed
	}

	child := NewScope(name, s.generation)
	child.parent = s
	s.children = append(s.children, child)
	return child, nil
}

func (s *Scope) Register(name string, value any, disposer Disposer) error {
	if name == "" || value == nil {
		return ErrInvalidBinding
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.disposed {
		return ErrScopeDisposed
	}
	if _, exists := s.bindings[name]; exists {
		return fmt.Errorf("%w: %s", ErrBindingExists, name)
	}
	if err := s.lifecycle.Register(Resource{
		Name:       name,
		Generation: s.generation,
		Dispose:    disposer,
	}); err != nil {
		return err
	}
	s.bindings[name] = Binding{
		Name:       name,
		Value:      value,
		Generation: s.generation,
	}
	return nil
}

func (s *Scope) Effect(name string, disposer Disposer) error {
	if disposer == nil {
		return ErrInvalidBinding
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.disposed {
		return ErrScopeDisposed
	}
	return s.lifecycle.Register(Resource{
		Name:       name,
		Generation: s.generation,
		Dispose:    disposer,
	})
}

func (s *Scope) Resolve(name string) (Binding, error) {
	for current := s; current != nil; current = current.Parent() {
		current.mu.RLock()
		if current.disposed {
			current.mu.RUnlock()
			return Binding{}, ErrScopeDisposed
		}
		binding, exists := current.bindings[name]
		current.mu.RUnlock()
		if exists {
			return binding, nil
		}
	}
	return Binding{}, fmt.Errorf("%w: %s", ErrBindingNotFound, name)
}

func (s *Scope) IsDisposed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disposed
}

func (s *Scope) Dispose(ctx context.Context) error {
	s.mu.Lock()
	if s.closing || s.disposed {
		done := s.disposeDone
		s.mu.Unlock()
		select {
		case <-done:
			s.mu.RLock()
			err := s.disposeErr
			s.mu.RUnlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.closing = true
	s.disposeDone = make(chan struct{})
	done := s.disposeDone
	children := append([]*Scope(nil), s.children...)
	s.children = nil
	s.mu.Unlock()

	var disposeErr error
	for index := len(children) - 1; index >= 0; index-- {
		disposeErr = errors.Join(disposeErr, children[index].Dispose(ctx))
	}
	disposeErr = errors.Join(disposeErr, s.lifecycle.Dispose(ctx))

	s.mu.Lock()
	parent := s.parent
	s.parent = nil
	s.bindings = nil
	s.disposed = true
	s.disposeErr = disposeErr
	close(done)
	s.mu.Unlock()
	if parent != nil {
		parent.detachChild(s)
	}
	return disposeErr
}

func (s *Scope) detachChild(child *Scope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for index, candidate := range s.children {
		if candidate != child {
			continue
		}
		s.children = append(s.children[:index], s.children[index+1:]...)
		return
	}
}
