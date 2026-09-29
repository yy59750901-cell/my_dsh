package runtime

import (
	"context"
	"errors"
	"sync"
)

type Disposer func(context.Context) error

type Resource struct {
	Name       string
	Generation uint64
	Dispose    Disposer
}

type Lifecycle struct {
	mu          sync.Mutex
	resources   []Resource
	disposed    bool
	disposeDone chan struct{}
	disposeErr  error
}

func (l *Lifecycle) Register(resource Resource) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.disposed {
		return errors.New("lifecycle is disposed")
	}
	l.resources = append(l.resources, resource)
	return nil
}

// Dispose releases resources in reverse registration order. Concurrent calls
// wait for the same disposal and observe the same final error.
func (l *Lifecycle) Dispose(ctx context.Context) error {
	l.mu.Lock()
	if l.disposed {
		done := l.disposeDone
		l.mu.Unlock()
		select {
		case <-done:
			l.mu.Lock()
			err := l.disposeErr
			l.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.disposed = true
	l.disposeDone = make(chan struct{})
	done := l.disposeDone
	resources := append([]Resource(nil), l.resources...)
	l.resources = nil
	l.mu.Unlock()

	var disposeErr error
	for index := len(resources) - 1; index >= 0; index-- {
		if resources[index].Dispose == nil {
			continue
		}
		disposeErr = errors.Join(disposeErr, resources[index].Dispose(ctx))
	}

	l.mu.Lock()
	l.disposeErr = disposeErr
	close(done)
	l.mu.Unlock()
	return disposeErr
}
