package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrEventBusDisposed = errors.New("event bus is disposed")
	ErrInvalidHandler   = errors.New("invalid event handler")
)

type EventHandler func(context.Context, any) (any, error)

type eventSubscriber struct {
	id      uint64
	handler EventHandler
}

type EventBus struct {
	mu          sync.RWMutex
	nextID      uint64
	subscribers map[string][]eventSubscriber
	inflight    sync.WaitGroup
	disposed    bool
	disposeDone chan struct{}
}

func NewEventBus() *EventBus {
	return &EventBus{subscribers: make(map[string][]eventSubscriber)}
}

func (b *EventBus) Subscribe(topic string, handler EventHandler) (*Subscription, error) {
	if topic == "" || handler == nil {
		return nil, ErrInvalidHandler
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.disposed {
		return nil, ErrEventBusDisposed
	}
	return b.subscribeLocked(topic, handler), nil
}

func (b *EventBus) subscribeLocked(topic string, handler EventHandler) *Subscription {
	b.nextID++
	subscriber := eventSubscriber{id: b.nextID, handler: handler}
	b.subscribers[topic] = append(b.subscribers[topic], subscriber)
	return &Subscription{bus: b, topic: topic, id: subscriber.id}
}

func (b *EventBus) SubscribeScoped(scope *Scope, topic string, handler EventHandler) (*Subscription, error) {
	if scope == nil || topic == "" || handler == nil {
		return nil, ErrInvalidHandler
	}

	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.closing || scope.disposed {
		return nil, ErrScopeDisposed
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.disposed {
		return nil, ErrEventBusDisposed
	}

	subscription := b.subscribeLocked(topic, handler)
	if err := scope.lifecycle.Register(Resource{
		Name:       fmt.Sprintf("event:%s:%d", topic, subscription.id),
		Generation: scope.generation,
		Dispose: func(context.Context) error {
			subscription.Unsubscribe()
			return nil
		},
	}); err != nil {
		subscription.unsubscribeLocked()
		return nil, err
	}
	return subscription, nil
}

func (b *EventBus) beginDispatch(topic string) ([]eventSubscriber, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.disposed {
		return nil, ErrEventBusDisposed
	}
	b.inflight.Add(1)
	return append([]eventSubscriber(nil), b.subscribers[topic]...), nil
}

// Broadcast invokes a snapshot of subscribers concurrently. The returned error
// slice preserves registration order; callers decide whether observer failures
// are only recorded or fail a durability checkpoint.
func (b *EventBus) Broadcast(ctx context.Context, topic string, input any) ([]error, error) {
	subscribers, err := b.beginDispatch(topic)
	if err != nil {
		return nil, err
	}
	defer b.inflight.Done()

	errorsBySubscriber := make([]error, len(subscribers))
	var group sync.WaitGroup
	group.Add(len(subscribers))
	for index, subscriber := range subscribers {
		index := index
		subscriber := subscriber
		go func() {
			defer group.Done()
			_, errorsBySubscriber[index] = subscriber.handler(ctx, input)
		}()
	}
	group.Wait()
	return errorsBySubscriber, nil
}

func (b *EventBus) Serial(ctx context.Context, topic string, input any) error {
	subscribers, err := b.beginDispatch(topic)
	if err != nil {
		return err
	}
	defer b.inflight.Done()
	for _, subscriber := range subscribers {
		if _, err := subscriber.handler(ctx, input); err != nil {
			return err
		}
	}
	return nil
}

func (b *EventBus) Waterfall(ctx context.Context, topic string, input any) (any, error) {
	subscribers, err := b.beginDispatch(topic)
	if err != nil {
		return nil, err
	}
	defer b.inflight.Done()
	output := input
	for _, subscriber := range subscribers {
		output, err = subscriber.handler(ctx, output)
		if err != nil {
			return nil, err
		}
	}
	return output, nil
}

// Dispose initiates shutdown and is safe to call from an event handler.
// Use DisposeAndWait at an owning lifecycle boundary that must await handlers.
func (b *EventBus) Dispose(context.Context) error {
	b.Shutdown()
	return nil
}

func (b *EventBus) Shutdown() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.disposed {
		return
	}
	b.disposed = true
	b.subscribers = nil
	b.disposeDone = make(chan struct{})
	done := b.disposeDone
	go func() {
		b.inflight.Wait()
		close(done)
	}()
}

func (b *EventBus) Wait(ctx context.Context) error {
	b.mu.RLock()
	done := b.disposeDone
	disposed := b.disposed
	b.mu.RUnlock()
	if !disposed {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *EventBus) DisposeAndWait(ctx context.Context) error {
	b.Shutdown()
	return b.Wait(ctx)
}

type Subscription struct {
	once  sync.Once
	bus   *EventBus
	topic string
	id    uint64
}

func (s *Subscription) Unsubscribe() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		s.bus.mu.Lock()
		defer s.bus.mu.Unlock()
		s.unsubscribeLocked()
	})
}

func (s *Subscription) unsubscribeLocked() {
	subscribers := s.bus.subscribers[s.topic]
	for index, subscriber := range subscribers {
		if subscriber.id != s.id {
			continue
		}
		s.bus.subscribers[s.topic] = append(subscribers[:index], subscribers[index+1:]...)
		if len(s.bus.subscribers[s.topic]) == 0 {
			delete(s.bus.subscribers, s.topic)
		}
		return
	}
}
