package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/yy59750901/go-dsh/internal/runtime"
)

func TestEventBusBroadcastInvokesAllAndKeepsErrorOrder(t *testing.T) {
	bus := runtime.NewEventBus()
	firstErr := errors.New("first")
	thirdErr := errors.New("third")
	var calls []int
	var mu sync.Mutex
	for index, handlerErr := range []error{firstErr, nil, thirdErr} {
		index := index
		handlerErr := handlerErr
		_, err := bus.Subscribe("session/event", func(context.Context, any) (any, error) {
			mu.Lock()
			calls = append(calls, index)
			mu.Unlock()
			return nil, handlerErr
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	errorsBySubscriber, err := bus.Broadcast(context.Background(), "session/event", "committed")
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 {
		t.Fatalf("calls = %v, want all subscribers", calls)
	}
	if !errors.Is(errorsBySubscriber[0], firstErr) || errorsBySubscriber[1] != nil || !errors.Is(errorsBySubscriber[2], thirdErr) {
		t.Fatalf("errors = %v", errorsBySubscriber)
	}
}

func TestEventBusSerialStopsAtFirstError(t *testing.T) {
	bus := runtime.NewEventBus()
	wantErr := errors.New("veto")
	var order []int
	for index := range 3 {
		index := index
		_, err := bus.Subscribe("session/created", func(context.Context, any) (any, error) {
			order = append(order, index)
			if index == 1 {
				return nil, wantErr
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := bus.Serial(context.Background(), "session/created", nil); !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if want := []int{0, 1}; !reflect.DeepEqual(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestEventBusWaterfallChainsOutputs(t *testing.T) {
	bus := runtime.NewEventBus()
	for _, suffix := range []string{"-scope", "-generation", "-ready"} {
		suffix := suffix
		_, err := bus.Subscribe("config/apply", func(_ context.Context, input any) (any, error) {
			return input.(string) + suffix, nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	output, err := bus.Waterfall(context.Background(), "config/apply", "candidate")
	if err != nil {
		t.Fatal(err)
	}
	if output != "candidate-scope-generation-ready" {
		t.Fatalf("output = %v", output)
	}
}

func TestScopedSubscriptionIsRemovedWithScope(t *testing.T) {
	bus := runtime.NewEventBus()
	scope := runtime.NewScope("agent", 1)
	calls := 0
	if _, err := bus.SubscribeScoped(scope, "agent/event", func(context.Context, any) (any, error) {
		calls++
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Broadcast(context.Background(), "agent/event", nil); err != nil {
		t.Fatal(err)
	}
	if err := scope.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Broadcast(context.Background(), "agent/event", nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestEventBusDisposeWaitsForInflightDispatch(t *testing.T) {
	bus := runtime.NewEventBus()
	started := make(chan struct{})
	release := make(chan struct{})
	if _, err := bus.Subscribe("event", func(context.Context, any) (any, error) {
		close(started)
		<-release
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	dispatchDone := make(chan struct{})
	go func() {
		_, _ = bus.Broadcast(context.Background(), "event", nil)
		close(dispatchDone)
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := bus.DisposeAndWait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispose error = %v, want context.Canceled", err)
	}
	close(release)
	<-dispatchDone
	if err := bus.DisposeAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEventBusCanBeDisposedFromHandlerContext(t *testing.T) {
	bus := runtime.NewEventBus()
	if _, err := bus.Subscribe("event", func(context.Context, any) (any, error) {
		return nil, bus.Dispose(context.Background())
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Broadcast(context.Background(), "event", nil); err != nil {
		t.Fatal(err)
	}
	if err := bus.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEventBusRejectsDispatchAfterDispose(t *testing.T) {
	bus := runtime.NewEventBus()
	if err := bus.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := bus.Broadcast(context.Background(), "event", nil); !errors.Is(err, runtime.ErrEventBusDisposed) {
		t.Fatalf("error = %v, want ErrEventBusDisposed", err)
	}
}
