package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/yy59750901/go-dsh/internal/runtime"
)

func TestLifecycleDisposesInReverseOrderAndIsIdempotent(t *testing.T) {
	var order []string
	lifecycle := new(runtime.Lifecycle)
	for _, name := range []string{"parent", "child", "leaf"} {
		name := name
		if err := lifecycle.Register(runtime.Resource{Name: name, Dispose: func(context.Context) error {
			order = append(order, name)
			return nil
		}}); err != nil {
			t.Fatal(err)
		}
	}

	if err := lifecycle.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}

	want := []string{"leaf", "child", "parent"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("dispose order = %v, want %v", order, want)
	}
}

func TestLifecycleConcurrentDisposeWaitsAndSharesError(t *testing.T) {
	lifecycle := new(runtime.Lifecycle)
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("dispose failed")
	if err := lifecycle.Register(runtime.Resource{Dispose: func(context.Context) error {
		close(started)
		<-release
		return wantErr
	}}); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	var group sync.WaitGroup
	group.Add(2)
	for range 2 {
		go func() {
			defer group.Done()
			results <- lifecycle.Dispose(context.Background())
		}()
	}
	<-started
	close(release)
	group.Wait()
	close(results)

	for err := range results {
		if !errors.Is(err, wantErr) {
			t.Fatalf("dispose error = %v, want %v", err, wantErr)
		}
	}
}
