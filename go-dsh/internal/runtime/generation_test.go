package runtime_test

import (
	"context"
	"errors"
	goruntime "runtime"
	"testing"

	"github.com/yy59750901/go-dsh/internal/runtime"
)

func TestGenerationCommitKeepsLeasedGenerationAlive(t *testing.T) {
	initial := runtime.NewScope("generation-1", 1)
	if err := initial.Register("model", "v1", nil); err != nil {
		t.Fatal(err)
	}
	manager, err := runtime.NewGenerationManager(initial)
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}

	candidate, err := manager.Begin(context.Background(), func(_ context.Context, id uint64) (*runtime.Scope, error) {
		scope := runtime.NewScope("candidate", id)
		if err := scope.Register("model", "v2", nil); err != nil {
			return scope, err
		}
		return scope, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := candidate.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if initial.IsDisposed() {
		t.Fatal("leased old generation was disposed during commit")
	}

	newLease, err := manager.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	binding, err := newLease.Scope().Resolve("model")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Value != "v2" || newLease.ID() != 2 {
		t.Fatalf("new generation binding = %#v, id = %d", binding, newLease.ID())
	}

	if err := oldLease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !initial.IsDisposed() {
		t.Fatal("old generation must be disposed after its last lease releases")
	}
	if err := newLease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationRollbackLeavesCurrentUnchanged(t *testing.T) {
	initial := runtime.NewScope("generation-1", 1)
	manager, err := runtime.NewGenerationManager(initial)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := manager.Begin(context.Background(), func(_ context.Context, id uint64) (*runtime.Scope, error) {
		return runtime.NewScope("candidate", id), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	candidateScope := candidate.Scope()
	if err := candidate.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !candidateScope.IsDisposed() {
		t.Fatal("rolled back candidate must be disposed")
	}
	currentID, err := manager.CurrentID()
	if err != nil {
		t.Fatal(err)
	}
	if currentID != 1 {
		t.Fatalf("current generation = %d, want 1", currentID)
	}
	if initial.IsDisposed() {
		t.Fatal("rollback must not dispose current generation")
	}
	if err := manager.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationDisposeOwnsBuilderThatFinishesAfterShutdownStarts(t *testing.T) {
	manager, err := runtime.NewGenerationManager(runtime.NewScope("generation-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	builderStarted := make(chan struct{})
	releaseBuilder := make(chan struct{})
	candidateDisposed := make(chan struct{})
	beginResult := make(chan error, 1)
	go func() {
		_, err := manager.Begin(context.Background(), func(_ context.Context, id uint64) (*runtime.Scope, error) {
			close(builderStarted)
			<-releaseBuilder
			scope := runtime.NewScope("candidate", id)
			if err := scope.Effect("cleanup", func(context.Context) error {
				close(candidateDisposed)
				return nil
			}); err != nil {
				return scope, err
			}
			return scope, nil
		})
		beginResult <- err
	}()
	<-builderStarted
	disposeDone := make(chan error, 1)
	go func() {
		disposeDone <- manager.Dispose(context.Background())
	}()
	for {
		_, err := manager.CurrentID()
		if errors.Is(err, runtime.ErrGenerationDisposed) {
			break
		}
		goruntime.Gosched()
	}
	close(releaseBuilder)
	if err := <-beginResult; !errors.Is(err, runtime.ErrGenerationDisposed) {
		t.Fatalf("begin error = %v, want ErrGenerationDisposed", err)
	}
	if err := <-disposeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-candidateDisposed:
	default:
		t.Fatal("candidate built during shutdown was not disposed")
	}
}

func TestGenerationDisposeAndCandidateRollbackShareCleanup(t *testing.T) {
	manager, err := runtime.NewGenerationManager(runtime.NewScope("generation-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	candidate, err := manager.Begin(context.Background(), func(_ context.Context, id uint64) (*runtime.Scope, error) {
		scope := runtime.NewScope("candidate", id)
		if err := scope.Effect("cleanup", func(context.Context) error {
			close(cleanupStarted)
			<-releaseCleanup
			return nil
		}); err != nil {
			return scope, err
		}
		return scope, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	disposeDone := make(chan error, 1)
	go func() {
		disposeDone <- manager.Dispose(context.Background())
	}()
	<-cleanupStarted
	rollbackDone := make(chan error, 1)
	go func() {
		rollbackDone <- candidate.Rollback(context.Background())
	}()
	close(releaseCleanup)
	if err := <-disposeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-rollbackDone; err != nil {
		t.Fatal(err)
	}
}

func TestGenerationRejectsStaleCandidateCommit(t *testing.T) {
	manager, err := runtime.NewGenerationManager(runtime.NewScope("generation-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	builder := func(_ context.Context, id uint64) (*runtime.Scope, error) {
		return runtime.NewScope("candidate", id), nil
	}
	first, err := manager.Begin(context.Background(), builder)
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.Begin(context.Background(), builder)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(context.Background()); !errors.Is(err, runtime.ErrGenerationConflict) {
		t.Fatalf("commit error = %v, want ErrGenerationConflict", err)
	}
	if err := second.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := manager.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
}
