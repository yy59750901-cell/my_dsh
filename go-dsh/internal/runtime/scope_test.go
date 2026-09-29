package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/yy59750901/go-dsh/internal/runtime"
)

func TestScopeResolvesNearestBinding(t *testing.T) {
	root := runtime.NewScope("root", 1)
	if err := root.Register("model", "root-model", nil); err != nil {
		t.Fatal(err)
	}
	child, err := root.Fork("agent")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Register("model", "agent-model", nil); err != nil {
		t.Fatal(err)
	}

	binding, err := child.Resolve("model")
	if err != nil {
		t.Fatal(err)
	}
	if binding.Value != "agent-model" || binding.Generation != 1 {
		t.Fatalf("binding = %#v", binding)
	}

	rootBinding, err := root.Resolve("model")
	if err != nil {
		t.Fatal(err)
	}
	if rootBinding.Value != "root-model" {
		t.Fatalf("root binding = %#v", rootBinding)
	}
}

func TestChildDisposerCanResolveParentDuringParentDispose(t *testing.T) {
	root := runtime.NewScope("root", 1)
	if err := root.Register("dependency", "available", nil); err != nil {
		t.Fatal(err)
	}
	child, err := root.Fork("child")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Effect("cleanup", func(context.Context) error {
		binding, err := child.Resolve("dependency")
		if err != nil {
			return err
		}
		if binding.Value != "available" {
			return errors.New("unexpected parent binding")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := root.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestScopeRejectsDuplicateBindingInSameScope(t *testing.T) {
	scope := runtime.NewScope("root", 1)
	if err := scope.Register("tool", "first", nil); err != nil {
		t.Fatal(err)
	}
	if err := scope.Register("tool", "second", nil); !errors.Is(err, runtime.ErrBindingExists) {
		t.Fatalf("error = %v, want ErrBindingExists", err)
	}
}

func TestScopeDisposeIsChildFirstThenReverseRegistration(t *testing.T) {
	var order []string
	root := runtime.NewScope("root", 1)
	child, err := root.Fork("child")
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Register("child-first", 1, func(context.Context) error {
		order = append(order, "child-first")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := child.Register("child-second", 2, func(context.Context) error {
		order = append(order, "child-second")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := root.Register("parent-late", 3, func(context.Context) error {
		order = append(order, "parent-late")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := root.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"child-second", "child-first", "parent-late"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("dispose order = %v, want %v", order, want)
	}
	if !root.IsDisposed() || !child.IsDisposed() {
		t.Fatal("root and child must both be disposed")
	}
	if _, err := child.Resolve("child-first"); !errors.Is(err, runtime.ErrScopeDisposed) {
		t.Fatalf("resolve error = %v, want ErrScopeDisposed", err)
	}
}
