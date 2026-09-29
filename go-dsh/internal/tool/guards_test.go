package tool

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type panicDefinition struct{}

func (panicDefinition) Definition() Definition                        { panic("private metadata") }
func (panicDefinition) Execute(context.Context, Call) (Result, error) { return Result{}, nil }

type panicError struct{}

func (panicError) Error() string { panic("private error") }

func TestMetadataFailureDoesNotReplaceGeneration(t *testing.T) {
	r := NewRegistry()
	original := register(t, r, NewEchoTool()).Definition()
	if !errors.Is(r.Register(panicDefinition{}), ErrToolPanic) {
		t.Fatal("metadata panic not recovered")
	}
	bad := fixture("echo")
	bad.def.InputSchema = json.RawMessage(`{"pattern":"bad"}`)
	if r.Register(bad) == nil {
		t.Fatal("invalid replacement accepted")
	}
	h, err := r.Resolve("echo")
	if err != nil || h.Definition().Generation != original.Generation {
		t.Fatal(h, err)
	}
}

func TestTimeoutMetadataAndEarlyRejection(t *testing.T) {
	r := NewRegistry()
	f := fixture("inspect")
	f.run = func(ctx context.Context, c Call) (Result, error) {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > MaxTimeout || c.Timeout != MaxTimeout || c.Name != "inspect" || c.Generation == 0 || c.DefinitionVersion != "1" {
			t.Error("execution metadata not normalized", c)
		}
		return Result{}, nil
	}
	h := register(t, r, f)
	if _, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`), Timeout: MaxTimeout * 2}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []Call{
		{Name: "different", Arguments: json.RawMessage(`{}`)},
		{DefinitionVersion: "different", Arguments: json.RawMessage(`{}`)},
		{Timeout: -time.Second, Arguments: json.RawMessage(`{}`)},
		{Arguments: make([]byte, MaxArgumentBytes+1)},
	} {
		if _, err := h.Execute(context.Background(), c); err == nil {
			t.Fatal("invalid call accepted")
		}
	}
	if _, err := h.Execute(nil, Call{}); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestInvalidUTF8AndErrorsAreBounded(t *testing.T) {
	r := NewRegistry()
	f := fixture("bytes")
	f.run = func(context.Context, Call) (Result, error) {
		return Result{Content: strings.Repeat("\xff", MaxOutputBytes+1)}, nil
	}
	h := register(t, r, f)
	result, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`), Timeout: time.Second})
	if err != nil || !result.Truncated || !utf8.ValidString(result.Content) {
		t.Fatal(result, err)
	}
	cause := errors.New(strings.Repeat("x", 10000))
	f = fixture("error")
	f.run = func(context.Context, Call) (Result, error) { return Result{}, cause }
	h = register(t, r, f)
	_, err = h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)})
	if err == nil || len(err.Error()) > 4096 || !errors.Is(err, cause) {
		t.Fatal("error lost bound or cause")
	}
	f = fixture("panicerror")
	f.run = func(context.Context, Call) (Result, error) { return Result{}, panicError{} }
	h = register(t, r, f)
	if _, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)}); !errors.Is(err, ErrToolPanic) {
		t.Fatal(err)
	}
	f = fixture("hugekey")
	h = register(t, r, f)
	args := json.RawMessage(`{"` + strings.Repeat("x", 10000) + `":true}`)
	if _, err := h.Execute(context.Background(), Call{Arguments: args}); !errors.Is(err, ErrInvalidArguments) || len(err.Error()) > 4096 {
		t.Fatal("validation error not bounded")
	}
}
