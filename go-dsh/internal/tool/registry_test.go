package tool

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

type testTool struct {
	def Definition
	run func(context.Context, Call) (Result, error)
}

func (t *testTool) Definition() Definition { return t.def }
func (t *testTool) Execute(ctx context.Context, c Call) (Result, error) {
	if t.run != nil {
		return t.run(ctx, c)
	}
	return Result{Content: t.def.Description}, nil
}
func fixture(name string) *testTool {
	return &testTool{def: Definition{Name: name, InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false}`)}}
}
func register(t *testing.T, r *Registry, tool Tool) Tool {
	t.Helper()
	if err := r.Register(tool); err != nil {
		t.Fatal(err)
	}
	h, err := r.Resolve(tool.Definition().Name)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestRegistryContractAndImmutableGeneration(t *testing.T) {
	var r Registry
	first := fixture("sample")
	first.def.Description = "old"
	first.def.Generation = 900
	first.def.RequiredCapabilities = []string{"read"}
	old := register(t, &r, first)
	original := old.Definition()
	if original.Version != "1" || original.Generation != 1 {
		t.Fatal(original)
	}
	first.def.InputSchema[0] = '!'
	first.def.RequiredCapabilities[0] = "mutated"
	d := old.Definition()
	d.InputSchema[0] = '!'
	d.RequiredCapabilities[0] = "changed"
	if old.Definition().InputSchema[0] != '{' || old.Definition().RequiredCapabilities[0] != "read" {
		t.Fatal("definition aliases caller storage")
	}
	second := fixture("sample")
	second.def.Description = "new"
	second.def.Version = "2.0.0"
	current := register(t, &r, second)
	if current.Definition().Generation <= original.Generation {
		t.Fatal("generation did not advance")
	}
	got, err := old.Execute(context.Background(), Call{ID: "call", Name: "sample", Arguments: json.RawMessage(`{}`), Generation: original.Generation, DefinitionVersion: "1"})
	if err != nil || got.Content != "old" || got.CallID != "call" {
		t.Fatalf("%+v %v", got, err)
	}
	_, err = current.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`), Generation: original.Generation})
	if !errors.Is(err, ErrStaleDefinition) {
		t.Fatal(err)
	}
	if !r.Unregister("sample") || r.Unregister("sample") {
		t.Fatal("unregister result")
	}
	if _, err := r.Resolve("sample"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := old.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	again := register(t, &r, fixture("sample"))
	if again.Definition().Generation <= current.Definition().Generation {
		t.Fatal("generation reused")
	}
	register(t, &r, fixture("aaa"))
	defs := r.Definitions()
	if len(defs) != 2 || defs[0].Name != "aaa" {
		t.Fatal(defs)
	}
	defs[1].InputSchema[0] = '!'
	if again.Definition().InputSchema[0] != '{' {
		t.Fatal("Definitions aliases storage")
	}
}

func TestSchemaSupportedSubset(t *testing.T) {
	s := `{"type":"object","properties":{"n":{"type":"integer","minimum":1,"maximum":3},"s":{"type":"string","minLength":1,"maxLength":2,"enum":["中","ab"]},"a":{"type":"array","items":{"type":"number","exclusiveMinimum":0,"exclusiveMaximum":2},"minItems":1,"maxItems":2},"o":{"type":"object","minProperties":1,"maxProperties":2,"additionalProperties":{"type":"boolean"}}},"required":["n","s","a","o"],"additionalProperties":false}`
	r := NewRegistry()
	f := fixture("validate")
	f.def.InputSchema = json.RawMessage(s)
	h := register(t, r, f)
	cases := []struct {
		json  string
		valid bool
	}{
		{`{"n":1,"s":"中","a":[1.5],"o":{"x":true}}`, true},
		{`{"n":3.0,"s":"ab","a":[1,0.1],"o":{"x":false}}`, true},
		{`{"n":0,"s":"ab","a":[1],"o":{"x":true}}`, false},
		{`{"n":4,"s":"ab","a":[1],"o":{"x":true}}`, false},
		{`{"n":1.1,"s":"ab","a":[1],"o":{"x":true}}`, false},
		{`{"n":"1","s":"ab","a":[1],"o":{"x":true}}`, false},
		{`{"n":1,"s":"cd","a":[1],"o":{"x":true}}`, false},
		{`{"n":1,"s":"abc","a":[1],"o":{"x":true}}`, false},
		{`{"n":1,"s":"ab","a":[],"o":{"x":true}}`, false},
		{`{"n":1,"s":"ab","a":[0],"o":{"x":true}}`, false},
		{`{"n":1,"s":"ab","a":[2],"o":{"x":true}}`, false},
		{`{"n":1,"s":"ab","a":[1,1,1],"o":{"x":true}}`, false},
		{`{"n":1,"s":"ab","a":[1],"o":{}}`, false},
		{`{"n":1,"s":"ab","a":[1],"o":{"x":"true"}}`, false},
		{`{"n":1,"s":"ab","a":[1],"o":{"x":true},"extra":1}`, false},
		{`{"n":1,"s":"ab","a":[1]}`, false},
		{`{"n":1,"n":2,"s":"ab","a":[1],"o":{"x":true}}`, false},
		{`{} {}`, false}, {`[]`, false}, {`null`, false},
	}
	for _, tc := range cases {
		_, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(tc.json)})
		if (err == nil) != tc.valid {
			t.Errorf("%s: %v", tc.json, err)
		}
	}
}

func TestSchemaRejectsUnknownOrMalformedAtRegistration(t *testing.T) {
	bad := []string{
		`{"type":"object","patternProperties":{}}`, `{"$ref":"https://invalid.example/schema"}`, `{"oneOf":[]}`, `{"type":"string","format":"email"}`, `{"properties":{"x":{"pattern":"a"}}}`, `{"items":[]}`, `{"type":"wat"}`, `{"required":"x"}`, `{"required":["a","a"]}`, `{"additionalProperties":4}`, `{"minItems":-1}`, `{"maxLength":1.2}`, `{"minimum":"1"}`, `{"enum":[]}`, `{"enum":[1,1.0]}`, `{"type":"object","type":"string"}`, `{"exclusiveMaximum":true}`, `{"type":[]}`, `{"type":["string","string"]}`, `{"minimum":1e1001}`, `[]`, ``,
	}
	for _, raw := range bad {
		f := fixture("bad")
		f.def.InputSchema = json.RawMessage(raw)
		if err := NewRegistry().Register(f); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	f := fixture("badoutput")
	f.def.OutputSchema = json.RawMessage(`{"unknown":true}`)
	if NewRegistry().Register(f) == nil {
		t.Fatal("unknown output schema accepted")
	}
}

func TestSchemaNumericPrecisionAndUnion(t *testing.T) {
	for _, tc := range []struct {
		s, v  string
		valid bool
	}{
		{`{"type":["null","integer"]}`, `null`, true},
		{`{"type":"integer"}`, `9007199254740992.1`, false},
		{`{"minimum":9007199254740993}`, `9007199254740992`, false},
		{`{"enum":[{"a":1}]}`, `{"a":1.0}`, true},
		{`false`, `{}`, false}, {`true`, `[]`, true},
		{`{"type":"object","properties":{"x":false}}`, `{"x":1}`, false},
	} {
		s, err := compileSchema([]byte(tc.s))
		if err != nil {
			t.Fatal(err)
		}
		v, err := decodeJSON([]byte(tc.v))
		if err != nil {
			t.Fatal(err)
		}
		if (s.validate(v, "$") == nil) != tc.valid {
			t.Errorf("%s / %s", tc.s, tc.v)
		}
	}
	if _, err := decodeJSON([]byte(strings.Repeat("[", 66) + strings.Repeat("]", 66))); err == nil {
		t.Fatal("unbounded nesting")
	}
	if _, err := decodeJSON([]byte(`{"n":1e99999999}`)); err == nil {
		t.Fatal("unbounded exponent")
	}
}

func TestExecutionGuards(t *testing.T) {
	r := NewRegistry()
	f := fixture("panic")
	f.run = func(context.Context, Call) (Result, error) { panic("private panic data") }
	h := register(t, r, f)
	got, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)})
	if !errors.Is(err, ErrToolPanic) || got.Err != err {
		t.Fatal(got, err)
	}
	f = fixture("timeout")
	release := make(chan struct{})
	stopped := make(chan struct{})
	f.run = func(ctx context.Context, c Call) (Result, error) {
		defer close(stopped)
		<-release
		return Result{}, nil
	}
	h = register(t, r, f)
	_, err = h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`), Timeout: 10 * time.Millisecond})
	close(release)
	<-stopped
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Execute(ctx, Call{Arguments: json.RawMessage(`{}`)}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	f = fixture("large")
	f.run = func(context.Context, Call) (Result, error) {
		return Result{Content: strings.Repeat("中", MaxOutputBytes), Structured: json.RawMessage(`{}`)}, nil
	}
	h = register(t, r, f)
	got, err = h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)})
	if err != nil || !got.Truncated || len(got.Content)+len(got.Structured) > MaxOutputBytes || !utf8.ValidString(got.Content) {
		t.Fatal("output not bounded", err)
	}
	f = fixture("structured")
	f.run = func(context.Context, Call) (Result, error) {
		return Result{Structured: json.RawMessage(`"` + strings.Repeat("x", MaxOutputBytes) + `"`)}, nil
	}
	h = register(t, r, f)
	got, err = h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)})
	if err != nil || !got.Truncated || got.Structured != nil {
		t.Fatal("oversize JSON must be dropped", err)
	}
	f = fixture("output")
	f.def.OutputSchema = json.RawMessage(`{"type":"integer"}`)
	f.run = func(context.Context, Call) (Result, error) {
		return Result{Structured: json.RawMessage(`"wrong"`)}, nil
	}
	h = register(t, r, f)
	if got, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)}); err == nil || got.Structured != nil {
		t.Fatal("output schema not checked")
	}
}

func TestExecutionSlotsBoundUncooperativeTools(t *testing.T) {
	r := NewRegistry()
	release := make(chan struct{})
	var wg sync.WaitGroup
	f := fixture("blocked")
	entered := make(chan struct{})
	f.run = func(context.Context, Call) (Result, error) {
		wg.Add(1)
		defer wg.Done()
		entered <- struct{}{}
		<-release
		return Result{}, nil
	}
	h := register(t, r, f)
	defer func() { close(release); wg.Wait() }()
	for range MaxConcurrentExecutions {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { _, err := h.Execute(ctx, Call{Arguments: json.RawMessage(`{}`)}); done <- err }()
		select {
		case <-entered:
		case err := <-done:
			cancel()
			t.Fatal("tool did not start", err)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if _, err := h.Execute(context.Background(), Call{Arguments: json.RawMessage(`{}`)}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
}

func TestRegistryConcurrentAndEcho(t *testing.T) {
	r := NewRegistry()
	register(t, r, NewEchoTool())
	if r.Definitions()[0].RequiresApproval {
		t.Fatal("echo must be read-only")
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				if err := r.Register(NewEchoTool()); err != nil {
					t.Error(err)
				}
				h, err := r.Resolve("echo")
				if err != nil {
					t.Error(err)
					return
				}
				result, err := h.Execute(context.Background(), Call{Name: "echo", Arguments: json.RawMessage(`{"text":"hello"}`)})
				if err != nil || result.Content != "hello" {
					t.Error(result, err)
				}
				_ = r.Definitions()
			}
		}()
	}
	wg.Wait()
	if _, err := r.Execute(context.Background(), Call{Name: "echo", Arguments: json.RawMessage(`{"text":1}`)}); !errors.Is(err, ErrInvalidArguments) {
		t.Fatal(err)
	}
	if _, err := r.Execute(context.Background(), Call{Name: "absent"}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	var nilTool *testTool
	if r.Register(nilTool) == nil || r.Register(nil) == nil {
		t.Fatal("accepted nil tool")
	}
	f := fixture("../invalid")
	if r.Register(f) == nil {
		t.Fatal("accepted invalid name")
	}
	f = fixture("version")
	f.def.Version = "bad version"
	if r.Register(f) == nil {
		t.Fatal("accepted invalid version")
	}
}
