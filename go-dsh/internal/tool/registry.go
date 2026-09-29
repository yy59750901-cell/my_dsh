package tool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	DefaultTimeout          = 30 * time.Second
	MaxTimeout              = 2 * time.Minute
	MaxArgumentBytes        = 1 << 20
	MaxOutputBytes          = 1 << 20
	MaxConcurrentExecutions = 64
)

var (
	ErrNotFound         = errors.New("tool not found")
	ErrInvalidArguments = errors.New("invalid tool arguments")
	ErrStaleDefinition  = errors.New("tool definition mismatch")
	ErrToolPanic        = errors.New("tool panicked")
	ErrBusy             = errors.New("tool execution limit reached")
	namePattern         = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$`)
	versionPattern      = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._+\-]{0,63}$`)
)

// Registry 的零值可用。重注册替换当前映射，不改变已解析 handle。
// RequiresApproval 是供调用方执行授权的元数据；Registry 不代替审批系统。
type Registry struct {
	mu         sync.RWMutex
	tools      map[string]*handle
	generation uint64
	slots      chan struct{}
}

func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]*handle), slots: make(chan struct{}, MaxConcurrentExecutions)}
}

func cloneDefinition(d Definition) Definition {
	d.InputSchema = append([]byte(nil), d.InputSchema...)
	d.OutputSchema = append([]byte(nil), d.OutputSchema...)
	d.RequiredCapabilities = append([]string(nil), d.RequiredCapabilities...)
	return d
}

// Register 为空版本设置 "1"，保留合法显式版本；Generation 由注册表单调分配，忽略调用方值。
// 注册后调用方须保持该 Tool 实现不变；注册表冻结元数据和路由，不复制实现内部的可变状态。
func (r *Registry) Register(t Tool) (err error) {
	if t == nil {
		return errors.New("nil tool")
	}
	v := reflect.ValueOf(t)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return errors.New("nil tool")
		}
	}
	defer func() {
		if recover() != nil {
			err = ErrToolPanic
		}
	}()
	d := cloneDefinition(t.Definition())
	// MCP 等扩展可返回已固定 handle；重注册保留其快照，但不能叠加两套 generation 校验。
	if resolved, ok := t.(*handle); ok {
		t = resolved.tool
	}
	if !namePattern.MatchString(d.Name) {
		return errors.New("invalid tool name")
	}
	if d.Version == "" {
		d.Version = "1"
	}
	if !versionPattern.MatchString(d.Version) {
		return errors.New("invalid tool version")
	}
	input, err := compileSchema(d.InputSchema)
	if err != nil {
		return fmt.Errorf("input schema: %w", err)
	}
	var output *schema
	if len(d.OutputSchema) != 0 {
		output, err = compileSchema(d.OutputSchema)
		if err != nil {
			return fmt.Errorf("output schema: %w", err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.generation == ^uint64(0) {
		return errors.New("generation exhausted")
	}
	if r.tools == nil {
		r.tools = make(map[string]*handle)
	}
	if r.slots == nil {
		r.slots = make(chan struct{}, MaxConcurrentExecutions)
	}
	r.generation++
	d.Generation = r.generation
	r.tools[d.Name] = &handle{tool: t, definition: d, input: input, output: output, slots: r.slots}
	return nil
}

func (r *Registry) Unregister(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.tools[name]
	delete(r.tools, name)
	return ok
}

func (r *Registry) Definitions() []Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Definition, 0, len(r.tools))
	for _, h := range r.tools {
		out = append(out, cloneDefinition(h.definition))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *Registry) Resolve(name string) (Tool, error) {
	if !namePattern.MatchString(name) {
		return nil, ErrNotFound
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return h, nil
}

func (r *Registry) Execute(ctx context.Context, call Call) (Result, error) {
	t, err := r.Resolve(call.Name)
	if err != nil {
		return Result{CallID: call.ID, Err: err}, err
	}
	return t.Execute(ctx, call)
}

type handle struct {
	tool          Tool
	definition    Definition
	input, output *schema
	slots         chan struct{}
}

func (h *handle) Definition() Definition { return cloneDefinition(h.definition) }

type cappedError struct {
	cause error
	text  string
}

func (e *cappedError) Error() string { return e.text }
func (e *cappedError) Unwrap() error { return e.cause }

func (h *handle) Execute(ctx context.Context, call Call) (result Result, err error) {
	start := time.Now()
	defer func() {
		result.CallID = call.ID
		result.Duration = time.Since(start)
		result.Err = err
	}()
	if ctx == nil {
		return Result{}, errors.New("nil context")
	}
	if call.Name != "" && call.Name != h.definition.Name ||
		call.DefinitionVersion != "" && call.DefinitionVersion != h.definition.Version ||
		call.Generation != 0 && call.Generation != h.definition.Generation {
		return Result{}, ErrStaleDefinition
	}
	if call.Timeout < 0 {
		return Result{}, errors.New("negative timeout")
	}
	timeout := call.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout > MaxTimeout {
		timeout = MaxTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if len(call.Arguments) > MaxArgumentBytes {
		return Result{}, fmt.Errorf("%w: too large", ErrInvalidArguments)
	}
	call.Arguments = append([]byte(nil), call.Arguments...)
	call.Name, call.DefinitionVersion, call.Generation = h.definition.Name, h.definition.Version, h.definition.Generation
	call.Timeout = timeout
	select {
	case h.slots <- struct{}{}:
	default:
		return Result{}, ErrBusy
	}
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	// 不合作的工具超时后仍占用槽位，避免无限创建 goroutine；context 不能撤销副作用。
	go func() {
		var o outcome
		defer func() {
			if recover() != nil {
				o = outcome{err: ErrToolPanic}
			}
			<-h.slots
			done <- o
		}()
		defer func() {
			if o.err != nil {
				text := o.err.Error()
				if len(text) > 4096 {
					o.err = &cappedError{cause: o.err, text: strings.ToValidUTF8(text[:4096], "")}
				}
			}
		}()
		args, validationErr := decodeJSON(call.Arguments)
		if validationErr == nil {
			validationErr = h.input.validate(args, "$")
		}
		if validationErr != nil {
			o.err = fmt.Errorf("%w: %v", ErrInvalidArguments, validationErr)
			return
		}
		if o.err = ctx.Err(); o.err != nil {
			return
		}
		o.result, o.err = h.tool.Execute(ctx, call)
		if o.err == nil {
			o.err = o.result.Err
		}
		if len(o.result.Structured) > MaxOutputBytes {
			o.result.Structured = nil
			o.result.Truncated = true
		} else if len(o.result.Structured) > 0 {
			value, e := decodeJSON(o.result.Structured)
			if e == nil && h.output != nil {
				e = h.output.validate(value, "$")
			}
			if e != nil {
				o.result.Structured = nil
				o.err = errors.New("invalid structured output")
			}
		} else if h.output != nil && o.err == nil {
			o.err = errors.New("missing structured output")
		}
		budget := MaxOutputBytes - len(o.result.Structured)
		if len(o.result.Content) > budget {
			o.result.Content = strings.Clone(o.result.Content[:budget])
			o.result.Truncated = true
		}
		o.result.Content = strings.ToValidUTF8(o.result.Content, "")
		o.result.Structured = append([]byte(nil), o.result.Structured...)
	}()
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case o := <-done:
		if ctx.Err() != nil {
			return Result{}, ctx.Err()
		}
		return o.result, o.err
	}
}
