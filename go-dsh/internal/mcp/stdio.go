//go:build darwin || linux

package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/yy59750901/go-dsh/internal/tool"
)

var (
	ErrStdioTrust   = errors.New("MCP stdio requires explicit trusted operator opt-in; no sandbox")
	ErrStdioProcess = errors.New("MCP stdio process failed")
	ErrStdioClose   = errors.New("MCP stdio process cleanup incomplete")
	stdioEnvName    = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,127}$`)
)

// StdioConfig 只能由可信操作员提供，不能来自模型、远端元数据或个人 MCP 配置。
// 无沙箱：二进制拥有当前用户权限。默认拒绝启动；绝对路径及 args 原样传给 exec，
// 不经过 shell，不展开变量。仅继承 EnvRefs 显式引用的 ENV，不存储其值。
// POSIX 进程组清理不限制恶意进程 setsid/逃逸；不适用于不可信程序。
type StdioConfig struct {
	TrustedOperatorOnly bool
	Executable          string
	Args                []string
	EnvRefs             map[string]string
	Timeout             time.Duration
	CloseGrace          time.Duration
}

type stdioReply struct {
	raw json.RawMessage
	err error
}
type stdioWrite struct {
	ctx  context.Context
	data []byte
	done chan error
}
type StdioClient struct {
	cmd                                                    *exec.Cmd
	input, output                                          *os.File
	timeout, grace                                         time.Duration
	mu                                                     sync.Mutex
	nextID                                                 uint64
	pending                                                map[uint64]chan stdioReply
	cancelled                                              map[uint64]bool
	initialized                                            bool
	terminal                                               error
	closeErr                                               error
	initGate                                               chan struct{}
	writes                                                 chan stdioWrite
	done, cleanupDone, processDone, readerDone, writerDone chan struct{}
	stopOnce                                               sync.Once
}

func NewStdioClient(config StdioConfig) (*StdioClient, error) {
	if !config.TrustedOperatorOnly {
		return nil, ErrStdioTrust
	}
	if !filepath.IsAbs(config.Executable) || strings.ContainsRune(config.Executable, 0) || config.Timeout < 0 || config.Timeout > 2*time.Minute || config.CloseGrace < 0 || config.CloseGrace > time.Second {
		return nil, ErrStdioProcess
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	if config.CloseGrace == 0 {
		config.CloseGrace = 100 * time.Millisecond
	}
	size := len(config.Executable)
	for _, arg := range config.Args {
		size += len(arg)
		if strings.ContainsRune(arg, 0) {
			return nil, ErrStdioProcess
		}
	}
	if size > MaxResponseBytes || len(config.Args) > 1024 || len(config.EnvRefs) > 128 {
		return nil, ErrResponseLimit
	}
	env := make([]string, 0, len(config.EnvRefs))
	for name, ref := range config.EnvRefs {
		if !stdioEnvName.MatchString(name) || !stdioEnvName.MatchString(ref) {
			return nil, ErrStdioProcess
		}
		value, ok := os.LookupEnv(ref)
		if !ok || strings.ContainsRune(value, 0) {
			return nil, ErrStdioProcess
		}
		size += len(name) + len(value)
		if size > MaxResponseBytes {
			return nil, ErrResponseLimit
		}
		env = append(env, name+"="+value)
	}
	childIn, input, err := os.Pipe()
	if err != nil {
		return nil, ErrStdioProcess
	}
	output, childOut, err := os.Pipe()
	if err != nil {
		_ = childIn.Close()
		_ = input.Close()
		return nil, ErrStdioProcess
	}
	cmd := exec.Command(config.Executable, append([]string(nil), config.Args...)...)
	cmd.Env = env
	cmd.Stdin, cmd.Stdout = childIn, childOut
	// stderr 默认丢弃，不回传可能包含凭据的进程诊断。
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	err = cmd.Start()
	_ = childIn.Close()
	_ = childOut.Close()
	// exec 已复制环境到子进程；父客户端不持有已解析凭据。
	cmd.Env = nil
	if err != nil {
		_ = input.Close()
		_ = output.Close()
		return nil, ErrStdioProcess
	}
	c := &StdioClient{cmd: cmd, input: input, output: output, timeout: config.Timeout, grace: config.CloseGrace, pending: make(map[uint64]chan stdioReply), cancelled: make(map[uint64]bool), initGate: make(chan struct{}, 1), writes: make(chan stdioWrite, 64), done: make(chan struct{}), cleanupDone: make(chan struct{}), processDone: make(chan struct{}), readerDone: make(chan struct{}), writerDone: make(chan struct{})}
	go c.readLoop()
	go c.writeLoop()
	go func() { _ = cmd.Wait(); close(c.processDone); c.stop(ErrStdioProcess) }()
	return c, nil
}

func (c *StdioClient) stoppedError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	return ErrClosed
}
func (c *StdioClient) stop(reason error) {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.terminal = reason
		c.pending = make(map[uint64]chan stdioReply)
		c.cancelled = make(map[uint64]bool)
		c.mu.Unlock()
		close(c.done)
		_ = c.input.Close()
		_ = c.output.Close()
		go func() {
			defer close(c.cleanupDone)
			// 即使父进程已经退出，也必须终止仍持有管道的同组子进程。
			_ = syscall.Kill(-c.cmd.Process.Pid, syscall.SIGTERM)
			timer := time.NewTimer(c.grace)
			<-timer.C
			killErr := syscall.Kill(-c.cmd.Process.Pid, syscall.SIGKILL)
			// 以最终终止结果为准：Darwin 对正在退出的组发送 TERM 可返回 EPERM，
			// 后续 ESRCH 已确认该组不存在，不应误报清理失败。
			failed := killErr != nil && killErr != syscall.ESRCH
			bound := time.NewTimer(time.Second)
			defer bound.Stop()
			for _, done := range []<-chan struct{}{c.processDone, c.readerDone, c.writerDone} {
				select {
				case <-done:
				case <-bound.C:
					failed = true
					c.mu.Lock()
					c.closeErr = ErrStdioClose
					c.mu.Unlock()
					return
				}
			}
			if failed {
				c.mu.Lock()
				c.closeErr = ErrStdioClose
				c.mu.Unlock()
			}
		}()
	})
}

// Close 有界终止进程组并回收父进程及 I/O goroutine；不会等待服务器合作。
func (c *StdioClient) Close() error {
	c.stop(ErrClosed)
	<-c.cleanupDone
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}
func (c *StdioClient) writeLoop() {
	defer close(c.writerDone)
	for {
		select {
		case <-c.done:
			return
		case job := <-c.writes:
			if err := job.ctx.Err(); err != nil {
				job.done <- err
				continue
			}
			deadline := time.Now().Add(c.timeout)
			if d, ok := job.ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
			if err := c.input.SetWriteDeadline(deadline); err != nil {
				job.done <- ErrStdioProcess
				c.stop(ErrStdioProcess)
				return
			}
			_, err := c.input.Write(job.data)
			if err != nil {
				safe := ErrStdioProcess
				if errors.Is(err, os.ErrDeadlineExceeded) {
					safe = context.DeadlineExceeded
				}
				job.done <- safe
				c.stop(safe)
				return
			}
			job.done <- nil
		}
	}
}
func (c *StdioClient) send(ctx context.Context, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return ErrProtocol
	}
	if len(data) > MaxResponseBytes {
		return ErrResponseLimit
	}
	data = append(data, '\n')
	job := stdioWrite{ctx: ctx, data: data, done: make(chan error, 1)}
	select {
	case <-c.done:
		return c.stoppedError()
	case <-ctx.Done():
		return ctx.Err()
	case c.writes <- job:
	}
	select {
	case err := <-job.done:
		return err
	case <-c.done:
		return c.stoppedError()
	case <-ctx.Done():
		// 写入可能只完成了一部分，关闭会话而非继续发送损坏的 JSONL。
		c.stop(ErrClosed)
		return ctx.Err()
	}
}

func (c *StdioClient) readLoop() {
	defer close(c.readerDone)
	scanner := bufio.NewScanner(c.output)
	scanner.Buffer(make([]byte, 4096), MaxResponseBytes+2)
	for scanner.Scan() {
		raw := scanner.Bytes()
		if len(raw) > MaxResponseBytes {
			c.stop(ErrResponseLimit)
			return
		}
		if !validStdioJSON(raw) {
			c.stop(ErrProtocol)
			return
		}
		var e envelope
		if json.Unmarshal(raw, &e) != nil || e.JSONRPC != "2.0" {
			c.stop(ErrProtocol)
			return
		}
		if len(e.ID) == 0 && strings.HasPrefix(e.Method, "notifications/") && e.Result == nil && e.Error == nil {
			continue
		}
		id, err := strconv.ParseUint(string(e.ID), 10, 64)
		if err != nil || id == 0 || e.Method != "" {
			c.stop(ErrProtocol)
			return
		}
		result, matched, rpcErr := decodeEnvelope(raw, id)
		if rpcErr == nil && !matched {
			c.stop(ErrProtocol)
			return
		}
		if errors.Is(rpcErr, ErrProtocol) {
			c.stop(ErrProtocol)
			return
		}
		c.mu.Lock()
		receiver, ok := c.pending[id]
		if ok {
			delete(c.pending, id)
		}
		cancelled := c.cancelled[id]
		if cancelled {
			delete(c.cancelled, id)
		}
		c.mu.Unlock()
		if cancelled {
			continue
		}
		if !ok {
			c.stop(ErrProtocol)
			return
		}
		receiver <- stdioReply{raw: result, err: rpcErr}
	}
	if scanner.Err() != nil {
		c.stop(ErrResponseLimit)
	} else {
		c.stop(ErrStdioProcess)
	}
}

func (c *StdioClient) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if ctx == nil {
		return nil, ErrProtocol
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.terminal != nil {
		err := c.terminal
		c.mu.Unlock()
		return nil, err
	}
	if len(c.pending) >= 64 || c.nextID == ^uint64(0) {
		c.mu.Unlock()
		return nil, ErrResponseLimit
	}
	c.nextID++
	id := c.nextID
	reply := make(chan stdioReply, 1)
	c.pending[id] = reply
	c.mu.Unlock()
	if err := c.send(ctx, rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case r := <-reply:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return r.raw, r.err
	case <-c.done:
		return nil, c.stoppedError()
	case <-ctx.Done():
		c.mu.Lock()
		_, pending := c.pending[id]
		delete(c.pending, id)
		overflow := len(c.cancelled) >= 128
		if pending && !overflow {
			c.cancelled[id] = true
		}
		c.mu.Unlock()
		if overflow {
			c.stop(ErrResponseLimit)
		} else if pending {
			data, _ := json.Marshal(rpcRequest{JSONRPC: "2.0", Method: "notifications/cancelled", Params: map[string]any{"requestId": id}})
			// 通知不阻塞调用方；写管道阻塞最终由后续超时或 Close 中断。
			select {
			case c.writes <- stdioWrite{ctx: context.Background(), data: append(data, '\n'), done: make(chan error, 1)}:
			default:
				c.stop(ErrResponseLimit)
			}
		}
		return nil, ctx.Err()
	}
}
func (c *StdioClient) state() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	if !c.initialized {
		return ErrNotInitialized
	}
	return nil
}
func (c *StdioClient) Initialize(ctx context.Context) error {
	if ctx == nil {
		return ErrProtocol
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.initGate <- struct{}{}:
		defer func() { <-c.initGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.stoppedError()
	}
	if err := c.state(); err == nil {
		return nil
	} else if err != ErrNotInitialized {
		return err
	}
	raw, err := c.request(ctx, "initialize", map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "go-dsh", "version": "1"}})
	if err != nil {
		c.stop(err)
		return err
	}
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
		Capabilities    struct {
			Tools *struct{} `json:"tools"`
		} `json:"capabilities"`
	}
	if json.Unmarshal(raw, &result) != nil || !supportedVersion(result.ProtocolVersion) || result.Capabilities.Tools == nil {
		c.stop(ErrProtocol)
		return ErrProtocol
	}
	if err = c.send(ctx, rpcRequest{JSONRPC: "2.0", Method: "notifications/initialized"}); err != nil {
		c.stop(err)
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminal != nil {
		return c.terminal
	}
	c.initialized = true
	return nil
}
func (c *StdioClient) rpc(ctx context.Context, method string, params any) (json.RawMessage, error) {
	if err := c.state(); err != nil {
		return nil, err
	}
	return c.request(ctx, method, params)
}
func (c *StdioClient) ListTools(ctx context.Context) ([]ToolDefinition, error) {
	if ctx == nil {
		return nil, ErrProtocol
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	out := make([]ToolDefinition, 0)
	names, cursors := map[string]bool{}, map[string]bool{}
	cursor := ""
	for range MaxPages {
		params := map[string]string{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		raw, err := c.rpc(ctx, "tools/list", params)
		if err != nil {
			return nil, err
		}
		var page struct {
			Tools      []ToolDefinition `json:"tools"`
			NextCursor string           `json:"nextCursor"`
		}
		if json.Unmarshal(raw, &page) != nil || page.Tools == nil {
			return nil, ErrProtocol
		}
		for _, d := range page.Tools {
			if d.Name == "" || len(d.Name) > 128 || names[d.Name] || len(out) >= MaxTools {
				return nil, ErrProtocol
			}
			names[d.Name] = true
			out = append(out, d)
		}
		if page.NextCursor == "" {
			return out, nil
		}
		if len(page.NextCursor) > 4096 || cursors[page.NextCursor] {
			return nil, ErrProtocol
		}
		cursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
	return nil, ErrResponseLimit
}
func (c *StdioClient) CallTool(ctx context.Context, name string, arguments json.RawMessage) (tool.Result, error) {
	if name == "" || len(name) > 128 || len(arguments) > tool.MaxArgumentBytes || !validStdioJSON(arguments) || len(bytes.TrimSpace(arguments)) == 0 || bytes.TrimSpace(arguments)[0] != '{' {
		return tool.Result{}, tool.ErrInvalidArguments
	}
	raw, err := c.rpc(ctx, "tools/call", struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}{name, arguments})
	if err != nil {
		return tool.Result{}, err
	}
	var response struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		IsError           bool            `json:"isError"`
	}
	if json.Unmarshal(raw, &response) != nil || response.Content == nil {
		return tool.Result{}, ErrProtocol
	}
	if response.IsError {
		return tool.Result{}, ErrRemoteTool
	}
	texts := make([]string, 0, len(response.Content))
	for _, part := range response.Content {
		if part.Type != "text" {
			return tool.Result{}, ErrProtocol
		}
		texts = append(texts, part.Text)
	}
	result := tool.Result{Content: strings.Join(texts, "\n"), Structured: response.StructuredContent}
	if len(result.Content)+len(result.Structured) > tool.MaxOutputBytes {
		return tool.Result{}, ErrResponseLimit
	}
	return result, nil
}
func (c *StdioClient) Tools(ctx context.Context) ([]tool.Tool, error) {
	definitions, err := c.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	registry := tool.NewRegistry()
	out := make([]tool.Tool, 0, len(definitions))
	for _, d := range definitions {
		if err := registry.Register(&stdioRemoteTool{client: c, definition: d}); err != nil {
			return nil, ErrProtocol
		}
		h, err := registry.Resolve(d.Name)
		if err != nil {
			return nil, ErrProtocol
		}
		out = append(out, h)
	}
	return out, nil
}

type stdioRemoteTool struct {
	client     *StdioClient
	definition ToolDefinition
}

func (t *stdioRemoteTool) Definition() tool.Definition {
	return tool.Definition{Name: t.definition.Name, Description: t.definition.Description, Version: "1", InputSchema: append(json.RawMessage(nil), t.definition.InputSchema...), OutputSchema: append(json.RawMessage(nil), t.definition.OutputSchema...), RequiresApproval: true}
}
func (t *stdioRemoteTool) Execute(ctx context.Context, call tool.Call) (tool.Result, error) {
	result, err := t.client.CallTool(ctx, t.definition.Name, call.Arguments)
	result.CallID = call.ID
	result.Err = err
	return result, err
}

func validStdioJSON(raw []byte) bool {
	if len(raw) == 0 || len(raw) > MaxResponseBytes || !utf8.Valid(raw) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	nodes := 0
	var read func(int) bool
	read = func(depth int) bool {
		nodes++
		if depth > 64 || nodes > 20000 {
			return false
		}
		t, err := d.Token()
		if err != nil {
			return false
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return true
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return false
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return false
				}
				seen[s] = true
				if !read(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim('}')
		case '[':
			for d.More() {
				if !read(depth + 1) {
					return false
				}
			}
			end, err := d.Token()
			return err == nil && end == json.Delim(']')
		}
		return false
	}
	if !read(0) {
		return false
	}
	_, err := d.Token()
	return err == io.EOF
}
