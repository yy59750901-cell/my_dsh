//go:build darwin || linux

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/tool"
)

// 唯一启动目标是当前 go test 二进制；没有 shell、第三方 MCP 或个人配置。
func TestMCPStdioHelper(t *testing.T) {
	marker := -1
	for i, arg := range os.Args {
		if arg == "go-dsh-stdio-helper" {
			marker = i
			break
		}
	}
	if marker < 0 {
		return
	}
	if marker+1 >= len(os.Args) {
		os.Exit(2)
	}
	mode := os.Args[marker+1]
	if mode == "treechild" {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		ready := os.NewFile(3, "ready")
		_, _ = ready.Write([]byte{1})
		_ = ready.Close()
		<-signals
		os.Exit(0)
	}
	var idle chan os.Signal
	if mode == "ignore" || mode == "no_read" {
		signal.Ignore(syscall.SIGTERM)
		idle = make(chan os.Signal, 1)
		signal.Notify(idle, syscall.SIGUSR1)
	}
	if mode == "no_read" {
		<-idle
		os.Exit(0)
	}
	childPID := 0
	if mode == "tree" {
		exe, _ := os.Executable()
		ready, writeReady, _ := os.Pipe()
		child := exec.Command(exe, "-test.run=^TestMCPStdioHelper$", "--", "go-dsh-stdio-helper", "treechild")
		child.Env = []string{"GORACE=atexit_sleep_ms=0"}
		child.ExtraFiles = []*os.File{writeReady}
		child.Stdout = os.Stdout
		if child.Start() != nil {
			os.Exit(3)
		}
		_ = writeReady.Close()
		var b [1]byte
		if _, err := io.ReadFull(ready, b[:]); err != nil {
			os.Exit(4)
		}
		_ = ready.Close()
		childPID = child.Process.Pid
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		go func() { <-signals; _ = child.Wait(); os.Exit(0) }()
	}
	encoder := json.NewEncoder(os.Stdout)
	send := func(id uint64, result any) {
		_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
	}
	text := func(value string) any {
		return map[string]any{"content": []any{map[string]string{"type": "text", "text": value}}}
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 2*MaxResponseBytes)
	initialized := false
	cancelled := 0
	type delayed struct {
		id    uint64
		value string
	}
	var held *delayed
	for scanner.Scan() {
		var request struct {
			ID     uint64          `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if json.Unmarshal(scanner.Bytes(), &request) != nil {
			os.Exit(5)
		}
		switch request.Method {
		case "initialize":
			version := ProtocolVersion
			if mode == "bad_init" {
				version = "unsupported"
			}
			send(request.ID, map[string]any{"protocolVersion": version, "capabilities": map[string]any{"tools": map[string]any{}}})
		case "notifications/initialized":
			initialized = true
		case "notifications/cancelled":
			cancelled++
			var p struct {
				RequestID uint64 `json:"requestId"`
			}
			_ = json.Unmarshal(request.Params, &p)
			send(p.RequestID, text("late"))
		case "tools/list":
			if !initialized {
				os.Exit(6)
			}
			switch mode {
			case "wrong_id":
				send(request.ID+100, text("wrong"))
				continue
			case "duplicate":
				fmt.Fprintf(os.Stdout, "{\"jsonrpc\":\"2.0\",\"id\":%d,\"id\":%d,\"result\":{}}\n", request.ID, request.ID)
				continue
			case "malformed":
				fmt.Fprintln(os.Stdout, `{"jsonrpc":`)
				continue
			case "huge":
				fmt.Fprintln(os.Stdout, strings.Repeat("x", MaxResponseBytes+1))
				continue
			case "server_request":
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "method": "sampling/createMessage", "params": map[string]any{}})
				continue
			case "rpc_error":
				_ = encoder.Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "error": map[string]any{"code": -32603, "message": "SECRET"}})
				continue
			}
			var p struct {
				Cursor string `json:"cursor"`
			}
			_ = json.Unmarshal(request.Params, &p)
			name := "remote"
			cursor := ""
			if mode == "pages" || mode == "loop" {
				if p.Cursor == "" {
					cursor = "next"
				} else {
					name = "second"
				}
				if mode == "loop" {
					cursor = "next"
				}
			}
			send(request.ID, map[string]any{"tools": []any{map[string]any{"name": name, "inputSchema": map[string]any{"type": "object"}}}, "nextCursor": cursor})
		case "tools/call":
			var p struct {
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(request.Params, &p)
			op, _ := p.Arguments["op"].(string)
			switch op {
			case "block":
				continue
			case "cancel_seen":
				send(request.ID, text(strconv.Itoa(cancelled)))
				continue
			case "pid":
				send(request.ID, text(strconv.Itoa(childPID)))
				continue
			case "environment":
				send(request.ID, text(os.Getenv("DSH_ALLOWED")+"|"+os.Getenv("DSH_FORBIDDEN")))
				continue
			case "args":
				send(request.ID, text(strings.Join(os.Args[marker+2:], "|")))
				continue
			case "fail":
				send(request.ID, map[string]any{"isError": true, "content": []any{map[string]string{"type": "text", "text": "SECRET"}}})
				continue
			}
			value, _ := p.Arguments["value"].(string)
			if mode == "reverse" {
				if held == nil {
					held = &delayed{id: request.ID, value: value}
				} else {
					send(request.ID, text(value))
					send(held.id, text(held.value))
					held = nil
				}
			} else {
				send(request.ID, text(value))
			}
		}
	}
	if mode == "ignore" {
		<-idle
	}
	if mode == "tree" {
		select {}
	}
	os.Exit(0)
}

func stdioHelperConfig(t *testing.T, mode string) StdioConfig {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GO_DSH_STDIO_TEST_GORACE", "atexit_sleep_ms=0")
	return StdioConfig{TrustedOperatorOnly: true, Executable: exe, Args: []string{"-test.run=^TestMCPStdioHelper$", "--", "go-dsh-stdio-helper", mode}, EnvRefs: map[string]string{"GORACE": "GO_DSH_STDIO_TEST_GORACE"}, Timeout: 2 * time.Second, CloseGrace: 50 * time.Millisecond}
}
func newStdioHelper(t *testing.T, mode string) *StdioClient {
	t.Helper()
	c, err := NewStdioClient(stdioHelperConfig(t, mode))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}
func initializedStdio(t *testing.T, mode string) *StdioClient {
	t.Helper()
	c := newStdioHelper(t, mode)
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestStdioExplicitOptInAndArgumentsEnvironment(t *testing.T) {
	if _, err := NewStdioClient(StdioConfig{}); !errors.Is(err, ErrStdioTrust) {
		t.Fatal(err)
	}
	for _, config := range []StdioConfig{
		{TrustedOperatorOnly: true, Executable: "relative"},
		{TrustedOperatorOnly: true, Executable: "/no-such-go-dsh-helper"},
		{TrustedOperatorOnly: true, Executable: "/invalid\x00"},
		{TrustedOperatorOnly: true, Executable: "/unused", Args: []string{"\x00"}},
	} {
		if _, err := NewStdioClient(config); err == nil {
			t.Fatal("非法配置被接受")
		}
	}
	config := stdioHelperConfig(t, "normal")
	config.Args = append(config.Args, "$HOME", "$(literal)", "semi;colon", "space value")
	t.Setenv("GO_DSH_TEST_ALLOWED", "allowed")
	t.Setenv("DSH_FORBIDDEN", "SECRET")
	config.EnvRefs["DSH_ALLOWED"] = "GO_DSH_TEST_ALLOWED"
	c, err := NewStdioClient(config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ListTools(context.Background()); !errors.Is(err, ErrNotInitialized) {
		t.Fatal(err)
	}
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	for op, want := range map[string]string{"args": "$HOME|$(literal)|semi;colon|space value", "environment": "allowed|"} {
		raw, _ := json.Marshal(map[string]string{"op": op})
		r, err := c.CallTool(context.Background(), "remote", raw)
		if err != nil || r.Content != want {
			t.Fatal(r, err)
		}
	}
}

func TestStdioInitializeToolsBindingAndPagination(t *testing.T) {
	c := newStdioHelper(t, "normal")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Initialize(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	tools, err := c.Tools(context.Background())
	if err != nil || len(tools) != 1 || !tools[0].Definition().RequiresApproval {
		t.Fatal(tools, err)
	}
	r, err := tools[0].Execute(context.Background(), tool.Call{ID: "call-id", Arguments: json.RawMessage(`{"value":"ok"}`)})
	if err != nil || r.CallID != "call-id" || r.Content != "ok" {
		t.Fatal(r, err)
	}
	for _, raw := range []string{`{"a":1,"a":2}`, `[]`, `{} {}`} {
		if _, err := c.CallTool(context.Background(), "remote", json.RawMessage(raw)); err == nil {
			t.Fatal("接受非法参数")
		}
	}
	pages := initializedStdio(t, "pages")
	definitions, err := pages.ListTools(context.Background())
	if err != nil || len(definitions) != 2 {
		t.Fatal(definitions, err)
	}
	loop := initializedStdio(t, "loop")
	if _, err := loop.ListTools(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestStdioConcurrentOutOfOrderReplies(t *testing.T) {
	c := initializedStdio(t, "reverse")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := fmt.Sprintf("value%d", i)
			args, _ := json.Marshal(map[string]string{"value": value})
			r, err := c.CallTool(context.Background(), "remote", args)
			if err != nil || r.Content != value {
				t.Error(r, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestStdioCancelNotificationAndLateReply(t *testing.T) {
	c := initializedStdio(t, "normal")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := c.CallTool(ctx, "remote", json.RawMessage(`{"op":"block"}`))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	result, err := c.CallTool(context.Background(), "remote", json.RawMessage(`{"op":"cancel_seen"}`))
	if err != nil || result.Content != "1" {
		t.Fatal(result, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := c.CallTool(ctx, "remote", json.RawMessage(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if r, err := c.CallTool(context.Background(), "remote", json.RawMessage(`{"op":"fail"}`)); !errors.Is(err, ErrRemoteTool) || strings.Contains(r.Content, "SECRET") {
		t.Fatal(r, err)
	}
}

func TestStdioRejectsMalformedAndUnsafeResponses(t *testing.T) {
	for _, mode := range []string{"wrong_id", "duplicate", "malformed", "huge", "server_request", "rpc_error"} {
		t.Run(mode, func(t *testing.T) {
			c := initializedStdio(t, mode)
			if _, err := c.ListTools(context.Background()); err == nil || strings.Contains(err.Error(), "SECRET") {
				t.Fatal(err)
			}
		})
	}
	c := newStdioHelper(t, "bad_init")
	if err := c.Initialize(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestStdioCloseKillsProcessGroupAndUnblocksRequests(t *testing.T) {
	c := initializedStdio(t, "tree")
	r, err := c.CallTool(context.Background(), "remote", json.RawMessage(`{"op":"pid"}`))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(r.Content)
	if err != nil || pid <= 0 {
		t.Fatal(r, err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.CallTool(context.Background(), "remote", json.RawMessage(`{"op":"block"}`))
		done <- err
	}()
	var wg sync.WaitGroup
	start := time.Now()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if time.Since(start) > 2*time.Second {
		t.Fatal("Close 超出界限")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("关闭后请求成功")
		}
	case <-time.After(time.Second):
		t.Fatal("在途请求未结束")
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("同组子进程未被回收: %d %v", pid, err)
	}
	if _, err := c.ListTools(context.Background()); err == nil {
		t.Fatal("关闭后仍可调用")
	}
}

func TestStdioKillEscalationAndBlockedWrite(t *testing.T) {
	c := initializedStdio(t, "ignore")
	start := time.Now()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("未有界结束忽略 TERM 的进程")
	}
	if err := syscall.Kill(c.cmd.Process.Pid, 0); err != syscall.ESRCH {
		t.Fatal("父进程未回收", err)
	}
	blocked := newStdioHelper(t, "no_read")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	_, err := blocked.request(ctx, "initialize", map[string]string{"data": strings.Repeat("x", 512<<10)})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := blocked.Close(); err != nil {
		t.Fatal(err)
	}
}
