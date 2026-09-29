package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/tool"
)

type incoming struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func reply(w http.ResponseWriter, id uint64, result any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}
func fake(t *testing.T, fn func(http.ResponseWriter, *http.Request, incoming), timeout time.Duration) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/mcp" {
			t.Error("unexpected endpoint", r.URL)
			w.WriteHeader(404)
			return
		}
		if r.Header.Get("Authorization") != "Bearer local-test" {
			t.Error("missing configured authorization")
		}
		if r.Method == http.MethodDelete {
			if r.Header.Get("Mcp-Session-Id") != "session-test" {
				t.Error("missing DELETE session")
			}
			w.WriteHeader(204)
			return
		}
		if r.Header.Get("Accept") != "application/json, text/event-stream" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("missing request negotiation headers")
		}
		var request incoming
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if request.JSONRPC != "2.0" {
			t.Error("invalid JSON-RPC version")
		}
		switch request.Method {
		case "initialize":
			if request.ID == 0 || r.Header.Get("Mcp-Session-Id") != "" {
				t.Error("invalid initialize request")
			}
			var params struct {
				ProtocolVersion string `json:"protocolVersion"`
			}
			_ = json.Unmarshal(request.Params, &params)
			if params.ProtocolVersion != ProtocolVersion {
				t.Error("wrong offered version")
			}
			w.Header().Set("Mcp-Session-Id", "session-test")
			reply(w, request.ID, map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "fake", "version": "1"}})
		case "notifications/initialized":
			if request.ID != 0 || r.Header.Get("Mcp-Session-Id") != "session-test" || r.Header.Get("MCP-Protocol-Version") != ProtocolVersion {
				t.Error("invalid initialized notification")
			}
			w.WriteHeader(202)
		default:
			if r.Header.Get("Mcp-Session-Id") != "session-test" || r.Header.Get("MCP-Protocol-Version") != ProtocolVersion {
				t.Error("missing negotiated headers")
			}
			fn(w, r, request)
		}
	}))
	t.Cleanup(server.Close)
	c, err := NewStreamableHTTPClient(Config{Endpoint: server.URL + "/mcp", Authorization: "Bearer local-test", Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}
func listedTool() map[string]any {
	return map[string]any{"name": "remote_echo", "description": "本地 fake", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"text": map[string]string{"type": "string"}}, "required": []string{"text"}, "additionalProperties": false}, "annotations": map[string]any{"readOnlyHint": true}}
}

func TestStreamableHTTPJSONLifecycleAndTools(t *testing.T) {
	var calls atomic.Int32
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		switch req.Method {
		case "tools/list":
			reply(w, req.ID, map[string]any{"tools": []any{listedTool()}})
		case "tools/call":
			calls.Add(1)
			var params struct {
				Name      string `json:"name"`
				Arguments struct {
					Text string `json:"text"`
				} `json:"arguments"`
			}
			if err := json.Unmarshal(req.Params, &params); err != nil || params.Name != "remote_echo" {
				t.Error(params, err)
			}
			reply(w, req.ID, map[string]any{"content": []any{map[string]string{"type": "text", "text": params.Arguments.Text}, map[string]string{"type": "resource_link", "uri": "http://invalid.example/never-follow"}}, "structuredContent": map[string]bool{"ok": true}})
		default:
			t.Error("unexpected method", req.Method)
			w.WriteHeader(400)
		}
	}, time.Second)
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	tools, err := c.Tools(context.Background())
	if err != nil || len(tools) != 1 {
		t.Fatal(tools, err)
	}
	if !tools[0].Definition().RequiresApproval {
		t.Fatal("trusted remote readOnlyHint")
	}
	result, err := tools[0].Execute(context.Background(), tool.Call{Arguments: json.RawMessage(`{"text":"hello"}`)})
	if err != nil || result.Content != "hello" || string(result.Structured) != `{"ok":true}` {
		t.Fatal(result, err)
	}
	if _, err := tools[0].Execute(context.Background(), tool.Call{Arguments: json.RawMessage(`{"text":1}`)}); !errors.Is(err, tool.ErrInvalidArguments) {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("invalid arguments sent to server")
	}
	registry := tool.NewRegistry()
	if err := registry.Register(tool.NewEchoTool()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tools[0]); err != nil {
		t.Fatal(err)
	}
	result, err = registry.Execute(context.Background(), tool.Call{Name: "remote_echo", Arguments: json.RawMessage(`{"text":"registered"}`)})
	if err != nil || result.Content != "registered" {
		t.Fatal("MCP handle cannot be re-registered", result, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListTools(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestStreamableHTTPSSEProgressMultilineAndEarlyReturn(t *testing.T) {
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		_, _ = io.WriteString(w, ": heartbeat\n\nevent: endpoint\ndata: http://invalid.example/ignored\n\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\n")
		_, _ = fmt.Fprintf(w, "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%d,\ndata: \"result\":{\"content\":[{\"type\":\"text\",\"text\":\"SSE\"}]}}\n\n", req.ID)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}, time.Second)
	result, err := c.CallTool(context.Background(), "echo", json.RawMessage(`{}`))
	if err != nil || result.Content != "SSE" {
		t.Fatal(result, err)
	}
}

func TestStreamableHTTPTimeoutAndCloseCancelInFlight(t *testing.T) {
	entered := make(chan struct{}, 2)
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		entered <- struct{}{}
		<-r.Context().Done()
	}, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.CallTool(ctx, "echo", json.RawMessage(`{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-entered
	done := make(chan error, 1)
	go func() { _, err := c.CallTool(context.Background(), "echo", json.RawMessage(`{}`)); done <- err }()
	<-entered
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("in-flight call succeeded after close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel request")
	}
}

func TestStreamableHTTPRejectsRedirectWithoutFollowing(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1); w.WriteHeader(500) }))
	defer target.Close()
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}, time.Second)
	if _, err := c.ListTools(context.Background()); err == nil {
		t.Fatal("redirect accepted")
	}
	if followed.Load() != 0 {
		t.Fatal("server URL followed")
	}
}

func TestStreamableHTTPPaginationAndFailClosedSchemas(t *testing.T) {
	var page atomic.Int32
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		if page.Add(1) == 1 {
			reply(w, req.ID, map[string]any{"tools": []any{listedTool()}, "nextCursor": "page2"})
			return
		}
		var params map[string]string
		_ = json.Unmarshal(req.Params, &params)
		if params["cursor"] != "page2" {
			t.Error(params)
		}
		d := listedTool()
		d["name"] = "second"
		reply(w, req.ID, map[string]any{"tools": []any{d}})
	}, time.Second)
	tools, err := c.Tools(context.Background())
	if err != nil || len(tools) != 2 {
		t.Fatal(tools, err)
	}
	bad := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		d := listedTool()
		d["inputSchema"] = map[string]string{"$ref": "http://invalid.example/schema"}
		reply(w, req.ID, map[string]any{"tools": []any{d}})
	}, time.Second)
	if _, err := bad.Tools(context.Background()); err == nil {
		t.Fatal("unsupported remote schema accepted")
	}
	loop := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		reply(w, req.ID, map[string]any{"tools": []any{}, "nextCursor": "loop"})
	}, time.Second)
	if _, err := loop.ListTools(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	duplicate := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		reply(w, req.ID, map[string]any{"tools": []any{listedTool(), listedTool()}})
	}, time.Second)
	if _, err := duplicate.Tools(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
}

func TestStreamableHTTPInvalidResponsesAndRemoteErrors(t *testing.T) {
	cases := []struct {
		name    string
		respond func(http.ResponseWriter, incoming)
	}{
		{"wrong-id", func(w http.ResponseWriter, r incoming) { reply(w, r.ID+1, map[string]any{"tools": []any{}}) }},
		{"unknown-content-type", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, "bad")
		}},
		{"oversize-json", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, strings.Repeat("x", MaxResponseBytes+1))
		}},
		{"oversize-sse", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+strings.Repeat("x", MaxResponseBytes+1))
		}},
		{"truncated-sse", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{}}\n", r.ID)
		}},
		{"wrong-version", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"1.0","id":%d,"result":{}}`, r.ID)
		}},
		{"session-rotation", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Mcp-Session-Id", "new-session")
			reply(w, r.ID, map[string]any{"tools": []any{}})
		}},
		{"rpc-error", func(w http.ResponseWriter, r incoming) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"error":{"code":-32601,"message":"secret"}}`, r.ID)
		}},
		{"missing-tools", func(w http.ResponseWriter, r incoming) { reply(w, r.ID, map[string]any{}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) { tc.respond(w, req) }, time.Second)
			if _, err := c.ListTools(context.Background()); err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
	c := fake(t, func(w http.ResponseWriter, r *http.Request, req incoming) {
		reply(w, req.ID, map[string]any{"content": []any{map[string]string{"type": "text", "text": "remote failure"}}, "isError": true})
	}, time.Second)
	result, err := c.CallTool(context.Background(), "echo", json.RawMessage(`{}`))
	if !errors.Is(err, ErrRemoteTool) || result.Err != err {
		t.Fatal(result, err)
	}
}

func TestStreamableHTTPConfigurationAndNegotiationFailures(t *testing.T) {
	for _, endpoint := range []string{"", "file:///etc/passwd", "https://user:pass@host/mcp", "https://host/mcp#fragment", "/relative"} {
		if c, err := NewStreamableHTTPClient(Config{Endpoint: endpoint}); err == nil {
			c.Close()
			t.Errorf("accepted %s", endpoint)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req incoming
		_ = json.NewDecoder(r.Body).Decode(&req)
		reply(w, req.ID, map[string]any{"protocolVersion": "invalid", "capabilities": map[string]any{"tools": map[string]any{}}})
	}))
	defer server.Close()
	c, err := NewStreamableHTTPClient(Config{Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.ListTools(context.Background()); !errors.Is(err, ErrNotInitialized) {
		t.Fatal(err)
	}
	if err := c.Initialize(context.Background()); !errors.Is(err, ErrProtocol) {
		t.Fatal(err)
	}
	if _, err := NewStreamableHTTPClient(Config{Endpoint: server.URL, Authorization: "bad\r\nheader"}); err == nil {
		t.Fatal("header injection accepted")
	}
	if _, err := NewStreamableHTTPClient(Config{Endpoint: server.URL, Timeout: -time.Second}); err == nil {
		t.Fatal("negative timeout accepted")
	}
}
