package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitializeSSEAndStatelessNegotiation(t *testing.T) {
	const version = "2025-06-18"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Mcp-Session-Id") != "" {
			t.Error("stateless session header or unexpected DELETE")
		}
		var req incoming
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "initialize" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: {\"jsonrpc\":\"2.0\",\"id\":%d,\"result\":{\"protocolVersion\":\"%s\",\"capabilities\":{\"tools\":{}}}}\n\n", req.ID, version)
			return
		}
		if r.Header.Get("MCP-Protocol-Version") != version {
			t.Error("negotiated version not propagated")
		}
		if req.Method == "notifications/initialized" {
			w.WriteHeader(202)
			return
		}
		reply(w, req.ID, map[string]any{"tools": []any{}})
	}))
	defer server.Close()
	c, err := NewStreamableHTTPClient(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tools, err := c.ListTools(context.Background()); err != nil || len(tools) != 0 {
		t.Fatal(tools, err)
	}
}

func TestFailedInitializedNotificationRetainsClosableSession(t *testing.T) {
	var deletes, initializes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			if r.Header.Get("Mcp-Session-Id") != "partial" {
				t.Error("lost partial session")
			}
			deletes.Add(1)
			w.WriteHeader(405)
			return
		}
		var req incoming
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "initialize" {
			initializes.Add(1)
			w.Header().Set("Mcp-Session-Id", "partial")
			reply(w, req.ID, map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}}})
			return
		}
		w.WriteHeader(500)
	}))
	defer server.Close()
	c, err := NewStreamableHTTPClient(Config{Endpoint: server.URL, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Initialize(context.Background()); err == nil {
		t.Fatal("notification failure ignored")
	}
	if err := c.Initialize(context.Background()); err == nil {
		t.Fatal("partial initialization retried without cleanup")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if deletes.Load() != 1 || initializes.Load() != 1 {
		t.Fatal(deletes.Load(), initializes.Load())
	}
}

func TestConcurrentInitializeHonorsWaitingContext(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req incoming
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method == "initialize" {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			reply(w, req.ID, map[string]any{"protocolVersion": ProtocolVersion, "capabilities": map[string]any{"tools": map[string]any{}}})
			return
		}
		w.WriteHeader(202)
	}))
	defer server.Close()
	c, err := NewStreamableHTTPClient(Config{Endpoint: server.URL, Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	done := make(chan error, 1)
	go func() { done <- c.Initialize(context.Background()) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondErr := c.Initialize(ctx)
	close(release)
	if !errors.Is(secondErr, context.Canceled) {
		t.Fatal(secondErr)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSessionHeaderValidation(t *testing.T) {
	for _, session := range []string{"has space", "nonascii-中", string(make([]byte, 1025))} {
		if validSession(session) {
			t.Errorf("accepted session %q", session)
		}
	}
	if !validSession("") || !validSession("session-123") {
		t.Fatal("valid session rejected")
	}
}
