package agent_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type scriptProvider struct {
	mu       sync.Mutex
	requests []llm.ModelRequest
	script   func(int, llm.ModelRequest) (llm.Stream, error)
}

func (p *scriptProvider) Stream(_ context.Context, r llm.ModelRequest) (llm.Stream, error) {
	p.mu.Lock()
	n := len(p.requests)
	p.requests = append(p.requests, r)
	p.mu.Unlock()
	return p.script(n, r)
}
func (p *scriptProvider) got() []llm.ModelRequest {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]llm.ModelRequest(nil), p.requests...)
}

type testStream struct {
	chunks []llm.StreamChunk
	gate   <-chan struct{}
	closed atomic.Bool
}

func (s *testStream) Next(ctx context.Context) (llm.StreamChunk, error) {
	if len(s.chunks) > 0 {
		c := s.chunks[0]
		s.chunks = s.chunks[1:]
		return c, nil
	}
	if s.gate != nil {
		select {
		case <-ctx.Done():
			return llm.StreamChunk{}, ctx.Err()
		case <-s.gate:
		}
	}
	return llm.StreamChunk{}, io.EOF
}
func (s *testStream) Close() error { s.closed.Store(true); return nil }
func textStream(text string) llm.Stream {
	return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkTextDelta, Text: text}, {Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}}}
}
func callStream(name, id string) llm.Stream {
	return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkToolCallDelta, ToolCallID: id, ToolName: name, ArgumentsDelta: `{"text":"hello"}`}, {Kind: llm.StreamChunkBlockEnd}, {Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishToolCalls}}}}
}
func openStore(t *testing.T, path string) *gormrepo.EventStore {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sql.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sql.Close() })
	if err = db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	return gormrepo.NewEventStore(db)
}
func newHarness(t *testing.T, store session.EventStore, p llm.Provider, tools *tool.Registry) *agent.Harness {
	t.Helper()
	h, err := agent.NewHarness(store, p, tools, agent.HarnessOptions{Model: "test-model", SystemPrompt: "durable system prompt"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return h
}
func sessionID(t *testing.T, h *agent.Harness) string {
	t.Helper()
	id, err := h.CreateSession(context.Background(), "历史会话")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func prompt(t *testing.T, h *agent.Harness, id, text, key string) agent.Receipt {
	t.Helper()
	r, err := h.Prompt(context.Background(), id, text, key)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func events(t *testing.T, h *agent.Harness, id string) []session.Event {
	t.Helper()
	es, err := h.ListEvents(context.Background(), id, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	return es
}
func waitEvents(t *testing.T, h *agent.Harness, id string, predicate func([]session.Event) bool) []session.Event {
	t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		es := events(t, h, id)
		if predicate(es) {
			return es
		}
		select {
		case <-timer.C:
			t.Fatalf("等待事件超时: %s", summarizeEvents(es))
		case <-tick.C:
		}
	}
}
func summarizeEvents(es []session.Event) string {
	var out strings.Builder
	for _, e := range es {
		fmt.Fprintf(&out, "\n%d %s %s", e.Seq, e.EventType, e.Data)
	}
	return out.String()
}

func latestApprovalID(t *testing.T, h *agent.Harness, id string) string {
	t.Helper()
	var approval agent.ApprovalState
	for _, e := range events(t, h, id) {
		if e.EventType == "approval/requested" {
			if err := json.Unmarshal(e.Data, &approval); err != nil {
				t.Fatal(err)
			}
		}
	}
	if approval.ID == "" {
		t.Fatal("未找到持久化审批请求")
	}
	return approval.ID
}

func count(es []session.Event, kind string) int {
	n := 0
	for _, e := range es {
		if e.EventType == kind {
			n++
		}
	}
	return n
}
func waitTurns(t *testing.T, h *agent.Harness, id string, n int) []session.Event {
	t.Helper()
	return waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "turn/end") >= n })
}

func TestHarnessSQLiteTextHistoryIdempotencyAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.sqlite")
	store := openStore(t, path)
	p := &scriptProvider{script: func(n int, r llm.ModelRequest) (llm.Stream, error) {
		return textStream(fmt.Sprintf("reply-%d", n)), nil
	}}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	first := prompt(t, h, id, "first", "same")
	waitTurns(t, h, id, 1)
	duplicate := prompt(t, h, id, "first", "same")
	if !duplicate.Duplicate || duplicate.AcceptedSeq != first.AcceptedSeq {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	prompt(t, h, id, "second", "second")
	waitTurns(t, h, id, 2)
	req := p.got()
	if len(req) != 2 || len(req[1].Messages) != 4 || req[1].Messages[0].Content[0].Text != "durable system prompt" || req[1].Messages[2].Content[0].Text != "reply-0" {
		t.Fatalf("requests=%+v", req)
	}
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	p2 := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("after restart"), nil }}
	h2, err := agent.NewHarness(openStore(t, path), p2, nil, agent.HarnessOptions{Model: "changed-model", SystemPrompt: "changed prompt"})
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close(context.Background())
	list, err := h2.ListSessions(context.Background())
	if err != nil || len(list) != 1 || list[0].ID != id || list[0].Title != "历史会话" {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	prompt(t, h2, id, "third", "third")
	es := waitTurns(t, h2, id, 3)
	req = p2.got()
	if len(req) != 1 || req[0].Model != "test-model" || req[0].Messages[0].Content[0].Text != "durable system prompt" {
		t.Fatalf("reconstructed=%+v", req)
	}
	var requested agent.ModelRequestedPayload
	for _, e := range es {
		if e.EventType == agent.EventModelRequested {
			if err := json.Unmarshal(e.Data, &requested); err != nil {
				t.Fatal(err)
			}
		}
	}
	var durable struct {
		MessageCount int    `json:"message_count"`
		RequestHash  string `json:"request_hash"`
	}
	if err = json.Unmarshal(requested.RequestSummary, &durable); err != nil || durable.MessageCount != 6 || !strings.HasPrefix(durable.RequestHash, "sha256:") {
		t.Fatalf("durable summary=%+v err=%v", durable, err)
	}
	for _, e := range es {
		if e.EventType == agent.EventModelRequested && (strings.Contains(string(e.Data), "durable system prompt") || strings.Contains(string(e.Data), `"messages"`)) {
			t.Fatalf("model/requested 复制了 Prompt: %s", e.Data)
		}
	}
}

type writeTool struct{ executed atomic.Int32 }

func (w *writeTool) Definition() tool.Definition {
	return tool.Definition{Name: "write", Version: "1", RequiresApproval: true, InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (w *writeTool) Execute(ctx context.Context, c tool.Call) (tool.Result, error) {
	if err := ctx.Err(); err != nil {
		return tool.Result{}, err
	}
	w.executed.Add(1)
	return tool.Result{CallID: c.ID, Content: "written"}, nil
}
func TestHarnessSQLiteToolEchoAndApproval(t *testing.T) {
	for _, mode := range []string{"echo", "allow", "deny"} {
		t.Run(mode, func(t *testing.T) {
			store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
			tools := tool.NewRegistry()
			w := &writeTool{}
			if err := tools.Register(tool.NewEchoTool()); err != nil {
				t.Fatal(err)
			}
			if err := tools.Register(w); err != nil {
				t.Fatal(err)
			}
			name := "write"
			if mode == "echo" {
				name = "echo"
			}
			p := &scriptProvider{script: func(n int, r llm.ModelRequest) (llm.Stream, error) {
				if n == 0 {
					return callStream(name, "call-1"), nil
				}
				return textStream("done"), nil
			}}
			h := newHarness(t, store, p, tools)
			id := sessionID(t, h)
			prompt(t, h, id, "tool", "key")
			if mode != "echo" {
				waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == 1 })
				if w.executed.Load() != 0 {
					t.Fatal("审批前发生写操作")
				}
				if err := h.ResolveApproval(context.Background(), id, latestApprovalID(t, h, id), mode == "allow"); err != nil {
					t.Fatal(err)
				}
			}
			es := waitTurns(t, h, id, 1)
			if count(es, "tool/call") != 1 || count(es, "tool/result") != 1 || count(es, "step/start") != 2 {
				t.Fatalf("events=%s", summarizeEvents(es))
			}
			expected := int32(0)
			if mode == "allow" {
				expected = 1
			}
			if w.executed.Load() != expected {
				t.Fatalf("executions=%d", w.executed.Load())
			}
			req := p.got()
			if len(req) != 2 || req[1].Messages[len(req[1].Messages)-1].Role != llm.RoleTool {
				t.Fatalf("tool request=%+v", req)
			}
			if mode == "allow" {
				decision, started, result := uint64(0), uint64(0), uint64(0)
				for _, e := range es {
					switch e.EventType {
					case "approval/resolved":
						decision = e.Seq
					case agent.EventToolStarted:
						started = e.Seq
					case "tool/result":
						result = e.Seq
					}
				}
				if decision == 0 || decision >= started || started >= result {
					t.Fatal("审批提交顺序错误")
				}
			}
		})
	}
}

func TestHarnessSQLiteSameStepRetryKeepsFailedPartialOutOfSurface(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	zero := int64(0)
	p := &scriptProvider{script: func(n int, r llm.ModelRequest) (llm.Stream, error) {
		if n == 0 {
			return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkTextDelta, Text: "failed partial"}, {Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureTransport, Message: "temporary", ProviderRetryAfterMs: &zero}}}}}, nil
		}
		return textStream("success"), nil
	}}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "retry", "key")
	es := waitTurns(t, h, id, 1)
	hashes := []string{}
	for _, e := range es {
		if e.EventType != agent.EventModelRequested {
			continue
		}
		var payload agent.ModelRequestedPayload
		if err := json.Unmarshal(e.Data, &payload); err != nil {
			t.Fatal(err)
		}
		var summary struct {
			RequestHash string `json:"request_hash"`
		}
		if err := json.Unmarshal(payload.RequestSummary, &summary); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(e.Data), "durable system prompt") || strings.Contains(string(e.Data), `"messages"`) {
			t.Fatalf("重试摘要泄露 Prompt: %s", e.Data)
		}
		hashes = append(hashes, summary.RequestHash)
	}
	if len(hashes) != 2 || hashes[0] == "" || hashes[0] != hashes[1] {
		t.Fatalf("retry hashes=%v", hashes)
	}
	if count(es, "step/start") != 1 || count(es, agent.EventRetry) != 1 || count(es, agent.EventRetryStarted) != 1 || count(es, "step/end") != 1 || count(es, "assistant/message") != 1 {
		t.Fatalf("events=%s", summarizeEvents(es))
	}
	req := p.got()
	if len(req) != 2 || req[0].StepID != req[1].StepID || req[1].Attempt != 2 {
		t.Fatalf("requests=%+v", req)
	}
	s, err := h.Snapshot(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := s.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(messages)
	if strings.Contains(string(data), "failed partial") || !strings.Contains(string(data), "success") {
		t.Fatalf("surface=%s", data)
	}
}

func TestHarnessSQLiteCancelAndFreshInput(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	gate := make(chan struct{})
	p := &scriptProvider{script: func(n int, r llm.ModelRequest) (llm.Stream, error) {
		if n == 0 {
			return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkTextDelta, Text: "partial"}}, gate: gate}, nil
		}
		return textStream("fresh"), nil
	}}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	prompt(t, h, id, "block", "key")
	waitEvents(t, h, id, func(es []session.Event) bool { return count(es, agent.EventAssistantChunk) > 0 })
	if err := h.Cancel(context.Background(), id, "first"); err != nil {
		t.Fatal(err)
	}
	if err := h.Cancel(context.Background(), id, "second"); err != nil {
		t.Fatal(err)
	}
	es := waitTurns(t, h, id, 1)
	if count(es, agent.EventCancelRequested) != 1 || count(es, "assistant/message") != 1 {
		t.Fatalf("cancel events=%+v", es)
	}
	for _, e := range es {
		if e.EventType == agent.EventCancelRequested && !strings.Contains(string(e.Data), "first") {
			t.Fatal("first cause lost")
		}
		if e.EventType == "assistant/message" {
			var payload agent.AssistantMessagePayload
			if err := json.Unmarshal(e.Data, &payload); err != nil {
				t.Fatal(err)
			}
			if !payload.Interrupted || len(payload.Message.Content) != 1 || payload.Message.Content[0].Text != "partial" || payload.Message.Content[0].Complete || len(e.SourceEventSeqs) != 1 {
				t.Fatalf("安全前缀=%s", e.Data)
			}
		}
	}
	prompt(t, h, id, "fresh", "fresh")
	waitTurns(t, h, id, 2)
}

func TestHarnessSQLiteEmptyResponseAndPermanentFailure(t *testing.T) {
	for _, code := range []llm.FailureCode{llm.FailureEmptyResponse, llm.FailureInvalidCredential} {
		t.Run(string(code), func(t *testing.T) {
			store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
			p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
				if code == llm.FailureInvalidCredential {
					return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: code}}}}}, nil
				}
				if n == 0 {
					return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishStop}}}}, nil
				}
				return textStream("recovered"), nil
			}}
			h := newHarness(t, store, p, nil)
			id := sessionID(t, h)
			prompt(t, h, id, "test", "key")
			es := waitTurns(t, h, id, 1)
			expected := 1
			if code == llm.FailureEmptyResponse {
				expected = 2
			}
			if len(p.got()) != expected || count(es, "model/error") != 1 {
				t.Fatalf("failure=%s", summarizeEvents(es))
			}
		})
	}
}

func TestHarnessSQLiteSteerAndInject(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("ok"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	if err := h.Inject(context.Background(), id, "context only"); err != nil {
		t.Fatal(err)
	}
	if len(p.got()) != 0 {
		t.Fatal("Inject 唤醒了空闲 Driver")
	}
	if _, err := h.Steer(context.Background(), id, "steering", "steer"); err != nil {
		t.Fatal(err)
	}
	waitTurns(t, h, id, 1)
	req := p.got()
	if len(req) != 1 || len(req[0].Messages) != 3 {
		t.Fatalf("steer=%+v", req)
	}
}

func TestHarnessSQLiteRestartWakesDurablePendingInput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.sqlite")
	store := openStore(t, path)
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("recovered"), nil }}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	if err := h.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry, err := agent.NewSessionActorRegistry(store, agent.SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := agent.NewInboxWriter(registry, noWake{}, agent.InboxWriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.FollowUp(context.Background(), agent.InboxRequest{SessionID: id, RequestID: "pending", IdempotencyKey: "pending", Content: []agent.ContentBlock{{Type: "text", Text: "restart pending"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	h2 := newHarness(t, openStore(t, path), p, nil)
	waitTurns(t, h2, id, 1)
	if len(p.got()) != 1 {
		t.Fatalf("calls=%d", len(p.got()))
	}
}

func TestHarnessSQLiteCancelBackoffAndRetryExhaustion(t *testing.T) {
	for _, cancelBackoff := range []bool{true, false} {
		t.Run(fmt.Sprint(cancelBackoff), func(t *testing.T) {
			store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
			delay := int64(0)
			if cancelBackoff {
				delay = 10000
			}
			p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) {
				return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureRateLimit, ProviderRetryAfterMs: &delay}}}}}, nil
			}}
			h := newHarness(t, store, p, nil)
			id := sessionID(t, h)
			prompt(t, h, id, "retry", "key")
			if cancelBackoff {
				waitEvents(t, h, id, func(es []session.Event) bool { return count(es, agent.EventRetry) == 1 })
				if err := h.Cancel(context.Background(), id, "stop backoff"); err != nil {
					t.Fatal(err)
				}
			}
			es := waitTurns(t, h, id, 1)
			if cancelBackoff {
				if len(p.got()) != 1 || count(es, agent.EventRetryStarted) != 0 {
					t.Fatal("取消后开始了新 attempt")
				}
			} else {
				if len(p.got()) != 6 || count(es, agent.EventRetry) != 5 || count(es, "step/start") != 1 {
					t.Fatalf("exhaustion=%s", summarizeEvents(es))
				}
			}
		})
	}
}

type uncertainStore struct {
	*gormrepo.EventStore
	kind  string
	fired atomic.Bool
}

func (s *uncertainStore) Append(ctx context.Context, id string, epoch, seq uint64, es []session.NewEvent) ([]session.Event, error) {
	result, err := s.EventStore.Append(ctx, id, epoch, seq, es)
	if err != nil {
		return result, err
	}
	for _, e := range es {
		if e.EventType == s.kind && s.fired.CompareAndSwap(false, true) {
			return nil, errors.New("append response lost")
		}
	}
	return result, nil
}
func TestHarnessSQLiteUncertainToolCommitsNeverExecuteTwice(t *testing.T) {
	for _, kind := range []string{"model/requested", "tool/call", "approval/resolved", "tool/started", "tool/result"} {
		t.Run(kind, func(t *testing.T) {
			store := &uncertainStore{EventStore: openStore(t, filepath.Join(t.TempDir(), "agent.sqlite")), kind: kind}
			tools := tool.NewRegistry()
			w := &writeTool{}
			if err := tools.Register(w); err != nil {
				t.Fatal(err)
			}
			p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
				if n == 0 {
					return callStream("write", "uncertain"), nil
				}
				return textStream("done"), nil
			}}
			h := newHarness(t, store, p, tools)
			id := sessionID(t, h)
			prompt(t, h, id, "write", "key")
			waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == 1 })
			if err := h.ResolveApproval(context.Background(), id, latestApprovalID(t, h, id), true); err != nil {
				t.Fatal(err)
			}
			es := waitTurns(t, h, id, 1)
			if !store.fired.Load() || w.executed.Load() != 1 || len(p.got()) != 2 || count(es, "tool/call") != 1 || count(es, "tool/result") != 1 {
				t.Fatalf("uncertain commit=%s executed=%d", summarizeEvents(es), w.executed.Load())
			}
		})
	}
}

func TestHarnessSQLiteConcurrentInputsUseOneDriver(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	var active, maximum atomic.Int32
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) {
		n := active.Add(1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		return &countedStream{Stream: textStream("ok"), active: &active}, nil
	}}
	h := newHarness(t, store, p, nil)
	id := sessionID(t, h)
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.Prompt(context.Background(), id, fmt.Sprint(i), fmt.Sprint(i))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	waitTurns(t, h, id, 10)
	if maximum.Load() != 1 || len(p.got()) != 10 {
		t.Fatalf("drivers=%d requests=%d", maximum.Load(), len(p.got()))
	}
}

type countedStream struct {
	llm.Stream
	active *atomic.Int32
	once   sync.Once
}

func (s *countedStream) Close() error {
	s.once.Do(func() { s.active.Add(-1) })
	return s.Stream.Close()
}

type structuredResultTool struct{ mode string }

func (w *structuredResultTool) Definition() tool.Definition {
	return tool.Definition{Name: "structured", Version: "1", InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (w *structuredResultTool) Execute(_ context.Context, call tool.Call) (tool.Result, error) {
	r := tool.Result{CallID: call.ID, Content: "result text", Structured: json.RawMessage(`{"value":42,"error":"nested-data"}`)}
	switch w.mode {
	case "returned-error":
		return r, errors.New("execution failed")
	case "result-error":
		r.Err = errors.New("execution failed")
	}
	return r, nil
}

func TestHarnessSQLiteToolEnvelopeSurvivesOpenAIEncoding(t *testing.T) {
	for _, mode := range []string{"success", "returned-error", "result-error"} {
		t.Run(mode, func(t *testing.T) {
			store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
			tools := tool.NewRegistry()
			if err := tools.Register(&structuredResultTool{mode: mode}); err != nil {
				t.Fatal(err)
			}
			p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
				if n == 0 {
					return callStream("structured", "call-1"), nil
				}
				return textStream("done"), nil
			}}
			h := newHarness(t, store, p, tools)
			id := sessionID(t, h)
			prompt(t, h, id, "run", "run")
			es := waitTurns(t, h, id, 1)
			reqs := p.got()
			if len(reqs) != 2 {
				t.Fatalf("requests=%d events=%s", len(reqs), summarizeEvents(es))
			}
			block := reqs[1].Messages[len(reqs[1].Messages)-1].Content[0]
			var envelope struct {
				Content    string         `json:"content"`
				Structured map[string]any `json:"structured"`
				Error      string         `json:"error"`
			}
			if err := json.Unmarshal(block.ToolResult, &envelope); err != nil {
				t.Fatal(err)
			}
			wantCode := ""
			if mode != "success" {
				wantCode = "TOOL_ERROR"
			}
			if !strings.Contains(envelope.Content, "result text") || envelope.Error != wantCode || envelope.Structured["value"] != float64(42) || envelope.Structured["error"] != "nested-data" || block.ToolCallID != "call-1" {
				t.Fatalf("工具结果丢失字段: %s", block.ToolResult)
			}
			if mode != "success" && !strings.Contains(envelope.Content, "execution failed") {
				t.Fatal("工具错误消息丢失")
			}
			type wireMessage struct {
				Role       string `json:"role"`
				Content    string `json:"content"`
				ToolCallID string `json:"tool_call_id"`
			}
			wire := make(chan []wireMessage, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload struct {
					Messages []wireMessage `json:"messages"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				wire <- payload.Messages
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			}))
			defer server.Close()
			adapter, err := llm.NewOpenAIProvider(llm.OpenAIOptions{BaseURL: server.URL, APIKey: "test-only-key"})
			if err != nil {
				t.Fatal(err)
			}
			stream, err := adapter.Stream(context.Background(), reqs[1])
			if err != nil {
				t.Fatal(err)
			}
			defer stream.Close()
			select {
			case messages := <-wire:
				last := messages[len(messages)-1]
				if last.Role != "tool" || last.ToolCallID != "call-1" || last.Content != string(block.ToolResult) {
					t.Fatalf("OpenAI 丢失封装: %+v", last)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("OpenAI 未发送工具结果")
			}
		})
	}
}

func TestHarnessSQLiteRepeatedProviderCallIDAcrossSteps(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	tools := tool.NewRegistry()
	if err := tools.Register(tool.NewEchoTool()); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
		if n < 2 {
			return callStream("echo", "call-1"), nil
		}
		return textStream("done"), nil
	}}
	h := newHarness(t, store, p, tools)
	id := sessionID(t, h)
	prompt(t, h, id, "repeat", "repeat")
	es := waitTurns(t, h, id, 1)
	if count(es, "tool/call") != 2 || count(es, "tool/result") != 2 || count(es, "step/start") != 3 {
		t.Fatalf("events=%s", summarizeEvents(es))
	}
	reqs := p.got()
	if len(reqs) != 3 || reqs[0].TurnID != reqs[2].TurnID {
		t.Fatalf("requests=%+v", reqs)
	}
	for _, req := range reqs[1:] {
		assistant := req.Messages[len(req.Messages)-2].Content[0]
		result := req.Messages[len(req.Messages)-1].Content[0]
		if assistant.ToolCallID != "call-1" || result.ToolCallID != "call-1" {
			t.Fatal("Step 间修改了供应商调用 ID")
		}
	}
}

func TestHarnessSQLiteRepeatedProviderCallIDAcrossTurns(t *testing.T) {
	store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
	tools := tool.NewRegistry()
	w := &writeTool{}
	if err := tools.Register(w); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(n int, _ llm.ModelRequest) (llm.Stream, error) {
		if n%2 == 0 {
			return callStream("write", "call-1"), nil
		}
		return textStream("done"), nil
	}}
	h := newHarness(t, store, p, tools)
	id := sessionID(t, h)
	approvalIDs := map[string]bool{}
	for turn := 1; turn <= 2; turn++ {
		prompt(t, h, id, "write", fmt.Sprint(turn))
		waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == turn })
		approvalID := latestApprovalID(t, h, id)
		if approvalIDs[approvalID] || w.executed.Load() != int32(turn-1) {
			t.Fatal("审批命名空间或执行隔离失效")
		}
		approvalIDs[approvalID] = true
		if err := h.ResolveApproval(context.Background(), id, approvalID, true); err != nil {
			t.Fatal(err)
		}
		waitTurns(t, h, id, turn)
	}
	es := events(t, h, id)
	calls := map[string]agent.ToolCallState{}
	results := map[string]bool{}
	for _, e := range es {
		switch e.EventType {
		case "tool/call":
			var state agent.ToolCallState
			if err := json.Unmarshal(e.Data, &state); err != nil {
				t.Fatal(err)
			}
			if _, exists := calls[state.Call.ID]; exists {
				t.Fatal("内部调用 ID 重复")
			}
			namespace, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(state.Call.ID, "call-"))
			var parts []string
			if err != nil || json.Unmarshal(namespace, &parts) != nil || len(parts) != 3 || parts[0] != e.TurnID || parts[1] != e.StepID || parts[2] != "call-1" {
				t.Fatalf("调用 namespace=%s err=%v", namespace, err)
			}
			if state.ProviderCallID != "call-1" || state.Call.ID == "call-1" || state.Call.IdempotencyKey != state.Call.ID || strings.ContainsAny(state.ApprovalID, "/?#%") {
				t.Fatalf("调用隔离或 URL 安全性=%+v", state)
			}
			calls[state.Call.ID] = state
		case "tool/result":
			if results[e.CallID] {
				t.Fatal("工具结果重复持久化")
			}
			results[e.CallID] = true
		}
	}
	if len(calls) != 2 || len(results) != 2 || w.executed.Load() != 2 {
		t.Fatalf("calls=%d results=%d executions=%d", len(calls), len(results), w.executed.Load())
	}
	for id := range calls {
		if !results[id] {
			t.Fatal("未配对的工具结果")
		}
	}
	reqs := p.got()
	if len(reqs) != 4 {
		t.Fatalf("requests=%d", len(reqs))
	}
	for _, i := range []int{1, 3} {
		req := reqs[i]
		assistant := req.Messages[len(req.Messages)-2].Content[0]
		result := req.Messages[len(req.Messages)-1].Content[0]
		if assistant.ToolCallID != "call-1" || result.ToolCallID != assistant.ToolCallID {
			t.Fatal("模型侧调用 ID 未保留")
		}
	}
}

type blockingWriteTool struct{ writeTool }

func (w *blockingWriteTool) Execute(ctx context.Context, c tool.Call) (tool.Result, error) {
	w.executed.Add(1)
	<-ctx.Done()
	return tool.Result{}, ctx.Err()
}

func TestHarnessSQLiteStartedSideEffectAndBackoffCrashAreNotReplayed(t *testing.T) {
	for _, mode := range []string{"tool-started", "backoff"} {
		t.Run(mode, func(t *testing.T) {
			store := openStore(t, filepath.Join(t.TempDir(), "agent.sqlite"))
			tools := tool.NewRegistry()
			w := &blockingWriteTool{}
			if err := tools.Register(w); err != nil {
				t.Fatal(err)
			}
			delay := int64(10000)
			p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) {
				if mode == "tool-started" {
					return callStream("write", "started"), nil
				}
				return &testStream{chunks: []llm.StreamChunk{{Kind: llm.StreamChunkFinish, Finish: &llm.FinishReason{Kind: llm.FinishError, Failure: &llm.LlmFailure{Code: llm.FailureTransport, ProviderRetryAfterMs: &delay}}}}}, nil
			}}
			h := newHarness(t, store, p, tools)
			id := sessionID(t, h)
			prompt(t, h, id, "crash", "key")
			var durable []session.Event
			if mode == "tool-started" {
				waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == 1 })
				if err := h.ResolveApproval(context.Background(), id, latestApprovalID(t, h, id), true); err != nil {
					t.Fatal(err)
				}
				durable = waitEvents(t, h, id, func(es []session.Event) bool { return count(es, agent.EventToolStarted) == 1 })
			} else {
				durable = waitEvents(t, h, id, func(es []session.Event) bool { return count(es, agent.EventRetry) == 1 })
			}
			clone := openStore(t, filepath.Join(t.TempDir(), "crash.sqlite"))
			if err := clone.Create(context.Background(), session.NewSession{ID: id, TenantID: "local", WorkspaceID: "default"}); err != nil {
				t.Fatal(err)
			}
			epoch, _, err := clone.ClaimWriter(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			input := make([]session.NewEvent, len(durable))
			for i, e := range durable {
				input[i] = session.NewEvent{SchemaVersion: e.SchemaVersion, EventType: e.EventType, EventID: e.EventID, OccurredAt: e.OccurredAt, ReplayPolicy: e.ReplayPolicy, Data: e.Data, TurnID: e.TurnID, StepID: e.StepID, CallID: e.CallID, SourceEventSeqs: e.SourceEventSeqs, SurfaceOp: e.SurfaceOp}
			}
			if _, err = clone.Append(context.Background(), id, epoch, 0, input); err != nil {
				t.Fatal(err)
			}
			freshTools := tool.NewRegistry()
			fresh := &writeTool{}
			if err = freshTools.Register(fresh); err != nil {
				t.Fatal(err)
			}
			p2 := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return textStream("must not run"), nil }}
			h2 := newHarness(t, clone, p2, freshTools)
			es := waitTurns(t, h2, id, 1)
			if fresh.executed.Load() != 0 || len(p2.got()) != 0 {
				t.Fatal("重启重放了未决执行")
			}
			if mode == "tool-started" && count(es, "tool/result") != 1 {
				t.Fatalf("unpaired tool call=%s", summarizeEvents(es))
			}
			if mode == "backoff" && count(es, agent.EventRetryStarted) != 0 {
				t.Fatal("重启继续了 backoff")
			}
		})
	}
}

type noWake struct{}

func (noWake) RequestWake(string) error { return nil }

func TestHarnessSQLiteApprovalCrashFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.sqlite")
	store := openStore(t, path)
	tools := tool.NewRegistry()
	w := &writeTool{}
	if err := tools.Register(w); err != nil {
		t.Fatal(err)
	}
	p := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return callStream("write", "crash-call"), nil }}
	h := newHarness(t, store, p, tools)
	id := sessionID(t, h)
	prompt(t, h, id, "write", "key")
	waitEvents(t, h, id, func(es []session.Event) bool { return count(es, "approval/requested") == 1 })
	// 复制已提交数据库事实模拟硬崩溃，不依赖正常 Close 的取消事件。
	sourceEvents := events(t, h, id)
	clone := openStore(t, filepath.Join(t.TempDir(), "crash.sqlite"))
	if err := clone.Create(context.Background(), session.NewSession{ID: id, TenantID: "local", WorkspaceID: "default"}); err != nil {
		t.Fatal(err)
	}
	epoch, _, err := clone.ClaimWriter(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	input := make([]session.NewEvent, len(sourceEvents))
	for i, e := range sourceEvents {
		input[i] = session.NewEvent{SchemaVersion: e.SchemaVersion, EventType: e.EventType, EventID: e.EventID, OccurredAt: e.OccurredAt, ReplayPolicy: e.ReplayPolicy, Data: e.Data, TurnID: e.TurnID, StepID: e.StepID, CallID: e.CallID, SourceEventSeqs: e.SourceEventSeqs, SurfaceOp: e.SurfaceOp}
	}
	if _, err = clone.Append(context.Background(), id, epoch, 0, input); err != nil {
		t.Fatal(err)
	}
	p2 := &scriptProvider{script: func(int, llm.ModelRequest) (llm.Stream, error) { return nil, errors.New("不应重新调用模型") }}
	h2 := newHarness(t, clone, p2, tools)
	es := waitTurns(t, h2, id, 1)
	if count(es, "tool/result") != 1 || count(es, "approval/resolved") != 1 || w.executed.Load() != 0 || len(p2.got()) != 0 {
		t.Fatalf("crash recovery=%+v", es)
	}
	if err = h2.ResolveApproval(context.Background(), id, latestApprovalID(t, h2, id), true); !errors.Is(err, agent.ErrApprovalResolved) {
		t.Fatalf("approval err=%v", err)
	}
	if err = h2.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	h3 := newHarness(t, clone, p2, tools)
	es2 := events(t, h3, id)
	if len(es2) != len(es) {
		t.Fatalf("repair not idempotent: %d -> %d", len(es), len(es2))
	}
}
