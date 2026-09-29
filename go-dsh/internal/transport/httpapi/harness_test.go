package httpapi_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/orchestration"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const testToken = "transport-test-secret-4a079cef"

type countedStore struct {
	*gormrepo.EventStore
	claims atomic.Int64
	writes atomic.Int64
}

func (s *countedStore) ClaimWriter(ctx context.Context, id string) (uint64, uint64, error) {
	s.claims.Add(1)
	return s.EventStore.ClaimWriter(ctx, id)
}
func (s *countedStore) Append(ctx context.Context, id string, epoch, head uint64, events []session.NewEvent) ([]session.Event, error) {
	s.writes.Add(1)
	return s.EventStore.Append(ctx, id, epoch, head, events)
}

type approvalEcho struct{ calls atomic.Int32 }

func (e *approvalEcho) Definition() tool.Definition {
	d := tool.NewEchoTool().Definition()
	d.RequiresApproval = true
	return d
}
func (e *approvalEcho) Execute(ctx context.Context, c tool.Call) (tool.Result, error) {
	e.calls.Add(1)
	return tool.NewEchoTool().Execute(ctx, c)
}

type fixture struct {
	db      *gorm.DB
	h       *agent.Harness
	store   *countedStore
	handler http.Handler
	tools   *tool.Registry
}

func setup(t *testing.T, approval *approvalEcho) fixture {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "http.sqlite")+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
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
	store := &countedStore{EventStore: gormrepo.NewEventStore(db)}
	tools := tool.NewRegistry()
	var echo tool.Tool = tool.NewEchoTool()
	if approval != nil {
		echo = approval
	}
	if err = tools.Register(echo); err != nil {
		t.Fatal(err)
	}
	h, err := agent.NewHarness(store, llm.NewDemoProvider(), tools, agent.HarnessOptions{Model: "demo", SystemPrompt: "internal-system-prompt"})
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
	return fixture{db: db, h: h, store: store, tools: tools, handler: httpapi.NewHandlerWithHarness(h, httpapi.Options{Token: testToken, Mode: "demo", Model: "demo", Tools: tools})}
}
func call(t *testing.T, f fixture, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	if method == "POST" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}
func requireCode(t *testing.T, w *httptest.ResponseRecorder, code int) {
	t.Helper()
	if w.Code != code {
		t.Fatalf("状态=%d，期望=%d，响应=%s", w.Code, code, w.Body)
	}
}
func createSession(t *testing.T, f fixture) string {
	t.Helper()
	w := call(t, f, "POST", "/api/sessions", `{"title":"测试会话"}`)
	requireCode(t, w, 201)
	var s agent.SessionInfo
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatal(err)
	}
	return s.ID
}
func waitEvent(t *testing.T, f fixture, id, kind string) []session.Event {
	t.Helper()
	return waitEventAfter(t, f, id, kind, 0)
}
func waitEventAfter(t *testing.T, f fixture, id, kind string, after uint64) []session.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		events, err := f.h.ListEvents(context.Background(), id, 0, 1000)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if e.EventType == kind && e.Seq > after {
				return events
			}
		}
		select {
		case <-timer.C:
			t.Fatalf("等待 %s 超时", kind)
		case <-tick.C:
		}
	}
}
func TestHarnessHTTPTextToolContextAndIdempotency(t *testing.T) {
	for _, text := range []string{"你好 <img src=x onerror=alert(1)>", "使用工具 echo"} {
		t.Run(text, func(t *testing.T) {
			f := setup(t, nil)
			id := createSession(t, f)
			prefix := "/api/sessions/" + id
			body, _ := json.Marshal(map[string]string{"text": text, "idempotencyKey": "stable-key"})
			first := call(t, f, "POST", prefix+"/prompts", string(body))
			requireCode(t, first, 202)
			events := waitEvent(t, f, id, "turn/end")
			if len(events) < 5 {
				t.Fatal("缺少持久化执行事件")
			}
			duplicate := call(t, f, "POST", prefix+"/prompts", string(body))
			requireCode(t, duplicate, 202)
			var a, b struct {
				Receipt struct {
					AcceptedSeq string `json:"acceptedSeq"`
					Duplicate   bool   `json:"duplicate"`
				} `json:"receipt"`
			}
			_ = json.Unmarshal(first.Body.Bytes(), &a)
			_ = json.Unmarshal(duplicate.Body.Bytes(), &b)
			if !b.Receipt.Duplicate || a.Receipt.AcceptedSeq != b.Receipt.AcceptedSeq {
				t.Fatal("未复用原始回执")
			}
			requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"不同消息","idempotencyKey":"stable-key"}`), 409)
			w := call(t, f, "GET", prefix+"/context", "")
			requireCode(t, w, 200)
			var derived struct {
				Messages []json.RawMessage `json:"messages"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &derived); err != nil {
				t.Fatal(err)
			}
			snap, err := f.h.Snapshot(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			want, err := snap.DeriveMessages()
			if err != nil {
				t.Fatal(err)
			}
			left, _ := json.Marshal(want)
			right, _ := json.Marshal(derived.Messages)
			var leftValue, rightValue any
			if err := json.Unmarshal(left, &leftValue); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(right, &rightValue); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(leftValue, rightValue) {
				t.Fatalf("上下文未使用 surface：%s != %s", left, right)
			}
			if !strings.Contains(w.Body.String(), "非真实 LLM") {
				t.Fatal("Demo 未标识")
			}
			if strings.Contains(w.Body.String(), "<img") {
				t.Fatal("JSON 未转义 HTML")
			}
			requireCode(t, call(t, f, "GET", prefix, ""), 200)
			requireCode(t, call(t, f, "GET", "/api/sessions", ""), 200)
			requireCode(t, call(t, f, "GET", "/api/tools", ""), 200)
			requireCode(t, call(t, f, "POST", prefix+"/cancel", `{}`), 202)
		})
	}
}
func TestHTTPApprovalValidation(t *testing.T) {
	for _, allow := range []bool{true, false} {
		t.Run(fmt.Sprint(allow), func(t *testing.T) {
			echo := &approvalEcho{}
			f := setup(t, echo)
			id := createSession(t, f)
			other := createSession(t, f)
			prefix := "/api/sessions/" + id
			requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"使用工具","idempotencyKey":"approval"}`), 202)
			es := waitEvent(t, f, id, "approval/requested")
			approval := ""
			for _, e := range es {
				if e.EventType == "approval/requested" {
					var data agent.ApprovalState
					_ = json.Unmarshal(e.Data, &data)
					approval = data.ID
				}
			}
			if approval == "" || echo.calls.Load() != 0 {
				t.Fatal("审批前发生调用")
			}
			path := prefix + "/approvals/" + approval
			requireCode(t, call(t, f, "POST", path, `{}`), 400)
			requireCode(t, call(t, f, "POST", path, `{"allow":"true"}`), 400)
			requireCode(t, call(t, f, "POST", "/api/sessions/"+other+"/approvals/"+approval, `{"allow":true}`), 404)
			requireCode(t, call(t, f, "POST", prefix+"/approvals/unknown", `{"allow":true}`), 404)
			if echo.calls.Load() != 0 {
				t.Fatal("无效审批触发调用")
			}
			requireCode(t, call(t, f, "POST", path, fmt.Sprintf(`{"allow":%t}`, allow)), 200)
			waitEvent(t, f, id, "turn/end")
			want := int32(0)
			if allow {
				want = 1
			}
			if echo.calls.Load() != want {
				t.Fatalf("执行次数=%d", echo.calls.Load())
			}
			requireCode(t, call(t, f, "POST", path, fmt.Sprintf(`{"allow":%t}`, allow)), 200)
			requireCode(t, call(t, f, "POST", path, fmt.Sprintf(`{"allow":%t}`, !allow)), 409)
			if echo.calls.Load() != want {
				t.Fatal("重复审批重新执行了工具")
			}
		})
	}
}
func TestHTTPAuthOriginAndLimits(t *testing.T) {
	f := setup(t, nil)
	cases := []struct {
		name, host, origin, site, auth string
		code                           int
	}{
		{"授权", "localhost", "", "", "Bearer " + testToken, 200},
		{"缺失令牌", "localhost", "", "", "", 401}, {"错误令牌", "localhost", "", "", "Bearer bad", 401},
		{"同源", "localhost:8080", "http://localhost:8080", "same-origin", "Bearer " + testToken, 200},
		{"跨端口", "localhost:8080", "http://localhost:8081", "", "Bearer " + testToken, 403},
		{"恶意 Host", "evil.test", "", "", "Bearer " + testToken, 403},
		{"恶意 Origin", "localhost", "https://evil.test", "", "Bearer " + testToken, 403},
		{"空 Origin", "localhost", "null", "", "Bearer " + testToken, 403},
		{"跨站 Fetch", "localhost", "", "cross-site", "Bearer " + testToken, 403},
		{"同站跨源 Fetch", "localhost", "", "same-site", "Bearer " + testToken, 403},
		{"IPv4", "127.0.0.1:8080", "http://127.0.0.1:8080", "", "Bearer " + testToken, 200},
		{"IPv6", "[::1]:8080", "http://[::1]:8080", "", "Bearer " + testToken, 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://localhost/api/info", nil)
			r.Host = tc.host
			r.Header.Set("Origin", tc.origin)
			if tc.origin == "" {
				r.Header.Del("Origin")
			}
			r.Header.Set("Sec-Fetch-Site", tc.site)
			r.Header.Set("Authorization", tc.auth)
			w := httptest.NewRecorder()
			f.handler.ServeHTTP(w, r)
			requireCode(t, w, tc.code)
			if strings.Contains(w.Body.String(), testToken) {
				t.Fatal("泄露令牌")
			}
		})
	}
	id := createSession(t, f)
	prefix := "/api/sessions/" + id
	for _, body := range []string{`null`, `{"text":"a"} {}`, `{"text":"a","content":[{"type":"image"}]}`, `{"text":""}`} {
		requireCode(t, call(t, f, "POST", prefix+"/prompts", body), 400)
	}
	requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"`+strings.Repeat("a", 1<<20)+`"}`), 413)
	r := httptest.NewRequest("POST", "http://localhost"+prefix+"/prompts", strings.NewReader(`{"text":"form"}`))
	r.Header.Set("Authorization", "Bearer "+testToken)
	r.Header.Set("Content-Type", "text/plain")
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	requireCode(t, w, 415)
	for _, path := range []string{"/events", "/context", "/stream", ""} {
		requireCode(t, call(t, f, "GET", "/api/sessions/missing"+path, ""), 404)
	}
	for _, query := range []string{"after=-1", "after=bad", "limit=0", "limit=1001", "after=0&after=1"} {
		requireCode(t, call(t, f, "GET", prefix+"/events?"+query, ""), 400)
	}
	public := httpapi.NewHandlerWithHarness(f.h, httpapi.Options{Mode: "demo"})
	r = httptest.NewRequest("GET", "http://localhost/api/info", nil)
	w = httptest.NewRecorder()
	public.ServeHTTP(w, r)
	requireCode(t, w, 200)
}
func TestHTTPUIAndRedaction(t *testing.T) {
	f := setup(t, nil)
	w := call(t, f, "GET", "/", "")
	requireCode(t, w, 200)
	html := w.Body.String()
	for _, bad := range []string{"innerHTML", "insertAdjacentHTML", "localStorage", testToken, "https://", "http://"} {
		if strings.Contains(html, bad) {
			t.Fatalf("UI 含不允许的构造 %q", bad)
		}
	}
	if !strings.Contains(html, "textContent") || !strings.Contains(html, "getReader()") || !strings.Contains(html, "crypto.randomUUID()") || !strings.Contains(w.Header().Get("Content-Security-Policy"), "sha256-") {
		t.Fatal("UI 安全或流式功能缺失")
	}
	script := strings.SplitN(strings.SplitN(html, "<script>", 2)[1], "</script>", 2)[0]
	hash := sha256.Sum256([]byte(script))
	if !strings.Contains(w.Header().Get("Content-Security-Policy"), "'sha256-"+base64.StdEncoding.EncodeToString(hash[:])+"'") {
		t.Fatal("CSP 与当前内嵌脚本不匹配")
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("未禁用缓存")
	}
	id := createSession(t, f)
	body, _ := json.Marshal(map[string]string{"text": testToken + " api_key=private-value", "idempotencyKey": "redact"})
	requireCode(t, call(t, f, "POST", "/api/sessions/"+id+"/prompts", string(body)), 202)
	waitEvent(t, f, id, "turn/end")
	for _, path := range []string{"events", "context"} {
		w = call(t, f, "GET", "/api/sessions/"+id+"/"+path, "")
		requireCode(t, w, 200)
		for _, secret := range []string{testToken, "private-value", "internal-system-prompt"} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatalf("%s 泄露 %s", path, secret)
			}
		}
	}
}
func TestHTTPReadOnlyDoesNotLoadActor(t *testing.T) {
	f := setup(t, nil)
	ctx := context.Background()
	id := "unloaded-view"
	if err := f.store.Create(ctx, session.NewSession{ID: id, TenantID: "local", WorkspaceID: "default"}); err != nil {
		t.Fatal(err)
	}
	epoch, _, err := f.store.ClaimWriter(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.store.Append(ctx, id, epoch, 0, []session.NewEvent{{SchemaVersion: session.SchemaVersion{Major: 1}, EventType: "session/created", EventID: "view-event", OccurredAt: time.Now(), ReplayPolicy: session.ReplayRequired, Data: json.RawMessage(`{"title":"只读"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	claims, writes := f.store.claims.Load(), f.store.writes.Load()
	for _, path := range []string{"/api/sessions", "/api/sessions/" + id, "/api/sessions/" + id + "/context", "/api/sessions/" + id + "/events"} {
		requireCode(t, call(t, f, "GET", path, ""), 200)
	}
	if claims != f.store.claims.Load() || writes != f.store.writes.Load() {
		t.Fatal("只读接口加载或修改了 Actor")
	}
}
func nextSSE(t *testing.T, reader *bufio.Reader) session.Event {
	t.Helper()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			var e session.Event
			if err = json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &e); err != nil {
				t.Fatal(err)
			}
			return e
		}
	}
}
func openSSE(t *testing.T, server *httptest.Server, path, last string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	r, _ := http.NewRequestWithContext(ctx, "GET", server.URL+path, nil)
	r.Header.Set("Authorization", "Bearer "+testToken)
	if last != "" {
		r.Header.Set("Last-Event-ID", last)
	}
	response, err := server.Client().Do(r)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if response.StatusCode != 200 {
		cancel()
		_ = response.Body.Close()
		t.Fatalf("SSE=%d", response.StatusCode)
	}
	return response, cancel
}
func TestHTTPSSECursorReconnectAndWriteDeadline(t *testing.T) {
	f := setup(t, nil)
	id := createSession(t, f)
	server := httptest.NewUnstartedServer(f.handler)
	server.Config.WriteTimeout = 20 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)
	prefix := "/api/sessions/" + id
	first, cancel := openSSE(t, server, prefix+"/stream?after=0", "")
	e := nextSSE(t, bufio.NewReader(first.Body))
	cancel()
	_ = first.Body.Close()
	if e.Seq != 1 {
		t.Fatalf("首 seq=%d", e.Seq)
	}
	requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"断线期间发送","idempotencyKey":"offline"}`), 202)
	all := waitEvent(t, f, id, "turn/end")
	response, stop := openSSE(t, server, prefix+"/stream?after=0", "1")
	reader := bufio.NewReader(response.Body)
	for _, want := range all[1:] {
		got := nextSSE(t, reader)
		if got.Seq != want.Seq || got.EventID != want.EventID {
			t.Fatalf("续传=%+v，期望=%+v", got, want)
		}
	}
	// 上一轮轮询后没有新数据，下一事件必须跨过服务器的 REST 写期限。
	requireCode(t, call(t, f, "POST", prefix+"/inject", `{"text":"实时增量"}`), 202)
	got := nextSSE(t, reader)
	if got.Seq != all[len(all)-1].Seq+1 {
		t.Fatalf("增量 seq=%d", got.Seq)
	}
	stop()
	_ = response.Body.Close()
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("断开后 SSE handler 未退出")
	}
}
func contextHead(t *testing.T, f fixture, id string) string {
	t.Helper()
	w := call(t, f, "GET", "/api/sessions/"+id+"/context", "")
	requireCode(t, w, 200)
	var view struct {
		Head string `json:"headSeq"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || view.Head == "" || view.Head == "0" {
		t.Fatalf("预览缺失字符串 headSeq：%s", w.Body.String())
	}
	return view.Head
}

func compactBody(t *testing.T, summary, head string) string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"summary": summary, "expected_head": head})
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestHTTPCompactValidation(t *testing.T) {
	f := setup(t, nil)
	id := createSession(t, f)
	path := "/api/sessions/" + id + "/compact"
	head := contextHead(t, f, id)
	claims, writes := f.store.claims.Load(), f.store.writes.Load()
	for _, body := range []string{`{}`, `null`, `[]`, `{"summary":null,"expected_head":"1"}`, `{"summary":12,"expected_head":"1"}`, `{"summary":{},"expected_head":"1"}`, `{"summary":true,"expected_head":"1"}`, `{"summary":"a","expected_head":"1","extra":1}`, `{"summary":"a","summary":"b","expected_head":"1"}`, `{"Summary":"a","expected_head":"1"}`, `{"summary":"a","expected_head":"1"} {}`, `{"summary":`, "{\"summary\":\"\xff\",\"expected_head\":\"1\"}", `{"summary":"a"}`, `{"expected_head":"1"}`, `{"summary":"a","expected_head":"1","expected_head":"2"}`} {
		requireCode(t, call(t, f, "POST", path, body), 400)
	}
	for _, value := range []string{`null`, `0`, `1`, `9007199254740993`, `true`, `[]`, `{}`, `""`, `"0"`, `"-1"`, `"+1"`, `"01"`, `"1.0"`, `"1e3"`, `" 1"`, `"18446744073709551616"`} {
		requireCode(t, call(t, f, "POST", path, `{"summary":"人工摘要","expected_head":`+value+`}`), 400)
	}
	if claims != f.store.claims.Load() || writes != f.store.writes.Load() {
		t.Fatal("无效版本触发了写操作")
	}
	for _, summary := range []string{"", "  \n\t "} {
		w := call(t, f, "POST", path, compactBody(t, summary, head))
		requireCode(t, w, 409)
		if !strings.Contains(w.Body.String(), `"category":"context_summary"`) {
			t.Fatal(w.Body.String())
		}
	}
	w := call(t, f, "POST", path, compactBody(t, "没有历史", head))
	requireCode(t, w, 409)
	if !strings.Contains(w.Body.String(), `"category":"context_empty"`) {
		t.Fatal(w.Body.String())
	}
	requireCode(t, call(t, f, "POST", path, `{"summary":"`+strings.Repeat("a", 1<<20)+`"}`), 413)
	requireCode(t, call(t, f, "POST", "/api/sessions/missing/compact", compactBody(t, "手工摘要", head)), 404)
	for _, tc := range []struct {
		contentType, token, origin string
		code                       int
	}{
		{"text/plain", testToken, "", 415},
		{"application/json", "bad", "", 401},
		{"application/json", testToken, "https://evil.test", 403},
	} {
		r := httptest.NewRequest("POST", "http://localhost"+path, strings.NewReader(compactBody(t, "手工摘要", head)))
		r.Header.Set("Content-Type", tc.contentType)
		r.Header.Set("Authorization", "Bearer "+tc.token)
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, r)
		requireCode(t, w, tc.code)
	}
}

func TestHTTPCompactRejectsActiveAndQueued(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprint(queued), func(t *testing.T) {
			f := setup(t, &approvalEcho{})
			id := createSession(t, f)
			prefix := "/api/sessions/" + id
			if queued {
				requireCode(t, call(t, f, "POST", prefix+"/inject", `{"text":"排队上下文"}`), 202)
			} else {
				requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"使用工具","idempotencyKey":"compact-active"}`), 202)
				waitEvent(t, f, id, "approval/requested")
			}
			before, err := f.h.ListEvents(context.Background(), id, 0, 1000)
			if err != nil {
				t.Fatal(err)
			}
			w := call(t, f, "POST", prefix+"/compact", compactBody(t, "人工摘要", contextHead(t, f, id)))
			requireCode(t, w, 409)
			if !strings.Contains(w.Body.String(), `"category":"context_busy"`) {
				t.Fatal(w.Body.String())
			}
			after, err := f.h.ListEvents(context.Background(), id, 0, 1000)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("拒绝压缩却改动审计：%v", err)
			}
			w = call(t, f, "GET", prefix, "")
			requireCode(t, w, 200)
			if !strings.Contains(w.Body.String(), `"canCompact":false`) {
				t.Fatal(w.Body.String())
			}
			requireCode(t, call(t, f, "POST", prefix+"/cancel", `{}`), 202)
		})
	}
}

func TestHTTPCompactRejectsFirstSubmissionFromStalePreview(t *testing.T) {
	f := setup(t, nil)
	id := createSession(t, f)
	prefix := "/api/sessions/" + id
	requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"第一段历史","idempotencyKey":"history-h1"}`), 202)
	waitEvent(t, f, id, "turn/end")
	h1 := contextHead(t, f, id)
	body := compactBody(t, "只总结第一段历史，首次提交", h1)
	snap, _, err := httpapi.ReadSnapshot(context.Background(), f.h, id)
	if err != nil {
		t.Fatal(err)
	}
	requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"必须保留的第二段历史","idempotencyKey":"history-h2"}`), 202)
	waitEventAfter(t, f, id, "turn/end", snap.HeadSeq)
	h2 := contextHead(t, f, id)
	if h1 == h2 {
		t.Fatal("第二轮历史没有推进 head")
	}
	before, events, err := httpapi.ReadSnapshot(context.Background(), f.h, id)
	if err != nil {
		t.Fatal(err)
	}
	claims, writes := f.store.claims.Load(), f.store.writes.Load()
	w := call(t, f, "POST", prefix+"/compact", body)
	requireCode(t, w, 409)
	if !strings.Contains(w.Body.String(), `"category":"context_changed"`) {
		t.Fatal(w.Body.String())
	}
	after, remaining, err := httpapi.ReadSnapshot(context.Background(), f.h, id)
	if err != nil || !reflect.DeepEqual(events, remaining) || before.HeadSeq != after.HeadSeq || !reflect.DeepEqual(before.Surface(), after.Surface()) || claims != f.store.claims.Load() || writes != f.store.writes.Load() {
		t.Fatalf("旧预览首次提交产生副作用：%v", err)
	}
	for _, head := range []string{"9007199254740993", "18446744073709551615"} {
		w := call(t, f, "POST", prefix+"/compact", compactBody(t, "范围内但非当前的 uint64", head))
		requireCode(t, w, 409)
		if !strings.Contains(w.Body.String(), `"category":"context_changed"`) {
			t.Fatal(w.Body.String())
		}
	}
}

func TestHTTPCompactAuditReplayAndReadOnly(t *testing.T) {
	f := setup(t, nil)
	id := createSession(t, f)
	prefix := "/api/sessions/" + id
	requireCode(t, call(t, f, "POST", prefix+"/prompts", `{"text":"使用工具","idempotencyKey":"compact-idle"}`), 202)
	waitEvent(t, f, id, "turn/end")
	ctx := context.Background()
	before, original, err := httpapi.ReadSnapshot(ctx, f.h, id)
	if err != nil {
		t.Fatal(err)
	}
	summary := "用户手工记录：工具任务已完成。<img src=x onerror=alert(1)>"
	body := compactBody(t, summary, contextHead(t, f, id))
	w := call(t, f, "GET", prefix, "")
	requireCode(t, w, 200)
	if !strings.Contains(w.Body.String(), `"canCompact":true`) {
		t.Fatal(w.Body.String())
	}
	requireCode(t, call(t, f, "POST", prefix+"/compact", string(body)), 200)
	after, events, err := httpapi.ReadSnapshot(ctx, f.h, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(original)+2 || !reflect.DeepEqual(original, events[:len(original)]) {
		t.Fatal("压缩没有保留完整原始审计")
	}
	marker, replacement := events[len(original)], events[len(original)+1]
	var audit agent.ContextCompactedPayload
	if err := json.Unmarshal(marker.Data, &audit); err != nil {
		t.Fatal(err)
	}
	if marker.EventType != agent.EventContextCompacted || !audit.Manual || audit.Summary != summary || audit.SourceHeadSeq != before.HeadSeq || !reflect.DeepEqual(audit.SourceEventSeqs, before.Surface().Nodes()) {
		t.Fatal("人工摘要来源审计不完整")
	}
	sources := append(before.Surface().Nodes(), marker.Seq)
	if replacement.EventType != "user/message" || replacement.SurfaceOp == nil || replacement.SurfaceOp.Kind != session.SurfaceOpReplace || !reflect.DeepEqual(replacement.SourceEventSeqs, sources) || after.Surface().ReplaceGeneration() != before.Surface().ReplaceGeneration()+1 {
		t.Fatal("摘要替换或 source seq 不正确")
	}
	var message llm.Message
	if err := json.Unmarshal(replacement.Data, &message); err != nil || len(message.Content) != 1 || !strings.Contains(message.Content[0].Text, summary) {
		t.Fatalf("摘要不是直接 message：%v", err)
	}
	w = call(t, f, "GET", prefix+"/context", "")
	requireCode(t, w, 200)
	var view struct {
		SessionID string        `json:"sessionId"`
		HeadSeq   string        `json:"headSeq"`
		Messages  []llm.Message `json:"messages"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if view.SessionID != id || view.HeadSeq != fmt.Sprint(after.HeadSeq) || len(view.Messages) != 1 || !reflect.DeepEqual(view.Messages[0], message) {
		t.Fatalf("旧 messages 合同不兼容：%s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "internal-system-prompt") || strings.Contains(w.Body.String(), "<img") {
		t.Fatal("上下文未安全编码")
	}
	requireCode(t, call(t, f, "POST", prefix+"/compact", string(body)), 200)
	head, err := f.store.Head(ctx, id)
	if err != nil || head != after.HeadSeq {
		t.Fatalf("重复摘要不是幂等：%v", err)
	}
	wrongRetry := call(t, f, "POST", prefix+"/compact", compactBody(t, summary, contextHead(t, f, id)))
	requireCode(t, wrongRetry, 409)
	if !strings.Contains(wrongRetry.Body.String(), `"category":"context_changed"`) {
		t.Fatal(wrongRetry.Body.String())
	}

	// 不同于默认预算，验证 transport 使用 Harness 的真实配置而非本地复制估算。
	other, err := agent.NewHarness(f.store, llm.NewDemoProvider(), f.tools, agent.HarnessOptions{Model: "demo", ContextTokenBudget: 128, ContextOutputReservedTokens: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close(ctx)
	cold := fixture{h: other, handler: httpapi.NewHandlerWithHarness(other, httpapi.Options{Token: testToken})}
	claims, writes := f.store.claims.Load(), f.store.writes.Load()
	for _, suffix := range []string{"", "/context", "/events"} {
		requireCode(t, call(t, cold, "GET", prefix+suffix, ""), 200)
	}
	budgetResponse := call(t, cold, "GET", prefix+"/context", "")
	requireCode(t, budgetResponse, 200)
	var budgetView struct {
		Head        string               `json:"headSeq"`
		Generation  string               `json:"replaceGeneration"`
		Sources     []string             `json:"sourceEventSeqs"`
		Messages    []llm.Message        `json:"messages"`
		Tools       []llm.ToolDefinition `json:"tools"`
		Budget      agent.ContextBudget  `json:"budget"`
		Explanation string               `json:"budgetExplanation"`
	}
	if err := json.Unmarshal(budgetResponse.Body.Bytes(), &budgetView); err != nil {
		t.Fatal(err)
	}
	want, err := other.ContextPreviewFromSnapshot(after)
	if err != nil {
		t.Fatal(err)
	}
	if budgetView.Head != fmt.Sprint(after.HeadSeq) || budgetView.Generation != "1" || !reflect.DeepEqual(budgetView.Sources, []string{fmt.Sprint(replacement.Seq)}) || !reflect.DeepEqual(budgetView.Messages, view.Messages) || !reflect.DeepEqual(budgetView.Budget, want.Budget) || len(budgetView.Tools) != len(want.Tools) || budgetView.Budget.LimitTokens != 128 || budgetView.Budget.ReservedOutputTokens != 32 || !budgetView.Budget.Exceeded || budgetView.Budget.Exact || budgetView.Budget.EstimatedSystemTokens == 0 || budgetView.Budget.EstimatedToolTokens == 0 || budgetView.Explanation == "" {
		t.Fatalf("只读预算或旧 messages 不兼容：%s", budgetResponse.Body.String())
	}
	if strings.Contains(budgetResponse.Body.String(), "internal-system-prompt") {
		t.Fatal("预算响应泄露系统提示")
	}
	if claims != f.store.claims.Load() || writes != f.store.writes.Load() {
		t.Fatal("冷加载只读重放夺取 writer 或写入修复")
	}
	page := call(t, f, "GET", prefix+"/events", "")
	requireCode(t, page, 200)
	var envelope struct {
		Events []json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(page.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	html := call(t, f, "GET", "/", "").Body.String()
	runUICompactionReplay(t, html, envelope.Events, summary)

	requireCode(t, call(t, f, "POST", prefix+"/compact", compactBody(t, "用户提供的另一份摘要", contextHead(t, f, id))), 200)
	w = call(t, f, "POST", prefix+"/compact", string(body))
	requireCode(t, w, 409)
	if !strings.Contains(w.Body.String(), `"category":"context_changed"`) {
		t.Fatal(w.Body.String())
	}
}

func runUICompactionReplay(t *testing.T, html string, events []json.RawMessage, summary string) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("未安装 Node，跳过无浏览器 DOM 合同测试")
	}
	script := strings.SplitN(strings.SplitN(html, "<script>", 2)[1], "</script>", 2)[0]
	script = strings.Replace(script, "void connect();", "", 1)
	data, _ := json.Marshal(events)
	text, _ := json.Marshal(summary)
	program := `
const assert=require('node:assert/strict');
const crypto=require('node:crypto').webcrypto;
class Element {
 constructor(tag){this.tag=tag;this.children=[];this.listeners={};this.dataset={};this.className='';this.value='';this.disabled=false;this._text='';this.scrollHeight=0;this.scrollTop=0;this.clientHeight=0;this.classList={toggle:()=>{}}}
 set textContent(v){this._text=String(v);this.children=[]}
 get textContent(){return this._text+this.children.map(n=>n.textContent).join('')}
 append(...nodes){for(const n of nodes){n.parent=this;this.children.push(n)}}
 replaceChildren(...nodes){this._text='';this.children=[];this.append(...nodes)}
 get firstChild(){return this.children[0]}
 remove(){if(this.parent)this.parent.children=this.parent.children.filter(n=>n!==this)}
 querySelector(s){return this.children.find(n=>s==='.'+n.className)||null}
 addEventListener(type,fn){this.listeners[type]=fn}
}
const elements=new Map();
const document={getElementById(id){if(!elements.has(id))elements.set(id,new Element('div'));return elements.get(id)},createElement:tag=>new Element(tag),createTextNode(text){const n=new Element('#text');n.textContent=text;return n}};
const saved=new Map();
const sessionStorage={getItem:k=>saved.get(k)||null,setItem:(k,v)=>saved.set(k,v),removeItem:k=>saved.delete(k)};
const window={addEventListener:()=>{},prompt:()=>null};
` + script + "\nconst events=" + string(data) + ";const summary=" + string(text) + ";\n" + `
(async()=>{
 state.id='test-session';state.ready=true;state.synced=true;state.controller=new AbortController();
 for(const e of events)event(e);
 assert.equal(state.cards.size,1,'摘要替换后不应残留旧消息卡片');
 assert($('messages').textContent.includes(summary),'直接 message 中的摘要必须可见');
 assert($('messages').textContent.includes('完整审计记录保留'));
 assert.equal($('event-list').children.length,events.length,'摘要替换不能清空审计轨迹');
 assert.equal(state.seq,BigInt(events.at(-1).seq));
 const shown=$('messages').textContent;
 event(events.at(-1));
 assert.equal($('event-list').children.length,events.length,'重复 SSE 事件应去重');
 state.seq=0n;state.cards.clear();state.approvals.clear();state.attempts.clear();$('messages').replaceChildren();$('event-list').replaceChildren();
 for(const e of events)event(e);
 assert.equal($('messages').textContent,shown,'刷新重放与实时增量结果必须一致');
 $('event-list').children.at(-1).listeners.click();
 assert($('inspector').textContent.includes(summary),'原始事件检查器应保留摘要');
 state.canCompact=true;buttons();assert.equal($('compact').disabled,false);
 for(const key of ['busy','pending']){state[key]=true;buttons();assert.equal($('compact').disabled,true);state[key]=false}
 state.synced=false;buttons();assert.equal($('compact').disabled,true);state.synced=true;
 state.canCompact=false;buttons();assert.equal($('compact').disabled,true);state.canCompact=true;buttons();
 let calls=0,replayed=0,previews=0,prompts=0,fail=false,latestHead='9007199254740993';
 let expectedHead=latestHead;
 const realRefresh=refreshStatus;
 refreshStatus=async()=>{};
 request=async(path,options)=>{
  if(path.endsWith('/context')){assert.equal(path,'/api/sessions/test-session/context');previews++;return {headSeq:latestHead,messages:[],budget:{exact:false}}}
  calls++;assert.equal(path,'/api/sessions/test-session/compact');assert.deepEqual(options,{method:'POST',body:{summary:'  手工原文  ',expected_head:expectedHead}});
  if(fail)throw new TypeError('网络中断，提交结果未知');
  return {accepted:true}
 };
 select=async(id)=>{assert.equal(id,'test-session');replayed++;return true};
 window.prompt=()=>null;await compact();assert.equal(calls,0);assert.equal(previews,1);
 window.prompt=()=>'';await compact();assert.equal(calls,0,'空摘要不能自动补内容');assert.equal(previews,2);
 window.prompt=(label,value)=>{prompts++;assert.equal(previews,3,'必须先预览再输入');assert.equal(value,'','摘要输入不能预填模型或历史内容');assert(label.includes(expectedHead));assert(label.includes('完整审计记录保留'));latestHead='9007199254740999';return '  手工原文  '};
 fail=true;await compact();assert.equal(calls,1);assert.equal(replayed,0);
 assert.equal(state.compactPending.expected_head,expectedHead,'首次提交不得取输入后的新 head');
 assert.equal($('send').disabled,true,'待确认摘要期间不能新发消息');
 assert(saved.has('dsh.compact.test-session'));
 state.compactPending=null;restoreCompact();buttons();
 assert.deepEqual(state.compactPending,{summary:'  手工原文  ',expected_head:expectedHead},'刷新必须恢复原摘要及预览版本');
 fail=false;await compact();assert.equal(calls,2);assert.equal(previews,3,'网络重试不得重取预览');assert.equal(prompts,1,'网络重试不得重新填写摘要');assert.equal(replayed,1,'成功后必须从日志重新选择会话');
 assert.equal(state.compactPending,null);assert(!saved.has('dsh.compact.test-session'));assert.equal(state.busy,false);
 for(const value of [0,1,9007199254740993,'0','-1','+1','01','1e3','18446744073709551616',undefined])assert.throws(()=>previewHead(value));
 assert.equal(previewHead('18446744073709551615'),18446744073709551615n);
 // 首次提交即迟到：用户填写时发生新事件，仍应提交旧版本并保留 409 重试材料。
 expectedHead='18446744073709551614';latestHead=expectedHead;
 request=async(path,options)=>{if(path.endsWith('/context')){previews++;return {headSeq:latestHead}}calls++;assert.equal(options.body.expected_head,expectedHead);const err=new Error('历史已变化');err.status=409;throw err};
 window.prompt=()=>{latestHead='18446744073709551615';event({seq:String(state.seq+1n),eventType:'turn/start',data:{}});return '  手工原文  '};
 await compact();assert.equal(calls,3);assert.equal(previews,4);assert.equal(state.compactPending.expected_head,expectedHead);assert.equal(replayed,1);
 state.canCompact=true;buttons();await compact();assert.equal(calls,4);assert.equal(previews,4,'409 重试也不得偷偷换 head');
 window.confirm=()=>true;$('discard-compact').listeners.click();assert.equal(state.compactPending,null);assert(!saved.has('dsh.compact.test-session'));
 let asked=false;
 request=async()=>({head_seq:'9'});window.prompt=()=>{asked=true;return '摘要'};
 await compact();assert.equal(asked,false,'错误的 head 字段名必须拒绝，不能隐式补当前版本');
 assert.equal(state.compactPending,null);
 refreshStatus=realRefresh;
 request=async()=>({status:{state:'running',canCompact:true,lastEventSeq:String(state.seq)}});
 await refreshStatus(state.version,state.controller.signal);assert.equal(state.canCompact,false);
 request=async()=>({status:{state:'idle',canCompact:true,lastEventSeq:String(state.seq)}});
 await refreshStatus(state.version,state.controller.signal);assert.equal(state.canCompact,true);
 let resolve;
 request=()=>new Promise(r=>{resolve=r});
 const pendingStatus=refreshStatus(state.version,state.controller.signal);
 event({seq:String(state.seq+1n),eventType:'turn/start',data:{}});
 resolve({status:{state:'idle',canCompact:true,lastEventSeq:String(state.seq)}});
 await pendingStatus;assert.equal(state.canCompact,false,'迟到的空闲状态不能覆盖新执行事件');
 state.orchestration=true;state.taskTab=false;state.busy=false;state.taskPending=null;
 const requests=[];let taskFail=true;
 request=async(path,options)=>{requests.push({path,body:JSON.parse(JSON.stringify(options?.body||{}))});if(taskFail)throw new Error('响应丢失');return {id:'workflow-ui'}};
 $('task-input').value='第一节点\n第二节点';
 await createTask('workflows');assert.equal(requests.length,1);assert.deepEqual(requests[0].body.steps,['第一节点','第二节点']);assert.equal(typeof requests[0].body.key,'string');
 assert.equal($('new-child').disabled,true);assert.equal($('task-input').disabled,true);
 state.taskPending=null;restoreTasks();assert.deepEqual(state.taskPending.body,requests[0].body,'编排刷新重试必须保留原内容与 key');
 taskFail=false;await createTask('workflows');assert.deepEqual(requests[1],requests[0]);assert.equal(state.taskPending,null);assert(state.knownWorkflows.includes('workflow-ui'));
 const workflow={id:'workflow-ui',state:'running',current_step:0,steps:['一','二']};
 const job={id:'job-ui',child_session_id:'child-ui',workflow_id:workflow.id,step_index:0,prompt:'<img src=x onerror=alert(1)>',state:'running',cancel_requested:false};
 renderTasks([job],[workflow],state.id);assert($('task-list').textContent.includes(job.prompt));
 const controls=$('task-list').children[0].children.at(-1).children;
 assert.equal(controls[0].textContent,'暂停');assert.equal(controls[0].disabled,false);assert.equal(controls[1].disabled,true);
 await controls[0].listeners.click();assert(requests.at(-1).path.endsWith('/workflows/workflow-ui/pause'));
 workflow.state='paused';renderTasks([job],[workflow],state.id);
 await $('task-list').children[0].children.at(-1).children[1].listeners.click();assert(requests.at(-1).path.endsWith('/workflows/workflow-ui/resume'));
 window.confirm=()=>true;await $('task-list').children[0].children.at(-1).children[2].listeners.click();assert(requests.at(-1).path.endsWith('/workflows/workflow-ui/cancel'));
 const beforeWrong=requests.length;await taskControl('wrong-parent','jobs/job-ui/cancel','');assert.equal(requests.length,beforeWrong,'过期父会话不能控制当前资源');
 window.confirm=()=>false;await $('cancel-tree').listeners.click();assert.equal(requests.length,beforeWrong);
 let warning='';window.confirm=message=>{warning=message;return true};await $('cancel-tree').listeners.click();assert(warning.includes('停止所有子任务并永久关闭本会话新编排'));assert(requests.at(-1).path.endsWith('/cancel-tree'));assert.deepEqual(requests.at(-1).body,{});
 await $('cancel').listeners.click();assert(requests.at(-1).path.endsWith('/test-session/cancel'),'普通取消必须保留原路径');
 let opened='';request=async(path)=>{assert(path.endsWith('/child-ui'));return {id:'child-ui',title:'子会话'}};select=async(id)=>{opened=id};sessions=async()=>{};
 await openChildSession('test-session','child-ui');assert.equal(opened,'child-ui');
 const treeActions=requests.filter(r=>r.path.endsWith('/cancel-tree'));assert.equal(treeActions.length,1);
 state.orchestration=false;taskButtons();assert.equal($('new-child').disabled,true);assert.equal($('cancel-tree').disabled,true);
 process.stdout.write('UI 人工摘要与任务面板合同通过\n');
})().catch(err=>{console.error(err);process.exitCode=1});
`
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, node)
	cmd.Stdin = strings.NewReader(program)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("UI 合同失败：%v\n%s", err, out)
	}
}

func setupOrchestration(t *testing.T, run bool) (fixture, *orchestration.Manager) {
	t.Helper()
	f := setup(t, nil)
	m, err := orchestration.NewWithOptions(f.db, f.h, orchestration.Options{PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	f.handler = httpapi.NewHandlerWithHarness(f.h, httpapi.Options{Token: testToken, Mode: "demo", Tools: f.tools, Orchestration: m})
	if run {
		runManager(t, m)
	}
	return f, m
}
func runManager(t *testing.T, m *orchestration.Manager) {
	t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Manager Run 未退出")
		}
	})
}
func orchestrationCall(t *testing.T, f fixture, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	for {
		w := call(t, f, method, path, body)
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"category":"busy"`) {
			return w
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("编排协调器持续忙：%s", path)
		}
	}
}
func responseValue[T any](t *testing.T, w *httptest.ResponseRecorder, code int) T {
	t.Helper()
	requireCode(t, w, code)
	var value T
	if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
func awaitHTTP[T any](t *testing.T, f fixture, path string, done func(T) bool) T {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	for {
		value := responseValue[T](t, call(t, f, "GET", path, ""), 200)
		if done(value) {
			return value
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatalf("等待 HTTP 编排终态超时：%s %+v", path, value)
		}
	}
}
func TestHTTPOrchestrationJobsAndFullWorkflow(t *testing.T) {
	f, _ := setupOrchestration(t, true)
	parent := createSession(t, f)
	other := createSession(t, f)
	prefix := "/api/sessions/" + parent
	body := `{"prompt":"子任务 <img src=x onerror=alert(1)>","idempotency_key":"child-stable"}`
	job := responseValue[orchestration.Job](t, orchestrationCall(t, f, "POST", prefix+"/jobs", body), 202)
	duplicate := responseValue[orchestration.Job](t, orchestrationCall(t, f, "POST", prefix+"/jobs", body), 202)
	if job.ID == "" || job.ChildSessionID == "" || job.ID != duplicate.ID || job.ParentID != parent {
		t.Fatal("子任务幂等或父归属错误")
	}
	requireCode(t, orchestrationCall(t, f, "POST", prefix+"/jobs", `{"prompt":"不同内容","idempotency_key":"child-stable"}`), 409)
	job = awaitHTTP[orchestration.Job](t, f, prefix+"/jobs/"+job.ID, func(j orchestration.Job) bool { return j.State == orchestration.Succeeded })
	if job.AcceptedSeq == 0 || job.EndSeq <= job.AcceptedSeq || job.TurnID == "" {
		t.Fatalf("终态未由事件确认：%+v", job)
	}
	requireCode(t, call(t, f, "GET", "/api/sessions/"+job.ChildSessionID, ""), 200)
	for _, method := range []string{"GET", "POST"} {
		suffix := ""
		body := ""
		if method == "POST" {
			suffix = "/cancel"
			body = "{}"
		}
		requireCode(t, orchestrationCall(t, f, method, "/api/sessions/"+other+"/jobs/"+job.ID+suffix, body), 404)
	}
	workflowBody := `{"steps":["第一节点","第二节点"],"key":"flow-stable"}`
	flow := responseValue[orchestration.Workflow](t, orchestrationCall(t, f, "POST", prefix+"/workflows", workflowBody), 202)
	again := responseValue[orchestration.Workflow](t, orchestrationCall(t, f, "POST", prefix+"/workflows", workflowBody), 202)
	if flow.ID != again.ID {
		t.Fatal("工作流未幂等")
	}
	requireCode(t, orchestrationCall(t, f, "POST", prefix+"/workflows", `{"steps":["改变节点"],"key":"flow-stable"}`), 409)
	flow = awaitHTTP[orchestration.Workflow](t, f, prefix+"/workflows/"+flow.ID, func(w orchestration.Workflow) bool { return w.State == orchestration.Succeeded })
	if flow.CurrentStep != 2 || len(flow.Jobs) != 2 {
		t.Fatalf("未完成两节点工作流：%+v", flow)
	}
	for i, j := range flow.Jobs {
		if j.State != orchestration.Succeeded || j.WorkflowID != flow.ID || j.StepIndex != i || j.ParentID != parent {
			t.Fatalf("节点映射错误：%+v", j)
		}
	}
	if flow.Jobs[1].CreatedAt.Before(flow.Jobs[0].CreatedAt) {
		t.Fatal("工作流顺序不正确")
	}
	for _, action := range []string{"", "/pause", "/resume", "/cancel"} {
		method, body := "GET", ""
		if action != "" {
			method, body = "POST", "{}"
		}
		requireCode(t, orchestrationCall(t, f, method, "/api/sessions/"+other+"/workflows/"+flow.ID+action, body), 404)
	}
	var list struct {
		Workflows []orchestration.Workflow `json:"workflows"`
		Partial   bool                     `json:"partial"`
		Source    string                   `json:"source"`
	}
	list = responseValue[struct {
		Workflows []orchestration.Workflow `json:"workflows"`
		Partial   bool                     `json:"partial"`
		Source    string                   `json:"source"`
	}](t, call(t, f, "GET", prefix+"/workflows", ""), 200)
	if len(list.Workflows) != 1 || list.Workflows[0].ID != flow.ID || !list.Partial || list.Source != "jobs" {
		t.Fatalf("工作流聚合合同错误：%+v", list)
	}
	w := call(t, f, "GET", prefix+"/jobs", "")
	requireCode(t, w, 200)
	if strings.Contains(w.Body.String(), "<img") {
		t.Fatal("子任务未安全编码")
	}
	before, _ := f.store.Head(context.Background(), parent)
	claims, writes := f.store.claims.Load(), f.store.writes.Load()
	for _, suffix := range []string{"/jobs", "/schedules", "/workflows", "/jobs/" + job.ID, "/workflows/" + flow.ID} {
		requireCode(t, call(t, f, "GET", prefix+suffix, ""), 200)
	}
	after, _ := f.store.Head(context.Background(), parent)
	if before != after || claims != f.store.claims.Load() || writes != f.store.writes.Load() {
		t.Fatal("编排 GET 改动会话")
	}
}
func TestHTTPOrchestrationControlsAndSchedules(t *testing.T) {
	f, m := setupOrchestration(t, false)
	parent := createSession(t, f)
	other := createSession(t, f)
	prefix := "/api/sessions/" + parent
	flow := responseValue[orchestration.Workflow](t, call(t, f, "POST", prefix+"/workflows", `{"steps":["第一步","第二步"],"key":"controls"}`), 202)
	path := prefix + "/workflows/" + flow.ID
	requireCode(t, call(t, f, "POST", path+"/pause", "{}"), 202)
	paused := responseValue[orchestration.Workflow](t, call(t, f, "GET", path, ""), 200)
	if paused.State != orchestration.Paused || len(paused.Jobs) != 0 {
		t.Fatal("暂停未阻止待投递节点")
	}
	listing := call(t, f, "GET", prefix+"/workflows", "")
	requireCode(t, listing, 200)
	if !strings.Contains(listing.Body.String(), `"workflows":[]`) {
		t.Fatal("未生成 job 的工作流不应伪装进入完整列表")
	}
	requireCode(t, call(t, f, "POST", path+"/resume", "{}"), 202)
	requireCode(t, call(t, f, "POST", path+"/cancel", "{}"), 202)
	cancelled := responseValue[orchestration.Workflow](t, call(t, f, "GET", path, ""), 200)
	if cancelled.State != orchestration.Cancelled {
		t.Fatal("未取消工作流")
	}
	due := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	scheduleBody := fmt.Sprintf(`{"text":"计划内容","idempotency_key":"schedule","due":%q,"interval_seconds":60}`, due)
	schedule := responseValue[orchestration.Schedule](t, call(t, f, "POST", prefix+"/schedules", scheduleBody), 202)
	duplicate := responseValue[orchestration.Schedule](t, call(t, f, "POST", prefix+"/schedules", scheduleBody), 202)
	parsed, _ := time.Parse(time.RFC3339, due)
	if schedule.ID != duplicate.ID || schedule.Interval != time.Minute || !schedule.Due.Equal(parsed) {
		t.Fatal("计划任务时间或幂等错误")
	}
	requireCode(t, call(t, f, "POST", "/api/sessions/"+other+"/schedules/"+schedule.ID+"/cancel", "{}"), 404)
	requireCode(t, call(t, f, "POST", prefix+"/schedules/"+schedule.ID+"/cancel", "{}"), 202)
	// 普通父 Turn 取消不应持久关闭新编排。
	requireCode(t, call(t, f, "POST", prefix+"/cancel", `{"reason":"普通取消"}`), 202)
	child := responseValue[orchestration.Job](t, call(t, f, "POST", prefix+"/jobs", `{"prompt":"仍然允许新子任务","idempotency_key":"after-turn-cancel"}`), 202)
	requireCode(t, call(t, f, "POST", prefix+"/jobs/"+child.ID+"/cancel", "{}"), 202)
	before, _ := f.store.Head(context.Background(), parent)
	requireCode(t, call(t, f, "POST", prefix+"/cancel-tree", "{}"), 202)
	after, _ := f.store.Head(context.Background(), parent)
	if before != after {
		t.Fatal("cancel-tree 不应取消父 Turn")
	}
	requireCode(t, call(t, f, "POST", prefix+"/jobs", `{"prompt":"不应被准入","idempotency_key":"after-tree"}`), 409)
	requireCode(t, call(t, f, "POST", prefix+"/workflows", `{"steps":["不应准入"],"key":"after-tree"}`), 409)
	requireCode(t, call(t, f, "POST", prefix+"/schedules", strings.Replace(scheduleBody, `"schedule"`, `"after-tree"`, 1)), 409)
	runManager(t, m)
	awaitHTTP[orchestration.Job](t, f, prefix+"/jobs/"+child.ID, func(j orchestration.Job) bool { return j.State == orchestration.Cancelled })
	// 一次性计划必须由 Manager Run 投递；due 是显式 RFC3339，不接受 Unix timestamp。
	future := time.Now().Add(300 * time.Millisecond).UTC().Format(time.RFC3339Nano)
	fireBody := fmt.Sprintf(`{"text":"定时投递","idempotency_key":"one-shot","due":%q,"interval_seconds":0}`, future)
	fire := responseValue[orchestration.Schedule](t, orchestrationCall(t, f, "POST", "/api/sessions/"+other+"/schedules", fireBody), 202)
	type schedulesView struct {
		Schedules []orchestration.Schedule `json:"schedules"`
	}
	awaitHTTP[schedulesView](t, f, "/api/sessions/"+other+"/schedules", func(v schedulesView) bool {
		for _, s := range v.Schedules {
			if s.ID == fire.ID {
				return s.State == orchestration.Exhausted && s.FireCount == 1
			}
		}
		return false
	})
	waitEvent(t, f, other, "turn/end")
}
func TestHTTPOrchestrationSecurityAndMissingManager(t *testing.T) {
	f := setup(t, nil)
	parent := createSession(t, f)
	prefix := "/api/sessions/" + parent
	for _, tc := range []struct{ method, path string }{{"GET", "/jobs"}, {"POST", "/jobs"}, {"GET", "/jobs/x"}, {"POST", "/jobs/x/cancel"}, {"GET", "/schedules"}, {"POST", "/schedules"}, {"POST", "/schedules/x/cancel"}, {"GET", "/workflows"}, {"POST", "/workflows"}, {"GET", "/workflows/x"}, {"POST", "/workflows/x/pause"}, {"POST", "/workflows/x/resume"}, {"POST", "/workflows/x/cancel"}, {"POST", "/cancel-tree"}} {
		requireCode(t, call(t, f, tc.method, prefix+tc.path, "{}"), 503)
	}
	f, m := setupOrchestration(t, false)
	parent = createSession(t, f)
	prefix = "/api/sessions/" + parent
	for _, tc := range []struct{ path, body string }{{"/jobs", `{"prompt":"x","idempotency_key":"k","parent_id":"evil"}`}, {"/jobs", `{"prompt":12,"idempotency_key":"k"}`}, {"/jobs", `{"prompt":"x"}`}, {"/workflows", `{"steps":["a",null],"key":"k"}`}, {"/workflows", `{"steps":"a","key":"k"}`}, {"/workflows", `{"steps":[],"key":"k"}`}, {"/jobs/x/cancel", `{"allow":true}`}, {"/cancel-tree", `{"reason":"not-supported"}`}} {
		requireCode(t, call(t, f, "POST", prefix+tc.path, tc.body), 400)
	}
	for _, due := range []string{`1234567890`, `"1234567890"`, `"2030-01-01"`, `"2030-01-01T12:00:00"`, `null`} {
		requireCode(t, call(t, f, "POST", prefix+"/schedules", `{"text":"x","idempotency_key":"s","due":`+due+`,"interval_seconds":0}`), 400)
	}
	for _, interval := range []string{`-1`, `1`, `59`, `31536001`, `9223372036854775807`, `1.2`, `"60"`, `null`} {
		requireCode(t, call(t, f, "POST", prefix+"/schedules", fmt.Sprintf(`{"text":"x","idempotency_key":"s","due":%q,"interval_seconds":%s}`, time.Now().Add(time.Hour).Format(time.RFC3339), interval)), 400)
	}
	for _, body := range []string{`null`, `{} {}`, `{"prompt":"` + strings.Repeat("x", 1<<20) + `","idempotency_key":"k"}`} {
		code := 400
		if len(body) > 1<<20 {
			code = 413
		}
		requireCode(t, call(t, f, "POST", prefix+"/jobs", body), code)
	}
	for _, tc := range []struct {
		token, origin, typ string
		code               int
	}{{"bad", "", "application/json", 401}, {testToken, "https://evil.test", "application/json", 403}, {testToken, "", "text/plain", 415}} {
		r := httptest.NewRequest("POST", "http://localhost"+prefix+"/jobs", strings.NewReader(`{"prompt":"x","idempotency_key":"k"}`))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		r.Header.Set("Content-Type", tc.typ)
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		w := httptest.NewRecorder()
		f.handler.ServeHTTP(w, r)
		requireCode(t, w, tc.code)
	}
	requireCode(t, call(t, f, "GET", "/api/sessions/missing/jobs", ""), 404)
	if err := m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := call(t, f, "GET", prefix+"/jobs", "")
	requireCode(t, w, 503)
	if !strings.Contains(w.Body.String(), `"category":"unavailable"`) {
		t.Fatal(w.Body.String())
	}
}

func TestHTTPInjectSteerAndCancellationWhileWaiting(t *testing.T) {
	echo := &approvalEcho{}
	f := setup(t, echo)
	id := createSession(t, f)
	prefix := "/api/sessions/" + id
	requireCode(t, call(t, f, "POST", prefix+"/inject", `{"text":"背景"}`), 202)
	requireCode(t, call(t, f, "POST", prefix+"/inject", `{"text":"背景","idempotencyKey":"not-supported"}`), 501)
	requireCode(t, call(t, f, "POST", prefix+"/steer", `{"text":"使用工具","idempotencyKey":"steer"}`), 202)
	waitEvent(t, f, id, "approval/requested")
	requireCode(t, call(t, f, "POST", prefix+"/cancel", `{"reason":"取消待审批操作"}`), 202)
	waitEvent(t, f, id, "turn/end")
	if echo.calls.Load() != 0 {
		t.Fatal("取消后执行工具")
	}
}
