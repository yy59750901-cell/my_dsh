package dsh_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/llm"
	"github.com/yy59750901/go-dsh/internal/orchestration"
	"github.com/yy59750901/go-dsh/internal/repository/gormrepo"
	"github.com/yy59750901/go-dsh/internal/tool"
	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
	dsh "github.com/yy59750901/go-dsh/sdk/go"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestClientSQLiteHarness(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "sdk.sqlite")+"?_busy_timeout=5000&_journal_mode=WAL"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sql.Close()
	sql.SetMaxOpenConns(1)
	if err = db.AutoMigrate(&gormrepo.SessionModel{}, &gormrepo.SessionEventModel{}, &gormrepo.SessionProjectionModel{}); err != nil {
		t.Fatal(err)
	}
	tools := tool.NewRegistry()
	if err = tools.Register(tool.NewEchoTool()); err != nil {
		t.Fatal(err)
	}
	h, err := agent.NewHarness(gormrepo.NewEventStore(db), llm.NewDemoProvider(), tools, agent.HarnessOptions{Model: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	m, err := orchestration.NewWithOptions(db, h, orchestration.Options{PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(context.Background())
	token := "sdk-header-secret"
	handler := httpapi.NewHandlerWithHarness(h, httpapi.Options{Mode: "demo", Model: "demo", Token: token, Tools: tools, Orchestration: m})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.String(), token) {
			t.Error("令牌进入 URL")
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	c, err := dsh.NewClient(server.URL, token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := c.Create(ctx, "SDK 会话")
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.Prompt(ctx, s.ID, "使用工具", "same-key")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := c.Prompt(ctx, s.ID, "使用工具", "same-key")
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.AcceptedSeq != r.AcceptedSeq {
		t.Fatal("SDK 幂等回执错误")
	}
	after := uint64(0)
	done := false
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !done {
		page, err := c.Events(ctx, s.ID, after, 3)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Events {
			if e.Seq != after+1 {
				t.Fatal("事件缺口")
			}
			after = e.Seq
			if e.EventType == "turn/end" {
				done = true
			}
		}
		if page.NextAfter != after {
			t.Fatal("错误续传游标")
		}
		if !page.HasMore && !done {
			select {
			case <-tick.C:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	if err = c.Compact(ctx, s.ID, "用户手工摘要：工具已完成", after); err != nil {
		t.Fatal(err)
	}
	compacted, err := c.Events(ctx, s.ID, after, 100)
	if err != nil || len(compacted.Events) != 2 || compacted.Events[0].EventType != agent.EventContextCompacted || compacted.Events[1].EventType != "user/message" || len(compacted.Events[1].SourceEventSeqs) == 0 {
		t.Fatalf("SDK 压缩审计合同错误：%+v %v", compacted, err)
	}
	if err = c.Compact(ctx, s.ID, "用户手工摘要：工具已完成", after); err != nil {
		t.Fatal(err)
	}
	duplicatePage, err := c.Events(ctx, s.ID, compacted.NextAfter, 100)
	if err != nil || len(duplicatePage.Events) != 0 {
		t.Fatalf("SDK 重复摘要不是幂等：%v", err)
	}
	for _, tc := range []struct {
		id, summary, category string
		status                int
	}{
		{s.ID, "", "context_summary", 409},
		{"missing", "手工摘要", "not_found", 404},
	} {
		err := c.Compact(ctx, tc.id, tc.summary, after)
		var apiErr *dsh.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != tc.status || apiErr.Category != tc.category {
			t.Fatalf("SDK 压缩错误合同：%v", err)
		}
	}
	if err = c.Compact(ctx, "../escape", "摘要", after); err == nil {
		t.Fatal("SDK 接受了无效会话路径")
	}
	if err = c.Compact(ctx, s.ID, "摘要", 0); err == nil {
		t.Fatal("SDK 接受了零 head")
	}
	var changed *dsh.APIError
	err = c.Compact(ctx, s.ID, "用户手工摘要：工具已完成", compacted.NextAfter)
	if !errors.As(err, &changed) || changed.StatusCode != 409 || changed.Category != "context_changed" {
		t.Fatalf("SDK 重试不能换用压缩后 head：%v", err)
	}
	if err = c.Cancel(ctx, s.ID, "用户取消"); err != nil {
		t.Fatal(err)
	}
	_, err = c.Events(ctx, "missing", 0, 1)
	var apiErr *dsh.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 || apiErr.Category != "not_found" {
		t.Fatalf("错误契约=%v", err)
	}
	bad, _ := dsh.NewClient(server.URL, "bad", nil)
	_, err = bad.Create(ctx, "no")
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Fatalf("令牌校验=%v", err)
	}
	testClientOrchestration(t, c, m, s.ID)
}

func testClientOrchestration(t *testing.T, c *dsh.Client, m *orchestration.Manager, parent string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	job, err := c.CreateChild(ctx, parent, "SDK 子任务", "sdk-child")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := c.CreateChild(ctx, parent, "SDK 子任务", "sdk-child")
	if err != nil || duplicate.ID != job.ID {
		t.Fatalf("SDK 子任务幂等：%v", err)
	}
	flow, err := c.CreateWorkflow(ctx, parent, []string{"SDK 节点一", "SDK 节点二"}, "sdk-flow")
	if err != nil {
		t.Fatal(err)
	}
	flowAgain, err := c.CreateWorkflow(ctx, parent, []string{"SDK 节点一", "SDK 节点二"}, "sdk-flow")
	if err != nil || flowAgain.ID != flow.ID {
		t.Fatalf("SDK 工作流幂等：%v", err)
	}
	other, err := c.Create(ctx, "错误父会话")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetJob(ctx, other.ID, job.ID)
	var apiErr *dsh.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("SDK 错误 job parent：%v", err)
	}
	_, err = c.GetWorkflow(ctx, other.ID, flow.ID)
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("SDK 错误 workflow parent：%v", err)
	}
	if _, err = c.GetJob(ctx, parent, "../escape"); err == nil {
		t.Fatal("SDK 未验证 jobID")
	}
	if _, err = c.GetWorkflow(ctx, parent, "../escape"); err == nil {
		t.Fatal("SDK 未验证 workflowID")
	}
	if _, err = c.CreateChild(ctx, parent, "x", ""); err == nil {
		t.Fatal("SDK 未验证 key")
	}
	if _, err = c.CreateWorkflow(ctx, parent, []string{"x"}, ""); err == nil {
		t.Fatal("SDK 未验证 workflow key")
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- m.Run(runCtx) }()
	defer func() {
		stop()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("Manager 未结束")
		}
	}()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		job, err = c.GetJob(ctx, parent, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		flow, err = c.GetWorkflow(ctx, parent, flow.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == "succeeded" && flow.State == "succeeded" {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("SDK 编排没有进入终态")
		}
	}
	if job.ChildSessionID == "" || job.EndSeq <= job.AcceptedSeq || flow.CurrentStep != 2 || len(flow.Jobs) != 2 || flow.Jobs[1].State != "succeeded" {
		t.Fatalf("SDK 编排映射错误：%+v %+v", job, flow)
	}
}
func TestCompactPreservesUint64AndRetryBody(t *testing.T) {
	requests := make(chan map[string]string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/sessions/fixed/compact" || r.URL.RawQuery != "" || r.Header.Get("Authorization") != "Bearer sdk-secret" {
			t.Error("压缩请求路径或认证错误")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"category": "unavailable"}})
	}))
	defer server.Close()
	c, err := dsh.NewClient(server.URL, "sdk-secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		err := c.Compact(context.Background(), "fixed", "  原始人工摘要  ", ^uint64(0))
		var apiErr *dsh.APIError
		if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
			t.Fatalf("重试返回=%v", err)
		}
		body := <-requests
		if len(body) != 2 || body["summary"] != "  原始人工摘要  " || body["expected_head"] != "18446744073709551615" {
			t.Fatalf("重试改变摘要或丢失 uint64 精度：%v", body)
		}
	}
}

func TestClientDoesNotRedirectCredentials(t *testing.T) {
	touched := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { touched = true }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	c, err := dsh.NewClient(server.URL, "private", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Create(context.Background(), "redirect")
	if err == nil || touched {
		t.Fatal("自动重定向了认证请求")
	}
	for _, base := range []string{"file:///tmp/data", "http://user:pass@localhost", "http://localhost?token=x", "http://localhost/api"} {
		if _, err := dsh.NewClient(base, "", nil); err == nil {
			t.Fatalf("接受了无效地址 %s", base)
		}
	}
}
