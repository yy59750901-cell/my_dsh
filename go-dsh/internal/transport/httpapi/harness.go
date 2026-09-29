package httpapi

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/orchestration"
	"github.com/yy59750901/go-dsh/internal/session"
	"github.com/yy59750901/go-dsh/internal/tool"
)

//go:embed index.html
var indexHTML string

type Options struct {
	Token         string
	Mode          string
	Model         string
	Tools         *tool.Registry
	Orchestration *orchestration.Manager
}

type api struct {
	h       *agent.Harness
	options Options
}

const maxBody = 1 << 20

func NewHandlerWithHarness(h *agent.Harness, options Options) http.Handler {
	a := &api{h: h, options: options}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, indexHTML)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { a.json(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /api/info", a.info)
	mux.HandleFunc("GET /api/sessions", a.sessions)
	mux.HandleFunc("POST /api/sessions", a.create)
	mux.HandleFunc("GET /api/sessions/{id}", a.get)
	mux.HandleFunc("POST /api/sessions/{id}/prompts", a.input)
	mux.HandleFunc("POST /api/sessions/{id}/steer", a.input)
	mux.HandleFunc("POST /api/sessions/{id}/inject", a.input)
	mux.HandleFunc("POST /api/sessions/{id}/cancel", a.cancel)
	mux.HandleFunc("POST /api/sessions/{id}/compact", a.compact)
	mux.HandleFunc("POST /api/sessions/{id}/approvals/{approvalID}", a.approve)
	mux.HandleFunc("GET /api/sessions/{id}/events", a.events)
	mux.HandleFunc("GET /api/sessions/{id}/stream", a.stream)
	mux.HandleFunc("GET /api/sessions/{id}/context", a.context)
	mux.HandleFunc("GET /api/tools", a.tools)
	for pattern, handler := range map[string]http.HandlerFunc{
		"GET /api/sessions/{id}/jobs":                           a.jobs,
		"POST /api/sessions/{id}/jobs":                          a.createChild,
		"GET /api/sessions/{id}/jobs/{jobID}":                   a.job,
		"POST /api/sessions/{id}/jobs/{jobID}/cancel":           a.cancelJob,
		"GET /api/sessions/{id}/schedules":                      a.schedules,
		"POST /api/sessions/{id}/schedules":                     a.createSchedule,
		"POST /api/sessions/{id}/schedules/{scheduleID}/cancel": a.cancelSchedule,
		"GET /api/sessions/{id}/workflows":                      a.workflows,
		"POST /api/sessions/{id}/workflows":                     a.createWorkflow,
		"GET /api/sessions/{id}/workflows/{workflowID}":         a.workflow,
		"POST /api/sessions/{id}/workflows/{workflowID}/pause":  a.workflowControl,
		"POST /api/sessions/{id}/workflows/{workflowID}/resume": a.workflowControl,
		"POST /api/sessions/{id}/workflows/{workflowID}/cancel": a.workflowControl,
		"POST /api/sessions/{id}/cancel-tree":                   a.cancelTree,
	} {
		mux.HandleFunc(pattern, a.withOrchestration(handler))
	}
	script := strings.SplitN(strings.SplitN(indexHTML, "<script>", 2)[1], "</script>", 2)[0]
	hash := sha256.Sum256([]byte(script))
	csp := "default-src 'none'; script-src 'sha256-" + base64.StdEncoding.EncodeToString(hash[:]) + "'; style-src 'unsafe-inline'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", csp)
		if !sameOrigin(r) {
			a.failure(w, 403, "forbidden")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if !BearerAuthorized(r.Header.Values("Authorization"), options.Token) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="local"`)
				a.failure(w, 401, "unauthenticated")
				return
			}
			if h == nil {
				a.failure(w, 503, "unavailable")
				return
			}
		}
		// 流使用独立写期限和请求取消；不套 REST deadline/TimeoutHandler。
		if strings.HasSuffix(r.URL.Path, "/stream") {
			mux.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a *api) json(w http.ResponseWriter, code int, value any) {
	raw, err := json.Marshal(value)
	if err == nil {
		raw, err = RedactJSON(raw, a.options.Token)
	}
	if err != nil {
		a.failure(w, 500, "internal")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(append(raw, '\n'))
}
func (a *api) failure(w http.ResponseWriter, code int, category string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"category": category, "message": category}})
}
func (a *api) err(w http.ResponseWriter, err error) {
	code, kind := 500, "internal"
	switch {
	case errors.Is(err, orchestration.ErrBusy):
		code, kind = 503, "busy"
	case errors.Is(err, orchestration.ErrClosed), errors.Is(err, orchestration.ErrStorage), errors.Is(err, orchestration.ErrExecution), errors.Is(err, orchestration.ErrVersion):
		code, kind = 503, "unavailable"
	case errors.Is(err, orchestration.ErrInvalid):
		code, kind = 400, "validation"
	case errors.Is(err, orchestration.ErrConflict):
		code, kind = 409, "conflict"
	case errors.Is(err, orchestration.ErrState):
		code, kind = 409, "state"
	case errors.Is(err, session.ErrNotFound), errors.Is(err, agent.ErrApprovalNotFound), errors.Is(err, orchestration.ErrNotFound):
		code, kind = 404, "not_found"
	case errors.Is(err, agent.ErrInvalidInboxRequest), errors.Is(err, session.ErrInvalidSession):
		code, kind = 400, "validation"
	case errors.Is(err, agent.ErrIdempotencyConflict), errors.Is(err, session.ErrConflict):
		code, kind = 409, "conflict"
	case errors.Is(err, agent.ErrContextBusy):
		code, kind = 409, "context_busy"
	case errors.Is(err, agent.ErrContextChanged):
		code, kind = 409, "context_changed"
	case errors.Is(err, agent.ErrContextEmpty):
		code, kind = 409, "context_empty"
	case errors.Is(err, agent.ErrInvalidContextSummary):
		code, kind = 409, "context_summary"
	case errors.Is(err, agent.ErrApprovalResolved), errors.Is(err, agent.ErrApprovalNotPending):
		code, kind = 409, "approval"
	case errors.Is(err, context.DeadlineExceeded):
		code, kind = 504, "deadline"
	case errors.Is(err, context.Canceled):
		code, kind = 408, "cancelled"
	case errors.Is(err, agent.ErrHarnessClosed), errors.Is(err, session.ErrRegistryClosed):
		code, kind = 503, "unavailable"
	}
	a.failure(w, code, kind)
}
func (a *api) decode(w http.ResponseWriter, r *http.Request, out any) bool {
	typ, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || typ != "application/json" {
		a.failure(w, 415, "content_type")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			a.failure(w, 413, "body_limit")
		} else {
			a.failure(w, 400, "validation")
		}
		return false
	}
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		a.failure(w, 400, "validation")
		return false
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if dec.Decode(out) != nil {
		a.failure(w, 400, "validation")
		return false
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		a.failure(w, 400, "validation")
		return false
	}
	return true
}
func (a *api) exists(w http.ResponseWriter, r *http.Request) bool {
	_, err := FindSession(r.Context(), a.h, r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return false
	}
	return true
}
func (a *api) info(w http.ResponseWriter, r *http.Request) {
	mode := a.options.Mode
	if mode == "" {
		mode = "unknown"
	}
	label := "真实模型"
	if mode == "demo" {
		label = "演示模式（非真实 LLM）"
	} else if mode == "unknown" {
		label = "模式未配置"
	}
	a.json(w, 200, map[string]any{"mode": mode, "model": a.options.Model, "label": label, "demo": mode == "demo", "authRequired": a.options.Token != "", "orchestrationAvailable": a.options.Orchestration != nil})
}
func (a *api) sessions(w http.ResponseWriter, r *http.Request) {
	list, err := a.h.ListSessions(r.Context())
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]any{"sessions": list})
}
func (a *api) create(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Title string `json:"title"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	if len(input.Title) > 512 {
		a.failure(w, 400, "validation")
		return
	}
	id, err := a.h.CreateSession(r.Context(), input.Title)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 201, agent.SessionInfo{ID: id, Title: input.Title})
}
func (a *api) get(w http.ResponseWriter, r *http.Request) {
	info, err := FindSession(r.Context(), a.h, r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return
	}
	snap, _, err := ReadSnapshot(r.Context(), a.h, info.ID)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]any{"id": info.ID, "title": info.Title, "status": SnapshotStatus(snap)})
}
func (a *api) input(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Text           string `json:"text"`
		IdempotencyKey string `json:"idempotencyKey"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key != "" && input.IdempotencyKey != "" && key != input.IdempotencyKey {
		a.failure(w, 400, "validation")
		return
	}
	if key == "" {
		key = input.IdempotencyKey
	}
	if strings.TrimSpace(input.Text) == "" || len(key) > 256 {
		a.failure(w, 400, "validation")
		return
	}
	if !a.exists(w, r) {
		return
	}
	var receipt agent.Receipt
	var err error
	switch {
	case strings.HasSuffix(r.URL.Path, "/inject"):
		if key != "" {
			a.failure(w, 501, "unimplemented")
			return
		}
		err = a.h.Inject(r.Context(), r.PathValue("id"), input.Text)
		if err != nil {
			a.err(w, err)
			return
		}
		a.json(w, 202, map[string]bool{"accepted": true})
		return
	case strings.HasSuffix(r.URL.Path, "/steer"):
		receipt, err = a.h.Steer(r.Context(), r.PathValue("id"), input.Text, key)
	default:
		receipt, err = a.h.Prompt(r.Context(), r.PathValue("id"), input.Text, key)
	}
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, map[string]any{"receipt": map[string]any{"requestId": receipt.RequestID, "sessionId": receipt.SessionID, "commandId": receipt.CommandID, "acceptedSeq": strconv.FormatUint(receipt.AcceptedSeq, 10), "placement": receipt.Placement, "duplicate": receipt.Duplicate, "acceptedAt": receipt.AcceptedAt, "inputId": receipt.InputID, "target": receipt.Target, "idempotencyKey": receipt.IdempotencyKey}})
}
func (a *api) cancel(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Reason string `json:"reason"`
	}
	if !a.decode(w, r, &input) || !a.exists(w, r) {
		return
	}
	if err := a.h.Cancel(r.Context(), r.PathValue("id"), input.Reason); err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, map[string]bool{"accepted": true})
}

type compactInput struct {
	Summary      string
	ExpectedHead uint64
}

func (input *compactInput) UnmarshalJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return session.ErrInvalidSession
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if first, err := dec.Token(); err != nil || first != json.Delim('{') {
		return session.ErrInvalidSession
	}
	fields := map[string]string{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil || (key != "summary" && key != "expected_head") {
			return session.ErrInvalidSession
		}
		name := key.(string)
		if _, exists := fields[name]; exists {
			return session.ErrInvalidSession
		}
		var value *string
		if err := dec.Decode(&value); err != nil || value == nil {
			return session.ErrInvalidSession
		}
		fields[name] = *value
	}
	if _, err := dec.Token(); err != nil || len(fields) != 2 {
		return session.ErrInvalidSession
	}
	// 十进制字符串避免浏览器 Number 损失 uint64 精度；不接受隐式当前版本。
	head, err := strconv.ParseUint(fields["expected_head"], 10, 64)
	if err != nil || head == 0 || strconv.FormatUint(head, 10) != fields["expected_head"] {
		return session.ErrInvalidSession
	}
	input.Summary, input.ExpectedHead = fields["summary"], head
	return nil
}

func (a *api) compact(w http.ResponseWriter, r *http.Request) {
	var input compactInput
	if !a.decode(w, r, &input) || !a.exists(w, r) {
		return
	}
	if err := a.h.CompactAt(r.Context(), r.PathValue("id"), input.Summary, input.ExpectedHead); err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]bool{"accepted": true})
}

func (a *api) approve(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Allow *bool `json:"allow"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	if input.Allow == nil {
		a.failure(w, 400, "validation")
		return
	}
	if !a.exists(w, r) {
		return
	}
	if err := a.h.ResolveApproval(r.Context(), r.PathValue("id"), r.PathValue("approvalID"), *input.Allow); err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]bool{"accepted": true})
}
func cursor(r *http.Request, stream bool) (uint64, int, error) {
	after := uint64(0)
	limit := 1000
	for key, values := range r.URL.Query() {
		if (key != "after" && (key != "limit" || stream)) || len(values) != 1 {
			return 0, 0, session.ErrInvalidSession
		}
	}
	if value, ok := r.URL.Query()["after"]; ok {
		n, err := strconv.ParseUint(value[0], 10, 64)
		if err != nil {
			return 0, 0, session.ErrInvalidSession
		}
		after = n
	}
	if stream && len(r.Header.Values("Last-Event-ID")) > 1 {
		return 0, 0, session.ErrInvalidSession
	}
	if value := r.Header.Get("Last-Event-ID"); stream && value != "" {
		n, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0, 0, session.ErrInvalidSession
		}
		after = max(after, n)
	}
	if value, ok := r.URL.Query()["limit"]; ok {
		n, err := strconv.Atoi(value[0])
		if err != nil || n < 1 || n > 1000 {
			return 0, 0, session.ErrInvalidSession
		}
		limit = n
	}
	return after, limit, nil
}
func (a *api) events(w http.ResponseWriter, r *http.Request) {
	after, limit, err := cursor(r, false)
	if err != nil {
		a.err(w, err)
		return
	}
	if !a.exists(w, r) {
		return
	}
	events, err := a.h.ListEvents(r.Context(), r.PathValue("id"), after, limit)
	if err != nil {
		a.err(w, err)
		return
	}
	if events == nil {
		events = []session.Event{}
	}
	if len(events) > 0 {
		after = events[len(events)-1].Seq
	}
	more, err := a.h.ListEvents(r.Context(), r.PathValue("id"), after, 1)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]any{"events": events, "nextAfter": strconv.FormatUint(after, 10), "hasMore": len(more) > 0})
}
func (a *api) context(w http.ResponseWriter, r *http.Request) {
	s, _, err := ReadSnapshot(r.Context(), a.h, r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return
	}
	messages, err := s.DeriveMessages()
	if err != nil {
		a.err(w, err)
		return
	}
	if messages == nil {
		messages = []json.RawMessage{}
	}
	view, err := a.h.ContextPreviewFromSnapshot(s)
	if err != nil {
		a.err(w, err)
		return
	}
	sources := make([]string, len(view.SourceEventSeqs))
	for i, seq := range view.SourceEventSeqs {
		sources[i] = strconv.FormatUint(seq, 10)
	}
	a.json(w, 200, map[string]any{
		"sessionId": s.SessionID, "headSeq": strconv.FormatUint(s.HeadSeq, 10), "messages": messages,
		"replaceGeneration": strconv.FormatUint(view.ReplaceGeneration, 10), "sourceEventSeqs": sources,
		"tools": view.Tools, "budget": view.Budget,
		"budgetExplanation": "本地保守估算：消息按完整 JSON 的 UTF-8 字节数 +16，工具定义 +32，请求开销 64，输出单独预留；不是供应商 tokenizer、实际 usage 或窗口保证。预算包含系统提示，但 messages 保持仅持久化 Surface 的兼容合同。",
	})
}
func (a *api) tools(w http.ResponseWriter, r *http.Request) {
	list := []any{}
	if a.options.Tools != nil {
		for _, d := range a.options.Tools.Definitions() {
			list = append(list, map[string]any{"name": d.Name, "description": d.Description, "inputSchema": d.InputSchema, "outputSchema": d.OutputSchema, "version": d.Version, "generation": strconv.FormatUint(d.Generation, 10), "requiredCapabilities": d.RequiredCapabilities, "requiresApproval": d.RequiresApproval})
		}
	}
	a.json(w, 200, map[string]any{"tools": list})
}
func (a *api) withOrchestration(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.options.Orchestration == nil {
			a.failure(w, 503, "unavailable")
			return
		}
		// 资源级归属仍由每个 Manager 方法使用 URL 中的 parentID 验证。
		if !a.exists(w, r) {
			return
		}
		next(w, r)
	}
}

func (a *api) jobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.options.Orchestration.Jobs(r.Context(), r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]any{"jobs": jobs})
}

func (a *api) createChild(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Prompt string `json:"prompt"`
		Key    string `json:"idempotency_key"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	job, err := a.options.Orchestration.CreateChild(r.Context(), r.PathValue("id"), input.Prompt, input.Key)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, job)
}

func (a *api) job(w http.ResponseWriter, r *http.Request) {
	job, err := a.options.Orchestration.GetJob(r.Context(), r.PathValue("id"), r.PathValue("jobID"))
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, job)
}

func (a *api) cancelJob(w http.ResponseWriter, r *http.Request) {
	if !a.decode(w, r, &struct{}{}) {
		return
	}
	if err := a.options.Orchestration.CancelJob(r.Context(), r.PathValue("id"), r.PathValue("jobID")); err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, map[string]bool{"accepted": true})
}

func (a *api) schedules(w http.ResponseWriter, r *http.Request) {
	list, err := a.options.Orchestration.Schedules(r.Context(), r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, map[string]any{"schedules": list})
}

func (a *api) createSchedule(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Text            string `json:"text"`
		Key             string `json:"idempotency_key"`
		Due             string `json:"due"`
		IntervalSeconds *int64 `json:"interval_seconds"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	due, err := time.Parse(time.RFC3339, input.Due)
	if err != nil || input.IntervalSeconds == nil || *input.IntervalSeconds < 0 || *input.IntervalSeconds > 365*24*60*60 {
		a.failure(w, 400, "validation")
		return
	}
	schedule, err := a.options.Orchestration.CreateSchedule(r.Context(), r.PathValue("id"), input.Text, input.Key, due, time.Duration(*input.IntervalSeconds)*time.Second)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, schedule)
}

func (a *api) cancelSchedule(w http.ResponseWriter, r *http.Request) {
	if !a.decode(w, r, &struct{}{}) {
		return
	}
	if err := a.options.Orchestration.CancelSchedule(r.Context(), r.PathValue("id"), r.PathValue("scheduleID")); err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, map[string]bool{"accepted": true})
}

func (a *api) workflows(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.options.Orchestration.Jobs(r.Context(), r.PathValue("id"))
	if err != nil {
		a.err(w, err)
		return
	}
	list, seen := []orchestration.Workflow{}, map[string]bool{}
	for _, job := range jobs {
		if job.WorkflowID == "" || seen[job.WorkflowID] {
			continue
		}
		seen[job.WorkflowID] = true
		workflow, err := a.options.Orchestration.GetWorkflow(r.Context(), r.PathValue("id"), job.WorkflowID)
		if err != nil {
			a.err(w, err)
			return
		}
		list = append(list, workflow)
	}
	// 没有首个 job 的工作流无法由此列表发现，不伪装成完整目录。
	a.json(w, 200, map[string]any{"workflows": list, "partial": true, "source": "jobs"})
}

func (a *api) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Steps []string `json:"steps"`
		Key   string   `json:"key"`
	}
	if !a.decode(w, r, &input) {
		return
	}
	workflow, err := a.options.Orchestration.CreateWorkflow(r.Context(), r.PathValue("id"), input.Steps, input.Key)
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, workflow)
}

func (a *api) workflow(w http.ResponseWriter, r *http.Request) {
	workflow, err := a.options.Orchestration.GetWorkflow(r.Context(), r.PathValue("id"), r.PathValue("workflowID"))
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 200, workflow)
}

func (a *api) workflowControl(w http.ResponseWriter, r *http.Request) {
	if !a.decode(w, r, &struct{}{}) {
		return
	}
	m, ctx, parentID, id := a.options.Orchestration, r.Context(), r.PathValue("id"), r.PathValue("workflowID")
	var err error
	switch {
	case strings.HasSuffix(r.URL.Path, "/pause"):
		err = m.PauseWorkflow(ctx, parentID, id)
	case strings.HasSuffix(r.URL.Path, "/resume"):
		err = m.ResumeWorkflow(ctx, parentID, id)
	default:
		err = m.CancelWorkflow(ctx, parentID, id)
	}
	if err != nil {
		a.err(w, err)
		return
	}
	a.json(w, 202, map[string]bool{"accepted": true})
}

func (a *api) cancelTree(w http.ResponseWriter, r *http.Request) {
	if !a.decode(w, r, &struct{}{}) {
		return
	}
	if err := a.options.Orchestration.CancelChildren(r.Context(), r.PathValue("id")); err != nil {
		a.err(w, err)
		return
	}
	// 与普通父 Turn 取消分离；这里只关闭编排准入，不取消父 Turn。
	a.json(w, 202, map[string]bool{"accepted": true, "admission_closed": true})
}

func (a *api) stream(w http.ResponseWriter, r *http.Request) {
	after, _, err := cursor(r, true)
	if err != nil {
		a.err(w, err)
		return
	}
	if !a.exists(w, r) {
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		a.failure(w, 500, "stream_unavailable")
		return
	}
	ctl := http.NewResponseController(w)
	_ = ctl.SetWriteDeadline(time.Time{})
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	write := func(s string) bool {
		_ = ctl.SetWriteDeadline(time.Now().Add(15 * time.Second))
		_, err := io.WriteString(w, s)
		if err == nil {
			err = ctl.Flush()
		}
		_ = ctl.SetWriteDeadline(time.Time{})
		return err == nil
	}
	if !write(": connected\n\n") {
		return
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	for {
		events, err := a.h.ListEvents(r.Context(), r.PathValue("id"), after, 1000)
		if err != nil {
			if r.Context().Err() == nil {
				write("event: error\ndata: {\"error\":{\"category\":\"unavailable\"}}\n\n")
			}
			return
		}
		for _, e := range events {
			if e.Seq <= after {
				continue
			}
			raw, err := json.Marshal(e)
			if err == nil {
				raw, err = RedactJSON(raw, a.options.Token)
			}
			if err != nil || !write(fmt.Sprintf("id: %d\nevent: event\ndata: %s\n\n", e.Seq, raw)) {
				return
			}
			after = e.Seq
		}
		if len(events) == 1000 {
			continue
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		case <-heartbeat.C:
			if !write(": keepalive\n\n") {
				return
			}
		}
	}
}
