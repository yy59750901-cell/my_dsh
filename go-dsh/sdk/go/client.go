package dsh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	base  string
	token string
	http  *http.Client
}

// NewClient 不把令牌放进 URL，并禁止自动重定向携带认证信息。
func NewClient(baseURL, token string, httpClient *http.Client) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("无效的服务地址")
	}
	if strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("无效的访问令牌")
	}
	client := http.Client{Timeout: 30 * time.Second}
	if httpClient != nil {
		client = *httpClient
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: strings.TrimSuffix(u.String(), "/"), token: token, http: &client}, nil
}

type Session struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}
type Receipt struct {
	RequestID      string    `json:"requestId"`
	SessionID      string    `json:"sessionId"`
	CommandID      string    `json:"commandId"`
	AcceptedSeq    uint64    `json:"acceptedSeq,string"`
	Duplicate      bool      `json:"duplicate"`
	AcceptedAt     time.Time `json:"acceptedAt"`
	InputID        string    `json:"inputId"`
	Placement      string    `json:"placement"`
	Target         string    `json:"target"`
	IdempotencyKey string    `json:"idempotencyKey"`
}
type Event struct {
	SchemaVersion struct {
		Major uint32 `json:"major"`
		Minor uint32 `json:"minor"`
	} `json:"schemaVersion"`
	EventType        string          `json:"eventType"`
	EventID          string          `json:"eventId"`
	SessionID        string          `json:"sessionId"`
	Seq              uint64          `json:"seq,string"`
	OccurredAt       time.Time       `json:"occurredAt"`
	CommittedAt      time.Time       `json:"committedAt"`
	ReplayPolicy     string          `json:"replayPolicy"`
	Data             json.RawMessage `json:"data"`
	TurnID           string          `json:"turnId,omitempty"`
	StepID           string          `json:"stepId,omitempty"`
	CallID           string          `json:"callId,omitempty"`
	Trace            json.RawMessage `json:"trace,omitempty"`
	CausationEventID string          `json:"causationEventId,omitempty"`
	SourceEventSeqs  []uint64        `json:"sourceEventSeqs,omitempty"`
	SurfaceOp        json.RawMessage `json:"surfaceOp,omitempty"`
	Extensions       json.RawMessage `json:"extensions,omitempty"`
}
type EventsPage struct {
	Events    []Event `json:"events"`
	NextAfter uint64  `json:"nextAfter,string"`
	HasMore   bool    `json:"hasMore"`
}
type Job struct {
	ID              string    `json:"id"`
	ParentID        string    `json:"parent_id"`
	ChildSessionID  string    `json:"child_session_id"`
	WorkflowID      string    `json:"workflow_id,omitempty"`
	StepIndex       int       `json:"step_index"`
	Prompt          string    `json:"prompt"`
	State           string    `json:"state"`
	CancelRequested bool      `json:"cancel_requested"`
	AcceptedSeq     uint64    `json:"accepted_seq"`
	TurnID          string    `json:"turn_id,omitempty"`
	EndSeq          uint64    `json:"end_seq"`
	Outcome         string    `json:"outcome,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type Workflow struct {
	ID          string    `json:"id"`
	ParentID    string    `json:"parent_id"`
	Steps       []string  `json:"steps"`
	State       string    `json:"state"`
	CurrentStep int       `json:"current_step"`
	Jobs        []Job     `json:"jobs"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// CreateChild 的调用方需在重试时保留原 prompt 和 key。
func (c *Client) CreateChild(ctx context.Context, parentID, prompt, key string) (Job, error) {
	path, err := sessionPath(parentID)
	if err != nil {
		return Job{}, err
	}
	if strings.TrimSpace(key) == "" || len(key) > 256 {
		return Job{}, errors.New("需要有效且稳定的幂等键")
	}
	var result Job
	err = c.do(ctx, http.MethodPost, path+"/jobs", map[string]string{"prompt": prompt, "idempotency_key": key}, &result, "")
	return result, err
}

func (c *Client) GetJob(ctx context.Context, parentID, jobID string) (Job, error) {
	path, err := sessionPath(parentID)
	if err != nil {
		return Job{}, err
	}
	if _, err := sessionPath(jobID); err != nil {
		return Job{}, err
	}
	var result Job
	err = c.do(ctx, http.MethodGet, path+"/jobs/"+url.PathEscape(jobID), nil, &result, "")
	return result, err
}

// CreateWorkflow 创建顺序节点，重试必须复用原 steps 和 key。
func (c *Client) CreateWorkflow(ctx context.Context, parentID string, steps []string, key string) (Workflow, error) {
	path, err := sessionPath(parentID)
	if err != nil {
		return Workflow{}, err
	}
	if strings.TrimSpace(key) == "" || len(key) > 256 {
		return Workflow{}, errors.New("需要有效且稳定的幂等键")
	}
	var result Workflow
	err = c.do(ctx, http.MethodPost, path+"/workflows", map[string]any{"steps": steps, "key": key}, &result, "")
	return result, err
}

func (c *Client) GetWorkflow(ctx context.Context, parentID, workflowID string) (Workflow, error) {
	path, err := sessionPath(parentID)
	if err != nil {
		return Workflow{}, err
	}
	if _, err := sessionPath(workflowID); err != nil {
		return Workflow{}, err
	}
	var result Workflow
	err = c.do(ctx, http.MethodGet, path+"/workflows/"+url.PathEscape(workflowID), nil, &result, "")
	return result, err
}

type APIError struct {
	StatusCode int
	Category   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("DSH 请求失败：HTTP %d (%s)", e.StatusCode, e.Category)
}

func (c *Client) do(ctx context.Context, method, path string, input, output any, key string) error {
	var body io.Reader
	if input != nil {
		raw, err := json.Marshal(input)
		if err != nil {
			return errors.New("请求编码失败")
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return errors.New("请求构造失败")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("DSH 连接失败")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var payload struct {
			Error struct {
				Category string `json:"category"`
			} `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&payload)
		category := "transport"
		switch payload.Error.Category {
		case "validation", "not_found", "unauthenticated", "forbidden", "conflict", "approval", "deadline", "cancelled", "unavailable", "internal", "content_type", "body_limit", "unimplemented", "context_busy", "context_changed", "context_empty", "context_summary", "busy", "state":
			category = payload.Error.Category
		}
		return &APIError{StatusCode: resp.StatusCode, Category: category}
	}
	if output == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20+1))
	if err != nil || len(raw) > 32<<20 {
		return errors.New("响应读取失败或超过限制")
	}
	if err = json.Unmarshal(raw, output); err != nil {
		return errors.New("无效的服务响应")
	}
	return nil
}
func sessionPath(id string) (string, error) {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\\r\n") {
		return "", errors.New("无效的会话 ID")
	}
	return "/api/sessions/" + url.PathEscape(id), nil
}
func (c *Client) Create(ctx context.Context, title string) (Session, error) {
	var result Session
	err := c.do(ctx, http.MethodPost, "/api/sessions", map[string]string{"title": title}, &result, "")
	return result, err
}

// Prompt 的调用者必须在同一次发送的所有重试中复用 key。
func (c *Client) Prompt(ctx context.Context, id, text, key string) (Receipt, error) {
	path, err := sessionPath(id)
	if err != nil {
		return Receipt{}, err
	}
	if key == "" || len(key) > 256 || strings.ContainsAny(key, "\r\n") {
		return Receipt{}, errors.New("需要有效且稳定的幂等键")
	}
	var result struct {
		Receipt Receipt `json:"receipt"`
	}
	err = c.do(ctx, http.MethodPost, path+"/prompts", map[string]string{"text": text}, &result, key)
	return result.Receipt, err
}
func (c *Client) Cancel(ctx context.Context, id, reason string) error {
	path, err := sessionPath(id)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, path+"/cancel", map[string]string{"reason": reason}, nil, "")
}

// Compact 提交人工摘要和预览时的 head；所有重试必须复用原摘要与 head。
func (c *Client) Compact(ctx context.Context, id, summary string, expectedHead uint64) error {
	path, err := sessionPath(id)
	if err != nil {
		return err
	}
	if expectedHead == 0 {
		return errors.New("需要预览时的非零 head")
	}
	return c.do(ctx, http.MethodPost, path+"/compact", map[string]string{"summary": summary, "expected_head": strconv.FormatUint(expectedHead, 10)}, nil, "")
}

func (c *Client) Events(ctx context.Context, id string, after uint64, limit int) (EventsPage, error) {
	path, err := sessionPath(id)
	if err != nil {
		return EventsPage{}, err
	}
	if limit < 1 || limit > 1000 {
		return EventsPage{}, errors.New("limit 必须在 1 到 1000 之间")
	}
	var result EventsPage
	err = c.do(ctx, http.MethodGet, path+"/events?after="+strconv.FormatUint(after, 10)+"&limit="+strconv.Itoa(limit), nil, &result, "")
	return result, err
}
