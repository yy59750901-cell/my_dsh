package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const ProtocolVersion = "2025-03-26"
const MaxResponseBytes = 1 << 20

var (
	ErrClosed         = errors.New("MCP client closed")
	ErrNotInitialized = errors.New("MCP client not initialized")
	ErrProtocol       = errors.New("invalid or unsupported MCP response")
	ErrResponseLimit  = errors.New("MCP response size limit exceeded")
	ErrRemoteTool     = errors.New("MCP tool reported failure")
)

// Config 仅由可信管理员提供，不得从模型参数、服务端元数据或用户 MCP 配置推导。
// endpoint 始终固定；不跟随重定向、认证发现、SSE endpoint 事件或内容中的 URL。
type Config struct {
	Endpoint      string
	Authorization string
	Timeout       time.Duration
}

type Client struct {
	endpoint      string
	authorization string
	timeout       time.Duration
	http          *http.Client
	transport     *http.Transport
	ctx           context.Context
	cancel        context.CancelFunc
	nextID        atomic.Uint64
	initGate      chan struct{}
	mu            sync.RWMutex
	session       string
	version       string
	initialized   bool
	closed        bool
	closeOnce     sync.Once
	closeErr      error
}

func NewStreamableHTTPClient(config Config) (*Client, error) {
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, errors.New("invalid administrator MCP endpoint")
	}
	if strings.ContainsAny(config.Authorization, "\r\n") {
		return nil, errors.New("invalid authorization header")
	}
	if config.Timeout < 0 || config.Timeout > 2*time.Minute {
		return nil, errors.New("invalid MCP timeout")
	}
	if config.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}
	transport := &http.Transport{
		DialContext:       (&net.Dialer{Timeout: config.Timeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, TLSHandshakeTimeout: config.Timeout, IdleConnTimeout: 90 * time.Second,
		ResponseHeaderTimeout: config.Timeout, MaxResponseHeaderBytes: 32 << 10,
		MaxConnsPerHost: 16, MaxIdleConns: 16,
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{endpoint: u.String(), authorization: config.Authorization, timeout: config.Timeout, transport: transport, initGate: make(chan struct{}, 1),
		http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, ctx: ctx, cancel: cancel}, nil
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func supportedVersion(v string) bool { return v == ProtocolVersion || v == "2025-06-18" }
func validSession(s string) bool {
	if len(s) > 1024 {
		return false
	}
	for _, c := range s {
		if c < 0x21 || c > 0x7e {
			return false
		}
	}
	return true
}

func (c *Client) Initialize(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	select {
	case c.initGate <- struct{}{}:
		defer func() { <-c.initGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	}
	c.mu.RLock()
	closed, initialized, pending := c.closed, c.initialized, c.version != ""
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if initialized {
		return nil
	}
	if pending {
		return errors.New("incomplete MCP initialization; close and recreate client")
	}
	id := c.nextID.Add(1)
	payload := rpcRequest{JSONRPC: "2.0", ID: id, Method: "initialize", Params: map[string]any{
		"protocolVersion": ProtocolVersion, "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "go-dsh", "version": "1"},
	}}
	result, session, err := c.post(ctx, payload, "", "", false)
	if err != nil {
		return err
	}
	var init struct {
		ProtocolVersion string `json:"protocolVersion"`
		Capabilities    struct {
			Tools *struct {
				ListChanged bool `json:"listChanged"`
			} `json:"tools"`
		} `json:"capabilities"`
	}
	if err := json.Unmarshal(result, &init); err != nil || !supportedVersion(init.ProtocolVersion) || init.Capabilities.Tools == nil {
		_ = c.terminateSession(ctx, session, ProtocolVersion)
		return ErrProtocol
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = c.terminateSession(ctx, session, init.ProtocolVersion)
		return ErrClosed
	}
	// 先保存已分配会话，使通知失败或 Close 竞争时仍可释放服务端资源。
	c.session, c.version = session, init.ProtocolVersion
	c.mu.Unlock()
	_, _, err = c.post(ctx, rpcRequest{JSONRPC: "2.0", Method: "notifications/initialized"}, session, init.ProtocolVersion, true)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	c.session, c.version, c.initialized = session, init.ProtocolVersion, true
	return nil
}

func (c *Client) state() (string, string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return "", "", ErrClosed
	}
	if !c.initialized {
		return "", "", ErrNotInitialized
	}
	return c.session, c.version, nil
}

func (c *Client) rpc(ctx context.Context, method string, params any) (json.RawMessage, error) {
	session, version, err := c.state()
	if err != nil {
		return nil, err
	}
	result, _, err := c.post(ctx, rpcRequest{JSONRPC: "2.0", ID: c.nextID.Add(1), Method: method, Params: params}, session, version, false)
	return result, err
}

func (c *Client) headers(req *http.Request, session, version string) {
	req.Header.Set("Accept", "application/json, text/event-stream")
	if req.Method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
	}
	if version != "" {
		req.Header.Set("MCP-Protocol-Version", version)
	}
	if c.authorization != "" {
		req.Header.Set("Authorization", c.authorization)
	}
}

func (c *Client) post(ctx context.Context, payload rpcRequest, session, version string, notification bool) (json.RawMessage, string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	if c.ctx.Err() != nil {
		return nil, "", ErrClosed
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}
	if len(body) > MaxResponseBytes {
		return nil, "", ErrResponseLimit
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, "", errors.New("cannot construct MCP request")
	}
	c.headers(req, session, version)
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, "", ctx.Err()
		}
		return nil, "", errors.New("MCP HTTP request failed")
	}
	defer resp.Body.Close()
	newSession := resp.Header.Get("Mcp-Session-Id")
	if !validSession(newSession) || (version != "" && newSession != "" && newSession != session) {
		return nil, "", ErrProtocol
	}
	if notification {
		if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
			return nil, "", fmt.Errorf("MCP notification HTTP status %d", resp.StatusCode)
		}
		return nil, newSession, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("MCP HTTP status %d", resp.StatusCode)
	}
	media, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, "", ErrProtocol
	}
	var result json.RawMessage
	switch media {
	case "application/json":
		data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
		if err != nil {
			return nil, "", err
		}
		if len(data) > MaxResponseBytes {
			return nil, "", ErrResponseLimit
		}
		var matched bool
		result, matched, err = decodeEnvelope(data, payload.ID)
		if err != nil {
			return nil, "", err
		}
		if !matched {
			return nil, "", ErrProtocol
		}
	case "text/event-stream":
		result, err = readSSE(resp.Body, payload.ID)
		if err != nil {
			return nil, "", err
		}
	default:
		return nil, "", ErrProtocol
	}
	return result, newSession, nil
}

func decodeEnvelope(data []byte, id uint64) (json.RawMessage, bool, error) {
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil || e.JSONRPC != "2.0" {
		return nil, false, ErrProtocol
	}
	if len(e.ID) == 0 && e.Method != "" && e.Result == nil && e.Error == nil {
		return nil, false, nil
	}
	if string(e.ID) != strconv.FormatUint(id, 10) || e.Method != "" || (e.Result == nil) == (e.Error == nil) {
		return nil, false, ErrProtocol
	}
	if e.Error != nil {
		return nil, false, fmt.Errorf("MCP JSON-RPC error %d", e.Error.Code)
	}
	return e.Result, true, nil
}

func readSSE(body io.Reader, id uint64) (json.RawMessage, error) {
	limited := &io.LimitedReader{R: body, N: MaxResponseBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), MaxResponseBytes+1)
	var data strings.Builder
	event := ""
	dispatch := func() (json.RawMessage, bool, error) {
		if data.Len() == 0 {
			event = ""
			return nil, false, nil
		}
		payload := data.String()
		data.Reset()
		kind := event
		event = ""
		if kind != "" && kind != "message" {
			return nil, false, nil
		}
		return decodeEnvelope([]byte(strings.TrimSuffix(payload, "\n")), id)
	}
	for scanner.Scan() {
		if limited.N == 0 {
			return nil, ErrResponseLimit
		}
		line := scanner.Text()
		if line == "" {
			result, done, err := dispatch()
			if err != nil {
				return nil, err
			}
			if done {
				return result, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			value = ""
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "data":
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if limited.N == 0 {
		return nil, ErrResponseLimit
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	// SSE 事件必须用空行结束；不将截断的响应当作成功。
	return nil, ErrProtocol
}

// Close 取消在途请求，并在已建立会话时尝试 DELETE；405 表示服务端不支持会话终止。
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		session, version := c.session, c.version
		c.mu.Unlock()
		c.cancel()
		defer c.transport.CloseIdleConnections()
		c.closeErr = c.terminateSession(context.Background(), session, version)
	})
	return c.closeErr
}

func (c *Client) terminateSession(ctx context.Context, session, version string) error {
	if session == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.endpoint, nil)
	if err != nil {
		return errors.New("cannot construct MCP close request")
	}
	c.headers(req, session, version)
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.New("MCP session close failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusMethodNotAllowed && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("MCP close HTTP status %d", resp.StatusCode)
	}
	return nil
}
