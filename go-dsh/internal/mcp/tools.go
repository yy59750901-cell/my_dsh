package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/yy59750901/go-dsh/internal/tool"
)

const MaxTools = 128
const MaxPages = 16

type ToolDefinition struct {
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema,omitempty"`
}

func (c *Client) ListTools(ctx context.Context) ([]ToolDefinition, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	out := make([]ToolDefinition, 0)
	seenNames := make(map[string]bool)
	seenCursors := make(map[string]bool)
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
		if err := json.Unmarshal(raw, &page); err != nil || page.Tools == nil {
			return nil, ErrProtocol
		}
		for _, d := range page.Tools {
			if d.Name == "" || seenNames[d.Name] || len(out) >= MaxTools {
				return nil, ErrProtocol
			}
			seenNames[d.Name] = true
			out = append(out, d)
		}
		if page.NextCursor == "" {
			return out, nil
		}
		if len(page.NextCursor) > 4096 || seenCursors[page.NextCursor] {
			return nil, ErrProtocol
		}
		seenCursors[page.NextCursor] = true
		cursor = page.NextCursor
	}
	return nil, ErrResponseLimit
}

// Tools 返回可直接执行的固定 handle，使用 Registry 同一 schema 子集和执行防护。
// 不信任服务器的 readOnlyHint：所有远程工具都要求调用方审批。
func (c *Client) Tools(ctx context.Context) ([]tool.Tool, error) {
	definitions, err := c.ListTools(ctx)
	if err != nil {
		return nil, err
	}
	r := tool.NewRegistry()
	out := make([]tool.Tool, 0, len(definitions))
	for _, d := range definitions {
		remote := &remoteTool{client: c, definition: d}
		if err := r.Register(remote); err != nil {
			return nil, err
		}
		h, err := r.Resolve(d.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, nil
}

type remoteTool struct {
	client     *Client
	definition ToolDefinition
}

func (t *remoteTool) Definition() tool.Definition {
	return tool.Definition{Name: t.definition.Name, Description: t.definition.Description, Version: "1",
		InputSchema: append(json.RawMessage(nil), t.definition.InputSchema...), OutputSchema: append(json.RawMessage(nil), t.definition.OutputSchema...), RequiresApproval: true}
}
func (t *remoteTool) Execute(ctx context.Context, call tool.Call) (tool.Result, error) {
	return t.client.CallTool(ctx, t.definition.Name, call.Arguments)
}

func (c *Client) CallTool(ctx context.Context, name string, arguments json.RawMessage) (tool.Result, error) {
	if len(name) == 0 || len(name) > 128 || len(arguments) > tool.MaxArgumentBytes || !json.Valid(arguments) {
		return tool.Result{}, errors.New("invalid MCP tool call")
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
	if err := json.Unmarshal(raw, &response); err != nil || response.Content == nil {
		return tool.Result{}, ErrProtocol
	}
	texts := make([]string, 0, len(response.Content))
	for _, item := range response.Content {
		if item.Type == "text" {
			texts = append(texts, item.Text)
		}
	}
	result := tool.Result{Content: strings.Join(texts, "\n"), Structured: response.StructuredContent}
	if response.IsError {
		result.Err = ErrRemoteTool
		return result, ErrRemoteTool
	}
	return result, nil
}
