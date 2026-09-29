package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/yy59750901/go-dsh/internal/tool"
)

// NewWorkspaceTools 返回共享固定根句柄的三个工具。调用方可将任一个工具断言为
// io.Closer 并 Close（同时关闭整组工具），或使用 Open + Tools + Workspace.Close。
func NewWorkspaceTools(root string) ([]tool.Tool, error) {
	w, err := Open(root)
	if err != nil {
		return nil, err
	}
	return w.Tools(), nil
}

func (w *Workspace) Tools() []tool.Tool {
	return []tool.Tool{&fileTool{w: w, name: "list_files"}, &fileTool{w: w, name: "read_file"}, &fileTool{w: w, name: "write_file"}}
}

type fileTool struct {
	w    *Workspace
	name string
}

func (f *fileTool) Close() error { return f.w.Close() }
func (f *fileTool) Definition() tool.Definition {
	d := tool.Definition{Name: f.name, Version: "1"}
	switch f.name {
	case "list_files":
		d.Description = "列出 workspace 相对目录，不递归，不展示敏感目录和符号链接。"
		d.InputSchema = json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","minLength":1,"maxLength":4096}},"additionalProperties":false}`)
	case "read_file":
		d.Description = "读取 workspace 内 UTF-8 文本文件，最多 256 KiB。"
		d.InputSchema = json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","minLength":1,"maxLength":4096}},"required":["path"],"additionalProperties":false}`)
	case "write_file":
		d.Description = "在 workspace 既有目录内原子写入 UTF-8 文本文件，需调用方审批，最多 256 KiB。"
		d.RequiresApproval = true
		d.InputSchema = json.RawMessage(`{"type":"object","properties":{"path":{"type":"string","minLength":1,"maxLength":4096},"content":{"type":"string","maxLength":262144}},"required":["path","content"],"additionalProperties":false}`)
	}
	return d
}

func (f *fileTool) Execute(ctx context.Context, call tool.Call) (tool.Result, error) {
	var args struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil {
		return tool.Result{}, err
	}
	switch f.name {
	case "list_files":
		if args.Path == "" {
			args.Path = "."
		}
		entries, err := f.w.List(ctx, args.Path)
		if err != nil {
			return tool.Result{}, err
		}
		data, err := json.Marshal(entries)
		return tool.Result{Content: string(data), Structured: data}, err
	case "read_file":
		data, err := f.w.ReadFile(ctx, args.Path, MaxFileBytes)
		if err != nil {
			return tool.Result{}, err
		}
		if !utf8.Valid(data) {
			return tool.Result{}, errors.New("file is not UTF-8 text")
		}
		return tool.Result{Content: string(data)}, nil
	case "write_file":
		if !utf8.ValidString(args.Content) {
			return tool.Result{}, errors.New("content is not UTF-8 text")
		}
		if err := f.w.WriteFile(ctx, args.Path, []byte(args.Content)); err != nil {
			return tool.Result{}, err
		}
		return tool.Result{Content: "written"}, nil
	default:
		return tool.Result{}, errors.New("unknown workspace tool")
	}
}
