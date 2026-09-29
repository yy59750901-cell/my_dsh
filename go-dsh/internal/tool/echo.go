package tool

import (
	"context"
	"encoding/json"
)

type echoTool struct{}

func NewEchoTool() Tool { return echoTool{} }

func (echoTool) Definition() Definition {
	return Definition{
		Name: "echo", Description: "返回输入文本，不产生副作用。", Version: "1",
		InputSchema:      json.RawMessage(`{"type":"object","properties":{"text":{"type":"string","maxLength":262144}},"required":["text"],"additionalProperties":false}`),
		RequiresApproval: false,
	}
}

func (echoTool) Execute(ctx context.Context, call Call) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	var args struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(call.Arguments, &args); err != nil {
		return Result{}, err
	}
	return Result{CallID: call.ID, Content: args.Text}, nil
}
