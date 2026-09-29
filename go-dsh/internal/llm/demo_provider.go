package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"sync"
)

type demoProvider struct{}

// NewDemoProvider 返回确定性本地演示适配器，不访问网络，也不冒充真实 LLM。
func NewDemoProvider() Provider { return demoProvider{} }

func (demoProvider) Stream(ctx context.Context, request ModelRequest) (Stream, error) {
	if err := ctx.Err(); err != nil {
		return openAITerminal(openAIReadFailure(err)), nil
	}
	prompt := ""
	toolResult := ""
	hasToolResult := false
	for i := len(request.Messages) - 1; i >= 0; i-- {
		message := request.Messages[i]
		if message.Role == RoleTool {
			hasToolResult = true
			for _, block := range message.Content {
				if block.Type == ContentBlockToolResult {
					toolResult += string(block.ToolResult)
				} else if block.Type == ContentBlockText {
					toolResult += block.Text
				}
			}
			break
		}
		if message.Role == RoleUser {
			for _, block := range message.Content {
				if block.Type == ContentBlockText {
					prompt += block.Text
				}
			}
			break
		}
	}
	prefix := "演示模式（非真实 LLM）："
	blockType, finish := ContentBlockText, FinishStop
	delta := StreamChunk{Kind: StreamChunkTextDelta, Text: prefix + "已收到你的输入：" + prompt}
	if hasToolResult {
		delta.Text = prefix + "echo 工具结果：" + toolResult
	} else if strings.Contains(prompt, "工具") || strings.Contains(strings.ToLower(prompt), "tool") {
		args, _ := json.Marshal(struct {
			Text string `json:"text"`
		}{Text: prompt})
		digest := sha256.Sum256([]byte(request.TurnID + "\x00" + request.StepID + "\x00" + prompt))
		blockType, finish = ContentBlockToolCall, FinishToolCalls
		delta = StreamChunk{Kind: StreamChunkToolCallDelta, ToolCallID: "demo_echo_" + hex.EncodeToString(digest[:8]), ToolName: "echo", ArgumentsDelta: string(args)}
	}
	delta.BlockType = blockType
	return &demoStream{ctx: ctx, chunks: []StreamChunk{
		{Kind: StreamChunkBlockStart, BlockType: blockType},
		delta,
		{Kind: StreamChunkBlockEnd, BlockType: blockType},
		{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: finish}},
	}}, nil
}

type demoStream struct {
	mu     sync.Mutex
	ctx    context.Context
	chunks []StreamChunk
}

func (stream *demoStream) Next(ctx context.Context) (StreamChunk, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.chunks) == 0 {
		return StreamChunk{}, io.EOF
	}
	err := ctx.Err()
	if err == nil {
		err = stream.ctx.Err()
	}
	if err != nil {
		stream.chunks = nil
		return openAIFailureChunk(openAIReadFailure(err)), nil
	}
	chunk := stream.chunks[0]
	stream.chunks[0] = StreamChunk{}
	stream.chunks = stream.chunks[1:]
	return chunk, nil
}

func (stream *demoStream) Close() error {
	stream.mu.Lock()
	stream.chunks = nil
	stream.mu.Unlock()
	return nil
}
