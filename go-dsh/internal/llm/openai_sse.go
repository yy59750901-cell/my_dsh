package llm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

var errOpenAISSE = errors.New("模型服务流协议无效")

type openAISSEDecoder struct {
	scanner *bufio.Scanner
	total   int
	first   bool
}

func newOpenAIStream(ctx context.Context, cancel context.CancelFunc, body io.ReadCloser) *openAIStream {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), openAIMaxFrameBytes+2)
	scanner.Split(openAISSELineSplitter())
	return &openAIStream{
		ctx: ctx, cancel: cancel, body: body,
		decoder: &openAISSEDecoder{scanner: scanner, first: true},
		tools:   make(map[int]int), textIndex: -1, reasoningIndex: -1,
	}
}

// CR 立即结束一行，下一次跳过可选 LF，避免持久连接以 CR 结束时等待额外字节。
func openAISSELineSplitter() bufio.SplitFunc {
	skipLF := false
	return func(data []byte, atEOF bool) (int, []byte, error) {
		start := 0
		if skipLF && len(data) > 0 {
			skipLF = false
			if data[0] == '\n' {
				start = 1
			}
		}
		for i := start; i < len(data); i++ {
			c := data[i]
			if c == '\r' || c == '\n' {
				skipLF = c == '\r'
				return i + 1, data[start:i], nil
			}
		}
		if atEOF && len(data) > start {
			return len(data), data[start:], nil
		}
		return start, nil, nil
	}
}

func (decoder *openAISSEDecoder) next() ([]byte, error) {
	var data []byte
	frameBytes := 0
	event := ""
	for decoder.scanner.Scan() {
		line := decoder.scanner.Bytes()
		// 多行 data、注释和忽略字段也计入上限，避免绕过帧限制。
		frameBytes += len(line) + 2
		decoder.total += len(line) + 2
		if frameBytes > openAIMaxFrameBytes || decoder.total > openAIMaxStreamBytes {
			return nil, errOpenAISSE
		}
		if decoder.first {
			line = bytes.TrimPrefix(line, []byte{0xef, 0xbb, 0xbf})
			decoder.first = false
		}
		if len(line) == 0 {
			if event == "error" {
				return nil, errOpenAISSE
			}
			if len(data) > 0 {
				return bytes.TrimSuffix(data, []byte{'\n'}), nil
			}
			frameBytes, event = 0, ""
			continue
		}
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "data":
			data = append(data, value...)
			data = append(data, '\n')
		case "event":
			event = string(value)
		}
	}
	if err := decoder.scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, errOpenAISSE
		}
		return nil, err
	}
	if frameBytes != 0 {
		return nil, io.ErrUnexpectedEOF
	}
	return nil, io.EOF
}

type openAIResponse struct {
	Choices []struct {
		Index *int `json:"index"`
		Delta struct {
			Role             string          `json:"role"`
			Content          *string         `json:"content"`
			ReasoningContent *string         `json:"reasoning_content"`
			Reasoning        *string         `json:"reasoning"`
			Refusal          *string         `json:"refusal"`
			FunctionCall     json.RawMessage `json:"function_call"`
			ToolCalls        []struct {
				Index    *int   `json:"index"`
				ID       string `json:"id"`
				Type     string `json:"type"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
		PromptDetails    struct {
			CachedTokens *int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
		CompletionDetails struct {
			ReasoningTokens *int64 `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
	Error json.RawMessage `json:"error"`
}

type openAIBlock struct {
	kind                ContentBlockType
	id, name, arguments []byte
}

func (stream *openAIStream) startBlock(kind ContentBlockType) int {
	index := len(stream.blocks)
	stream.blocks = append(stream.blocks, openAIBlock{kind: kind})
	stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkBlockStart, Index: uint32(index), BlockType: kind})
	return index
}

func (stream *openAIStream) textDelta(text *string, reasoning bool) {
	if text == nil || *text == "" {
		return
	}
	index, kind, blockType := &stream.textIndex, StreamChunkTextDelta, ContentBlockText
	if reasoning {
		index, kind, blockType = &stream.reasoningIndex, StreamChunkReasoningDelta, ContentBlockReasoning
	}
	if *index < 0 {
		*index = stream.startBlock(blockType)
	}
	stream.queue = append(stream.queue, StreamChunk{Kind: kind, Index: uint32(*index), BlockType: blockType, Text: *text})
}

func (stream *openAIStream) accept(data []byte) error {
	if bytes.Equal(data, []byte("[DONE]")) {
		if stream.finish == nil {
			kind := FinishStop
			if len(stream.tools) > 0 {
				kind = FinishToolCalls
			}
			stream.finish = &FinishReason{Kind: kind}
		}
		stream.complete()
		return nil
	}
	var response openAIResponse
	if !utf8.Valid(data) || json.Unmarshal(data, &response) != nil || response.Choices == nil || len(response.Choices) > 1 || nonNullOpenAIRaw(response.Error) {
		return errOpenAISSE
	}
	if response.Usage != nil {
		usage := &TokenUsage{
			InputTokens: response.Usage.PromptTokens, OutputTokens: response.Usage.CompletionTokens,
			CacheReadTokens: response.Usage.PromptDetails.CachedTokens,
			ReasoningTokens: response.Usage.CompletionDetails.ReasoningTokens,
		}
		if validateUsage(*usage) != nil {
			return errOpenAISSE
		}
		// 上游可能发送累计快照；Assembler 只接受一次 usage，终态前发最终值。
		stream.usage = usage
	}
	for _, choice := range response.Choices {
		if choice.Index == nil || *choice.Index != 0 {
			return errOpenAISSE
		}
		delta := choice.Delta
		if (delta.Role != "" && delta.Role != "assistant") || nonNullOpenAIRaw(delta.FunctionCall) || (delta.Refusal != nil && *delta.Refusal != "") {
			return errOpenAISSE
		}
		if stream.finish != nil {
			if choice.FinishReason != nil || nonEmptyOpenAIText(delta.Content) || nonEmptyOpenAIText(delta.ReasoningContent) || nonEmptyOpenAIText(delta.Reasoning) || len(delta.ToolCalls) > 0 {
				return errOpenAISSE
			}
			continue
		}
		if nonEmptyOpenAIText(delta.ReasoningContent) && nonEmptyOpenAIText(delta.Reasoning) {
			return errOpenAISSE
		}
		stream.textDelta(delta.ReasoningContent, true)
		stream.textDelta(delta.Reasoning, true)
		stream.textDelta(delta.Content, false)
		if len(delta.ToolCalls) > openAIMaxTools {
			return errOpenAISSE
		}
		for _, tool := range delta.ToolCalls {
			if tool.Index == nil || *tool.Index < 0 || (tool.Type != "" && tool.Type != "function") {
				return errOpenAISSE
			}
			index, exists := stream.tools[*tool.Index]
			if !exists {
				if len(stream.tools) >= openAIMaxTools {
					return errOpenAISSE
				}
				index = stream.startBlock(ContentBlockToolCall)
				stream.tools[*tool.Index] = index
			}
			block := &stream.blocks[index]
			if len(block.id)+len(tool.ID) > 4096 || len(block.name)+len(tool.Function.Name) > 1024 || len(block.arguments)+len(tool.Function.Arguments) > openAIMaxFrameBytes {
				return errOpenAISSE
			}
			block.id = append(block.id, tool.ID...)
			block.name = append(block.name, tool.Function.Name...)
			block.arguments = append(block.arguments, tool.Function.Arguments...)
			// ID 分片不能进入 Assembler 的 stable-string 合并；完整后只发送一次。
			if tool.Function.Name != "" || tool.Function.Arguments != "" {
				stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkToolCallDelta, Index: uint32(index), BlockType: ContentBlockToolCall, ToolName: tool.Function.Name, ArgumentsDelta: tool.Function.Arguments})
			}
		}
		if choice.FinishReason != nil {
			var kind FinishKind
			switch *choice.FinishReason {
			case "stop":
				kind = FinishStop
			case "tool_calls":
				kind = FinishToolCalls
			case "length":
				kind = FinishMaxTokens
			default:
				return errOpenAISSE
			}
			stream.finish = &FinishReason{Kind: kind}
		}
	}
	return nil
}

func nonNullOpenAIRaw(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func nonEmptyOpenAIText(text *string) bool { return text != nil && *text != "" }

func (stream *openAIStream) complete() {
	if stream.finish.Kind == FinishToolCalls && len(stream.tools) == 0 {
		stream.fail(openAIFailure(FailureProtocol))
		return
	}
	if len(stream.blocks) == 0 && stream.finish.Kind == FinishStop {
		stream.fail(openAIFailure(FailureEmptyResponse))
		return
	}
	seenIDs := make(map[string]bool)
	for index, block := range stream.blocks {
		if block.kind == ContentBlockToolCall {
			valid := len(block.id) != 0 && len(block.name) != 0 && validateToolArguments(string(block.arguments)) == nil
			if !valid && stream.finish.Kind == FinishMaxTokens {
				continue
			}
			if !valid || seenIDs[string(block.id)] {
				stream.fail(openAIFailure(FailureProtocol))
				return
			}
			seenIDs[string(block.id)] = true
			stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkToolCallDelta, Index: uint32(index), BlockType: block.kind, ToolCallID: string(block.id)})
		} else if stream.finish.Kind == FinishMaxTokens {
			// 保留长度截断文本的不完整状态，不能伪装成正常 block-end。
			continue
		}
		stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkBlockEnd, Index: uint32(index), BlockType: block.kind})
	}
	if stream.usage != nil {
		stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkUsage, Usage: stream.usage})
	}
	stream.queue = append(stream.queue, StreamChunk{Kind: StreamChunkFinish, Finish: stream.finish})
	stream.final = true
	// 在发布 canonical finish 之前停止网络，Runtime 的 lookahead 只能看到本地 EOF。
	stream.release()
}
