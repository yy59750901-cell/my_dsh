package llm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestDemoProviderExplicitLocalResponse(t *testing.T) {
	terminal, _ := consumeOpenAITest(t, NewDemoProvider(), openAITestRequest())
	if terminal.Finish.Kind != FinishStop || terminal.Message == nil || len(terminal.Message.Content) != 1 || !strings.Contains(terminal.Message.Content[0].Text, "演示模式（非真实 LLM）") || !strings.Contains(terminal.Message.Content[0].Text, "你好") || terminal.Usage != nil {
		t.Fatalf("Demo 模式标识或内容错误：%+v", terminal)
	}
}

func TestDemoProviderDeterministicEchoRoundTrip(t *testing.T) {
	for _, prompt := range []string{"请使用工具回显", "please use tool", "use TOOL with \"quotes\"\n换行"} {
		t.Run(prompt, func(t *testing.T) {
			provider := NewDemoProvider()
			request := openAITestRequest()
			request.Messages[0].Content[0].Text = prompt
			first, chunks := consumeOpenAITest(t, provider, request)
			second, chunksAgain := consumeOpenAITest(t, provider, request)
			if !reflect.DeepEqual(first, second) || !reflect.DeepEqual(chunks, chunksAgain) {
				t.Fatal("Demo 结果不确定")
			}
			if first.Finish.Kind != FinishToolCalls || first.Message == nil || len(first.Message.Content) != 1 {
				t.Fatalf("未产生工具调用：%+v", first)
			}
			call := first.Message.Content[0]
			var args struct {
				Text string `json:"text"`
			}
			if call.ToolName != "echo" || !strings.HasPrefix(call.ToolCallID, "demo_echo_") || json.Unmarshal([]byte(call.ArgumentsJSONRaw), &args) != nil || args.Text != prompt {
				t.Fatalf("echo 参数错误：%+v", call)
			}
			request.Messages = append(request.Messages, *first.Message, Message{Role: RoleTool, Content: []ContentBlock{{Type: ContentBlockToolResult, ToolCallID: call.ToolCallID, ToolResult: json.RawMessage(`{"text":"回显结果"}`)}}})
			answer, _ := consumeOpenAITest(t, provider, request)
			if answer.Finish.Kind != FinishStop || answer.Message == nil || !strings.Contains(answer.Message.Content[0].Text, "非真实 LLM") || !strings.Contains(answer.Message.Content[0].Text, "回显结果") {
				t.Fatalf("工具往返未完成：%+v", answer)
			}
			request.Messages = append(request.Messages, Message{Role: RoleUser, Content: []ContentBlock{{Type: ContentBlockText, Text: "新问题"}}})
			fresh, _ := consumeOpenAITest(t, provider, request)
			if fresh.Message == nil || strings.Contains(fresh.Message.Content[0].Text, "回显结果") || !strings.Contains(fresh.Message.Content[0].Text, "新问题") {
				t.Fatal("旧工具结果污染新输入")
			}
		})
	}
}

func TestDemoProviderCancellationAndClose(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := NewDemoProvider().Stream(ctx, openAITestRequest())
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		if closeFirst {
			_ = stream.Close()
		}
		chunk, err := stream.Next(context.Background())
		if closeFirst {
			if !errors.Is(err, io.EOF) || !isZeroStreamChunk(chunk) {
				t.Fatal("Close 后仍有数据")
			}
		} else {
			assertFailureFinish(t, chunk, err, FinishAborted, FailureAborted)
		}
		if _, err := stream.Next(context.Background()); !errors.Is(err, io.EOF) {
			t.Fatal("取消后不是 EOF")
		}
		_ = stream.Close()
		_ = stream.Close()
	}
}

func TestDemoProviderConcurrentClose(t *testing.T) {
	stream, err := NewDemoProvider().Stream(context.Background(), openAITestRequest())
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 10 {
		group.Add(1)
		go func() { defer group.Done(); _ = stream.Close() }()
	}
	_, _ = stream.Next(context.Background())
	group.Wait()
	chunk, err := stream.Next(context.Background())
	if !errors.Is(err, io.EOF) || !isZeroStreamChunk(chunk) {
		t.Fatal("并发 Close 后不是 EOF")
	}
}
