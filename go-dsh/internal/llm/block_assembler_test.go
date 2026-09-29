package llm

import (
	"errors"
	"reflect"
	"testing"
)

func TestBlockAssemblerPlanDoesNotMutateUntilCommit(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mutation, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText})
	if err != nil {
		t.Fatal(err)
	}
	if mutation.ChunkIndex() != 0 {
		t.Fatalf("chunk index = %d, want 0", mutation.ChunkIndex())
	}
	before := assembler.Snapshot()
	if before.Revision != 0 || len(before.Blocks) != 0 {
		t.Fatalf("PlanPush mutated live state: %+v", before)
	}
	if err := commitMutation(mutation, 10); err != nil {
		t.Fatal(err)
	}
	after := assembler.Snapshot()
	if after.Revision != 1 || len(after.Blocks) != 1 || after.Blocks[0].Type != ContentBlockText {
		t.Fatalf("committed snapshot = %+v", after)
	}
}

func TestBlockAssemblerRejectsStaleAndRepeatedMutation(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText}, 10)

	first, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkTextDelta, Index: 0, BlockType: ContentBlockText, Text: "first"})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkTextDelta, Index: 0, BlockType: ContentBlockText, Text: "stale"})
	if err != nil {
		t.Fatal(err)
	}
	if first.ChunkIndex() != 1 || stale.ChunkIndex() != 1 {
		t.Fatalf("parallel plans used unexpected indexes: %d, %d", first.ChunkIndex(), stale.ChunkIndex())
	}
	if err := commitMutation(first, 11); err != nil {
		t.Fatal(err)
	}
	if err := commitMutation(stale, 12); !errors.Is(err, ErrStaleAssemblyMutation) {
		t.Fatalf("stale commit error = %v", err)
	}
	if err := commitMutation(first, 12); !errors.Is(err, ErrAssemblyMutationCommitted) {
		t.Fatalf("repeated commit error = %v", err)
	}
	if got := assembler.Snapshot().Blocks[0].Text; got != "first" {
		t.Fatalf("stale mutation changed text to %q", got)
	}
}

func TestBlockAssemblerRejectsMismatchedCommittedChunkReceipt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CommittedChunk)
	}{
		{name: "attempt identity", mutate: func(receipt *CommittedChunk) { receipt.Identity.Attempt++ }},
		{name: "chunk index", mutate: func(receipt *CommittedChunk) { receipt.ChunkIndex++ }},
		{name: "canonical chunk", mutate: func(receipt *CommittedChunk) { receipt.Chunk.Text = "different" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
			chunk := StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText}
			mutation, err := assembler.PlanPush(chunk)
			if err != nil {
				t.Fatal(err)
			}
			receipt := CommittedChunk{Identity: assembler.identity, ChunkIndex: mutation.ChunkIndex(), SourceEventSeq: 10, Chunk: chunk}
			test.mutate(&receipt)
			if err := mutation.Commit(receipt); !errors.Is(err, ErrInvalidAssemblyTransition) {
				t.Fatalf("receipt mismatch error = %v", err)
			}
			if snapshot := assembler.Snapshot(); snapshot.Revision != 0 || len(snapshot.Blocks) != 0 {
				t.Fatalf("mismatched receipt mutated state: %+v", snapshot)
			}
		})
	}
}

func TestBlockAssemblerRejectsNonMonotonicSourceSequenceWithoutMutation(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText}, 10)
	mutation, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkTextDelta, Index: 0, Text: "late"})
	if err != nil {
		t.Fatal(err)
	}
	if err := commitMutation(mutation, 10); !errors.Is(err, ErrInvalidAssemblyTransition) {
		t.Fatalf("non-monotonic commit error = %v", err)
	}
	snapshot := assembler.Snapshot()
	if snapshot.Revision != 1 || snapshot.Blocks[0].Text != "" {
		t.Fatalf("failed commit mutated state: %+v", snapshot)
	}
}

func TestBlockAssemblerAssemblesInterleavedBlocksAndSourceSequences(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	chunks := []StreamChunk{
		{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText},
		{Kind: StreamChunkBlockStart, Index: 1, BlockType: ContentBlockReasoning},
		{Kind: StreamChunkReasoningDelta, Index: 1, Text: "think"},
		{Kind: StreamChunkTextDelta, Index: 0, Text: "answer"},
		{Kind: StreamChunkBlockEnd, Index: 1},
		{Kind: StreamChunkBlockEnd, Index: 0},
		{Kind: StreamChunkUsage, Usage: &TokenUsage{InputTokens: 3, OutputTokens: 2}},
		{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}},
	}
	for index, chunk := range chunks {
		mutation, err := assembler.PlanPush(chunk)
		if err != nil {
			t.Fatalf("PlanPush %d: %v", index, err)
		}
		if mutation.ChunkIndex() != uint64(index) {
			t.Fatalf("chunk index %d = %d", index, mutation.ChunkIndex())
		}
		if err := commitMutation(mutation, uint64(100+index)); err != nil {
			t.Fatalf("Commit %d: %v", index, err)
		}
	}

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 2 {
		t.Fatalf("terminal message = %+v", terminal.Message)
	}
	if terminal.Message.Content[0].Type != ContentBlockText || terminal.Message.Content[0].Text != "answer" || !terminal.Message.Content[0].Complete {
		t.Fatalf("text block = %+v", terminal.Message.Content[0])
	}
	if terminal.Message.Content[1].Type != ContentBlockReasoning || terminal.Message.Content[1].Text != "think" || !terminal.Message.Content[1].Complete {
		t.Fatalf("reasoning block = %+v", terminal.Message.Content[1])
	}
	if !reflect.DeepEqual(terminal.SourceEventSeqs, []uint64{100, 101, 102, 103, 104, 105}) {
		t.Fatalf("source event seqs = %v", terminal.SourceEventSeqs)
	}
	if terminal.Usage == nil || terminal.Usage.InputTokens != 3 || terminal.Usage.OutputTokens != 2 {
		t.Fatalf("usage = %+v", terminal.Usage)
	}
}

func TestBlockAssemblerAssemblesToolCallOnlyMessage(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{
		Kind:       StreamChunkBlockStart,
		Index:      0,
		BlockType:  ContentBlockToolCall,
		ToolCallID: "call-1",
	}, 20)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkToolCallDelta, Index: 0, ToolName: "wea", ArgumentsDelta: `{"city":"Bei`}, 21)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkToolCallDelta, Index: 0, ToolName: "ther", ArgumentsDelta: `jing"}`}, 22)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockEnd, Index: 0}, 23)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishToolCalls}}, 24)

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 1 {
		t.Fatalf("tool-call-only message = %+v", terminal.Message)
	}
	block := terminal.Message.Content[0]
	if block.Type != ContentBlockToolCall || block.ToolCallID != "call-1" || block.ToolName != "weather" || block.ArgumentsJSONRaw != `{"city":"Beijing"}` || !block.Complete {
		t.Fatalf("tool-call block = %+v", block)
	}
}

func TestBlockAssemblerRejectsUnsafeCompletedToolArguments(t *testing.T) {
	unsafeArguments := []string{`null`, `[]`, `"value"`, `{"id":1,"id":2}`}
	for _, arguments := range unsafeArguments {
		t.Run(arguments, func(t *testing.T) {
			assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
			mustPush(t, assembler, StreamChunk{
				Kind:           StreamChunkBlockStart,
				Index:          0,
				BlockType:      ContentBlockToolCall,
				ToolCallID:     "call-1",
				ToolName:       "lookup",
				ArgumentsDelta: arguments,
			}, 25)
			if _, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkBlockEnd, Index: 0}); !errors.Is(err, ErrInvalidAssemblyTransition) {
				t.Fatalf("unsafe arguments %q end error = %v", arguments, err)
			}
		})
	}
}

func TestBlockAssemblerMaxTokensKeepsSafeTextPrefixAndDropsIncompleteToolCall(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText}, 30)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkTextDelta, Index: 0, Text: "safe prefix"}, 31)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 1, BlockType: ContentBlockToolCall, ToolCallID: "call-1", ToolName: "lookup"}, 32)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkToolCallDelta, Index: 1, ArgumentsDelta: `{"id":`}, 33)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishMaxTokens}}, 34)

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 1 || terminal.Message.Content[0].Text != "safe prefix" || terminal.Message.Content[0].Complete {
		t.Fatalf("safe prefix message = %+v", terminal.Message)
	}
	if !reflect.DeepEqual(terminal.DroppedBlockIndexes, []uint32{1}) {
		t.Fatalf("dropped blocks = %v", terminal.DroppedBlockIndexes)
	}
	if !reflect.DeepEqual(terminal.SourceEventSeqs, []uint64{30, 31}) {
		t.Fatalf("safe prefix source seqs = %v", terminal.SourceEventSeqs)
	}
}

func TestBlockAssemblerAbortedDropsCompletedToolCall(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockToolCall, ToolCallID: "call-1", ToolName: "lookup", ArgumentsDelta: `{}`}, 40)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockEnd, Index: 0}, 41)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishAborted, Failure: &LlmFailure{Code: FailureAborted}}}, 42)

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message != nil || !reflect.DeepEqual(terminal.DroppedBlockIndexes, []uint32{0}) {
		t.Fatalf("aborted terminal = %+v", terminal)
	}
}

func TestBlockAssemblerNormalFinishClosesTextWithoutExplicitBlockEnd(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText}, 50)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkTextDelta, Index: 0, Text: "complete at finish"}, 51)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}, 52)
	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 1 || !terminal.Message.Content[0].Complete {
		t.Fatalf("normal finish did not close text block: %+v", terminal.Message)
	}
	if !assembler.Snapshot().Blocks[0].Complete {
		t.Fatal("normal finish did not close the committed block state")
	}
}

func TestBlockAssemblerIgnoresLateDeltaAndRejectsBlockTypeConflict(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockStart, Index: 7, BlockType: ContentBlockText}, 60)
	if _, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkReasoningDelta, Index: 7, Text: "wrong"}); !errors.Is(err, ErrInvalidAssemblyTransition) {
		t.Fatalf("type conflict error = %v", err)
	}
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkTextDelta, Index: 7, Text: "done"}, 61)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockEnd, Index: 7}, 62)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkTextDelta, Index: 7, Text: "late"}, 63)
	snapshot := assembler.Snapshot()
	if snapshot.Blocks[0].Text != "done" || len(snapshot.Diagnostics) != 1 || snapshot.Diagnostics[0].Code != "LATE_DELTA" || snapshot.Diagnostics[0].ChunkIndex != 3 {
		t.Fatalf("late delta handling = %+v", snapshot)
	}
	if !reflect.DeepEqual(snapshot.SourceEventSeqs, []uint64{60, 61, 62}) {
		t.Fatalf("ignored delta entered source seqs: %v", snapshot.SourceEventSeqs)
	}
}

func TestBlockAssemblerCreatesImplicitBlocksInFirstAppearanceOrder(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkTextDelta, Index: 7, Text: "answer"}, 70)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkReasoningDelta, Index: 2, Text: "thought"}, 71)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}, 72)

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 2 {
		t.Fatalf("implicit blocks = %+v", terminal.Message)
	}
	if terminal.Message.Content[0].Index != 7 || terminal.Message.Content[0].Text != "answer" || !terminal.Message.Content[0].Complete {
		t.Fatalf("first implicit block = %+v", terminal.Message.Content[0])
	}
	if terminal.Message.Content[1].Index != 2 || terminal.Message.Content[1].Text != "thought" || !terminal.Message.Content[1].Complete {
		t.Fatalf("second implicit block = %+v", terminal.Message.Content[1])
	}
}

func TestBlockAssemblerMaxTokensKeepsCompletedSafeToolCall(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkToolCallDelta, Index: 4, ToolCallID: "call-1", ToolName: "lookup", ArgumentsDelta: `{}`}, 80)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkBlockEnd, Index: 4}, 81)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishMaxTokens}}, 82)

	terminal, err := assembler.Terminal()
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Message == nil || len(terminal.Message.Content) != 1 || terminal.Message.Content[0].Type != ContentBlockToolCall {
		t.Fatalf("completed safe tool call was not preserved: %+v", terminal)
	}
}

func TestBlockAssemblerRejectsPushAfterFinish(t *testing.T) {
	assembler := newAssemblerForTest(t, "turn-1", "step-1", 1)
	mustPush(t, assembler, StreamChunk{Kind: StreamChunkFinish, Finish: &FinishReason{Kind: FinishStop}}, 70)
	if _, err := assembler.PlanPush(StreamChunk{Kind: StreamChunkUsage, Usage: &TokenUsage{}}); !errors.Is(err, ErrInvalidAssemblyTransition) {
		t.Fatalf("post-finish push error = %v", err)
	}
}

func TestBlockAssemblerAttemptsAreIsolated(t *testing.T) {
	first := newAssemblerForTest(t, "turn-1", "step-1", 1)
	second := newAssemblerForTest(t, "turn-1", "step-1", 2)
	mustPush(t, first, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText, Text: "first"}, 80)
	mustPush(t, second, StreamChunk{Kind: StreamChunkBlockStart, Index: 0, BlockType: ContentBlockText, Text: "second"}, 80)
	if first.Snapshot().Blocks[0].Text != "first" || second.Snapshot().Blocks[0].Text != "second" {
		t.Fatalf("attempt state leaked: first=%+v second=%+v", first.Snapshot(), second.Snapshot())
	}
	if first.Snapshot().Identity.Attempt != 1 || second.Snapshot().Identity.Attempt != 2 {
		t.Fatal("attempt identities were not preserved")
	}
}

func newAssemblerForTest(t *testing.T, turnID, stepID string, attempt uint32) *BlockAssembler {
	t.Helper()
	assembler, err := NewBlockAssembler(AttemptIdentity{TurnID: turnID, StepID: stepID, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	return assembler
}

func mustPush(t *testing.T, assembler *BlockAssembler, chunk StreamChunk, sourceEventSeq uint64) {
	t.Helper()
	mutation, err := assembler.PlanPush(chunk)
	if err != nil {
		t.Fatal(err)
	}
	if err := commitMutation(mutation, sourceEventSeq); err != nil {
		t.Fatal(err)
	}
}

func commitMutation(mutation *AssemblyMutation, sourceEventSeq uint64) error {
	return mutation.Commit(CommittedChunk{
		Identity:       mutation.assembler.identity,
		ChunkIndex:     mutation.ChunkIndex(),
		SourceEventSeq: sourceEventSeq,
		Chunk:          cloneStreamChunk(mutation.chunk),
	})
}
