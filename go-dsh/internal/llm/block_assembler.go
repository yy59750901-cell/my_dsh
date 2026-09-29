package llm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"
)

var (
	ErrInvalidAssemblyTransition = errors.New("invalid block assembly transition")
	ErrStaleAssemblyMutation     = errors.New("stale block assembly mutation")
	ErrAssemblyMutationCommitted = errors.New("block assembly mutation already committed")
)

type AttemptIdentity struct {
	TurnID  string
	StepID  string
	Attempt uint32
}

type CommittedChunk struct {
	Identity       AttemptIdentity
	ChunkIndex     uint64
	SourceEventSeq uint64
	Chunk          StreamChunk
}

type AssemblyDiagnostic struct {
	Code       string
	Message    string
	BlockIndex uint32
	ChunkIndex uint64
}

type AssemblySnapshot struct {
	Identity        AttemptIdentity
	Revision        uint64
	Finished        bool
	Blocks          []ContentBlock
	Usage           *TokenUsage
	Finish          *FinishReason
	SourceEventSeqs []uint64
	Diagnostics     []AssemblyDiagnostic
}

type TerminalAssembly struct {
	Identity            AttemptIdentity
	Message             *Message
	Usage               *TokenUsage
	Finish              FinishReason
	SourceEventSeqs     []uint64
	DroppedBlockIndexes []uint32
	Diagnostics         []AssemblyDiagnostic
}

type BlockAssembler struct {
	mu       sync.Mutex
	identity AttemptIdentity
	state    assemblyState
}

type assemblyState struct {
	revision      uint64
	finished      bool
	blocks        []assembledBlock
	usage         *TokenUsage
	finish        *FinishReason
	lastSourceSeq uint64
	diagnostics   []AssemblyDiagnostic
}

type assembledBlock struct {
	content         ContentBlock
	open            bool
	sourceEventSeqs []uint64
}

type AssemblyMutation struct {
	assembler          *BlockAssembler
	baseRevision       uint64
	chunkIndex         uint64
	chunk              StreamChunk
	next               assemblyState
	contributesToBlock bool
	committed          bool
}

func NewBlockAssembler(identity AttemptIdentity) (*BlockAssembler, error) {
	if identity.TurnID == "" || identity.StepID == "" || identity.Attempt == 0 {
		return nil, fmt.Errorf("%w: turn id, step id, and positive attempt are required", ErrInvalidAssemblyTransition)
	}
	return &BlockAssembler{identity: identity}, nil
}

// PlanPush validates one canonical stream chunk against a detached copy of the
// current attempt state. The live assembler changes only after Commit records
// the sequence of the already-committed assistant/chunk event.
func (assembler *BlockAssembler) PlanPush(chunk StreamChunk) (*AssemblyMutation, error) {
	if assembler == nil {
		return nil, fmt.Errorf("%w: nil assembler", ErrInvalidAssemblyTransition)
	}
	if err := validateStreamChunk(chunk); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAssemblyTransition, err)
	}

	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	if assembler.state.finished {
		return nil, fmt.Errorf("%w: stream already finished", ErrInvalidAssemblyTransition)
	}

	next := cloneAssemblyState(assembler.state)
	chunk = cloneStreamChunk(chunk)
	contributesToBlock, err := applyPlannedChunk(&next, chunk, assembler.state.revision)
	if err != nil {
		return nil, err
	}
	return &AssemblyMutation{
		assembler:          assembler,
		baseRevision:       assembler.state.revision,
		chunkIndex:         assembler.state.revision,
		chunk:              chunk,
		next:               next,
		contributesToBlock: contributesToBlock,
	}, nil
}

func (mutation *AssemblyMutation) ChunkIndex() uint64 {
	if mutation == nil {
		return 0
	}
	return mutation.chunkIndex
}

// Commit installs a previously planned state only after the corresponding
// assistant/chunk event has committed. The receipt binds the event sequence,
// attempt identity, chunk index, and canonical chunk to this mutation.
func (mutation *AssemblyMutation) Commit(committed CommittedChunk) error {
	if mutation == nil || mutation.assembler == nil {
		return fmt.Errorf("%w: nil mutation", ErrInvalidAssemblyTransition)
	}
	assembler := mutation.assembler
	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	if mutation.committed {
		return ErrAssemblyMutationCommitted
	}
	mutation.committed = true
	if assembler.state.revision != mutation.baseRevision {
		return fmt.Errorf("%w: planned revision %d, current revision %d", ErrStaleAssemblyMutation, mutation.baseRevision, assembler.state.revision)
	}
	if committed.Identity != assembler.identity || committed.ChunkIndex != mutation.chunkIndex || !reflect.DeepEqual(cloneStreamChunk(committed.Chunk), mutation.chunk) {
		return fmt.Errorf("%w: committed chunk receipt does not match planned attempt and chunk", ErrInvalidAssemblyTransition)
	}
	if committed.SourceEventSeq == 0 || committed.SourceEventSeq <= assembler.state.lastSourceSeq {
		return fmt.Errorf("%w: source event seq %d must exceed %d", ErrInvalidAssemblyTransition, committed.SourceEventSeq, assembler.state.lastSourceSeq)
	}

	next := cloneAssemblyState(mutation.next)
	if mutation.contributesToBlock {
		block := findBlock(&next, mutation.chunk.Index)
		if block == nil {
			return fmt.Errorf("%w: planned block %d disappeared", ErrInvalidAssemblyTransition, mutation.chunk.Index)
		}
		block.sourceEventSeqs = append(block.sourceEventSeqs, committed.SourceEventSeq)
	}
	next.lastSourceSeq = committed.SourceEventSeq
	next.revision = mutation.baseRevision + 1
	assembler.state = next
	return nil
}

func (assembler *BlockAssembler) Snapshot() AssemblySnapshot {
	if assembler == nil {
		return AssemblySnapshot{}
	}
	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	return snapshotFromState(assembler.identity, assembler.state)
}

func (assembler *BlockAssembler) Terminal() (TerminalAssembly, error) {
	if assembler == nil {
		return TerminalAssembly{}, fmt.Errorf("%w: nil assembler", ErrInvalidAssemblyTransition)
	}
	assembler.mu.Lock()
	defer assembler.mu.Unlock()
	if !assembler.state.finished || assembler.state.finish == nil {
		return TerminalAssembly{}, fmt.Errorf("%w: attempt has not finished", ErrInvalidAssemblyTransition)
	}
	return terminalFromState(assembler.identity, assembler.state)
}

func applyPlannedChunk(state *assemblyState, chunk StreamChunk, chunkIndex uint64) (bool, error) {
	switch chunk.Kind {
	case StreamChunkBlockStart:
		return true, planBlockStart(state, chunk)
	case StreamChunkTextDelta:
		return planTextDelta(state, chunk, ContentBlockText, chunkIndex)
	case StreamChunkReasoningDelta:
		return planTextDelta(state, chunk, ContentBlockReasoning, chunkIndex)
	case StreamChunkToolCallDelta:
		return planToolCallDelta(state, chunk, chunkIndex)
	case StreamChunkBlockEnd:
		return true, planBlockEnd(state, chunk)
	case StreamChunkUsage:
		if state.usage != nil {
			return false, fmt.Errorf("%w: duplicate usage chunk", ErrInvalidAssemblyTransition)
		}
		usage := *chunk.Usage
		usage.CacheReadTokens = cloneInt64Pointer(usage.CacheReadTokens)
		usage.CacheWriteTokens = cloneInt64Pointer(usage.CacheWriteTokens)
		usage.ReasoningTokens = cloneInt64Pointer(usage.ReasoningTokens)
		state.usage = &usage
		return false, nil
	case StreamChunkFinish:
		finish := *chunk.Finish
		if finish.Failure != nil {
			failure := cloneFailure(*finish.Failure)
			finish.Failure = &failure
		}
		if finish.Kind == FinishStop || finish.Kind == FinishToolCalls {
			for index := range state.blocks {
				block := &state.blocks[index]
				if block.open && (block.content.Type == ContentBlockText || block.content.Type == ContentBlockReasoning) {
					block.open = false
					block.content.Complete = true
				}
			}
		}
		state.finish = &finish
		state.finished = true
		if _, err := terminalFromState(AttemptIdentity{}, *state); err != nil {
			return false, err
		}
		return false, nil
	default:
		return false, fmt.Errorf("%w: unsupported chunk kind %q", ErrInvalidAssemblyTransition, chunk.Kind)
	}
}

func planBlockStart(state *assemblyState, chunk StreamChunk) error {
	if findBlock(state, chunk.Index) != nil {
		return fmt.Errorf("%w: block %d already exists", ErrInvalidAssemblyTransition, chunk.Index)
	}
	if !assemblableBlockType(chunk.BlockType) {
		return fmt.Errorf("%w: unsupported block type %q", ErrInvalidAssemblyTransition, chunk.BlockType)
	}

	block := ContentBlock{
		Type:             chunk.BlockType,
		Index:            chunk.Index,
		Text:             chunk.Text,
		ToolCallID:       chunk.ToolCallID,
		ToolName:         chunk.ToolName,
		ArgumentsJSONRaw: chunk.ArgumentsDelta,
	}
	if chunk.Block != nil {
		if chunk.Text != "" || chunk.ToolCallID != "" || chunk.ToolName != "" || chunk.ArgumentsDelta != "" {
			return fmt.Errorf("%w: block-start cannot mix Block with top-level block data", ErrInvalidAssemblyTransition)
		}
		block = cloneContentBlock(*chunk.Block)
		if block.Type != chunk.BlockType || block.Index != chunk.Index || block.Complete {
			return fmt.Errorf("%w: block-start snapshot identity or completeness mismatch", ErrInvalidAssemblyTransition)
		}
	}
	if err := validateBlockFields(block, false); err != nil {
		return err
	}
	state.blocks = append(state.blocks, assembledBlock{content: block, open: true})
	return nil
}

func planTextDelta(state *assemblyState, chunk StreamChunk, expectedType ContentBlockType, chunkIndex uint64) (bool, error) {
	if chunk.Block != nil || chunk.Text == "" || chunk.ToolCallID != "" || chunk.ToolName != "" || chunk.ArgumentsDelta != "" {
		return false, fmt.Errorf("%w: malformed %s delta", ErrInvalidAssemblyTransition, expectedType)
	}
	block, ignored, err := openOrCreateDeltaBlock(state, chunk.Index, expectedType, chunk.BlockType, chunkIndex)
	if err != nil || ignored {
		return false, err
	}
	block.content.Text += chunk.Text
	return true, nil
}

func planToolCallDelta(state *assemblyState, chunk StreamChunk, chunkIndex uint64) (bool, error) {
	if chunk.Block != nil || chunk.Text != "" || (chunk.ToolCallID == "" && chunk.ToolName == "" && chunk.ArgumentsDelta == "") {
		return false, fmt.Errorf("%w: malformed tool-call delta", ErrInvalidAssemblyTransition)
	}
	block, ignored, err := openOrCreateDeltaBlock(state, chunk.Index, ContentBlockToolCall, chunk.BlockType, chunkIndex)
	if err != nil || ignored {
		return false, err
	}
	if err := mergeStableString(&block.content.ToolCallID, chunk.ToolCallID, "tool call id"); err != nil {
		return false, err
	}
	block.content.ToolName += chunk.ToolName
	block.content.ArgumentsJSONRaw += chunk.ArgumentsDelta
	return true, nil
}

func planBlockEnd(state *assemblyState, chunk StreamChunk) error {
	block, err := openBlock(state, chunk.Index, "", chunk.BlockType)
	if err != nil {
		return err
	}
	if chunk.Text != "" || chunk.ToolCallID != "" || chunk.ToolName != "" || chunk.ArgumentsDelta != "" {
		return fmt.Errorf("%w: block-end carries top-level delta data", ErrInvalidAssemblyTransition)
	}
	completed := cloneContentBlock(block.content)
	completed.Complete = true
	if chunk.Block != nil {
		provided := cloneContentBlock(*chunk.Block)
		if !contentBlocksEqual(provided, completed) {
			return fmt.Errorf("%w: block-end snapshot does not match assembled block", ErrInvalidAssemblyTransition)
		}
	}
	if err := validateBlockFields(completed, true); err != nil {
		return err
	}
	block.content = completed
	block.open = false
	return nil
}

func openBlock(state *assemblyState, index uint32, expectedType, declaredType ContentBlockType) (*assembledBlock, error) {
	block := findBlock(state, index)
	if block == nil {
		return nil, fmt.Errorf("%w: block %d has not started", ErrInvalidAssemblyTransition, index)
	}
	if !block.open {
		return nil, fmt.Errorf("%w: block %d is already closed", ErrInvalidAssemblyTransition, index)
	}
	if expectedType != "" && block.content.Type != expectedType {
		return nil, fmt.Errorf("%w: block %d is %q, not %q", ErrInvalidAssemblyTransition, index, block.content.Type, expectedType)
	}
	if declaredType != "" && block.content.Type != declaredType {
		return nil, fmt.Errorf("%w: block %d type conflicts with declared %q", ErrInvalidAssemblyTransition, index, declaredType)
	}
	return block, nil
}

func openOrCreateDeltaBlock(state *assemblyState, index uint32, expectedType, declaredType ContentBlockType, chunkIndex uint64) (*assembledBlock, bool, error) {
	if declaredType != "" && declaredType != expectedType {
		return nil, false, fmt.Errorf("%w: block %d delta declares incompatible type %q", ErrInvalidAssemblyTransition, index, declaredType)
	}
	block := findBlock(state, index)
	if block == nil {
		state.blocks = append(state.blocks, assembledBlock{
			content: ContentBlock{Type: expectedType, Index: index},
			open:    true,
		})
		return &state.blocks[len(state.blocks)-1], false, nil
	}
	if block.content.Type != expectedType {
		return nil, false, fmt.Errorf("%w: block %d is %q, not %q", ErrInvalidAssemblyTransition, index, block.content.Type, expectedType)
	}
	if !block.open {
		state.diagnostics = append(state.diagnostics, AssemblyDiagnostic{
			Code:       "LATE_DELTA",
			Message:    fmt.Sprintf("ignored %s delta after block end", expectedType),
			BlockIndex: index,
			ChunkIndex: chunkIndex,
		})
		return block, true, nil
	}
	return block, false, nil
}

func findBlock(state *assemblyState, index uint32) *assembledBlock {
	for position := range state.blocks {
		if state.blocks[position].content.Index == index {
			return &state.blocks[position]
		}
	}
	return nil
}

func mergeStableString(target *string, delta, field string) error {
	if delta == "" {
		return nil
	}
	if *target == "" {
		*target = delta
		return nil
	}
	if *target != delta {
		return fmt.Errorf("%w: conflicting %s %q and %q", ErrInvalidAssemblyTransition, field, *target, delta)
	}
	return nil
}

func validateBlockFields(block ContentBlock, complete bool) error {
	switch block.Type {
	case ContentBlockText, ContentBlockReasoning:
		if block.ToolCallID != "" || block.ToolName != "" || block.ArgumentsJSONRaw != "" || block.ToolResult != nil {
			return fmt.Errorf("%w: %s block carries tool data", ErrInvalidAssemblyTransition, block.Type)
		}
	case ContentBlockToolCall:
		if block.Text != "" || block.ToolResult != nil {
			return fmt.Errorf("%w: tool-call block carries text or tool result", ErrInvalidAssemblyTransition)
		}
		if complete {
			if block.ToolCallID == "" || block.ToolName == "" || validateToolArguments(block.ArgumentsJSONRaw) != nil {
				return fmt.Errorf("%w: completed tool-call requires id, name, and a duplicate-free JSON object", ErrInvalidAssemblyTransition)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported block type %q", ErrInvalidAssemblyTransition, block.Type)
	}
	return nil
}

func validateToolArguments(raw string) error {
	if raw == "" {
		return errors.New("empty tool arguments")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := first.(json.Delim)
	if !ok || delimiter != '{' {
		return errors.New("tool arguments must be a JSON object")
	}
	if err := consumeJSONObject(decoder); err != nil {
		return err
	}
	if trailing, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected trailing JSON token %v", trailing)
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		return consumeJSONObject(decoder)
	case '[':
		for decoder.More() {
			if err := consumeJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return errors.New("invalid JSON array closing token")
		}
		return nil
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func consumeJSONObject(decoder *json.Decoder) error {
	keys := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("JSON object key is not a string")
		}
		if _, duplicate := keys[key]; duplicate {
			return fmt.Errorf("duplicate JSON object key %q", key)
		}
		keys[key] = struct{}{}
		if err := consumeJSONValue(decoder); err != nil {
			return err
		}
	}
	closing, err := decoder.Token()
	if err != nil {
		return err
	}
	if closing != json.Delim('}') {
		return errors.New("invalid JSON object closing token")
	}
	return nil
}

func assemblableBlockType(blockType ContentBlockType) bool {
	return blockType == ContentBlockText || blockType == ContentBlockReasoning || blockType == ContentBlockToolCall
}

func terminalFromState(identity AttemptIdentity, state assemblyState) (TerminalAssembly, error) {
	if !state.finished || state.finish == nil {
		return TerminalAssembly{}, fmt.Errorf("%w: attempt has not finished", ErrInvalidAssemblyTransition)
	}
	terminal := TerminalAssembly{
		Identity:    identity,
		Finish:      *state.finish,
		Usage:       cloneUsage(state.usage),
		Diagnostics: cloneDiagnostics(state.diagnostics),
	}
	if terminal.Finish.Failure != nil {
		failure := cloneFailure(*terminal.Finish.Failure)
		terminal.Finish.Failure = &failure
	}

	selected := make([]ContentBlock, 0, len(state.blocks))
	selectedSources := make([]uint64, 0)
	for _, assembled := range state.blocks {
		block, keep, err := terminalBlock(terminal.Finish.Kind, assembled.content, assembled.open)
		if err != nil {
			return TerminalAssembly{}, err
		}
		if !keep {
			terminal.DroppedBlockIndexes = append(terminal.DroppedBlockIndexes, block.Index)
			continue
		}
		selected = append(selected, block)
		selectedSources = append(selectedSources, assembled.sourceEventSeqs...)
	}
	if terminal.Finish.Kind == FinishToolCalls && !containsToolCall(selected) {
		return TerminalAssembly{}, fmt.Errorf("%w: tool-calls finish has no complete tool-call block", ErrInvalidAssemblyTransition)
	}
	if len(selected) != 0 {
		terminal.Message = &Message{Role: RoleAssistant, Source: MessageSourceStep, Content: selected}
		terminal.SourceEventSeqs = sortedUnique(selectedSources)
	}
	return terminal, nil
}

func terminalBlock(kind FinishKind, source ContentBlock, open bool) (ContentBlock, bool, error) {
	block := cloneContentBlock(source)
	if kind == FinishError {
		return block, false, nil
	}

	switch block.Type {
	case ContentBlockText, ContentBlockReasoning:
		if block.Text == "" {
			return block, false, nil
		}
		if open {
			if kind != FinishStop && kind != FinishToolCalls {
				block.Complete = false
			} else {
				block.Complete = true
			}
		}
		return block, true, nil
	case ContentBlockToolCall:
		if kind == FinishAborted || open || !block.Complete {
			if open && (kind == FinishStop || kind == FinishToolCalls) {
				return block, false, fmt.Errorf("%w: finish %q has incomplete tool-call block %d", ErrInvalidAssemblyTransition, kind, block.Index)
			}
			return block, false, nil
		}
		if block.ToolCallID == "" || block.ToolName == "" || validateToolArguments(block.ArgumentsJSONRaw) != nil {
			return block, false, fmt.Errorf("%w: terminal tool-call block %d is invalid", ErrInvalidAssemblyTransition, block.Index)
		}
		return block, true, nil
	default:
		return block, false, fmt.Errorf("%w: unsupported terminal block type %q", ErrInvalidAssemblyTransition, block.Type)
	}
}

func containsToolCall(blocks []ContentBlock) bool {
	for _, block := range blocks {
		if block.Type == ContentBlockToolCall {
			return true
		}
	}
	return false
}

func snapshotFromState(identity AttemptIdentity, state assemblyState) AssemblySnapshot {
	snapshot := AssemblySnapshot{
		Identity:    identity,
		Revision:    state.revision,
		Finished:    state.finished,
		Usage:       cloneUsage(state.usage),
		Diagnostics: cloneDiagnostics(state.diagnostics),
	}
	if state.finish != nil {
		finish := *state.finish
		if finish.Failure != nil {
			failure := cloneFailure(*finish.Failure)
			finish.Failure = &failure
		}
		snapshot.Finish = &finish
	}
	if state.blocks != nil {
		snapshot.Blocks = make([]ContentBlock, len(state.blocks))
		for index, block := range state.blocks {
			snapshot.Blocks[index] = cloneContentBlock(block.content)
			snapshot.SourceEventSeqs = append(snapshot.SourceEventSeqs, block.sourceEventSeqs...)
		}
		snapshot.SourceEventSeqs = sortedUnique(snapshot.SourceEventSeqs)
	}
	return snapshot
}

func cloneAssemblyState(state assemblyState) assemblyState {
	cloned := state
	cloned.usage = cloneUsage(state.usage)
	if state.finish != nil {
		finish := *state.finish
		if finish.Failure != nil {
			failure := cloneFailure(*finish.Failure)
			finish.Failure = &failure
		}
		cloned.finish = &finish
	}
	if state.blocks != nil {
		cloned.blocks = make([]assembledBlock, len(state.blocks))
		for index, block := range state.blocks {
			cloned.blocks[index] = assembledBlock{
				content:         cloneContentBlock(block.content),
				open:            block.open,
				sourceEventSeqs: append([]uint64(nil), block.sourceEventSeqs...),
			}
		}
	}
	cloned.diagnostics = cloneDiagnostics(state.diagnostics)
	return cloned
}

func cloneContentBlock(block ContentBlock) ContentBlock {
	block.ToolResult = cloneRawMessage(block.ToolResult)
	return block
}

func cloneUsage(usage *TokenUsage) *TokenUsage {
	if usage == nil {
		return nil
	}
	cloned := *usage
	cloned.CacheReadTokens = cloneInt64Pointer(usage.CacheReadTokens)
	cloned.CacheWriteTokens = cloneInt64Pointer(usage.CacheWriteTokens)
	cloned.ReasoningTokens = cloneInt64Pointer(usage.ReasoningTokens)
	return &cloned
}

func cloneDiagnostics(diagnostics []AssemblyDiagnostic) []AssemblyDiagnostic {
	if diagnostics == nil {
		return nil
	}
	return append(make([]AssemblyDiagnostic, 0, len(diagnostics)), diagnostics...)
}

func contentBlocksEqual(left, right ContentBlock) bool {
	return left.Type == right.Type &&
		left.Text == right.Text &&
		left.Index == right.Index &&
		left.ToolCallID == right.ToolCallID &&
		left.ToolName == right.ToolName &&
		left.ArgumentsJSONRaw == right.ArgumentsJSONRaw &&
		string(left.ToolResult) == string(right.ToolResult) &&
		left.Complete == right.Complete
}

func sortedUnique(values []uint64) []uint64 {
	if len(values) == 0 {
		return nil
	}
	result := append([]uint64(nil), values...)
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	write := 1
	for read := 1; read < len(result); read++ {
		if result[read] == result[write-1] {
			continue
		}
		result[write] = result[read]
		write++
	}
	return result[:write]
}
