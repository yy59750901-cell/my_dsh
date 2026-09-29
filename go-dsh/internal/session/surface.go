package session

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrInvalidSurface = errors.New("invalid session surface")

const (
	SurfaceEventUserMessage             = "user/message"
	SurfaceEventAssistantMessage        = "assistant/message"
	SurfaceEventToolResult              = "tool/result"
	SurfaceSchemaMinor           uint32 = 1
)

type SurfaceOpKind string

const (
	SurfaceOpAppend  SurfaceOpKind = "append"
	SurfaceOpReplace SurfaceOpKind = "replace"
)

// SurfaceOp records how a message-producing event joins the model-visible
// ordered surface. ReplaceStart and ReplaceEnd are inclusive event sequences.
type SurfaceOp struct {
	Kind         SurfaceOpKind
	ReplaceStart uint64
	ReplaceEnd   uint64
}

func AppendSurfaceOp() *SurfaceOp {
	return &SurfaceOp{Kind: SurfaceOpAppend}
}

func ReplaceSurfaceOp(start, end uint64) *SurfaceOp {
	return &SurfaceOp{Kind: SurfaceOpReplace, ReplaceStart: start, ReplaceEnd: end}
}

func (op SurfaceOp) MarshalJSON() ([]byte, error) {
	switch op.Kind {
	case SurfaceOpAppend:
		if op.ReplaceStart != 0 || op.ReplaceEnd != 0 {
			return nil, fmt.Errorf("%w: append operation cannot carry a replacement range", ErrInvalidSurface)
		}
		return json.Marshal(string(SurfaceOpAppend))
	case SurfaceOpReplace:
		return json.Marshal(struct {
			Op    SurfaceOpKind `json:"op"`
			Start uint64        `json:"start"`
			End   uint64        `json:"end"`
		}{Op: SurfaceOpReplace, Start: op.ReplaceStart, End: op.ReplaceEnd})
	default:
		return nil, fmt.Errorf("%w: unknown surface operation %q", ErrInvalidSurface, op.Kind)
	}
}

func (op *SurfaceOp) UnmarshalJSON(data []byte) error {
	if op == nil {
		return fmt.Errorf("%w: nil surface operation", ErrInvalidSurface)
	}
	var marker string
	if err := json.Unmarshal(data, &marker); err == nil {
		if marker != string(SurfaceOpAppend) {
			return fmt.Errorf("%w: unknown surface operation %q", ErrInvalidSurface, marker)
		}
		*op = SurfaceOp{Kind: SurfaceOpAppend}
		return nil
	}
	var replacement struct {
		Op    SurfaceOpKind `json:"op"`
		Start uint64        `json:"start"`
		End   uint64        `json:"end"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&replacement); err != nil {
		return fmt.Errorf("%w: decode surface operation: %v", ErrInvalidSurface, err)
	}
	if replacement.Op != SurfaceOpReplace {
		return fmt.Errorf("%w: unknown surface operation %q", ErrInvalidSurface, replacement.Op)
	}
	*op = SurfaceOp{Kind: SurfaceOpReplace, ReplaceStart: replacement.Start, ReplaceEnd: replacement.End}
	return nil
}

type surfaceNode struct {
	Seq             uint64
	EventType       string
	Data            json.RawMessage
	SourceEventSeqs []uint64
}

// Surface is the current model-visible projection over the append-only event
// log. Its zero value is ready for use.
type Surface struct {
	nodes             []surfaceNode
	throughSeq        uint64
	replaceGeneration uint64
}

func (s Surface) Clone() Surface {
	cloned := Surface{
		nodes:             make([]surfaceNode, len(s.nodes)),
		throughSeq:        s.throughSeq,
		replaceGeneration: s.replaceGeneration,
	}
	for index, node := range s.nodes {
		cloned.nodes[index] = surfaceNode{
			Seq:             node.Seq,
			EventType:       node.EventType,
			Data:            cloneRawMessage(node.Data),
			SourceEventSeqs: cloneEventSeqs(node.SourceEventSeqs),
		}
	}
	return cloned
}

func (s Surface) Nodes() []uint64 {
	nodes := make([]uint64, len(s.nodes))
	for index, node := range s.nodes {
		nodes[index] = node.Seq
	}
	return nodes
}

func (s Surface) ThroughSeq() uint64 {
	return s.throughSeq
}

func (s Surface) ReplaceGeneration() uint64 {
	return s.replaceGeneration
}

// Apply validates and folds one event atomically. Non-surface events do not
// alter the projection, but they are rejected if they carry surface metadata.
func (s *Surface) Apply(event Event) error {
	if s == nil {
		return fmt.Errorf("%w: nil surface", ErrInvalidSurface)
	}
	if event.Seq != s.throughSeq+1 {
		return fmt.Errorf("%w: event seq %d is not contiguous; expected %d", ErrInvalidSurface, event.Seq, s.throughSeq+1)
	}
	eligible := IsSurfaceEligibleType(event.EventType)
	if !eligible {
		if event.SurfaceOp != nil || event.SourceEventSeqs != nil {
			return fmt.Errorf("%w: event %q cannot carry surface metadata", ErrInvalidSurface, event.EventType)
		}
		s.throughSeq = event.Seq
		return nil
	}
	if event.SurfaceOp == nil {
		return fmt.Errorf("%w: event %q requires a surface operation", ErrInvalidSurface, event.EventType)
	}
	if event.Seq == 0 {
		return fmt.Errorf("%w: surface event sequence must be positive", ErrInvalidSurface)
	}
	if _, err := deriveEventMessage(event.EventType, event.Data); err != nil {
		return err
	}
	if err := validateSourceEventSeqs(event); err != nil {
		return err
	}

	node := surfaceNode{
		Seq:             event.Seq,
		EventType:       event.EventType,
		Data:            cloneRawMessage(event.Data),
		SourceEventSeqs: cloneEventSeqs(event.SourceEventSeqs),
	}
	switch event.SurfaceOp.Kind {
	case SurfaceOpAppend:
		if event.SurfaceOp.ReplaceStart != 0 || event.SurfaceOp.ReplaceEnd != 0 {
			return fmt.Errorf("%w: append operation cannot carry a replacement range", ErrInvalidSurface)
		}
		s.nodes = append(s.nodes, node)
		s.throughSeq = event.Seq
		return nil
	case SurfaceOpReplace:
		startIndex, endIndex, shadowed, err := s.replacementRange(*event.SurfaceOp)
		if err != nil {
			return err
		}
		if err := requireShadowedSources(event.SourceEventSeqs, shadowed); err != nil {
			return err
		}
		nodes := make([]surfaceNode, 0, len(s.nodes)-(endIndex-startIndex))
		nodes = append(nodes, s.nodes[:startIndex]...)
		nodes = append(nodes, node)
		nodes = append(nodes, s.nodes[endIndex+1:]...)
		s.nodes = nodes
		s.throughSeq = event.Seq
		s.replaceGeneration++
		return nil
	default:
		return fmt.Errorf("%w: unknown surface operation %q", ErrInvalidSurface, event.SurfaceOp.Kind)
	}
}

func (s Surface) replacementRange(op SurfaceOp) (int, int, []uint64, error) {
	startIndex := -1
	endIndex := -1
	for index, node := range s.nodes {
		if node.Seq == op.ReplaceStart {
			startIndex = index
		}
		if node.Seq == op.ReplaceEnd {
			endIndex = index
		}
	}
	if startIndex < 0 {
		return 0, 0, nil, fmt.Errorf("%w: replacement start seq %d is not on the surface", ErrInvalidSurface, op.ReplaceStart)
	}
	if endIndex < 0 {
		return 0, 0, nil, fmt.Errorf("%w: replacement end seq %d is not on the surface", ErrInvalidSurface, op.ReplaceEnd)
	}
	if startIndex > endIndex {
		return 0, 0, nil, fmt.Errorf("%w: replacement start seq %d is after end seq %d", ErrInvalidSurface, op.ReplaceStart, op.ReplaceEnd)
	}
	shadowed := make([]uint64, 0, endIndex-startIndex+1)
	for _, node := range s.nodes[startIndex : endIndex+1] {
		shadowed = append(shadowed, node.Seq)
	}
	return startIndex, endIndex, shadowed, nil
}

func validateSourceEventSeqs(event Event) error {
	if event.SourceEventSeqs == nil {
		return nil
	}
	if len(event.SourceEventSeqs) == 0 && event.EventType != SurfaceEventAssistantMessage {
		return fmt.Errorf("%w: source event sequences cannot be empty for %q", ErrInvalidSurface, event.EventType)
	}
	seen := make(map[uint64]struct{}, len(event.SourceEventSeqs))
	for _, source := range event.SourceEventSeqs {
		if source == 0 {
			return fmt.Errorf("%w: source event sequences must be positive", ErrInvalidSurface)
		}
		if source >= event.Seq {
			return fmt.Errorf("%w: source seq %d must be earlier than current seq %d", ErrInvalidSurface, source, event.Seq)
		}
		if _, exists := seen[source]; exists {
			return fmt.Errorf("%w: source event sequences contain duplicate %d", ErrInvalidSurface, source)
		}
		seen[source] = struct{}{}
	}
	return nil
}

func requireShadowedSources(sources, shadowed []uint64) error {
	provided := make(map[uint64]struct{}, len(sources))
	for _, source := range sources {
		provided[source] = struct{}{}
	}
	var missing []uint64
	for _, seq := range shadowed {
		if _, exists := provided[seq]; !exists {
			missing = append(missing, seq)
		}
	}
	if len(missing) != 0 {
		return fmt.Errorf("%w: replacement sources do not cover shadowed nodes %v", ErrInvalidSurface, missing)
	}
	return nil
}

func IsSurfaceEligibleType(eventType string) bool {
	switch eventType {
	case SurfaceEventUserMessage, SurfaceEventAssistantMessage, SurfaceEventToolResult:
		return true
	default:
		return false
	}
}

// DeriveMessages returns a detached, deterministic model transcript in current
// surface order. Each returned JSON message is copied from the durable event.
func (s Surface) DeriveMessages() ([]json.RawMessage, error) {
	messages := make([]json.RawMessage, 0, len(s.nodes))
	for _, node := range s.nodes {
		message, err := deriveEventMessage(node.EventType, node.Data)
		if err != nil {
			return nil, err
		}
		if message != nil {
			messages = append(messages, message)
		}
	}
	return messages, nil
}

func surfaceEventForReplay(event Event) (Event, error) {
	legacy := event
	if event.SchemaVersion.Minor < SurfaceSchemaMinor && len(event.SourceEventSeqs) == 0 {
		legacy.SourceEventSeqs = nil
	}
	if !IsSurfaceEligibleType(event.EventType) || legacy.SurfaceOp != nil || event.SchemaVersion.Minor >= SurfaceSchemaMinor {
		return legacy, nil
	}
	legacy.SurfaceOp = AppendSurfaceOp()
	if event.EventType == SurfaceEventAssistantMessage || event.EventType == SurfaceEventToolResult {
		var envelope struct {
			Message json.RawMessage `json:"message"`
		}
		if err := json.Unmarshal(event.Data, &envelope); err != nil {
			return Event{}, fmt.Errorf("%w: decode legacy event %q: %v", ErrInvalidSurface, event.EventType, err)
		}
		if len(envelope.Message) == 0 {
			wrapped, err := json.Marshal(struct {
				Message json.RawMessage `json:"message"`
			}{Message: event.Data})
			if err != nil {
				return Event{}, fmt.Errorf("%w: upgrade legacy event %q: %v", ErrInvalidSurface, event.EventType, err)
			}
			legacy.Data = wrapped
		}
	}
	return legacy, nil
}

func deriveEventMessage(eventType string, data json.RawMessage) (json.RawMessage, error) {
	if !isJSONObject(data) {
		return nil, fmt.Errorf("%w: event %q requires JSON object data", ErrInvalidSurface, eventType)
	}
	switch eventType {
	case SurfaceEventUserMessage:
		return cloneRawMessage(data), nil
	case SurfaceEventAssistantMessage, SurfaceEventToolResult:
		var envelope struct {
			Message json.RawMessage `json:"message"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil || !isJSONObject(envelope.Message) {
			return nil, fmt.Errorf("%w: event %q requires a valid message object", ErrInvalidSurface, eventType)
		}
		if eventType == SurfaceEventAssistantMessage {
			var assistant struct {
				Content json.RawMessage `json:"content"`
			}
			if err := json.Unmarshal(envelope.Message, &assistant); err != nil {
				return nil, fmt.Errorf("%w: decode assistant message: %v", ErrInvalidSurface, err)
			}
			if assistant.Content != nil {
				var content []json.RawMessage
				if err := json.Unmarshal(assistant.Content, &content); err != nil {
					return nil, fmt.Errorf("%w: assistant message content must be an array", ErrInvalidSurface)
				}
				if len(content) == 0 {
					return nil, nil
				}
			}
		}
		return cloneRawMessage(envelope.Message), nil
	default:
		return nil, nil
	}
}

func isJSONObject(value []byte) bool {
	if !json.Valid(value) {
		return false
	}
	var object map[string]json.RawMessage
	return json.Unmarshal(value, &object) == nil && object != nil
}

func cloneSurfaceOp(op *SurfaceOp) *SurfaceOp {
	if op == nil {
		return nil
	}
	cloned := *op
	return &cloned
}

func cloneEventSeqs(seqs []uint64) []uint64 {
	if seqs == nil {
		return nil
	}
	return append([]uint64{}, seqs...)
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	return append(json.RawMessage{}, value...)
}
