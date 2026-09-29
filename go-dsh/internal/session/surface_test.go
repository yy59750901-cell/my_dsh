package session

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func surfaceEvent(seq uint64, eventType string, data string, op *SurfaceOp, sources []uint64) Event {
	return Event{
		SchemaVersion:   SchemaVersion{Major: 1, Minor: SurfaceSchemaMinor},
		EventType:       eventType,
		EventID:         "event",
		SessionID:       "session-1",
		Seq:             seq,
		ReplayPolicy:    ReplayRequired,
		Data:            json.RawMessage(data),
		SourceEventSeqs: sources,
		SurfaceOp:       op,
	}
}

func TestSurfaceAppendsEligibleEventsAndDerivesMessages(t *testing.T) {
	var surface Surface
	events := []Event{
		surfaceEvent(1, SurfaceEventUserMessage, `{"role":"user","content":[{"type":"text","text":"hello"}]}`, AppendSurfaceOp(), nil),
		surfaceEvent(2, "step/start", `{}`, nil, nil),
		surfaceEvent(3, SurfaceEventToolResult, `{"message":{"role":"tool","content":[{"type":"text","text":"result"}]}}`, AppendSurfaceOp(), nil),
		surfaceEvent(4, SurfaceEventAssistantMessage, `{"message":{"role":"assistant","content":[]},"usage":{"outputTokens":1}}`, AppendSurfaceOp(), []uint64{}),
	}
	for _, event := range events {
		if err := surface.Apply(event); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := surface.Nodes(), []uint64{1, 3, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
	messages, err := surface.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("message count = %d, want 2", len(messages))
	}
	assertJSONEqual(t, messages[0], `{"role":"user","content":[{"type":"text","text":"hello"}]}`)
	assertJSONEqual(t, messages[1], `{"role":"tool","content":[{"type":"text","text":"result"}]}`)
}

func TestSurfaceReplacementShadowsRangeWithoutChangingPriorEvents(t *testing.T) {
	var surface Surface
	original := []Event{
		surfaceEvent(1, SurfaceEventUserMessage, `{"role":"user","content":[{"type":"text","text":"first"}]}`, AppendSurfaceOp(), nil),
		surfaceEvent(2, SurfaceEventAssistantMessage, `{"message":{"role":"assistant","content":[{"type":"text","text":"second"}]}}`, AppendSurfaceOp(), nil),
		surfaceEvent(3, SurfaceEventToolResult, `{"message":{"role":"tool","content":[{"type":"text","text":"third"}]}}`, AppendSurfaceOp(), nil),
	}
	for _, event := range original {
		if err := surface.Apply(event); err != nil {
			t.Fatal(err)
		}
	}
	replacement := surfaceEvent(
		4,
		SurfaceEventAssistantMessage,
		`{"message":{"role":"assistant","content":[{"type":"text","text":"summary"}]}}`,
		ReplaceSurfaceOp(1, 2),
		[]uint64{1, 2},
	)
	if err := surface.Apply(replacement); err != nil {
		t.Fatal(err)
	}
	if got, want := surface.Nodes(), []uint64{4, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nodes = %v, want %v", got, want)
	}
	if surface.ReplaceGeneration() != 1 {
		t.Fatalf("replace generation = %d, want 1", surface.ReplaceGeneration())
	}
	messages, err := surface.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, messages[0], `{"role":"assistant","content":[{"type":"text","text":"summary"}]}`)
	assertJSONEqual(t, messages[1], `{"role":"tool","content":[{"type":"text","text":"third"}]}`)
	if got := string(original[0].Data); got != `{"role":"user","content":[{"type":"text","text":"first"}]}` {
		t.Fatalf("original event data changed: %s", got)
	}
}

func TestSurfaceRejectsInvalidReplacementAtomically(t *testing.T) {
	var surface Surface
	for _, event := range []Event{
		surfaceEvent(1, SurfaceEventUserMessage, `{"role":"user","content":[]}`, AppendSurfaceOp(), nil),
		surfaceEvent(2, SurfaceEventUserMessage, `{"role":"user","content":[]}`, AppendSurfaceOp(), nil),
	} {
		if err := surface.Apply(event); err != nil {
			t.Fatal(err)
		}
	}
	before := surface.Clone()
	err := surface.Apply(surfaceEvent(
		3,
		SurfaceEventAssistantMessage,
		`{"message":{"role":"assistant","content":[{"type":"text","text":"summary"}]}}`,
		ReplaceSurfaceOp(1, 2),
		[]uint64{1},
	))
	if !errors.Is(err, ErrInvalidSurface) {
		t.Fatalf("error = %v, want ErrInvalidSurface", err)
	}
	if !reflect.DeepEqual(surface.Nodes(), before.Nodes()) || surface.ReplaceGeneration() != before.ReplaceGeneration() {
		t.Fatal("failed replacement mutated the surface")
	}
}

func TestSurfaceValidatesMetadataAndProvenance(t *testing.T) {
	tests := []struct {
		name  string
		event Event
	}{
		{
			name:  "eligible event requires operation",
			event: surfaceEvent(1, SurfaceEventUserMessage, `{}`, nil, nil),
		},
		{
			name:  "non surface event rejects operation",
			event: surfaceEvent(1, "turn/start", `{}`, AppendSurfaceOp(), nil),
		},
		{
			name:  "non surface event rejects sources",
			event: surfaceEvent(2, "turn/start", `{}`, nil, []uint64{1}),
		},
		{
			name:  "user message rejects explicit empty sources",
			event: surfaceEvent(1, SurfaceEventUserMessage, `{}`, AppendSurfaceOp(), []uint64{}),
		},
		{
			name:  "sources reject zero",
			event: surfaceEvent(2, SurfaceEventUserMessage, `{}`, AppendSurfaceOp(), []uint64{0}),
		},
		{
			name:  "sources reject duplicates",
			event: surfaceEvent(2, SurfaceEventUserMessage, `{}`, AppendSurfaceOp(), []uint64{1, 1}),
		},
		{
			name:  "sources reject current sequence",
			event: surfaceEvent(2, SurfaceEventUserMessage, `{}`, AppendSurfaceOp(), []uint64{2}),
		},
		{
			name:  "replacement start must exist",
			event: surfaceEvent(2, SurfaceEventUserMessage, `{}`, ReplaceSurfaceOp(1, 1), []uint64{1}),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var surface Surface
			if err := surface.Apply(test.event); !errors.Is(err, ErrInvalidSurface) {
				t.Fatalf("error = %v, want ErrInvalidSurface", err)
			}
		})
	}
}

func TestSurfaceCloneDoesNotAliasMutableData(t *testing.T) {
	var surface Surface
	if err := surface.Apply(surfaceEvent(1, SurfaceEventUserMessage, `{"role":"user","content":[]}`, AppendSurfaceOp(), nil)); err != nil {
		t.Fatal(err)
	}
	clone := surface.Clone()
	if err := clone.Apply(surfaceEvent(2, SurfaceEventUserMessage, `{"role":"user","content":[]}`, AppendSurfaceOp(), nil)); err != nil {
		t.Fatal(err)
	}
	if got := surface.Nodes(); !reflect.DeepEqual(got, []uint64{1}) {
		t.Fatalf("original nodes = %v", got)
	}
	if got := clone.Nodes(); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("clone nodes = %v", got)
	}
}

func TestSurfaceOperationJSONMatchesSessionEnvelopeShape(t *testing.T) {
	appendJSON, err := json.Marshal(AppendSurfaceOp())
	if err != nil {
		t.Fatal(err)
	}
	if string(appendJSON) != `"append"` {
		t.Fatalf("append JSON = %s", appendJSON)
	}
	replaceJSON, err := json.Marshal(ReplaceSurfaceOp(4, 7))
	if err != nil {
		t.Fatal(err)
	}
	assertJSONEqual(t, replaceJSON, `{"op":"replace","start":4,"end":7}`)
	var decoded SurfaceOp
	if err := json.Unmarshal(replaceJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != *ReplaceSurfaceOp(4, 7) {
		t.Fatalf("decoded = %#v", decoded)
	}
}

type replaySurfaceStore struct {
	events []Event
}

func (s replaySurfaceStore) Create(context.Context, NewSession) error { return nil }
func (s replaySurfaceStore) ClaimWriter(context.Context, string) (uint64, uint64, error) {
	return 1, uint64(len(s.events)), nil
}
func (s replaySurfaceStore) Append(context.Context, string, uint64, uint64, []NewEvent) ([]Event, error) {
	panic("not used")
}
func (s replaySurfaceStore) Load(_ context.Context, _ string, afterSeq uint64, limit int) ([]Event, error) {
	if afterSeq >= uint64(len(s.events)) {
		return nil, nil
	}
	end := int(afterSeq) + limit
	if end > len(s.events) {
		end = len(s.events)
	}
	return append([]Event(nil), s.events[int(afterSeq):end]...), nil
}
func (s replaySurfaceStore) Head(context.Context, string) (uint64, error) {
	return uint64(len(s.events)), nil
}

func TestReplayUpgradesLegacySurfaceEvents(t *testing.T) {
	events := []Event{
		{
			SchemaVersion: SchemaVersion{Major: 1, Minor: 0}, EventType: SurfaceEventUserMessage,
			EventID: "legacy-1", SessionID: "session-1", Seq: 1, ReplayPolicy: ReplayRequired,
			Data: json.RawMessage(`{"text":"hello"}`),
		},
		{
			SchemaVersion: SchemaVersion{Major: 1, Minor: 0}, EventType: SurfaceEventAssistantMessage,
			EventID: "legacy-2", SessionID: "session-1", Seq: 2, ReplayPolicy: ReplayRequired,
			Data: json.RawMessage(`{"text":"legacy answer"}`),
		},
	}
	snapshot, err := Replay(context.Background(), replaySurfaceStore{events: events}, "session-1", 1, 2, ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := snapshot.Surface().Nodes(); !reflect.DeepEqual(got, []uint64{1, 2}) {
		t.Fatalf("legacy nodes = %v", got)
	}
	messages, err := snapshot.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("legacy message count = %d, want 2", len(messages))
	}
	assertJSONEqual(t, messages[1], `{"text":"legacy answer"}`)
}

func TestReplayRequiresSurfaceMetadataForCurrentSchema(t *testing.T) {
	event := surfaceEvent(1, SurfaceEventUserMessage, `{"text":"hello"}`, nil, nil)
	_, err := Replay(context.Background(), replaySurfaceStore{events: []Event{event}}, "session-1", 1, 1, ReplayOptions{})
	if !errors.Is(err, ErrInvalidSurface) {
		t.Fatalf("error = %v, want ErrInvalidSurface", err)
	}
}

func TestReplayRebuildsDeterministicSurface(t *testing.T) {
	events := []Event{
		surfaceEvent(1, SurfaceEventUserMessage, `{"role":"user","content":[{"type":"text","text":"first"}]}`, AppendSurfaceOp(), nil),
		surfaceEvent(2, SurfaceEventAssistantMessage, `{"message":{"role":"assistant","content":[{"type":"text","text":"answer"}]}}`, AppendSurfaceOp(), nil),
		surfaceEvent(3, SurfaceEventUserMessage, `{"role":"user","content":[{"type":"text","text":"summary"}]}`, ReplaceSurfaceOp(1, 2), []uint64{1, 2}),
	}
	store := replaySurfaceStore{events: events}
	first, err := Replay(context.Background(), store, "session-1", 1, 3, ReplayOptions{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Replay(context.Background(), store, "session-1", 2, 3, ReplayOptions{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	firstMessages, err := first.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	secondMessages, err := second.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first.Surface().Nodes(), []uint64{3}) || !reflect.DeepEqual(firstMessages, secondMessages) {
		t.Fatalf("first nodes=%v first messages=%s second messages=%s", first.Surface().Nodes(), firstMessages, secondMessages)
	}
}

func assertJSONEqual(t *testing.T, actual json.RawMessage, expected string) {
	t.Helper()
	var actualValue any
	if err := json.Unmarshal(actual, &actualValue); err != nil {
		t.Fatalf("decode actual JSON: %v", err)
	}
	var expectedValue any
	if err := json.Unmarshal([]byte(expected), &expectedValue); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	if !reflect.DeepEqual(actualValue, expectedValue) {
		t.Fatalf("actual JSON = %s, want %s", actual, expected)
	}
}
