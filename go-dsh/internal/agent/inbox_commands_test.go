package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

func TestInboxWriterPersistsFollowUpAndWakesRuntime(t *testing.T) {
	writer, store, runtime, dispose := newTestInboxWriter(t)
	defer dispose()

	receipt, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID:      "session-1",
		RequestID:      "request-1",
		IdempotencyKey: "key-1",
		Content:        []ContentBlock{{Type: "text", Text: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.SessionID != "session-1" || receipt.InputID == "" || receipt.AcceptedSeq != 1 || receipt.Placement != "queued" || receipt.Target != InboxNextTurn || receipt.Duplicate {
		t.Fatalf("receipt = %+v", receipt)
	}
	if runtime.callCount() != 1 {
		t.Fatalf("wake calls = %d, want 1", runtime.callCount())
	}
	if events := store.snapshot(); len(events) != 1 || events[0].EventType != EventInboxSpliced {
		t.Fatalf("events = %+v", events)
	}
}

func TestInboxWriterReturnsOriginalReceiptForIdempotentRetry(t *testing.T) {
	writer, store, runtime, dispose := newTestInboxWriter(t)
	defer dispose()

	first, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1", IdempotencyKey: "key-1",
		Content: []ContentBlock{{Type: "text", Text: "same payload"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-2", IdempotencyKey: "key-1",
		Content: []ContentBlock{{Type: "text", Text: "same payload"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.InputID != first.InputID || duplicate.CommandID != first.CommandID || duplicate.AcceptedSeq != first.AcceptedSeq || duplicate.RequestID != first.RequestID {
		t.Fatalf("first = %+v, duplicate = %+v", first, duplicate)
	}
	if got := len(store.snapshot()); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
	if runtime.callCount() != 2 {
		t.Fatalf("duplicate queued input must retry wake; calls = %d", runtime.callCount())
	}
}

func TestInboxWriterRejectsIdempotencyConflict(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()

	_, err := writer.Steer(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1", IdempotencyKey: "key-1",
		Content: []ContentBlock{{Type: "text", Text: "first"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Steer(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-2", IdempotencyKey: "key-1",
		Content: []ContentBlock{{Type: "text", Text: "changed"}},
	})
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}
	if got := len(store.snapshot()); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
}

func TestInboxWriterAcceptsMultipleRequestsWithoutIdempotencyKey(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()

	for index := 0; index < 2; index++ {
		_, err := writer.FollowUp(context.Background(), InboxRequest{
			SessionID: "session-1",
			RequestID: fmt.Sprintf("request-%d", index+1),
			Content:   []ContentBlock{{Type: "text", Text: "same payload"}},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := len(store.snapshot()); got != 2 {
		t.Fatalf("persisted events = %d, want 2", got)
	}
}

func TestInboxWriterInjectPersistsWithoutWake(t *testing.T) {
	writer, _, runtime, dispose := newTestInboxWriter(t)
	defer dispose()

	receipt, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1", Source: InputSourceSystem,
		Content: []ContentBlock{{Type: "text", Text: "system context"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Placement != "context" || receipt.Target != InboxNextStep {
		t.Fatalf("receipt = %+v", receipt)
	}
	if runtime.callCount() != 0 {
		t.Fatalf("inject wake calls = %d, want 0", runtime.callCount())
	}
}

func TestInboxWriterReturnsReceiptWhenWakeFails(t *testing.T) {
	writer, store, runtime, dispose := newTestInboxWriter(t)
	defer dispose()
	runtime.err = errors.New("runtime unavailable")

	receipt, err := writer.FollowUp(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1", IdempotencyKey: "key-1",
		Content: []ContentBlock{{Type: "text", Text: "persist me"}},
	})
	if !errors.Is(err, ErrInputPersistedSchedulingFailed) {
		t.Fatalf("error = %v, want ErrInputPersistedSchedulingFailed", err)
	}
	if receipt.AcceptedSeq != 1 || receipt.InputID == "" || len(store.snapshot()) != 1 {
		t.Fatalf("receipt = %+v, events = %d", receipt, len(store.snapshot()))
	}
	var schedulingError *SchedulingError
	if !errors.As(err, &schedulingError) || schedulingError.Receipt.InputID != receipt.InputID {
		t.Fatalf("structured scheduling error = %#v", err)
	}
}

func TestInboxWriterRejectsInvalidInjectSource(t *testing.T) {
	writer, store, _, dispose := newTestInboxWriter(t)
	defer dispose()

	_, err := writer.Inject(context.Background(), InboxRequest{
		SessionID: "session-1", RequestID: "request-1", Source: InputSourceUser,
		Content: []ContentBlock{{Type: "text", Text: "invalid"}},
	})
	if !errors.Is(err, ErrInvalidInboxRequest) {
		t.Fatalf("error = %v, want ErrInvalidInboxRequest", err)
	}
	if len(store.snapshot()) != 0 {
		t.Fatal("invalid inject persisted an event")
	}
}

func TestCanonicalInputPayloadHashUsesRFC8785(t *testing.T) {
	left, err := canonicalInputPayloadHash(CommandInject, InputSourceSystem,
		[]ContentBlock{{Type: "data", Data: json.RawMessage(`{"value":1.0,"nested":{"b":2,"a":1}}`)}},
		map[string]json.RawMessage{"settings": json.RawMessage(`{"z":0,"a":true}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := canonicalInputPayloadHash(CommandInject, InputSourceSystem,
		[]ContentBlock{{Type: "data", Data: json.RawMessage(`{"nested":{"a":1,"b":2},"value":1}`)}},
		map[string]json.RawMessage{"settings": json.RawMessage(`{"a":true,"z":0}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	if left != right || len(left) != len("sha256:")+64 {
		t.Fatalf("hashes differ: left=%s right=%s", left, right)
	}
}

func TestCanonicalizeJSONMatchesRFC8785Vector(t *testing.T) {
	input := []byte(`{"literals":[null,true,false],"numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],"string":"€$\u000f\nA'B\"\\\\\"/"}`)
	want := `{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\u000f\nA'B\"\\\\\"/"}`
	canonical, err := canonicalizeJSON(input)
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != want {
		t.Fatalf("canonical json = %s, want %s", canonical, want)
	}
}

func TestInboxWriterRejectsUnsafeIJSONIntegers(t *testing.T) {
	tests := []struct {
		name     string
		content  []ContentBlock
		metadata map[string]json.RawMessage
	}{
		{name: "content", content: []ContentBlock{{Type: "data", Data: json.RawMessage(`{"id":9007199254740992}`)}}},
		{name: "metadata", content: []ContentBlock{{Type: "text", Text: "hello"}}, metadata: map[string]json.RawMessage{"cursor": json.RawMessage(`-9007199254740992`)}},
		{name: "exponent", content: []ContentBlock{{Type: "data", Data: json.RawMessage(`{"id":9.007199254740992e15}`)}}},
		{name: "fraction-above-range-a", content: []ContentBlock{{Type: "data", Data: json.RawMessage(`{"id":9007199254740992.1}`)}}},
		{name: "fraction-above-range-b", content: []ContentBlock{{Type: "data", Data: json.RawMessage(`{"id":9007199254740992.2}`)}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, store, _, dispose := newTestInboxWriter(t)
			defer dispose()
			_, err := writer.Inject(context.Background(), InboxRequest{
				SessionID: "session-1", RequestID: "request-1", Source: InputSourceSystem,
				Content: test.content, Metadata: test.metadata,
			})
			if !errors.Is(err, ErrInvalidInboxRequest) {
				t.Fatalf("error = %v, want ErrInvalidInboxRequest", err)
			}
			if len(store.snapshot()) != 0 {
				t.Fatal("unsafe numeric payload persisted an event")
			}
		})
	}
}

func TestInboxWriterWakesAfterContextCancellationDuringAppend(t *testing.T) {
	store := &memoryInboxEventStore{
		epoch:       1,
		appendStart: make(chan struct{}),
		appendGate:  make(chan struct{}),
	}
	writer, _, runtime, dispose := newTestInboxWriterWithStore(t, store)
	defer dispose()
	var releaseAppendOnce sync.Once
	releaseAppend := func() { releaseAppendOnce.Do(func() { close(store.appendGate) }) }
	defer releaseAppend()

	ctx, cancel := context.WithCancel(context.Background())
	type acceptResult struct {
		receipt Receipt
		err     error
	}
	resultCh := make(chan acceptResult, 1)
	go func() {
		receipt, err := writer.FollowUp(ctx, InboxRequest{
			SessionID: "session-1", RequestID: "request-1", IdempotencyKey: "key-1",
			Content: []ContentBlock{{Type: "text", Text: "persist and wake"}},
		})
		resultCh <- acceptResult{receipt: receipt, err: err}
	}()

	select {
	case <-store.appendStart:
	case <-time.After(2 * time.Second):
		t.Fatal("append did not start")
	}
	cancel()
	select {
	case result := <-resultCh:
		t.Fatalf("writer returned before append outcome was known: receipt=%+v err=%v", result.receipt, result.err)
	case <-time.After(250 * time.Millisecond):
	}

	releaseAppend()
	select {
	case result := <-resultCh:
		if result.err != nil || result.receipt.AcceptedSeq != 1 || result.receipt.InputID == "" {
			t.Fatalf("result = receipt:%+v err:%v", result.receipt, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not return after append completed")
	}
	if len(store.snapshot()) != 1 || runtime.callCount() != 1 {
		t.Fatalf("persisted=%d wake_calls=%d, want 1 and 1", len(store.snapshot()), runtime.callCount())
	}
}

func TestInboxWriterSerializesConcurrentIdempotentRequests(t *testing.T) {
	writer, store, runtime, dispose := newTestInboxWriter(t)
	defer dispose()

	const callers = 16
	receipts := make(chan Receipt, callers)
	errorsCh := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for index := range callers {
		index := index
		go func() {
			defer group.Done()
			receipt, err := writer.FollowUp(context.Background(), InboxRequest{
				SessionID: "session-1", RequestID: fmt.Sprintf("request-%d", index), IdempotencyKey: "shared-key",
				Content: []ContentBlock{{Type: "text", Text: "same payload"}},
			})
			receipts <- receipt
			errorsCh <- err
		}()
	}
	group.Wait()
	close(receipts)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	var inputID string
	nonDuplicates := 0
	for receipt := range receipts {
		if inputID == "" {
			inputID = receipt.InputID
		}
		if receipt.InputID != inputID || receipt.AcceptedSeq != 1 {
			t.Fatalf("receipt = %+v, want input %q at seq 1", receipt, inputID)
		}
		if !receipt.Duplicate {
			nonDuplicates++
		}
	}
	if nonDuplicates != 1 || len(store.snapshot()) != 1 || runtime.callCount() != callers {
		t.Fatalf("non_duplicates=%d persisted=%d wakes=%d", nonDuplicates, len(store.snapshot()), runtime.callCount())
	}
}

func newTestInboxWriter(t *testing.T) (*InboxWriter, *memoryInboxEventStore, *recordingWakeRequester, func()) {
	t.Helper()
	return newTestInboxWriterWithStore(t, &memoryInboxEventStore{epoch: 1})
}

func newTestInboxWriterWithStore(t *testing.T, store *memoryInboxEventStore) (*InboxWriter, *memoryInboxEventStore, *recordingWakeRequester, func()) {
	t.Helper()
	registry, err := NewSessionActorRegistry(store, SessionActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := new(recordingWakeRequester)
	var counter atomic.Uint64
	writer, err := NewInboxWriter(registry, runtime, InboxWriterOptions{
		IDGenerator: InboxIDGeneratorFunc(func(prefix string) (string, error) {
			return fmt.Sprintf("%s-%d", prefix, counter.Add(1)), nil
		}),
		Now: func() time.Time { return time.Date(2026, 8, 26, 14, 0, 0, 0, time.UTC) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return writer, store, runtime, func() {
		if err := registry.Dispose(context.Background()); err != nil {
			t.Errorf("dispose registry: %v", err)
		}
	}
}

type recordingWakeRequester struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (requester *recordingWakeRequester) RequestWake(sessionID string) error {
	requester.mu.Lock()
	defer requester.mu.Unlock()
	requester.calls = append(requester.calls, sessionID)
	return requester.err
}

func (requester *recordingWakeRequester) callCount() int {
	requester.mu.Lock()
	defer requester.mu.Unlock()
	return len(requester.calls)
}

type memoryInboxEventStore struct {
	mu                     sync.Mutex
	epoch                  uint64
	events                 []session.Event
	appendStart            chan struct{}
	appendGate             chan struct{}
	appendErrorAfterWrite  error
	appendErrorBeforeWrite error
}

func (store *memoryInboxEventStore) Create(context.Context, session.NewSession) error {
	return nil
}

func (store *memoryInboxEventStore) ClaimWriter(context.Context, string) (uint64, uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.epoch, uint64(len(store.events)), nil
}

func (store *memoryInboxEventStore) Append(_ context.Context, sessionID string, epoch, expectedSeq uint64, events []session.NewEvent) ([]session.Event, error) {
	if len(events) != 0 && store.appendGate != nil {
		if store.appendStart != nil {
			close(store.appendStart)
		}
		<-store.appendGate
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if epoch != store.epoch {
		return nil, session.ErrWriterFenced
	}
	if expectedSeq != uint64(len(store.events)) {
		return nil, session.ErrConflict
	}
	if store.appendErrorBeforeWrite != nil && len(events) > 0 {
		err := store.appendErrorBeforeWrite
		store.appendErrorBeforeWrite = nil
		return nil, err
	}
	committed := make([]session.Event, len(events))
	for index, event := range events {
		committedAt := time.Date(2026, 8, 26, 14, 1, index+len(store.events), 0, time.UTC)
		committed[index] = session.Event{
			SchemaVersion: event.SchemaVersion, EventType: event.EventType, EventID: event.EventID,
			SessionID: sessionID, Seq: uint64(len(store.events) + index + 1), OccurredAt: event.OccurredAt,
			CommittedAt: committedAt, ReplayPolicy: event.ReplayPolicy, Data: append(json.RawMessage(nil), event.Data...),
			TurnID: event.TurnID, StepID: event.StepID, CallID: event.CallID, Trace: event.Trace,
			CausationEventID: event.CausationEventID, SourceEventSeqs: append([]uint64(nil), event.SourceEventSeqs...),
			SurfaceOp: event.SurfaceOp, Extensions: append(json.RawMessage(nil), event.Extensions...),
		}
	}
	store.events = append(store.events, committed...)
	if store.appendErrorAfterWrite != nil && len(events) != 0 {
		err := store.appendErrorAfterWrite
		store.appendErrorAfterWrite = nil
		return nil, err
	}
	return append([]session.Event(nil), committed...), nil
}

func (store *memoryInboxEventStore) Load(_ context.Context, _ string, afterSeq uint64, limit int) ([]session.Event, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if afterSeq >= uint64(len(store.events)) {
		return nil, nil
	}
	end := int(afterSeq) + limit
	if end > len(store.events) {
		end = len(store.events)
	}
	return append([]session.Event(nil), store.events[int(afterSeq):end]...), nil
}

func (store *memoryInboxEventStore) Head(context.Context, string) (uint64, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return uint64(len(store.events)), nil
}

func (store *memoryInboxEventStore) snapshot() []session.Event {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]session.Event(nil), store.events...)
}
