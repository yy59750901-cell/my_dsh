package session_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

type memoryEventStore struct {
	mu                    sync.Mutex
	epoch                 uint64
	events                []session.Event
	claimCalls            int
	claimStart            chan struct{}
	claimGate             chan struct{}
	appendStart           chan struct{}
	appendGate            chan struct{}
	appendErrorAfterWrite error
}

func (s *memoryEventStore) Create(context.Context, session.NewSession) error {
	return nil
}

func (s *memoryEventStore) ClaimWriter(ctx context.Context, _ string) (uint64, uint64, error) {
	s.mu.Lock()
	s.claimCalls++
	if s.claimStart != nil && s.claimCalls == 1 {
		close(s.claimStart)
	}
	gate := s.claimGate
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	return s.epoch, uint64(len(s.events)), nil
}

func (s *memoryEventStore) Append(ctx context.Context, sessionID string, epoch uint64, expectedSeq uint64, newEvents []session.NewEvent) ([]session.Event, error) {
	if len(newEvents) != 0 && s.appendGate != nil {
		if s.appendStart != nil {
			close(s.appendStart)
		}
		select {
		case <-s.appendGate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch != s.epoch {
		return nil, session.ErrWriterFenced
	}
	if expectedSeq != uint64(len(s.events)) {
		return nil, session.ErrConflict
	}
	committed := make([]session.Event, 0, len(newEvents))
	for _, newEvent := range newEvents {
		committedAt := time.Now().UTC().Truncate(time.Microsecond)
		occurredAt := newEvent.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = committedAt
		} else {
			occurredAt = occurredAt.UTC().Truncate(time.Microsecond)
		}
		event := session.Event{
			SchemaVersion:    newEvent.SchemaVersion,
			EventType:        newEvent.EventType,
			EventID:          newEvent.EventID,
			SessionID:        sessionID,
			Seq:              uint64(len(s.events) + 1),
			OccurredAt:       occurredAt,
			CommittedAt:      committedAt,
			ReplayPolicy:     newEvent.ReplayPolicy,
			Data:             newEvent.Data,
			TurnID:           newEvent.TurnID,
			StepID:           newEvent.StepID,
			CallID:           newEvent.CallID,
			Trace:            newEvent.Trace,
			CausationEventID: newEvent.CausationEventID,
			SourceEventSeqs:  cloneEventSeqs(newEvent.SourceEventSeqs),
			SurfaceOp:        cloneSurfaceOp(newEvent.SurfaceOp),
			Extensions:       append(json.RawMessage(nil), newEvent.Extensions...),
		}
		s.events = append(s.events, event)
		committed = append(committed, event)
	}
	if s.appendErrorAfterWrite != nil && len(newEvents) != 0 {
		err := s.appendErrorAfterWrite
		s.appendErrorAfterWrite = nil
		return nil, err
	}
	return committed, nil
}

func (s *memoryEventStore) Load(_ context.Context, sessionID string, afterSeq uint64, limit int) ([]session.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if afterSeq >= uint64(len(s.events)) {
		return nil, nil
	}
	end := int(afterSeq) + limit
	if end > len(s.events) {
		end = len(s.events)
	}
	result := append([]session.Event(nil), s.events[int(afterSeq):end]...)
	for index := range result {
		if result[index].SessionID == "" {
			result[index].SessionID = sessionID
		}
	}
	return result, nil
}

func (s *memoryEventStore) Head(context.Context, string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.events)), nil
}

func (s *memoryEventStore) snapshotEvents() []session.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]session.Event(nil), s.events...)
}

func newUserMessage(eventID string) session.NewEvent {
	return session.NewEvent{
		SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor},
		EventType:     "user/message",
		EventID:       eventID,
		OccurredAt:    time.Now(),
		ReplayPolicy:  session.ReplayRequired,
		Data:          json.RawMessage(`{"text":"hello"}`),
		SurfaceOp:     session.AppendSurfaceOp(),
	}
}

func cloneEventSeqs(value []uint64) []uint64 {
	if value == nil {
		return nil
	}
	return append([]uint64{}, value...)
}

func cloneSurfaceOp(value *session.SurfaceOp) *session.SurfaceOp {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func TestActorSerializesConcurrentSubmissions(t *testing.T) {
	store := new(memoryEventStore)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{MailboxSize: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	const commandCount = 64
	var group sync.WaitGroup
	group.Add(commandCount)
	errorsCh := make(chan error, commandCount)
	for index := range commandCount {
		index := index
		go func() {
			defer group.Done()
			_, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
				return []session.NewEvent{newUserMessage(fmt.Sprintf("event-%d", index))}, nil
			}))
			errorsCh <- err
		}()
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}

	events := store.snapshotEvents()
	if len(events) != commandCount {
		t.Fatalf("event count = %d, want %d", len(events), commandCount)
	}
	seqs := make([]int, 0, len(events))
	for _, event := range events {
		seqs = append(seqs, int(event.Seq))
	}
	sort.Ints(seqs)
	for index, seq := range seqs {
		if seq != index+1 {
			t.Fatalf("seqs = %v", seqs)
		}
	}
	if snapshot := actor.Snapshot(); snapshot.HeadSeq != commandCount {
		t.Fatalf("head = %d, want %d", snapshot.HeadSeq, commandCount)
	}
}

type failingPublisher struct {
	calls int
	err   error
}

func (p *failingPublisher) PublishCommitted(context.Context, []session.Event) []error {
	p.calls++
	return []error{p.err}
}

func TestActorUpdatesSurfaceFromCommittedEvents(t *testing.T) {
	store := new(memoryEventStore)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	if _, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-1")}, nil
	})); err != nil {
		t.Fatal(err)
	}
	snapshot := actor.Snapshot()
	if got := snapshot.Surface().Nodes(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("surface nodes = %v, want [1]", got)
	}
	messages, err := snapshot.DeriveMessages()
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || string(messages[0]) != `{"text":"hello"}` {
		t.Fatalf("messages = %s", messages)
	}

	// Snapshot must be detached from actor state.
	detachedSurface := snapshot.Surface()
	if err := detachedSurface.Apply(session.Event{
		SchemaVersion: session.SchemaVersion{Major: 1, Minor: session.SurfaceSchemaMinor},
		EventType:     "user/message",
		EventID:       "detached",
		SessionID:     "session-1",
		Seq:           2,
		ReplayPolicy:  session.ReplayRequired,
		Data:          json.RawMessage(`{"text":"detached"}`),
		SurfaceOp:     session.AppendSurfaceOp(),
	}); err != nil {
		t.Fatal(err)
	}
	if got := actor.Snapshot().Surface().Nodes(); len(got) != 1 {
		t.Fatalf("actor surface was mutated through snapshot: %v", got)
	}
}

func TestActorRejectsInvalidSurfaceBeforeAppend(t *testing.T) {
	store := new(memoryEventStore)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	_, err = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		invalid := newUserMessage("event-1")
		invalid.SurfaceOp = nil
		return []session.NewEvent{invalid}, nil
	}))
	if !errors.Is(err, session.ErrInvalidSurface) {
		t.Fatalf("error = %v, want ErrInvalidSurface", err)
	}
	if events := store.snapshotEvents(); len(events) != 0 {
		t.Fatalf("invalid surface event was appended: %#v", events)
	}
}

func TestActorObserverFailureDoesNotRollbackCommittedEvents(t *testing.T) {
	store := new(memoryEventStore)
	observerErr := errors.New("observer failed")
	publisher := &failingPublisher{err: observerErr}
	var observed []error
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{
		Publisher: publisher,
		OnObserverError: func(err error) {
			observed = append(observed, err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	events, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-1")}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || publisher.calls != 1 || len(observed) != 1 || !errors.Is(observed[0], observerErr) {
		t.Fatalf("events=%d calls=%d observed=%v", len(events), publisher.calls, observed)
	}
	if len(store.snapshotEvents()) != 1 {
		t.Fatal("observer failure must not roll back committed event")
	}
}

func TestActorBoundsPersistenceAfterMailboxAdmission(t *testing.T) {
	store := &memoryEventStore{appendStart: make(chan struct{}), appendGate: make(chan struct{})}
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{PersistenceTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	started := time.Now()
	_, err = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-timeout")}, nil
	}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("persistence timeout took %v", elapsed)
	}
	if _, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return nil, nil
	})); err != nil {
		t.Fatalf("actor mailbox remained blocked after persistence timeout: %v", err)
	}
}

func TestActorRecoversAfterAppendResultIsUncertain(t *testing.T) {
	appendErr := errors.New("transport lost after commit")
	store := &memoryEventStore{appendErrorAfterWrite: appendErr}
	publisher := &failingPublisher{}
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{Publisher: publisher})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	recoveredEvents, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-1")}, nil
	}))
	if !errors.Is(err, appendErr) {
		t.Fatalf("error = %v, want append uncertainty", err)
	}
	if len(recoveredEvents) != 1 || recoveredEvents[0].EventID != "event-1" || recoveredEvents[0].Seq != 1 {
		t.Fatalf("recovered events = %+v, want authoritative event-1 receipt", recoveredEvents)
	}
	if snapshot := actor.Snapshot(); snapshot.HeadSeq != 1 || snapshot.Surface().ThroughSeq() != 1 {
		t.Fatalf("actor did not recover committed state: head=%d surface=%d", snapshot.HeadSeq, snapshot.Surface().ThroughSeq())
	}
	if publisher.calls != 1 {
		t.Fatalf("recovered events were not published: calls=%d", publisher.calls)
	}
	if _, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-2")}, nil
	})); err != nil {
		t.Fatalf("actor did not remain usable after recovery: %v", err)
	}
}

func TestActorSubmitWaitsForAuthoritativeResultAfterAdmission(t *testing.T) {
	store := &memoryEventStore{
		appendStart: make(chan struct{}),
		appendGate:  make(chan struct{}),
	}
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())
	var releaseAppendOnce sync.Once
	releaseAppend := func() { releaseAppendOnce.Do(func() { close(store.appendGate) }) }
	defer releaseAppend()

	ctx, cancel := context.WithCancel(context.Background())
	type submitResult struct {
		events []session.Event
		err    error
	}
	resultCh := make(chan submitResult, 1)
	go func() {
		events, err := actor.Submit(ctx, session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			return []session.NewEvent{newUserMessage("event-1")}, nil
		}))
		resultCh <- submitResult{events: events, err: err}
	}()

	select {
	case <-store.appendStart:
	case <-time.After(2 * time.Second):
		t.Fatal("append did not start")
	}
	cancel()
	select {
	case result := <-resultCh:
		t.Fatalf("submit returned before append outcome was known: events=%d err=%v", len(result.events), result.err)
	case <-time.After(250 * time.Millisecond):
	}

	releaseAppend()
	select {
	case result := <-resultCh:
		if result.err != nil || len(result.events) != 1 || result.events[0].EventID != "event-1" {
			t.Fatalf("result = events:%+v err:%v, want committed event", result.events, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("submit did not return after append completed")
	}
	if got := len(store.snapshotEvents()); got != 1 {
		t.Fatalf("persisted events = %d, want 1", got)
	}
}

func TestActorSubmitRejectsCancelledContextBeforeAdmission(t *testing.T) {
	store := new(memoryEventStore)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{MailboxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		_, _ = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			close(started)
			<-release
			return nil, nil
		}))
		close(firstDone)
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resultCh := make(chan error, 1)
	go func() {
		_, err := actor.Submit(ctx, session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			return []session.NewEvent{newUserMessage("must-not-run")}, nil
		}))
		resultCh <- err
	}()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(release)
		<-firstDone
		t.Fatal("cancelled request was admitted behind the active command")
	}
	close(release)
	<-firstDone
	if len(store.snapshotEvents()) != 0 {
		t.Fatal("cancelled command persisted an event")
	}
}

func TestReplayRejectsUnknownRequiredAndSkipsIgnorable(t *testing.T) {
	store := &memoryEventStore{epoch: 1, events: []session.Event{
		{
			SchemaVersion:   session.SchemaVersion{Major: 99},
			EventType:       "extension/unknown",
			EventID:         "event-1",
			SessionID:       "session-1",
			Seq:             1,
			ReplayPolicy:    session.ReplayIgnorable,
			SourceEventSeqs: []uint64{1},
			SurfaceOp:       session.AppendSurfaceOp(),
		},
		{
			SchemaVersion: session.SchemaVersion{Major: 1},
			EventType:     "user/message",
			EventID:       "event-2",
			SessionID:     "session-1",
			Seq:           2,
			ReplayPolicy:  session.ReplayRequired,
			Data:          json.RawMessage(`{"text":"hello"}`),
			SurfaceOp:     session.AppendSurfaceOp(),
		},
	}}
	snapshot, err := session.Replay(context.Background(), store, "session-1", 2, 2, session.ReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.HeadSeq != 2 {
		t.Fatalf("head = %d, want 2", snapshot.HeadSeq)
	}
	if throughSeq := snapshot.Surface().ThroughSeq(); throughSeq != 2 {
		t.Fatalf("surface through seq = %d, want 2", throughSeq)
	}

	store.events[0].ReplayPolicy = session.ReplayRequired
	if _, err := session.Replay(context.Background(), store, "session-1", 2, 2, session.ReplayOptions{}); !errors.Is(err, session.ErrUnknownRequired) {
		t.Fatalf("error = %v, want ErrUnknownRequired", err)
	}
	store.events[0].ReplayPolicy = "invalid"
	if _, err := session.Replay(context.Background(), store, "session-1", 2, 2, session.ReplayOptions{}); !errors.Is(err, session.ErrInvalidEvent) {
		t.Fatalf("error = %v, want ErrInvalidEvent", err)
	}
}

type committedTimeProjection struct {
	value string
}

func TestActorProjectsCommittedEventValues(t *testing.T) {
	store := new(memoryEventStore)
	projectionKey := session.ProjectionKey("test/committed-time")
	schema := testSessionSchema(t, nil, nil, session.SchemaContribution{
		Name: "test-committed-time",
		Projections: []session.ProjectionSpec{{
			Key:   projectionKey,
			Order: 300,
			New:   func() any { return new(committedTimeProjection) },
			Clone: func(state any) any {
				cloned := *state.(*committedTimeProjection)
				return &cloned
			},
			Apply: func(state any, event session.Event) error {
				state.(*committedTimeProjection).value = event.CommittedAt.Format(time.RFC3339Nano)
				return nil
			},
			ValidateBoundary: func(any, session.ProjectionBoundary) error { return nil },
		}},
	})
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	if _, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-1")}, nil
	})); err != nil {
		t.Fatal(err)
	}
	projection, ok := session.ProjectionAs[*committedTimeProjection](actor.Snapshot(), projectionKey)
	if !ok || projection.value == (time.Time{}).Format(time.RFC3339Nano) {
		t.Fatal("actor retained the synthetic pre-commit projection")
	}
}

func TestActorValidationFailurePreventsAppend(t *testing.T) {
	store := new(memoryEventStore)
	validationErr := errors.New("validation failed")
	schema := testSessionSchema(t, session.CandidateEventValidatorFunc(func(session.Event) error {
		return validationErr
	}), nil)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer actor.Dispose(context.Background())

	_, err = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("event-1")}, nil
	}))
	if !errors.Is(err, validationErr) {
		t.Fatalf("error = %v, want validation error", err)
	}
	if len(store.snapshotEvents()) != 0 {
		t.Fatal("validation failure must be detected before append")
	}
}

func TestActorCommittedValidatorSeesSameShapeLiveAndReplay(t *testing.T) {
	store := new(memoryEventStore)
	validationCalls := 0
	validator := session.EventValidatorFunc(func(event session.Event) error {
		validationCalls++
		if event.CommittedAt.IsZero() || event.OccurredAt.IsZero() {
			return errors.New("committed validator received an uncommitted event")
		}
		return nil
	})
	schema := testSessionSchema(t, nil, validator)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		event := newUserMessage("event-1")
		event.OccurredAt = time.Time{}
		return []session.NewEvent{event}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if validationCalls != 1 {
		t.Fatalf("live validation calls = %d, want 1", validationCalls)
	}
	if err := actor.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}

	validationCalls = 0
	replayed, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	defer replayed.Dispose(context.Background())
	if validationCalls != 1 {
		t.Fatalf("replay validation calls = %d, want 1", validationCalls)
	}
}

func testSessionSchema(t *testing.T, candidate session.CandidateEventValidator, committed session.EventValidator, extra ...session.SchemaContribution) *session.SessionSchema {
	t.Helper()
	core := session.CoreSchemaContribution()
	for index := range core.Events {
		if core.Events[index].EventType != "user/message" {
			continue
		}
		if candidate != nil {
			core.Events[index].CandidateValidators = append(core.Events[index].CandidateValidators, candidate)
		}
		if committed != nil {
			core.Events[index].CommittedValidators = append(core.Events[index].CommittedValidators, committed)
		}
	}
	contributions := []session.SchemaContribution{core, session.SurfaceSchemaContribution()}
	contributions = append(contributions, extra...)
	schema, err := session.NewSessionSchema(1, contributions...)
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

func TestActorDisposeHonorsContextWhenMailboxIsFull(t *testing.T) {
	store := new(memoryEventStore)
	actor, err := session.LoadActor(context.Background(), store, "session-1", session.ActorOptions{MailboxSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan struct{})
	go func() {
		_, _ = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			close(started)
			<-release
			return nil, nil
		}))
		close(firstDone)
	}()
	<-started
	secondDone := make(chan struct{})
	go func() {
		_, _ = actor.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
			return nil, nil
		}))
		close(secondDone)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := actor.Dispose(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispose error = %v, want context.Canceled", err)
	}
	close(release)
	<-firstDone
	<-secondDone
	if err := actor.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRegistrySharedLoadSurvivesLeaderCancellation(t *testing.T) {
	store := &memoryEventStore{
		claimStart: make(chan struct{}),
		claimGate:  make(chan struct{}),
	}
	registry := session.NewRegistry(store, session.ActorOptions{})
	defer registry.Dispose(context.Background())

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := registry.GetOrLoad(leaderCtx, "session-1")
		leaderDone <- err
	}()
	<-store.claimStart
	followerDone := make(chan error, 1)
	go func() {
		lease, err := registry.GetOrLoad(context.Background(), "session-1")
		if lease != nil {
			lease.Release()
		}
		followerDone <- err
	}()
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	close(store.claimGate)
	if err := <-followerDone; err != nil {
		t.Fatalf("follower load failed: %v", err)
	}
	store.mu.Lock()
	claimCalls := store.claimCalls
	store.mu.Unlock()
	if claimCalls != 1 {
		t.Fatalf("ClaimWriter calls = %d, want 1", claimCalls)
	}
}

func TestRegistryReloadsActorAfterWriterIsFenced(t *testing.T) {
	store := new(memoryEventStore)
	registry := session.NewRegistry(store, session.ActorOptions{})
	defer registry.Dispose(context.Background())

	firstLease, err := registry.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	first := firstLease.Actor()
	if _, _, err := store.ClaimWriter(context.Background(), "session-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("fenced-event")}, nil
	})); !errors.Is(err, session.ErrWriterFenced) {
		t.Fatalf("submit error = %v, want ErrWriterFenced", err)
	}
	firstLease.Release()

	secondLease, err := registry.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release()
	if secondLease.Actor() == first {
		t.Fatal("registry returned the fenced actor")
	}
	if _, err := secondLease.Actor().Submit(context.Background(), session.CommandFunc(func(context.Context, session.Snapshot) ([]session.NewEvent, error) {
		return []session.NewEvent{newUserMessage("recovered-event")}, nil
	})); err != nil {
		t.Fatalf("reloaded actor submit failed: %v", err)
	}
}

func TestRegistryDoesNotEvictLeasedActor(t *testing.T) {
	store := new(memoryEventStore)
	registry := session.NewRegistry(store, session.ActorOptions{})
	defer registry.Dispose(context.Background())

	lease, err := registry.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	evicted, err := registry.EvictIdle(context.Background(), time.Minute, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if evicted != 0 || lease.Actor().IsClosed() {
		t.Fatalf("evicted=%d closed=%v", evicted, lease.Actor().IsClosed())
	}
	lease.Release()
}

func TestRegistryEvictsIdleActorBeforeReload(t *testing.T) {
	store := new(memoryEventStore)
	registry := session.NewRegistry(store, session.ActorOptions{})
	defer registry.Dispose(context.Background())

	firstLease, err := registry.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	first := firstLease.Actor()
	firstLease.Release()
	evicted, err := registry.EvictIdle(context.Background(), time.Minute, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if evicted != 1 || !first.IsClosed() {
		t.Fatalf("evicted=%d closed=%v", evicted, first.IsClosed())
	}
	secondLease, err := registry.GetOrLoad(context.Background(), "session-1")
	if err != nil {
		t.Fatal(err)
	}
	defer secondLease.Release()
	second := secondLease.Actor()
	if second == first {
		t.Fatal("expected a newly loaded actor after eviction")
	}
	store.mu.Lock()
	claimCalls := store.claimCalls
	store.mu.Unlock()
	if claimCalls != 2 {
		t.Fatalf("ClaimWriter calls = %d, want 2", claimCalls)
	}
}

func TestRegistryLoadsSessionOnceForConcurrentCallers(t *testing.T) {
	store := &memoryEventStore{
		claimStart: make(chan struct{}),
		claimGate:  make(chan struct{}),
	}
	registry := session.NewRegistry(store, session.ActorOptions{})
	defer registry.Dispose(context.Background())

	const callers = 16
	actors := make(chan *session.Actor, callers)
	errorsCh := make(chan error, callers)
	var group sync.WaitGroup
	group.Add(callers)
	for range callers {
		go func() {
			defer group.Done()
			lease, err := registry.GetOrLoad(context.Background(), "session-1")
			if lease != nil {
				actors <- lease.Actor()
				lease.Release()
			} else {
				actors <- nil
			}
			errorsCh <- err
		}()
	}
	<-store.claimStart
	close(store.claimGate)
	group.Wait()
	close(actors)
	close(errorsCh)

	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *session.Actor
	for actor := range actors {
		if first == nil {
			first = actor
			continue
		}
		if actor != first {
			t.Fatal("registry returned different actors for the same session")
		}
	}
	store.mu.Lock()
	claimCalls := store.claimCalls
	store.mu.Unlock()
	if claimCalls != 1 {
		t.Fatalf("ClaimWriter calls = %d, want 1", claimCalls)
	}
}
