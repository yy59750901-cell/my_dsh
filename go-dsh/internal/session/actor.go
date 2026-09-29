package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"
)

var (
	ErrActorClosed    = errors.New("session actor is closed")
	ErrActorUnhealthy = errors.New("session actor is unhealthy")
	ErrInvalidCommand = errors.New("invalid session command")
)

type Command interface {
	Decide(context.Context, Snapshot) ([]NewEvent, error)
}

type CommandFunc func(context.Context, Snapshot) ([]NewEvent, error)

func (f CommandFunc) Decide(ctx context.Context, snapshot Snapshot) ([]NewEvent, error) {
	return f(ctx, snapshot)
}

type CommittedPublisher interface {
	PublishCommitted(context.Context, []Event) []error
}

type ActorInitializer interface {
	Initialize(context.Context, Snapshot) ([]NewEvent, error)
}

type ActorInitializerFunc func(context.Context, Snapshot) ([]NewEvent, error)

func (f ActorInitializerFunc) Initialize(ctx context.Context, snapshot Snapshot) ([]NewEvent, error) {
	return f(ctx, snapshot)
}

type ActorOptions struct {
	MailboxSize        int
	PersistenceTimeout time.Duration
	Schema             *SessionSchema
	Initializers       []ActorInitializer
	Publisher          CommittedPublisher
	OnObserverError    func(error)
}

type commandRequest struct {
	ctx      context.Context
	command  Command
	response chan commandResponse
}

type commandResponse struct {
	events []Event
	err    error
}

type Actor struct {
	store              EventStore
	schema             *SessionSchema
	persistenceTimeout time.Duration
	publisher          CommittedPublisher
	onObserverError    func(error)

	stateMu    sync.RWMutex
	state      Snapshot
	fatal      error
	busy       bool
	lastActive time.Time

	admissionMu  sync.RWMutex
	accepting    bool
	useCount     int
	mailbox      chan commandRequest
	mailboxSpace chan struct{}
	stop         chan struct{}
	done         chan struct{}
	disposeOnce  sync.Once
}

func LoadActor(ctx context.Context, store EventStore, sessionID string, options ActorOptions) (*Actor, error) {
	if store == nil || sessionID == "" {
		return nil, ErrInvalidSession
	}
	if options.MailboxSize <= 0 {
		options.MailboxSize = 64
	}
	if options.PersistenceTimeout <= 0 {
		options.PersistenceTimeout = 30 * time.Second
	}
	if options.Schema == nil {
		options.Schema = DefaultSessionSchema()
	}
	if options.Initializers == nil {
		options.Initializers = []ActorInitializer{CoreRepairInitializer{}}
	} else {
		options.Initializers = append([]ActorInitializer(nil), options.Initializers...)
	}

	epoch, head, err := store.ClaimWriter(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	snapshot, err := Replay(ctx, store, sessionID, epoch, head, ReplayOptions{Schema: options.Schema})
	if err != nil {
		return nil, err
	}
	if _, err := store.Append(ctx, sessionID, epoch, head, nil); err != nil {
		return nil, err
	}
	for _, initializer := range options.Initializers {
		if initializer == nil {
			return nil, fmt.Errorf("%w: nil actor initializer", ErrInvalidCommand)
		}
		newEvents, err := initializer.Initialize(ctx, snapshot.Clone())
		if err != nil {
			return nil, err
		}
		if len(newEvents) == 0 {
			continue
		}
		snapshot, err = commitInitialization(ctx, store, options.Schema, snapshot, newEvents, options.PersistenceTimeout, options.Publisher, options.OnObserverError)
		if err != nil {
			return nil, err
		}
	}
	actor := &Actor{
		store:              store,
		schema:             options.Schema,
		persistenceTimeout: options.PersistenceTimeout,
		publisher:          options.Publisher,
		onObserverError:    options.OnObserverError,
		state:              snapshot,
		lastActive:         time.Now(),
		accepting:          true,
		mailbox:            make(chan commandRequest, options.MailboxSize),
		mailboxSpace:       make(chan struct{}, 1),
		stop:               make(chan struct{}),
		done:               make(chan struct{}),
	}
	go actor.run()
	return actor, nil
}

func commitInitialization(ctx context.Context, store EventStore, schema *SessionSchema, snapshot Snapshot, newEvents []NewEvent, persistenceTimeout time.Duration, publisher CommittedPublisher, onObserverError func(error)) (Snapshot, error) {
	candidates := make([]Event, len(newEvents))
	for index, newEvent := range newEvents {
		candidates[index] = eventBeforeCommit(snapshot.SessionID, snapshot.HeadSeq+uint64(index)+1, newEvent)
	}
	if _, err := schema.foldBatch(snapshot, candidates, BoundaryCandidateBatch, phaseCandidate); err != nil {
		return Snapshot{}, err
	}
	appendCtx, cancelAppend := context.WithTimeout(context.WithoutCancel(ctx), persistenceTimeout)
	committed, err := store.Append(appendCtx, snapshot.SessionID, snapshot.Epoch, snapshot.HeadSeq, newEvents)
	cancelAppend()
	if err != nil {
		return recoverInitializationAfterCommit(store, schema, snapshot, newEvents, err, persistenceTimeout, publisher, onObserverError)
	}
	if err := validateCommittedBatch(snapshot, newEvents, committed); err != nil {
		return recoverInitializationAfterCommit(store, schema, snapshot, newEvents, err, persistenceTimeout, publisher, onObserverError)
	}
	updated, err := schema.foldBatch(snapshot, committed, BoundaryCommittedBatch, phaseCommitted)
	if err != nil {
		return recoverInitializationAfterCommit(store, schema, snapshot, newEvents, err, persistenceTimeout, publisher, onObserverError)
	}
	publishInitializationEvents(publisher, onObserverError, committed)
	return updated, nil
}

func recoverInitializationAfterCommit(store EventStore, schema *SessionSchema, previous Snapshot, requested []NewEvent, cause error, persistenceTimeout time.Duration, publisher CommittedPublisher, onObserverError func(error)) (Snapshot, error) {
	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), persistenceTimeout)
	defer cancelRecovery()
	head, err := store.Head(recoveryCtx, previous.SessionID)
	if err == nil {
		var recovered Snapshot
		recovered, err = Replay(recoveryCtx, store, previous.SessionID, previous.Epoch, head, ReplayOptions{Schema: schema})
		if err == nil {
			_, err = store.Append(recoveryCtx, previous.SessionID, previous.Epoch, head, nil)
		}
		if err == nil {
			var recoveredEvents []Event
			recoveredEvents, err = loadCommittedRange(recoveryCtx, store, previous.SessionID, previous.HeadSeq, head)
			if err == nil {
				err = validateCommittedBatch(previous, requested, recoveredEvents)
			}
			if err == nil {
				publishInitializationEvents(publisher, onObserverError, recoveredEvents)
				return recovered, nil
			}
		}
	}
	return Snapshot{}, errors.Join(cause, err)
}

func publishInitializationEvents(publisher CommittedPublisher, onObserverError func(error), committed []Event) {
	if publisher == nil {
		return
	}
	for _, observerErr := range publisher.PublishCommitted(context.Background(), cloneEvents(committed)) {
		if observerErr != nil && onObserverError != nil {
			onObserverError(observerErr)
		}
	}
}

func (a *Actor) Snapshot() Snapshot {
	a.stateMu.RLock()
	defer a.stateMu.RUnlock()
	return a.state.Clone()
}

func (a *Actor) IsClosed() bool {
	a.admissionMu.RLock()
	defer a.admissionMu.RUnlock()
	return !a.accepting
}

func (a *Actor) acquireUse() bool {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	if !a.accepting {
		return false
	}
	a.useCount++
	a.stateMu.Lock()
	a.lastActive = time.Now()
	a.stateMu.Unlock()
	return true
}

func (a *Actor) releaseUse() {
	a.admissionMu.Lock()
	defer a.admissionMu.Unlock()
	if a.useCount > 0 {
		a.useCount--
	}
	a.stateMu.Lock()
	a.lastActive = time.Now()
	a.stateMu.Unlock()
}

func (a *Actor) tryShutdownIfIdle(before time.Time) bool {
	a.admissionMu.Lock()
	if !a.accepting || a.useCount != 0 || len(a.mailbox) != 0 {
		a.admissionMu.Unlock()
		return false
	}
	a.stateMu.RLock()
	idle := !a.busy && a.lastActive.Before(before)
	a.stateMu.RUnlock()
	if !idle {
		a.admissionMu.Unlock()
		return false
	}
	a.accepting = false
	a.admissionMu.Unlock()
	a.signalStop()
	return true
}

func (a *Actor) Submit(ctx context.Context, command Command) ([]Event, error) {
	if command == nil {
		return nil, ErrInvalidCommand
	}
	request := commandRequest{
		ctx:      ctx,
		command:  command,
		response: make(chan commandResponse, 1),
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		a.admissionMu.RLock()
		if !a.accepting {
			a.admissionMu.RUnlock()
			return nil, ErrActorClosed
		}
		sent := false
		select {
		case a.mailbox <- request:
			sent = true
		default:
		}
		a.admissionMu.RUnlock()
		if sent {
			break
		}
		select {
		case <-a.mailboxSpace:
		case <-a.stop:
			return nil, ErrActorClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	// Once admitted to the mailbox, the command owns a definitive outcome. The
	// request context still prevents queued work from starting and is passed to
	// Decide, but the caller must not return early while Append may already be in
	// flight: doing so would make a committed result indistinguishable from a
	// cancelled command.
	response := <-request.response
	return response.events, response.err
}

func (a *Actor) run() {
	defer close(a.done)
	for {
		select {
		case <-a.stop:
			a.rejectQueued()
			return
		default:
		}
		select {
		case request := <-a.mailbox:
			a.stateMu.Lock()
			a.busy = true
			a.stateMu.Unlock()
			a.notifyMailboxSpace()
			a.handle(request)
			a.stateMu.Lock()
			a.busy = false
			a.lastActive = time.Now()
			a.stateMu.Unlock()
		case <-a.stop:
			a.rejectQueued()
			return
		}
	}
}

func (a *Actor) rejectQueued() {
	for {
		select {
		case request := <-a.mailbox:
			a.notifyMailboxSpace()
			request.response <- commandResponse{err: ErrActorClosed}
		default:
			return
		}
	}
}

func (a *Actor) notifyMailboxSpace() {
	select {
	case a.mailboxSpace <- struct{}{}:
	default:
	}
}

func (a *Actor) handle(request commandRequest) {
	if err := request.ctx.Err(); err != nil {
		request.response <- commandResponse{err: err}
		return
	}

	a.stateMu.RLock()
	snapshot := a.state.Clone()
	fatal := a.fatal
	a.stateMu.RUnlock()
	if fatal != nil {
		request.response <- commandResponse{err: errors.Join(ErrActorUnhealthy, fatal)}
		return
	}

	newEvents, err := request.command.Decide(request.ctx, snapshot)
	if err != nil {
		request.response <- commandResponse{err: err}
		return
	}
	if len(newEvents) == 0 {
		request.response <- commandResponse{}
		return
	}
	if err := a.validateNewEvents(newEvents); err != nil {
		request.response <- commandResponse{err: err}
		return
	}
	if err := request.ctx.Err(); err != nil {
		request.response <- commandResponse{err: err}
		return
	}

	candidates := make([]Event, len(newEvents))
	for index, newEvent := range newEvents {
		candidates[index] = eventBeforeCommit(snapshot.SessionID, snapshot.HeadSeq+uint64(index)+1, newEvent)
	}
	if _, err := a.schema.foldBatch(snapshot, candidates, BoundaryCandidateBatch, phaseCandidate); err != nil {
		request.response <- commandResponse{err: err}
		return
	}

	appendCtx, cancelAppend := context.WithTimeout(context.Background(), a.persistenceTimeout)
	committed, err := a.store.Append(appendCtx, snapshot.SessionID, snapshot.Epoch, snapshot.HeadSeq, newEvents)
	cancelAppend()
	if err != nil {
		if errors.Is(err, ErrWriterFenced) {
			a.fail(err)
			a.Shutdown()
			request.response <- commandResponse{err: err}
			return
		}
		if recovered, healthy := a.recoverAfterCommit(snapshot, newEvents, err); healthy {
			request.response <- commandResponse{events: recovered, err: err}
		} else {
			request.response <- commandResponse{err: errors.Join(ErrActorUnhealthy, err)}
		}
		return
	}
	if err := validateCommittedBatch(snapshot, newEvents, committed); err != nil {
		if recovered, healthy := a.recoverAfterCommit(snapshot, newEvents, err); healthy {
			request.response <- commandResponse{events: recovered, err: err}
		} else {
			request.response <- commandResponse{err: errors.Join(ErrActorUnhealthy, err)}
		}
		return
	}
	committedState, err := a.schema.foldBatch(snapshot, committed, BoundaryCommittedBatch, phaseCommitted)
	if err != nil {
		if recovered, healthy := a.recoverAfterCommit(snapshot, newEvents, err); healthy {
			request.response <- commandResponse{events: recovered, err: err}
		} else {
			request.response <- commandResponse{err: errors.Join(ErrActorUnhealthy, err)}
		}
		return
	}

	a.stateMu.Lock()
	a.state = committedState
	a.stateMu.Unlock()
	a.publishCommitted(committed)
	request.response <- commandResponse{events: cloneEvents(committed)}
}

func eventBeforeCommit(sessionID string, seq uint64, newEvent NewEvent) Event {
	return Event{
		SchemaVersion:    newEvent.SchemaVersion,
		EventType:        newEvent.EventType,
		EventID:          newEvent.EventID,
		SessionID:        sessionID,
		Seq:              seq,
		OccurredAt:       newEvent.OccurredAt,
		ReplayPolicy:     newEvent.ReplayPolicy,
		Data:             cloneRawMessage(newEvent.Data),
		TurnID:           newEvent.TurnID,
		StepID:           newEvent.StepID,
		CallID:           newEvent.CallID,
		Trace:            newEvent.Trace,
		CausationEventID: newEvent.CausationEventID,
		SourceEventSeqs:  cloneEventSeqs(newEvent.SourceEventSeqs),
		SurfaceOp:        cloneSurfaceOp(newEvent.SurfaceOp),
		Extensions:       cloneRawMessage(newEvent.Extensions),
	}
}

func validateCommittedBatch(snapshot Snapshot, requested []NewEvent, committed []Event) error {
	if len(committed) != len(requested) {
		return fmt.Errorf("%w: append returned %d events, want %d", ErrConflict, len(committed), len(requested))
	}
	if err := ValidateSequence(committed, snapshot.HeadSeq); err != nil {
		return err
	}
	for index, event := range committed {
		input := requested[index]
		if event.SessionID != snapshot.SessionID ||
			event.EventID != input.EventID ||
			event.EventType != input.EventType ||
			event.SchemaVersion != input.SchemaVersion ||
			event.ReplayPolicy != input.ReplayPolicy ||
			event.TurnID != input.TurnID ||
			event.StepID != input.StepID ||
			event.CallID != input.CallID ||
			event.Trace != input.Trace ||
			event.CausationEventID != input.CausationEventID ||
			!jsonValuesEqual(event.Data, input.Data) ||
			!jsonValuesEqual(event.Extensions, input.Extensions) ||
			!slices.Equal(event.SourceEventSeqs, input.SourceEventSeqs) ||
			!surfaceOpsEqual(event.SurfaceOp, input.SurfaceOp) ||
			(!input.OccurredAt.IsZero() && !timestampsEquivalent(event.OccurredAt, input.OccurredAt)) {
			return fmt.Errorf("%w: append returned a different event at index %d", ErrConflict, index)
		}
	}
	return nil
}

func surfaceOpsEqual(left, right *SurfaceOp) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func timestampsEquivalent(left, right time.Time) bool {
	return left.UTC().Truncate(time.Microsecond).Equal(right.UTC().Truncate(time.Microsecond))
}

func jsonValuesEqual(left, right []byte) bool {
	if bytes.Equal(left, right) {
		return true
	}
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	decode := func(value []byte) (any, error) {
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		var decoded any
		if err := decoder.Decode(&decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	}
	leftValue, leftErr := decode(left)
	rightValue, rightErr := decode(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

func (a *Actor) recoverAfterCommit(previous Snapshot, requested []NewEvent, cause error) ([]Event, bool) {
	recoveryCtx, cancelRecovery := context.WithTimeout(context.Background(), a.persistenceTimeout)
	defer cancelRecovery()
	head, err := a.store.Head(recoveryCtx, previous.SessionID)
	if err == nil {
		var recovered Snapshot
		recovered, err = Replay(recoveryCtx, a.store, previous.SessionID, previous.Epoch, head, ReplayOptions{Schema: a.schema})
		if err == nil {
			_, err = a.store.Append(recoveryCtx, previous.SessionID, previous.Epoch, head, nil)
		}
		if err == nil {
			var recoveredEvents []Event
			recoveredEvents, err = loadCommittedRange(recoveryCtx, a.store, previous.SessionID, previous.HeadSeq, head)
			if err == nil && len(recoveredEvents) != 0 {
				err = validateCommittedBatch(previous, requested, recoveredEvents)
			}
			if err == nil {
				a.stateMu.Lock()
				a.state = recovered
				a.stateMu.Unlock()
				a.publishCommitted(recoveredEvents)
				return cloneEvents(recoveredEvents), true
			}
		}
	}
	a.fail(errors.Join(cause, err))
	a.Shutdown()
	return nil, false
}

func loadCommittedRange(ctx context.Context, store EventStore, sessionID string, afterSeq, head uint64) ([]Event, error) {
	const pageSize = 256
	var events []Event
	for afterSeq < head {
		page, err := store.Load(ctx, sessionID, afterSeq, pageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("%w: expected recovery head %d, reached %d", ErrReplayHeadMismatch, head, afterSeq)
		}
		for _, event := range page {
			if event.Seq > head {
				break
			}
			events = append(events, cloneEvent(event))
			afterSeq = event.Seq
		}
	}
	return events, nil
}

func (a *Actor) publishCommitted(events []Event) {
	if a.publisher == nil || len(events) == 0 {
		return
	}
	for _, observerErr := range a.publisher.PublishCommitted(context.Background(), cloneEvents(events)) {
		if observerErr != nil && a.onObserverError != nil {
			a.onObserverError(observerErr)
		}
	}
}

func (a *Actor) validateNewEvents(events []NewEvent) error {
	for _, event := range events {
		if event.EventID == "" || event.EventType == "" {
			return ErrInvalidCommand
		}
		if event.ReplayPolicy != ReplayRequired && event.ReplayPolicy != ReplayIgnorable {
			return ErrInvalidCommand
		}
		if IsSurfaceEligibleType(event.EventType) && event.SchemaVersion.Minor < SurfaceSchemaMinor {
			return fmt.Errorf("%w: surface event %s requires schema minor %d or newer", ErrUnsupportedSchema, event.EventType, SurfaceSchemaMinor)
		}
	}
	return nil
}

func (a *Actor) fail(err error) {
	a.stateMu.Lock()
	a.fatal = err
	a.stateMu.Unlock()
}

func (a *Actor) Shutdown() {
	a.admissionMu.Lock()
	a.accepting = false
	a.admissionMu.Unlock()
	a.signalStop()
}

func (a *Actor) signalStop() {
	a.disposeOnce.Do(func() {
		close(a.stop)
	})
}

func (a *Actor) Wait(ctx context.Context) error {
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Actor) Dispose(ctx context.Context) error {
	a.Shutdown()
	return a.Wait(ctx)
}
