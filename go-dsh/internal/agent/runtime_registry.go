package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRuntimeRegistryClosed = errors.New("agent runtime registry is closed")
	ErrRuntimeRemoving       = errors.New("agent runtime is being removed")
	ErrInvalidSessionID      = errors.New("session id is required")
)

type CancelSource string

const (
	CancelSourceUser             CancelSource = "user"
	CancelSourceClientDisconnect CancelSource = "client-disconnect"
	CancelSourceDeadline         CancelSource = "deadline"
	CancelSourceParent           CancelSource = "parent"
	CancelSourcePolicy           CancelSource = "policy"
	CancelSourceSessionDispose   CancelSource = "session-dispose"
	CancelSourceServerShutdown   CancelSource = "server-shutdown"
	CancelSourceToolProvider     CancelSource = "tool-provider"
	CancelSourceModelProvider    CancelSource = "model-provider"
	CancelSourceSystem           CancelSource = "system"
)

type CancelCause struct {
	Source CancelSource
	Reason string
}

var LifecycleDisposedCause = CancelCause{
	Source: CancelSourceServerShutdown,
	Reason: "lifecycle-disposed",
}

type DriverPhase string

const (
	DriverIdle        DriverPhase = "idle"
	DriverMaintenance DriverPhase = "maintenance"
	DriverRunning     DriverPhase = "running"
	DriverDisposed    DriverPhase = "disposed"
)

type StartError struct {
	Code     string
	Message  string
	Attempt  uint32
	FailedAt time.Time
	RetryAt  time.Time
}

type Driver interface {
	Run(context.Context, *WakeLatch) error
	Cancel(CancelCause) bool
	Status(context.Context) (Status, error)
}

type DriverFactory interface {
	Create(context.Context, string) (Driver, error)
}

type DriverFactoryFunc func(context.Context, string) (Driver, error)

func (f DriverFactoryFunc) Create(ctx context.Context, sessionID string) (Driver, error) {
	return f(ctx, sessionID)
}

type RuntimeRegistry interface {
	RequestWake(sessionID string) error
	CancelActive(sessionID string, cause CancelCause) bool
	Status(ctx context.Context, sessionID string) (Status, error)
	Remove(ctx context.Context, sessionID string, cause CancelCause) error
	Dispose(ctx context.Context) error
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTimer(delay time.Duration) Timer {
	return realTimer{Timer: time.NewTimer(delay)}
}

type realTimer struct {
	*time.Timer
}

func (timer realTimer) C() <-chan time.Time {
	return timer.Timer.C
}

type RuntimeRegistryOptions struct {
	Clock          Clock
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// WakeLatch coalesces wake notifications without losing a request that races
// with a Driver acknowledging previously observed work.
type WakeLatch struct {
	signal       chan struct{}
	requestedSeq atomic.Uint64
	acknowledged atomic.Uint64
}

func NewWakeLatch() *WakeLatch {
	return &WakeLatch{signal: make(chan struct{}, 1)}
}

func (latch *WakeLatch) Request() uint64 {
	requested := latch.requestedSeq.Add(1)
	select {
	case latch.signal <- struct{}{}:
	default:
	}
	return requested
}

func (latch *WakeLatch) Signal() <-chan struct{} {
	return latch.signal
}

func (latch *WakeLatch) Snapshot() uint64 {
	return latch.requestedSeq.Load()
}

// Acknowledge must be called only after the Driver has checked persistent
// state and confirmed that all work visible at observedSeq has been drained.
func (latch *WakeLatch) Acknowledge(observedSeq uint64) {
	for {
		current := latch.acknowledged.Load()
		requested := latch.requestedSeq.Load()
		if observedSeq > requested {
			observedSeq = requested
		}
		if observedSeq <= current || latch.acknowledged.CompareAndSwap(current, observedSeq) {
			return
		}
	}
}

func (latch *WakeLatch) Requested() bool {
	return latch.requestedSeq.Load() > latch.acknowledged.Load()
}

type runtimeEntry struct {
	sessionID string
	latch     *WakeLatch
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}

	driver                 Driver
	phase                  DriverPhase
	stopping               bool
	stopCause              CancelCause
	pendingCommittedCancel CancelCause
	hasPendingCancel       bool
	lastStartError         *StartError
	startAttempts          uint32
	err                    error
}

type Registry struct {
	mu      sync.Mutex
	factory DriverFactory
	options RuntimeRegistryOptions
	ctx     context.Context
	cancel  context.CancelFunc
	entries map[string]*runtimeEntry

	closed      bool
	disposeDone chan struct{}
	disposeErr  error
}

func NewRuntimeRegistry(factory DriverFactory, options RuntimeRegistryOptions) *Registry {
	if factory == nil {
		panic("agent DriverFactory is required")
	}
	if options.Clock == nil {
		options.Clock = realClock{}
	}
	const minimumInitialBackoff = 500 * time.Millisecond
	const maximumBackoff = 10 * time.Second
	if options.InitialBackoff < minimumInitialBackoff {
		options.InitialBackoff = minimumInitialBackoff
	}
	if options.InitialBackoff > maximumBackoff {
		options.InitialBackoff = maximumBackoff
	}
	if options.MaxBackoff <= 0 || options.MaxBackoff > maximumBackoff {
		options.MaxBackoff = maximumBackoff
	}
	if options.MaxBackoff < options.InitialBackoff {
		options.MaxBackoff = options.InitialBackoff
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Registry{
		factory: factory,
		options: options,
		ctx:     ctx,
		cancel:  cancel,
		entries: make(map[string]*runtimeEntry),
	}
}

func (registry *Registry) RequestWake(sessionID string) error {
	if sessionID == "" {
		return ErrInvalidSessionID
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return ErrRuntimeRegistryClosed
	}
	entry := registry.entries[sessionID]
	if entry != nil {
		if entry.stopping {
			return ErrRuntimeRemoving
		}
		entry.latch.Request()
		return nil
	}

	entryCtx, cancel := context.WithCancel(registry.ctx)
	entry = &runtimeEntry{
		sessionID: sessionID,
		latch:     NewWakeLatch(),
		ctx:       entryCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
		phase:     DriverIdle,
	}
	entry.latch.Request()
	registry.entries[sessionID] = entry
	go registry.supervise(entry)
	return nil
}

func (registry *Registry) supervise(entry *runtimeEntry) {
	defer registry.finishEntry(entry)
	backoff := registry.options.InitialBackoff

	for {
		if err := entry.ctx.Err(); err != nil {
			return
		}
		if !entry.latch.Requested() {
			select {
			case <-entry.ctx.Done():
				return
			case <-entry.latch.Signal():
				continue
			}
		}

		driver, err := registry.createDriver(entry.ctx, entry.sessionID)
		if err != nil {
			registry.recordStartFailure(entry, err, backoff)
			timer := registry.options.Clock.NewTimer(backoff)
			select {
			case <-entry.ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			backoff = nextBackoff(backoff, registry.options.MaxBackoff)
			continue
		}
		if driver == nil {
			err = errors.New("agent DriverFactory returned a nil Driver")
			registry.recordStartFailure(entry, err, backoff)
			timer := registry.options.Clock.NewTimer(backoff)
			select {
			case <-entry.ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			backoff = nextBackoff(backoff, registry.options.MaxBackoff)
			continue
		}

		registry.mu.Lock()
		entry.driver = driver
		entry.phase = DriverRunning
		entry.lastStartError = nil
		entry.startAttempts = 0
		stopping := entry.stopping
		stopCause := entry.stopCause
		registry.mu.Unlock()

		if stopping {
			driver.Cancel(stopCause)
		}
		runErr := registry.runDriver(entry.ctx, driver, entry.latch)

		registry.mu.Lock()
		if entry.driver == driver {
			entry.driver = nil
			if !entry.stopping {
				entry.phase = DriverIdle
			}
		}
		if runErr != nil && entry.ctx.Err() == nil {
			entry.err = errors.Join(entry.err, runErr)
		}
		registry.mu.Unlock()

		if entry.ctx.Err() != nil {
			return
		}
		if runErr != nil {
			registry.recordStartFailure(entry, runErr, backoff)
			timer := registry.options.Clock.NewTimer(backoff)
			select {
			case <-entry.ctx.Done():
				timer.Stop()
				return
			case <-timer.C():
			}
			backoff = nextBackoff(backoff, registry.options.MaxBackoff)
			continue
		}
		backoff = registry.options.InitialBackoff
	}
}

func (registry *Registry) createDriver(ctx context.Context, sessionID string) (driver Driver, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("create agent Driver panic: %v", recovered)
		}
	}()
	return registry.factory.Create(ctx, sessionID)
}

func (registry *Registry) runDriver(ctx context.Context, driver Driver, latch *WakeLatch) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("run agent Driver panic: %v", recovered)
		}
	}()
	return driver.Run(ctx, latch)
}

func (registry *Registry) recordStartFailure(entry *runtimeEntry, startErr error, retryDelay time.Duration) {
	now := registry.options.Clock.Now()
	registry.mu.Lock()
	entry.startAttempts++
	entry.lastStartError = &StartError{
		Code:     "DRIVER_START_FAILED",
		Message:  startErr.Error(),
		Attempt:  entry.startAttempts,
		FailedAt: now,
		RetryAt:  now.Add(retryDelay),
	}
	registry.mu.Unlock()
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func (registry *Registry) finishEntry(entry *runtimeEntry) {
	registry.mu.Lock()
	entry.driver = nil
	entry.phase = DriverDisposed
	if registry.entries[entry.sessionID] == entry {
		delete(registry.entries, entry.sessionID)
	}
	close(entry.done)
	registry.mu.Unlock()
}

func (registry *Registry) CancelActive(sessionID string, cause CancelCause) bool {
	registry.mu.Lock()
	entry := registry.entries[sessionID]
	if entry == nil || entry.driver == nil || entry.stopping || entry.hasPendingCancel {
		registry.mu.Unlock()
		return false
	}
	entry.pendingCommittedCancel = cause
	entry.hasPendingCancel = true
	driver := entry.driver
	registry.mu.Unlock()

	installed := driver.Cancel(cause)
	registry.mu.Lock()
	if entry.stopping && entry.stopCause == cause {
		installed = true
	}
	if !entry.stopping && entry.pendingCommittedCancel == cause {
		entry.pendingCommittedCancel = CancelCause{}
		entry.hasPendingCancel = false
	}
	registry.mu.Unlock()
	return installed
}

func (registry *Registry) Status(ctx context.Context, sessionID string) (Status, error) {
	if sessionID == "" {
		return Status{}, ErrInvalidSessionID
	}
	registry.mu.Lock()
	entry := registry.entries[sessionID]
	if entry == nil {
		registry.mu.Unlock()
		return Status{SessionID: sessionID, State: StateIdle, Phase: DriverIdle}, nil
	}
	driver := entry.driver
	status := Status{
		SessionID:     sessionID,
		State:         StateIdle,
		Phase:         entry.phase,
		WakeRequested: entry.latch.Requested(),
		LastError:     cloneStartError(entry.lastStartError),
	}
	if entry.stopping {
		status.State = StateDisposed
		status.Phase = DriverDisposed
	} else if entry.lastStartError != nil {
		status.State = StateFailed
	}
	registry.mu.Unlock()

	if driver == nil {
		return status, nil
	}
	driverStatus, err := driver.Status(ctx)
	if err != nil {
		return status, err
	}
	driverStatus.SessionID = sessionID
	if driverStatus.Phase == "" {
		driverStatus.Phase = status.Phase
	}
	driverStatus.WakeRequested = status.WakeRequested
	driverStatus.LastError = status.LastError
	return driverStatus, nil
}

func cloneStartError(startErr *StartError) *StartError {
	if startErr == nil {
		return nil
	}
	cloned := *startErr
	return &cloned
}

func (registry *Registry) Remove(ctx context.Context, sessionID string, cause CancelCause) error {
	if sessionID == "" {
		return ErrInvalidSessionID
	}
	registry.mu.Lock()
	entry := registry.entries[sessionID]
	if entry == nil {
		registry.mu.Unlock()
		return nil
	}
	if !entry.stopping {
		entry.stopping = true
		entry.phase = DriverDisposed
		entry.stopCause = cause
		if entry.hasPendingCancel {
			entry.stopCause = entry.pendingCommittedCancel
		}
	}
	driver := entry.driver
	committedCause := entry.stopCause
	done := entry.done
	registry.mu.Unlock()

	if driver != nil {
		driver.Cancel(committedCause)
	}
	entry.cancel()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (registry *Registry) Dispose(ctx context.Context) error {
	type driverCancellation struct {
		driver Driver
		cause  CancelCause
	}

	registry.mu.Lock()
	if !registry.closed {
		registry.closed = true
		registry.disposeDone = make(chan struct{})
		entries := make([]*runtimeEntry, 0, len(registry.entries))
		cancellations := make([]driverCancellation, 0, len(registry.entries))
		for _, entry := range registry.entries {
			if !entry.stopping {
				entry.stopping = true
				entry.phase = DriverDisposed
				entry.stopCause = LifecycleDisposedCause
				if entry.hasPendingCancel {
					entry.stopCause = entry.pendingCommittedCancel
				}
			}
			entries = append(entries, entry)
			if entry.driver != nil {
				cancellations = append(cancellations, driverCancellation{driver: entry.driver, cause: entry.stopCause})
			}
		}
		done := registry.disposeDone
		registry.mu.Unlock()

		for _, cancellation := range cancellations {
			cancellation.driver.Cancel(cancellation.cause)
		}
		registry.cancel()
		for _, entry := range entries {
			entry.cancel()
		}
		go registry.finishDispose(entries, done)
	} else {
		registry.mu.Unlock()
	}

	registry.mu.Lock()
	done := registry.disposeDone
	registry.mu.Unlock()
	select {
	case <-done:
		registry.mu.Lock()
		err := registry.disposeErr
		registry.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (registry *Registry) finishDispose(entries []*runtimeEntry, done chan struct{}) {
	for _, entry := range entries {
		<-entry.done
	}

	var disposeErr error
	registry.mu.Lock()
	for _, entry := range entries {
		disposeErr = errors.Join(disposeErr, entry.err)
	}
	registry.disposeErr = disposeErr
	close(done)
	registry.mu.Unlock()
}
