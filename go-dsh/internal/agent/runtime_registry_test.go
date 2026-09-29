package agent_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
)

type fakeDriver struct {
	startedOnce sync.Once
	started     chan struct{}
	cancelled   chan struct{}
	cleanup     <-chan struct{}

	cancelMu    sync.Mutex
	cancelCause agent.CancelCause
	cancelSet   bool
}

func newFakeDriver() *fakeDriver {
	return &fakeDriver{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (driver *fakeDriver) Run(ctx context.Context, _ *agent.WakeLatch) error {
	driver.startedOnce.Do(func() { close(driver.started) })
	<-ctx.Done()
	select {
	case <-driver.cancelled:
	default:
		close(driver.cancelled)
	}
	if driver.cleanup != nil {
		<-driver.cleanup
	}
	return ctx.Err()
}

func (driver *fakeDriver) Cancel(cause agent.CancelCause) bool {
	driver.cancelMu.Lock()
	defer driver.cancelMu.Unlock()
	if driver.cancelSet {
		return false
	}
	driver.cancelSet = true
	driver.cancelCause = cause
	return true
}

type blockingCancelDriver struct {
	*fakeDriver
	firstEntered chan struct{}
	releaseFirst chan struct{}
	calls        atomic.Int32
}

func (driver *blockingCancelDriver) Cancel(cause agent.CancelCause) bool {
	if driver.calls.Add(1) == 1 {
		close(driver.firstEntered)
		<-driver.releaseFirst
	}
	return driver.fakeDriver.Cancel(cause)
}

func (driver *fakeDriver) Status(context.Context) (agent.Status, error) {
	return agent.Status{State: agent.StateRunning}, nil
}

type repeatableCancelDriver struct {
	*fakeDriver
	cancelCalls atomic.Int32
}

func (driver *repeatableCancelDriver) Cancel(agent.CancelCause) bool {
	driver.cancelCalls.Add(1)
	return true
}

type statusDriver struct {
	*fakeDriver
	status agent.Status
}

func (driver *statusDriver) Status(context.Context) (agent.Status, error) {
	return driver.status, nil
}

func (driver *fakeDriver) cause() (agent.CancelCause, bool) {
	driver.cancelMu.Lock()
	defer driver.cancelMu.Unlock()
	return driver.cancelCause, driver.cancelSet
}

func TestRuntimeRegistryCreatesOneDriverForConcurrentWakeRequests(t *testing.T) {
	driver := newFakeDriver()
	var createCount atomic.Int32
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		createCount.Add(1)
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	const callers = 64
	start := make(chan struct{})
	errorsByCaller := make(chan error, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			errorsByCaller <- registry.RequestWake("session-1")
		}()
	}
	close(start)
	callersDone.Wait()
	close(errorsByCaller)
	for err := range errorsByCaller {
		if err != nil {
			t.Fatalf("RequestWake() error = %v", err)
		}
	}

	waitClosed(t, driver.started, "Driver start")
	if got := createCount.Load(); got != 1 {
		t.Fatalf("Driver create count = %d, want 1", got)
	}
	status, err := registry.Status(context.Background(), "session-1")
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.Phase != agent.DriverRunning || !status.WakeRequested {
		t.Fatalf("Status() = %+v, want running with wake requested", status)
	}
}

func TestWakeLatchDoesNotLoseRequestRacingWithAcknowledge(t *testing.T) {
	latch := agent.NewWakeLatch()
	first := latch.Request()
	<-latch.Signal()
	second := latch.Request()
	latch.Acknowledge(first)
	if !latch.Requested() {
		t.Fatal("wake request racing with acknowledge was lost")
	}
	latch.Acknowledge(second)
	if latch.Requested() {
		t.Fatal("wake latch remained requested after latest sequence was acknowledged")
	}
}

func TestRuntimeRegistryRetainsWakeAndRetriesDriverCreation(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	driver := newFakeDriver()
	var createCount atomic.Int32
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		if createCount.Add(1) == 1 {
			return nil, errors.New("actor load failed")
		}
		return driver, nil
	}), agent.RuntimeRegistryOptions{Clock: clock})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-retry"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitFor(t, func() bool {
		status, err := registry.Status(context.Background(), "session-retry")
		return err == nil && status.LastError != nil && clock.timerCount() == 1
	}, "first start failure")

	status, err := registry.Status(context.Background(), "session-retry")
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.State != agent.StateFailed || status.LastError.Attempt != 1 || !status.WakeRequested {
		t.Fatalf("Status() after failure = %+v", status)
	}

	clock.Advance(500 * time.Millisecond)
	waitClosed(t, driver.started, "Driver retry start")
	if got := createCount.Load(); got != 2 {
		t.Fatalf("Driver create count = %d, want 2", got)
	}
	status, err = registry.Status(context.Background(), "session-retry")
	if err != nil {
		t.Fatalf("Status() after retry error = %v", err)
	}
	if status.LastError != nil || !status.WakeRequested || status.Phase != agent.DriverRunning {
		t.Fatalf("Status() after retry = %+v", status)
	}
}

func TestRuntimeRegistryPreservesDriverPhaseAndActivityStatus(t *testing.T) {
	baseDriver := newFakeDriver()
	driver := &statusDriver{
		fakeDriver: baseDriver,
		status: agent.Status{
			State:         agent.StateRunning,
			Phase:         agent.DriverMaintenance,
			ActiveTurnID:  "turn-1",
			ActiveStepID:  "step-2",
			StepIndex:     2,
			ActiveAttempt: 3,
			Cancellation:  &agent.CancelCause{Source: agent.CancelSourcePolicy, Reason: "review"},
		},
	}
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-status"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, baseDriver.started, "Driver start")
	status, err := registry.Status(context.Background(), "session-status")
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.Phase != agent.DriverMaintenance || status.ActiveTurnID != "turn-1" || status.ActiveStepID != "step-2" || status.StepIndex != 2 || status.ActiveAttempt != 3 {
		t.Fatalf("Status() = %+v", status)
	}
	if status.Cancellation == nil || status.Cancellation.Reason != "review" {
		t.Fatalf("Status().Cancellation = %+v", status.Cancellation)
	}
}

func TestRuntimeRegistryCancelActiveInstallsFirstCause(t *testing.T) {
	driver := newFakeDriver()
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-cancel"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, driver.started, "Driver start")
	first := agent.CancelCause{Source: agent.CancelSourceUser, Reason: "stop"}
	second := agent.CancelCause{Source: agent.CancelSourcePolicy, Reason: "policy"}
	if !registry.CancelActive("session-cancel", first) {
		t.Fatal("first CancelActive() = false, want true")
	}
	if registry.CancelActive("session-cancel", second) {
		t.Fatal("second CancelActive() = true, want false")
	}
	if got, ok := driver.cause(); !ok || got != first {
		t.Fatalf("installed cancel cause = %+v, %v, want %+v, true", got, ok, first)
	}
}

func TestRuntimeRegistryRemoveWaitsForDriverExit(t *testing.T) {
	cleanup := make(chan struct{})
	driver := newFakeDriver()
	driver.cleanup = cleanup
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-remove"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, driver.started, "Driver start")

	removeDone := make(chan error, 1)
	cause := agent.CancelCause{Source: agent.CancelSourceSessionDispose, Reason: "session-removed"}
	go func() {
		removeDone <- registry.Remove(context.Background(), "session-remove", cause)
	}()
	waitClosed(t, driver.cancelled, "Driver cancellation")
	select {
	case err := <-removeDone:
		t.Fatalf("Remove() returned before Driver cleanup: %v", err)
	default:
	}
	if err := registry.RequestWake("session-remove"); !errors.Is(err, agent.ErrRuntimeRemoving) {
		t.Fatalf("RequestWake() during removal error = %v, want %v", err, agent.ErrRuntimeRemoving)
	}
	close(cleanup)
	if err := waitError(t, removeDone, "Remove"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	status, err := registry.Status(context.Background(), "session-remove")
	if err != nil {
		t.Fatalf("Status() after Remove error = %v", err)
	}
	if status.State != agent.StateIdle || status.Phase != agent.DriverIdle {
		t.Fatalf("Status() after Remove = %+v", status)
	}
}

func TestRuntimeRegistryDoesNotRetainCancelCauseAcrossActivities(t *testing.T) {
	baseDriver := newFakeDriver()
	driver := &repeatableCancelDriver{fakeDriver: baseDriver}
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-repeat-cancel"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, baseDriver.started, "Driver start")
	if !registry.CancelActive("session-repeat-cancel", agent.CancelCause{Source: agent.CancelSourceUser, Reason: "turn-1"}) {
		t.Fatal("first CancelActive() = false, want true")
	}
	if !registry.CancelActive("session-repeat-cancel", agent.CancelCause{Source: agent.CancelSourceUser, Reason: "turn-2"}) {
		t.Fatal("later CancelActive() = false, want true")
	}
	if got := driver.cancelCalls.Load(); got != 2 {
		t.Fatalf("Driver.Cancel calls = %d, want 2", got)
	}
}

func TestRuntimeRegistryCommittedCancelCauseWinsConcurrentRemove(t *testing.T) {
	baseDriver := newFakeDriver()
	driver := &blockingCancelDriver{
		fakeDriver:   baseDriver,
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})
	defer registry.Dispose(context.Background())

	if err := registry.RequestWake("session-cancel-remove"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, baseDriver.started, "Driver start")
	committedCause := agent.CancelCause{Source: agent.CancelSourceUser, Reason: "committed-user-cancel"}
	cancelDone := make(chan bool, 1)
	go func() {
		cancelDone <- registry.CancelActive("session-cancel-remove", committedCause)
	}()
	waitClosed(t, driver.firstEntered, "first Driver.Cancel")

	removeDone := make(chan error, 1)
	go func() {
		removeDone <- registry.Remove(context.Background(), "session-cancel-remove", agent.CancelCause{
			Source: agent.CancelSourceSessionDispose,
			Reason: "session-removed",
		})
	}()
	waitFor(t, func() bool { return driver.calls.Load() >= 2 }, "Remove cancellation attempt")
	close(driver.releaseFirst)
	if !<-cancelDone {
		t.Fatal("CancelActive() = false, want true")
	}
	if err := waitError(t, removeDone, "Remove"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if got, ok := baseDriver.cause(); !ok || got != committedCause {
		t.Fatalf("cancel cause = %+v, %v, want %+v, true", got, ok, committedCause)
	}
}

func TestRuntimeRegistryRemoveCauseWinsConcurrentDispose(t *testing.T) {
	cleanup := make(chan struct{})
	driver := newFakeDriver()
	driver.cleanup = cleanup
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})

	if err := registry.RequestWake("session-remove-dispose"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, driver.started, "Driver start")
	removeCause := agent.CancelCause{Source: agent.CancelSourceSessionDispose, Reason: "session-removed"}
	removeDone := make(chan error, 1)
	go func() {
		removeDone <- registry.Remove(context.Background(), "session-remove-dispose", removeCause)
	}()
	waitFor(t, func() bool {
		cause, set := driver.cause()
		return set && cause == removeCause
	}, "Remove cancel cause")

	disposeDone := make(chan error, 1)
	go func() {
		disposeDone <- registry.Dispose(context.Background())
	}()
	close(cleanup)
	if err := waitError(t, removeDone, "Remove"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if err := waitError(t, disposeDone, "Dispose"); err != nil {
		t.Fatalf("Dispose() error = %v", err)
	}
	if got, ok := driver.cause(); !ok || got != removeCause {
		t.Fatalf("cancel cause = %+v, %v, want %+v, true", got, ok, removeCause)
	}
}

func TestRuntimeRegistryDisposeClosesAdmissionAndWaitsForDrivers(t *testing.T) {
	cleanup := make(chan struct{})
	driver := newFakeDriver()
	driver.cleanup = cleanup
	registry := agent.NewRuntimeRegistry(agent.DriverFactoryFunc(func(context.Context, string) (agent.Driver, error) {
		return driver, nil
	}), agent.RuntimeRegistryOptions{})

	if err := registry.RequestWake("session-dispose"); err != nil {
		t.Fatalf("RequestWake() error = %v", err)
	}
	waitClosed(t, driver.started, "Driver start")

	disposeDone := make(chan error, 1)
	go func() {
		disposeDone <- registry.Dispose(context.Background())
	}()
	waitClosed(t, driver.cancelled, "Driver cancellation")
	if err := registry.RequestWake("new-session"); !errors.Is(err, agent.ErrRuntimeRegistryClosed) {
		t.Fatalf("RequestWake() after Dispose error = %v, want %v", err, agent.ErrRuntimeRegistryClosed)
	}
	select {
	case err := <-disposeDone:
		t.Fatalf("Dispose() returned before Driver cleanup: %v", err)
	default:
	}
	close(cleanup)
	if err := waitError(t, disposeDone, "Dispose"); err != nil {
		t.Fatalf("Dispose() error = %v", err)
	}
	if got, ok := driver.cause(); !ok || got != agent.LifecycleDisposedCause {
		t.Fatalf("dispose cause = %+v, %v, want %+v, true", got, ok, agent.LifecycleDisposedCause)
	}
	if err := registry.Dispose(context.Background()); err != nil {
		t.Fatalf("second Dispose() error = %v", err)
	}
}

func waitClosed(t *testing.T, channel <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitError(t *testing.T, channel <-chan error, description string) error {
	t.Helper()
	select {
	case err := <-channel:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
		return nil
	}
}

func waitFor(t *testing.T, predicate func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*manualTimer
}

type manualTimer struct {
	mu      sync.Mutex
	channel chan time.Time
	due     time.Time
	stopped bool
	fired   bool
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{now: now}
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) NewTimer(delay time.Duration) agent.Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{
		channel: make(chan time.Time, 1),
		due:     clock.now.Add(delay),
	}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	timers := append([]*manualTimer(nil), clock.timers...)
	clock.mu.Unlock()
	for _, timer := range timers {
		timer.fire(now)
	}
}

func (clock *manualClock) timerCount() int {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return len(clock.timers)
}

func (timer *manualTimer) C() <-chan time.Time {
	return timer.channel
}

func (timer *manualTimer) Stop() bool {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if timer.stopped || timer.fired {
		return false
	}
	timer.stopped = true
	return true
}

func (timer *manualTimer) fire(now time.Time) {
	timer.mu.Lock()
	defer timer.mu.Unlock()
	if timer.stopped || timer.fired || now.Before(timer.due) {
		return
	}
	timer.fired = true
	timer.channel <- now
}
