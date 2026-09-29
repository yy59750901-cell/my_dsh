package agent

import (
	"context"
	"time"

	"github.com/yy59750901/go-dsh/internal/session"
)

type State string

const (
	StateIdle            State = "idle"
	StateRunning         State = "running"
	StateWaitingApproval State = "waiting_approval"
	StateCancelling      State = "cancelling"
	StateFailed          State = "failed"
	StateDisposed        State = "disposed"
)

type UserMessage struct {
	ID        string
	Text      string
	CreatedAt time.Time
}

type InjectedContext struct {
	Source string
	Text   string
}

type Receipt struct {
	RequestID      string
	SessionID      string
	CommandID      string
	AcceptedSeq    uint64
	Placement      string
	Duplicate      bool
	AcceptedAt     time.Time
	InputID        string
	Target         InboxTarget
	IdempotencyKey string
}

type Status struct {
	SessionID     string
	State         State
	ActiveTurnID  string
	ActiveStepID  string
	StepIndex     uint32
	ActiveAttempt uint32
	Queued        int
	Phase         DriverPhase
	WakeRequested bool
	Cancellation  *CancelCause
	LastError     *StartError
}

// SessionActorOptions exposes only the generic Actor settings that Agent
// callers may customize. Schema and initializer wiring stay owned by this
// package so claim-aware repair cannot be accidentally omitted.
type SessionActorOptions struct {
	MailboxSize        int
	PersistenceTimeout time.Duration
	Publisher          session.CommittedPublisher
	OnObserverError    func(error)
}

func NewSessionSchema() (*session.SessionSchema, error) {
	return session.NewSessionSchema(
		1,
		session.CoreSchemaContribution(),
		session.SurfaceSchemaContribution(),
		AgentSchemaContribution(),
		contextSchemaContribution(),
	)
}

func NewSessionActorRegistry(store session.EventStore, options SessionActorOptions) (*session.Registry, error) {
	actorOptions, err := newSessionActorOptions(options)
	if err != nil {
		return nil, err
	}
	return session.NewRegistry(store, actorOptions), nil
}

func LoadSessionActor(ctx context.Context, store session.EventStore, sessionID string, options SessionActorOptions) (*session.Actor, error) {
	actorOptions, err := newSessionActorOptions(options)
	if err != nil {
		return nil, err
	}
	return session.LoadActor(ctx, store, sessionID, actorOptions)
}

func newSessionActorOptions(options SessionActorOptions) (session.ActorOptions, error) {
	schema, err := NewSessionSchema()
	if err != nil {
		return session.ActorOptions{}, err
	}
	return session.ActorOptions{
		MailboxSize:        options.MailboxSize,
		PersistenceTimeout: options.PersistenceTimeout,
		Schema:             schema,
		Initializers:       AgentActorInitializers(),
		Publisher:          options.Publisher,
		OnObserverError:    options.OnObserverError,
	}, nil
}

type Agent interface {
	FollowUp(ctx context.Context, message UserMessage) (Receipt, error)
	Steer(ctx context.Context, message UserMessage) (Receipt, error)
	Cancel(ctx context.Context, reason string) error
	Inject(ctx context.Context, input InjectedContext) error
	Status() Status
	Dispose(ctx context.Context) error
}
