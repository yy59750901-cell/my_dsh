package session

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrRegistryClosed = errors.New("session actor registry is closed")

type ActorLease struct {
	actor *Actor
	once  sync.Once
}

func (l *ActorLease) Actor() *Actor {
	return l.actor
}

func (l *ActorLease) Release() {
	if l == nil || l.actor == nil {
		return
	}
	l.once.Do(l.actor.releaseUse)
}

type actorLoad struct {
	done  chan struct{}
	actor *Actor
	err   error
}

type actorRemoval struct {
	done chan struct{}
	err  error
}

type Registry struct {
	mu          sync.Mutex
	store       EventStore
	options     ActorOptions
	actors      map[string]*Actor
	loading     map[string]*actorLoad
	removing    map[string]*actorRemoval
	loadCtx     context.Context
	cancelLoads context.CancelFunc
	closed      bool
	disposeDone chan struct{}
	disposeErr  error
}

func NewRegistry(store EventStore, options ActorOptions) *Registry {
	if options.Schema == nil {
		options.Schema = DefaultSessionSchema()
	}
	if options.Initializers == nil {
		options.Initializers = []ActorInitializer{CoreRepairInitializer{}}
	} else {
		options.Initializers = append([]ActorInitializer(nil), options.Initializers...)
	}
	loadCtx, cancelLoads := context.WithCancel(context.Background())
	return &Registry{
		store:       store,
		options:     options,
		actors:      make(map[string]*Actor),
		loading:     make(map[string]*actorLoad),
		removing:    make(map[string]*actorRemoval),
		loadCtx:     loadCtx,
		cancelLoads: cancelLoads,
	}
}

func (r *Registry) GetOrLoad(ctx context.Context, sessionID string) (*ActorLease, error) {
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, ErrRegistryClosed
		}
		if removal := r.removing[sessionID]; removal != nil {
			r.mu.Unlock()
			if err := waitActorRemoval(ctx, removal); err != nil {
				return nil, err
			}
			continue
		}
		if actor := r.actors[sessionID]; actor != nil {
			if actor.acquireUse() {
				r.mu.Unlock()
				return &ActorLease{actor: actor}, nil
			}
			removal := r.startRemovalLocked(sessionID, actor, false)
			r.mu.Unlock()
			if err := waitActorRemoval(ctx, removal); err != nil {
				return nil, err
			}
			continue
		}
		loading := r.loading[sessionID]
		if loading == nil {
			loading = &actorLoad{done: make(chan struct{})}
			r.loading[sessionID] = loading
			go r.load(sessionID, loading)
		}
		r.mu.Unlock()

		select {
		case <-loading.done:
			if loading.err != nil {
				return nil, loading.err
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (r *Registry) load(sessionID string, loading *actorLoad) {
	actor, err := LoadActor(r.loadCtx, r.store, sessionID, r.options)

	r.mu.Lock()
	closed := r.closed
	if err == nil && !closed {
		r.actors[sessionID] = actor
		delete(r.loading, sessionID)
		loading.actor = actor
		close(loading.done)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	if actor != nil {
		err = errors.Join(err, actor.Dispose(context.Background()))
	}
	if closed {
		err = errors.Join(ErrRegistryClosed, err)
	}
	r.mu.Lock()
	delete(r.loading, sessionID)
	loading.err = err
	close(loading.done)
	r.mu.Unlock()
}

func (r *Registry) startRemovalLocked(sessionID string, actor *Actor, alreadyShutdown bool) *actorRemoval {
	if removal := r.removing[sessionID]; removal != nil {
		return removal
	}
	removal := &actorRemoval{done: make(chan struct{})}
	r.removing[sessionID] = removal
	if !alreadyShutdown {
		actor.Shutdown()
	}
	go func() {
		err := actor.Wait(context.Background())
		r.mu.Lock()
		if r.actors[sessionID] == actor {
			delete(r.actors, sessionID)
		}
		delete(r.removing, sessionID)
		removal.err = err
		close(removal.done)
		r.mu.Unlock()
	}()
	return removal
}

func waitActorRemoval(ctx context.Context, removal *actorRemoval) error {
	select {
	case <-removal.done:
		return removal.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Registry) Remove(ctx context.Context, sessionID string) error {
	for {
		r.mu.Lock()
		if loading := r.loading[sessionID]; loading != nil {
			r.mu.Unlock()
			select {
			case <-loading.done:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if removal := r.removing[sessionID]; removal != nil {
			r.mu.Unlock()
			return waitActorRemoval(ctx, removal)
		}
		actor := r.actors[sessionID]
		if actor == nil {
			r.mu.Unlock()
			return nil
		}
		removal := r.startRemovalLocked(sessionID, actor, false)
		r.mu.Unlock()
		return waitActorRemoval(ctx, removal)
	}
}

func (r *Registry) EvictIdle(ctx context.Context, idleFor time.Duration, now time.Time) (int, error) {
	cutoff := now.Add(-idleFor)
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return 0, ErrRegistryClosed
	}
	removals := make([]*actorRemoval, 0)
	for sessionID, actor := range r.actors {
		if !actor.tryShutdownIfIdle(cutoff) {
			continue
		}
		removals = append(removals, r.startRemovalLocked(sessionID, actor, true))
	}
	r.mu.Unlock()

	var evictionErr error
	for _, removal := range removals {
		evictionErr = errors.Join(evictionErr, waitActorRemoval(ctx, removal))
	}
	return len(removals), evictionErr
}

func (r *Registry) Dispose(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		done := r.disposeDone
		r.mu.Unlock()
		select {
		case <-done:
			r.mu.Lock()
			err := r.disposeErr
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.closed = true
	r.cancelLoads()
	r.disposeDone = make(chan struct{})
	done := r.disposeDone
	loads := make([]*actorLoad, 0, len(r.loading))
	for _, loading := range r.loading {
		loads = append(loads, loading)
	}
	actors := make([]*Actor, 0, len(r.actors))
	for _, actor := range r.actors {
		actor.Shutdown()
		actors = append(actors, actor)
	}
	r.mu.Unlock()

	var disposeErr error
	for _, actor := range actors {
		disposeErr = errors.Join(disposeErr, actor.Wait(ctx))
	}
	for _, loading := range loads {
		select {
		case <-loading.done:
			if loading.err != nil && !errors.Is(loading.err, ErrRegistryClosed) && !errors.Is(loading.err, context.Canceled) {
				disposeErr = errors.Join(disposeErr, loading.err)
			}
		case <-ctx.Done():
			disposeErr = errors.Join(disposeErr, ctx.Err())
		}
	}

	r.mu.Lock()
	r.disposeErr = disposeErr
	close(done)
	r.mu.Unlock()
	return disposeErr
}
