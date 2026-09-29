package session

import (
	"errors"
	"fmt"
	"sort"
)

var ErrInvalidSessionSchema = errors.New("invalid session schema")

type ProjectionKey string

type ProjectionBoundary string

const (
	BoundaryCandidateBatch ProjectionBoundary = "candidate-batch"
	BoundaryCommittedBatch ProjectionBoundary = "committed-batch"
	BoundaryReplayEnd      ProjectionBoundary = "replay-end"

	CoreProjectionKey    ProjectionKey = "core"
	SurfaceProjectionKey ProjectionKey = "surface"
)

type ProjectionSpec struct {
	Key              ProjectionKey
	Order            int
	New              func() any
	Clone            func(any) any
	Apply            func(any, Event) error
	ValidateBoundary func(any, ProjectionBoundary) error
}

type EventDefinition struct {
	EventType           string
	ReplayPolicy        ReplayPolicy
	CandidateValidators []CandidateEventValidator
	CommittedValidators []EventValidator
}

type SchemaContribution struct {
	Name        string
	Events      []EventDefinition
	Projections []ProjectionSpec
}

type SessionSchema struct {
	supportedSchemaMajor uint32
	events               map[string]EventDefinition
	projections          []ProjectionSpec
}

func NewSessionSchema(supportedSchemaMajor uint32, contributions ...SchemaContribution) (*SessionSchema, error) {
	if supportedSchemaMajor == 0 {
		return nil, fmt.Errorf("%w: supported schema major must be positive", ErrInvalidSessionSchema)
	}
	schema := &SessionSchema{
		supportedSchemaMajor: supportedSchemaMajor,
		events:               make(map[string]EventDefinition),
	}
	contributionNames := make(map[string]struct{}, len(contributions))
	projectionKeys := make(map[ProjectionKey]struct{})
	for _, contribution := range contributions {
		if contribution.Name == "" {
			return nil, fmt.Errorf("%w: contribution name is required", ErrInvalidSessionSchema)
		}
		if _, exists := contributionNames[contribution.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate contribution %q", ErrInvalidSessionSchema, contribution.Name)
		}
		contributionNames[contribution.Name] = struct{}{}
		for _, definition := range contribution.Events {
			if definition.EventType == "" {
				return nil, fmt.Errorf("%w: empty event type in contribution %q", ErrInvalidSessionSchema, contribution.Name)
			}
			if definition.ReplayPolicy != ReplayRequired && definition.ReplayPolicy != ReplayIgnorable {
				return nil, fmt.Errorf("%w: event %q has invalid replay policy %q", ErrInvalidSessionSchema, definition.EventType, definition.ReplayPolicy)
			}
			if _, exists := schema.events[definition.EventType]; exists {
				return nil, fmt.Errorf("%w: duplicate event type %q", ErrInvalidSessionSchema, definition.EventType)
			}
			definition.CandidateValidators = append([]CandidateEventValidator(nil), definition.CandidateValidators...)
			definition.CommittedValidators = append([]EventValidator(nil), definition.CommittedValidators...)
			for _, validator := range definition.CandidateValidators {
				if validator == nil {
					return nil, fmt.Errorf("%w: event %q has nil candidate validator", ErrInvalidSessionSchema, definition.EventType)
				}
			}
			for _, validator := range definition.CommittedValidators {
				if validator == nil {
					return nil, fmt.Errorf("%w: event %q has nil committed validator", ErrInvalidSessionSchema, definition.EventType)
				}
			}
			schema.events[definition.EventType] = definition
		}
		for _, projection := range contribution.Projections {
			if projection.Key == "" || projection.New == nil || projection.Clone == nil || projection.Apply == nil || projection.ValidateBoundary == nil {
				return nil, fmt.Errorf("%w: projection in contribution %q is incomplete", ErrInvalidSessionSchema, contribution.Name)
			}
			if _, exists := projectionKeys[projection.Key]; exists {
				return nil, fmt.Errorf("%w: duplicate projection key %q", ErrInvalidSessionSchema, projection.Key)
			}
			projectionKeys[projection.Key] = struct{}{}
			schema.projections = append(schema.projections, projection)
		}
	}
	if len(schema.projections) == 0 {
		return nil, fmt.Errorf("%w: at least one projection is required", ErrInvalidSessionSchema)
	}
	sort.Slice(schema.projections, func(left, right int) bool {
		if schema.projections[left].Order == schema.projections[right].Order {
			return schema.projections[left].Key < schema.projections[right].Key
		}
		return schema.projections[left].Order < schema.projections[right].Order
	})
	return schema, nil
}

func (schema *SessionSchema) SupportedSchemaMajor() uint32 {
	if schema == nil {
		return 0
	}
	return schema.supportedSchemaMajor
}

func (schema *SessionSchema) KnownEventTypes() map[string]struct{} {
	known := make(map[string]struct{}, len(schema.events))
	for eventType := range schema.events {
		known[eventType] = struct{}{}
	}
	return known
}

func (schema *SessionSchema) newSnapshot(sessionID string, epoch uint64) Snapshot {
	projections := make(map[ProjectionKey]any, len(schema.projections))
	for _, projection := range schema.projections {
		projections[projection.Key] = projection.New()
	}
	return Snapshot{SessionID: sessionID, Epoch: epoch, schema: schema, projections: projections}
}

func (schema *SessionSchema) cloneSnapshot(snapshot Snapshot) Snapshot {
	cloned := snapshot
	cloned.schema = schema
	cloned.projections = make(map[ProjectionKey]any, len(schema.projections))
	for _, projection := range schema.projections {
		cloned.projections[projection.Key] = projection.Clone(snapshot.projections[projection.Key])
	}
	return cloned
}

type validationPhase uint8

const (
	phaseCandidate validationPhase = iota + 1
	phaseCommitted
)

func (schema *SessionSchema) foldBatch(snapshot Snapshot, events []Event, boundary ProjectionBoundary, phase validationPhase) (Snapshot, error) {
	folded := schema.cloneSnapshot(snapshot)
	for _, event := range events {
		definition, known, err := schema.validateEvent(event)
		if err != nil {
			return Snapshot{}, err
		}
		if known {
			switch phase {
			case phaseCandidate:
				for _, validator := range definition.CandidateValidators {
					if err := validator.ValidateCandidate(cloneEvent(event)); err != nil {
						return Snapshot{}, err
					}
				}
			case phaseCommitted:
				for _, validator := range definition.CommittedValidators {
					if err := validator.Validate(cloneEvent(event)); err != nil {
						return Snapshot{}, err
					}
				}
			}
		}
		projectionEvent := event
		if !known {
			projectionEvent.Data = nil
			projectionEvent.SourceEventSeqs = nil
			projectionEvent.SurfaceOp = nil
			projectionEvent.Extensions = nil
		}
		for _, projection := range schema.projections {
			if err := projection.Apply(folded.projections[projection.Key], cloneEvent(projectionEvent)); err != nil {
				return Snapshot{}, fmt.Errorf("projection %q: %w", projection.Key, err)
			}
		}
		folded.HeadSeq = event.Seq
	}
	if boundary != "" {
		if err := schema.validateBoundary(folded, boundary); err != nil {
			return Snapshot{}, err
		}
	}
	return folded, nil
}

func (schema *SessionSchema) validateEvent(event Event) (EventDefinition, bool, error) {
	if event.EventID == "" || event.EventType == "" || event.SessionID == "" || event.Seq == 0 {
		return EventDefinition{}, false, ErrInvalidEvent
	}
	if event.ReplayPolicy != ReplayRequired && event.ReplayPolicy != ReplayIgnorable {
		return EventDefinition{}, false, fmt.Errorf("%w: invalid replay policy %q", ErrInvalidEvent, event.ReplayPolicy)
	}
	definition, known := schema.events[event.EventType]
	if !known {
		if event.ReplayPolicy == ReplayRequired {
			return EventDefinition{}, false, fmt.Errorf("%w: %s at seq %d", ErrUnknownRequired, event.EventType, event.Seq)
		}
		return EventDefinition{}, false, nil
	}
	if event.SchemaVersion.Major != schema.supportedSchemaMajor {
		return EventDefinition{}, false, fmt.Errorf("%w: event %s at seq %d uses major %d", ErrUnsupportedSchema, event.EventType, event.Seq, event.SchemaVersion.Major)
	}
	if event.ReplayPolicy != definition.ReplayPolicy {
		return EventDefinition{}, false, fmt.Errorf("%w: event %s at seq %d uses replay policy %q, want %q", ErrInvalidEvent, event.EventType, event.Seq, event.ReplayPolicy, definition.ReplayPolicy)
	}
	return definition, true, nil
}

func (schema *SessionSchema) validateBoundary(snapshot Snapshot, boundary ProjectionBoundary) error {
	for _, projection := range schema.projections {
		validationState := projection.Clone(snapshot.projections[projection.Key])
		if err := projection.ValidateBoundary(validationState, boundary); err != nil {
			return fmt.Errorf("projection %q boundary %q: %w", projection.Key, boundary, err)
		}
	}
	return nil
}

func ProjectionAs[T any](snapshot Snapshot, key ProjectionKey) (T, bool) {
	value, ok := snapshot.projections[key]
	if !ok {
		var zero T
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}

type CoreProjection struct {
	Status  string
	TurnID  string
	StepID  string
	Created bool
}

func CoreSchemaContribution() SchemaContribution {
	required := []string{
		"session/created", "turn/start", "user/message", "step/start",
		"assistant/message", "model/error", "tool/call", "tool/result",
		"approval/requested", "approval/resolved", "step/end", "turn/end",
		"session/cancelled",
	}
	events := make([]EventDefinition, 0, len(required)+1)
	for _, eventType := range required {
		events = append(events, EventDefinition{EventType: eventType, ReplayPolicy: ReplayRequired})
	}
	events = append(events,
		EventDefinition{EventType: "assistant/chunk", ReplayPolicy: ReplayIgnorable},
		EventDefinition{EventType: "model/usage", ReplayPolicy: ReplayIgnorable},
		EventDefinition{EventType: EventSessionRepaired, ReplayPolicy: ReplayIgnorable},
	)
	return SchemaContribution{
		Name:   "core",
		Events: events,
		Projections: []ProjectionSpec{{
			Key:   CoreProjectionKey,
			Order: 100,
			New: func() any {
				return &CoreProjection{Status: StatusIdle}
			},
			Clone: func(state any) any {
				cloned := *state.(*CoreProjection)
				return &cloned
			},
			Apply: func(state any, event Event) error {
				projection := state.(*CoreProjection)
				switch event.EventType {
				case "session/created":
					if projection.Created || projection.TurnID != "" || projection.StepID != "" {
						return fmt.Errorf("%w: session/created is not the first core event", ErrInvalidEvent)
					}
					projection.Created = true
					projection.Status = StatusIdle
				case "turn/start":
					if event.TurnID == "" || projection.Status != StatusIdle || projection.TurnID != "" || projection.StepID != "" {
						return fmt.Errorf("%w: invalid turn/start transition", ErrInvalidEvent)
					}
					projection.Status = StatusRunning
					projection.TurnID = event.TurnID
				case "step/start":
					if event.StepID == "" || projection.Status != StatusRunning || projection.TurnID == "" || projection.StepID != "" || (event.TurnID != "" && event.TurnID != projection.TurnID) {
						return fmt.Errorf("%w: invalid step/start transition", ErrInvalidEvent)
					}
					projection.StepID = event.StepID
				case "step/end":
					if projection.StepID == "" || (event.StepID != "" && event.StepID != projection.StepID) || (event.TurnID != "" && event.TurnID != projection.TurnID) {
						return fmt.Errorf("%w: invalid step/end transition", ErrInvalidEvent)
					}
					projection.StepID = ""
				case "turn/end":
					if projection.TurnID == "" || projection.StepID != "" || (event.TurnID != "" && event.TurnID != projection.TurnID) {
						return fmt.Errorf("%w: invalid turn/end transition", ErrInvalidEvent)
					}
					projection.Status = StatusIdle
					projection.TurnID = ""
				case "session/cancelled":
					projection.Status = StatusCancelled
					projection.TurnID = ""
					projection.StepID = ""
				}
				return nil
			},
			ValidateBoundary: func(any, ProjectionBoundary) error { return nil },
		}},
	}
}

func SurfaceSchemaContribution() SchemaContribution {
	return SchemaContribution{
		Name: "surface",
		Projections: []ProjectionSpec{{
			Key:   SurfaceProjectionKey,
			Order: 200,
			New:   func() any { return new(Surface) },
			Clone: func(state any) any {
				cloned := state.(*Surface).Clone()
				return &cloned
			},
			Apply: func(state any, event Event) error {
				surfaceEvent, err := surfaceEventForReplay(event)
				if err != nil {
					return err
				}
				return state.(*Surface).Apply(surfaceEvent)
			},
			ValidateBoundary: func(any, ProjectionBoundary) error { return nil },
		}},
	}
}

func DefaultSessionSchema() *SessionSchema {
	schema, err := NewSessionSchema(1, CoreSchemaContribution(), SurfaceSchemaContribution())
	if err != nil {
		panic(err)
	}
	return schema
}
