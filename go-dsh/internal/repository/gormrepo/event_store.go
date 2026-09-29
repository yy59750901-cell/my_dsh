package gormrepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yy59750901/go-dsh/internal/agent"
	"github.com/yy59750901/go-dsh/internal/session"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type EventStore struct {
	db *gorm.DB
}

func NewEventStore(db *gorm.DB) *EventStore {
	return &EventStore{db: db}
}

func (store *EventStore) ListResumableSessionIDs(ctx context.Context, cursor string, limit int) (agent.ResumableSessionPage, error) {
	if limit <= 0 {
		limit = 100
	}
	var models []SessionModel
	query := store.db.WithContext(ctx).
		Select("id").
		Where("archived_at IS NULL")
	if cursor != "" {
		query = query.Where("id > ?", cursor)
	}
	if err := query.Order("id ASC").Limit(limit).Find(&models).Error; err != nil {
		return agent.ResumableSessionPage{}, fmt.Errorf("list resumable Sessions: %w", err)
	}
	page := agent.ResumableSessionPage{SessionIDs: make([]string, 0, len(models))}
	for _, model := range models {
		page.SessionIDs = append(page.SessionIDs, model.ID)
	}
	if len(models) == limit {
		page.NextCursor = models[len(models)-1].ID
	}
	return page, nil
}

func (store *EventStore) Create(ctx context.Context, input session.NewSession) error {
	if input.ID == "" || input.TenantID == "" || input.WorkspaceID == "" {
		return fmt.Errorf("%w: id, tenant and workspace are required", session.ErrInvalidSession)
	}
	if (input.ParentID == "") != (input.ForkSeq == nil) {
		return fmt.Errorf("%w: parent_id and fork_seq must be set together", session.ErrInvalidSession)
	}
	if input.ParentID == input.ID {
		return fmt.Errorf("%w: session cannot fork from itself", session.ErrInvalidSession)
	}

	createdAt := input.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	model := SessionModel{
		ID:          input.ID,
		TenantID:    input.TenantID,
		WorkspaceID: input.WorkspaceID,
		ParentID:    stringPointer(input.ParentID),
		ForkSeq:     input.ForkSeq,
		Status:      "idle",
		CreatedAt:   createdAt,
		UpdatedAt:   createdAt,
	}
	if err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if input.ParentID != "" {
			var parent SessionModel
			if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&parent, "id = ?", input.ParentID).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return session.ErrNotFound
				}
				return err
			}
			if parent.TenantID != input.TenantID {
				return fmt.Errorf("%w: parent belongs to another tenant", session.ErrInvalidSession)
			}
			if *input.ForkSeq > parent.LastSeq {
				return fmt.Errorf("%w: fork_seq %d exceeds parent head %d", session.ErrInvalidSession, *input.ForkSeq, parent.LastSeq)
			}
		}
		return tx.Create(&model).Error
	}); err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

func (store *EventStore) ClaimWriter(ctx context.Context, sessionID string) (uint64, uint64, error) {
	var epoch uint64
	var head uint64
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&SessionModel{}).
			Where("id = ?", sessionID).
			UpdateColumn("actor_epoch", gorm.Expr("actor_epoch + 1"))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return session.ErrNotFound
		}
		var model SessionModel
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&model, "id = ?", sessionID).Error; err != nil {
			return err
		}
		epoch, head = model.ActorEpoch, model.LastSeq
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("claim session writer: %w", err)
	}
	return epoch, head, nil
}

func (store *EventStore) Append(ctx context.Context, sessionID string, epoch uint64, expectedSeq uint64, inputs []session.NewEvent) ([]session.Event, error) {
	if len(inputs) == 0 {
		var model SessionModel
		if err := store.db.WithContext(ctx).First(&model, "id = ?", sessionID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, session.ErrNotFound
		} else if err != nil {
			return nil, fmt.Errorf("validate empty append: %w", err)
		}
		if model.ActorEpoch != epoch {
			return nil, session.ErrWriterFenced
		}
		if model.LastSeq != expectedSeq {
			return nil, session.ErrConflict
		}
		return nil, nil
	}
	committedAt := time.Now().UTC().Truncate(time.Microsecond)
	models := make([]SessionEventModel, len(inputs))
	for index, input := range inputs {
		seq := expectedSeq + uint64(index) + 1
		occurredAt := input.OccurredAt
		if occurredAt.IsZero() {
			occurredAt = committedAt
		} else {
			occurredAt = occurredAt.UTC().Truncate(time.Microsecond)
		}
		event := session.Event{
			SchemaVersion:    input.SchemaVersion,
			EventType:        input.EventType,
			EventID:          input.EventID,
			SessionID:        sessionID,
			Seq:              seq,
			OccurredAt:       occurredAt,
			CommittedAt:      committedAt,
			ReplayPolicy:     input.ReplayPolicy,
			Data:             cloneBytes(input.Data),
			TurnID:           input.TurnID,
			StepID:           input.StepID,
			CallID:           input.CallID,
			Trace:            input.Trace,
			CausationEventID: input.CausationEventID,
			SourceEventSeqs:  cloneEventSeqs(input.SourceEventSeqs),
			SurfaceOp:        cloneSurfaceOp(input.SurfaceOp),
			Extensions:       cloneBytes(input.Extensions),
		}
		model, err := eventToModel(event)
		if err != nil {
			return nil, err
		}
		models[index] = model
	}

	var committedModels []SessionEventModel
	err := store.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&SessionModel{}).
			Where("id = ? AND actor_epoch = ? AND last_seq = ?", sessionID, epoch, expectedSeq).
			Updates(map[string]any{
				"last_seq":   expectedSeq + uint64(len(inputs)),
				"updated_at": committedAt,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			var model SessionModel
			if err := tx.First(&model, "id = ?", sessionID).Error; errors.Is(err, gorm.ErrRecordNotFound) {
				return session.ErrNotFound
			} else if err != nil {
				return err
			}
			if model.ActorEpoch != epoch {
				return session.ErrWriterFenced
			}
			return session.ErrConflict
		}
		if err := tx.Create(&models).Error; err != nil {
			return err
		}
		return tx.
			Where("session_id = ? AND seq > ? AND seq <= ?", sessionID, expectedSeq, expectedSeq+uint64(len(inputs))).
			Order("seq ASC").
			Find(&committedModels).Error
	})
	if err != nil {
		return nil, fmt.Errorf("append session events: %w", err)
	}
	if len(committedModels) != len(inputs) {
		return nil, fmt.Errorf("append session events: %w: read back %d events, want %d", session.ErrConflict, len(committedModels), len(inputs))
	}
	committed := make([]session.Event, 0, len(committedModels))
	for _, model := range committedModels {
		event, err := modelToEvent(model)
		if err != nil {
			return nil, err
		}
		committed = append(committed, event)
	}
	return committed, nil
}

func (store *EventStore) Load(ctx context.Context, sessionID string, afterSeq uint64, limit int) ([]session.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	var models []SessionEventModel
	if err := store.db.WithContext(ctx).
		Where("session_id = ? AND seq > ?", sessionID, afterSeq).
		Order("seq ASC").
		Limit(limit).
		Find(&models).Error; err != nil {
		return nil, fmt.Errorf("load session events: %w", err)
	}
	events := make([]session.Event, 0, len(models))
	for _, model := range models {
		event, err := modelToEvent(model)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (store *EventStore) Head(ctx context.Context, sessionID string) (uint64, error) {
	var model SessionModel
	if err := store.db.WithContext(ctx).Select("last_seq").First(&model, "id = ?", sessionID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return 0, session.ErrNotFound
		}
		return 0, fmt.Errorf("read session head: %w", err)
	}
	return model.LastSeq, nil
}

func eventToModel(event session.Event) (SessionEventModel, error) {
	sourceSeqs, err := json.Marshal(event.SourceEventSeqs)
	if err != nil {
		return SessionEventModel{}, fmt.Errorf("marshal source event seqs: %w", err)
	}
	var surfaceOp []byte
	if event.SurfaceOp != nil {
		surfaceOp, err = json.Marshal(event.SurfaceOp)
		if err != nil {
			return SessionEventModel{}, fmt.Errorf("marshal surface operation: %w", err)
		}
	}
	return SessionEventModel{
		SessionID:        event.SessionID,
		Seq:              event.Seq,
		EventID:          event.EventID,
		Type:             event.EventType,
		SchemaMajor:      event.SchemaVersion.Major,
		SchemaMinor:      event.SchemaVersion.Minor,
		ReplayPolicy:     string(event.ReplayPolicy),
		OccurredAt:       event.OccurredAt,
		CommittedAt:      event.CommittedAt,
		TurnID:           stringPointer(event.TurnID),
		StepID:           stringPointer(event.StepID),
		CallID:           stringPointer(event.CallID),
		TraceID:          stringPointer(event.Trace.TraceID),
		SpanID:           stringPointer(event.Trace.SpanID),
		ParentSpanID:     stringPointer(event.Trace.ParentSpanID),
		CausationEventID: stringPointer(event.CausationEventID),
		SourceEventSeqs:  sourceSeqs,
		SurfaceOp:        nullableJSON(surfaceOp),
		Data:             cloneBytes(event.Data),
		Extensions:       nullableJSON(event.Extensions),
		CreatedAt:        event.CommittedAt,
	}, nil
}

func modelToEvent(model SessionEventModel) (session.Event, error) {
	var sourceSeqs []uint64
	if err := json.Unmarshal(model.SourceEventSeqs, &sourceSeqs); err != nil {
		return session.Event{}, fmt.Errorf("unmarshal source event seqs: %w", err)
	}
	var surfaceOp *session.SurfaceOp
	if len(model.SurfaceOp) != 0 && string(model.SurfaceOp) != "null" {
		surfaceOp = new(session.SurfaceOp)
		if err := json.Unmarshal(model.SurfaceOp, surfaceOp); err != nil {
			return session.Event{}, fmt.Errorf("unmarshal surface operation: %w", err)
		}
	}
	return session.Event{
		SchemaVersion: session.SchemaVersion{Major: model.SchemaMajor, Minor: model.SchemaMinor},
		EventType:     model.Type,
		EventID:       model.EventID,
		SessionID:     model.SessionID,
		Seq:           model.Seq,
		OccurredAt:    model.OccurredAt,
		CommittedAt:   model.CommittedAt,
		ReplayPolicy:  session.ReplayPolicy(model.ReplayPolicy),
		Data:          cloneBytes(model.Data),
		TurnID:        stringValue(model.TurnID),
		StepID:        stringValue(model.StepID),
		CallID:        stringValue(model.CallID),
		Trace: session.TraceContext{
			TraceID:      stringValue(model.TraceID),
			SpanID:       stringValue(model.SpanID),
			ParentSpanID: stringValue(model.ParentSpanID),
		},
		CausationEventID: stringValue(model.CausationEventID),
		SourceEventSeqs:  sourceSeqs,
		SurfaceOp:        surfaceOp,
		Extensions:       cloneBytes(model.Extensions),
	}, nil
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
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

func nullableJSON(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return cloneBytes(value)
}
