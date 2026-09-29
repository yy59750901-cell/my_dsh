package gormrepo

import (
	"time"

	"gorm.io/datatypes"
)

type SessionModel struct {
	ID          string     `gorm:"column:id;primaryKey"`
	TenantID    string     `gorm:"column:tenant_id;not null;index:idx_sessions_tenant_updated,priority:1"`
	WorkspaceID string     `gorm:"column:workspace_id;not null;index"`
	ParentID    *string    `gorm:"column:parent_id;index"`
	ForkSeq     *uint64    `gorm:"column:fork_seq"`
	Status      string     `gorm:"column:status;not null"`
	LastSeq     uint64     `gorm:"column:last_seq;not null"`
	ActorEpoch  uint64     `gorm:"column:actor_epoch;not null"`
	CreatedAt   time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt   time.Time  `gorm:"column:updated_at;not null;index:idx_sessions_tenant_updated,priority:2"`
	ArchivedAt  *time.Time `gorm:"column:archived_at"`
}

func (SessionModel) TableName() string { return "sessions" }

type SessionEventModel struct {
	SessionID        string         `gorm:"column:session_id;primaryKey"`
	Seq              uint64         `gorm:"column:seq;primaryKey"`
	EventID          string         `gorm:"column:event_id;not null;uniqueIndex"`
	Type             string         `gorm:"column:type;not null;index:idx_session_events_type,priority:2"`
	SchemaMajor      uint32         `gorm:"column:schema_major;not null"`
	SchemaMinor      uint32         `gorm:"column:schema_minor;not null"`
	ReplayPolicy     string         `gorm:"column:replay_policy;not null"`
	OccurredAt       time.Time      `gorm:"column:occurred_at;not null"`
	CommittedAt      time.Time      `gorm:"column:committed_at;not null"`
	TurnID           *string        `gorm:"column:turn_id"`
	StepID           *string        `gorm:"column:step_id"`
	CallID           *string        `gorm:"column:call_id;index"`
	TraceID          *string        `gorm:"column:trace_id;index"`
	SpanID           *string        `gorm:"column:span_id"`
	ParentSpanID     *string        `gorm:"column:parent_span_id"`
	CausationEventID *string        `gorm:"column:causation_event_id"`
	SourceEventSeqs  datatypes.JSON `gorm:"column:source_event_seqs;not null"`
	SurfaceOp        datatypes.JSON `gorm:"column:surface_op"`
	Data             datatypes.JSON `gorm:"column:data;not null"`
	Extensions       datatypes.JSON `gorm:"column:extensions"`
	CreatedAt        time.Time      `gorm:"column:created_at;not null"`
}

func (SessionEventModel) TableName() string { return "session_events" }

type SessionProjectionModel struct {
	SessionID         string         `gorm:"column:session_id;primaryKey"`
	Name              string         `gorm:"column:name;primaryKey"`
	ProjectionVersion uint32         `gorm:"column:projection_version;not null"`
	ThroughSeq        uint64         `gorm:"column:through_seq;not null"`
	Data              datatypes.JSON `gorm:"column:data;not null"`
	UpdatedAt         time.Time      `gorm:"column:updated_at;not null"`
}

func (SessionProjectionModel) TableName() string { return "session_projections" }
