CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    parent_id TEXT NULL REFERENCES sessions(id),
    fork_seq BIGINT NULL CHECK (fork_seq >= 0),
    status TEXT NOT NULL,
    last_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    actor_epoch BIGINT NOT NULL DEFAULT 0 CHECK (actor_epoch >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NULL,
    CHECK ((parent_id IS NULL AND fork_seq IS NULL) OR (parent_id IS NOT NULL AND fork_seq IS NOT NULL)),
    CHECK (parent_id IS NULL OR parent_id <> id)
);

CREATE INDEX idx_sessions_tenant_updated ON sessions(tenant_id, updated_at DESC);
CREATE INDEX idx_sessions_workspace ON sessions(workspace_id);
CREATE INDEX idx_sessions_parent ON sessions(parent_id);

CREATE TABLE session_events (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    seq BIGINT NOT NULL CHECK (seq > 0),
    event_id TEXT NOT NULL UNIQUE,
    type VARCHAR(128) NOT NULL,
    schema_major INTEGER NOT NULL CHECK (schema_major > 0),
    schema_minor INTEGER NOT NULL CHECK (schema_minor >= 0),
    replay_policy TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL,
    turn_id TEXT NULL,
    step_id TEXT NULL,
    call_id TEXT NULL,
    trace_id TEXT NULL,
    span_id TEXT NULL,
    parent_span_id TEXT NULL,
    causation_event_id TEXT NULL,
    source_event_seqs JSONB NOT NULL DEFAULT '[]'::jsonb,
    data JSONB NOT NULL,
    extensions JSONB NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, seq)
);

CREATE INDEX idx_session_events_type ON session_events(session_id, type, seq);
CREATE INDEX idx_session_events_call ON session_events(call_id) WHERE call_id IS NOT NULL;
CREATE INDEX idx_session_events_trace ON session_events(trace_id) WHERE trace_id IS NOT NULL;

CREATE TABLE session_projections (
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    projection_version INTEGER NOT NULL,
    through_seq BIGINT NOT NULL CHECK (through_seq >= 0),
    data JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, name)
);
