PRAGMA foreign_keys = ON;

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    parent_id TEXT NULL,
    fork_seq INTEGER NULL CHECK (fork_seq >= 0),
    status TEXT NOT NULL,
    last_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_seq >= 0),
    actor_epoch INTEGER NOT NULL DEFAULT 0 CHECK (actor_epoch >= 0),
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    archived_at DATETIME NULL,
    CHECK ((parent_id IS NULL AND fork_seq IS NULL) OR (parent_id IS NOT NULL AND fork_seq IS NOT NULL)),
    CHECK (parent_id IS NULL OR parent_id <> id),
    FOREIGN KEY (parent_id) REFERENCES sessions(id)
);

CREATE INDEX idx_sessions_tenant_updated ON sessions(tenant_id, updated_at);
CREATE INDEX idx_sessions_workspace ON sessions(workspace_id);
CREATE INDEX idx_sessions_parent ON sessions(parent_id);

CREATE TABLE session_events (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    event_id TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL,
    schema_major INTEGER NOT NULL CHECK (schema_major > 0),
    schema_minor INTEGER NOT NULL CHECK (schema_minor >= 0),
    replay_policy TEXT NOT NULL,
    occurred_at DATETIME NOT NULL,
    committed_at DATETIME NOT NULL,
    turn_id TEXT NULL,
    step_id TEXT NULL,
    call_id TEXT NULL,
    trace_id TEXT NULL,
    span_id TEXT NULL,
    parent_span_id TEXT NULL,
    causation_event_id TEXT NULL,
    source_event_seqs TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(source_event_seqs)),
    data TEXT NOT NULL CHECK (json_valid(data)),
    extensions TEXT NULL CHECK (extensions IS NULL OR json_valid(extensions)),
    created_at DATETIME NOT NULL,
    PRIMARY KEY (session_id, seq),
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE INDEX idx_session_events_type ON session_events(session_id, type, seq);
CREATE INDEX idx_session_events_call ON session_events(call_id);
CREATE INDEX idx_session_events_trace ON session_events(trace_id);

CREATE TABLE session_projections (
    session_id TEXT NOT NULL,
    name TEXT NOT NULL,
    projection_version INTEGER NOT NULL,
    through_seq INTEGER NOT NULL CHECK (through_seq >= 0),
    data TEXT NOT NULL CHECK (json_valid(data)),
    updated_at DATETIME NOT NULL,
    PRIMARY KEY (session_id, name),
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
