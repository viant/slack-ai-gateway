-- Initial SQLite schema for the first release.
-- Earlier development schemas are archived outside the published source.

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at INTEGER NOT NULL
);

CREATE TABLE conversation_bindings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    conversation_id TEXT,
    access_mode TEXT NOT NULL,
    principal_id TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('reserved', 'creating', 'ready', 'needs_review')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (workspace_id, channel_id, root_thread_ts)
);

CREATE TABLE inbox_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app_id TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    event_id TEXT NOT NULL,
    author_id TEXT NOT NULL,
    message_ts TEXT NOT NULL,
    query_text TEXT NOT NULL,
    has_attachments INTEGER NOT NULL CHECK (has_attachments IN (0, 1)),
    binding_id INTEGER REFERENCES conversation_bindings(id),
    access_mode TEXT NOT NULL DEFAULT '',
    principal_id TEXT NOT NULL DEFAULT '',
    owner_id TEXT NOT NULL DEFAULT '',
    request_id TEXT NOT NULL UNIQUE,
    turn_id TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL CHECK (state IN ('pending', 'processing', 'querying', 'completed', 'rejected', 'needs_review')),
    result_text TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    query_started_at INTEGER NOT NULL DEFAULT 0,
    poll_attempts INTEGER NOT NULL DEFAULT 0,
    poll_ready INTEGER NOT NULL DEFAULT 0 CHECK (poll_ready IN (0,1)),
    message_surface TEXT NOT NULL DEFAULT 'channel' CHECK (message_surface IN ('channel', 'direct_message')),
    UNIQUE (app_id, workspace_id, event_id)
);

CREATE TABLE outbox_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL REFERENCES inbox_jobs(id),
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    part INTEGER NOT NULL CHECK (part >= 0),
    content TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'delivered', 'delivery_unknown', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    slack_ts TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    format TEXT NOT NULL DEFAULT 'plain' CHECK (format IN ('plain', 'markdown')),
    UNIQUE (job_id, part)
);

CREATE TABLE delivery_resolutions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id INTEGER NOT NULL REFERENCES outbox_messages(id),
    action TEXT NOT NULL CHECK (action IN ('delivered','retry')),
    slack_ts TEXT NOT NULL DEFAULT '',
    duplicate_risk_accepted INTEGER NOT NULL CHECK (duplicate_risk_accepted IN (0,1)),
    created_at INTEGER NOT NULL
);

CREATE TABLE "request_status" (
    job_id INTEGER PRIMARY KEY REFERENCES inbox_jobs(id),
    slack_ts TEXT NOT NULL DEFAULT '',
    observed_state TEXT NOT NULL DEFAULT '',
    observed_at INTEGER NOT NULL DEFAULT 0,
    observation_uncertain INTEGER NOT NULL DEFAULT 0 CHECK (observation_uncertain IN (0,1)),
    outcome TEXT NOT NULL DEFAULT '' CHECK (outcome IN ('','completed','failed','canceled')),
    phase TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 0,
    sent_revision INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','sending','settled','skipped','failed','delivery_unknown')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    refresh_at INTEGER NOT NULL DEFAULT 0,
    rate_limited_until INTEGER NOT NULL DEFAULT 0,
    first_sent_at INTEGER NOT NULL DEFAULT 0,
    last_sent_at INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL,
    mode TEXT NOT NULL DEFAULT 'message' CHECK (mode IN ('message','agent_session','hybrid'))
);

CREATE TABLE agent_session_publications (
    workspace_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    root_thread_ts TEXT NOT NULL,
    latest_job_id INTEGER NOT NULL REFERENCES inbox_jobs(id),
    source_job_id INTEGER NOT NULL REFERENCES inbox_jobs(id),
    initiator_user_id TEXT NOT NULL,
    desired_status TEXT NOT NULL DEFAULT 'processing' CHECK (desired_status IN ('processing','active','suspended')),
    agent_status TEXT NOT NULL DEFAULT '',
    session_status TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 1,
    sent_revision INTEGER NOT NULL DEFAULT 0,
    claimed_revision INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','sending','settled','skipped','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    refresh_at INTEGER NOT NULL DEFAULT 0,
    rate_limited_until INTEGER NOT NULL DEFAULT 0,
    first_sent_at INTEGER NOT NULL DEFAULT 0,
    last_sent_at INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    warning TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (workspace_id,channel_id,root_thread_ts)
);

CREATE INDEX agent_session_due ON agent_session_publications(state,next_attempt_at,last_sent_at);

CREATE INDEX inbox_pending ON inbox_jobs(state, next_attempt_at, id);

CREATE INDEX inbox_thread ON inbox_jobs(workspace_id, channel_id, root_thread_ts, id, state);

CREATE INDEX outbox_pending ON outbox_messages(state, next_attempt_at, id);

CREATE INDEX outbox_thread ON outbox_messages(workspace_id, channel_id, root_thread_ts, id, state);

CREATE INDEX request_status_due ON request_status(state,next_attempt_at,job_id);
