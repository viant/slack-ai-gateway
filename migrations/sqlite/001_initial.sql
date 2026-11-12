-- Initial SQLite schema for a fresh developer database.
-- Integration assignments and operation scope match MySQL; SQLite retains its native CHECK constraints.
-- Moments use UTC DATETIME normalized to milliseconds; absent/not-yet values use NULL.

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at DATETIME NOT NULL
);

CREATE TABLE integration (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0,1)),
    slack_app_id TEXT NOT NULL,
    slack_workspace_id TEXT NOT NULL,
    backend_type TEXT NOT NULL,
    backend_endpoint TEXT NOT NULL,
    backend_agent_id TEXT NOT NULL DEFAULT '',
    backend_principal_id TEXT NOT NULL,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

CREATE TABLE conversation_binding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    integration_id INTEGER NOT NULL,
    slack_channel_id TEXT NOT NULL,
    slack_thread_root_message_id TEXT NOT NULL,
    backend_conversation_id TEXT,
    gateway_access_mode TEXT NOT NULL,
    backend_principal_id TEXT NOT NULL,
    gateway_binding_owner_id TEXT NOT NULL,
    gateway_binding_state TEXT NOT NULL CHECK (gateway_binding_state IN ('reserved', 'creating', 'ready', 'needs_review', 'closed')),
    backend_protocol_messages TEXT NOT NULL DEFAULT '[]',
    backend_protocol_state TEXT NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    last_activity_at DATETIME NOT NULL,
    closed_at DATETIME NULL,
    UNIQUE (integration_id, slack_channel_id, slack_thread_root_message_id),
    FOREIGN KEY (integration_id) REFERENCES integration(id)
);

CREATE TABLE gateway_job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gateway_request_id TEXT NOT NULL UNIQUE,
    integration_id INTEGER NOT NULL,
    conversation_binding_id INTEGER,
    slack_event_id TEXT NOT NULL,
    slack_channel_id TEXT NOT NULL,
    slack_conversation_type TEXT NOT NULL DEFAULT 'channel' CHECK (slack_conversation_type IN ('channel', 'direct_message')),
    slack_thread_root_message_id TEXT NOT NULL,
    slack_prompt_message_id TEXT NOT NULL,
    slack_prompt_author_id TEXT NOT NULL,
    prompt_text TEXT NULL,
    prompt_has_attachments INTEGER NOT NULL CHECK (prompt_has_attachments IN (0, 1)),
    gateway_execution_state TEXT NOT NULL CHECK (gateway_execution_state IN ('pending', 'processing', 'querying', 'completed', 'rejected', 'needs_review')),
    gateway_execution_reason TEXT NOT NULL DEFAULT '',
    gateway_execution_outcome TEXT NOT NULL DEFAULT '' CHECK (gateway_execution_outcome IN ('','completed','failed','canceled')),
    gateway_execution_result_text TEXT NULL,
    gateway_execution_claim_count INTEGER NOT NULL DEFAULT 0,
    backend_submission_started_at DATETIME NULL,
    backend_next_poll_at DATETIME NULL,
    backend_poll_attempts INTEGER NOT NULL DEFAULT 0,
    backend_poll_ready INTEGER NOT NULL DEFAULT 0 CHECK (backend_poll_ready IN (0,1)),
    backend_observed_state TEXT NOT NULL DEFAULT '',
    backend_observed_at DATETIME NULL,
    backend_observation_uncertain INTEGER NOT NULL DEFAULT 0 CHECK (backend_observation_uncertain IN (0,1)),
    slack_progress_mode TEXT NOT NULL DEFAULT 'message' CHECK (slack_progress_mode IN ('message','agent_session','hybrid')),
    slack_progress_message_id TEXT NOT NULL DEFAULT '',
    slack_progress_state TEXT NOT NULL DEFAULT 'ready' CHECK (slack_progress_state IN ('ready','sending','settled','skipped','failed','delivery_unknown')),
    slack_progress_phase TEXT NOT NULL DEFAULT '',
    slack_progress_reason TEXT NOT NULL DEFAULT '',
    slack_progress_revision INTEGER NOT NULL DEFAULT 0,
    slack_progress_sent_revision INTEGER NOT NULL DEFAULT 0,
    slack_progress_attempts INTEGER NOT NULL DEFAULT 0,
    slack_progress_next_attempt_at DATETIME NULL,
    slack_progress_refresh_at DATETIME NULL,
    slack_progress_rate_limited_until DATETIME NULL,
    slack_progress_first_sent_at DATETIME NULL,
    slack_progress_last_sent_at DATETIME NULL,
    created_at DATETIME NOT NULL,
    gateway_execution_updated_at DATETIME NOT NULL,
    gateway_status_updated_at DATETIME NOT NULL,
    gateway_execution_finished_at DATETIME NULL,
    content_purged_at DATETIME NULL,
    UNIQUE (slack_event_id),
    FOREIGN KEY (integration_id) REFERENCES integration(id),
    FOREIGN KEY (conversation_binding_id) REFERENCES conversation_binding(id)
);

CREATE TABLE slack_outgoing_message (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    slack_message_index INTEGER NOT NULL CHECK (slack_message_index >= 0),
    slack_message_text TEXT NULL,
    slack_message_kind TEXT NOT NULL DEFAULT 'response' CHECK (slack_message_kind IN ('response','integration_notice')),
    slack_delivery_state TEXT NOT NULL CHECK (slack_delivery_state IN ('pending', 'sending', 'delivered', 'delivery_unknown', 'failed')),
    slack_delivery_attempts INTEGER NOT NULL DEFAULT 0,
    slack_delivery_next_attempt_at DATETIME NULL,
    slack_message_id TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    slack_message_format TEXT NOT NULL DEFAULT 'plain' CHECK (slack_message_format IN ('plain', 'markdown')),
    UNIQUE (job_id, slack_message_index),
    FOREIGN KEY (job_id) REFERENCES gateway_job(id)
);

CREATE TABLE slack_delivery_resolution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    outgoing_message_id INTEGER NOT NULL,
    resolution_action TEXT NOT NULL CHECK (resolution_action IN ('delivered','retry')),
    slack_message_id TEXT NOT NULL DEFAULT '',
    duplicate_delivery_risk_accepted INTEGER NOT NULL CHECK (duplicate_delivery_risk_accepted IN (0,1)),
    created_at DATETIME NOT NULL,
    FOREIGN KEY (outgoing_message_id) REFERENCES slack_outgoing_message(id)
);



CREATE TABLE slack_agent_session_publication (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    initial_integration_id INTEGER NOT NULL,
    slack_channel_id TEXT NOT NULL,
    slack_thread_root_message_id TEXT NOT NULL,
    latest_status_job_id INTEGER NOT NULL,
    status_source_job_id INTEGER NOT NULL,
    slack_initiator_user_id TEXT NOT NULL,
    slack_agent_desired_status TEXT NOT NULL DEFAULT 'processing' CHECK (slack_agent_desired_status IN ('processing','active','suspended')),
    slack_agent_observed_status TEXT NOT NULL DEFAULT '',
    slack_session_observed_status TEXT NOT NULL DEFAULT '',
    slack_publication_revision INTEGER NOT NULL DEFAULT 1,
    slack_publication_sent_revision INTEGER NOT NULL DEFAULT 0,
    slack_publication_claimed_revision INTEGER NOT NULL DEFAULT 0,
    slack_publication_state TEXT NOT NULL DEFAULT 'ready' CHECK (slack_publication_state IN ('ready','sending','settled','skipped','failed')),
    slack_publication_attempts INTEGER NOT NULL DEFAULT 0,
    slack_publication_next_attempt_at DATETIME NULL,
    slack_publication_refresh_at DATETIME NULL,
    slack_publication_rate_limited_until DATETIME NULL,
    slack_publication_first_sent_at DATETIME NULL,
    slack_publication_last_sent_at DATETIME NULL,
    slack_publication_reason TEXT NOT NULL DEFAULT '',
    slack_publication_warning TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE (initial_integration_id, slack_channel_id, slack_thread_root_message_id),
    FOREIGN KEY (initial_integration_id) REFERENCES integration(id),
    FOREIGN KEY (latest_status_job_id) REFERENCES gateway_job(id),
    FOREIGN KEY (status_source_job_id) REFERENCES gateway_job(id)
);

CREATE INDEX integration_name_active ON integration(name, active, id);
CREATE INDEX integration_slack ON integration(slack_app_id, slack_workspace_id, active, id);
CREATE INDEX gateway_job_pending ON gateway_job(integration_id, gateway_execution_state, backend_next_poll_at, id);
CREATE INDEX gateway_job_thread ON gateway_job(integration_id, slack_channel_id, slack_thread_root_message_id, gateway_execution_state, id);
CREATE INDEX slack_outgoing_message_pending ON slack_outgoing_message(slack_delivery_state, slack_delivery_next_attempt_at, id);
CREATE INDEX slack_outgoing_message_job_state ON slack_outgoing_message(job_id, slack_delivery_state, id);
CREATE INDEX slack_outgoing_message_cooldown ON slack_outgoing_message(slack_delivery_state, slack_delivery_next_attempt_at, job_id);
CREATE INDEX gateway_job_progress_due ON gateway_job(slack_progress_state, slack_progress_next_attempt_at, id);
CREATE INDEX gateway_job_progress_cooldown ON gateway_job(slack_progress_rate_limited_until, id);
CREATE INDEX slack_agent_session_publication_due ON slack_agent_session_publication(initial_integration_id, slack_publication_state, slack_publication_next_attempt_at, slack_publication_last_sent_at);

CREATE INDEX gateway_job_retention_content ON gateway_job(content_purged_at, gateway_execution_finished_at, id);
CREATE INDEX gateway_job_binding_finished ON gateway_job(conversation_binding_id, gateway_execution_finished_at, id);
CREATE INDEX conversation_binding_last_activity ON conversation_binding(last_activity_at, id);
CREATE INDEX slack_delivery_resolution_outgoing ON slack_delivery_resolution(outgoing_message_id);
CREATE INDEX slack_agent_session_publication_latest_status_job ON slack_agent_session_publication(latest_status_job_id);
CREATE INDEX slack_agent_session_publication_status_source_job ON slack_agent_session_publication(status_source_job_id);
