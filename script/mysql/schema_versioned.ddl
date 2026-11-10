-- MySQL 8.0.16+ schema for slack-ai-gateway.
-- Explicit provisioning only: the gateway never applies this file on startup.
-- Reapplying an installed version preserves business data. No SQLite history is imported.
-- Binary NO PAD collation preserves case and trailing spaces in external identifiers.
CREATE DATABASE IF NOT EXISTS `slack_ai_gateway`
    CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
USE `slack_ai_gateway`;

DELIMITER $$
CREATE TABLE IF NOT EXISTS schema_version (
    version_number INT UNSIGNED NOT NULL PRIMARY KEY
) ENGINE=InnoDB $$

DROP FUNCTION IF EXISTS get_schema_version $$
CREATE FUNCTION get_schema_version() RETURNS INT READS SQL DATA
BEGIN
    DECLARE installed INT DEFAULT 0;
    SELECT COALESCE(MAX(version_number), 0) INTO installed FROM schema_version;
    RETURN installed;
END $$

DROP PROCEDURE IF EXISTS set_schema_version $$
CREATE PROCEDURE set_schema_version(IN requested_version INT)
BEGIN
    DELETE FROM schema_version;
    INSERT INTO schema_version(version_number) VALUES (requested_version);
END $$

DROP PROCEDURE IF EXISTS schema_upgrade_1 $$
CREATE PROCEDURE schema_upgrade_1()
BEGIN
    IF get_schema_version() > 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Installed gateway schema is newer than this DDL';
    END IF;
    IF get_schema_version() = 0 THEN
CREATE TABLE IF NOT EXISTS conversation_bindings (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    workspace_id VARCHAR(255) NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    root_thread_ts VARCHAR(64) NOT NULL,
    conversation_id LONGTEXT,
    access_mode LONGTEXT NOT NULL,
    principal_id LONGTEXT NOT NULL,
    owner_id LONGTEXT NOT NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('reserved', 'creating', 'ready', 'needs_review')),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE (workspace_id, channel_id, root_thread_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS inbox_jobs (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    app_id VARCHAR(255) NOT NULL,
    workspace_id VARCHAR(255) NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    root_thread_ts VARCHAR(64) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    author_id LONGTEXT NOT NULL,
    message_ts LONGTEXT NOT NULL,
    query_text LONGTEXT NOT NULL,
    has_attachments BIGINT NOT NULL CHECK (has_attachments IN (0, 1)),
    binding_id BIGINT,
    access_mode LONGTEXT NOT NULL DEFAULT (''),
    principal_id LONGTEXT NOT NULL DEFAULT (''),
    owner_id LONGTEXT NOT NULL DEFAULT (''),
    request_id VARCHAR(255) NOT NULL UNIQUE,
    turn_id LONGTEXT NOT NULL DEFAULT (''),
    state VARCHAR(32) NOT NULL CHECK (state IN ('pending', 'processing', 'querying', 'completed', 'rejected', 'needs_review')),
    result_text LONGTEXT NOT NULL DEFAULT (''),
    reason LONGTEXT NOT NULL DEFAULT (''),
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    query_started_at BIGINT NOT NULL DEFAULT 0,
    poll_attempts BIGINT NOT NULL DEFAULT 0,
    poll_ready BIGINT NOT NULL DEFAULT 0 CHECK (poll_ready IN (0,1)),
    message_surface VARCHAR(32) NOT NULL DEFAULT 'channel' CHECK (message_surface IN ('channel', 'direct_message')),
    UNIQUE (app_id, workspace_id, event_id),
    FOREIGN KEY (binding_id) REFERENCES conversation_bindings(id),
    INDEX inbox_pending (state, next_attempt_at, id),
    INDEX inbox_thread (workspace_id, channel_id, root_thread_ts, id, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    job_id BIGINT NOT NULL,
    workspace_id VARCHAR(255) NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    root_thread_ts VARCHAR(64) NOT NULL,
    part BIGINT NOT NULL CHECK (part >= 0),
    content LONGTEXT NOT NULL,
    state VARCHAR(32) NOT NULL CHECK (state IN ('pending', 'sending', 'delivered', 'delivery_unknown', 'failed')),
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0,
    slack_ts LONGTEXT NOT NULL DEFAULT (''),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    format VARCHAR(32) NOT NULL DEFAULT 'plain' CHECK (format IN ('plain', 'markdown')),
    UNIQUE (job_id, part),
    FOREIGN KEY (job_id) REFERENCES inbox_jobs(id),
    INDEX outbox_pending (state, next_attempt_at, id),
    INDEX outbox_thread (workspace_id, channel_id, root_thread_ts, id, state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS delivery_resolutions (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    delivery_id BIGINT NOT NULL,
    action VARCHAR(32) NOT NULL CHECK (action IN ('delivered','retry')),
    slack_ts LONGTEXT NOT NULL DEFAULT (''),
    duplicate_risk_accepted BIGINT NOT NULL CHECK (duplicate_risk_accepted IN (0,1)),
    created_at BIGINT NOT NULL,
    FOREIGN KEY (delivery_id) REFERENCES outbox_messages(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS request_status (
    job_id BIGINT NOT NULL PRIMARY KEY,
    FOREIGN KEY (job_id) REFERENCES inbox_jobs(id),
    slack_ts LONGTEXT NOT NULL DEFAULT (''),
    observed_state LONGTEXT NOT NULL DEFAULT (''),
    observed_at BIGINT NOT NULL DEFAULT 0,
    observation_uncertain BIGINT NOT NULL DEFAULT 0 CHECK (observation_uncertain IN (0,1)),
    outcome VARCHAR(32) NOT NULL DEFAULT '' CHECK (outcome IN ('','completed','failed','canceled')),
    phase LONGTEXT NOT NULL DEFAULT (''),
    revision BIGINT NOT NULL DEFAULT 0,
    sent_revision BIGINT NOT NULL DEFAULT 0,
    state VARCHAR(32) NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','sending','settled','skipped','failed','delivery_unknown')),
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0,
    refresh_at BIGINT NOT NULL DEFAULT 0,
    rate_limited_until BIGINT NOT NULL DEFAULT 0,
    first_sent_at BIGINT NOT NULL DEFAULT 0,
    last_sent_at BIGINT NOT NULL DEFAULT 0,
    reason LONGTEXT NOT NULL DEFAULT (''),
    updated_at BIGINT NOT NULL,
    mode VARCHAR(32) NOT NULL DEFAULT 'message' CHECK (mode IN ('message','agent_session','hybrid')),
    INDEX request_status_due (state,next_attempt_at,job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS agent_session_publications (
    workspace_id VARCHAR(255) NOT NULL,
    channel_id VARCHAR(255) NOT NULL,
    root_thread_ts VARCHAR(64) NOT NULL,
    latest_job_id BIGINT NOT NULL,
    source_job_id BIGINT NOT NULL,
    initiator_user_id LONGTEXT NOT NULL,
    desired_status VARCHAR(32) NOT NULL DEFAULT 'processing' CHECK (desired_status IN ('processing','active','suspended')),
    agent_status LONGTEXT NOT NULL DEFAULT (''),
    session_status LONGTEXT NOT NULL DEFAULT (''),
    revision BIGINT NOT NULL DEFAULT 1,
    sent_revision BIGINT NOT NULL DEFAULT 0,
    claimed_revision BIGINT NOT NULL DEFAULT 0,
    state VARCHAR(32) NOT NULL DEFAULT 'ready' CHECK (state IN ('ready','sending','settled','skipped','failed')),
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at BIGINT NOT NULL DEFAULT 0,
    refresh_at BIGINT NOT NULL DEFAULT 0,
    rate_limited_until BIGINT NOT NULL DEFAULT 0,
    first_sent_at BIGINT NOT NULL DEFAULT 0,
    last_sent_at BIGINT NOT NULL DEFAULT 0,
    reason LONGTEXT NOT NULL DEFAULT (''),
    warning LONGTEXT NOT NULL DEFAULT (''),
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (workspace_id,channel_id,root_thread_ts),
    FOREIGN KEY (latest_job_id) REFERENCES inbox_jobs(id),
    FOREIGN KEY (source_job_id) REFERENCES inbox_jobs(id),
    INDEX agent_session_due (state,next_attempt_at,last_sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
        CALL set_schema_version(1);
    END IF;
END $$
CALL schema_upgrade_1() $$
DROP PROCEDURE schema_upgrade_1 $$
DELIMITER ;
