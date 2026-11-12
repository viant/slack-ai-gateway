-- MySQL 8.0.16+ schema for slack-ai-gateway.
-- Explicit provisioning only: the gateway never applies this file on startup.
-- Reapplying an installed version preserves business data. No SQLite history is imported.
-- Binary NO PAD collation preserves case and trailing spaces in external identifiers.
-- Persisted moments use UTC DATETIME(3); NULL represents absent/not-yet values.
-- Runtime configuration reconciles numbered integration assignments before work.
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
    IF get_schema_version() = 1 AND (
        (SELECT COUNT(*) FROM information_schema.columns
         WHERE table_schema=DATABASE() AND table_name='integration'
           AND column_name IN ('name','active','slack_app_id','slack_workspace_id','backend_endpoint')) <> 5
        OR (SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema=DATABASE() AND table_name IN ('slack_installations','backend_connections','agents','inbox_jobs','request_status','outbox_messages','delivery_resolutions','agent_session_publications','integrations','conversation_bindings','requests','slack_outgoing_messages','slack_delivery_resolutions','slack_agent_session_publications')) <> 0
        OR (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='gateway_job'
              AND column_name IN ('gateway_request_id','backend_submission_started_at','slack_prompt_message_id','slack_progress_mode','gateway_status_updated_at','gateway_execution_finished_at','content_purged_at')) <> 7
        OR (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='conversation_binding'
              AND column_name IN ('last_activity_at','closed_at')) <> 2
        OR (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name='integration' AND column_name='created_at'
              AND data_type='datetime' AND datetime_precision=3) <> 1
        OR (SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema=DATABASE() AND table_name IN ('integration','conversation_binding','gateway_job','slack_outgoing_message','slack_delivery_resolution','slack_agent_session_publication')
              AND ((column_name='gateway_request_id' AND character_maximum_length=255)
                OR (column_name IN ('slack_thread_root_message_id','slack_prompt_message_id','slack_progress_message_id','slack_message_id') AND character_maximum_length=255)
                OR (column_name IN ('backend_type','gateway_access_mode','gateway_binding_state','slack_conversation_type','gateway_execution_state','gateway_execution_outcome','slack_progress_mode','slack_progress_state','slack_message_kind','slack_delivery_state','slack_message_format','resolution_action','slack_agent_desired_status','slack_agent_observed_status','slack_session_observed_status','slack_publication_state') AND character_maximum_length=128))) <> 24
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Incompatible developer gateway schema; provision a fresh database explicitly';
    END IF;
    IF get_schema_version() = 0 THEN
CREATE TABLE IF NOT EXISTS integration (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(128) NOT NULL,
    active TINYINT NOT NULL DEFAULT 0,
    slack_app_id VARCHAR(255) NOT NULL,
    slack_workspace_id VARCHAR(255) NOT NULL,
    backend_type VARCHAR(128) NOT NULL,
    backend_endpoint TEXT NOT NULL,
    backend_agent_id TEXT NOT NULL DEFAULT (''),
    backend_principal_id VARCHAR(255) NOT NULL,
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    INDEX integration_name_active (name, active, id),
    INDEX integration_slack (slack_app_id, slack_workspace_id, active, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS conversation_binding (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    integration_id BIGINT NOT NULL,
    slack_channel_id VARCHAR(255) NOT NULL,
    slack_thread_root_message_id VARCHAR(255) NOT NULL,
    backend_conversation_id TEXT,
    gateway_access_mode VARCHAR(128) NOT NULL,
    backend_principal_id VARCHAR(255) NOT NULL,
    gateway_binding_owner_id VARCHAR(255) NOT NULL,
    gateway_binding_state VARCHAR(128) NOT NULL,
    backend_protocol_messages LONGTEXT NOT NULL DEFAULT ('[]'),
    backend_protocol_state LONGTEXT NOT NULL DEFAULT ('{}'),
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    last_activity_at DATETIME(3) NOT NULL,
    closed_at DATETIME(3) NULL,
    UNIQUE (integration_id, slack_channel_id, slack_thread_root_message_id),
    FOREIGN KEY (integration_id) REFERENCES integration(id),
    INDEX conversation_binding_last_activity (last_activity_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS gateway_job (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    gateway_request_id VARCHAR(255) NOT NULL UNIQUE,
    integration_id BIGINT NOT NULL,
    conversation_binding_id BIGINT,
    slack_event_id VARCHAR(255) NOT NULL,
    slack_channel_id VARCHAR(255) NOT NULL,
    slack_conversation_type VARCHAR(128) NOT NULL DEFAULT 'channel',
    slack_thread_root_message_id VARCHAR(255) NOT NULL,
    slack_prompt_message_id VARCHAR(255) NOT NULL,
    slack_prompt_author_id VARCHAR(255) NOT NULL,
    prompt_text MEDIUMTEXT NULL,
    prompt_has_attachments TINYINT NOT NULL,
    gateway_execution_state VARCHAR(128) NOT NULL,
    gateway_execution_reason VARCHAR(255) NOT NULL DEFAULT '',
    gateway_execution_outcome VARCHAR(128) NOT NULL DEFAULT '',
    gateway_execution_result_text LONGTEXT NULL,
    gateway_execution_claim_count INT NOT NULL DEFAULT 0,
    backend_submission_started_at DATETIME(3) NULL,
    backend_next_poll_at DATETIME(3) NULL,
    backend_poll_attempts INT NOT NULL DEFAULT 0,
    backend_poll_ready TINYINT NOT NULL DEFAULT 0,
    backend_observed_state TEXT NOT NULL DEFAULT (''),
    backend_observed_at DATETIME(3) NULL,
    backend_observation_uncertain TINYINT NOT NULL DEFAULT 0,
    slack_progress_mode VARCHAR(128) NOT NULL DEFAULT 'message',
    slack_progress_message_id VARCHAR(255) NOT NULL DEFAULT '',
    slack_progress_state VARCHAR(128) NOT NULL DEFAULT 'ready',
    slack_progress_phase TEXT NOT NULL DEFAULT (''),
    slack_progress_reason VARCHAR(255) NOT NULL DEFAULT '',
    slack_progress_revision BIGINT NOT NULL DEFAULT 0,
    slack_progress_sent_revision BIGINT NOT NULL DEFAULT 0,
    slack_progress_attempts INT NOT NULL DEFAULT 0,
    slack_progress_next_attempt_at DATETIME(3) NULL,
    slack_progress_refresh_at DATETIME(3) NULL,
    slack_progress_rate_limited_until DATETIME(3) NULL,
    slack_progress_first_sent_at DATETIME(3) NULL,
    slack_progress_last_sent_at DATETIME(3) NULL,
    created_at DATETIME(3) NOT NULL,
    gateway_execution_updated_at DATETIME(3) NOT NULL,
    gateway_status_updated_at DATETIME(3) NOT NULL,
    gateway_execution_finished_at DATETIME(3) NULL,
    content_purged_at DATETIME(3) NULL,
    UNIQUE (slack_event_id),
    FOREIGN KEY (integration_id) REFERENCES integration(id),
    FOREIGN KEY (conversation_binding_id) REFERENCES conversation_binding(id),
    INDEX gateway_job_pending (integration_id, gateway_execution_state, backend_next_poll_at, id),
    INDEX gateway_job_thread (integration_id, slack_channel_id, slack_thread_root_message_id, gateway_execution_state, id),
    INDEX gateway_job_progress_due (slack_progress_state, slack_progress_next_attempt_at, id),
    INDEX gateway_job_progress_cooldown (slack_progress_rate_limited_until, id),
    INDEX gateway_job_retention_content (content_purged_at, gateway_execution_finished_at, id),
    INDEX gateway_job_binding_finished (conversation_binding_id, gateway_execution_finished_at, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS slack_outgoing_message (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    job_id BIGINT NOT NULL,
    slack_message_index INT NOT NULL,
    slack_message_text TEXT NULL,
    slack_message_kind VARCHAR(128) NOT NULL DEFAULT 'response',
    slack_delivery_state VARCHAR(128) NOT NULL,
    slack_delivery_attempts INT NOT NULL DEFAULT 0,
    slack_delivery_next_attempt_at DATETIME(3) NULL,
    slack_message_id VARCHAR(255) NOT NULL DEFAULT '',
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    slack_message_format VARCHAR(128) NOT NULL DEFAULT 'plain',
    UNIQUE (job_id, slack_message_index),
    FOREIGN KEY (job_id) REFERENCES gateway_job(id),
    INDEX slack_outgoing_message_pending (slack_delivery_state, slack_delivery_next_attempt_at, id),
    INDEX slack_outgoing_message_job_state (job_id, slack_delivery_state, id),
    INDEX slack_outgoing_message_cooldown (slack_delivery_state, slack_delivery_next_attempt_at, job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;

CREATE TABLE IF NOT EXISTS slack_delivery_resolution (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    outgoing_message_id BIGINT NOT NULL,
    resolution_action VARCHAR(128) NOT NULL,
    slack_message_id VARCHAR(255) NOT NULL DEFAULT '',
    duplicate_delivery_risk_accepted TINYINT NOT NULL,
    created_at DATETIME(3) NOT NULL,
    FOREIGN KEY (outgoing_message_id) REFERENCES slack_outgoing_message(id),
    INDEX slack_delivery_resolution_outgoing (outgoing_message_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin COMMENT='Append-only delivery resolution audit';



CREATE TABLE IF NOT EXISTS slack_agent_session_publication (
    id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    initial_integration_id BIGINT NOT NULL,
    slack_channel_id VARCHAR(255) NOT NULL,
    slack_thread_root_message_id VARCHAR(255) NOT NULL,
    latest_status_job_id BIGINT NOT NULL,
    status_source_job_id BIGINT NOT NULL,
    slack_initiator_user_id VARCHAR(255) NOT NULL,
    slack_agent_desired_status VARCHAR(128) NOT NULL DEFAULT 'processing',
    slack_agent_observed_status VARCHAR(128) NOT NULL DEFAULT '',
    slack_session_observed_status VARCHAR(128) NOT NULL DEFAULT '',
    slack_publication_revision BIGINT NOT NULL DEFAULT 1,
    slack_publication_sent_revision BIGINT NOT NULL DEFAULT 0,
    slack_publication_claimed_revision BIGINT NOT NULL DEFAULT 0,
    slack_publication_state VARCHAR(128) NOT NULL DEFAULT 'ready',
    slack_publication_attempts INT NOT NULL DEFAULT 0,
    slack_publication_next_attempt_at DATETIME(3) NULL,
    slack_publication_refresh_at DATETIME(3) NULL,
    slack_publication_rate_limited_until DATETIME(3) NULL,
    slack_publication_first_sent_at DATETIME(3) NULL,
    slack_publication_last_sent_at DATETIME(3) NULL,
    slack_publication_reason VARCHAR(255) NOT NULL DEFAULT '',
    slack_publication_warning TEXT NOT NULL DEFAULT (''),
    created_at DATETIME(3) NOT NULL,
    updated_at DATETIME(3) NOT NULL,
    UNIQUE (initial_integration_id, slack_channel_id, slack_thread_root_message_id),
    FOREIGN KEY (initial_integration_id) REFERENCES integration(id),
    FOREIGN KEY (latest_status_job_id) REFERENCES gateway_job(id),
    FOREIGN KEY (status_source_job_id) REFERENCES gateway_job(id),
    INDEX slack_agent_session_publication_due (initial_integration_id, slack_publication_state, slack_publication_next_attempt_at, slack_publication_last_sent_at),
    INDEX slack_agent_session_publication_latest_status_job (latest_status_job_id),
    INDEX slack_agent_session_publication_status_source_job (status_source_job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;
        CALL set_schema_version(1);
    END IF;
END $$
CALL schema_upgrade_1() $$
DROP PROCEDURE schema_upgrade_1 $$
DELIMITER ;
