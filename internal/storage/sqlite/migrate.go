package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

	"github.com/viant/slack-ai-gateway/migrations"
)

type migration struct {
	version       int
	sql, checksum string
}

func migrationList() ([]migration, error) {
	paths, err := fs.Glob(migrations.SQLite, "sqlite/*.sql")
	if err != nil {
		return nil, err
	}
	var result []migration
	for _, path := range paths {
		name := strings.TrimPrefix(path, "sqlite/")
		v, err := strconv.Atoi(strings.SplitN(name, "_", 2)[0])
		if err != nil || v != len(result)+1 {
			return nil, fmt.Errorf("invalid migration sequence")
		}
		body, err := migrations.SQLite.ReadFile(path)
		if err != nil {
			return nil, err
		}
		result = append(result, migration{version: v, sql: string(body), checksum: fmt.Sprintf("%x", sha256.Sum256(body))})
	}
	return result, nil
}

// applied validates history including checksums, rejecting unknown/newer schema.
func applied(ctx context.Context, tx *sql.Tx, available []migration) (int, error) {
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'").Scan(&n); err != nil {
		return 0, err
	}
	if n == 0 {
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'").Scan(&n); err != nil {
			return 0, err
		}
		if n != 0 {
			return 0, fmt.Errorf("database is not an empty gateway database")
		}
		return 0, nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT version, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n = 0
	for rows.Next() {
		var v int
		var checksum string
		if err := rows.Scan(&v, &checksum); err != nil {
			return 0, err
		}
		if n >= len(available) || v != available[n].version || checksum != available[n].checksum {
			return 0, fmt.Errorf("incompatible migration history")
		}
		n++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, fmt.Errorf("incomplete migration history")
	}
	return n, nil
}

func (s *Store) Migrate(ctx context.Context) error {
	if err := s.prepareSchema(ctx); err != nil {
		return err
	}
	list, err := migrationList()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := applied(ctx, tx, list)
	if err != nil {
		return err
	}
	for _, m := range list[n:] {
		if _, err = tx.ExecContext(ctx, m.sql); err != nil {
			return fmt.Errorf("migration %d: %w", m.version, err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(?,?,?)", m.version, m.checksum, time.Now().UTC().Truncate(time.Millisecond)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.CheckSchema(ctx)
}

func (s *Store) CheckSchema(ctx context.Context) error {
	if err := s.prepareSchema(ctx); err != nil {
		return err
	}
	if err := s.checkSchema(ctx); err != nil {
		return err
	}
	return s.startRuntime(ctx)
}

func (s *Store) checkSchema(ctx context.Context) error {
	list, err := migrationList()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	n, err := applied(ctx, tx, list)
	if err != nil {
		return err
	}
	if n != len(list) {
		return fmt.Errorf("schema is not current; run migrate explicitly")
	}
	// Catch missing tables/columns without performing any DDL.
	for _, query := range []string{
		"SELECT version,checksum,applied_at FROM schema_migrations LIMIT 0",
		"SELECT id,name,active,slack_app_id,slack_workspace_id,backend_type,backend_endpoint,backend_agent_id,backend_principal_id,created_at,updated_at FROM integration LIMIT 0",
		"SELECT id,integration_id,slack_channel_id,slack_thread_root_message_id,backend_conversation_id,gateway_access_mode,backend_principal_id,gateway_binding_owner_id,gateway_binding_state,backend_protocol_messages,backend_protocol_state,created_at,updated_at,last_activity_at,closed_at FROM conversation_binding LIMIT 0",
		"SELECT id,gateway_request_id,integration_id,conversation_binding_id,slack_event_id,slack_channel_id,slack_conversation_type,slack_thread_root_message_id,slack_prompt_message_id,slack_prompt_author_id,prompt_text,prompt_has_attachments,gateway_execution_state,gateway_execution_reason,gateway_execution_outcome,gateway_execution_result_text,gateway_execution_claim_count,backend_submission_started_at,backend_next_poll_at,backend_poll_attempts,backend_poll_ready,backend_observed_state,backend_observed_at,backend_observation_uncertain,slack_progress_mode,slack_progress_message_id,slack_progress_state,slack_progress_phase,slack_progress_reason,slack_progress_revision,slack_progress_sent_revision,slack_progress_attempts,slack_progress_next_attempt_at,slack_progress_refresh_at,slack_progress_rate_limited_until,slack_progress_first_sent_at,slack_progress_last_sent_at,created_at,gateway_execution_updated_at,gateway_status_updated_at,gateway_execution_finished_at,content_purged_at FROM gateway_job LIMIT 0",
		"SELECT id,job_id,slack_message_index,slack_message_text,slack_message_kind,slack_delivery_state,slack_delivery_attempts,slack_delivery_next_attempt_at,slack_message_id,created_at,updated_at,slack_message_format FROM slack_outgoing_message LIMIT 0",
		"SELECT id,outgoing_message_id,resolution_action,slack_message_id,duplicate_delivery_risk_accepted,created_at FROM slack_delivery_resolution LIMIT 0",
		"SELECT id,initial_integration_id,slack_channel_id,slack_thread_root_message_id,latest_status_job_id,status_source_job_id,slack_initiator_user_id,slack_agent_desired_status,slack_agent_observed_status,slack_session_observed_status,slack_publication_revision,slack_publication_sent_revision,slack_publication_claimed_revision,slack_publication_state,slack_publication_attempts,slack_publication_next_attempt_at,slack_publication_refresh_at,slack_publication_rate_limited_until,slack_publication_first_sent_at,slack_publication_last_sent_at,slack_publication_reason,slack_publication_warning,created_at,updated_at FROM slack_agent_session_publication LIMIT 0",
	} {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("incompatible gateway schema: %w", err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	for _, name := range []string{"integration_name_active", "integration_slack", "gateway_job_pending", "gateway_job_thread", "slack_outgoing_message_pending", "slack_outgoing_message_job_state", "slack_outgoing_message_cooldown", "gateway_job_progress_due", "gateway_job_progress_cooldown", "slack_agent_session_publication_due", "gateway_job_retention_content", "gateway_job_binding_finished", "conversation_binding_last_activity", "slack_delivery_resolution_outgoing", "slack_agent_session_publication_latest_status_job", "slack_agent_session_publication_status_source_job"} {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", name).Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("missing gateway index %s", name)
		}
	}
	return nil
}
