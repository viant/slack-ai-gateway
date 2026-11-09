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

	"github.com/viant/agently-slack-gateway/migrations"
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
		if _, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(?,?,?)", m.version, m.checksum, time.Now().UnixMilli()); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.CheckSchema(ctx)
}

func (s *Store) CheckSchema(ctx context.Context) error {
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
		"SELECT workspace_id,channel_id,root_thread_ts,latest_job_id,source_job_id,initiator_user_id,desired_status,agent_status,session_status,revision,sent_revision,claimed_revision,state,attempts,next_attempt_at,refresh_at,rate_limited_until,first_sent_at,last_sent_at,reason,warning,updated_at FROM agent_session_publications LIMIT 0",
		"SELECT version,checksum,applied_at FROM schema_migrations LIMIT 0",
		"SELECT " + jobColumns + ",poll_ready,attempts,next_attempt_at,created_at,updated_at FROM inbox_jobs LIMIT 0",
		"SELECT " + bindingColumns + ",created_at,updated_at FROM conversation_bindings LIMIT 0",
		"SELECT " + deliveryColumns + ",next_attempt_at,created_at,updated_at FROM outbox_messages LIMIT 0",
		"SELECT id,delivery_id,action,slack_ts,duplicate_risk_accepted,created_at FROM delivery_resolutions LIMIT 0",
		"SELECT mode,job_id,slack_ts,observed_state,observed_at,observation_uncertain,outcome,phase,revision,sent_revision,state,attempts,next_attempt_at,refresh_at,rate_limited_until,first_sent_at,last_sent_at,reason,updated_at FROM request_status LIMIT 0",
	} {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("incompatible gateway schema: %w", err)
		}
		if err = rows.Close(); err != nil {
			return err
		}
	}
	for _, name := range []string{"inbox_pending", "inbox_thread", "outbox_pending", "outbox_thread", "request_status_due", "agent_session_due"} {
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
