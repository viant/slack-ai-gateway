package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
)

func TestRuntimeIndexMigrationPreservesVersionOneRowsAndChecksum(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "version-one.sqlite")
	db, err := sql.Open("sqlite", path)
	storetest.Must(t, err)
	initial, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, string(initial))
	storetest.Must(t, err)
	list, err := migrationList()
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(1,?,'2026-01-01 00:00:00.123')", list[0].checksum)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO integration(id,name,active,slack_app_id,slack_workspace_id,backend_type,backend_endpoint,backend_agent_id,backend_principal_id,created_at,updated_at)
 VALUES(7,'kept',1,'A','T','agently','https://backend.example.test','steward','shared','2026-01-01 00:00:00.123','2026-01-01 00:00:00.123')`)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO gateway_job(id,integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
 VALUES(42,7,'C','thread','kept-event','alice','1.000001','kept Unicode 🙂',0,'kept-request','pending','2026-01-01 00:00:00.123','2026-01-01 00:00:00.123','2026-01-01 00:00:00.123')`)
	storetest.Must(t, err)
	storetest.Must(t, db.Close())
	s, err := Open(ctx, path, false)
	storetest.Must(t, err)
	defer s.Close()
	if err = s.CheckSchema(ctx); err == nil {
		t.Fatal("opening version one silently applied the index migration")
	}
	storetest.Must(t, s.Migrate(ctx))
	inspection := fixtureDB(t, s)
	var id int64
	var text, request, checksum string
	storetest.Must(t, inspection.QueryRowContext(ctx, "SELECT id,prompt_text,gateway_request_id FROM gateway_job WHERE slack_event_id='kept-event'").Scan(&id, &text, &request))
	if id != 42 || text != "kept Unicode 🙂" || request != "kept-request" {
		t.Fatal("index migration changed durable work")
	}
	storetest.Must(t, inspection.QueryRowContext(ctx, "SELECT checksum FROM schema_migrations WHERE version=1").Scan(&checksum))
	if checksum != list[0].checksum {
		t.Fatal("initial migration checksum changed")
	}
	var versions, indexes int
	storetest.Must(t, inspection.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&versions))
	storetest.Must(t, inspection.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN ('gateway_job_state','slack_agent_session_publication_cooldown')").Scan(&indexes))
	if versions != 2 || indexes != 2 {
		t.Fatalf("incomplete additive migration: versions=%d indexes=%d", versions, indexes)
	}
	for _, index := range []struct{ name, columns string }{
		{"gateway_job_state", "gateway_execution_state,integration_id,id"},
		{"slack_agent_session_publication_cooldown", "slack_publication_rate_limited_until,initial_integration_id,id"},
	} {
		var columns string
		storetest.Must(t, inspection.QueryRowContext(ctx, "SELECT GROUP_CONCAT(name, ',') FROM (SELECT name FROM pragma_index_info(?) ORDER BY seqno)", index.name).Scan(&columns))
		if columns != index.columns {
			t.Fatalf("index %s columns=%s, want %s", index.name, columns, index.columns)
		}
	}
	storetest.Must(t, s.Migrate(ctx))
	storetest.Must(t, s.CheckSchema(ctx))
}
