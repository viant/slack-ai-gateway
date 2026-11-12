package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/bootstrap/connector"
	datlystore "github.com/viant/slack-ai-gateway/internal/storage/datly"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
)

func TestNativeOpenUninitializedSQLite(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "foreign"}[foreign], func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "uninitialized.sqlite")
			db, err := sql.Open("sqlite", path)
			storetest.Must(t, err)
			if foreign {
				_, err = db.ExecContext(ctx, "CREATE TABLE foreign_data(id INTEGER)")
				storetest.Must(t, err)
			} else {
				storetest.Must(t, db.PingContext(ctx))
			}
			storetest.Must(t, db.Close())
			s, err := datlystore.Open(ctx, connector.Config{
				Name: "slack", Driver: "sqlite", DSN: path, MaxOpenConns: 1,
			})
			storetest.Must(t, err)
			storetest.Must(t, s.Close())
		})
	}
}

func TestSQLitePoolHandoffKeepsLifetimeLockAndRejectsFailedSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "handoff.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	defer s.Close()
	if s.db != nil || s.Store == nil {
		t.Fatal("Open retained an administration pool or omitted the native runtime")
	}
	storetest.Must(t, s.Migrate(ctx))
	storetest.RegisterDefault(t, s)
	active := storetest.Scope(t, s, "default")
	inserted, err := active.Enqueue(ctx, storetest.Message("kept", "thread", "alice"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("expected a persisted event")
	}
	for _, operation := range []func(context.Context) error{s.Migrate, s.CheckSchema, s.Migrate} {
		storetest.Must(t, operation(ctx))
		active = storetest.Scope(t, s, "default")
		if s.db != nil || s.Store == nil {
			t.Fatal("schema preparation did not hand its pool back to Datly")
		}
		if second, err := Open(ctx, path, false); err == nil {
			second.Close()
			t.Fatal("schema preparation released the lifetime lock")
		}
	}
	db := fixtureDB(t, s)
	list, err := migrationList()
	storetest.Must(t, err)
	_, err = db.Exec("UPDATE schema_migrations SET checksum='invalid'")
	storetest.Must(t, err)
	if err = s.CheckSchema(ctx); err == nil {
		t.Fatal("invalid schema accepted")
	}
	if s.Store != nil {
		t.Fatal("failed schema check left the business runtime enabled")
	}
	if _, err := active.Enqueue(ctx, storetest.Message("blocked", "thread", "alice")); err == nil {
		t.Fatal("business work accepted after failed schema verification")
	}
	var count int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&count))
	if count != 1 {
		t.Fatal("schema verification changed business data")
	}
	if second, err := Open(ctx, path, false); err == nil {
		second.Close()
		t.Fatal("failed verification released the lifetime lock")
	}
	_, err = db.Exec("UPDATE schema_migrations SET checksum=?", list[0].checksum)
	storetest.Must(t, err)
	storetest.Must(t, s.CheckSchema(ctx))
	if s.db != nil || s.Store == nil {
		t.Fatal("repaired schema did not restore the native runtime")
	}
}

func TestCanceledSQLiteOpenReleasesLifetimeLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canceled.sqlite")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if s, err := Open(ctx, path, true); err == nil {
		s.Close()
		t.Fatal("canceled open succeeded")
	}
	s, err := Open(context.Background(), path, false)
	storetest.Must(t, err)
	storetest.Must(t, s.Close())
	if err := s.CheckSchema(context.Background()); err == nil {
		t.Fatal("closed storage reopened itself for schema verification")
	}
}

func TestCanceledSchemaOperationKeepsNativeRuntime(t *testing.T) {
	s := fixture(t).Store.(*scopedFixture)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runtime := s.Store
	for _, operation := range []func(context.Context) error{s.Migrate, s.CheckSchema} {
		if err := operation(ctx); err == nil {
			t.Fatal("canceled schema operation succeeded")
		}
		if s.Store != runtime || s.owner.db != nil {
			t.Fatal("canceled schema operation replaced the running pool")
		}
	}
	inserted, err := s.Enqueue(context.Background(), storetest.Message("after-cancel", "thread", "alice"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("canceled maintenance prevented subsequent business work")
	}
}

// This fixture registers topology through the native graph and inserts business
// rows directly. Reopening preserves IDs, migration history, payloads and queue
// state while callers select their explicit integration scope.
func TestExistingSQLiteFileContinuesThroughDatly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "existing.sqlite")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	storetest.Must(t, err)
	storetest.Must(t, f.Close())
	db, err := sql.Open("sqlite", path)
	storetest.Must(t, err)
	schema, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, string(schema))
	storetest.Must(t, err)
	list, err := migrationList()
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(1,?,'1970-01-01 00:00:00.100')", list[0].checksum)
	storetest.Must(t, err)
	registration, err := Open(ctx, path, false)
	storetest.Must(t, err)
	storetest.Must(t, registration.CheckSchema(ctx))
	storetest.RegisterDefault(t, registration)
	storetest.Must(t, registration.Close())
	_, err = db.ExecContext(ctx, `INSERT INTO gateway_job(integration_id,id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
	VALUES ((SELECT id FROM integration WHERE name='default' AND active=1),37,'C','old-thread','old-event','alice','1.001','original question 🙂',0,'original-request','pending','1970-01-01 00:00:00.100','1970-01-01 00:00:00.100','1970-01-01 00:00:00.000')`)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, "UPDATE gateway_job SET gateway_status_updated_at='1970-01-01 00:00:00.100' WHERE id=37")
	storetest.Must(t, err)
	storetest.Must(t, db.Close())
	s, err := Open(ctx, path, false)
	storetest.Must(t, err)
	defer func() { s.Close() }()
	if s.Created() {
		t.Fatal("existing file reported as newly created")
	}
	storetest.Must(t, s.CheckSchema(ctx))
	job, err := storetest.Scope(t, s, "default").ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil || job.ID != 37 || job.Text != "original question 🙂" || job.GatewayRequestID != "original-request" {
		t.Fatalf("existing job was not preserved: %+v", job)
	}
	inserted, err := storetest.Scope(t, s, "default").Enqueue(ctx, storetest.Message("new-event", "new-thread", "bob"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("new event was not persisted")
	}
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	// Existing files remain immediately usable without an explicit schema call,
	// just as the established Open API permits for offline callers.
	job, err = storetest.Scope(t, s, "default").ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil || job.ID <= 37 || job.AuthorID != "bob" {
		t.Fatalf("native identity or persisted queue lost across reopen: %+v", job)
	}
	inspection := fixtureDB(t, s)
	var checksum string
	storetest.Must(t, inspection.QueryRow("SELECT checksum FROM schema_migrations WHERE version=1").Scan(&checksum))
	if checksum != list[0].checksum {
		t.Fatal("native takeover changed migration history")
	}
}
