package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	datlystore "github.com/viant/agently-slack-gateway/internal/storage/datly"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/agently-slack-gateway/migrations"
	"github.com/viant/datly/bootstrap/connector"
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
	inserted, err := s.Enqueue(ctx, storetest.Message("kept", "thread", "alice"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("expected a persisted event")
	}
	for _, operation := range []func(context.Context) error{s.Migrate, s.CheckSchema, s.Migrate} {
		storetest.Must(t, operation(ctx))
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
	if _, err := s.Enqueue(ctx, storetest.Message("blocked", "thread", "alice")); err == nil {
		t.Fatal("business work accepted after failed schema verification")
	}
	var count int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM inbox_jobs").Scan(&count))
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
	s := fixture(t).Store.(*Store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runtime := s.Store
	for _, operation := range []func(context.Context) error{s.Migrate, s.CheckSchema} {
		if err := operation(ctx); err == nil {
			t.Fatal("canceled schema operation succeeded")
		}
		if s.Store != runtime || s.db != nil {
			t.Fatal("canceled schema operation replaced the running pool")
		}
	}
	inserted, err := s.Enqueue(context.Background(), storetest.Message("after-cancel", "thread", "alice"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("canceled maintenance prevented subsequent business work")
	}
}

// This fixture uses the original schema and SQL INSERT shape, without creating
// its rows through the new runtime. Existing durable files keep their IDs,
// migration history, payloads and queue state when Datly takes over.
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
	_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(1,?,100)", list[0].checksum)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO inbox_jobs(id,app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,request_id,state,created_at,updated_at)
 VALUES(37,'A','T','C','old-thread','old-event','alice','1.001','original question 🙂',0,'original-request','pending',100,100)`)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO request_status(job_id,updated_at) VALUES(37,100)")
	storetest.Must(t, err)
	storetest.Must(t, db.Close())
	s, err := Open(ctx, path, false)
	storetest.Must(t, err)
	defer func() { s.Close() }()
	if s.Created() {
		t.Fatal("existing file reported as newly created")
	}
	storetest.Must(t, s.CheckSchema(ctx))
	job, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil || job.ID != 37 || job.Text != "original question 🙂" || job.RequestID != "original-request" {
		t.Fatalf("existing job was not preserved: %+v", job)
	}
	inserted, err := s.Enqueue(ctx, storetest.Message("new-event", "new-thread", "bob"))
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("new event was not persisted")
	}
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	// Existing files remain immediately usable without an explicit schema call,
	// just as the established Open API permits for offline callers.
	job, err = s.ClaimJob(ctx)
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
