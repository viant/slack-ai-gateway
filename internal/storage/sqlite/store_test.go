package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// Fixture inspection uses its own test-only connection. Production schema
// preparation hands its only pool over to Datly before business operations.
type scopedFixture struct {
	storage.Store
	owner *Store
	id    int64
}

func (s *scopedFixture) Migrate(ctx context.Context) error {
	if err := s.owner.Migrate(ctx); err != nil {
		return err
	}
	s.Store = s.owner.Scope(s.id)
	return nil
}

func (s *scopedFixture) CheckSchema(ctx context.Context) error {
	if err := s.owner.CheckSchema(ctx); err != nil {
		return err
	}
	s.Store = s.owner.Scope(s.id)
	return nil
}

func (s *scopedFixture) Close() error { return s.owner.Close() }

func fixtureDB(t *testing.T, source any) *sql.DB {
	t.Helper()
	var s *Store
	switch value := source.(type) {
	case *Store:
		s = value
	case *scopedFixture:
		s = value.owner
	default:
		t.Fatalf("unexpected fixture store %T", source)
	}
	db, err := sql.Open("sqlite", s.dsn)
	storetest.Must(t, err)
	db.SetMaxOpenConns(1)
	storetest.Must(t, db.PingContext(context.Background()))
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	return db
}

func fixture(t *testing.T) storetest.Fixture {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	integrationID := storetest.RegisterDefault(t, s)
	t.Cleanup(func() {
		if s != nil {
			storetest.Must(t, s.Close())
		}
	})
	return storetest.Fixture{Store: &scopedFixture{Store: s.Scope(integrationID), owner: s, id: integrationID}, Root: func() storage.Store { return s }, Reopen: func() gateway.Store {
		storetest.Must(t, s.Close())
		s = nil
		var err error
		s, err = Open(ctx, path, false)
		storetest.Must(t, err)
		storetest.Must(t, s.CheckSchema(ctx))
		storetest.Must(t, s.Recover(ctx))
		return &scopedFixture{Store: s.Scope(integrationID), owner: s, id: integrationID}
	}}
}

func TestStoreContract(t *testing.T) { storetest.Run(t, fixture) }

func TestMigrations(t *testing.T) {
	ctx := context.Background()
	t.Run("explicit_and_repeatable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "gateway.sqlite")
		s, err := Open(ctx, path, true)
		storetest.Must(t, err)
		defer s.Close()
		if err = s.CheckSchema(ctx); err == nil {
			t.Fatal("unmigrated database accepted")
		}
		storetest.Must(t, s.Migrate(ctx))
		storetest.Must(t, s.Migrate(ctx))
		storetest.Must(t, s.CheckSchema(ctx))
		var n int
		storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&n))
		if n != 1 {
			t.Fatal("initial schema applied more than once")
		}
		storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&n))
		if n != 7 {
			t.Fatalf("expected 7 tables, got %d", n)
		}
		info, err := os.Stat(path)
		storetest.Must(t, err)
		if info.Mode().Perm()&0077 != 0 {
			t.Fatal("database is readable by other users")
		}
	})
	for _, tc := range []struct{ name, sql string }{
		{"checksum", "UPDATE schema_migrations SET checksum='changed'"},
		{"future_version", "UPDATE schema_migrations SET version=99 WHERE version=1"},
		{"missing_table", "DROP TABLE slack_outgoing_message"},
		{"missing_index", "DROP INDEX gateway_job_thread"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t).Store.(*scopedFixture)
			_, err := fixtureDB(t, s).Exec(tc.sql)
			storetest.Must(t, err)
			if err = s.CheckSchema(ctx); err == nil {
				t.Fatal("incompatible schema accepted")
			}
		})
	}
	t.Run("foreign_database_rejected", func(t *testing.T) {
		s, err := Open(ctx, filepath.Join(t.TempDir(), "foreign.sqlite"), true)
		storetest.Must(t, err)
		defer s.Close()
		_, err = fixtureDB(t, s).Exec("CREATE TABLE foreign_data(id INTEGER)")
		storetest.Must(t, err)
		if err = s.Migrate(ctx); err == nil {
			t.Fatal("foreign database migrated")
		}
	})
}

func TestResultAndOutboxRollbackTogether(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	// Failure on the second INSERT proves both the job update and first part roll back.
	_, err = fixtureDB(t, s).Exec("CREATE TRIGGER reject_part BEFORE INSERT ON slack_outgoing_message WHEN NEW.slack_message_index=1 BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	if err = s.Complete(ctx, j.ID, "", "result", []gateway.ReplyPart{{Text: "one", Format: gateway.ReplyPlain}, {Text: "two", Format: gateway.ReplyPlain}}); err == nil {
		t.Fatal("expected injected write failure")
	}
	var state string
	var result sql.NullString
	var n int
	storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT gateway_execution_state,gateway_execution_result_text FROM gateway_job WHERE id=?", j.ID).Scan(&state, &result))
	storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&n))
	if state != "processing" || result.Valid || n != 0 {
		t.Fatal("partial result/outbox commit")
	}
	_, err = fixtureDB(t, s).Exec("DROP TRIGGER reject_part")
	storetest.Must(t, err)
	storetest.Must(t, s.Complete(ctx, j.ID, "", "result", []gateway.ReplyPart{{Text: "one", Format: gateway.ReplyPlain}, {Text: "two", Format: gateway.ReplyPlain}}))
}

func TestReplyFormatsSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	t.Cleanup(func() { s.Close() })
	storetest.Must(t, s.Migrate(ctx))
	storetest.RegisterDefault(t, s)
	// Omitting format must retain the plain-text default.
	_, err = fixtureDB(t, s).ExecContext(ctx, `INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
	VALUES ((SELECT id FROM integration WHERE name='default' AND active=1),'C','old-thread','old','alice','1','old',0,'old-request','completed','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000')`)
	storetest.Must(t, err)
	oldText := "&lt;literal&gt; **plain** &amp; 🙂"
	_, err = fixtureDB(t, s).ExecContext(ctx, `INSERT INTO slack_outgoing_message(job_id,slack_message_index,slack_message_text,slack_delivery_state,created_at,updated_at)
	VALUES (1,0,?,'pending','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000')`, oldText)
	storetest.Must(t, err)
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	storetest.Must(t, s.CheckSchema(ctx))
	d, err := storetest.Scope(t, s, "default").ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != oldText || d.Format != gateway.ReplyPlain {
		t.Fatal("pending plain reply changed content or delivery format")
	}
	_, err = storetest.Scope(t, s, "default").Enqueue(ctx, storetest.Message("new", "new-thread", "alice"))
	storetest.Must(t, err)
	j, err := storetest.Scope(t, s, "default").ClaimJob(ctx)
	storetest.Must(t, err)
	newText := "# Heading\n\n**bold** and 🙂"
	storetest.Must(t, storetest.Scope(t, s, "default").Complete(ctx, j.ID, "", newText, gateway.RenderMarkdown(newText)))
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	d, err = storetest.Scope(t, s, "default").ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != newText || d.Format != gateway.ReplyMarkdown {
		t.Fatal("new native Markdown format did not survive restart")
	}
}

func TestSingleActiveInstanceAndDurablePath(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "with spaces.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	if !s.Created() {
		t.Fatal("new database not reported as created")
	}
	if second, err := Open(ctx, path, false); err == nil {
		second.Close()
		t.Fatal("second instance accepted")
	}
	storetest.Must(t, s.Close())
	for _, create := range []bool{false, true} {
		s, err = Open(ctx, path, create)
		storetest.Must(t, err)
		if s.Created() {
			t.Fatal("existing database reported as created")
		}
		storetest.Must(t, s.Close())
	}
	for _, invalid := range []string{"", ":memory:", "file:memory?mode=memory"} {
		if s, err := Open(ctx, invalid, true); err == nil {
			s.Close()
			t.Fatal("non-durable path accepted")
		}
	}
	if s, err := Open(ctx, filepath.Join(t.TempDir(), "missing.sqlite"), false); err == nil {
		s.Close()
		t.Fatal("open without creation accepted missing storage")
	}
}

func TestAuditAndConstraints(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	_, err = s.BindJob(ctx, j.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	var author, principal, owner string
	storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT j.slack_prompt_author_id,b.backend_principal_id,b.gateway_binding_owner_id FROM gateway_job j JOIN conversation_binding b ON b.id=j.conversation_binding_id WHERE j.id=?", j.ID).Scan(&author, &principal, &owner))
	if author != "alice" || principal != "shared" || owner != "shared" {
		t.Fatal("audit identity conflated with execution identity")
	}
	if _, err = fixtureDB(t, s).Exec("UPDATE gateway_job SET conversation_binding_id=99999 WHERE id=?", j.ID); err == nil {
		t.Fatal("foreign key not enforced")
	}
	if _, err = fixtureDB(t, s).Exec("UPDATE gateway_job SET gateway_execution_state='invalid' WHERE id=?", j.ID); err == nil {
		t.Fatal("state constraint not enforced")
	}
}
