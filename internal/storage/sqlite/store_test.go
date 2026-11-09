package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func fixture(t *testing.T) storetest.Fixture {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	t.Cleanup(func() {
		if s != nil {
			storetest.Must(t, s.Close())
		}
	})
	return storetest.Fixture{Store: s, Reopen: func() gateway.Store {
		storetest.Must(t, s.Close())
		s = nil
		var err error
		s, err = Open(ctx, path, false)
		storetest.Must(t, err)
		storetest.Must(t, s.CheckSchema(ctx))
		storetest.Must(t, s.Recover(ctx))
		return s
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
		storetest.Must(t, s.db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&n))
		if n != 1 {
			t.Fatal("initial schema applied more than once")
		}
		storetest.Must(t, s.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").Scan(&n))
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
		{"missing_table", "DROP TABLE outbox_messages"},
		{"missing_index", "DROP INDEX inbox_thread"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t).Store.(*Store)
			_, err := s.db.Exec(tc.sql)
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
		_, err = s.db.Exec("CREATE TABLE foreign_data(id INTEGER)")
		storetest.Must(t, err)
		if err = s.Migrate(ctx); err == nil {
			t.Fatal("foreign database migrated")
		}
	})
}

func TestResultAndOutboxRollbackTogether(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	// Failure on the second INSERT proves both the job update and first part roll back.
	_, err = s.db.Exec("CREATE TRIGGER reject_part BEFORE INSERT ON outbox_messages WHEN NEW.part=1 BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	if err = s.Complete(ctx, j.ID, "", "result", []gateway.ReplyPart{{Text: "one", Format: gateway.ReplyPlain}, {Text: "two", Format: gateway.ReplyPlain}}); err == nil {
		t.Fatal("expected injected write failure")
	}
	var state, result string
	var n int
	storetest.Must(t, s.db.QueryRow("SELECT state,result_text FROM inbox_jobs WHERE id=?", j.ID).Scan(&state, &result))
	storetest.Must(t, s.db.QueryRow("SELECT COUNT(*) FROM outbox_messages").Scan(&n))
	if state != "processing" || result != "" || n != 0 {
		t.Fatal("partial result/outbox commit")
	}
	_, err = s.db.Exec("DROP TRIGGER reject_part")
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
	// Omitting format must retain the plain-text default.
	_, err = s.db.ExecContext(ctx, `INSERT INTO inbox_jobs
	(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,request_id,state,created_at,updated_at)
	VALUES('A','T','C','old-thread','old','alice','1','old',0,'old-request','completed',0,0)`)
	storetest.Must(t, err)
	oldText := "&lt;literal&gt; **plain** &amp; 🙂"
	_, err = s.db.ExecContext(ctx, `INSERT INTO outbox_messages(job_id,workspace_id,channel_id,root_thread_ts,part,content,state,created_at,updated_at) VALUES(1,'T','C','old-thread',0,?,'pending',0,0)`, oldText)
	storetest.Must(t, err)
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	storetest.Must(t, s.CheckSchema(ctx))
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != oldText || d.Format != gateway.ReplyPlain {
		t.Fatal("pending plain reply changed content or delivery format")
	}
	_, err = s.Enqueue(ctx, storetest.Message("new", "new-thread", "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	newText := "# Heading\n\n**bold** and 🙂"
	storetest.Must(t, s.Complete(ctx, j.ID, "", newText, gateway.RenderMarkdown(newText)))
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	d, err = s.ClaimDelivery(ctx)
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
	s := fixture(t).Store.(*Store)
	_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	_, err = s.BindJob(ctx, j.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	var author, principal, owner string
	storetest.Must(t, s.db.QueryRow("SELECT author_id,principal_id,owner_id FROM inbox_jobs WHERE id=?", j.ID).Scan(&author, &principal, &owner))
	if author != "alice" || principal != "shared" || owner != "shared" {
		t.Fatal("audit identity conflated with execution identity")
	}
	if _, err = s.db.Exec("UPDATE inbox_jobs SET binding_id=99999 WHERE id=?", j.ID); err == nil {
		t.Fatal("foreign key not enforced")
	}
	if _, err = s.db.Exec("UPDATE inbox_jobs SET state='invalid' WHERE id=?", j.ID); err == nil {
		t.Fatal("state constraint not enforced")
	}
}
