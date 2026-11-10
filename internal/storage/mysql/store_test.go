package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func fixtureDSN(t *testing.T) string { t.Helper(); return storetest.MySQLDSN(t) }

func fixture(t *testing.T) storetest.Fixture {
	t.Helper()
	f, _ := fixtureWithDB(t)
	return f
}

func fixtureWithDB(t *testing.T) (storetest.Fixture, *sql.DB) {
	t.Helper()
	dsn := fixtureDSN(t)
	ctx := context.Background()
	db, err := sql.Open("mysql", dsn)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	s, err := Open(ctx, dsn, nil)
	storetest.Must(t, err)
	t.Cleanup(func() {
		if s != nil {
			storetest.Must(t, s.Close())
		}
	})
	f := storetest.Fixture{Store: s, Reopen: func() gateway.Store {
		storetest.Must(t, s.Close())
		s = nil
		var err error
		s, err = Open(ctx, dsn, nil)
		storetest.Must(t, err)
		storetest.Must(t, s.Recover(ctx))
		return s
	}}
	return f, db
}

func fixtureDB(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	f, db := fixtureWithDB(t)
	return f.Store.(*Store), db
}

func TestStoreContract(t *testing.T) { storetest.Run(t, fixture) }

// Native transient identity reservation precedes the real insert. Commit order
// may therefore differ from ID order; an older ID becoming visible must never
// overlap another active query in the same Slack thread.
func TestClaimsSerializeThreadWhenNativeIDsCommitOutOfOrder(t *testing.T) {
	s, db := fixtureDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var database string
	storetest.Must(t, db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&database))
	readyKey, pauseKey := database+"_ready", database+"_pause"
	connection, err := db.Conn(ctx)
	storetest.Must(t, err)
	defer connection.Close()
	var locked int
	storetest.Must(t, connection.QueryRowContext(ctx, "SELECT GET_LOCK(?,0)", pauseKey).Scan(&locked))
	if locked != 1 {
		t.Fatal("cannot acquire test pause lock")
	}
	defer connection.ExecContext(context.Background(), "SELECT RELEASE_LOCK(?)", pauseKey)
	// The allocator's transient INSERT has NEW.id=0, while the real write
	// carries its final native ID. Only the latter is paused by this fixture.
	_, err = db.ExecContext(ctx, `CREATE TRIGGER pause_native_insert BEFORE INSERT ON inbox_jobs FOR EACH ROW
    BEGIN
      IF NEW.event_id='delayed' AND NEW.id <> 0 THEN
        SET @signal_ready = GET_LOCK('`+readyKey+`',0);
        SET @wait_for_release = GET_LOCK('`+pauseKey+`',15);
        DO RELEASE_LOCK('`+readyKey+`');
        DO RELEASE_LOCK('`+pauseKey+`');
      END IF;
    END`)
	storetest.Must(t, err)
	delayed := make(chan error, 1)
	delayedMessage := storetest.Message("delayed", "thread", "alice")
	delayedMessage.StatusMode = gateway.StatusMessage
	go func() { _, e := s.Enqueue(ctx, delayedMessage); delayed <- e }()
	for {
		var owner *int64
		storetest.Must(t, db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", readyKey).Scan(&owner))
		if owner != nil {
			break
		}
		select {
		case e := <-delayed:
			t.Fatalf("delayed insert did not reach native pause: %v", e)
		case <-ctx.Done():
			t.Fatal("native insert did not reach pause")
		case <-time.After(5 * time.Millisecond):
		}
	}
	firstMessage := storetest.Message("first_commit", "thread", "alice")
	firstMessage.StatusMode = gateway.StatusMessage
	inserted, err := s.Enqueue(ctx, firstMessage)
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("second event was not inserted")
	}
	running, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if running == nil || running.EventID != "first_commit" {
		t.Fatalf("expected first committed event, got %+v", running)
	}
	storetest.Must(t, connection.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", pauseKey).Scan(&locked))
	storetest.Must(t, <-delayed)
	var earlierID int64
	storetest.Must(t, db.QueryRowContext(ctx, "SELECT id FROM inbox_jobs WHERE event_id='delayed'").Scan(&earlierID))
	if earlierID >= running.ID {
		t.Fatal("fixture did not reverse native ID and commit ordering")
	}
	status, err := s.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if status == nil || status.JobID != earlierID || status.Phase != "queued" {
		t.Fatalf("late lower-ID job status must be queued: %+v", status)
	}
	confirmStatus(t, s, status, time.Now())
	overlap, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if overlap != nil {
		t.Fatalf("thread overlap: job %d claimed while later ID %d is processing", overlap.ID, running.ID)
	}
	binding, err := s.BindJob(ctx, running.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	storetest.Must(t, s.BeginCreate(ctx, binding.ID))
	storetest.Must(t, s.SetConversation(ctx, binding.ID, "native-order-conversation"))
	storetest.Must(t, s.BeginQuery(ctx, running.ID))
	storetest.Must(t, s.SchedulePoll(ctx, running.ID, time.Now().Add(-time.Second)))
	poll, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if poll == nil || poll.ID != running.ID {
		t.Fatalf("active query poll blocked by late lower-ID pending job: %+v", poll)
	}
	storetest.Must(t, s.Complete(ctx, running.ID, running.RequestID, "done", []gateway.ReplyPart{{Text: "done", Format: gateway.ReplyPlain}}))
	next, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if next == nil || next.ID != earlierID {
		t.Fatalf("older committed job did not resume: %+v", next)
	}
}
