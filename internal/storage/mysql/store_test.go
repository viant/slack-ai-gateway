package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
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
	owner, err := Open(ctx, dsn, nil)
	storetest.Must(t, err)
	storetest.RegisterDefault(t, owner)
	s := storetest.Scope(t, owner, "default").(*Store)
	t.Cleanup(func() {
		if owner != nil {
			storetest.Must(t, owner.Close())
		}
	})
	f := storetest.Fixture{Store: s, Root: func() storage.Store { return owner }, Reopen: func() gateway.Store {
		storetest.Must(t, owner.Close())
		owner = nil
		var err error
		owner, err = Open(ctx, dsn, nil)
		storetest.Must(t, err)
		storetest.Must(t, owner.Recover(ctx))
		s = storetest.Scope(t, owner, "default").(*Store)
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

// Older durable IDs may predate the current owner (for example after recovery).
// They must not overlap a later-ID execution that is already active. New
// same-process intake is serialized by the physical-thread owner.
func TestClaimsSerializeThreadWhenNativeIDsCommitOutOfOrder(t *testing.T) {
	s, db := fixtureDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, "ALTER TABLE gateway_job AUTO_INCREMENT=100")
	storetest.Must(t, err)
	s.StartThreadOwnership()
	owner := s.ForThread(gateway.PhysicalThreadKey{AppID: "A", WorkspaceID: "T", ChannelID: "C", RootTS: "thread"}).(*Store)
	message := storetest.Message("first_commit", "thread", "alice")
	message.StatusMode = gateway.StatusMessage
	inserted, err := owner.Enqueue(ctx, message)
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("event was not inserted")
	}
	running, err := owner.ClaimJob(ctx)
	storetest.Must(t, err)
	if running == nil || running.ID < 100 {
		t.Fatalf("missing active high-ID fixture: %+v", running)
	}
	// This fixture write is not a second gateway writer. It models historical
	// lower-ID work becoming visible; runtime mutations below all use the owner.
	var integration int64
	storetest.Must(t, db.QueryRowContext(ctx, "SELECT integration_id FROM gateway_job WHERE id=?", running.ID).Scan(&integration))
	now := time.Now().UTC().Truncate(time.Millisecond)
	_, err = db.ExecContext(ctx, `INSERT INTO gateway_job(id,integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
 VALUES(37,?,'C','thread','delayed','alice','1.000037','older durable prompt',0,'earlier-durable-request','pending',?,?,?)`, integration, now, now, now)
	storetest.Must(t, err)
	status, err := owner.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if status == nil || status.JobID != 37 || status.Phase != "queued" {
		t.Fatalf("late lower-ID job status must be queued: %+v", status)
	}
	confirmStatus(t, owner, status, time.Now())
	overlap, err := owner.ClaimJob(ctx)
	storetest.Must(t, err)
	if overlap != nil {
		t.Fatalf("thread overlap: claimed %d while %d processing", overlap.ID, running.ID)
	}
	binding, err := owner.BindJob(ctx, running.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	storetest.Must(t, owner.BeginCreate(ctx, binding.ID))
	storetest.Must(t, owner.SetConversation(ctx, binding.ID, "native-order-conversation"))
	storetest.Must(t, owner.BeginQuery(ctx, running.ID))
	storetest.Must(t, owner.SchedulePoll(ctx, running.ID, time.Now().Add(-time.Second)))
	poll, err := owner.ClaimJob(ctx)
	storetest.Must(t, err)
	if poll == nil || poll.ID != running.ID {
		t.Fatalf("active poll blocked by older pending job: %+v", poll)
	}
	storetest.Must(t, owner.Complete(ctx, running.ID, running.GatewayRequestID, "done", []gateway.ReplyPart{{Text: "done", Format: gateway.ReplyPlain}}))
	next, err := owner.ClaimJob(ctx)
	storetest.Must(t, err)
	if next == nil || next.ID != 37 {
		t.Fatalf("older durable job did not resume: %+v", next)
	}
}
