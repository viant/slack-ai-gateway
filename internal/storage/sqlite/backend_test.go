package sqlite

import (
	"context"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestBackendOutageIntakeRollsBackWithReply(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	db := fixtureDB(t, s)
	_, err := db.Exec("CREATE TRIGGER reject_notice BEFORE INSERT ON slack_outgoing_message WHEN (SELECT slack_thread_root_message_id FROM gateway_job WHERE id=NEW.job_id)='offline' BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	m := storetest.Message("offline", "offline", "alice")
	m.BackendReason = "backend_unavailable"
	if inserted, err := s.Enqueue(ctx, m); err == nil || inserted {
		t.Fatalf("failed local reply was acknowledged: inserted=%v, err=%v", inserted, err)
	}
	for _, table := range []string{"gateway_job", "slack_outgoing_message"} {
		var n int
		storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n))
		if n != 0 {
			t.Fatalf("partial outage intake commit in %s: %d rows", table, n)
		}
	}
	_, err = db.Exec("DROP TRIGGER reject_notice")
	storetest.Must(t, err)
	inserted, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	if !inserted {
		t.Fatal("rolled-back event could not be retried")
	}
}

func TestBackendOutageBatchRollsBackAllLocalReplies(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	db := fixtureDB(t, s)
	for _, event := range []string{"first", "second"} {
		inserted, err := s.Enqueue(ctx, storetest.Message(event, event, "alice"))
		storetest.Must(t, err)
		if !inserted {
			t.Fatal("event not inserted")
		}
	}
	_, err := db.Exec("CREATE TRIGGER reject_notice BEFORE INSERT ON slack_outgoing_message WHEN (SELECT slack_thread_root_message_id FROM gateway_job WHERE id=NEW.job_id)='second' BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	if err := s.FailUnstarted(ctx, "backend_unavailable"); err == nil {
		t.Fatal("expected reply write failure")
	}
	var pending, replies int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE gateway_execution_state='pending' AND gateway_execution_reason='' AND gateway_execution_result_text IS NULL").Scan(&pending))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&replies))
	if pending != 2 || replies != 0 {
		t.Fatalf("partial local failure batch committed: pending=%d, replies=%d", pending, replies)
	}
	_, err = db.Exec("DROP TRIGGER reject_notice")
	storetest.Must(t, err)
	storetest.Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE gateway_execution_state='completed' AND gateway_execution_reason='backend_unavailable'").Scan(&pending))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&replies))
	if pending != 2 || replies != 2 {
		t.Fatalf("retried batch lost terminal jobs or replies: completed=%d, replies=%d", pending, replies)
	}
}
