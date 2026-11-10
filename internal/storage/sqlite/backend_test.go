package sqlite

import (
	"context"
	"testing"

	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func TestBackendOutageIntakeRollsBackWithReply(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	db := fixtureDB(t, s)
	_, err := db.Exec("CREATE TRIGGER reject_notice BEFORE INSERT ON outbox_messages WHEN NEW.root_thread_ts='offline' BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	m := storetest.Message("offline", "offline", "alice")
	m.BackendReason = "backend_unavailable"
	if inserted, err := s.Enqueue(ctx, m); err == nil || inserted {
		t.Fatalf("failed local reply was acknowledged: inserted=%v, err=%v", inserted, err)
	}
	for _, table := range []string{"inbox_jobs", "request_status", "outbox_messages"} {
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
	s := fixture(t).Store.(*Store)
	db := fixtureDB(t, s)
	for _, event := range []string{"first", "second"} {
		inserted, err := s.Enqueue(ctx, storetest.Message(event, event, "alice"))
		storetest.Must(t, err)
		if !inserted {
			t.Fatal("event not inserted")
		}
	}
	_, err := db.Exec("CREATE TRIGGER reject_notice BEFORE INSERT ON outbox_messages WHEN NEW.root_thread_ts='second' BEGIN SELECT RAISE(ABORT,'test failure'); END")
	storetest.Must(t, err)
	if err := s.FailUnstarted(ctx, "backend_unavailable"); err == nil {
		t.Fatal("expected reply write failure")
	}
	var pending, replies int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM inbox_jobs WHERE state='pending' AND reason='' AND result_text=''").Scan(&pending))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM outbox_messages").Scan(&replies))
	if pending != 2 || replies != 0 {
		t.Fatalf("partial local failure batch committed: pending=%d, replies=%d", pending, replies)
	}
	_, err = db.Exec("DROP TRIGGER reject_notice")
	storetest.Must(t, err)
	storetest.Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM inbox_jobs WHERE state='completed' AND reason='backend_unavailable'").Scan(&pending))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM outbox_messages").Scan(&replies))
	if pending != 2 || replies != 2 {
		t.Fatalf("retried batch lost terminal jobs or replies: completed=%d, replies=%d", pending, replies)
	}
}
