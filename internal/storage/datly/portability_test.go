package datly_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func sharedFixture(t *testing.T, driver string) (storage.Store, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store storage.Store
	var dsn string
	if driver == "mysql" {
		dsn = storetest.MySQLDSN(t)
		s, err := mysql.Open(ctx, dsn, nil)
		storetest.Must(t, err)
		store = s
	} else {
		path := filepath.Join(t.TempDir(), "shared.sqlite")
		s, err := sqlite.Open(ctx, path, true)
		storetest.Must(t, err)
		if err = s.Migrate(ctx); err != nil {
			_ = s.Close()
			t.Fatal(err)
		}
		store, dsn = s, path+"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
	}
	t.Cleanup(func() { storetest.Must(t, store.Close()) })
	// Fixture/assertion access is separate from the store's sole runtime pool.
	db, err := sql.Open(driver, dsn)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	return store, db
}

func TestSmokeNamespacePreservesDatabaseComparisons(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			s, db := sharedFixture(t, driver)
			integrationID := storetest.RegisterDefault(t, s)
			s = s.Scope(integrationID)
			ctx := context.Background()
			inserted, err := s.Enqueue(ctx, storetest.Message("identity", "thread", "alice"))
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("missing event")
			}
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			binding, err := s.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
			storetest.Must(t, err)
			route := storetest.Route("A", "T", "C")
			for _, test := range []struct {
				conversation string
				smoke        bool
			}{
				{"real-conversation", false},
				{"slack-smoke-test", true},
				{"SLACK-SMOKE-test", true},
				{"sLaCk-sMoKe-test", true},
				{"slack-smoke-", true},
				{"slack-smoke-test ", true},
				{" slack-smoke-test", false},
				{"slack-smoke", false},
				{"ślack-smoke-test", driver == "mysql"},
				{"slack-smóke-test", driver == "mysql"},
				{"", false},
			} {
				t.Run(test.conversation, func(t *testing.T) {
					_, err := db.ExecContext(ctx, "UPDATE conversation_binding SET backend_conversation_id=? WHERE id=?", test.conversation, binding.ID)
					storetest.Must(t, err)
					// The bound graph Input has no optional smoke marker. The
					// namespace predicate must still execute on every invocation.
					err = s.CheckRunBindings(ctx, route, "shared")
					if (err != nil) != test.smoke {
						t.Fatalf("smoke=%v, error=%v", test.smoke, err)
					}
				})
			}
		})
	}
}

func TestPortableCooldownMaximum(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			s, db := sharedFixture(t, driver)
			integrationID := storetest.RegisterDefault(t, s)
			s = s.Scope(integrationID)
			ctx := context.Background()
			message := storetest.Message("cooldown", "thread", "alice")
			message.StatusMode = gateway.StatusHybrid
			_, err := s.Enqueue(ctx, message)
			storetest.Must(t, err)
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Complete(ctx, job.ID, "", "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
			for _, test := range []struct {
				name                      string
				delivery, status, session int64
				want                      int64
			}{
				{"zero", 0, 0, 0, 0},
				{"delivery", 300, 200, 100, 300},
				{"message_status", 100, 300, 200, 300},
				{"session", 100, 200, 300, 300},
				{"negative", -3, -1, -2, -1},
			} {
				t.Run(test.name, func(t *testing.T) {
					for _, update := range []struct {
						sql   string
						value int64
					}{
						{"UPDATE slack_outgoing_message SET slack_delivery_next_attempt_at=?", test.delivery},
						{"UPDATE gateway_job SET slack_progress_rate_limited_until=?", test.status},
						{"UPDATE slack_agent_session_publication SET slack_publication_rate_limited_until=?", test.session},
					} {
						_, err := db.ExecContext(ctx, update.sql, nullableFixtureTime(update.value))
						storetest.Must(t, err)
					}
					until, err := s.DeliveryNotBefore(ctx, "T")
					storetest.Must(t, err)
					if until.UnixMilli() != test.want {
						t.Fatalf("cooldown=%d, want=%d", until.UnixMilli(), test.want)
					}
					until, err = s.DeliveryNotBefore(ctx, "unrelated-workspace")
					storetest.Must(t, err)
					if until.UnixMilli() != 0 {
						t.Fatal("workspace scope or empty maximum changed")
					}
				})
			}
		})
	}
}

func TestSharedStoreWithoutSources(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			s, _ := sharedFixture(t, driver)
			integrationID := storetest.RegisterDefault(t, s)
			s = s.Scope(integrationID)
			t.Chdir(t.TempDir())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := s.Enqueue(ctx, storetest.Message("compiled", "thread", "alice"))
			storetest.Must(t, err)
			storetest.Must(t, s.CheckRunBindings(ctx, storetest.Route("A", "T", "C"), "shared"))
			until, err := s.DeliveryNotBefore(ctx, "T")
			storetest.Must(t, err)
			if until.UnixMilli() != 0 {
				t.Fatal("unexpected empty cooldown")
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if _, err := s.Enqueue(canceled, storetest.Message("canceled", "thread", "alice")); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation=%v", err)
			}
		})
	}
}

func nullableFixtureTime(milliseconds int64) any {
	if milliseconds == 0 {
		return nil
	}
	return storetest.Time(milliseconds)
}
