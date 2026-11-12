package pilot

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	write "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
	"github.com/viant/xdatly/handler"
)

// Exercise the actual generated writer under the existing native transaction
// owner. No error-text matching or substitute validation path is used.
func TestNativeWriterValidationEvidenceAndRollback(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			server, db := validationPilot(t, ctx, driver)
			target := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[PilotComponent]().PkgPath(), Name: "Pilot"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/gateway/pilot"}}
			invoke := func(job *write.GatewayJob) (error, handler.Outcome) {
				var outcome handler.Outcome
				_, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: &PilotInput{Action: "write", GatewayJob: job}, Completion: func(report handler.Outcome) { outcome = report }})
				return err, outcome
			}
			newJob := func(event, request string) *write.GatewayJob {
				return &write.GatewayJob{IntegrationId: 1, SlackChannelId: "C", SlackThreadRootMessageId: "1.000001", SlackEventId: event, SlackPromptAuthorId: "alice", SlackPromptMessageId: "1.000001", PromptText: storetest.Pointer("native validation evidence"), GatewayRequestId: request, GatewayExecutionState: "pending", CreatedAt: storetest.Time(1), GatewayExecutionUpdatedAt: storetest.Time(1), SlackConversationType: "channel", SlackProgressState: "ready", SlackProgressMode: "message", GatewayStatusUpdatedAt: storetest.Time(1)}
			}
			first := newJob("existing-event", "existing-request")
			if err, _ := invoke(first); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name, event, request, field, check string
				integration                        int
				checked                            any
			}{
				{"event_unique", first.SlackEventId, "different-request", "SlackEventId", "unique", 1, first.SlackEventId},
				{"request_unique", "different-event", first.GatewayRequestId, "GatewayRequestId", "unique", 1, first.GatewayRequestId},
				{"foreign_key", "missing-parent-event", "missing-parent-request", "IntegrationId", "refKey", 999999, 999999},
			} {
				t.Run(tc.name, func(t *testing.T) {
					job := newJob(tc.event, tc.request)
					job.IntegrationId = tc.integration
					err, outcome := invoke(job)
					var validation *handler.Validation
					if err == nil || !errors.As(err, &validation) {
						t.Fatalf("native writer did not retain typed validation: %T %v", err, err)
					}
					if outcome.State() != handler.TransactionRolledBack {
						t.Fatalf("failed native writer outcome=%v", outcome.State())
					}
					if len(validation.Violations) != 1 {
						t.Fatalf("unexpected validation facts: %+v", validation.Violations)
					}
					v := validation.Violations[0]
					if v.Field != tc.field || v.Check != tc.check || !v.HasCheckedValue || !reflect.DeepEqual(v.CheckedValue, tc.checked) {
						t.Fatalf("native fact=%+v checked=%#v (%T), want %s/%s/%#v", v, v.CheckedValue, v.CheckedValue, tc.field, tc.check, tc.checked)
					}
					t.Logf("native fact field=%s check=%s checked=%#v (%T); transaction rolled back", v.Field, v.Check, v.CheckedValue, v.CheckedValue)
					assertValidationPilotUnchanged(t, ctx, db, first.Id)
				})
			}
			t.Run("multiple_unique_fields", func(t *testing.T) {
				err, outcome := invoke(newJob(first.SlackEventId, first.GatewayRequestId))
				var validation *handler.Validation
				if err == nil || !errors.As(err, &validation) || outcome.State() != handler.TransactionRolledBack {
					t.Fatalf("multiple native uniqueness failures lost validation/rollback: %T %v", err, err)
				}
				fields := map[string]bool{}
				for _, v := range validation.Violations {
					if v != nil && v.Check == "unique" && v.HasCheckedValue {
						fields[v.Field] = true
					}
				}
				if len(validation.Violations) != 2 || !fields["SlackEventId"] || !fields["GatewayRequestId"] {
					t.Fatalf("native writer did not preserve distinct unique failures: %+v", validation.Violations)
				}
				assertValidationPilotUnchanged(t, ctx, db, first.Id)
			})

			// Non-null generated scalar fields cannot carry a Go nil. Force an
			// actual SQL NULL at the database boundary to prove that an unrelated
			// constraint failure is never event-unique evidence.
			trigger := `CREATE TRIGGER native_null_probe AFTER INSERT ON gateway_job WHEN NEW.slack_event_id='null-event' BEGIN UPDATE gateway_job SET slack_prompt_author_id=NULL WHERE id=NEW.id; END`
			if driver == "mysql" {
				trigger = `CREATE TRIGGER native_null_probe BEFORE INSERT ON gateway_job FOR EACH ROW SET NEW.slack_prompt_author_id=IF(NEW.slack_event_id='null-event',NULL,NEW.slack_prompt_author_id)`
			}
			_, err := db.ExecContext(ctx, trigger)
			must(t, err)
			err, outcome := invoke(newJob("null-event", "null-request"))
			if err == nil || outcome.State() != handler.TransactionRolledBack {
				t.Fatalf("SQL null failure did not roll back: outcome=%v err=%v", outcome.State(), err)
			}
			var validation *handler.Validation
			if errors.As(err, &validation) {
				t.Fatalf("database null error unexpectedly became native event validation: %+v", validation)
			}
			t.Logf("SQL null failure remains distinct (%T); transaction rolled back", err)
			assertValidationPilotUnchanged(t, ctx, db, first.Id)
		})
	}
}

func assertValidationPilotUnchanged(t *testing.T, ctx context.Context, db *sql.DB, firstID int) {
	t.Helper()
	for _, table := range []string{"gateway_job"} {
		var count int
		must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count))
		if count != 1 {
			t.Fatalf("failed native graph changed %s: count=%d", table, count)
		}
	}
	var id int
	must(t, db.QueryRowContext(ctx, "SELECT id FROM gateway_job").Scan(&id))
	if id != firstID {
		t.Fatal("failed native graph replaced the original job")
	}
}

func validationPilot(t *testing.T, ctx context.Context, driver string) (*standalone.Server, *sql.DB) {
	t.Helper()
	var dsn string
	connections := 8
	if driver == "mysql" {
		dsn = storetest.MySQLDSN(t)
	} else {
		location := &url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "validation.sqlite")}
		location.RawQuery = url.Values{"_pragma": {"foreign_keys(ON)", "busy_timeout(5000)", "journal_mode(WAL)"}, "_txlock": {"immediate"}}.Encode()
		dsn, connections = location.String(), 1
	}
	db, err := sql.Open(driver, dsn)
	must(t, err)
	t.Cleanup(func() { must(t, db.Close()) })
	if driver == "sqlite" {
		schema, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
		must(t, err)
		_, err = db.ExecContext(ctx, string(schema))
		must(t, err)
	}
	seedPilotTopology(t, db)
	server, err := standalone.New(ctx, standalone.Options{Config: &config.Config{BaseDir: t.TempDir(), Connector: "slack", Connectors: []connector.Config{{Name: "slack", Driver: driver, DSN: dsn, MaxOpenConns: connections, MaxIdleConns: connections}}, GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/slack-ai-gateway/internal/datly/...", "github.com/viant/slack-ai-gateway/internal/storage/datly/pilot"}}}, RequireLinked: true, Holders: []any{&write.WriterComponent{}, &PilotComponent{}}})
	must(t, err)
	t.Cleanup(func() { must(t, server.Shutdown(context.Background())) })
	must(t, server.Reload(ctx, 1))
	return server, db
}
