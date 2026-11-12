package datly_test

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	bindings "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	jobs "github.com/viant/slack-ai-gateway/internal/datly/read"
	jobwrite "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestMySQLClaimsIndependentThreadWhileOtherThreadIsLocked(t *testing.T) {
	ctx := context.Background()
	root, db := sharedFixture(t, "mysql")
	integrationID := storetest.RegisterDefault(t, root)
	store := root.Scope(integrationID)
	heldJob, heldBinding := preparedNormalizedQuestion(t, store, "held", "held", "A")
	bindingJob, existingBinding := preparedNormalizedQuestion(t, store, "binding", "binding", "A")
	deliveryJob, _ := preparedNormalizedQuestion(t, store, "delivery", "delivery", "A")
	storetest.Must(t, store.Complete(ctx, deliveryJob.ID, "", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
	for _, thread := range []string{"first", "second"} {
		message := storetest.Message(thread, thread, "alice")
		message.StatusMode = gateway.StatusMessage
		_, err := store.Enqueue(ctx, message)
		storetest.Must(t, err)
	}
	first, err := store.ClaimJob(ctx)
	storetest.Must(t, err)
	if first == nil || first.RootTS != "first" {
		t.Fatal("first thread was not claimed")
	}
	message := storetest.Message("status", "status", "alice")
	message.StatusMode = gateway.StatusAgentSession
	_, err = store.Enqueue(ctx, message)
	storetest.Must(t, err)

	// Runtime workers may hold another thread's job/binding while independent
	// threads continue. Integration assignments change only during exclusive
	// startup reconciliation. An X lock on their direct FK parent is therefore
	// not a runtime workload: InnoDB legitimately checks that parent when the
	// claim updates the composite FK-supporting index (integration_id,state,...).
	// Keep this regression on real concurrently mutable per-thread rows.
	hold, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	storetest.Must(t, err)
	defer hold.Rollback()
	var lockedID int64
	storetest.Must(t, hold.QueryRowContext(ctx,
		"SELECT id FROM gateway_job WHERE id=? FOR UPDATE", heldJob.ID).Scan(&lockedID))
	storetest.Must(t, hold.QueryRowContext(ctx,
		"SELECT id FROM conversation_binding WHERE id=? FOR UPDATE", heldBinding.ID).Scan(&lockedID))

	t.Run("claim_job", func(t *testing.T) {
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		second, err := store.ClaimJob(claimCtx)
		if err != nil {
			t.Fatalf("independent thread waited for another thread: %v", err)
		}
		if second == nil || second.ID == first.ID || second.RootTS != "second" {
			t.Fatal("second thread was not claimed while another thread remained locked")
		}
	})
	t.Run("bind_existing_conversation", func(t *testing.T) {
		bindCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		binding, err := store.BindJob(bindCtx, bindingJob.ID, existingBinding.Decision)
		if err != nil {
			t.Fatalf("existing binding waited for another thread: %v", err)
		}
		if binding == nil || binding.ID != existingBinding.ID || binding.RootTS != "binding" || binding.WorkspaceID != "T" {
			t.Fatal("binding lookup changed the recorded conversation")
		}
	})
	t.Run("claim_delivery", func(t *testing.T) {
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		delivery, err := store.ClaimDelivery(claimCtx)
		if err != nil {
			t.Fatalf("delivery waited for another thread: %v", err)
		}
		if delivery == nil || delivery.JobID != deliveryJob.ID || delivery.RootTS != "delivery" || delivery.WorkspaceID != "T" {
			t.Fatal("delivery did not retain its question's destination")
		}
	})
	t.Run("claim_native_status", func(t *testing.T) {
		claimCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		status, err := store.ClaimStatus(claimCtx, time.Now(), "urgent")
		if err != nil {
			t.Fatalf("native status waited for another thread: %v", err)
		}
		if status == nil || status.Target != gateway.StatusTargetAgentSession || status.RootTS != "status" || status.WorkspaceID != "T" || status.SessionState != "processing" {
			t.Fatal("native status did not retain its integration and thread")
		}
	})
}

func TestNativeReaderMetadataPredicatesPreservePresence(t *testing.T) {
	for _, driverName := range []string{"sqlite", "mysql"} {
		t.Run(driverName, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driverName)
			integrationID := storetest.RegisterDefault(t, root)
			job, binding := preparedNormalizedQuestion(t, root.Scope(integrationID), "metadata", "thread", "A")
			server := metadataReaderServer(t, driverName, db)
			jobTarget := dexec.ComponentTarget{
				Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[jobs.ReaderComponent]().PkgPath(), Name: "reader"},
				Route:     spec.RouteRef{Method: "GET", Path: "/internal/gateway/jobs"},
			}
			bindingTarget := dexec.ComponentTarget{
				Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[bindings.ReaderComponent]().PkgPath(), Name: "reader"},
				Route:     spec.RouteRef{Method: "GET", Path: "/internal/gateway/bindings"},
			}
			pointer := func(value string) *string { return &value }
			for _, test := range []struct {
				name           string
				app, workspace *string
				want           int
			}{
				{"absent", nil, nil, 1},
				{"correct", pointer("A"), pointer("T"), 1},
				{"app_only", pointer("A"), nil, 1},
				{"workspace_only", nil, pointer("T"), 1},
				{"empty_app", pointer(""), nil, 0},
				{"empty_workspace", nil, pointer(""), 0},
				{"wrong_app", pointer("another-app"), nil, 0},
				{"wrong_workspace", nil, pointer("another-workspace"), 0},
				{"correct_app_empty_workspace", pointer("A"), pointer(""), 0},
				{"empty_app_correct_workspace", pointer(""), pointer("T"), 0},
			} {
				t.Run("job/"+test.name, func(t *testing.T) {
					input := &jobs.Input{}
					input.SetMode("key")
					input.SetIntegrationID(int(integrationID))
					input.SetID(job.ID)
					if test.app != nil {
						input.SetAppID(*test.app)
					}
					if test.workspace != nil {
						input.SetWorkspaceID(*test.workspace)
					}
					value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: jobTarget, Input: input})
					storetest.Must(t, err)
					output, ok := value.(*jobs.Output)
					if !ok || output == nil {
						t.Fatalf("unexpected native job reader output %T", value)
					}
					if len(output.Data) != test.want {
						t.Fatalf("metadata predicate returned %d rows, want %d", len(output.Data), test.want)
					}
					if test.want == 1 && (output.Data[0].SlackAppId != "A" || output.Data[0].SlackWorkspaceId != "T") {
						t.Fatal("job metadata was not derived from its recorded integration")
					}
				})
			}
			for _, test := range []struct {
				name      string
				workspace *string
				want      int
			}{
				{"absent", nil, 1},
				{"correct", pointer("T"), 1},
				{"empty", pointer(""), 0},
				{"wrong", pointer("another-workspace"), 0},
			} {
				t.Run("binding/"+test.name, func(t *testing.T) {
					input := &bindings.Input{}
					input.SetIntegrationID(int(integrationID))
					input.SetID(binding.ID)
					if test.workspace != nil {
						input.SetWorkspaceID(*test.workspace)
					}
					value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: bindingTarget, Input: input})
					storetest.Must(t, err)
					output, ok := value.(*bindings.Output)
					if !ok || output == nil {
						t.Fatalf("unexpected native binding reader output %T", value)
					}
					if len(output.Data) != test.want {
						t.Fatalf("metadata predicate returned %d rows, want %d", len(output.Data), test.want)
					}
					if test.want == 1 && output.Data[0].SlackWorkspaceId != "T" {
						t.Fatal("binding workspace was not derived from its recorded integration")
					}
				})
			}
		})
	}
}

func metadataReaderServer(t *testing.T, driverName string, db *sql.DB) *standalone.Server {
	t.Helper()
	var dsn string
	if driverName == "mysql" {
		configuration, err := mysqldriver.ParseDSN(os.Getenv("SLACK_AI_GATEWAY_MYSQL_TEST_DSN"))
		if err != nil {
			t.Fatal("invalid MySQL test connection configuration")
		}
		storetest.Must(t, db.QueryRow("SELECT DATABASE()").Scan(&configuration.DBName))
		configuration.ParseTime = true
		dsn = configuration.FormatDSN()
	} else {
		var location string
		storetest.Must(t, db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&location))
		if location == "" {
			t.Fatal("metadata reader fixture requires a durable SQLite database")
		}
		dsn = location + "?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
	}
	server, err := standalone.New(context.Background(), standalone.Options{
		RequireLinked: true,
		Holders:       []any{&jobs.ReaderComponent{}, &bindings.ReaderComponent{}, &jobwrite.WriterComponent{}},
		Config: &config.Config{
			BaseDir: t.TempDir(), Connector: "slack",
			Connectors: []connector.Config{{Name: "slack", Driver: driverName, DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1}},
			GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{
				reflect.TypeFor[jobs.ReaderComponent]().PkgPath(), reflect.TypeFor[bindings.ReaderComponent]().PkgPath(), reflect.TypeFor[jobwrite.WriterComponent]().PkgPath(),
			}},
		},
	})
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, server.Shutdown(context.Background())) })
	storetest.Must(t, server.Reload(context.Background(), 1))
	return server
}

func TestNativeClaimDeadlineBoundariesAndThreadOrdering(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			integrationID := storetest.RegisterDefault(t, root)
			store := root.Scope(integrationID)
			server := metadataReaderServer(t, driver, db)
			target := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[jobs.ReaderComponent]().PkgPath(), Name: "reader"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/gateway/jobs"}}
			now := time.Date(2026, time.April, 3, 10, 11, 12, 123000000, time.UTC)
			claim := func() []*jobs.GatewayJob {
				in := &jobs.Input{}
				in.SetMode("claim")
				in.SetIntegrationID(int(integrationID))
				in.SetNow(now)
				out, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in})
				storetest.Must(t, err)
				return out.(*jobs.Output).Data
			}
			enqueue := func(event, thread string, deadline any) int64 {
				m := storetest.Message(event, thread, "alice")
				_, err := store.Enqueue(ctx, m)
				storetest.Must(t, err)
				var id int64
				storetest.Must(t, db.QueryRow("SELECT id FROM gateway_job WHERE slack_event_id=?", event).Scan(&id))
				_, err = db.Exec("UPDATE gateway_job SET backend_next_poll_at=? WHERE id=?", deadline, id)
				storetest.Must(t, err)
				return id
			}
			for _, test := range []struct {
				name     string
				deadline any
				due      bool
			}{
				{"absent", nil, true}, {"before", now.Add(-time.Millisecond), true}, {"equal", now, true}, {"after", now.Add(time.Millisecond), false},
			} {
				t.Run(test.name, func(t *testing.T) {
					id := enqueue(test.name, test.name, test.deadline)
					rows := claim()
					if test.due && (len(rows) != 1 || int64(rows[0].Id) != id) || !test.due && len(rows) != 0 {
						t.Fatalf("native claim at deadline=%v returned %+v", test.deadline, rows)
					}
					_, err := db.Exec("UPDATE gateway_job SET gateway_execution_state='completed' WHERE id=?", id)
					storetest.Must(t, err)
				})
			}
			first := enqueue("ordered-first", "ordered", now.Add(time.Millisecond))
			second := enqueue("ordered-second", "ordered", nil)
			independent := enqueue("independent", "independent", nil)
			rows := claim()
			if len(rows) != 1 || int64(rows[0].Id) != independent {
				t.Fatal("a later due question bypassed its earlier same-thread question")
			}
			_, err := db.Exec("UPDATE gateway_job SET gateway_execution_state='completed' WHERE id=?", first)
			storetest.Must(t, err)
			rows = claim()
			if len(rows) != 1 || int64(rows[0].Id) != second {
				t.Fatal("completed earlier request still blocked the due same-thread question")
			}
		})
	}
}
