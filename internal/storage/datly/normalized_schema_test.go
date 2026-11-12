package datly_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestNormalizedSchemaKeepsNativeSingleColumnReferences(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			_, db := sharedFixture(t, driver)
			removed := map[string][]string{
				"integration":                     {"app_id", "workspace_id", "principal_id"},
				"gateway_job":                     {"app_id", "workspace_id", "access_mode", "principal_id", "owner_id", "installation_id", "turn_id", "message_ts", "root_thread_ts", "request_id", "job_id", "query_started_at", "message_surface", "query_text", "binding_id", "state", "reason", "updated_at"},
				"conversation_binding":            {"workspace_id", "root_thread_ts", "conversation_id", "channel_id", "owner_id", "principal_id", "protocol_messages", "protocol_state"},
				"slack_outgoing_message":          {"integration_id", "workspace_id", "channel_id", "slack_thread_root_message_id", "slack_ts", "request_id", "part", "content", "kind", "format"},
				"slack_agent_session_publication": {"workspace_id", "installation_id", "root_thread_ts", "integration_id", "source_job_id", "latest_job_id", "latest_status_request_id", "status_source_request_id"},
			}
			for table, names := range removed {
				rows, err := db.Query("SELECT * FROM " + table + " LIMIT 0")
				storetest.Must(t, err)
				columns, err := rows.Columns()
				storetest.Must(t, err)
				storetest.Must(t, rows.Close())
				for _, name := range names {
					if slices.Contains(columns, name) {
						t.Errorf("%s still duplicates %s", table, name)
					}
				}
				if table != "gateway_job" && !slices.Contains(columns, "updated_at") {
					t.Errorf("%s lost updated_at", table)
				}
				required := map[string][]string{
					"integration":                     {"slack_app_id", "slack_workspace_id", "backend_principal_id"},
					"gateway_job":                     {"gateway_request_id", "backend_submission_started_at", "slack_prompt_message_id", "slack_thread_root_message_id", "slack_conversation_type", "slack_progress_message_id", "slack_progress_state", "gateway_execution_state", "gateway_execution_updated_at", "gateway_status_updated_at", "conversation_binding_id", "prompt_text"},
					"conversation_binding":            {"backend_conversation_id", "slack_thread_root_message_id", "gateway_binding_state", "backend_protocol_messages", "backend_protocol_state"},
					"slack_outgoing_message":          {"slack_message_id", "slack_message_kind", "job_id", "slack_delivery_state"},
					"slack_agent_session_publication": {"initial_integration_id", "slack_thread_root_message_id", "latest_status_job_id", "status_source_job_id"},
				}
				for _, name := range required[table] {
					if !slices.Contains(columns, name) {
						t.Errorf("%s lost %s", table, name)
					}
				}
			}
			for _, table := range []string{"slack_installations", "backend_connections", "agents", "inbox_jobs", "request_status", "outbox_messages", "delivery_resolutions", "agent_session_publications", "integrations", "conversation_bindings", "requests", "slack_outgoing_messages", "slack_delivery_resolutions", "slack_agent_session_publications"} {
				var count int
				query := "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
				if driver == "mysql" {
					query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name=?"
				}
				storetest.Must(t, db.QueryRow(query, table).Scan(&count))
				if count != 0 {
					t.Errorf("obsolete registry table %s still exists", table)
				}
			}
			for table, want := range map[string]int{
				"integration": 0, "conversation_binding": 1, "gateway_job": 2,
				"slack_outgoing_message": 1, "slack_delivery_resolution": 1,
				"slack_agent_session_publication": 3,
			} {
				if got := singleColumnReferences(t, db, driver, table); got != want {
					t.Errorf("%s references=%d, want %d", table, got, want)
				}
			}
		})
	}
}

func singleColumnReferences(t *testing.T, db *sql.DB, driver, table string) int {
	t.Helper()
	var rows *sql.Rows
	var err error
	if driver == "mysql" {
		rows, err = db.Query(`SELECT constraint_name,COUNT(*) FROM information_schema.key_column_usage
			WHERE table_schema=DATABASE() AND table_name=? AND referenced_table_name IS NOT NULL
			GROUP BY constraint_name`, table)
	} else {
		rows, err = db.Query("SELECT id,COUNT(*) FROM pragma_foreign_key_list(?) GROUP BY id", table)
	}
	storetest.Must(t, err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		var name string
		var columns int
		storetest.Must(t, rows.Scan(&name, &columns))
		if columns != 1 {
			t.Errorf("%s foreign key %s has %d columns", table, name, columns)
		}
		count++
	}
	storetest.Must(t, rows.Err())
	return count
}

func TestPublicationIdentitySurvivesNewQuestionsAndAcknowledgement(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			identity := testIdentity("a", "A")
			registerNativeIntegrations(t, root, &identity)
			store := root.Scope(identity.ID)
			for _, thread := range []string{"first", "second"} {
				message := storetest.Message(thread, thread, "alice")
				message.StatusMode = gateway.StatusAgentSession
				_, err := store.Enqueue(ctx, message)
				storetest.Must(t, err)
			}
			var first, second int64
			storetest.Must(t, db.QueryRow("SELECT id FROM slack_agent_session_publication WHERE slack_thread_root_message_id='first'").Scan(&first))
			storetest.Must(t, db.QueryRow("SELECT id FROM slack_agent_session_publication WHERE slack_thread_root_message_id='second'").Scan(&second))
			if first <= 0 || second <= 0 || first == second {
				t.Fatalf("native publication identities are not unique: %d/%d", first, second)
			}
			now := time.Now()
			status := claimSharedSession(t, store, identity.ID, "processing", now)
			acknowledgeSharedSession(t, store, status, now)
			followup := storetest.Message("followup", "first", "alice")
			followup.StatusMode = gateway.StatusAgentSession
			_, err := store.Enqueue(ctx, followup)
			storetest.Must(t, err)
			var unchanged int64
			var count int
			storetest.Must(t, db.QueryRow("SELECT id FROM slack_agent_session_publication WHERE slack_thread_root_message_id='first'").Scan(&unchanged))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&count))
			if unchanged != first || count != 2 {
				t.Fatalf("new question replaced its publication: id=%d want=%d count=%d", unchanged, first, count)
			}
		})
	}
}

func TestWorkflowRejectsBindingFromAnotherIntegrationOrThread(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		for _, differentIntegration := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/different_integration=%t", driver, differentIntegration), func(t *testing.T) {
				ctx := context.Background()
				root, db := sharedFixture(t, driver)
				a := testIdentity("a", "A")
				b := testIdentity("b", "B")
				registerNativeIntegrations(t, root, &a, &b)
				as := root.Scope(a.ID)
				first, firstBinding := preparedNormalizedQuestion(t, as, "first", "first", "A")
				otherStore, otherThread, app := as, "second", "A"
				if differentIntegration {
					otherStore, otherThread, app = root.Scope(b.ID), "first", "B"
				}
				_, otherBinding := preparedNormalizedQuestion(t, otherStore, "other", otherThread, app)
				// A single FK intentionally permits an existing parent's id.
				// The workflow must refuse the inconsistent relationship.
				_, err := db.Exec("UPDATE gateway_job SET conversation_binding_id=? WHERE id=?", otherBinding.ID, first.ID)
				storetest.Must(t, err)
				if err := as.BeginQuery(ctx, first.ID); err == nil {
					t.Fatal("query accepted an inconsistent binding")
				}
				var started sql.NullTime
				storetest.Must(t, db.QueryRow("SELECT backend_submission_started_at FROM gateway_job WHERE id=?", first.ID).Scan(&started))
				if started.Valid {
					t.Fatal("rejected relationship changed submission markers")
				}
				_, err = db.Exec("UPDATE gateway_job SET conversation_binding_id=? WHERE id=?", firstBinding.ID, first.ID)
				storetest.Must(t, err)
				storetest.Must(t, as.BeginQuery(ctx, first.ID))
			})
		}
	}
}

func preparedNormalizedQuestion(t *testing.T, store storage.Store, event, thread, app string) (*gateway.GatewayJob, *gateway.ConversationBinding) {
	t.Helper()
	ctx := context.Background()
	message := storetest.Message(event, thread, "alice")
	message.AppID = app
	_, err := store.Enqueue(ctx, message)
	storetest.Must(t, err)
	job, err := store.ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil {
		t.Fatal("missing prepared question")
	}
	binding, err := store.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	storetest.Must(t, store.BeginCreate(ctx, binding.ID))
	storetest.Must(t, store.SetConversation(ctx, binding.ID, "backend-"+event))
	return job, binding
}

func TestStartupGuardRejectsChangedSlackIdentity(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a := testIdentity("a", "A")
			b := testIdentity("b", "B")
			registerNativeIntegrations(t, root, &a, &b)
			store := root.Scope(a.ID)
			_, err := store.Enqueue(ctx, storetest.Message("first", "thread", "alice"))
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE integration SET slack_app_id=? WHERE id=?", b.AppID, a.ID)
			storetest.Must(t, err)
			if err := store.CheckRunBindings(ctx, storetest.Route("A", "T", "C"), "shared"); err == nil {
				t.Fatal("startup accepted a question from another installation")
			}
		})
	}
}

func TestWorkflowRejectsSessionJobsFromAnotherInstallationOrThread(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		for _, column := range []string{"status_source_job_id", "latest_status_job_id"} {
			for _, otherInstallation := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/other_installation=%t", driver, column, otherInstallation), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					identities := []storage.Integration{testIdentity("a", "A"), testIdentity("b", "B")}
					registerNativeIntegrations(t, root, &identities[0], &identities[1])
					as := root.Scope(identities[0].ID)
					message := storetest.Message("first", "first", "alice")
					message.StatusMode = gateway.StatusAgentSession
					_, err := as.Enqueue(ctx, message)
					storetest.Must(t, err)
					other := storetest.Message("other", "second", "bob")
					otherStore := as
					if otherInstallation {
						other.AppID = "B"
						other.RootTS = "first"
						otherStore = root.Scope(identities[1].ID)
					}
					_, err = otherStore.Enqueue(ctx, other)
					storetest.Must(t, err)
					var original, wrong int64
					storetest.Must(t, db.QueryRow("SELECT id FROM gateway_job WHERE slack_event_id='first'").Scan(&original))
					storetest.Must(t, db.QueryRow("SELECT id FROM gateway_job WHERE slack_event_id='other'").Scan(&wrong))
					_, err = db.Exec("UPDATE slack_agent_session_publication SET "+column+"=? WHERE initial_integration_id=?", wrong, identities[0].ID)
					storetest.Must(t, err)
					if _, err = as.ClaimStatus(ctx, time.Now(), "urgent"); err == nil {
						t.Fatal("status accepted a source outside its installation/thread")
					}
					var attempts int
					storetest.Must(t, db.QueryRow("SELECT slack_publication_attempts FROM slack_agent_session_publication WHERE initial_integration_id=?", identities[0].ID).Scan(&attempts))
					if attempts != 0 {
						t.Fatal("invalid status claimed an outbound operation")
					}
					_, err = db.Exec("UPDATE slack_agent_session_publication SET "+column+"=? WHERE initial_integration_id=?", original, identities[0].ID)
					storetest.Must(t, err)
					claimSharedSession(t, as, identities[0].ID, "processing", time.Now())
				})
			}
		}
	}
}
