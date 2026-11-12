package datly_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func testIdentity(name, app string) storage.Integration {
	return storage.Integration{Name: name, AppID: app, WorkspaceID: "T", BackendType: "agui", BackendURL: "https://agent.example.test/run", PrincipalID: "shared"}
}

func registerNativeIntegrations(t *testing.T, root storage.Store, desired ...*storage.Integration) {
	t.Helper()
	input := make([]storage.Integration, len(desired))
	for i, p := range desired {
		input[i] = *p
	}
	rows, err := root.ReconcileIntegrations(context.Background(), input)
	storetest.Must(t, err)
	if len(rows) != len(desired) {
		t.Fatal("reconciliation changed configured assignment count")
	}
	for i, p := range desired {
		if rows[i].ID <= 0 || rows[i].Name != p.Name || !rows[i].Active {
			t.Fatal("reconciliation returned wrong assignment")
		}
		*p = rows[i]
	}
}

func TestNativeIntegrationIsolation(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a, b := testIdentity("a", "A"), testIdentity("b", "B")
			registerNativeIntegrations(t, root, &a, &b)
			firstA, firstB := a.ID, b.ID
			registerNativeIntegrations(t, root, &a, &b)
			if a.ID != firstA || b.ID != firstB {
				t.Fatal("unchanged active assignment changed identity")
			}
			if _, err := root.Enqueue(ctx, storetest.Message("unscoped", "thread", "alice")); err == nil {
				t.Fatal("unscoped runtime mutation accepted")
			}
			as, bs := root.Scope(a.ID), root.Scope(b.ID)
			message := storetest.Message("event-a", "thread", "alice")
			inserted, err := as.Enqueue(ctx, message)
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("missing first event")
			}
			message.AppID, message.EventID = "B", "event-b"
			inserted, err = bs.Enqueue(ctx, message)
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("distinct event deduplicated across integration")
			}
			if _, err = as.Enqueue(ctx, message); err == nil {
				t.Fatal("foreign Slack application admitted")
			}
			var jobs []*gateway.GatewayJob
			for _, scoped := range []storage.Store{as, bs} {
				job, err := scoped.ClaimJob(ctx)
				storetest.Must(t, err)
				if job == nil {
					t.Fatal("missing scoped job")
				}
				jobs = append(jobs, job)
				binding, err := scoped.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
				storetest.Must(t, err)
				storetest.Must(t, scoped.BeginCreate(ctx, binding.ID))
				storetest.Must(t, scoped.SetConversation(ctx, binding.ID, "native-thread"))
				storetest.Must(t, scoped.BeginQuery(ctx, job.ID))
				snapshot := gateway.ProtocolSnapshot{Messages: json.RawMessage(`[{"id":"assistant","role":"assistant","content":"answer","metadata":{"opaque":false}}]`), State: json.RawMessage(`{"exact":9007199254740993}`)}
				storetest.Must(t, scoped.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}, snapshot))
				bound, err := scoped.GetBinding(ctx, job.ThreadKey)
				storetest.Must(t, err)
				if !reflect.DeepEqual(bound.ProtocolMessages, snapshot.Messages) || !reflect.DeepEqual(bound.ProtocolState, snapshot.State) {
					t.Fatal("protocol projection changed")
				}
			}
			if jobs[0].ID == jobs[1].ID {
				t.Fatal("independent jobs share identity")
			}
			if err := bs.Review(ctx, jobs[0].ID, "foreign"); err == nil {
				t.Fatal("foreign job mutated")
			}
			for _, scoped := range []storage.Store{as, bs} {
				delivery, err := scoped.ClaimDelivery(ctx)
				storetest.Must(t, err)
				if delivery == nil {
					t.Fatal("missing scoped delivery")
				}
				view, err := scoped.Status(ctx, 10)
				storetest.Must(t, err)
				if len(view.GatewayJobs) != 1 || len(view.ConversationBindings) != 1 || len(view.SlackOutgoingMessages) != 1 {
					t.Fatalf("scope leaked rows: %+v", view)
				}
			}
			view, err := root.Status(ctx, 10)
			storetest.Must(t, err)
			if len(view.GatewayJobs) != 2 {
				t.Fatal("owner diagnostics lost integration")
			}
			var total, active int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*),SUM(active) FROM integration").Scan(&total, &active))
			if total != 2 || active != 2 {
				t.Fatalf("unexpected recorded assignments %d/%d", total, active)
			}
		})
	}
}

func TestAssignmentChangesAppendAndCloseOriginalWork(t *testing.T) {
	changes := []struct {
		name   string
		change func(*storage.Integration)
	}{
		{"backend", func(v *storage.Integration) { v.BackendURL = "https://other.example.test/run" }},
		{"principal", func(v *storage.Integration) { v.PrincipalID = "other" }},
		{"agent", func(v *storage.Integration) { v.BackendAgentID = "other" }},
		{"app", func(v *storage.Integration) { v.AppID = "OTHER" }},
		{"workspace", func(v *storage.Integration) { v.WorkspaceID = "OTHER" }},
		{"name", func(v *storage.Integration) { v.Name = "renamed" }},
	}
	for _, driver := range []string{"sqlite", "mysql"} {
		for _, change := range changes {
			t.Run(driver+"/"+change.name, func(t *testing.T) {
				ctx := context.Background()
				root, db := sharedFixture(t, driver)
				original := testIdentity("a", "A")
				registerNativeIntegrations(t, root, &original)
				scoped := root.Scope(original.ID)
				job, binding := preparedNormalizedQuestion(t, scoped, "first", "thread", "A")
				storetest.Must(t, scoped.BeginQuery(ctx, job.ID))
				changed := original
				change.change(&changed)
				registerNativeIntegrations(t, root, &changed)
				if changed.ID == original.ID {
					t.Fatal("changed assignment reused old identity")
				}
				var oldActive int
				var oldURL string
				storetest.Must(t, db.QueryRow("SELECT active,backend_endpoint FROM integration WHERE id=?", original.ID).Scan(&oldActive, &oldURL))
				if oldActive != 0 || oldURL != original.BackendURL {
					t.Fatal("historical assignment was overwritten/reactivated")
				}
				view, err := root.Status(ctx, 20)
				storetest.Must(t, err)
				if len(view.ConversationBindings) != 1 || view.ConversationBindings[0].ID != binding.ID || view.ConversationBindings[0].State != "closed" {
					t.Fatal("old binding not closed")
				}
				wantReason := "integration_changed"
				if change.name == "name" {
					wantReason = "integration_removed"
				}
				if len(view.GatewayJobs) != 1 || view.GatewayJobs[0].State != "rejected" || view.GatewayJobs[0].Reason != wantReason || view.GatewayJobs[0].BackendSubmissionStartedAt == 0 {
					t.Fatalf("old work not terminated correctly: %+v", view.GatewayJobs)
				}
				rows, err := root.ReconcileIntegrations(ctx, []storage.Integration{original})
				storetest.Must(t, err)
				if rows[0].ID == original.ID || rows[0].ID == changed.ID {
					t.Fatal("returning to prior config resurrected historical identity")
				}
				view, err = root.Status(ctx, 20)
				storetest.Must(t, err)
				if view.ConversationBindings[0].State != "closed" {
					t.Fatal("returning config revived closed conversation")
				}
			})
		}
	}
}

func TestGlobalEventDedupKeepsOriginalAssignmentAcrossChange(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, _ := sharedFixture(t, driver)
			a := testIdentity("steward", "A")
			registerNativeIntegrations(t, root, &a)
			message := storetest.Message("globally-unique-event", "thread", "alice")
			inserted, err := root.Scope(a.ID).Enqueue(ctx, message)
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("event not admitted")
			}
			b := a
			b.BackendAgentID = "other"
			registerNativeIntegrations(t, root, &b)
			inserted, err = root.Scope(b.ID).Enqueue(ctx, message)
			storetest.Must(t, err)
			if inserted {
				t.Fatal("event accepted under new assignment")
			}
			job, err := root.Scope(b.ID).ClaimJob(ctx)
			storetest.Must(t, err)
			if job != nil {
				t.Fatal("new assignment stole old work")
			}
			view, err := root.Status(ctx, 10)
			storetest.Must(t, err)
			if len(view.GatewayJobs) != 1 || view.GatewayJobs[0].IntegrationID != a.ID {
				t.Fatal("dedup altered original job ownership")
			}
		})
	}
}

func TestRemovedAssignmentIsNeverReactivated(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, _ := sharedFixture(t, driver)
			a := testIdentity("steward", "A")
			registerNativeIntegrations(t, root, &a)
			job, binding := preparedNormalizedQuestion(t, root.Scope(a.ID), "old", "old-thread", "A")
			rows, err := root.ReconcileIntegrations(ctx, []storage.Integration{})
			storetest.Must(t, err)
			if len(rows) != 0 {
				t.Fatal("empty config kept active assignment")
			}
			b := a
			registerNativeIntegrations(t, root, &b)
			if b.ID == a.ID {
				t.Fatal("readded config reused inactive id")
			}
			view, err := root.Status(ctx, 10)
			storetest.Must(t, err)
			if len(view.GatewayJobs) != 1 || view.GatewayJobs[0].ID != job.ID || view.GatewayJobs[0].Reason != "integration_removed" || len(view.ConversationBindings) != 1 || view.ConversationBindings[0].ID != binding.ID || view.ConversationBindings[0].State != "closed" {
				t.Fatal("removed work was not permanently closed")
			}
		})
	}
}
