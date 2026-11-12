package mysql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestRunDatabaseIdentityGuard(t *testing.T) {
	ctx := context.Background()
	route := storetest.Route("A", "T", "C")
	for _, mode := range []string{"real", "smoke", "principal", "owner", "app", "channel_access_change", "workspace", "access_mode"} {
		t.Run(mode, func(t *testing.T) {
			s, db := fixtureDB(t)
			storetest.Must(t, s.CheckRunBindings(ctx, route, "shared"))
			m := storetest.Message("event", "root", "alice")
			_, err := s.Enqueue(ctx, m)
			storetest.Must(t, err)
			j, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			decision := gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"}
			b, err := s.BindJob(ctx, j.ID, decision)
			storetest.Must(t, err)
			storetest.Must(t, s.BeginCreate(ctx, b.ID))
			id := "real-conversation"
			if mode == "smoke" {
				id = "slack-smoke-fake"
			}
			storetest.Must(t, s.SetConversation(ctx, b.ID, id))
			// Admission rejects foreign installation identities. Inject historical
			// identity corruption directly to exercise the startup guard. Channel
			// policy changes below must leave this recorded identity valid.
			column := map[string]string{"app": "slack_app_id", "workspace": "slack_workspace_id", "principal": "backend_principal_id", "owner": "gateway_binding_owner_id", "access_mode": "gateway_access_mode"}[mode]
			switch column {
			case "slack_app_id", "slack_workspace_id":
				_, err = db.Exec("UPDATE integration SET " + column + "='OTHER' WHERE name='default' AND active=1")
				storetest.Must(t, err)
			case "backend_principal_id", "gateway_binding_owner_id", "gateway_access_mode":
				_, err = db.Exec("UPDATE conversation_binding SET "+column+"='OTHER' WHERE id=?", b.ID)
				storetest.Must(t, err)
			}
			before, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			checkRoute := route
			if mode == "channel_access_change" {
				// A newly selected channel changes access, not backend ownership.
				checkRoute = storetest.Route("A", "T", "OTHER")
			}
			err = s.CheckRunBindings(ctx, checkRoute, "shared")
			if (err == nil) != (mode == "real" || mode == "channel_access_change") {
				t.Fatalf("incorrect database guard result: %v", err)
			}
			after, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			x, _ := json.Marshal(before)
			y, _ := json.Marshal(after)
			if string(x) != string(y) {
				t.Fatal("preflight mutated existing state")
			}
		})
	}
}

func TestUncorrelatedQueryRecoveryDoesNotReplay(t *testing.T) {
	ctx := context.Background()
	s, db := fixtureDB(t)
	_, err := db.Exec(`INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
	VALUES ((SELECT id FROM integration WHERE name='default' AND active=1),'C','1.001','event','alice','1.001','preserve question',0,'request','querying','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.000')`)
	storetest.Must(t, err)
	storetest.Must(t, s.Recover(ctx))
	var text, state string
	storetest.Must(t, db.QueryRow("SELECT prompt_text,gateway_execution_state FROM gateway_job").Scan(&text, &state))
	if text != "preserve question" || state != "needs_review" {
		t.Fatal("question lost or uncertain query replayed")
	}
}

func uncertainDelivery(t *testing.T, s *Store) int64 {
	t.Helper()
	ctx := context.Background()
	m := storetest.Message("a", "one", "alice")
	m.Text = "secret-prompt-marker"
	_, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Complete(ctx, j.ID, "", "secret-result-marker", []gateway.ReplyPart{{Text: "secret-reply-marker", Format: gateway.ReplyPlain}, {Text: "second-part", Format: gateway.ReplyPlain}}))
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.DeliveryUnknown(ctx, d.ID))
	return d.ID
}

func TestStatusExcludesTextAndDoesNotMutate(t *testing.T) {
	ctx := context.Background()
	s, db := fixtureDB(t)
	uncertainDelivery(t, s)
	status, err := s.Status(ctx, 1)
	storetest.Must(t, err)
	data, err := json.Marshal(status)
	storetest.Must(t, err)
	if strings.Contains(string(data), "secret-") || strings.Contains(string(data), "second-part") {
		t.Fatal("diagnostic exposed text")
	}
	if len(status.SlackOutgoingMessages) != 1 || status.Counts["slack_outgoing_message.slack_delivery_state"]["delivery_unknown"] != 1 || status.Counts["slack_outgoing_message.slack_delivery_state"]["pending"] != 1 {
		t.Fatalf("limits or counts incorrect: deliveries=%d counts=%v", len(status.SlackOutgoingMessages), status.Counts["slack_outgoing_message.slack_delivery_state"])
	}
	for _, limit := range []int{0, 1001} {
		if _, err := s.Status(ctx, limit); err == nil {
			t.Fatal("unbounded status allowed")
		}
	}
	var state string
	storetest.Must(t, db.QueryRow("SELECT slack_delivery_state FROM slack_outgoing_message WHERE slack_message_index=0").Scan(&state))
	if state != "delivery_unknown" {
		t.Fatal("status mutated state")
	}
}

func TestResolveDeliveryRequiresExplicitDecisionAndAuditsAtomically(t *testing.T) {
	ctx := context.Background()
	s, db := fixtureDB(t)
	id := uncertainDelivery(t, s)
	for _, tc := range []struct {
		action, ts string
		risk       bool
	}{
		{"retry", "", false}, {"delivered", "", false}, {"delivered", "not-a-ts", false}, {"retry", "1.001", true}, {"other", "", true}, {"delivered", "1.001", true},
	} {
		if err := s.ResolveDelivery(ctx, id, tc.action, tc.ts, tc.risk); err == nil {
			t.Fatal("invalid resolution accepted")
		}
	}
	if err := s.ResolveDelivery(ctx, id+1, "delivered", "1.002", false); err == nil {
		t.Fatal("resolved a non-unknown part")
	}
	_, err := db.Exec("CREATE TRIGGER fail_audit BEFORE INSERT ON slack_delivery_resolution FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='audit failure'")
	storetest.Must(t, err)
	if err := s.ResolveDelivery(ctx, id, "retry", "", true); err == nil {
		t.Fatal("audit failure ignored")
	}
	var state string
	storetest.Must(t, db.QueryRow("SELECT slack_delivery_state FROM slack_outgoing_message WHERE id=?", id).Scan(&state))
	if state != "delivery_unknown" {
		t.Fatal("partial resolution commit")
	}
	_, err = db.Exec("DROP TRIGGER fail_audit")
	storetest.Must(t, err)
	storetest.Must(t, s.ResolveDelivery(ctx, id, "delivered", "1.001", false))
	var action, ts string
	var risk int
	storetest.Must(t, db.QueryRow("SELECT resolution_action,slack_message_id,duplicate_delivery_risk_accepted FROM slack_delivery_resolution WHERE outgoing_message_id=?", id).Scan(&action, &ts, &risk))
	if action != "delivered" || ts != "1.001" || risk != 0 {
		t.Fatal("audit incomplete")
	}
	if err := s.ResolveDelivery(ctx, id, "retry", "", true); err == nil {
		t.Fatal("repeated resolution accepted")
	}
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Part != 1 {
		t.Fatal("next part did not unblock")
	}
}
