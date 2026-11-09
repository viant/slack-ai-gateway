package mysql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func TestRunDatabaseIdentityGuard(t *testing.T) {
	ctx := context.Background()
	route := gateway.Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"}
	for _, mode := range []string{"real", "smoke", "principal", "owner", "app", "channel", "workspace", "access_mode"} {
		t.Run(mode, func(t *testing.T) {
			s := fixture(t).Store.(*Store)
			storetest.Must(t, s.CheckRunBindings(ctx, route, "shared"))
			m := storetest.Message("event", "root", "alice")
			if mode == "app" {
				m.AppID = "OTHER"
			}
			if mode == "channel" {
				m.ChannelID = "OTHER"
			}
			if mode == "workspace" {
				m.WorkspaceID = "OTHER"
			}
			_, err := s.Enqueue(ctx, m)
			storetest.Must(t, err)
			j, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			decision := gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"}
			if mode == "owner" {
				decision.OwnerID = "OTHER"
			}
			if mode == "principal" {
				decision.PrincipalID = "OTHER"
			}
			if mode == "access_mode" {
				decision.Mode = "OTHER"
			}
			b, err := s.BindJob(ctx, j.ID, decision)
			storetest.Must(t, err)
			storetest.Must(t, s.BeginCreate(ctx, b.ID))
			id := "real-conversation"
			if mode == "smoke" {
				id = "slack-smoke-fake"
			}
			storetest.Must(t, s.SetConversation(ctx, b.ID, id))
			before, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			err = s.CheckRunBindings(ctx, route, "shared")
			if (err == nil) != (mode == "real") {
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
	_, err := db.Exec(`INSERT INTO inbox_jobs(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,request_id,turn_id,state,created_at,updated_at)
	VALUES('A','T','C','1.001','event','alice','1.001','preserve question',0,'request','request','querying',1,1)`)
	storetest.Must(t, err)
	storetest.Must(t, s.Recover(ctx))
	var text, state string
	storetest.Must(t, db.QueryRow("SELECT query_text,state FROM inbox_jobs").Scan(&text, &state))
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
	if len(status.Deliveries) != 1 || status.Counts["outbox_messages"]["delivery_unknown"] != 1 || status.Counts["outbox_messages"]["pending"] != 1 {
		t.Fatalf("limits or counts incorrect: deliveries=%d counts=%v", len(status.Deliveries), status.Counts["outbox_messages"])
	}
	for _, limit := range []int{0, 1001} {
		if _, err := s.Status(ctx, limit); err == nil {
			t.Fatal("unbounded status allowed")
		}
	}
	var state string
	storetest.Must(t, db.QueryRow("SELECT state FROM outbox_messages WHERE part=0").Scan(&state))
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
	_, err := db.Exec("CREATE TRIGGER fail_audit BEFORE INSERT ON delivery_resolutions FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='audit failure'")
	storetest.Must(t, err)
	if err := s.ResolveDelivery(ctx, id, "retry", "", true); err == nil {
		t.Fatal("audit failure ignored")
	}
	var state string
	storetest.Must(t, db.QueryRow("SELECT state FROM outbox_messages WHERE id=?", id).Scan(&state))
	if state != "delivery_unknown" {
		t.Fatal("partial resolution commit")
	}
	_, err = db.Exec("DROP TRIGGER fail_audit")
	storetest.Must(t, err)
	storetest.Must(t, s.ResolveDelivery(ctx, id, "delivered", "1.001", false))
	var action, ts string
	var risk int
	storetest.Must(t, db.QueryRow("SELECT action,slack_ts,duplicate_risk_accepted FROM delivery_resolutions WHERE delivery_id=?", id).Scan(&action, &ts, &risk))
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
