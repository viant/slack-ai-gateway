package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestChannelHistoryAndPendingWorkSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.sqlite")
	s, err := Open(ctx, path, true)
	storetest.Must(t, err)
	t.Cleanup(func() { s.Close() })
	storetest.Must(t, s.Migrate(ctx))
	storetest.RegisterDefault(t, s)
	_, err = fixtureDB(t, s).Exec(`INSERT INTO conversation_binding(integration_id,slack_channel_id,slack_thread_root_message_id,backend_conversation_id,gateway_access_mode,backend_principal_id,gateway_binding_owner_id,gateway_binding_state,created_at,updated_at,last_activity_at)
	VALUES ((SELECT id FROM integration WHERE name='default' AND active=1),'C','1','existing-conversation','shared_channel','shared','shared','ready','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000')`)
	storetest.Must(t, err)
	_, err = fixtureDB(t, s).Exec(`INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,conversation_binding_id,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
	VALUES ((SELECT id FROM integration WHERE name='default' AND active=1),'C','1','legacy','alice','1','old query',0,1,'legacy-request','completed','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000'),
	       ((SELECT id FROM integration WHERE name='default' AND active=1),'C','2','queued','bob','2','pending query',0,NULL,'queued-request','pending','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000')`)
	storetest.Must(t, err)
	_, err = fixtureDB(t, s).Exec(`INSERT INTO slack_outgoing_message(job_id,slack_message_index,slack_message_text,slack_delivery_state,created_at,updated_at,slack_message_format)
	VALUES (1,0,'unchanged reply','pending','1970-01-01 00:00:00.000','1970-01-01 00:00:00.000','markdown')`)
	storetest.Must(t, err)
	_, err = fixtureDB(t, s).Exec(`UPDATE gateway_job SET slack_progress_message_id='existing-status',gateway_status_updated_at='1970-01-01 00:00:00.000' WHERE id=1`)
	storetest.Must(t, err)
	storetest.Must(t, s.Close())
	s, err = Open(ctx, path, false)
	storetest.Must(t, err)
	storetest.Must(t, s.CheckSchema(ctx))
	storetest.Must(t, s.Recover(ctx))
	b, err := storetest.Scope(t, s, "default").GetBinding(ctx, gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"})
	storetest.Must(t, err)
	if b == nil || b.ConversationID != "existing-conversation" || b.OwnerID != "shared" {
		t.Fatal("binding history changed")
	}
	j, err := storetest.Scope(t, s, "default").ClaimJob(ctx)
	storetest.Must(t, err)
	if j == nil || j.Surface != gateway.SurfaceChannel || !j.Mentioned || j.Text != "pending query" {
		t.Fatal("pending job lost default channel semantics")
	}
	d, err := storetest.Scope(t, s, "default").ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != "unchanged reply" || d.Format != gateway.ReplyMarkdown {
		t.Fatal("pending reply changed")
	}
	dc, err := storetest.Scope(t, s, "default").GetDeliveryContext(ctx, *d)
	storetest.Must(t, err)
	if dc.Message.Surface != gateway.SurfaceChannel || !dc.Message.Mentioned || dc.Message.AuthorID != "alice" || dc.ConversationBinding.ConversationID != b.ConversationID {
		t.Fatal("delivery context changed")
	}
	status, err := storetest.Scope(t, s, "default").ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if status == nil || status.SlackTS != "existing-status" {
		t.Fatal("existing status lost")
	}
	sc, err := storetest.Scope(t, s, "default").GetStatusContext(ctx, *status)
	storetest.Must(t, err)
	if sc.Message.Surface != gateway.SurfaceChannel || sc.Message.AuthorID != "alice" {
		t.Fatal("status context changed")
	}
}
