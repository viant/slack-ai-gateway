package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func TestChannelHistoryAndPendingWorkSurviveRestart(t *testing.T) {
	ctx := context.Background()
	f, db := fixtureWithDB(t)
	s := f.Store.(*Store)
	_, err := db.Exec(`INSERT INTO conversation_bindings(workspace_id,channel_id,root_thread_ts,conversation_id,access_mode,principal_id,owner_id,state,created_at,updated_at)
	VALUES('T','C','1','existing-conversation','shared_channel','shared','shared','ready',0,0)`)
	storetest.Must(t, err)
	_, err = db.Exec(`INSERT INTO inbox_jobs(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,binding_id,access_mode,principal_id,owner_id,request_id,state,created_at,updated_at)
	VALUES('A','T','C','1','legacy','alice','1','old query',0,1,'shared_channel','shared','shared','legacy-request','completed',0,0),
	('A','T','C','2','queued','bob','2','pending query',0,NULL,'','','','queued-request','pending',0,0)`)
	storetest.Must(t, err)
	_, err = db.Exec(`INSERT INTO outbox_messages(job_id,workspace_id,channel_id,root_thread_ts,part,content,state,created_at,updated_at,format)
	VALUES(1,'T','C','1',0,'unchanged reply','pending',0,0,'markdown')`)
	storetest.Must(t, err)
	_, err = db.Exec(`INSERT INTO request_status(job_id,slack_ts,updated_at) VALUES(1,'existing-status',0)`)
	storetest.Must(t, err)
	s = f.Reopen().(*Store)
	b, err := s.GetBinding(ctx, gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"})
	storetest.Must(t, err)
	if b == nil || b.ConversationID != "existing-conversation" || b.OwnerID != "shared" {
		t.Fatal("binding history changed")
	}
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if j == nil || j.Surface != gateway.SurfaceChannel || !j.Mentioned || j.Text != "pending query" {
		t.Fatal("pending job lost default channel semantics")
	}
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != "unchanged reply" || d.Format != gateway.ReplyMarkdown {
		t.Fatal("pending reply changed")
	}
	dc, err := s.GetDeliveryContext(ctx, *d)
	storetest.Must(t, err)
	if dc.Message.Surface != gateway.SurfaceChannel || !dc.Message.Mentioned || dc.Message.AuthorID != "alice" || dc.Binding.ConversationID != b.ConversationID {
		t.Fatal("delivery context changed")
	}
	status, err := s.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if status == nil || status.SlackTS != "existing-status" {
		t.Fatal("existing status lost")
	}
	sc, err := s.GetStatusContext(ctx, *status)
	storetest.Must(t, err)
	if sc.Message.Surface != gateway.SurfaceChannel || sc.Message.AuthorID != "alice" {
		t.Fatal("status context changed")
	}
}
