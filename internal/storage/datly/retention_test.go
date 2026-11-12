package datly_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	wire "github.com/viant/agently-core/protocol/agui"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/slack-ai-gateway/internal/agui"
	jobwrite "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type retentionFixture struct {
	root, store storage.Store
	db          *sql.DB
	worker      gateway.Worker
	mu          sync.Mutex
	calls       []wire.RunAgentInput
}

// All redaction/deletion below is fixture setup, deliberately not a production
// retention API. Normal intake, execution and continuation use the real path.
func newRetentionFixture(t *testing.T, driver string) *retentionFixture {
	t.Helper()
	f := &retentionFixture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in wire.RunAgentInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.calls = append(f.calls, in)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		emit := func(value any) {
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Error(err)
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", encoded)
		}
		emit(map[string]any{"type": "RUN_STARTED", "threadId": in.ThreadID, "runId": in.RunID, "protocolVersion": "1.0"})
		emit(json.RawMessage(`{"type":"STATE_SNAPSHOT","snapshot":{"opaque":9007199254740993,"continuation":true}}`))
		emit(map[string]any{"type": "TEXT_MESSAGE_START", "messageId": "answer-" + in.RunID, "role": "assistant"})
		emit(map[string]any{"type": "TEXT_MESSAGE_CONTENT", "messageId": "answer-" + in.RunID, "delta": "The mouse helped the baker."})
		emit(map[string]any{"type": "TEXT_MESSAGE_END", "messageId": "answer-" + in.RunID})
		emit(map[string]any{"type": "RUN_FINISHED", "threadId": in.ThreadID, "runId": in.RunID})
	}))
	t.Cleanup(server.Close)
	f.root, f.db = sharedFixture(t, driver)
	identity := testIdentity("retention", "A")
	identity.BackendURL = server.URL + "/v1/ag-ui/run"
	registerNativeIntegrations(t, f.root, &identity)
	f.store = f.root.Scope(identity.ID)
	clients, err := agui.NewFactory(identity.BackendURL, server.Client(), nil)
	storetest.Must(t, err)
	f.worker = gateway.Worker{Store: f.store, Clients: clients, Policy: gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "shared"}}
	return f
}
func (f *retentionFixture) inputs() []wire.RunAgentInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wire.RunAgentInput(nil), f.calls...)
}
func (f *retentionFixture) turn(t *testing.T, event, thread string, audit bool) (gateway.Message, int64, *gateway.ConversationBinding) {
	t.Helper()
	ctx := context.Background()
	m := storetest.Message(event, thread, "alice")
	m.Text = "Prompt " + event
	m.StatusMode = gateway.StatusHybrid
	accepted, err := (gateway.Intake{Store: f.store, StatusMode: gateway.StatusHybrid, Route: gateway.Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"}}).Accept(ctx, m)
	storetest.Must(t, err)
	if !accepted.Accepted || accepted.Duplicate {
		t.Fatalf("intake=%+v", accepted)
	}
	acknowledgeTerminationStatuses(t, f.store, "urgent", time.Now())
	worked, err := f.worker.Step(ctx)
	storetest.Must(t, err)
	if !worked {
		t.Fatal("real worker did not execute accepted prompt")
	}
	var id int64
	storetest.Must(t, f.db.QueryRow("SELECT id FROM gateway_job WHERE slack_event_id=?", event).Scan(&id))
	binding, err := f.store.GetBinding(ctx, m.ThreadKey)
	storetest.Must(t, err)
	if binding == nil || binding.ConversationID == "" {
		t.Fatal("worker did not persist backend binding")
	}
	for {
		outgoing, err := f.store.ClaimDelivery(ctx)
		storetest.Must(t, err)
		if outgoing == nil {
			break
		}
		if audit {
			storetest.Must(t, f.store.DeliveryUnknown(ctx, outgoing.ID))
			beforeOperator := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)
			_, err := f.db.Exec("UPDATE conversation_binding SET last_activity_at=? WHERE id=?", beforeOperator, binding.ID)
			storetest.Must(t, err)
			storetest.Must(t, f.root.ResolveDelivery(ctx, outgoing.ID, "delivered", "200.000001", false))
			var afterOperator time.Time
			storetest.Must(t, f.db.QueryRow("SELECT last_activity_at FROM conversation_binding WHERE id=?", binding.ID).Scan(&afterOperator))
			if !afterOperator.After(beforeOperator) {
				t.Fatal("operator-confirmed delivery did not record activity")
			}
			audit = false
		} else {
			storetest.Must(t, f.store.Delivered(ctx, outgoing.ID, "200.000001"))
		}
	}
	acknowledgeTerminationStatuses(t, f.store, "urgent", time.Now())
	return m, id, binding
}

func TestSoftContentRemovalKeepsRealAGUIContinuation(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			f := newRetentionFixture(t, driver)
			ctx := context.Background()
			original, id, binding := f.turn(t, "original", "old-thread", false)
			var outgoingID int
			storetest.Must(t, f.db.QueryRow("SELECT id FROM slack_outgoing_message WHERE job_id=?", id).Scan(&outgoingID))
			// First prove sparse omission keeps content; then clear the three bodies and
			// mark their common unit through the same generated native writer.
			server := metadataReaderServer(t, driver, f.db)
			target := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[jobwrite.WriterComponent]().PkgPath(), Name: "writer"}, Route: spec.RouteRef{Method: "PATCH", Path: "/internal/gateway/jobs"}}
			patch := &jobwrite.GatewayJob{}
			patch.SetId(int(id))
			patch.SetGatewayExecutionReason("retention-fixture")
			write := func() {
				in := &jobwrite.Input{}
				in.SetGatewayJobs([]*jobwrite.GatewayJob{patch})
				_, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in})
				storetest.Must(t, err)
			}
			write()
			var retained string
			storetest.Must(t, f.db.QueryRow("SELECT prompt_text FROM gateway_job WHERE id=?", id).Scan(&retained))
			if retained != original.Text {
				t.Fatal("sparse omission erased prompt")
			}
			patch = &jobwrite.GatewayJob{}
			patch.SetId(int(id))
			patch.SetPromptText(nil)
			patch.SetGatewayExecutionResultText(nil)
			purged := time.Now().UTC().Truncate(time.Millisecond)
			patch.SetContentPurgedAt(&purged)
			outgoing := &jobwrite.SlackOutgoingMessage{}
			outgoing.SetId(outgoingID)
			outgoing.SetSlackMessageText(nil)
			patch.SetSlackOutgoingMessages([]*jobwrite.SlackOutgoingMessage{outgoing})
			write()
			var promptNull, resultNull, outgoingNull bool
			var marker sql.NullTime
			storetest.Must(t, f.db.QueryRow("SELECT prompt_text IS NULL,gateway_execution_result_text IS NULL,content_purged_at FROM gateway_job WHERE id=?", id).Scan(&promptNull, &resultNull, &marker))
			storetest.Must(t, f.db.QueryRow("SELECT slack_message_text IS NULL FROM slack_outgoing_message WHERE id=?", outgoingID).Scan(&outgoingNull))
			if !promptNull || !resultNull || !outgoingNull || !marker.Valid || !marker.Time.Equal(purged) {
				t.Fatal("native explicit NULL or purge marker was lost")
			}
			inserted, err := f.store.Enqueue(ctx, original)
			storetest.Must(t, err)
			if inserted {
				t.Fatal("redaction lost original event deduplication")
			}
			_, _, nextBinding := f.turn(t, "followup", "old-thread", false)
			inputs := f.inputs()
			if len(inputs) != 2 || nextBinding.ID != binding.ID || nextBinding.ConversationID != binding.ConversationID || inputs[1].ThreadID != inputs[0].ThreadID {
				t.Fatal("redaction restarted the existing backend conversation")
			}
			if len(inputs[1].Messages) != 3 || string(inputs[1].Messages[0].Content) != `"Prompt original"` || string(inputs[1].Messages[1].Content) != `"The mouse helped the baker."` || !strings.Contains(string(inputs[1].State), "9007199254740993") {
				t.Fatal("followup lost retained AG-UI message/state context")
			}
			context, err := f.store.GetBinding(ctx, original.ThreadKey)
			storetest.Must(t, err)
			if len(context.ProtocolMessages) == 0 || len(context.ProtocolState) == 0 {
				t.Fatal("live continuation context was erased")
			}
		})
	}
}

func TestFullConversationRemovalForgetsOnlyItsCoherentGraph(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			f := newRetentionFixture(t, driver)
			ctx := context.Background()
			old, firstID, binding := f.turn(t, "first", "old-thread", true)
			_, unrelatedID, unrelated := f.turn(t, "other", "other-thread", false)
			_, secondID, _ := f.turn(t, "second", "old-thread", false)
			var auditCount int
			storetest.Must(t, f.db.QueryRow("SELECT COUNT(*) FROM slack_delivery_resolution").Scan(&auditCount))
			if auditCount != 1 {
				t.Fatal("fixture missed resolution dependency")
			}
			var nativeID int64
			storetest.Must(t, f.db.QueryRow("SELECT id FROM slack_agent_session_publication WHERE slack_channel_id=? AND slack_thread_root_message_id=?", old.ChannelID, old.RootTS).Scan(&nativeID))
			tx, err := f.db.BeginTx(ctx, nil)
			storetest.Must(t, err)
			defer tx.Rollback()
			// Explicit dependency order, only this disposable fixture's known IDs.
			for _, step := range []struct {
				sql  string
				args []any
			}{
				{"DELETE FROM slack_agent_session_publication WHERE id=?", []any{nativeID}},
				{"DELETE FROM slack_delivery_resolution WHERE outgoing_message_id IN (SELECT id FROM slack_outgoing_message WHERE job_id IN (?,?))", []any{firstID, secondID}},
				{"DELETE FROM slack_outgoing_message WHERE job_id IN (?,?)", []any{firstID, secondID}},
				{"DELETE FROM gateway_job WHERE id IN (?,?)", []any{firstID, secondID}},
				{"DELETE FROM conversation_binding WHERE id=?", []any{binding.ID}},
			} {
				_, err := tx.ExecContext(ctx, step.sql, step.args...)
				storetest.Must(t, err)
			}
			storetest.Must(t, tx.Commit())
			got, err := f.store.GetBinding(ctx, old.ThreadKey)
			storetest.Must(t, err)
			if got != nil {
				t.Fatal("full deletion left the old mapping")
			}
			for _, check := range []struct {
				sql string
				arg any
			}{
				{"SELECT COUNT(*) FROM gateway_job WHERE id=?", unrelatedID},
				{"SELECT COUNT(*) FROM conversation_binding WHERE id=?", unrelated.ID},
				{"SELECT COUNT(*) FROM slack_outgoing_message WHERE job_id=?", unrelatedID},
				{"SELECT COUNT(*) FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", unrelated.RootTS},
			} {
				var count int
				storetest.Must(t, f.db.QueryRow(check.sql, check.arg).Scan(&count))
				if count != 1 {
					t.Fatalf("full deletion damaged unrelated dependency: %s", check.sql)
				}
			}
			_, _, fresh := f.turn(t, "after-forgetting", "old-thread", false)
			inputs := f.inputs()
			if len(inputs) != 4 || fresh.ID == binding.ID || fresh.ConversationID == binding.ConversationID || inputs[3].ThreadID == inputs[0].ThreadID || len(inputs[3].Messages) != 1 {
				t.Fatal("old Slack thread did not start a fresh context after full deletion")
			}
			var history int
			storetest.Must(t, f.db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE slack_event_id=?", old.EventID).Scan(&history))
			if history != 0 {
				t.Fatal("full deletion kept event history")
			}
		})
	}
}

func TestConversationTimesTrackWorkWithoutHeartbeatActivity(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			f := newRetentionFixture(t, driver)
			ctx := context.Background()
			_, _, binding := f.turn(t, "complete-first", "tracked-thread", false)
			future := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Millisecond)
			_, err := f.db.Exec("UPDATE conversation_binding SET last_activity_at=? WHERE id=?", future, binding.ID)
			storetest.Must(t, err)
			next := storetest.Message("tracked-next", "tracked-thread", "alice")
			next.StatusMode = gateway.StatusHybrid
			inserted, err := f.store.Enqueue(ctx, next)
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("new activity was deduplicated")
			}
			activity := func() time.Time {
				var v time.Time
				storetest.Must(t, f.db.QueryRow("SELECT last_activity_at FROM conversation_binding WHERE id=?", binding.ID).Scan(&v))
				return v
			}
			if !activity().Equal(future) {
				t.Fatal("late activity moved its monotonic timestamp backwards")
			}
			duplicateBaseline := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Millisecond)
			_, err = f.db.Exec("UPDATE conversation_binding SET last_activity_at=? WHERE id=?", duplicateBaseline, binding.ID)
			storetest.Must(t, err)
			inserted, err = f.store.Enqueue(ctx, next)
			storetest.Must(t, err)
			if inserted || !activity().Equal(duplicateBaseline) {
				t.Fatal("duplicate intake changed activity")
			}
			queued := storetest.Message("accepted-after-past", "tracked-thread", "alice")
			queued.StatusMode = gateway.StatusHybrid
			inserted, err = f.store.Enqueue(ctx, queued)
			storetest.Must(t, err)
			if !inserted || !activity().After(duplicateBaseline) {
				t.Fatal("accepted queued prompt did not record activity")
			}

			acknowledgeTerminationStatuses(t, f.store, "urgent", time.Now())
			job, err := f.store.ClaimJob(ctx)
			storetest.Must(t, err)
			if job == nil {
				t.Fatal("missing next request")
			}
			_, err = f.store.BindJob(ctx, job.ID, binding.Decision)
			storetest.Must(t, err)
			storetest.Must(t, f.store.BeginQuery(ctx, job.ID))
			past := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)
			_, err = f.db.Exec("UPDATE conversation_binding SET last_activity_at=? WHERE id=?", past, binding.ID)
			storetest.Must(t, err)
			observed := time.Now().In(time.FixedZone("fixture", 2*60*60)).Add(123456 * time.Nanosecond)
			storetest.Must(t, f.store.RecordObservation(ctx, job.ID, "running", observed))
			acknowledgeTerminationStatuses(t, f.store, "urgent", time.Now())
			heartbeatAt := time.Now().Add(45 * time.Second)
			acknowledgeTerminationStatuses(t, f.store, "heartbeat", heartbeatAt)
			if !activity().Equal(past) {
				t.Fatal("observation or heartbeat counted as conversation activity")
			}
			var recorded, unfinished sql.NullTime
			storetest.Must(t, f.db.QueryRow("SELECT backend_observed_at,gateway_execution_finished_at FROM gateway_job WHERE id=?", job.ID).Scan(&recorded, &unfinished))
			if !recorded.Valid || !recorded.Time.Equal(observed.UTC().Truncate(time.Millisecond)) || unfinished.Valid {
				t.Fatal("observation UTC/millisecond precision or unfinished marker changed")
			}
			storetest.Must(t, f.store.Complete(ctx, job.ID, job.GatewayRequestID, "done", []gateway.ReplyPart{{Text: "done", Format: gateway.ReplyPlain}}))
			var finished sql.NullTime
			storetest.Must(t, f.db.QueryRow("SELECT gateway_execution_finished_at FROM gateway_job WHERE id=?", job.ID).Scan(&finished))
			if !finished.Valid || finished.Time.Nanosecond()%int(time.Millisecond) != 0 || !activity().After(past) {
				t.Fatal("local completion did not record factual completion and activity")
			}
			delivery, err := f.store.ClaimDelivery(ctx)
			storetest.Must(t, err)
			if delivery == nil {
				t.Fatal("missing answer")
			}
			_, err = f.db.Exec("UPDATE conversation_binding SET last_activity_at=? WHERE id=?", past, binding.ID)
			storetest.Must(t, err)
			storetest.Must(t, f.store.Delivered(ctx, delivery.ID, "300.000001"))
			if !activity().After(past) {
				t.Fatal("confirmed response did not record activity")
			}
			acknowledgeTerminationStatuses(t, f.store, "urgent", heartbeatAt.Add(time.Second))
			beforeClose := activity()
			_, err = f.root.ReconcileIntegrations(ctx, nil)
			storetest.Must(t, err)
			var closed, firstFinished sql.NullTime
			storetest.Must(t, f.db.QueryRow("SELECT closed_at FROM conversation_binding WHERE id=?", binding.ID).Scan(&closed))
			storetest.Must(t, f.db.QueryRow("SELECT gateway_execution_finished_at FROM gateway_job WHERE id=?", job.ID).Scan(&firstFinished))
			if !closed.Valid || !firstFinished.Valid || !firstFinished.Time.Equal(finished.Time) || !activity().Equal(beforeClose) {
				t.Fatal("configuration closure changed completed execution/activity or omitted closed_at")
			}
			_, err = f.root.ReconcileIntegrations(ctx, nil)
			storetest.Must(t, err)
			var repeated sql.NullTime
			storetest.Must(t, f.db.QueryRow("SELECT closed_at FROM conversation_binding WHERE id=?", binding.ID).Scan(&repeated))
			if !repeated.Valid || !repeated.Time.Equal(closed.Time) {
				t.Fatal("repeated closure replaced its original timestamp")
			}
		})
	}
}
