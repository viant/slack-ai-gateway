package datly_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// A native Slack session belongs to a physical bot/thread even when its
// originating integration assignment has been replaced.
func TestNativeSessionCleanupRetainsPhysicalThreadAcrossAssignments(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a := testIdentity("steward", "A")
			registerNativeIntegrations(t, root, &a)
			as := root.Scope(a.ID)
			now := time.Now()
			message := storetest.Message("event-a", "100.000001", "alice")
			message.StatusMode = gateway.StatusAgentSession
			inserted, err := as.Enqueue(ctx, message)
			storetest.Must(t, err)
			if !inserted {
				t.Fatal("initial event missing")
			}
			initial := claimSharedSession(t, as, a.ID, "processing", now)
			acknowledgeSharedSession(t, as, initial, now)
			job := startSharedSessionJob(t, as)
			storetest.Must(t, as.RecordObservation(ctx, job.ID, "completed", now))
			storetest.Must(t, as.Complete(ctx, job.ID, job.GatewayRequestID, "old answer", []gateway.ReplyPart{{Text: "first", Format: gateway.ReplyPlain}, {Text: "second", Format: gateway.ReplyPlain}}))
			deliverSharedSessionPart(t, as)
			b := a
			b.BackendAgentID = "new-agent"
			unrelated := testIdentity("other-bot", "B")
			registerNativeIntegrations(t, root, &b, &unrelated)
			bs := root.Scope(b.ID)
			cleanup, err := bs.ClaimStatus(ctx, now.Add(time.Second), "integration_terminal")
			storetest.Must(t, err)
			if cleanup == nil || cleanup.Target != gateway.StatusTargetAgentSession || cleanup.SessionState != "active" || !cleanup.Terminal {
				t.Fatalf("closed old work kept physical session working: %+v", cleanup)
			}
			original, err := bs.GetStatusContext(ctx, *cleanup)
			storetest.Must(t, err)
			if original.Message.AuthorID != "alice" || cleanup.InitiatorUserID != "alice" || original.IntegrationActive || original.Kind != gateway.DeliveryIntegrationNotice {
				t.Fatal("cleanup lost the original author or trusted inactive context")
			}
			if _, err = root.Scope(unrelated.ID).GetStatusContext(ctx, *cleanup); !errors.Is(err, gateway.ErrConflict) {
				t.Fatal("other bot borrowed historical cleanup context")
			}
			acknowledgeSharedSession(t, bs, cleanup, now.Add(time.Second))
			notice, err := bs.ClaimDelivery(ctx)
			storetest.Must(t, err)
			if notice == nil || notice.Kind != gateway.DeliveryIntegrationNotice {
				t.Fatal("current bot did not claim old closure notice")
			}
			storetest.Must(t, bs.Delivered(ctx, notice.ID, "200.000002"))
			followup := storetest.Message("event-b", "100.000001", "bob")
			followup.MessageTS = "100.000002"
			followup.StatusMode = gateway.StatusAgentSession
			_, err = bs.Enqueue(ctx, followup)
			storetest.Must(t, err)
			// A local stale-thread response must not create another native session
			// or move its immutable original-assignment anchor.
			var count int
			var anchor int64
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", message.RootTS).Scan(&count))
			storetest.Must(t, db.QueryRow("SELECT initial_integration_id FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", message.RootTS).Scan(&anchor))
			if count != 1 || anchor != a.ID {
				t.Fatal("new assignment replaced/duplicated the physical thread publication")
			}
			if job, err := bs.ClaimJob(ctx); err != nil || job != nil {
				t.Fatalf("stale thread resumed backend work: %+v %v", job, err)
			}
			for range 5 {
				status, err := bs.ClaimStatus(ctx, now.Add(2*time.Second), "urgent")
				storetest.Must(t, err)
				if status == nil {
					break
				}
				if status.Target == gateway.StatusTargetAgentSession && status.SessionState != "active" {
					t.Fatal("local closure response reactivated old session")
				}
				acknowledgeSharedSession(t, bs, status, now.Add(2*time.Second))
			}
			view, err := root.Status(ctx, 20)
			storetest.Must(t, err)
			for _, part := range view.SlackOutgoingMessages {
				if part.Part == 0 && part.JobID == job.ID && part.State != "delivered" {
					t.Fatal("confirmed old response part changed")
				}
			}
		})
	}
}

func assertNoSharedSession(t *testing.T, store storage.Store, now time.Time) {
	t.Helper()
	status, err := store.ClaimStatus(context.Background(), now, "urgent")
	storetest.Must(t, err)
	if status != nil {
		t.Fatalf("unexpected shared status claim: %+v", status)
	}
}

func claimSharedSession(t *testing.T, store storage.Store, integration int64, state string, now time.Time) *gateway.GatewayJobProgress {
	t.Helper()
	status, err := store.ClaimStatus(context.Background(), now, "urgent")
	storetest.Must(t, err)
	if status == nil || status.Target != gateway.StatusTargetAgentSession || status.IntegrationID != integration || status.SessionState != state {
		t.Fatalf("native claim=%+v, want owner=%d state=%s", status, integration, state)
	}
	return status
}

func acknowledgeSharedSession(t *testing.T, store storage.Store, status *gateway.GatewayJobProgress, now time.Time) {
	t.Helper()
	storetest.Must(t, store.FinishStatus(context.Background(), *status, gateway.StatusResult{
		State: "settled", Acknowledged: true, AgentStatus: status.SessionState, SessionStatus: status.SessionState,
		NextRefreshAt: now.Add(gateway.AgentSessionRefresh),
	}, now))
}

func startSharedSessionJob(t *testing.T, store storage.Store) *gateway.GatewayJob {
	t.Helper()
	ctx := context.Background()
	job, err := store.ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil {
		t.Fatal("missing integration-scoped job")
	}
	binding, err := store.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	storetest.Must(t, store.BeginCreate(ctx, binding.ID))
	storetest.Must(t, store.SetConversation(ctx, binding.ID, "backend-thread-"+job.GatewayRequestID))
	storetest.Must(t, store.BeginQuery(ctx, job.ID))
	return job
}

func deliverSharedSessionPart(t *testing.T, store storage.Store) {
	t.Helper()
	delivery, err := store.ClaimDelivery(context.Background())
	storetest.Must(t, err)
	if delivery == nil {
		t.Fatal("missing integration-scoped reply part")
	}
	storetest.Must(t, store.Delivered(context.Background(), delivery.ID, "200.000001"))
}
