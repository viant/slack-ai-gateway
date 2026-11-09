package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func nativeMessage(event, root, author string) gateway.Message {
	m := storetest.Message(event, root, author)
	m.StatusMode = gateway.StatusAgentSession
	return m
}
func acknowledgeSession(t *testing.T, s *Store, a *gateway.RequestStatus, now time.Time) {
	t.Helper()
	storetest.Must(t, s.FinishStatus(context.Background(), *a, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: a.SessionState, SessionStatus: "processing", NextRefreshAt: now.Add(gateway.AgentSessionRefresh)}, now))
}
func finishNativeJob(t *testing.T, s *Store, j *gateway.Job, count int) {
	t.Helper()
	parts := make([]gateway.ReplyPart, count)
	for i := range parts {
		parts[i] = gateway.ReplyPart{Text: "answer", Format: gateway.ReplyPlain}
	}
	storetest.Must(t, s.Complete(context.Background(), j.ID, j.RequestID, "answer", parts))
}
func deliverNativePart(t *testing.T, s *Store) {
	t.Helper()
	d, err := s.ClaimDelivery(context.Background())
	storetest.Must(t, err)
	if d == nil {
		t.Fatal("missing final part")
	}
	storetest.Must(t, s.Delivered(context.Background(), d.ID, "3.000001"))
}
func TestAgentSessionThreadAggregationAndInFlightIntake(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "processing")
	if a.SlackTS != "" || a.InitiatorUserID != "alice" || a.Mode != gateway.StatusAgentSession {
		t.Fatal("wrong session target")
	}
	acknowledgeSession(t, s, a, now)
	j := startStatusJob(t, s)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
	claimStatus(t, s, now, "urgent", "") // Phase changes do not create native writes.
	claimStatus(t, s, now.Add(9*time.Minute), "heartbeat", "")
	a = claimStatus(t, s, now.Add(10*time.Minute), "heartbeat", "processing")
	acknowledgeSession(t, s, a, now)
	finishNativeJob(t, s, j, 2)
	deliverNativePart(t, s)
	claimStatus(t, s, now, "urgent", "")
	deliverNativePart(t, s)
	a = claimStatus(t, s, now, "urgent", "active")
	// New author arrives while active is in flight. Its revision must survive the old ack.
	_, err = s.Enqueue(ctx, nativeMessage("b", "root", "bob"))
	storetest.Must(t, err)
	acknowledgeSession(t, s, a, now)
	b := claimStatus(t, s, now, "urgent", "processing")
	metadata, err := s.GetStatusContext(ctx, *b)
	storetest.Must(t, err)
	if metadata.Message.AuthorID != "bob" || b.InitiatorUserID != "alice" {
		t.Fatal("author confused with session initiator")
	}
	forged := *b
	forged.ChannelID = "OTHER"
	if _, err = s.GetStatusContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("forged target accepted")
	}
	acknowledgeSession(t, s, b, now)
	j = startStatusJob(t, s)
	finishNativeJob(t, s, j, 1)
	claimStatus(t, s, now, "urgent", "")
	deliverNativePart(t, s)
	b = claimStatus(t, s, now, "urgent", "active")
	acknowledgeSession(t, s, b, now)
	claimStatus(t, s, now.Add(time.Hour), "heartbeat", "")
	report, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(report.AgentSessions) != 1 || report.AgentSessions[0].AgentStatus != "active" || report.AgentSessions[0].SessionStatus != "processing" {
		t.Fatal("own and aggregate status lost")
	}
	for _, row := range report.RequestStatuses {
		if row.SlackTS != "" {
			t.Fatal("native publication invented message timestamp")
		}
	}
}
func TestAgentSessionBlockerResolutionAndObservation(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "processing")
	acknowledgeSession(t, s, a, now)
	j := startStatusJob(t, s)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "unknown", now))
	claimStatus(t, s, now, "urgent", "")
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "waiting_for_user", now))
	a = claimStatus(t, s, now, "urgent", "suspended")
	acknowledgeSession(t, s, a, now)
	_, err = s.Enqueue(ctx, nativeMessage("b", "root", "bob"))
	storetest.Must(t, err)
	a = claimStatus(t, s, now, "urgent", "suspended")
	acknowledgeSession(t, s, a, now)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
	a = claimStatus(t, s, now, "urgent", "processing")
	acknowledgeSession(t, s, a, now)
	finishNativeJob(t, s, j, 1)
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.DeliveryUnknown(ctx, d.ID))
	a = claimStatus(t, s, now, "urgent", "suspended")
	acknowledgeSession(t, s, a, now)
	// Existing operator reconciliation removes the blocker instead of latching history.
	storetest.Must(t, s.ResolveDelivery(ctx, d.ID, "delivered", "3.000001", false))
	a = claimStatus(t, s, now, "urgent", "processing")
	acknowledgeSession(t, s, a, now)
	j = startStatusJob(t, s)
	storetest.Must(t, s.Review(ctx, j.ID, "backend_auth_or_identity_rejected"))
	a = claimStatus(t, s, now, "terminal", "suspended")
	acknowledgeSession(t, s, a, now)
	claimStatus(t, s, now, "terminal", "")
}
func TestAgentSessionRecoveryRetryCooldownAndLatestProjection(t *testing.T) {
	ctx := context.Background()
	f, db := fixtureWithDB(t)
	s := f.Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
	storetest.Must(t, err)
	claimStatus(t, s, now, "urgent", "processing")
	s = f.Reopen().(*Store)
	claimStatus(t, s, now, "urgent", "")
	now = now.Add(time.Minute)
	a := claimStatus(t, s, now, "urgent", "processing")
	until := now.Add(time.Hour)
	storetest.Must(t, s.FinishStatus(ctx, *a, gateway.StatusResult{State: "ready", Reason: "rate_limited", RetryAt: until, RateLimitedUntil: until}, now))
	j := startStatusJob(t, s)
	finishNativeJob(t, s, j, 1)
	s = f.Reopen().(*Store)
	if d, err := s.ClaimDelivery(ctx); err != nil || d != nil {
		t.Fatal("native cooldown did not gate final parts")
	}
	at, err := s.DeliveryNotBefore(ctx, "T")
	storetest.Must(t, err)
	if at.UnixMilli() != until.UnixMilli() {
		t.Fatal("lost persistent cooldown")
	}
	claimStatus(t, s, now, "urgent", "")
	// Expire persisted cooldown; retry must project the latest final delivery state.
	_, err = db.Exec("UPDATE agent_session_publications SET rate_limited_until=0,next_attempt_at=0")
	storetest.Must(t, err)
	deliverNativePart(t, s)
	a = claimStatus(t, s, now, "urgent", "active")
	storetest.Must(t, s.FinishStatus(ctx, *a, gateway.StatusResult{State: "ready", Reason: "send_unknown", RetryAt: now.Add(gateway.StatusRetry)}, now))
	claimStatus(t, s, now, "urgent", "")
	a = claimStatus(t, s, now.Add(gateway.StatusRetry), "urgent", "active")
	acknowledgeSession(t, s, a, now)
}
func TestAgentSessionFastAnswersFailuresAndModeSwitch(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			s := fixture(t).Store.(*Store)
			now := time.Now()
			_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
			storetest.Must(t, err)
			j := startStatusJob(t, s)
			storetest.Must(t, s.RecordObservation(ctx, j.ID, outcome, now))
			finishNativeJob(t, s, j, 1)
			deliverNativePart(t, s)
			claimStatus(t, s, now, "urgent", "") // No empty session for work already delivered.
			_, err = s.Enqueue(ctx, nativeMessage("b", "root", "bob"))
			storetest.Must(t, err)
			a := claimStatus(t, s, now, "urgent", "processing")
			acknowledgeSession(t, s, a, now)
			j = startStatusJob(t, s)
			finishNativeJob(t, s, j, 1)
			deliverNativePart(t, s)
			a = claimStatus(t, s, now, "urgent", "active")
			acknowledgeSession(t, s, a, now)
			_, err = s.Enqueue(ctx, storetest.Message("c", "root", "alice"))
			storetest.Must(t, err)
			a = claimStatus(t, s, now, "urgent", "received")
			if a.Mode != gateway.StatusMessage {
				t.Fatal("mode belongs to binding instead of new question")
			}
			confirmStatus(t, s, a, now)
			j = startStatusJob(t, s)
			finishNativeJob(t, s, j, 1)
			deliverNativePart(t, s)
			a = claimStatus(t, s, now, "urgent", "delivered")
			confirmStatus(t, s, a, now)
			_, err = s.Enqueue(ctx, nativeMessage("d", "root", "alice"))
			storetest.Must(t, err)
			claimStatus(t, s, now, "urgent", "processing")
		})
	}
}
func TestAgentSessionPermanentFailureIndependentThreadsAndNewIntention(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "processing")
	storetest.Must(t, s.FinishStatus(ctx, *a, gateway.StatusResult{State: "failed", Reason: "feature_disabled"}, now))
	claimStatus(t, s, now.Add(time.Hour), "heartbeat", "")
	j := startStatusJob(t, s)
	finishNativeJob(t, s, j, 1)
	deliverNativePart(t, s)
	claimStatus(t, s, now, "urgent", "")
	m := nativeMessage("dm", "dmroot", "bob")
	m.ChannelID = "D"
	m.Surface = gateway.SurfaceDirectMessage
	_, err = s.Enqueue(ctx, m)
	storetest.Must(t, err)
	a = claimStatus(t, s, now, "urgent", "processing")
	if a.ChannelID != "D" || a.Surface != gateway.SurfaceDirectMessage {
		t.Fatal("independent DM incorrectly aggregated")
	}
	acknowledgeSession(t, s, a, now)
	_, err = s.Enqueue(ctx, nativeMessage("b", "root", "bob"))
	storetest.Must(t, err)
	a = claimStatus(t, s, now, "urgent", "processing")
	if a.ChannelID != "C" {
		t.Fatal("new intention did not retry failed publication")
	}
}
