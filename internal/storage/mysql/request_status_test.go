package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func claimStatus(t *testing.T, s *Store, now time.Time, kind, phase string) *gateway.GatewayJobProgress {
	t.Helper()
	status, err := s.ClaimStatus(context.Background(), now, kind)
	storetest.Must(t, err)
	if phase == "" {
		if status != nil {
			t.Fatalf("unexpected status: %+v", status)
		}
		return nil
	}
	if status == nil || status.Phase != phase {
		t.Fatalf("status: %+v; want %s", status, phase)
	}
	return status
}
func confirmStatus(t *testing.T, s *Store, status *gateway.GatewayJobProgress, now time.Time) {
	t.Helper()
	state := "ready"
	if status.Terminal {
		state = "settled"
	}
	ts := status.SlackTS
	if ts == "" {
		ts = "2.000001"
	}
	storetest.Must(t, s.FinishStatus(context.Background(), *status, gateway.StatusResult{State: state, SlackTS: ts, NextRefreshAt: now.Add(gateway.DefaultStatusRefresh)}, now))
}
func startStatusJob(t *testing.T, s *Store) *gateway.GatewayJob {
	t.Helper()
	ctx := context.Background()
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if j == nil {
		t.Fatal("missing job")
	}
	b, err := s.BindJob(ctx, j.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	if b.ConversationID == "" {
		storetest.Must(t, s.BeginCreate(ctx, b.ID))
		storetest.Must(t, s.SetConversation(ctx, b.ID, "conversation"))
	}
	storetest.Must(t, s.BeginQuery(ctx, j.ID))
	return j
}

func TestStatusIntentionAtomicDeduplicatedAndConstrained(t *testing.T) {
	s, db := fixtureDB(t)
	ctx := context.Background()
	m := storetest.Message("event", "root", "alice")
	_, err := db.Exec(`CREATE TRIGGER fail_status BEFORE INSERT ON gateway_job FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'`)
	storetest.Must(t, err)
	if _, err := s.Enqueue(ctx, m); err == nil {
		t.Fatal("ignored status persistence failure")
	}
	var n int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&n))
	if n != 0 {
		t.Fatal("partially committed intake")
	}
	_, err = db.Exec("DROP TRIGGER fail_status")
	storetest.Must(t, err)
	for i := 0; i < 2; i++ {
		inserted, err := s.Enqueue(ctx, m)
		storetest.Must(t, err)
		if inserted != (i == 0) {
			t.Fatal("incorrect dedup")
		}
	}
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&n))
	if n != 1 {
		t.Fatal("duplicated status")
	}
	if _, err = db.Exec("UPDATE gateway_job SET integration_id=999999"); err == nil {
		t.Fatal("missing integration FK")
	}
	if _, err = db.Exec("INSERT INTO gateway_job SELECT * FROM gateway_job"); err == nil {
		t.Fatal("missing unique job")
	}
}

func TestStatusProjectionLongRequestAndMultipartDelivery(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, storetest.Message("a", "root", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "received")
	confirmStatus(t, s, a, now)
	j := startStatusJob(t, s)
	a = claimStatus(t, s, now, "urgent", "contacting")
	confirmStatus(t, s, a, now)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
	a = claimStatus(t, s, now, "urgent", "running")
	confirmStatus(t, s, a, now)
	claimStatus(t, s, now.Add(29*time.Second), "heartbeat", "")
	now = now.Add(30 * time.Second)
	a = claimStatus(t, s, now, "heartbeat", "running")
	confirmStatus(t, s, a, now)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "unknown", now))
	a = claimStatus(t, s, now, "urgent", "checking")
	if a.ObservedState != "running" || a.ObservedAt.IsZero() || !strings.Contains(gateway.StatusText(*a, now), "Last confirmed state: running at") {
		t.Fatal("last confirmation lost or invented")
	}
	confirmStatus(t, s, a, now)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "completed", now))
	storetest.Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "result", []gateway.ReplyPart{{Text: "one", Format: gateway.ReplyPlain}, {Text: "two", Format: gateway.ReplyPlain}}))
	a = claimStatus(t, s, now, "urgent", "delivering")
	confirmStatus(t, s, a, now)
	for i := 0; i < 2; i++ {
		d, err := s.ClaimDelivery(ctx)
		storetest.Must(t, err)
		if d == nil || d.Part != i {
			t.Fatal("part order lost")
		}
		storetest.Must(t, s.Delivered(ctx, d.ID, "3.000001"))
		if i == 0 {
			claimStatus(t, s, now, "urgent", "")
		}
	}
	a = claimStatus(t, s, now, "urgent", "completed")
	if !a.Terminal || a.SlackTS != "2.000001" {
		t.Fatal("created a second status")
	}
	confirmStatus(t, s, a, now)
	claimStatus(t, s, now.Add(time.Hour), "heartbeat", "")
	metadata, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if metadata.GatewayJobs[0].Outcome != "completed" || metadata.GatewayJobs[0].AckLatencyMS == nil {
		t.Fatal("missing diagnostics")
	}
}

func TestStatusQueueBlockAndOriginalIdentity(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	now := time.Now()
	for _, author := range []string{"alice", "bob"} {
		_, err := s.Enqueue(ctx, storetest.Message(author, "same", author))
		storetest.Must(t, err)
	}
	a := claimStatus(t, s, now, "urgent", "received")
	confirmStatus(t, s, a, now)
	b := claimStatus(t, s, now, "urgent", "queued")
	metadata, err := s.GetStatusContext(ctx, *b)
	storetest.Must(t, err)
	if metadata.Message.AuthorID != "bob" || metadata.ConversationBinding != nil {
		t.Fatal("wrong original identity or status created binding")
	}
	forged := *b
	forged.ChannelID = "OTHER"
	if _, err = s.GetStatusContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("forged destination accepted")
	}
	forged = *b
	forged.SlackTS = "999.0001"
	if _, err = s.GetStatusContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("forged status timestamp accepted")
	}
	confirmStatus(t, s, b, now)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Review(ctx, j.ID, "test_review"))
	a = claimStatus(t, s, now, "urgent", "needs_review")
	confirmStatus(t, s, a, now)
	b = claimStatus(t, s, now, "urgent", "blocked")
	confirmStatus(t, s, b, now)
	if j, err := s.ClaimJob(ctx); err != nil || j != nil {
		t.Fatal("blocked job was claimed")
	}
}

func TestStatusBackendFailureCancellationAndFastPath(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := context.Background()
			s := fixture(t).Store.(*Store)
			now := time.Now()
			_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
			storetest.Must(t, err)
			a := claimStatus(t, s, now, "urgent", "received")
			confirmStatus(t, s, a, now)
			j := startStatusJob(t, s)
			storetest.Must(t, s.RecordObservation(ctx, j.ID, outcome, now))
			storetest.Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
			d, err := s.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Delivered(ctx, d.ID, "4.000001"))
			claimStatus(t, s, now, "urgent", outcome)
		})
	}
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	_, err := s.Enqueue(ctx, storetest.Message("fast", "one", "alice"))
	storetest.Must(t, err)
	j := startStatusJob(t, s)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "completed", time.Now()))
	storetest.Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
	claimStatus(t, s, time.Now(), "urgent", "")
	metadata, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if metadata.GatewayJobs[0].ProgressState != "skipped" {
		t.Fatal("fast path not persisted")
	}
}

func TestStatusRecoveryCoalescesKnownUpdatesAndQuarantinesFirstPost(t *testing.T) {
	ctx := context.Background()
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_post", true: "update"}[known], func(t *testing.T) {
			f := fixture(t)
			s := f.Store.(*Store)
			now := time.Now()
			_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
			storetest.Must(t, err)
			a := claimStatus(t, s, now, "urgent", "received")
			if known {
				confirmStatus(t, s, a, now)
				j := startStatusJob(t, s)
				storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
				claimStatus(t, s, now, "urgent", "running")
			}
			s = f.Reopen().(*Store)
			if !known {
				claimStatus(t, s, now, "urgent", "")
				return
			}
			j, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Review(ctx, j.ID, "test"))
			a = claimStatus(t, s, now, "urgent", "needs_review")
			if a.SlackTS != "2.000001" {
				t.Fatal("known status lost on restart")
			}
		})
	}
}

func TestStatusRateLimitGatesFinalRepliesAndSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	f, db := fixtureWithDB(t)
	s := f.Store.(*Store)
	now := time.Now()
	_, err := s.Enqueue(ctx, storetest.Message("a", "one", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "received")
	until := now.Add(time.Hour)
	storetest.Must(t, s.FinishStatus(ctx, *a, gateway.StatusResult{State: "ready", Reason: "rate_limited", RetryAt: until, RateLimitedUntil: until}, now))
	j := startStatusJob(t, s)
	storetest.Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
	s = f.Reopen().(*Store)
	if d, err := s.ClaimDelivery(ctx); err != nil || d != nil {
		t.Fatal("status cooldown did not gate final reply")
	}
	at, err := s.DeliveryNotBefore(ctx, "T")
	storetest.Must(t, err)
	if at.UnixMilli() != until.UnixMilli() {
		t.Fatal("cooldown lost")
	}
	claimStatus(t, s, now, "urgent", "")
	// The converse: a final POST's Retry-After gates status writes too.
	_, err = db.Exec("UPDATE gateway_job SET slack_progress_rate_limited_until=NULL,slack_progress_next_attempt_at=NULL")
	storetest.Must(t, err)
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.RetryDelivery(ctx, d.ID, until))
	_, err = s.Enqueue(ctx, storetest.Message("b", "two", "bob"))
	storetest.Must(t, err)
	claimStatus(t, s, now, "urgent", "")
}
