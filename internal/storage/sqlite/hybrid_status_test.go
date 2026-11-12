package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// Exercise the real claim/receipt path, including the original author's policy
// context. Bound the loop so an accidentally dirty receipt cannot hang a test.
func settleStatuses(t *testing.T, s *scopedFixture, now time.Time) []gateway.GatewayJobProgress {
	t.Helper()
	var calls []gateway.GatewayJobProgress
	for range 20 {
		a, err := s.ClaimStatus(context.Background(), now, "urgent")
		storetest.Must(t, err)
		if a == nil {
			return calls
		}
		_, err = s.GetStatusContext(context.Background(), *a)
		storetest.Must(t, err)
		calls = append(calls, *a)
		if a.Target == gateway.StatusTargetAgentSession {
			acknowledgeSession(t, s, a, now)
		} else {
			confirmStatus(t, s, a, now)
		}
	}
	t.Fatal("status receipts never converged")
	return nil
}

func TestOpenNativeCyclePublishesMessageAuthFailureDuringShutdown(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "thread", "alice"))
	storetest.Must(t, err)
	a := claimStatus(t, s, now, "urgent", "processing")
	acknowledgeSession(t, s, a, now)
	j := startStatusJob(t, s)
	finishNativeJob(t, s, j, 1)
	deliverNativePart(t, s)
	// Message intake occurs before active is confirmed, keeping the cycle open.
	_, err = s.Enqueue(ctx, storetest.Message("b", "thread", "bob"))
	storetest.Must(t, err)
	j = startStatusJob(t, s)
	storetest.Must(t, s.Review(ctx, j.ID, "backend_auth_or_identity_rejected"))
	seen := map[gateway.StatusTarget]bool{}
	for range 2 {
		a, err = s.ClaimStatus(ctx, now, "terminal")
		storetest.Must(t, err)
		if a == nil {
			t.Fatal("missing terminal publication")
		}
		if a.Target == gateway.StatusTargetAgentSession {
			if a.SessionState != "suspended" || a.Mode != gateway.StatusAgentSession {
				t.Fatal("message auth failure did not suspend the open native cycle")
			}
			metadata, err := s.GetStatusContext(ctx, *a)
			storetest.Must(t, err)
			if metadata.Message.AuthorID != "alice" {
				t.Fatal("native authorization used another question's author")
			}
			acknowledgeSession(t, s, a, now)
		} else {
			confirmStatus(t, s, a, now)
		}
		seen[a.Target] = true
	}
	if len(seen) != 2 {
		t.Fatal("shutdown did not publish both affected targets")
	}
}

func TestRestartDuringWorkPreservesEveryPresentationModePair(t *testing.T) {
	for _, before := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		for _, after := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
			t.Run(string(before)+"_to_"+string(after), func(t *testing.T) {
				ctx := context.Background()
				f := fixture(t)
				s := f.Store.(*scopedFixture)
				now := time.Now()
				first := storetest.Message("a", "thread", "alice")
				first.StatusMode = before
				_, err := s.Enqueue(ctx, first)
				storetest.Must(t, err)
				settleStatuses(t, s, now)
				j := startStatusJob(t, s)
				storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
				settleStatuses(t, s, now)
				// Interrupt every applicable publication after its claim. Known
				// message updates and native writes must both be recoverable.
				storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now.Add(gateway.AgentSessionRefresh)))
				claimed := 0
				for range 2 {
					a, err := s.ClaimStatus(ctx, now.Add(gateway.AgentSessionRefresh), "heartbeat")
					storetest.Must(t, err)
					if a != nil {
						claimed++
					}
				}
				wantClaims := 1
				if before == gateway.StatusHybrid {
					wantClaims = 2
				}
				if claimed != wantClaims {
					t.Fatalf("claimed %d targets, want %d", claimed, wantClaims)
				}
				s = f.Reopen().(*scopedFixture)
				now = now.Add(11 * time.Minute)
				second := storetest.Message("b", "thread", "bob")
				second.StatusMode = after
				_, err = s.Enqueue(ctx, second)
				storetest.Must(t, err)
				settleStatuses(t, s, now)
				resumed, err := s.ClaimJob(ctx)
				storetest.Must(t, err)
				if resumed == nil || resumed.ID != j.ID || resumed.GatewayRequestID != j.GatewayRequestID || resumed.BackendSubmissionStartedAt.IsZero() {
					t.Fatal("restart lost the known query; it must reconcile instead of resubmitting")
				}
				if other, err := s.ClaimJob(ctx); err != nil || other != nil {
					t.Fatal("second question bypassed active question in the same thread")
				}
				finishNativeJob(t, s, resumed, 2)
				for range 2 {
					deliverNativePart(t, s)
					for _, call := range settleStatuses(t, s, now) {
						if call.SessionState == "active" {
							t.Fatal("first question cleared the second question's processing")
						}
					}
				}
				j = startStatusJob(t, s)
				storetest.Must(t, s.RecordObservation(ctx, j.ID, "running", now))
				settleStatuses(t, s, now)
				finishNativeJob(t, s, j, 2)
				deliverNativePart(t, s)
				for _, call := range settleStatuses(t, s, now) {
					if call.SessionState == "active" {
						t.Fatal("native state cleared before the last response part")
					}
				}
				deliverNativePart(t, s)
				settleStatuses(t, s, now)
				report, err := s.Status(ctx, 10)
				storetest.Must(t, err)
				if len(report.GatewayJobs) != 2 {
					t.Fatal("restart duplicated the question")
				}
				for _, row := range report.GatewayJobs {
					want := before
					if row.ID == j.ID {
						want = after
					}
					if row.Mode != string(want) || (row.SlackTS != "") != (want != gateway.StatusAgentSession) {
						t.Fatalf("question mode/receipt changed across restart: %+v", row)
					}
					if row.SlackTS != "" && (row.FirstSentAt == 0 || row.ProgressState != "settled") {
						t.Fatal("message receipt or terminal update was lost")
					}
				}
				for _, a := range report.SlackAgentSessionPublications {
					if a.AgentStatus != "active" || a.State != "settled" || a.Revision != a.SentRevision {
						t.Fatalf("native indicator left open: %+v", a)
					}
				}
				if len(report.SlackAgentSessionPublications) == 0 && (before != gateway.StatusMessage || after != gateway.StatusMessage) {
					t.Fatal("native cycle missing")
				}
				// After active is confirmed, message-only intake must not reopen
				// the native cycle, even after another process restart.
				s = f.Reopen().(*scopedFixture)
				_, err = s.Enqueue(ctx, storetest.Message("c", "thread", "charlie"))
				storetest.Must(t, err)
				for _, call := range settleStatuses(t, s, now) {
					if call.Target != gateway.StatusTargetMessage {
						t.Fatal("closed native cycle reopened for message-only question")
					}
				}
			})
		}
	}
}

func TestHybridReceiptsAreIndependentAndClaimsAlternateTargets(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	now := time.Now()
	// Native has a backlog: the hybrid message must still get the next slot.
	for _, root := range []string{"first", "second", "third"} {
		m := nativeMessage(root, root, "alice")
		if root == "first" {
			m.StatusMode = gateway.StatusHybrid
		}
		_, err := s.Enqueue(ctx, m)
		storetest.Must(t, err)
	}
	native := claimStatus(t, s, now, "urgent", "processing")
	message := claimStatus(t, s, now, "urgent", "received")
	if native.JobID != message.JobID || native.Target != gateway.StatusTargetAgentSession || message.Target != gateway.StatusTargetMessage || message.Mode != gateway.StatusHybrid {
		t.Fatal("hybrid target starved or lost its mode")
	}
	for _, a := range []*gateway.GatewayJobProgress{native, message} {
		_, err := s.GetStatusContext(ctx, *a)
		storetest.Must(t, err)
		forged := *a
		forged.Mode = gateway.StatusMessage
		if _, err = s.GetStatusContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("accepted forged presentation mode")
		}
	}
	acknowledgeSession(t, s, native, now)
	// Native ACK cannot consume or overwrite the independently claimed message.
	_, err := s.GetStatusContext(ctx, *message)
	storetest.Must(t, err)
	confirmStatus(t, s, message, now)
	report, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	for _, row := range report.GatewayJobs {
		if row.ID == message.JobID && (row.SlackTS != "2.000001" || row.Target != "message" || row.SentRevision != message.Revision) {
			t.Fatalf("message ACK corrupted: %+v", row)
		}
	}
	for _, row := range report.SlackAgentSessionPublications {
		if row.StatusSourceJobID == native.JobID && (row.Mode != "hybrid" || row.Target != "agent_session" || row.SentRevision != native.Revision) {
			t.Fatalf("native ACK corrupted: %+v", row)
		}
	}
}

func TestAgentSessionReprojectsMessageIntakeDuringActiveWrite(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	now := time.Now()
	_, err := s.Enqueue(ctx, nativeMessage("a", "root", "alice"))
	storetest.Must(t, err)
	settleStatuses(t, s, now)
	j := startStatusJob(t, s)
	finishNativeJob(t, s, j, 1)
	deliverNativePart(t, s)
	a := claimStatus(t, s, now, "urgent", "active")
	_, err = s.Enqueue(ctx, storetest.Message("b", "root", "bob"))
	storetest.Must(t, err)
	acknowledgeSession(t, s, a, now)
	var processing bool
	for _, call := range settleStatuses(t, s, now) {
		if call.Target == gateway.StatusTargetAgentSession {
			processing = call.SessionState == "processing"
		}
	}
	if !processing {
		t.Fatal("late active ACK abandoned a new message-mode question")
	}
}

func TestSchemaInitializationPreservesPublicationFacts(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*scopedFixture)
	for _, m := range []gateway.Message{storetest.Message("a", "a", "alice"), nativeMessage("b", "b", "bob")} {
		_, err := s.Enqueue(ctx, m)
		storetest.Must(t, err)
	}
	now := time.Now()
	settleStatuses(t, s, now)
	_, err := fixtureDB(t, s).Exec(`UPDATE gateway_job SET backend_observed_state='running',backend_observed_at='1970-01-01 00:00:00.123',backend_observation_uncertain=1,gateway_execution_outcome='failed',slack_progress_revision=9,slack_progress_sent_revision=8,slack_progress_attempts=7,slack_progress_next_attempt_at='1970-01-01 00:00:00.006',slack_progress_refresh_at='1970-01-01 00:00:00.005',slack_progress_rate_limited_until='1970-01-01 00:00:00.004',slack_progress_reason='test_retry',gateway_status_updated_at='1970-01-01 00:00:00.003'`)
	storetest.Must(t, err)
	before, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	storetest.Must(t, s.Migrate(ctx))
	storetest.Must(t, s.CheckSchema(ctx))
	after, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("schema initialization changed existing jobs, observations or publication facts")
	}
	m := storetest.Message("c", "c", "charlie")
	m.StatusMode = gateway.StatusHybrid
	_, err = s.Enqueue(ctx, m)
	storetest.Must(t, err)
	m.EventID = "invalid"
	m.StatusMode = "both"
	if _, err = s.Enqueue(ctx, m); err == nil {
		t.Fatal("allowed-mode constraint missing")
	}
	var violations int
	storetest.Must(t, fixtureDB(t, s).QueryRow("SELECT COUNT(*) FROM pragma_foreign_key_check").Scan(&violations))
	if violations != 0 {
		t.Fatal("foreign key violation")
	}
}
