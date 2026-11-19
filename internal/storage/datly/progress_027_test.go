package datly_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func progress027Finish(t *testing.T, store gateway.Store, status *gateway.GatewayJobProgress, now time.Time) {
	t.Helper()
	receipt := gateway.StatusResult{State: "ready", NextRefreshAt: now.Add(10 * time.Second)}
	if status.Terminal {
		receipt.State = "settled"
	}
	if status.Target == gateway.StatusTargetAgentSession {
		receipt.Acknowledged = true
		receipt.AgentStatus, receipt.SessionStatus = status.SessionState, status.SessionState
		receipt.NextRefreshAt = now.Add(gateway.AgentSessionRefresh)
	} else {
		receipt.SlackTS = status.SlackTS
		if receipt.SlackTS == "" {
			receipt.SlackTS = "270.000010"
		}
	}
	storetest.Must(t, store.FinishStatus(context.Background(), *status, receipt, now))
}

// Normal claims may first return the native indicator in hybrid mode. Complete
// that independent publication before checking the message projection.
func progress027Message(t *testing.T, store gateway.Store, now time.Time, kind, phase string) *gateway.GatewayJobProgress {
	t.Helper()
	for range 3 {
		status, err := store.ClaimStatus(context.Background(), now, kind)
		storetest.Must(t, err)
		if status != nil && status.Target == gateway.StatusTargetAgentSession {
			progress027Finish(t, store, status, now)
			continue
		}
		if phase == "" {
			if status != nil {
				t.Fatalf("unchanged message was claimed before refresh: %+v", status)
			}
			return nil
		}
		if status == nil || status.Phase != phase || status.Target != gateway.StatusTargetMessage {
			t.Fatalf("message phase=%q: got %+v", phase, status)
		}
		return status
	}
	t.Fatal("native publication did not settle before message claim")
	return nil
}

func progress027AcknowledgeInitial(t *testing.T, store gateway.Store, now time.Time) {
	t.Helper()
	for range 4 {
		status, err := store.ClaimStatus(context.Background(), now, "urgent")
		storetest.Must(t, err)
		if status == nil {
			return
		}
		if status.Target == gateway.StatusTargetAgentSession && (status.SessionState != "processing" || status.Terminal) {
			t.Fatalf("initial native indicator does not describe active work: %+v", status)
		}
		progress027Finish(t, store, status, now)
	}
	t.Fatal("initial status publications did not settle")
}

func TestProgress027CompletionGapAndRecoveryRetainReadyResponse(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusHybrid, gateway.StatusAgentSession} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("ready-gap", "270.000001", "alice")
					message.StatusMode = mode
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, binding := closure024Start(t, store, true)
					now := time.Now().UTC().Truncate(time.Millisecond)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now))
					progress027AcknowledgeInitial(t, store, now)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now))
					recovering := progress027Message(t, store, now, "backend_recovering", "backend_recovering")
					progress027Finish(t, store, recovering, now)

					// Canonical completion is committed before response presentation and
					// outbox construction. There is no outgoing message at this point.
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", now.Add(time.Second)))
					ready := progress027Message(t, store, now.Add(time.Second), "backend_recovering", "delivering")
					if ready.Terminal {
						t.Fatal("backend completion claimed delivery before outbox existed")
					}
					progress027Finish(t, store, ready, now.Add(time.Second))
					var count int
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message WHERE job_id=?", job.ID).Scan(&count))
					if count != 0 {
						t.Fatal("observation or progress created an outgoing response")
					}

					// Exercise the actual startup recovery path while the response is
					// not yet in the outbox, retaining durable execution identity.
					var submitted time.Time
					storetest.Must(t, db.QueryRow("SELECT backend_submission_started_at FROM gateway_job WHERE id=?", job.ID).Scan(&submitted))
					storetest.Must(t, root.Recover(ctx))
					resumed, err := store.ClaimJob(ctx)
					storetest.Must(t, err)
					if resumed == nil || resumed.ID != job.ID || resumed.GatewayRequestID != job.GatewayRequestID || resumed.BackendObservedState != "completed" || !resumed.BackendSubmissionStartedAt.Equal(submitted) {
						t.Fatalf("recovery lost completed execution identity: %+v", resumed)
					}
					bound, err := store.GetBinding(ctx, message.ThreadKey)
					storetest.Must(t, err)
					if bound == nil || bound.ID != binding.ID || bound.ConversationID == "" {
						t.Fatal("recovery replaced the backend conversation")
					}
					progress027Message(t, store, now.Add(2*time.Second), "backend_recovering", "")
					storetest.Must(t, store.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "first", Format: gateway.ReplyPlain}, {Text: "second", Format: gateway.ReplyPlain}}))
					for index := 0; index < 2; index++ {
						part, err := store.ClaimDelivery(ctx)
						storetest.Must(t, err)
						if part == nil || part.Part != index {
							t.Fatalf("response order changed: %+v", part)
						}
						storetest.Must(t, store.Delivered(ctx, part.ID, fmt.Sprintf("271.%06d", index+1)))
						if index == 0 {
							early, err := store.ClaimStatus(ctx, now.Add(3*time.Second), "urgent")
							storetest.Must(t, err)
							if early != nil {
								t.Fatalf("message or native indicator completed before the last response part: %+v", early)
							}
						}
					}
					completed := progress027Message(t, store, now.Add(4*time.Second), "urgent", "completed")
					if !completed.Terminal || completed.SlackTS != ready.SlackTS {
						t.Fatal("final completion did not update the original progress message")
					}
					progress027Finish(t, store, completed, now.Add(4*time.Second))
					progress027Message(t, store, now.Add(time.Hour), "heartbeat", "")
				})
			}
		})
	}
}

func TestProgress027PollingKeepsRecoveryPublicationClean(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusHybrid, gateway.StatusAgentSession} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("recovery-cadence", "270.000002", "alice")
					message.StatusMode = mode
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, _ := closure024Start(t, store, true)
					now := time.Now().UTC().Truncate(time.Millisecond)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now))
					progress027AcknowledgeInitial(t, store, now)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now))
					first := progress027Message(t, store, now, "backend_recovering", "backend_recovering")
					progress027Finish(t, store, first, now)
					var revision, sent int64
					var refresh, since time.Time
					storetest.Must(t, db.QueryRow("SELECT slack_progress_revision,slack_progress_sent_revision,slack_progress_refresh_at,backend_observation_uncertain_since FROM gateway_job WHERE id=?", job.ID).Scan(&revision, &sent, &refresh, &since))
					for attempt := 1; attempt <= 4; attempt++ {
						storetest.Must(t, store.SchedulePoll(ctx, job.ID, time.Now().Add(-time.Second)))
						resumed, err := store.ClaimJob(ctx)
						storetest.Must(t, err)
						if resumed == nil || resumed.ID != job.ID || resumed.GatewayRequestID != job.GatewayRequestID {
							t.Fatalf("poll claimed another execution: %+v", resumed)
						}
						var actualRevision, actualSent int64
						var phase string
						var actualRefresh, actualSince time.Time
						storetest.Must(t, db.QueryRow("SELECT slack_progress_phase,slack_progress_revision,slack_progress_sent_revision,slack_progress_refresh_at,backend_observation_uncertain_since FROM gateway_job WHERE id=?", job.ID).Scan(&phase, &actualRevision, &actualSent, &actualRefresh, &actualSince))
						if phase != "backend_recovering" || actualRevision != revision || actualSent != sent || !actualRefresh.Equal(refresh) || !actualSince.Equal(since) {
							t.Fatalf("poll dirtied unchanged recovery: phase=%s revision=%d/%d sent=%d/%d refresh=%s/%s since=%s/%s", phase, actualRevision, revision, actualSent, sent, actualRefresh, refresh, actualSince, since)
						}
						progress027Message(t, store, now.Add(time.Duration(attempt)*time.Second), "backend_recovering", "")
					}
					progress027Message(t, store, now.Add(9*time.Second), "backend_recovering", "")
					refreshStatus := progress027Message(t, store, now.Add(10*time.Second), "backend_recovering", "backend_recovering")
					progress027Finish(t, store, refreshStatus, now.Add(10*time.Second))
					// A genuine manager-state change remains urgent before refresh.
					blocked := progress027Message(t, store, now.Add(11*time.Second), "backend_blocked", "backend_blocked")
					progress027Finish(t, store, blocked, now.Add(11*time.Second))
					storetest.Must(t, store.SchedulePoll(ctx, job.ID, time.Now().Add(-time.Second)))
					_, err = store.ClaimJob(ctx)
					storetest.Must(t, err)
					progress027Message(t, store, now.Add(12*time.Second), "backend_blocked", "")
					recovering := progress027Message(t, store, now.Add(13*time.Second), "backend_recovering", "backend_recovering")
					progress027Finish(t, store, recovering, now.Add(13*time.Second))
					checking := progress027Message(t, store, now.Add(14*time.Second), "urgent", "checking")
					progress027Finish(t, store, checking, now.Add(14*time.Second))
					storetest.Must(t, db.QueryRow("SELECT backend_observation_uncertain_since FROM gateway_job WHERE id=?", job.ID).Scan(&since))
					if !since.Equal(now) {
						t.Fatal("manager recovery reset the uncertainty clock without a fresh observation")
					}
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now.Add(15*time.Second)))
					running := progress027Message(t, store, now.Add(15*time.Second), "urgent", "running")
					progress027Finish(t, store, running, now.Add(15*time.Second))
					progress027Message(t, store, now.Add(24*time.Second), "heartbeat", "")
					heartbeat := progress027Message(t, store, now.Add(25*time.Second), "heartbeat", "running")
					progress027Finish(t, store, heartbeat, now.Add(25*time.Second))
				})
			}
		})
	}
}

func TestProgress027InflightRecoveryReceiptKeepsCompletedIntent(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusHybrid, gateway.StatusAgentSession} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("inflight-recovery", "270.000003", "alice")
					message.StatusMode = mode
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, _ := closure024Start(t, store, true)
					now := time.Now().UTC().Truncate(time.Millisecond)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now))
					progress027AcknowledgeInitial(t, store, now)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now))
					inflight := progress027Message(t, store, now, "backend_recovering", "backend_recovering")
					if mode == gateway.StatusAgentSession && inflight.SlackTS != "" {
						t.Fatal("native-only recovery did not exercise its first fallback message")
					}
					// Model completion after send preparation but before Slack's receipt.
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", now.Add(time.Second)))
					progress027Finish(t, store, inflight, now.Add(2*time.Second))
					var phase, state string
					var revision, sent int64
					storetest.Must(t, db.QueryRow("SELECT slack_progress_phase,slack_progress_state,slack_progress_revision,slack_progress_sent_revision FROM gateway_job WHERE id=?", job.ID).Scan(&phase, &state, &revision, &sent))
					if phase != "delivering" || state != "ready" || revision <= sent || sent != inflight.Revision {
						t.Fatalf("old recovery receipt discarded newer completion: phase=%s state=%s revisions=%d/%d old=%d", phase, state, revision, sent, inflight.Revision)
					}
					ready := progress027Message(t, store, now.Add(3*time.Second), "backend_recovering", "delivering")
					stale := gateway.StatusResult{State: "ready", SlackTS: "270.000010"}
					if err := store.FinishStatus(ctx, *inflight, stale, now.Add(3*time.Second)); !errors.Is(err, gateway.ErrConflict) {
						t.Fatalf("stale receipt crossed the newer publication fence: %v", err)
					}
					progress027Finish(t, store, ready, now.Add(3*time.Second))
					storetest.Must(t, store.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
					part, err := store.ClaimDelivery(ctx)
					storetest.Must(t, err)
					storetest.Must(t, store.Delivered(ctx, part.ID, "273.000001"))
					final := progress027Message(t, store, now.Add(4*time.Second), "urgent", "completed")
					progress027Finish(t, store, final, now.Add(4*time.Second))
				})
			}
		})
	}
}

func TestProgress027CompletionRespectsRecoveryRateLimit(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, _ := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			message := storetest.Message("recovery-rate-limit", "270.000004", "alice")
			message.StatusMode = gateway.StatusHybrid
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			job, _ := closure024Start(t, store, true)
			now := time.Now().UTC().Truncate(time.Millisecond)
			storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now))
			progress027AcknowledgeInitial(t, store, now)
			storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now))
			status := progress027Message(t, store, now, "backend_recovering", "backend_recovering")
			deadline := now.Add(20 * time.Second)
			storetest.Must(t, store.FinishStatus(ctx, *status, gateway.StatusResult{State: "ready", Reason: "rate_limited", RetryAt: deadline, RateLimitedUntil: deadline}, now))
			storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", now.Add(time.Second)))
			progress027Message(t, store, deadline.Add(-time.Millisecond), "urgent", "")
			ready := progress027Message(t, store, deadline, "urgent", "delivering")
			if ready.Terminal {
				t.Fatal("rate-limit retry converted ready response into delivered completion")
			}
			progress027Finish(t, store, ready, deadline)
		})
	}
}

func TestProgress027NativeCompletionDoesNotStartOutageFallback(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			message := storetest.Message("native-ready-no-fallback", "270.000005", "alice")
			message.StatusMode = gateway.StatusAgentSession
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			job, _ := closure024Start(t, store, true)
			now := time.Now().UTC().Truncate(time.Millisecond)
			storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", now))
			progress027AcknowledgeInitial(t, store, now)
			storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", now.Add(time.Second)))
			progress027Message(t, store, now.Add(time.Second), "backend_recovering", "")
			var slackID string
			storetest.Must(t, db.QueryRow("SELECT slack_progress_message_id FROM gateway_job WHERE id=?", job.ID).Scan(&slackID))
			if slackID != "" {
				t.Fatal("ready response started an ordinary native-only progress message")
			}
			storetest.Must(t, store.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
			part, err := store.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, store.Delivered(ctx, part.ID, "275.000001"))
			status, err := store.ClaimStatus(ctx, now.Add(2*time.Second), "urgent")
			storetest.Must(t, err)
			if status == nil || status.Target != gateway.StatusTargetAgentSession || status.SessionState != "active" || !status.Terminal {
				t.Fatalf("native indicator failed to settle after answer delivery: %+v", status)
			}
			progress027Finish(t, store, status, now.Add(2*time.Second))
			progress027Message(t, store, now.Add(3*time.Second), "urgent", "")
		})
	}
}
