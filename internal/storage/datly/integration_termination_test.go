package datly_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func enqueueTerminationQuestion(t *testing.T, scope storage.Store, app, event, thread string, mode gateway.StatusMode) {
	t.Helper()
	message := storetest.Message(event, thread, "alice")
	message.AppID, message.StatusMode = app, mode
	_, err := scope.Enqueue(context.Background(), message)
	storetest.Must(t, err)
}

// Exercise the same durable acknowledgements used by the single outbound owner,
// including the distinction between a message ID and a native session receipt.
func acknowledgeTerminationStatuses(t *testing.T, scope storage.Store, kind string, now time.Time) int {
	t.Helper()
	for count := 0; count < 10; count++ {
		status, err := scope.ClaimStatus(context.Background(), now, kind)
		storetest.Must(t, err)
		if status == nil {
			return count
		}
		result := gateway.StatusResult{State: "ready", NextRefreshAt: now.Add(time.Minute)}
		if status.Target == gateway.StatusTargetAgentSession {
			result.State, result.Acknowledged = "settled", true
			result.AgentStatus, result.SessionStatus = status.SessionState, status.SessionState
		} else {
			result.SlackTS = status.SlackTS
			if result.SlackTS == "" {
				result.SlackTS = fmt.Sprintf("progress-%d", status.JobID)
			}
			if status.Terminal {
				result.State = "settled"
			}
		}
		storetest.Must(t, scope.FinishStatus(context.Background(), *status, result, now))
	}
	t.Fatal("terminal status never settled")
	return 0
}

type terminationRevisions struct {
	messageRevision, messageSent, nativeRevision, nativeSent int64
	messageState, nativeState                                string
}

func readTerminationRevisions(t *testing.T, db *sql.DB, jobID int64) terminationRevisions {
	t.Helper()
	var value terminationRevisions
	storetest.Must(t, db.QueryRow("SELECT slack_progress_revision,slack_progress_sent_revision,slack_progress_state FROM gateway_job WHERE id=?", jobID).Scan(&value.messageRevision, &value.messageSent, &value.messageState))
	storetest.Must(t, db.QueryRow("SELECT slack_publication_revision,slack_publication_sent_revision,slack_publication_state FROM slack_agent_session_publication WHERE status_source_job_id=?", jobID).Scan(&value.nativeRevision, &value.nativeSent, &value.nativeState))
	return value
}

func TestConfigurationTerminationPreservesCompletedMultipartAuditAndSettlesOnce(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		for _, unfinished := range []string{"pending", "sending"} {
			t.Run(driver+"/"+unfinished, func(t *testing.T) {
				ctx := context.Background()
				root, db := sharedFixture(t, driver)
				original := testIdentity("steward", "A")
				registerNativeIntegrations(t, root, &original)
				old := root.Scope(original.ID)
				now := time.Now()
				enqueueTerminationQuestion(t, old, "A", "multipart-original", "100.001", gateway.StatusHybrid)
				if count := acknowledgeTerminationStatuses(t, old, "urgent", now); count != 2 {
					t.Fatalf("initial hybrid publications=%d", count)
				}
				job := startSharedSessionJob(t, old)
				storetest.Must(t, old.RecordObservation(ctx, job.ID, "completed", now))
				storetest.Must(t, old.Complete(ctx, job.ID, job.GatewayRequestID, "complete backend result", []gateway.ReplyPart{{Text: "first", Format: gateway.ReplyPlain}, {Text: "second", Format: gateway.ReplyPlain}}))
				first, err := old.ClaimDelivery(ctx)
				storetest.Must(t, err)
				if first == nil || first.Part != 0 {
					t.Fatal("missing first response part")
				}
				storetest.Must(t, old.Delivered(ctx, first.ID, "confirmed-first-part"))
				if unfinished == "sending" {
					second, err := old.ClaimDelivery(ctx)
					storetest.Must(t, err)
					if second == nil || second.Part != 1 {
						t.Fatal("missing interrupted second response part")
					}
				}
				var requestID, result string
				var submitted sql.NullTime
				storetest.Must(t, db.QueryRow("SELECT gateway_request_id,backend_submission_started_at,gateway_execution_result_text FROM gateway_job WHERE id=?", job.ID).Scan(&requestID, &submitted, &result))
				changed := original
				changed.BackendAgentID, changed.PrincipalID = "other-agent", "other-principal"
				registerNativeIntegrations(t, root, &changed)
				current := root.Scope(changed.ID)
				var gotRequest, gotResult, state, outcome string
				var gotSubmitted sql.NullTime
				storetest.Must(t, db.QueryRow("SELECT gateway_request_id,backend_submission_started_at,gateway_execution_result_text,gateway_execution_state,gateway_execution_outcome FROM gateway_job WHERE id=?", job.ID).Scan(&gotRequest, &gotSubmitted, &gotResult, &state, &outcome))
				if gotRequest != requestID || gotSubmitted.Valid != submitted.Valid || !gotSubmitted.Time.Equal(submitted.Time) || !submitted.Valid || gotResult != result || state != "completed" || outcome != "completed" {
					t.Fatal("configuration closure overwrote completed execution facts")
				}
				var firstState, firstID, secondState string
				storetest.Must(t, db.QueryRow("SELECT slack_delivery_state,slack_message_id FROM slack_outgoing_message WHERE id=?", first.ID).Scan(&firstState, &firstID))
				storetest.Must(t, db.QueryRow("SELECT slack_delivery_state FROM slack_outgoing_message WHERE job_id=? AND slack_message_index=1", job.ID).Scan(&secondState))
				wantSecond := "failed"
				if unfinished == "sending" {
					wantSecond = "delivery_unknown"
				}
				if firstState != "delivered" || firstID != "confirmed-first-part" || secondState != wantSecond {
					t.Fatalf("response audit changed: first=%s/%s second=%s", firstState, firstID, secondState)
				}
				if count := acknowledgeTerminationStatuses(t, current, "integration_terminal", now.Add(time.Second)); count != 2 {
					t.Fatalf("hybrid cleanup publications=%d", count)
				}
				notice, err := current.ClaimDelivery(ctx)
				storetest.Must(t, err)
				if notice == nil || notice.Kind != gateway.DeliveryIntegrationNotice {
					t.Fatal("old response blockage prevented terminal notice")
				}
				storetest.Must(t, current.Delivered(ctx, notice.ID, "confirmed-notice"))
				revisions := readTerminationRevisions(t, db, job.ID)
				for epoch := 0; epoch < 3; epoch++ {
					if epoch == 2 {
						changed.BackendAgentID = "third-agent"
					}
					registerNativeIntegrations(t, root, &changed)
					current = root.Scope(changed.ID)
					if after := readTerminationRevisions(t, db, job.ID); after != revisions {
						t.Fatalf("restart/reassignment repeated acknowledged terminal status: before=%+v after=%+v", revisions, after)
					}
					if count := acknowledgeTerminationStatuses(t, current, "integration_terminal", now.Add(time.Minute)); count != 0 {
						t.Fatal("acknowledged configuration closure was republished")
					}
					if next, err := current.ClaimDelivery(ctx); err != nil || next != nil {
						t.Fatalf("configuration restart retried old response/notice: %+v %v", next, err)
					}
				}
				var noticeCount int
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message WHERE slack_message_kind='integration_notice'").Scan(&noticeCount))
				if noticeCount != 1 {
					t.Fatal("multiple assignment changes duplicated the physical-thread notice")
				}
			})
		}
	}
}

func TestRemovingAllIntegrationsTerminatesWithoutSlackTransport(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			identity := testIdentity("steward", "A")
			registerNativeIntegrations(t, root, &identity)
			old := root.Scope(identity.ID)
			now := time.Now()
			enqueueTerminationQuestion(t, old, "A", "known", "known-thread", gateway.StatusHybrid)
			acknowledgeTerminationStatuses(t, old, "urgent", now)
			startSharedSessionJob(t, old)
			enqueueTerminationQuestion(t, old, "A", "unpublished", "unpublished-thread", gateway.StatusHybrid)
			enqueueTerminationQuestion(t, old, "A", "unknown", "unknown-thread", gateway.StatusMessage)
			// Model a crash after the first message send started but before its
			// acknowledgement; recovery must preserve the uncertain audit.
			_, err := db.Exec("UPDATE gateway_job SET slack_progress_state='sending',slack_progress_attempts=1 WHERE slack_event_id='unknown'")
			storetest.Must(t, err)
			rows, err := root.ReconcileIntegrations(ctx, nil)
			storetest.Must(t, err)
			if len(rows) != 0 {
				t.Fatal("removed integration remains active")
			}
			var liveJobs, liveParts, liveProgress, liveNative int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE gateway_execution_state IN ('pending','processing','querying')").Scan(&liveJobs))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message WHERE slack_delivery_state IN ('pending','sending')").Scan(&liveParts))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE slack_progress_state IN ('ready','sending')").Scan(&liveProgress))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication WHERE slack_publication_state IN ('ready','sending')").Scan(&liveNative))
			if liveJobs+liveParts+liveProgress+liveNative != 0 {
				t.Fatalf("transportless work left waiting: jobs=%d parts=%d message=%d native=%d", liveJobs, liveParts, liveProgress, liveNative)
			}
			var uncertain string
			storetest.Must(t, db.QueryRow("SELECT slack_progress_state FROM gateway_job WHERE slack_event_id='unknown'").Scan(&uncertain))
			if uncertain != "delivery_unknown" {
				t.Fatal("transport removal discarded ambiguous progress delivery")
			}
			for _, publication := range []struct{ table, state, reason string }{
				{"gateway_job", "slack_progress_state", "slack_progress_reason"},
				{"slack_agent_session_publication", "slack_publication_state", "slack_publication_reason"},
			} {
				var failed int
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM "+publication.table+" WHERE "+publication.state+"='failed' AND "+publication.reason+"='slack_transport_unavailable'").Scan(&failed))
				if failed == 0 {
					t.Errorf("%s did not record unavailable transport", publication.table)
				}
			}
			before, err := root.Status(ctx, 20)
			storetest.Must(t, err)
			_, err = root.ReconcileIntegrations(ctx, nil)
			storetest.Must(t, err)
			after, err := root.Status(ctx, 20)
			storetest.Must(t, err)
			if !reflect.DeepEqual(before.GatewayJobs, after.GatewayJobs) || !reflect.DeepEqual(before.SlackAgentSessionPublications, after.SlackAgentSessionPublications) || !reflect.DeepEqual(before.SlackOutgoingMessages, after.SlackOutgoingMessages) {
				t.Fatal("restart without transport changed settled terminal audit")
			}
		})
	}
}

func TestFailCleanupOnlyTerminatesMatchingHistoricalBot(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a, b := testIdentity("a", "A"), testIdentity("b", "B")
			registerNativeIntegrations(t, root, &a, &b)
			now := time.Now()
			for _, identity := range []storage.Integration{a, b} {
				scope := root.Scope(identity.ID)
				enqueueTerminationQuestion(t, scope, identity.AppID, "old-"+identity.AppID, "thread-"+identity.AppID, gateway.StatusHybrid)
				acknowledgeTerminationStatuses(t, scope, "urgent", now)
				startSharedSessionJob(t, scope)
			}
			a.BackendAgentID, b.BackendAgentID = "new-a", "new-b"
			registerNativeIntegrations(t, root, &a, &b)
			current := root.Scope(a.ID)
			enqueueTerminationQuestion(t, current, "A", "current-A", "current-thread", gateway.StatusHybrid)
			storetest.Must(t, current.FailCleanup(ctx, "slack_credentials_unavailable"))
			var failedA, pendingB, failedCurrent, nativeA, nativeB int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message d JOIN gateway_job j ON j.id=d.job_id JOIN integration i ON i.id=j.integration_id WHERE i.slack_app_id='A' AND i.active=0 AND d.slack_message_kind='integration_notice' AND d.slack_delivery_state='failed'").Scan(&failedA))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message d JOIN gateway_job j ON j.id=d.job_id JOIN integration i ON i.id=j.integration_id WHERE i.slack_app_id='B' AND d.slack_message_kind='integration_notice' AND d.slack_delivery_state='pending'").Scan(&pendingB))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE slack_event_id='current-A' AND slack_progress_state!='ready'").Scan(&failedCurrent))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication p JOIN integration i ON i.id=p.initial_integration_id WHERE i.slack_app_id='A' AND i.active=0 AND p.slack_publication_state='failed' AND p.slack_publication_reason='slack_credentials_unavailable'").Scan(&nativeA))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication p JOIN integration i ON i.id=p.initial_integration_id WHERE i.slack_app_id='B' AND p.slack_publication_state='ready'").Scan(&nativeB))
			if failedA != 1 || pendingB != 1 || failedCurrent != 0 || nativeA != 1 || nativeB != 1 {
				t.Fatalf("cleanup escaped historical bot scope: noticeA=%d noticeB=%d live=%d nativeA=%d nativeB=%d", failedA, pendingB, failedCurrent, nativeA, nativeB)
			}
			if status, err := current.ClaimStatus(ctx, now.Add(time.Second), "integration_terminal"); err != nil || status != nil {
				t.Fatalf("failed credential cleanup remains claimable: %+v %v", status, err)
			}
			other, err := root.Scope(b.ID).ClaimDelivery(ctx)
			storetest.Must(t, err)
			if other == nil || other.Kind != gateway.DeliveryIntegrationNotice {
				t.Fatal("another bot's cleanup was lost")
			}
		})
	}
}
