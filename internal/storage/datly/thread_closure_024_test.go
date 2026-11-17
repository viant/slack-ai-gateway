package datly_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func closure024Start(t *testing.T, store gateway.Store, create bool) (*gateway.GatewayJob, *gateway.ConversationBinding) {
	t.Helper()
	ctx := context.Background()
	job, err := store.ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil {
		t.Fatal("closure fixture did not claim its question")
	}
	binding, err := store.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	if create {
		storetest.Must(t, store.BeginCreate(ctx, binding.ID))
		storetest.Must(t, store.SetConversation(ctx, binding.ID, "closure-backend-"+job.GatewayRequestID))
		storetest.Must(t, store.BeginQuery(ctx, job.ID))
	}
	return job, binding
}

func closure024Job(t *testing.T, root storage.Store, id int64) storage.GatewayJobMetadata {
	t.Helper()
	view, err := root.Status(context.Background(), 100)
	storetest.Must(t, err)
	for _, job := range view.GatewayJobs {
		if job.ID == id {
			return job
		}
	}
	t.Fatalf("missing job %d", id)
	return storage.GatewayJobMetadata{}
}

func closure024CountNotices(t *testing.T, db *sql.DB) int {
	t.Helper()
	var count int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&count))
	return count
}

func closure024Acknowledge(t *testing.T, store gateway.Store, now time.Time) []*gateway.GatewayJobProgress {
	t.Helper()
	var published []*gateway.GatewayJobProgress
	for i := 0; i < 8; i++ {
		status, err := store.ClaimStatus(context.Background(), now, "urgent")
		storetest.Must(t, err)
		if status == nil {
			return published
		}
		published = append(published, status)
		receipt := gateway.StatusResult{State: "settled", Acknowledged: true}
		if status.Target == gateway.StatusTargetAgentSession {
			receipt.AgentStatus, receipt.SessionStatus = status.SessionState, status.SessionState
		} else {
			receipt.SlackTS = status.SlackTS
			if receipt.SlackTS == "" {
				receipt.SlackTS = fmt.Sprintf("240.%06d", i+1)
			}
		}
		if !status.Terminal {
			receipt.State = "ready"
			receipt.NextRefreshAt = now.Add(time.Hour)
		}
		storetest.Must(t, store.FinishStatus(context.Background(), *status, receipt, now))
	}
	t.Fatal("status publication did not settle")
	return nil
}

func TestThreadClosure024FinishesQueuedQuestionsAndKeepsNewRootsIndependent(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					scoped := root.Scope(id)
					first := storetest.Message("closure-first", "240.000001", "alice")
					first.Surface, first.ChannelID, first.StatusMode = gateway.SurfaceChannel, "C", mode
					_, err := scoped.Enqueue(ctx, first)
					storetest.Must(t, err)
					store := gateway.ForThread(scoped, first.PhysicalThread())
					now := time.Now().UTC().Truncate(time.Millisecond)
					closure024Acknowledge(t, store, now)
					job, binding := closure024Start(t, store, true)
					queued := first
					queued.EventID, queued.MessageTS, queued.AuthorID = "closure-queued", "240.000002", "bob"
					_, err = store.Enqueue(ctx, queued)
					storetest.Must(t, err)
					var queuedID int64
					var queuedBinding sql.NullInt64
					storetest.Must(t, db.QueryRow("SELECT id,conversation_binding_id FROM gateway_job WHERE slack_event_id=?", queued.EventID).Scan(&queuedID, &queuedBinding))
					if queuedBinding.Valid {
						t.Fatal("queued fixture unexpectedly had a backend binding")
					}

					closed, err := store.CloseThread(ctx, job.ID, "reconciliation_deadline")
					storetest.Must(t, err)
					if !closed {
						t.Fatal("first closure did not report its committed transition")
					}
					result := closure024Job(t, root, job.ID)
					waiting := closure024Job(t, root, queuedID)
					if result.State != "rejected" || result.Reason != "conversation_closed:reconciliation_deadline" || result.Outcome != "" || result.ExecutionFinishedAt == 0 || result.NextAttemptAt != 0 {
						t.Fatalf("uncertain execution was misrepresented: %+v", result)
					}
					if waiting.State != "rejected" || waiting.Reason != "conversation_closed" || waiting.BackendSubmissionStartedAt != 0 || waiting.Outcome != "" || waiting.ExecutionFinishedAt == 0 {
						t.Fatalf("queued question was not finished as unsubmitted: %+v", waiting)
					}
					bound, err := store.GetBinding(ctx, first.ThreadKey)
					storetest.Must(t, err)
					if bound == nil || bound.ID != binding.ID || bound.State != "closed" || bound.ConversationID == "" {
						t.Fatalf("backend history was lost or remained executable: %+v", bound)
					}
					if closure024CountNotices(t, db) != 1 {
						t.Fatal("closure did not create exactly one collective notice")
					}

					storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", now.Add(time.Minute)))
					storetest.Must(t, store.SchedulePoll(ctx, job.ID, now.Add(time.Minute)))
					storetest.Must(t, store.Complete(ctx, job.ID, job.GatewayRequestID, "late answer", []gateway.ReplyPart{{Text: "late answer", Format: gateway.ReplyPlain}}))
					closed, err = store.CloseThread(ctx, job.ID, "different_late_reason")
					storetest.Must(t, err)
					if closed || closure024CountNotices(t, db) != 1 {
						t.Fatal("late callback or repeated closure created work")
					}
					late := closure024Job(t, root, job.ID)
					if late.Reason != result.Reason || late.Outcome != result.Outcome || late.ExecutionFinishedAt != result.ExecutionFinishedAt {
						t.Fatalf("late callback rewrote the closure: before=%+v after=%+v", result, late)
					}

					notice, err := store.ClaimDelivery(ctx)
					storetest.Must(t, err)
					if notice == nil || notice.JobID != job.ID || strings.Contains(notice.Text, "Elapsed:") || !strings.Contains(strings.ToLower(notice.Text), "new thread") {
						t.Fatalf("missing definitive root-owned closure notice: %+v", notice)
					}
					access, err := store.GetDeliveryContext(ctx, *notice)
					storetest.Must(t, err)
					if access.Message.AuthorID != first.AuthorID {
						t.Fatal("queued author's identity replaced the closing question's author")
					}
					storetest.Must(t, store.Delivered(ctx, notice.ID, "241.000001"))
					for _, status := range closure024Acknowledge(t, store, now.Add(2*time.Second)) {
						if !status.Terminal || status.Target == gateway.StatusTargetAgentSession && status.SessionState != "active" {
							t.Fatalf("closed thread remained active work: %+v", status)
						}
						if status.Target == gateway.StatusTargetMessage && strings.Contains(gateway.StatusText(*status, now.Add(72*time.Hour)), "Elapsed:") {
							t.Fatal("final closure status retained a growing execution timer")
						}
					}
					if status, err := store.ClaimStatus(ctx, now.Add(72*time.Hour), "heartbeat"); err != nil || status != nil {
						t.Fatalf("settled closure was refreshed: %+v %v", status, err)
					}

					followup := first
					followup.EventID, followup.MessageTS = "closure-followup", "240.000003"
					_, err = store.Enqueue(ctx, followup)
					storetest.Must(t, err)
					if next, err := store.ClaimJob(ctx); err != nil || next != nil {
						t.Fatalf("closed-thread followup was sent for execution: %+v %v", next, err)
					}
					local, err := store.ClaimDelivery(ctx)
					storetest.Must(t, err)
					if local == nil || !strings.Contains(strings.ToLower(local.Text), "not submitted") || strings.Contains(strings.ToLower(local.Text), "configuration changed") {
						t.Fatalf("closed-thread followup received a misleading reply: %+v", local)
					}
					var desired string
					var publications int
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
					if mode == gateway.StatusMessage {
						if publications != 0 {
							t.Fatal("message-only closure created a native session")
						}
					} else {
						storetest.Must(t, db.QueryRow("SELECT slack_agent_desired_status FROM slack_agent_session_publication").Scan(&desired))
						if publications != 1 || desired != "active" {
							t.Fatal("local followup restarted or duplicated the native indicator")
						}
					}
					fresh := first
					fresh.EventID, fresh.MessageTS, fresh.RootTS = "closure-fresh-root", "242.000001", "242.000001"
					_, err = scoped.Enqueue(ctx, fresh)
					storetest.Must(t, err)
					next, err := gateway.ForThread(scoped, fresh.PhysicalThread()).ClaimJob(ctx)
					storetest.Must(t, err)
					if next == nil || next.EventID != fresh.EventID {
						t.Fatal("old closure blocked a new independent Slack root")
					}
				})
			}
		})
	}
}

func TestThreadClosure024BeforeConversationCreationNeedsNoSyntheticBinding(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, state := range []string{"absent", "reserved", "creating"} {
				t.Run(state, func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("early-closure", "243.000001", "alice")
					message.Surface, message.ChannelID, message.StatusMode = gateway.SurfaceDirectMessage, "D-early-closure", gateway.StatusAgentSession
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, err := store.ClaimJob(ctx)
					storetest.Must(t, err)
					var binding *gateway.ConversationBinding
					if state != "absent" {
						binding, err = store.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
						storetest.Must(t, err)
						if state == "creating" {
							storetest.Must(t, store.BeginCreate(ctx, binding.ID))
						}
					}
					closed, err := store.CloseThread(ctx, job.ID, "creation_unknown")
					storetest.Must(t, err)
					if !closed {
						t.Fatal("early closure was not persisted")
					}
					if state == "creating" {
						storetest.Must(t, store.SetConversation(ctx, binding.ID, "late-created-backend"))
						late, err := store.GetBinding(ctx, message.ThreadKey)
						storetest.Must(t, err)
						if late.State != "closed" || late.ConversationID != "late-created-backend" {
							t.Fatalf("late known creation was lost or reactivated: %+v", late)
						}
					}
					var bindings int
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM conversation_binding").Scan(&bindings))
					if state == "absent" && bindings != 0 {
						t.Fatal("closure invented an authorization binding")
					}
					followup := message
					followup.EventID, followup.MessageTS = "early-closure-followup", "243.000002"
					_, err = store.Enqueue(ctx, followup)
					storetest.Must(t, err)
					if next, err := store.ClaimJob(ctx); err != nil || next != nil {
						t.Fatalf("closure without a ready binding did not persist: %+v %v", next, err)
					}
					if closure024CountNotices(t, db) != 2 {
						t.Fatal("early closure or local followup reply missing")
					}
					var notice string
					storetest.Must(t, db.QueryRow("SELECT slack_message_text FROM slack_outgoing_message WHERE job_id=?", job.ID).Scan(&notice))
					if !strings.Contains(strings.ToLower(notice), "not submitted") {
						t.Fatal("definitely unsubmitted prompt was presented as a running query")
					}
				})
			}
		})
	}
}

func TestThreadClosure024StartupRepairsHistoricalBlockersOnce(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			scoped := root.Scope(id)
			var ids []int64
			old := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Millisecond)
			for _, kind := range []string{"review", "expired", "creating"} {
				message := storetest.Message("historical-"+kind, "historical-root-"+kind, "alice")
				_, err := scoped.Enqueue(ctx, message)
				storetest.Must(t, err)
				store := gateway.ForThread(scoped, message.PhysicalThread())
				job, binding := closure024Start(t, store, kind != "creating")
				ids = append(ids, job.ID)
				if kind == "creating" {
					storetest.Must(t, store.BeginCreate(ctx, binding.ID))
				} else {
					_, err = db.Exec("UPDATE gateway_job SET created_at=?,backend_submission_started_at=?,gateway_execution_updated_at=? WHERE id=?", old.Add(-time.Second), old, old, job.ID)
					storetest.Must(t, err)
					if kind == "review" {
						_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='needs_review',gateway_execution_reason='backend_protocol_or_access_error' WHERE id=?", job.ID)
						storetest.Must(t, err)
					}
				}
				queued := message
				queued.EventID, queued.MessageTS = "historical-queued-"+kind, message.MessageTS+"1"
				_, err = store.Enqueue(ctx, queued)
				storetest.Must(t, err)
			}
			storetest.Must(t, root.Recover(ctx))
			var unfinished int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE gateway_execution_state NOT IN ('completed','rejected')").Scan(&unfinished))
			if unfinished != 0 || closure024CountNotices(t, db) != 3 {
				t.Fatal("startup left historical blockers or omitted collective notices")
			}
			finished := map[int64]int64{}
			for _, id := range ids {
				job := closure024Job(t, root, id)
				if job.State != "rejected" || !strings.HasPrefix(job.Reason, "conversation_closed:") || job.ExecutionFinishedAt == 0 || job.Outcome != "" {
					t.Fatalf("historical job was not truthfully finalized: %+v", job)
				}
				finished[id] = job.ExecutionFinishedAt
			}
			storetest.Must(t, root.Recover(ctx))
			if closure024CountNotices(t, db) != 3 {
				t.Fatal("second recovery repeated a closure notice")
			}
			for _, id := range ids {
				if closure024Job(t, root, id).ExecutionFinishedAt != finished[id] {
					t.Fatal("second recovery changed a historical finish time")
				}
			}
		})
	}
}

func TestThreadClosure024RetriesOnlyItsDatabaseTransaction(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db, fault, id := retry022Open(t, driver)
			message := storetest.Message("retry-closure", "retry-closure-root", "alice")
			message.StatusMode = gateway.StatusHybrid
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			job, _ := closure024Start(t, store, true)
			before := root.WorkRevision(id)
			root.TakeWorkHints()
			fault.match, fault.requiredColumn = "insert into slack_outgoing_message", "slack_message_text"
			fault.arm(1)
			closed, err := store.CloseThread(ctx, job.ID, "reconciliation_deadline")
			storetest.Must(t, err)
			if !closed || fault.roots.Load() != 2 || fault.publicationCalls.Load() != 2 || fault.activeRoots.Load() != 0 {
				t.Fatalf("closure did not use one bounded clean-rollback retry: closed=%t roots=%d notices=%d active=%d", closed, fault.roots.Load(), fault.publicationCalls.Load(), fault.activeRoots.Load())
			}
			if closure024CountNotices(t, db) != 1 || root.WorkRevision(id) != before+1 || len(root.TakeWorkHints()) != 1 {
				t.Fatal("rolled-back closure leaked a notice or scheduling notification")
			}
			closed, err = store.CloseThread(ctx, job.ID, "reconciliation_deadline")
			storetest.Must(t, err)
			if closed || closure024CountNotices(t, db) != 1 || root.WorkRevision(id) != before+1 {
				t.Fatal("idempotent closure changed durable work")
			}
		})
	}
}

func TestThreadClosure024CommitUnknownDoesNotReplayOnRecovery(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db, fault, id := retry022OpenState(t, driver, false)
			message := storetest.Message("unknown-closure", "unknown-closure-root", "alice")
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			job, _ := closure024Start(t, store, true)
			fault.commitFailureAt = 1
			fault.arm(0)
			_, err = store.CloseThread(ctx, job.ID, "reconciliation_deadline")
			var issue *gateway.StorageError
			if !errors.As(err, &issue) || fault.roots.Load() != 1 {
				t.Fatalf("unknown commit was retried or lost native evidence: %v roots=%d", err, fault.roots.Load())
			}
			if closure024CountNotices(t, db) != 1 {
				t.Fatal("fixture did not commit the closure before losing its result")
			}
			storetest.Must(t, root.Recover(ctx))
			if closure024CountNotices(t, db) != 1 || closure024Job(t, root, job.ID).State != "rejected" {
				t.Fatal("recovery replayed a closure with an unknown commit result")
			}
		})
	}
}

func TestThreadClosure024RetainsInFlightStatusReceipts(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			message := storetest.Message("receipt-closure", "receipt-closure-root", "alice")
			message.StatusMode = gateway.StatusHybrid
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			now := time.Now().UTC().Truncate(time.Millisecond)
			first, err := store.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			second, err := store.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			if first == nil || second == nil || first.Target == second.Target {
				t.Fatal("fixture did not retain both independent in-flight publications")
			}
			job, _ := closure024Start(t, store, true)
			closed, err := store.CloseThread(ctx, job.ID, "reconciliation_deadline")
			storetest.Must(t, err)
			if !closed {
				t.Fatal("in-flight publications prevented local closure")
			}
			for _, status := range []*gateway.GatewayJobProgress{first, second} {
				receipt := gateway.StatusResult{State: "ready", Acknowledged: true, NextRefreshAt: now.Add(time.Hour)}
				if status.Target == gateway.StatusTargetAgentSession {
					receipt.AgentStatus, receipt.SessionStatus = "processing", "processing"
				} else {
					receipt.SlackTS = "244.000001"
				}
				storetest.Must(t, store.FinishStatus(ctx, *status, receipt, now.Add(time.Second)))
			}
			final := closure024Acknowledge(t, store, now.Add(2*time.Second))
			var native, messageFinal bool
			for _, status := range final {
				if !status.Terminal {
					t.Fatalf("old receipt reopened nonterminal progress: %+v", status)
				}
				if status.Target == gateway.StatusTargetAgentSession {
					native = status.SessionState == "active"
				} else {
					messageFinal = gateway.ConversationClosed(status.Phase)
				}
			}
			if !native || !messageFinal || closure024CountNotices(t, db) != 1 {
				t.Fatalf("closure did not follow the original receipts with both final updates: %+v", final)
			}
			if status, err := store.ClaimStatus(ctx, now.Add(72*time.Hour), "heartbeat"); err != nil || status != nil {
				t.Fatalf("in-flight receipt left recurring work: %+v %v", status, err)
			}
		})
	}
}

func TestThreadClosure024NoticePassesUnknownDeliveryWithoutReplayingIt(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			message := storetest.Message("old-delivery-closure", "old-delivery-closure-root", "alice")
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			original, _ := closure024Start(t, store, true)
			storetest.Must(t, store.RecordObservation(ctx, original.ID, "completed", time.Now()))
			storetest.Must(t, store.Complete(ctx, original.ID, original.GatewayRequestID, "old answer", []gateway.ReplyPart{{Text: "old answer", Format: gateway.ReplyPlain}}))
			oldPart, err := store.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, store.DeliveryUnknown(ctx, oldPart.ID))
			before := closure024Job(t, root, original.ID)
			question := message
			question.EventID, question.MessageTS = "uncertain-following-query", "245.000002"
			_, err = store.Enqueue(ctx, question)
			storetest.Must(t, err)
			job, err := store.ClaimJob(ctx)
			storetest.Must(t, err)
			_, err = store.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
			storetest.Must(t, err)
			storetest.Must(t, store.BeginQuery(ctx, job.ID))
			_, err = store.CloseThread(ctx, job.ID, "reconciliation_deadline")
			storetest.Must(t, err)
			closed, err := store.ClaimDelivery(ctx)
			storetest.Must(t, err)
			if closed == nil || closed.JobID != job.ID {
				t.Fatalf("old uncertain response blocked the definitive closure notice: %+v", closed)
			}
			storetest.Must(t, store.DeliveryUnknown(ctx, closed.ID))
			if repeat, err := store.ClaimDelivery(ctx); err != nil || repeat != nil {
				t.Fatalf("unknown closure notice was automatically repeated: %+v %v", repeat, err)
			}
			question.EventID, question.MessageTS = "closed-thread-local-reply", "245.000003"
			_, err = store.Enqueue(ctx, question)
			storetest.Must(t, err)
			local, err := store.ClaimDelivery(ctx)
			storetest.Must(t, err)
			if local == nil || local.ID == oldPart.ID || local.ID == closed.ID {
				t.Fatal("a local closed-thread reply was blocked or retried an uncertain message")
			}
			var unknown int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message WHERE slack_delivery_state='delivery_unknown'").Scan(&unknown))
			after := closure024Job(t, root, original.ID)
			if unknown != 2 || after.State != before.State || after.Reason != before.Reason || after.Outcome != before.Outcome || after.ExecutionFinishedAt != before.ExecutionFinishedAt {
				t.Fatal("closure altered completed history or uncertain delivery evidence")
			}
		})
	}
}

func TestThreadClosure024RecoveryUpdatesSettledLegacyStatus(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			message := storetest.Message("settled-legacy-closure", "settled-legacy-root", "alice")
			message.StatusMode = gateway.StatusMessage
			_, err := root.Scope(id).Enqueue(ctx, message)
			storetest.Must(t, err)
			store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
			job, _ := closure024Start(t, store, true)
			now := time.Now().UTC().Truncate(time.Millisecond)
			initial, err := store.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			if initial == nil || initial.Target != gateway.StatusTargetMessage {
				t.Fatal("missing original message progress publication")
			}
			const knownTS = "246.000001"
			storetest.Must(t, store.FinishStatus(ctx, *initial, gateway.StatusResult{State: "ready", SlackTS: knownTS, Acknowledged: true, NextRefreshAt: now.Add(time.Hour)}, now))
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='needs_review',gateway_execution_reason='reconciliation_deadline',slack_progress_phase='needs_review',slack_progress_state='settled',slack_progress_sent_revision=slack_progress_revision WHERE id=?", job.ID)
			storetest.Must(t, err)
			storetest.Must(t, root.Recover(ctx))
			final, err := store.ClaimStatus(ctx, now.Add(time.Second), "urgent")
			storetest.Must(t, err)
			if final == nil || final.Target != gateway.StatusTargetMessage || final.SlackTS != knownTS || !final.Terminal || !gateway.ConversationClosed(final.Phase) {
				t.Fatalf("settled legacy progress did not receive a closure update: %+v", final)
			}
			if strings.Contains(gateway.StatusText(*final, now.Add(72*time.Hour)), "Elapsed:") {
				t.Fatal("legacy final update retained the offline execution timer")
			}
			storetest.Must(t, store.FinishStatus(ctx, *final, gateway.StatusResult{State: "settled", SlackTS: knownTS, Acknowledged: true}, now.Add(time.Second)))
			storetest.Must(t, root.Recover(ctx))
			if status, err := store.ClaimStatus(ctx, now.Add(72*time.Hour), "heartbeat"); err != nil || status != nil {
				t.Fatalf("known terminal status resumed refresh after restart: %+v %v", status, err)
			}
			if closure024CountNotices(t, db) != 1 {
				t.Fatal("legacy status repair duplicated the closure notice")
			}
		})
	}
}
