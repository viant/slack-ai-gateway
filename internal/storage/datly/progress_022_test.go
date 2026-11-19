package datly_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestPhysicalThreadClaimsAndNativeDeliveryResolution(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			scoped := root.Scope(id)
			first := storetest.Message("owner-first", "1.000001", "alice")
			second := storetest.Message("owner-second", "2.000001", "bob")
			first.StatusMode, second.StatusMode = gateway.StatusAgentSession, gateway.StatusAgentSession
			_, err := scoped.Enqueue(ctx, first)
			storetest.Must(t, err)
			_, err = scoped.Enqueue(ctx, second)
			storetest.Must(t, err)
			a, b := gateway.ForThread(scoped, first.PhysicalThread()), gateway.ForThread(scoped, second.PhysicalThread())
			mismatch := second
			mismatch.EventID = "wrong-owner-intake"
			if _, err := a.Enqueue(ctx, mismatch); !errors.Is(err, gateway.ErrConflict) {
				t.Fatalf("intake mutated another owner's key: %v", err)
			}
			job, err := b.ClaimJob(ctx)
			storetest.Must(t, err)
			if job == nil || job.EventID != second.EventID {
				t.Fatalf("thread predicate was applied after LIMIT: %+v", job)
			}
			if err = a.BeginQuery(ctx, job.ID); !errors.Is(err, gateway.ErrConflict) {
				t.Fatalf("cross-thread ID accepted: %v", err)
			}
			now := time.Now().UTC().Truncate(time.Millisecond)
			status, err := b.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			if status == nil || status.Target != gateway.StatusTargetAgentSession || status.RootTS != second.RootTS {
				t.Fatalf("native claim escaped owner: %+v", status)
			}
			storetest.Must(t, b.FinishStatus(ctx, *status, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "processing", SessionStatus: "processing", NextRefreshAt: now.Add(gateway.AgentSessionRefresh)}, now))
			binding, err := b.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
			storetest.Must(t, err)
			if _, err := b.GetBinding(ctx, first.ThreadKey); !errors.Is(err, gateway.ErrConflict) {
				t.Fatalf("another thread's explicit binding key was silently substituted: %v", err)
			}
			bound, err := b.GetBinding(ctx, second.ThreadKey)
			storetest.Must(t, err)
			if bound == nil || bound.ID != binding.ID {
				t.Fatalf("owner binding lookup failed: %+v", bound)
			}
			storetest.Must(t, b.BeginCreate(ctx, binding.ID))
			storetest.Must(t, b.SetConversation(ctx, binding.ID, "physical-owner-test"))
			storetest.Must(t, b.BeginQuery(ctx, job.ID))
			storetest.Must(t, b.RecordObservation(ctx, job.ID, "completed", now))
			storetest.Must(t, b.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "part one", Format: gateway.ReplyPlain}, {Text: "part two", Format: gateway.ReplyPlain}}))
			if wrong, err := a.ClaimDelivery(ctx); err != nil || wrong != nil {
				t.Fatalf("another thread's response claimed: %+v %v", wrong, err)
			}
			part, err := b.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, b.Delivered(ctx, part.ID, "20.000001"))
			if early, err := b.ClaimStatus(ctx, now.Add(time.Second), "urgent"); err != nil || early != nil {
				t.Fatalf("processing ended before last part: %+v %v", early, err)
			}
			part, err = b.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, b.DeliveryUnknown(ctx, part.ID))
			suspended, err := b.ClaimStatus(ctx, now.Add(time.Second), "urgent")
			storetest.Must(t, err)
			if suspended == nil || suspended.SessionState != "suspended" {
				t.Fatalf("uncertain delivery did not suspend: %+v", suspended)
			}
			storetest.Must(t, b.FinishStatus(ctx, *suspended, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "suspended", SessionStatus: "suspended"}, now.Add(time.Second)))
			if clean, err := b.ClaimStatus(ctx, now.Add(2*time.Second), "heartbeat"); err != nil || clean != nil {
				t.Fatalf("clean suspended session was polled: %+v %v", clean, err)
			}
			followup := second
			followup.EventID, followup.MessageTS, followup.AuthorID = "older-blocker-followup", "2.000002", "charlie"
			_, err = b.Enqueue(ctx, followup)
			storetest.Must(t, err)
			blocked, err := b.ClaimStatus(ctx, now.Add(2*time.Second), "urgent")
			storetest.Must(t, err)
			if blocked == nil || blocked.SessionState != "suspended" || blocked.JobID != job.ID {
				t.Fatalf("newer question hid the older completed bad response: %+v", blocked)
			}
			storetest.Must(t, b.FinishStatus(ctx, *blocked, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "suspended", SessionStatus: "suspended"}, now.Add(2*time.Second)))
			newer, err := b.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, b.Reject(ctx, newer.ID, "fixture_finished_without_backend"))
			storetest.Must(t, root.ResolveDelivery(ctx, part.ID, "delivered", "20.000002", false))
			var revision, sent int
			storetest.Must(t, db.QueryRow("SELECT slack_publication_revision,slack_publication_sent_revision FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", second.RootTS).Scan(&revision, &sent))
			if revision <= sent {
				t.Fatal("offline resolution failed to persist dirty intent")
			}
			active, err := b.ClaimStatus(ctx, now.Add(3*time.Second), "urgent")
			storetest.Must(t, err)
			if active == nil || active.SessionState != "active" {
				t.Fatalf("resolved session remained suspended: %+v", active)
			}
		})
	}
}

func TestClosedNativeCycleDoesNotReopenForMessageOnlyTurn(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			scoped := root.Scope(id)
			m := storetest.Message("cycle-native", "3.000001", "alice")
			m.StatusMode = gateway.StatusAgentSession
			_, err := scoped.Enqueue(ctx, m)
			storetest.Must(t, err)
			s := gateway.ForThread(scoped, m.PhysicalThread())
			now := time.Now().UTC().Truncate(time.Millisecond)
			processing, err := s.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			storetest.Must(t, s.FinishStatus(ctx, *processing, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "processing", SessionStatus: "processing", NextRefreshAt: now.Add(gateway.AgentSessionRefresh)}, now))
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			binding, err := s.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
			storetest.Must(t, err)
			storetest.Must(t, s.BeginCreate(ctx, binding.ID))
			storetest.Must(t, s.SetConversation(ctx, binding.ID, "cycle-backend"))
			storetest.Must(t, s.BeginQuery(ctx, job.ID))
			storetest.Must(t, s.RecordObservation(ctx, job.ID, "completed", now))
			storetest.Must(t, s.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
			part, err := s.ClaimDelivery(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Delivered(ctx, part.ID, "30.000001"))
			active, err := s.ClaimStatus(ctx, now.Add(time.Second), "urgent")
			storetest.Must(t, err)
			if active == nil || active.SessionState != "active" {
				t.Fatalf("missing native completion: %+v", active)
			}
			storetest.Must(t, s.FinishStatus(ctx, *active, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "active", SessionStatus: "active"}, now.Add(time.Second)))
			var before int
			storetest.Must(t, db.QueryRow("SELECT slack_publication_revision FROM slack_agent_session_publication").Scan(&before))
			followup := m
			followup.EventID, followup.MessageTS, followup.StatusMode = "cycle-message", "3.000002", gateway.StatusMessage
			_, err = scoped.Enqueue(ctx, followup)
			storetest.Must(t, err)
			var after int
			var desired string
			storetest.Must(t, db.QueryRow("SELECT slack_publication_revision,slack_agent_desired_status FROM slack_agent_session_publication").Scan(&after, &desired))
			if after != before || desired != "active" {
				t.Fatalf("message turn reopened closed cycle: revision %d -> %d, desired %s", before, after, desired)
			}
			followup.EventID, followup.MessageTS, followup.StatusMode = "cycle-native-next", "3.000003", gateway.StatusAgentSession
			_, err = scoped.Enqueue(ctx, followup)
			storetest.Must(t, err)
			storetest.Must(t, db.QueryRow("SELECT slack_publication_revision,slack_agent_desired_status FROM slack_agent_session_publication").Scan(&after, &desired))
			if after <= before || desired != "processing" {
				t.Fatalf("native turn did not open next cycle: revision %d -> %d, desired %s", before, after, desired)
			}
		})
	}
}

func TestMessageRefreshDoesNotInventObservationUncertainty(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			scoped := root.Scope(id)
			m := storetest.Message("long-refresh", "4.000001", "alice")
			_, err := scoped.Enqueue(ctx, m)
			storetest.Must(t, err)
			s := gateway.ForThread(scoped, m.PhysicalThread())
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			now := time.Now().UTC().Truncate(time.Millisecond)
			storetest.Must(t, s.RecordObservation(ctx, job.ID, "running", now))
			status, err := s.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			if status == nil || status.Phase != "running" {
				t.Fatalf("missing running phase: %+v", status)
			}
			storetest.Must(t, s.FinishStatus(ctx, *status, gateway.StatusResult{State: "ready", SlackTS: "40.000001", NextRefreshAt: now.Add(time.Hour)}, now))
			var refresh time.Time
			storetest.Must(t, db.QueryRow("SELECT slack_progress_refresh_at FROM gateway_job WHERE id=?", job.ID).Scan(&refresh))
			if !refresh.Equal(now.Add(time.Hour)) {
				t.Fatalf("healthy observation shortened the configured refresh: %s", refresh)
			}
			later := now.Add(18 * time.Minute)
			if duplicate, err := s.ClaimStatus(ctx, later, "urgent"); err != nil || duplicate != nil {
				t.Fatalf("time alone changed confirmed running: %+v %v", duplicate, err)
			}
			storetest.Must(t, s.RecordObservation(ctx, job.ID, "unknown", later))
			checking, err := s.ClaimStatus(ctx, later, "urgent")
			storetest.Must(t, err)
			if checking == nil || checking.Phase != "checking" {
				t.Fatalf("actual observation failure did not publish checking: %+v", checking)
			}
			next := later.Add(time.Hour)
			storetest.Must(t, s.FinishStatus(ctx, *checking, gateway.StatusResult{State: "ready", SlackTS: "40.000001", NextRefreshAt: next}, later))
			storetest.Must(t, db.QueryRow("SELECT slack_progress_refresh_at FROM gateway_job WHERE id=?", job.ID).Scan(&refresh))
			if !refresh.Equal(next) {
				t.Fatalf("checking altered the configured refresh: %s", refresh)
			}
			if duplicate, err := s.ClaimStatus(ctx, later.Add(time.Second), "urgent"); err != nil || duplicate != nil {
				t.Fatalf("checking busy loop: %+v %v", duplicate, err)
			}
		})
	}
}

func TestStartupRepairsAcknowledgedProcessingFromCanonicalBlocker(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			scoped := root.Scope(id)
			m := storetest.Message("startup-processing-repair", "5.000001", "alice")
			m.StatusMode = gateway.StatusAgentSession
			_, err := scoped.Enqueue(ctx, m)
			storetest.Must(t, err)
			s := gateway.ForThread(scoped, m.PhysicalThread())
			now := time.Now().UTC().Truncate(time.Millisecond)
			processing, err := s.ClaimStatus(ctx, now, "urgent")
			storetest.Must(t, err)
			storetest.Must(t, s.FinishStatus(ctx, *processing, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "processing", SessionStatus: "processing", NextRefreshAt: now.Add(gateway.AgentSessionRefresh)}, now))
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='needs_review',gateway_execution_reason='interrupted_external_operation' WHERE id=?", job.ID)
			storetest.Must(t, err)
			// Historical acknowledged processing could survive while its source became
			// blocked. Startup must repair that intent without waiting ten minutes.
			_, err = db.Exec("UPDATE slack_agent_session_publication SET slack_agent_desired_status='processing',slack_publication_state='settled',slack_publication_sent_revision=slack_publication_revision")
			storetest.Must(t, err)
			storetest.Must(t, root.Recover(ctx))
			var desired, state string
			var revision, sent int
			storetest.Must(t, db.QueryRow("SELECT slack_agent_desired_status,slack_publication_state,slack_publication_revision,slack_publication_sent_revision FROM slack_agent_session_publication").Scan(&desired, &state, &revision, &sent))
			if desired != "active" || state != "ready" || revision <= sent {
				t.Fatalf("startup retained stale native processing: desired=%s state=%s revisions=%d/%d", desired, state, revision, sent)
			}
			recovered, err := s.ClaimStatus(ctx, now.Add(time.Second), "urgent")
			storetest.Must(t, err)
			if recovered == nil || recovered.SessionState != "active" {
				t.Fatalf("repair was not immediately publishable: %+v", recovered)
			}
		})
	}
}

func TestStaleThreadMetadataKeepsClosedBindingWithoutJobsAndBotScope(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a, b := testIdentity("stale-a", "A"), testIdentity("stale-b", "B")
			registerNativeIntegrations(t, root, &a, &b)
			first := storetest.Message("stale-original", "6.000001", "alice")
			as := root.Scope(a.ID)
			_, err := as.Enqueue(ctx, first)
			storetest.Must(t, err)
			job, err := as.ClaimJob(ctx)
			storetest.Must(t, err)
			binding, err := as.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE conversation_binding SET gateway_binding_state='closed',closed_at=? WHERE id=?", time.Now().UTC().Truncate(time.Millisecond), binding.ID)
			storetest.Must(t, err)
			_, err = db.Exec("DELETE FROM gateway_job WHERE id=?", job.ID)
			storetest.Must(t, err)
			followup := first
			followup.EventID = "stale-after-retention"
			followup.MessageTS = "6.000002"
			_, err = as.Enqueue(ctx, followup)
			storetest.Must(t, err)
			var reason string
			storetest.Must(t, db.QueryRow("SELECT gateway_execution_reason FROM gateway_job WHERE slack_event_id=?", followup.EventID).Scan(&reason))
			if reason != "conversation_closed" {
				t.Fatalf("closed binding without jobs was ignored: %q", reason)
			}
			followup.AppID = "B"
			followup.EventID = "other-bot-same-thread"
			bs := root.Scope(b.ID)
			_, err = bs.Enqueue(ctx, followup)
			storetest.Must(t, err)
			other, err := bs.ClaimJob(ctx)
			storetest.Must(t, err)
			if other == nil || other.EventID != followup.EventID {
				t.Fatalf("another bot inherited stale evidence: %+v", other)
			}
		})
	}
}
