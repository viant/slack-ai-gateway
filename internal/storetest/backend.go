package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

func backendSnapshot(t *testing.T, s gateway.Store) *storage.Status {
	t.Helper()
	state, err := s.(storage.Store).Status(context.Background(), 100)
	Must(t, err)
	return state
}

func runBackendOutage(t *testing.T, open func(*testing.T) Fixture) {
	ctx := context.Background()
	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		for _, reason := range []string{"backend_unavailable", "backend_blocked"} {
			t.Run(fmt.Sprintf("backend_outage_intake_%s_%s", mode, reason), func(t *testing.T) {
				f := open(t)
				s := f.Store
				m := Message("offline", "one", "alice")
				m.StatusMode, m.BackendReason = mode, reason
				enqueue(t, s, m)
				duplicate := m
				duplicate.Text, duplicate.BackendReason = "changed", "backend_blocked"
				inserted, err := s.Enqueue(ctx, duplicate)
				Must(t, err)
				if inserted {
					t.Fatal("offline duplicate inserted")
				}
				state := backendSnapshot(t, s)
				if len(state.Jobs) != 1 || state.Jobs[0].State != "completed" || state.Jobs[0].Reason != reason || state.Jobs[0].TurnID != "" {
					t.Fatalf("intake was not terminal: %+v", state.Jobs)
				}
				if len(state.RequestStatuses) != 1 || state.RequestStatuses[0].Outcome != "failed" || len(state.AgentSessions) != 0 {
					t.Fatalf("local rejection claimed a remote session or outcome: %+v", state)
				}
				noJob(t, s)
				d, err := s.ClaimDelivery(ctx)
				Must(t, err)
				if d == nil || d.Text != gateway.BackendNotice(reason) || d.Format != gateway.ReplyPlain {
					t.Fatalf("missing local failure reply: %+v", d)
				}
				Must(t, s.Delivered(ctx, d.ID, "reply-ts"))
				s = f.Reopen()
				Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
				noJob(t, s)
				if d, err = s.ClaimDelivery(ctx); err != nil || d != nil {
					t.Fatalf("restart duplicated local reply: %+v, %v", d, err)
				}
			})
		}
	}

	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusHybrid} {
		t.Run("backend_outage_existing_status_"+string(mode), func(t *testing.T) {
			s := open(t).Store
			m := Message("not-submitted", "one", "alice")
			m.StatusMode = mode
			enqueue(t, s, m)
			now := time.Now().Truncate(time.Millisecond)
			var p *gateway.RequestStatus
			for n := 0; n < 2; n++ {
				var err error
				p, err = s.ClaimStatus(ctx, now, "urgent")
				Must(t, err)
				if p == nil {
					t.Fatal("missing initial request status")
				}
				if p.Target == gateway.StatusTargetMessage {
					break
				}
				Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "active", SessionStatus: "active"}, now))
			}
			if p.Target != gateway.StatusTargetMessage || p.Submitted {
				t.Fatalf("unstarted status claimed remote submission: %+v", p)
			}
			Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "ready", SlackTS: "existing-status-ts", NextRefreshAt: now.Add(30 * time.Second)}, now))
			Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
			p, err := s.ClaimStatus(ctx, now.Add(time.Second), "backend_recovering")
			Must(t, err)
			if p == nil || p.SlackTS != "existing-status-ts" || p.Phase != "not_submitted" || !p.Terminal || p.Submitted {
				t.Fatalf("local rejection was presented as backend success: %+v", p)
			}
			_, err = s.GetStatusContext(ctx, *p)
			Must(t, err)
			Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", SlackTS: "existing-status-ts"}, now.Add(time.Second)))
		})
	}

	t.Run("backend_outage_closes_only_unstarted_and_never_replays", func(t *testing.T) {
		f := open(t)
		s := f.Store
		var closed []*gateway.Job
		for _, event := range []string{"claimed", "reserved", "ready"} {
			enqueue(t, s, Message(event, event, "alice"))
			j := claim(t, s)
			if event == "reserved" {
				_, err := s.BindJob(ctx, j.ID, decision())
				Must(t, err)
			} else if event == "ready" {
				ready(t, s, j)
			}
			closed = append(closed, j)
		}
		enqueue(t, s, Message("creating", "creating", "alice"))
		creating := claim(t, s)
		binding, err := s.BindJob(ctx, creating.ID, decision())
		Must(t, err)
		Must(t, s.BeginCreate(ctx, binding.ID))
		enqueue(t, s, Message("completed", "completed", "alice"))
		completed := claim(t, s)
		Must(t, s.Complete(ctx, completed.ID, "", "existing answer", []gateway.ReplyPart{{Text: "existing answer", Format: gateway.ReplyPlain}}))
		enqueue(t, s, Message("started", "started", "alice"))
		started := claim(t, s)
		ready(t, s, started)
		Must(t, s.BeginQuery(ctx, started.ID))
		Must(t, s.SchedulePoll(ctx, started.ID, time.Now().Add(-time.Second)))
		var deadline int64
		for _, j := range backendSnapshot(t, s).Jobs {
			if j.ID == started.ID {
				deadline = j.QueryStartedAt
			}
		}
		enqueue(t, s, Message("pending", "pending", "alice"))
		Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
		Must(t, s.FailUnstarted(ctx, "backend_blocked"))
		for _, j := range closed {
			if _, err := s.BindJob(ctx, j.ID, decision()); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale binding did not identify local rejection: %v", err)
			}
			if err := s.BeginQuery(ctx, j.ID); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale submission did not identify local rejection: %v", err)
			}
			if err := s.Complete(ctx, j.ID, "", "stale answer", []gateway.ReplyPart{{Text: "stale answer", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale result overwrote rejection: %v", err)
			}
			if err := s.Review(ctx, j.ID, "stale policy review"); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale review did not identify local rejection: %v", err)
			}
			if err := s.Reject(ctx, j.ID, "stale policy rejection"); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale policy rejection did not identify local rejection: %v", err)
			}
			if err := s.RecordObservation(ctx, j.ID, "completed", time.Now()); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("stale observation overwrote local outcome: %v", err)
			}
		}
		state := backendSnapshot(t, s)
		localCount := 0
		for _, j := range state.Jobs {
			switch j.ID {
			case creating.ID:
				if j.State != "processing" || j.Reason != "" {
					t.Fatalf("unknown conversation creation closed: %+v", j)
				}
			case started.ID:
				if j.State != "querying" || j.TurnID != started.RequestID || j.QueryStartedAt != deadline || deadline == 0 {
					t.Fatalf("started request/deadline changed: %+v", j)
				}
			case completed.ID:
				if j.State != "completed" || j.Reason != "" {
					t.Fatalf("completed answer changed: %+v", j)
				}
			default:
				if j.State != "completed" || j.Reason != "backend_unavailable" {
					t.Fatalf("unstarted work not closed exactly once: %+v", j)
				}
				localCount++
			}
		}
		if localCount != 4 {
			t.Fatalf("closed %d unstarted requests, expected 4", localCount)
		}
		for n := 0; n < 5; n++ {
			d, err := s.ClaimDelivery(ctx)
			Must(t, err)
			if d == nil || (d.Text != "existing answer" && d.Text != gateway.BackendNotice("backend_unavailable")) {
				t.Fatalf("missing or changed durable reply: %+v", d)
			}
			Must(t, s.Delivered(ctx, d.ID, fmt.Sprintf("reply-%d", n)))
		}
		s = f.Reopen()
		Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
		reconciled := claim(t, s)
		if reconciled.ID != started.ID || reconciled.TurnID != started.RequestID || reconciled.QueryStartedAt.UnixMilli() != deadline {
			t.Fatalf("restart did not preserve original started request: %+v", reconciled)
		}
		noJob(t, s)
		if d, err := s.ClaimDelivery(ctx); err != nil || d != nil {
			t.Fatalf("restart duplicated failure replies: %+v, %v", d, err)
		}
		for _, j := range backendSnapshot(t, s).Jobs {
			if j.ID == creating.ID && j.State != "needs_review" {
				t.Fatalf("unknown creation was replayed after restart: %+v", j)
			}
		}
	})

	for _, phase := range []string{"pending", "processing", "late_create"} {
		t.Run("backend_outage_per_job_reject_"+phase, func(t *testing.T) {
			s := open(t).Store
			enqueue(t, s, Message("closed", "closed", "alice"))
			var jobID int64
			if phase == "pending" {
				jobID = backendSnapshot(t, s).Jobs[0].ID
			} else {
				j := claim(t, s)
				jobID = j.ID
				if phase == "late_create" {
					b, err := s.BindJob(ctx, j.ID, decision())
					Must(t, err)
					Must(t, s.BeginCreate(ctx, b.ID))
					if err := s.Reject(ctx, j.ID, "backend_unavailable"); !errors.Is(err, gateway.ErrConflict) {
						t.Fatalf("unknown create was closed: %v", err)
					}
					Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
					Must(t, s.SetConversation(ctx, b.ID, "late-created-conversation"))
				}
			}
			// Fresh work can arrive after readiness returns while this older
			// worker is still finishing its local outage transition.
			enqueue(t, s, Message("healthy", "healthy", "alice"))
			Must(t, s.Reject(ctx, jobID, "backend_unavailable"))
			if err := s.Reject(ctx, jobID, "backend_blocked"); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Fatalf("repeated local close changed its terminal result: %v", err)
			}
			state := backendSnapshot(t, s)
			for _, j := range state.Jobs {
				if j.ID == jobID {
					if j.State != "completed" || j.Reason != "backend_unavailable" || j.TurnID != "" {
						t.Fatalf("specific unsubmitted job was not closed: %+v", j)
					}
				} else if j.State != "pending" || j.Reason != "" {
					t.Fatalf("healthy work was touched by stale worker: %+v", j)
				}
			}
			d, err := s.ClaimDelivery(ctx)
			Must(t, err)
			if d == nil || d.JobID != jobID || d.Text != gateway.BackendNotice("backend_unavailable") {
				t.Fatalf("per-job local reply missing: %+v", d)
			}
			Must(t, s.Delivered(ctx, d.ID, "notice-ts"))
			healthy := claim(t, s)
			Must(t, s.Reject(ctx, healthy.ID, "policy_denied"))
			for _, j := range backendSnapshot(t, s).Jobs {
				if j.ID == healthy.ID && (j.State != "rejected" || j.Reason != "policy_denied") {
					t.Fatalf("ordinary policy rejection semantics changed: %+v", j)
				}
			}
			if d, err = s.ClaimDelivery(ctx); err != nil || d != nil {
				t.Fatalf("per-job rejection duplicated a reply or changed policy rejection: %+v, %v", d, err)
			}
		})
	}

	t.Run("backend_outage_agent_session_completion_during_first_fallback", func(t *testing.T) {
		s := open(t).Store
		m := Message("fallback-race", "one", "alice")
		m.StatusMode = gateway.StatusAgentSession
		enqueue(t, s, m)
		j := claim(t, s)
		ready(t, s, j)
		Must(t, s.BeginQuery(ctx, j.ID))
		now := time.Now().Truncate(time.Millisecond)
		p, err := s.ClaimStatus(ctx, now, "backend_recovering")
		Must(t, err)
		if p == nil || p.Target != gateway.StatusTargetMessage || p.Phase != "backend_recovering" || p.SlackTS != "" {
			t.Fatalf("missing first outage fallback claim: %+v", p)
		}
		Must(t, s.RecordObservation(ctx, j.ID, "completed", now))
		Must(t, s.Complete(ctx, j.ID, j.RequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
		// The claim's revision and durable phase still own the publication,
		// even though remote execution completed before the Slack send.
		_, err = s.GetStatusContext(ctx, *p)
		Must(t, err)
		Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "ready", SlackTS: "fallback-status-ts", NextRefreshAt: now.Add(30 * time.Second)}, now))
		d, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if d == nil {
			t.Fatal("completed answer disappeared during fallback publication")
		}
		Must(t, s.Delivered(ctx, d.ID, "answer-ts"))
		for n := 0; n < 3; n++ {
			p, err = s.ClaimStatus(ctx, now.Add(time.Second), "urgent")
			Must(t, err)
			if p == nil {
				t.Fatal("completion did not refresh the first outage fallback")
			}
			if p.Target == gateway.StatusTargetAgentSession {
				Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "active", SessionStatus: "active"}, now.Add(time.Second)))
				continue
			}
			if p.SlackTS != "fallback-status-ts" || p.Phase != "completed" || !p.Terminal {
				t.Fatalf("completion did not reuse the valid fallback receipt: %+v", p)
			}
			_, err = s.GetStatusContext(ctx, *p)
			Must(t, err)
			Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", SlackTS: "fallback-status-ts"}, now.Add(time.Second)))
			return
		}
		t.Fatal("normal session updates starved fallback completion")
	})

	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		t.Run("backend_outage_started_status_"+string(mode), func(t *testing.T) {
			s := open(t).Store
			m := Message("started", "one", "alice")
			m.StatusMode = mode
			enqueue(t, s, m)
			j := claim(t, s)
			ready(t, s, j)
			Must(t, s.BeginQuery(ctx, j.ID))
			now := time.Now().Truncate(time.Millisecond)
			p, err := s.ClaimStatus(ctx, now, "backend_recovering")
			Must(t, err)
			if p == nil || p.JobID != j.ID || p.Mode != mode || p.Target != gateway.StatusTargetMessage || p.Phase != "backend_recovering" || !p.Submitted || p.Terminal {
				t.Fatalf("missing started outage fallback: %+v", p)
			}
			_, err = s.GetStatusContext(ctx, *p)
			Must(t, err)
			Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "ready", SlackTS: "one-status-ts", NextRefreshAt: now.Add(30 * time.Second)}, now))
			if p, err = s.ClaimStatus(ctx, now.Add(time.Second), "backend_recovering"); err != nil || p != nil {
				t.Fatalf("outage heartbeat ignored refresh interval: %+v, %v", p, err)
			}
			p, err = s.ClaimStatus(ctx, now.Add(2*time.Second), "backend_blocked")
			Must(t, err)
			if p == nil || p.SlackTS != "one-status-ts" || p.Phase != "backend_blocked" {
				t.Fatalf("outage transition did not reuse message: %+v", p)
			}
			Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "ready", SlackTS: "one-status-ts", NextRefreshAt: now.Add(32 * time.Second)}, now.Add(2*time.Second)))
			Must(t, s.RecordObservation(ctx, j.ID, "completed", now.Add(3*time.Second)))
			Must(t, s.Complete(ctx, j.ID, j.RequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
			d, err := s.ClaimDelivery(ctx)
			Must(t, err)
			Must(t, s.Delivered(ctx, d.ID, "answer-ts"))
			// Normal publication resumes after recovery. A native session update
			// may be due before the message target, but never needs a new message.
			for n := 0; n < 3; n++ {
				p, err = s.ClaimStatus(ctx, now.Add(4*time.Second), "urgent")
				Must(t, err)
				if p == nil {
					t.Fatal("fallback was not completed after recovery")
				}
				_, err = s.GetStatusContext(ctx, *p)
				Must(t, err)
				if p.Target == gateway.StatusTargetAgentSession {
					Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", Acknowledged: true, AgentStatus: "active", SessionStatus: "active"}, now.Add(4*time.Second)))
					continue
				}
				if p.JobID != j.ID || p.SlackTS != "one-status-ts" || p.Phase != "completed" || !p.Terminal {
					t.Fatalf("recovery lost terminal message update: %+v", p)
				}
				Must(t, s.FinishStatus(ctx, *p, gateway.StatusResult{State: "settled", SlackTS: "one-status-ts"}, now.Add(4*time.Second)))
				return
			}
			t.Fatal("normal session claims starved fallback completion")
		})
	}
}
