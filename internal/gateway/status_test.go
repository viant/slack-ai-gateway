package gateway_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type statusCall struct {
	Status gateway.RequestStatus
	Text   string
}
type statusSlack struct {
	calls []statusCall
	order []string
	fail  func(int) error
}

func (s *statusSlack) SendStatus(_ context.Context, status gateway.RequestStatus, text string) (gateway.StatusReceipt, error) {
	s.calls = append(s.calls, statusCall{status, text})
	s.order = append(s.order, "status")
	if s.fail != nil {
		if err := s.fail(len(s.calls)); err != nil {
			return gateway.StatusReceipt{SlackTS: ""}, err
		}
	}
	if status.Target == gateway.StatusTargetAgentSession {
		return gateway.StatusReceipt{Acknowledged: true, AgentStatus: status.SessionState, SessionStatus: status.SessionState}, nil
	}
	if status.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: status.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "2.000001"}, nil
}
func (s *statusSlack) Send(_ context.Context, d gateway.Delivery) (string, error) {
	s.order = append(s.order, "final")
	return "3.000001", nil
}

type statusPolicyFunc func(context.Context, gateway.DeliveryContext, bool) error

func (f statusPolicyFunc) AuthorizeDelivery(ctx context.Context, d gateway.DeliveryContext, fresh bool) error {
	return f(ctx, d, fresh)
}
func statusStep(t *testing.T, w *gateway.StatusWorker, kind string, want bool) {
	t.Helper()
	worked, err := w.Step(context.Background(), kind)
	storetest.Must(t, err)
	if worked != want {
		t.Fatalf("status worked=%v, want %v", worked, want)
	}
}

func TestStatusFirstPostErrorsDoNotBlockFinalDelivery(t *testing.T) {
	for _, kind := range []gateway.SendErrorKind{gateway.SendUnknown, gateway.SendRejected} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, reopen := database(t)
			now := time.Now()
			accept(t, s, storetest.Message("a", "one", "alice"))
			sender := &statusSlack{fail: func(int) error { return &gateway.SendError{Kind: kind} }}
			w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), Now: func() time.Time { return now }}
			statusStep(t, w, "urgent", true)
			s = reopen()
			w.Store = s
			now = now.Add(time.Hour)
			statusStep(t, w, "urgent", false)
			work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
			d := &gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy()}
			worked, err := d.Step(ctx)
			storetest.Must(t, err)
			if !worked || len(sender.calls) != 1 || len(sender.order) != 2 || sender.order[1] != "final" {
				t.Fatal("status failure blocked final or duplicated first POST")
			}
		})
	}
}

func TestStatusUnknownUpdateRetriesLatestSnapshotWithoutNewPost(t *testing.T) {
	ctx := context.Background()
	s, _ := database(t)
	now := time.Now()
	accept(t, s, storetest.Message("a", "one", "alice"))
	sender := &statusSlack{fail: func(n int) error {
		if n == 2 {
			return &gateway.SendError{Kind: gateway.SendUnknown}
		}
		return nil
	}}
	w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), RefreshInterval: 5 * time.Minute, Now: func() time.Time { return now }}
	statusStep(t, w, "urgent", true)
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{state: "running"}, AgentID: "steward"})
	statusStep(t, w, "urgent", true)
	statusStep(t, w, "urgent", false)
	state, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	j := state.Jobs[0]
	now = now.Add(31 * time.Second)
	storetest.Must(t, s.RecordObservation(ctx, j.ID, "completed", now))
	storetest.Must(t, s.Complete(ctx, j.ID, j.RequestID, "latest", []gateway.ReplyPart{{Text: "latest", Format: gateway.ReplyPlain}}))
	statusStep(t, w, "urgent", true)
	if len(sender.calls) != 3 || sender.calls[2].Status.SlackTS != "2.000001" || sender.calls[2].Status.Phase != "delivering" {
		t.Fatal("update replayed obsolete status or created a new message")
	}
	// A terminal update with an uncertain result must also be retried.
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Delivered(ctx, d.ID, "4.000001"))
	sender.fail = func(n int) error {
		if n == 4 {
			return &gateway.SendError{Kind: gateway.SendUnknown}
		}
		return nil
	}
	statusStep(t, w, "urgent", true)
	now = now.Add(31 * time.Second)
	statusStep(t, w, "urgent", true)
	if sender.calls[4].Status.Phase != "completed" {
		t.Fatal("terminal update was lost")
	}
}

func TestDeletedStatusIsNotRecreated(t *testing.T) {
	s, _ := database(t)
	now := time.Now()
	accept(t, s, storetest.Message("a", "one", "alice"))
	sender := &statusSlack{fail: func(n int) error {
		if n > 1 {
			return &gateway.SendError{Kind: gateway.SendRejected}
		}
		return nil
	}}
	w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), Now: func() time.Time { return now }}
	statusStep(t, w, "urgent", true)
	now = now.Add(30 * time.Second)
	statusStep(t, w, "heartbeat", true)
	now = now.Add(time.Hour)
	statusStep(t, w, "heartbeat", false)
	if len(sender.calls) != 2 || sender.calls[1].Status.SlackTS == "" {
		t.Fatal("replacement message created")
	}
}

func TestStatusChecksOriginalAuthorAndRevocationOnEveryWrite(t *testing.T) {
	for _, initialDenied := range []bool{false, true} {
		t.Run(map[bool]string{false: "revoked", true: "initially_denied"}[initialDenied], func(t *testing.T) {
			ctx := context.Background()
			s, _ := database(t)
			now := time.Now()
			m := storetest.Message("a", "one", "alice")
			accept(t, s, m)
			denied := initialDenied
			checks := 0
			p := statusPolicyFunc(func(_ context.Context, d gateway.DeliveryContext, fresh bool) error {
				checks++
				if !fresh || d.Message.AuthorID != "alice" {
					t.Error("did not verify original author freshly")
				}
				if denied {
					return gateway.ErrDenied
				}
				return nil
			})
			sender := &statusSlack{}
			w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: p, Now: func() time.Time { return now }}
			statusStep(t, w, "urgent", true)
			if !initialDenied {
				denied = true
				now = now.Add(30 * time.Second)
				statusStep(t, w, "heartbeat", true)
			}
			now = now.Add(time.Hour)
			statusStep(t, w, "urgent", false)
			want := 1
			if initialDenied {
				want = 0
			}
			expectedChecks := 2
			if initialDenied {
				expectedChecks = 1
			}
			if checks != expectedChecks {
				t.Fatal("policy check skipped")
			}
			if len(sender.calls) != want {
				t.Fatal("unauthorized status was sent")
			}
			if b, err := s.GetBinding(ctx, m.ThreadKey); err != nil || b != nil {
				t.Fatal("status created a conversation binding")
			}
			metadata, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			if metadata.RequestStatuses[0].Reason != "policy_denied" {
				t.Fatal("missing denial metadata")
			}
		})
	}
}

func TestStatusMetadataFailuresAreDurableAndFailClosed(t *testing.T) {
	for _, cause := range []error{gateway.ErrSlackAuth, gateway.ErrMetadataUnavailable} {
		s, _ := database(t)
		accept(t, s, storetest.Message("a", "one", "alice"))
		sender := &statusSlack{}
		w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: statusPolicyFunc(func(context.Context, gateway.DeliveryContext, bool) error { return cause })}
		worked, err := w.Step(context.Background(), "urgent")
		if !worked || !errors.Is(err, cause) || len(sender.calls) != 0 {
			t.Fatal("metadata failure bypassed")
		}
		metadata, err := s.Status(context.Background(), 10)
		storetest.Must(t, err)
		if metadata.RequestStatuses[0].State != "ready" || metadata.RequestStatuses[0].Reason != "policy_unavailable" {
			t.Fatal("unsent status not preserved")
		}
	}
}

func TestOutboundFairnessPacingAndHeartbeatPriority(t *testing.T) {
	s, _ := database(t)
	now := time.Now()
	m := storetest.Message("final", "final-thread", "alice")
	m.Text = strings.Repeat("x", 25000)
	accept(t, s, m)
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
	accept(t, s, storetest.Message("ack1", "one", "bob"))
	accept(t, s, storetest.Message("ack2", "two", "carol"))
	sender := &statusSlack{}
	w := &gateway.OutboundWorker{Delivery: &gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy()}, Status: &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), Now: func() time.Time { return now }}, Now: func() time.Time { return now }, Interval: time.Second}
	expected := []string{"final", "status", "final", "status", "final"}
	for i, want := range expected {
		worked, err := w.Step(context.Background())
		storetest.Must(t, err)
		if !worked || sender.order[i] != want {
			t.Fatalf("unfair scheduling: %v", sender.order)
		}
		if worked, err := w.Step(context.Background()); err != nil || worked {
			t.Fatal("shared pacing bypassed")
		}
		now = now.Add(time.Second)
	}
	// Old statuses due for heartbeat cannot take precedence over a new final.
	for range 2 {
		work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{state: "running"}, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Hour, PollMax: time.Hour, MaxWait: 2 * time.Hour}})
	}
	statusStep(t, w.Status, "urgent", true)
	statusStep(t, w.Status, "urgent", true)
	now = now.Add(30 * time.Second)
	accept(t, s, storetest.Message("last-final", "last", "dave"))
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
	worked, err := w.Step(context.Background())
	storetest.Must(t, err)
	if !worked || sender.order[len(sender.order)-1] != "final" {
		t.Fatal("heartbeat starved a final reply")
	}
}

func TestStatusClockLongWaitDoesNotInventBackendProgress(t *testing.T) {
	s, _ := database(t)
	now := time.Now()
	accept(t, s, storetest.Message("a", "one", "alice"))
	sender := &statusSlack{}
	w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), RefreshInterval: 5 * time.Minute, Now: func() time.Time { return now }}
	statusStep(t, w, "urgent", true)
	// Anchor the fake clock to durable request time, independently of how long
	// the selected storage implementation takes to enqueue and load the graph.
	now = sender.calls[0].Status.CreatedAt
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{state: "running"}, AgentID: "steward"})
	storetest.Must(t, s.RecordObservation(context.Background(), sender.calls[0].Status.JobID, "running", now))
	statusStep(t, w, "urgent", true)
	now = now.Add(61 * time.Second)
	statusStep(t, w, "urgent", true)
	last := sender.calls[len(sender.calls)-1]
	if last.Status.Phase != "checking" || !strings.Contains(last.Text, "Last confirmed state: running at") || !strings.Contains(last.Text, "Elapsed: 1m1s") {
		t.Fatalf("stale observation presented as current progress: status=%+v text=%q", last.Status, last.Text)
	}
}

func TestConfiguredRefreshDoesNotDelayPhaseChangesOrChangeRetries(t *testing.T) {
	ctx := context.Background()
	s, _ := database(t)
	now := time.Now()
	accept(t, s, storetest.Message("a", "one", "alice"))
	sender := &statusSlack{}
	w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), RefreshInterval: 7 * time.Second, Now: func() time.Time { return now }}
	statusStep(t, w, "urgent", true)
	now = now.Add(6 * time.Second)
	statusStep(t, w, "heartbeat", false)
	now = now.Add(time.Second)
	statusStep(t, w, "heartbeat", true)
	w.RefreshInterval = 90 * time.Second // Restart changes the following successful update.
	now = now.Add(7 * time.Second)
	statusStep(t, w, "heartbeat", true)
	now = now.Add(8 * time.Second)
	statusStep(t, w, "heartbeat", false)
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
	statusStep(t, w, "urgent", true) // Result transition ignores the long interval.
	d := &gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy()}
	if worked, err := d.Step(ctx); err != nil || !worked {
		t.Fatalf("delivery: %v", err)
	}
	statusStep(t, w, "urgent", true)
	if sender.calls[len(sender.calls)-1].Status.Phase != "completed" {
		t.Fatal("completion delayed")
	}
	for _, call := range sender.calls[1:] {
		if call.Status.SlackTS != "2.000001" {
			t.Fatal("new status created instead of update")
		}
	}
}

func TestNativeStatusErrorsAndDeliveryUseExistingWorkers(t *testing.T) {
	for _, kind := range []gateway.SendErrorKind{gateway.SendUnknown, gateway.SendRejected} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			s, _ := database(t)
			now := time.Now()
			m := storetest.Message("a", "one", "alice")
			m.StatusMode = gateway.StatusAgentSession
			nativeIntake := intake(s)
			nativeIntake.StatusMode = gateway.StatusAgentSession
			accepted, err := nativeIntake.Accept(context.Background(), m)
			storetest.Must(t, err)
			if !accepted.Accepted {
				t.Fatal("native intake rejected")
			}
			sender := &statusSlack{fail: func(n int) error {
				if n == 1 {
					return &gateway.SendError{Kind: kind, Code: "feature_disabled"}
				}
				return nil
			}}
			statuses := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), Now: func() time.Time { return now }}
			statusStep(t, statuses, "urgent", true)
			work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
			delivery := &gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy()}
			outbound := &gateway.OutboundWorker{Delivery: delivery, Status: statuses, Now: func() time.Time { return now }}
			worked, err := outbound.Step(ctx)
			storetest.Must(t, err)
			if !worked || sender.order[len(sender.order)-1] != "final" {
				t.Fatal("indicator failure blocked reply")
			}
			statusStep(t, statuses, "urgent", false)
			now = now.Add(gateway.StatusRetry)
			statusStep(t, statuses, "urgent", kind == gateway.SendUnknown)
			for _, call := range sender.calls {
				if call.Text != "" || call.Status.SlackTS != "" {
					t.Fatal("native mode emitted ordinary progress")
				}
			}
			if kind == gateway.SendUnknown && sender.calls[1].Status.SessionState != "active" {
				t.Fatal("retry replayed obsolete processing")
			}
		})
	}
}
func TestNativeStatusRechecksOriginalAuthor(t *testing.T) {
	ctx := context.Background()
	s, _ := database(t)
	now := time.Now()
	m := storetest.Message("a", "one", "alice")
	m.StatusMode = gateway.StatusAgentSession
	nativeIntake := intake(s)
	nativeIntake.StatusMode = gateway.StatusAgentSession
	accepted, err := nativeIntake.Accept(context.Background(), m)
	storetest.Must(t, err)
	if !accepted.Accepted {
		t.Fatal("native intake rejected")
	}
	calls := 0
	allow := true
	p := statusPolicyFunc(func(_ context.Context, d gateway.DeliveryContext, fresh bool) error {
		calls++
		if !fresh || d.Message.AuthorID != "alice" {
			t.Fatal("wrong identity or cached authorization")
		}
		if !allow {
			return gateway.ErrDenied
		}
		return nil
	})
	sender := &statusSlack{}
	w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: p, Now: func() time.Time { return now }}
	statusStep(t, w, "urgent", true)
	allow = false
	now = now.Add(gateway.AgentSessionRefresh)
	statusStep(t, w, "heartbeat", true)
	if calls != 2 || len(sender.calls) != 1 {
		t.Fatal("revoked authorization bypassed")
	}
	state, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if state.AgentSessions[0].Reason != "policy_denied" {
		t.Fatal("denial not persisted")
	}
}

func TestHybridTargetFailuresSurviveRestartWithoutBlockingOtherTargetOrAnswer(t *testing.T) {
	for _, target := range []gateway.StatusTarget{gateway.StatusTargetMessage, gateway.StatusTargetAgentSession} {
		for _, kind := range []gateway.SendErrorKind{gateway.SendUnknown, gateway.SendRejected, gateway.SendRateLimited} {
			t.Run(string(target)+"/"+string(kind), func(t *testing.T) {
				ctx := context.Background()
				s, reopen := database(t)
				now := time.Now()
				if kind == gateway.SendRateLimited {
					// Delivery uses the wall clock. Keep its persisted cooldown in
					// the past while exercising status retry with a controlled clock.
					now = now.Add(-2 * time.Minute)
				}
				i := intake(s)
				i.StatusMode = gateway.StatusHybrid
				accepted, err := i.Accept(ctx, storetest.Message("a", "one", "alice"))
				storetest.Must(t, err)
				if !accepted.Accepted {
					t.Fatal("hybrid intake rejected")
				}
				failed := false
				sender := &statusSlack{}
				sender.fail = func(int) error {
					call := sender.calls[len(sender.calls)-1]
					if call.Status.Target == target && !failed {
						failed = true
						return &gateway.SendError{Kind: kind, Code: "feature_disabled", RetryAfter: 37 * time.Second}
					}
					return nil
				}
				w := &gateway.StatusWorker{Store: s, Sender: sender, Policy: policy(), Now: func() time.Time { return now }}
				statusStep(t, w, "urgent", true)
				if kind == gateway.SendRateLimited && target == gateway.StatusTargetAgentSession {
					statusStep(t, w, "urgent", false) // Shared workspace cooldown.
					now = now.Add(38 * time.Second)
				}
				statusStep(t, w, "urgent", true)
				s = reopen()
				w.Store = s
				now = now.Add(time.Minute)
				client := &fakeAgently{}
				work(t, gateway.Worker{Store: s, Policy: policy(), Clients: client, AgentID: "steward"})
				d := &gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy()}
				worked, err := d.Step(ctx)
				storetest.Must(t, err)
				if !worked {
					t.Fatal("status failure blocked final delivery")
				}
				for range 10 {
					worked, err = w.Step(ctx, "urgent")
					storetest.Must(t, err)
					if !worked {
						break
					}
				}
				created, queries := client.snapshot()
				if len(created) != 1 || len(queries) != 1 {
					t.Fatal("status targets duplicated Agently execution")
				}
				counts := map[gateway.StatusTarget]int{}
				for _, call := range sender.calls {
					counts[call.Status.Target]++
					if call.Status.Mode != gateway.StatusHybrid {
						t.Fatal("presentation mode lost")
					}
					if call.Status.Target == gateway.StatusTargetAgentSession && (call.Text != "" || call.Status.SlackTS != "") {
						t.Fatal("native publication used a message payload")
					}
					if call.Status.Target == gateway.StatusTargetMessage && (!strings.Contains(call.Text, "Elapsed:") || strings.Contains(call.Text, "Request ")) {
						t.Fatal("detailed message has missing elapsed time or a technical header")
					}
				}
				if counts[gateway.StatusTargetMessage] == 0 || counts[gateway.StatusTargetAgentSession] == 0 {
					t.Fatal("failure suppressed the other target")
				}
				if kind == gateway.SendRejected || (target == gateway.StatusTargetMessage && kind == gateway.SendUnknown) {
					if counts[target] != 1 {
						t.Fatal("permanent rejection or ambiguous first message retried")
					}
				}
				if worked, err = d.Step(ctx); err != nil || worked {
					t.Fatal("answer delivered twice")
				}
			})
		}
	}
}
