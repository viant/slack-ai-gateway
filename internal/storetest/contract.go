// Package storetest contains reusable contract tests, not production adapters.
// SQLite and MySQL run the same behavioral contract.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

type Fixture struct {
	Store gateway.Store
	// Root returns the current lifecycle owner for explicit integration setup.
	Root func() storage.Store
	// Reopen closes/reopens the same durable database and runs recovery.
	Reopen func() gateway.Store
}

// Route builds a complete local channel rule for native storage/runtime tests.
func Route(app, workspace string, channels ...string) gateway.Route {
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Enabled: true, Scope: "selected", Allowlist: channels},
		gateway.AccessRules{Users: &gateway.UserRule{Scope: "workspace_members_and_guests"}})
	if err != nil {
		panic(err)
	}
	return gateway.Route{AppID: app, WorkspaceID: workspace, Resolver: r}
}

// Policy exercises the production authorization path with synthetic Slack facts.
func Policy(app, workspace, principal string, channels ...string) gateway.ConfiguredPolicy {
	return gateway.ConfiguredPolicy{Route: Route(app, workspace, channels...), PrincipalID: principal, Directory: directory{}}
}

type directory struct{}

func (directory) Channel(_ context.Context, workspace, id string, _ bool) (gateway.ChannelInfo, error) {
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: id, Member: true}, nil
}

func (directory) User(_ context.Context, workspace, id string) (gateway.UserInfo, error) {
	return gateway.UserInfo{WorkspaceID: workspace, ID: id}, nil
}

func Message(event, root, author string) gateway.Message {
	return gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: root}, AppID: "A", EventID: event, AuthorID: author, MessageTS: event + ".000001", Text: "diagnose " + event, Mentioned: true}
}

func Must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func enqueue(t *testing.T, s gateway.Store, m gateway.Message) {
	t.Helper()
	inserted, err := s.Enqueue(context.Background(), m)
	Must(t, err)
	if !inserted {
		t.Fatal("expected new event")
	}
}

func claim(t *testing.T, s gateway.Store) *gateway.GatewayJob {
	t.Helper()
	j, err := s.ClaimJob(context.Background())
	Must(t, err)
	if j == nil {
		t.Fatal("expected a claimed job")
	}
	return j
}

func noJob(t *testing.T, s gateway.Store) {
	t.Helper()
	j, err := s.ClaimJob(context.Background())
	Must(t, err)
	if j != nil {
		t.Fatalf("unexpected job %d", j.ID)
	}
}

func decision() gateway.Decision {
	return gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"}
}

func ready(t *testing.T, s gateway.Store, j *gateway.GatewayJob) *gateway.ConversationBinding {
	t.Helper()
	ctx := context.Background()
	b, err := s.BindJob(ctx, j.ID, decision())
	Must(t, err)
	if b.ConversationID == "" {
		Must(t, s.BeginCreate(ctx, b.ID))
		Must(t, s.SetConversation(ctx, b.ID, fmt.Sprintf("conversation-%d", b.ID)))
	}
	return b
}

func Run(t *testing.T, open func(*testing.T) Fixture) {
	runBackendOutage(t, open)
	ctx := context.Background()
	t.Run("integrations_isolate_independent_events_threads_and_deliveries", func(t *testing.T) {
		f := open(t)
		first := f.Store
		identity := DefaultIntegration()
		identity.Name, identity.AppID = "second", "A2"
		_, err := f.Root().ReconcileIntegrations(ctx, []storage.Integration{DefaultIntegration(), identity})
		Must(t, err)
		second := Scope(t, f.Root(), "second")
		m := Message("same-event", "same-thread", "alice")
		enqueue(t, first, m)
		other := m
		other.AppID, other.EventID = "A2", "second-event"
		enqueue(t, second, other)
		a, b := claim(t, first), claim(t, second)
		if a.ID == b.ID || a.AppID != "A" || b.AppID != "A2" {
			t.Fatalf("installation intake identities mixed: first=%+v second=%+v", a, b)
		}
		ready(t, first, a)
		ready(t, second, b)
		// BindJob returns its pre-create snapshot. Read the persisted backend
		// links instead of comparing two intentionally empty local snapshots.
		left, err := first.GetBinding(ctx, a.ThreadKey)
		Must(t, err)
		right, err := second.GetBinding(ctx, b.ThreadKey)
		Must(t, err)
		if left == nil || right == nil || left.ID == right.ID || left.ConversationID == "" || right.ConversationID == "" || left.ConversationID == right.ConversationID {
			t.Fatal("equal Slack thread keys shared a backend conversation across installations")
		}
		if err := first.BeginQuery(ctx, b.ID); !errors.Is(err, gateway.ErrConflict) {
			t.Fatalf("foreign integration job mutation was not rejected: %v", err)
		}
		for _, item := range []struct {
			store gateway.Store
			job   *gateway.GatewayJob
			text  string
		}{{first, a, "first reply"}, {second, b, "second reply"}} {
			Must(t, item.store.BeginQuery(ctx, item.job.ID))
			Must(t, item.store.Complete(ctx, item.job.ID, item.job.GatewayRequestID, item.text, []gateway.ReplyPart{{Text: item.text, Format: gateway.ReplyPlain}}))
			delivery, err := item.store.ClaimDelivery(ctx)
			Must(t, err)
			if delivery == nil || delivery.JobID != item.job.ID || delivery.Text != item.text {
				t.Fatalf("outbound integration isolation lost: %+v", delivery)
			}
			original, err := item.store.GetDeliveryContext(ctx, *delivery)
			Must(t, err)
			if original.Message.AppID != item.job.AppID {
				t.Fatal("delivery reconstructed another installation's Slack identity")
			}
		}
	})
	t.Run("changed_target_gets_new_assignment_and_closes_old_work", func(t *testing.T) {
		f := open(t)
		enqueue(t, f.Store, Message("old-event", "old-thread", "alice"))
		old := claim(t, f.Store)
		ready(t, f.Store, old)
		identity := DefaultIntegration()
		identity.BackendURL = "https://other-backend.example.test"
		rows, err := f.Root().ReconcileIntegrations(ctx, []storage.Integration{identity})
		Must(t, err)
		if len(rows) != 1 || !rows[0].Active {
			t.Fatal("changed configuration not activated")
		}
		history, err := f.Root().ScopeName("default").Status(ctx, 100)
		Must(t, err)
		if len(history.Integrations) != 2 {
			t.Fatal("prior assignment history lost")
		}
		active, inactive := 0, 0
		for _, row := range history.Integrations {
			if row.Active {
				active++
			} else {
				inactive++
			}
		}
		if active != 1 || inactive != 1 {
			t.Fatal("assignment activation not reconciled")
		}
		for _, job := range history.GatewayJobs {
			if job.ID == old.ID && (job.State != "rejected" || job.Reason != "integration_changed") {
				t.Fatal("old work was not terminally closed")
			}
		}
		current := f.Root().Scope(rows[0].ID)
		enqueue(t, current, Message("new-event", "new-thread", "alice"))
		if job := claim(t, current); job.EventID != "new-event" {
			t.Fatal("new assignment did not accept fresh work")
		}
	})
	t.Run("workspace_cooldown_is_durable_and_covers_new_threads", func(t *testing.T) {
		f := open(t)
		s := f.Store
		queue := func(target gateway.Store, m gateway.Message) {
			enqueue(t, target, m)
			j := claim(t, target)
			Must(t, target.Complete(ctx, j.ID, "", "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
		}
		queue(s, Message("a", "one", "alice"))
		d, err := s.ClaimDelivery(ctx)
		Must(t, err)
		until := time.Now().Add(time.Hour).Truncate(time.Millisecond)
		Must(t, s.RetryDelivery(ctx, d.ID, until))
		if err := s.FailDelivery(ctx, d.ID); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("pending delivery marked failed")
		}
		s = f.Reopen()
		got, err := s.DeliveryNotBefore(ctx, "T")
		Must(t, err)
		if !got.Equal(until) {
			t.Fatal("cooldown lost on restart")
		}
		queue(s, Message("b", "two", "bob"))
		if next, err := s.ClaimDelivery(ctx); err != nil || next != nil {
			t.Fatal("new thread bypassed workspace cooldown")
		}
		other := Message("c", "three", "carol")
		other.AppID, other.WorkspaceID = "A_OTHER", "OTHER"
		identity := DefaultIntegration()
		identity.Name, identity.AppID, identity.WorkspaceID = "other", "A_OTHER", "OTHER"
		_, err = f.Root().ReconcileIntegrations(ctx, []storage.Integration{DefaultIntegration(), identity})
		Must(t, err)
		otherStore := Scope(t, f.Root(), "other")
		queue(otherStore, other)
		next, err := otherStore.ClaimDelivery(ctx)
		Must(t, err)
		if next == nil || next.WorkspaceID != "OTHER" {
			t.Fatal("unrelated workspace blocked")
		}
	})
	t.Run("retry_preserves_part_and_failed_blocks_only_its_thread", func(t *testing.T) {
		s := open(t).Store
		for _, m := range []gateway.Message{Message("a", "one", "alice"), Message("b", "two", "bob")} {
			enqueue(t, s, m)
			j := claim(t, s)
			Must(t, s.Complete(ctx, j.ID, "", "result", []gateway.ReplyPart{{Text: "first", Format: gateway.ReplyPlain}, {Text: "second", Format: gateway.ReplyPlain}}))
		}
		d, err := s.ClaimDelivery(ctx)
		Must(t, err)
		Must(t, s.RetryDelivery(ctx, d.ID, time.Now().Add(-time.Second)))
		again, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if again.ID != d.ID || again.Text != d.Text || again.Attempts != 2 {
			t.Fatal("retry did not preserve persisted part")
		}
		Must(t, s.FailDelivery(ctx, d.ID))
		next, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if next == nil || next.RootTS != "two" {
			t.Fatal("failed part bypassed, or other thread blocked")
		}
	})
	t.Run("global_dedup_preserves_original_and_isolates_execution", func(t *testing.T) {
		f := open(t)
		s := f.Store
		identity := DefaultIntegration()
		identity.Name, identity.AppID, identity.WorkspaceID = "other", "A_OTHER", "OTHER"
		_, err := f.Root().ReconcileIntegrations(ctx, []storage.Integration{DefaultIntegration(), identity})
		Must(t, err)
		otherStore := Scope(t, f.Root(), "other")
		m := Message("event1", "1.000001", "alice")
		enqueue(t, s, m)
		duplicate := m
		duplicate.AuthorID, duplicate.Text = "mallory", "changed"
		inserted, err := s.Enqueue(ctx, duplicate)
		Must(t, err)
		if inserted {
			t.Fatal("duplicate inserted")
		}
		j := claim(t, s)
		if j.AuthorID != "alice" || j.Text != m.Text || j.GatewayRequestID == "" {
			t.Fatal("original event changed")
		}
		m.AppID, m.WorkspaceID = "A_OTHER", "OTHER"
		inserted, err = otherStore.Enqueue(ctx, m)
		Must(t, err)
		if inserted {
			t.Fatal("same global event was accepted by another assignment")
		}
		noJob(t, otherStore)
		m.EventID = "independent-event"
		enqueue(t, otherStore, m)
		other := claim(t, otherStore)
		if other.WorkspaceID != "OTHER" {
			t.Fatal("independent event used the wrong workspace")
		}
		noJob(t, s)
	})
	t.Run("per_thread_order_and_independent_progress", func(t *testing.T) {
		s := open(t).Store
		enqueue(t, s, Message("a", "one", "alice"))
		enqueue(t, s, Message("b", "one", "bob"))
		enqueue(t, s, Message("c", "two", "carol"))
		a := claim(t, s)
		c := claim(t, s)
		if a.EventID != "a" || c.EventID != "c" {
			t.Fatal("thread ordering violated")
		}
		noJob(t, s)
		Must(t, s.Complete(ctx, a.ID, "", "a", []gateway.ReplyPart{{Text: "a", Format: gateway.ReplyPlain}}))
		b := claim(t, s)
		if b.EventID != "b" {
			t.Fatal("next thread message not released")
		}
		Must(t, s.Review(ctx, b.ID, "test_unknown"))
		enqueue(t, s, Message("d", "one", "alice"))
		noJob(t, s)
	})
	t.Run("binding_identity_immutable", func(t *testing.T) {
		s := open(t).Store
		m := Message("a", "one", "alice")
		enqueue(t, s, m)
		a := claim(t, s)
		b := ready(t, s, a)
		Must(t, s.BeginQuery(ctx, a.ID))
		Must(t, s.Complete(ctx, a.ID, a.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
		enqueue(t, s, Message("b", "one", "bob"))
		next := claim(t, s)
		bad := decision()
		bad.PrincipalID = "bob"
		if _, err := s.BindJob(ctx, next.ID, bad); !errors.Is(err, gateway.ErrConflict) {
			t.Fatalf("expected identity conflict, got %v", err)
		}
		again, err := s.BindJob(ctx, next.ID, decision())
		Must(t, err)
		if again.ID != b.ID || again.OwnerID != "shared" || again.ConversationID == "" {
			t.Fatal("binding mutated")
		}
	})
	t.Run("restart_preserves_request_binding_and_outbox", func(t *testing.T) {
		f := open(t)
		s := f.Store
		m := Message("a", "one", "alice")
		enqueue(t, s, m)
		a := claim(t, s)
		// Claim alone has no remote side effect and is safe to reclaim.
		s = f.Reopen()
		again := claim(t, s)
		if again.ID != a.ID || again.GatewayRequestID != a.GatewayRequestID {
			t.Fatal("lost durable correlation")
		}
		b := ready(t, s, again)
		Must(t, s.BeginQuery(ctx, again.ID))
		Must(t, s.Complete(ctx, again.ID, again.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "part one", Format: gateway.ReplyPlain}, {Text: "part two", Format: gateway.ReplyPlain}}))
		s = f.Reopen()
		noJob(t, s)
		binding, err := s.GetBinding(ctx, m.ThreadKey)
		Must(t, err)
		if binding == nil || binding.ID != b.ID || binding.ConversationID == "" {
			t.Fatal("binding lost on restart")
		}
		d, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if d == nil || d.Text != "part one" || d.Part != 0 {
			t.Fatal("first persisted part missing")
		}
		next, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if next != nil {
			t.Fatal("second part overtook first")
		}
		Must(t, s.Delivered(ctx, d.ID, "100.001"))
		s = f.Reopen()
		d, err = s.ClaimDelivery(ctx)
		Must(t, err)
		if d == nil || d.Part != 1 || d.Text != "part two" {
			t.Fatal("second part missing")
		}
		Must(t, s.Delivered(ctx, d.ID, "100.002"))
		inserted, err := s.Enqueue(ctx, m)
		Must(t, err)
		if inserted {
			t.Fatal("dedup lost on restart")
		}
		noJob(t, s)
		d, err = s.ClaimDelivery(ctx)
		Must(t, err)
		if d != nil {
			t.Fatal("sent response repeated")
		}
	})
	t.Run("interrupted_query_is_not_replayed", func(t *testing.T) {
		f := open(t)
		s := f.Store
		enqueue(t, s, Message("a", "one", "alice"))
		a := claim(t, s)
		ready(t, s, a)
		Must(t, s.BeginQuery(ctx, a.ID))
		enqueue(t, s, Message("b", "one", "bob"))
		enqueue(t, s, Message("c", "two", "carol"))
		s = f.Reopen()
		next := claim(t, s)
		if next.ID != a.ID || next.GatewayRequestID != a.GatewayRequestID || next.BackendSubmissionStartedAt.IsZero() {
			t.Fatal("query not recovered for exact-turn read")
		}
		if err := s.BeginQuery(ctx, next.ID); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("recovered query could be submitted again")
		}
		next = claim(t, s)
		if next.EventID != "c" {
			t.Fatal("ambiguous query or its successor was replayed")
		}
		noJob(t, s)
	})
	t.Run("poll_claim_is_exclusive_and_preserves_deadline", func(t *testing.T) {
		f := open(t)
		s := f.Store
		enqueue(t, s, Message("a", "one", "alice"))
		j := claim(t, s)
		ready(t, s, j)
		Must(t, s.BeginQuery(ctx, j.ID))
		noJob(t, s) // A query currently on the network must not be polled by a second worker.
		Must(t, s.SchedulePoll(ctx, j.ID, time.Now().Add(time.Hour)))
		noJob(t, s)
		s = f.Reopen()
		noJob(t, s) // Persisted not-before is honored after restart.
		// Another fixture tests a due poll, without sleeping an hour.
		g := open(t)
		s = g.Store
		enqueue(t, s, Message("a", "one", "alice"))
		j = claim(t, s)
		ready(t, s, j)
		Must(t, s.BeginQuery(ctx, j.ID))
		Must(t, s.SchedulePoll(ctx, j.ID, time.Now().Add(-time.Second)))
		poll := claim(t, s)
		if poll.GatewayRequestID != j.GatewayRequestID || poll.PollAttempts != 1 || poll.BackendSubmissionStartedAt.IsZero() {
			t.Fatal("poll metadata missing")
		}
		noJob(t, s)
		s = g.Reopen() // Crash during a read: recover another read, not a query.
		recovered := claim(t, s)
		if !recovered.BackendSubmissionStartedAt.Equal(poll.BackendSubmissionStartedAt) || recovered.PollAttempts != 1 {
			t.Fatal("poll deadline reset on restart")
		}
		if err := s.Complete(ctx, j.ID, "other-turn", "wrong", []gateway.ReplyPart{{Text: "wrong", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("wrong polled turn accepted")
		}
		Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "right", []gateway.ReplyPart{{Text: "right", Format: gateway.ReplyPlain}}))
	})
	t.Run("interrupted_creation_is_not_repeated", func(t *testing.T) {
		f := open(t)
		s := f.Store
		m := Message("a", "one", "alice")
		enqueue(t, s, m)
		j := claim(t, s)
		b, err := s.BindJob(ctx, j.ID, decision())
		Must(t, err)
		Must(t, s.BeginCreate(ctx, b.ID))
		s = f.Reopen()
		noJob(t, s)
		b, err = s.GetBinding(ctx, m.ThreadKey)
		Must(t, err)
		if b.State != "needs_review" {
			t.Fatal("ambiguous creation not quarantined")
		}
	})
	t.Run("interrupted_delivery_blocks_only_its_thread", func(t *testing.T) {
		f := open(t)
		s := f.Store
		enqueue(t, s, Message("a", "one", "alice"))
		a := claim(t, s)
		Must(t, s.Complete(ctx, a.ID, "", "a", []gateway.ReplyPart{{Text: "a1", Format: gateway.ReplyPlain}, {Text: "a2", Format: gateway.ReplyPlain}}))
		enqueue(t, s, Message("b", "two", "bob"))
		b := claim(t, s)
		Must(t, s.Complete(ctx, b.ID, "", "b", []gateway.ReplyPart{{Text: "b", Format: gateway.ReplyPlain}}))
		d, err := s.ClaimDelivery(ctx)
		Must(t, err)
		if d == nil || d.JobID != a.ID {
			t.Fatal("wrong first delivery")
		}
		s = f.Reopen()
		d, err = s.ClaimDelivery(ctx)
		Must(t, err)
		if d == nil || d.JobID != b.ID {
			t.Fatal("unknown send replayed or unrelated delivery blocked")
		}
		Must(t, s.Delivered(ctx, d.ID, "200.001"))
		d, err = s.ClaimDelivery(ctx)
		Must(t, err)
		if d != nil {
			t.Fatal("later part overtook ambiguous delivery")
		}
	})
	t.Run("concurrent_duplicates_and_claims", func(t *testing.T) {
		s := open(t).Store
		m := Message("a", "one", "alice")
		var inserted atomic.Int32
		var wg sync.WaitGroup
		for range 24 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok, err := s.Enqueue(ctx, m)
				if err != nil {
					t.Error(err)
				}
				if ok {
					inserted.Add(1)
				}
			}()
		}
		wg.Wait()
		if inserted.Load() != 1 {
			t.Fatal("concurrent dedup failed")
		}
		var claimed atomic.Int32
		for range 24 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				j, err := s.ClaimJob(ctx)
				if err != nil {
					t.Error(err)
				}
				if j != nil {
					claimed.Add(1)
				}
			}()
		}
		wg.Wait()
		if claimed.Load() != 1 {
			t.Fatal("concurrent claim failed")
		}
	})
	t.Run("state_transitions_guarded", func(t *testing.T) {
		s := open(t).Store
		enqueue(t, s, Message("a", "one", "alice"))
		j := claim(t, s)
		if err := s.BeginQuery(ctx, j.ID); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("query without conversation accepted")
		}
		ready(t, s, j)
		Must(t, s.BeginQuery(ctx, j.ID))
		if err := s.Complete(ctx, j.ID, "wrong-turn", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("wrong turn accepted")
		}
		Must(t, s.Complete(ctx, j.ID, j.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
		if err := s.Complete(ctx, j.ID, j.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("duplicate completion accepted")
		}
		if err := s.Reject(ctx, j.ID, "denied"); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("terminal state changed")
		}
	})
}
