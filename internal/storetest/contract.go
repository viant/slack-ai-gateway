// Package storetest contains reusable contract tests, not production adapters.
// Stage E must run this same suite against a real MySQL implementation.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

type Fixture struct {
	Store gateway.Store
	// Reopen closes/reopens the same durable database and runs recovery.
	Reopen func() gateway.Store
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

func claim(t *testing.T, s gateway.Store) *gateway.Job {
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

func ready(t *testing.T, s gateway.Store, j *gateway.Job) *gateway.Binding {
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
	ctx := context.Background()
	t.Run("workspace_cooldown_is_durable_and_covers_new_threads", func(t *testing.T) {
		f := open(t)
		s := f.Store
		queue := func(m gateway.Message) {
			enqueue(t, s, m)
			j := claim(t, s)
			Must(t, s.Complete(ctx, j.ID, "", "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
		}
		queue(Message("a", "one", "alice"))
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
		queue(Message("b", "two", "bob"))
		if next, err := s.ClaimDelivery(ctx); err != nil || next != nil {
			t.Fatal("new thread bypassed workspace cooldown")
		}
		other := Message("c", "three", "carol")
		other.WorkspaceID = "OTHER"
		queue(other)
		next, err := s.ClaimDelivery(ctx)
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
	t.Run("dedup_preserves_original_and_scope", func(t *testing.T) {
		s := open(t).Store
		m := Message("event1", "1.000001", "alice")
		enqueue(t, s, m)
		duplicate := m
		duplicate.AuthorID = "mallory"
		duplicate.Text = "changed"
		inserted, err := s.Enqueue(ctx, duplicate)
		Must(t, err)
		if inserted {
			t.Fatal("duplicate inserted")
		}
		j := claim(t, s)
		if j.AuthorID != "alice" || j.Text != m.Text || j.RequestID == "" {
			t.Fatal("original event changed")
		}
		m.WorkspaceID = "OTHER"
		enqueue(t, s, m)
		other := claim(t, s)
		if other.WorkspaceID != "OTHER" {
			t.Fatal("dedup scope is too broad")
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
		Must(t, s.Complete(ctx, a.ID, a.RequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
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
		if again.ID != a.ID || again.RequestID != a.RequestID {
			t.Fatal("lost durable correlation")
		}
		b := ready(t, s, again)
		Must(t, s.BeginQuery(ctx, again.ID))
		Must(t, s.Complete(ctx, again.ID, again.RequestID, "answer", []gateway.ReplyPart{{Text: "part one", Format: gateway.ReplyPlain}, {Text: "part two", Format: gateway.ReplyPlain}}))
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
		if next.ID != a.ID || next.TurnID != a.RequestID || next.QueryStartedAt.IsZero() {
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
		if poll.TurnID != j.RequestID || poll.PollAttempts != 1 || poll.QueryStartedAt.IsZero() {
			t.Fatal("poll metadata missing")
		}
		noJob(t, s)
		s = g.Reopen() // Crash during a read: recover another read, not a query.
		recovered := claim(t, s)
		if !recovered.QueryStartedAt.Equal(poll.QueryStartedAt) || recovered.PollAttempts != 1 {
			t.Fatal("poll deadline reset on restart")
		}
		if err := s.Complete(ctx, j.ID, "other-turn", "wrong", []gateway.ReplyPart{{Text: "wrong", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("wrong polled turn accepted")
		}
		Must(t, s.Complete(ctx, j.ID, j.RequestID, "right", []gateway.ReplyPart{{Text: "right", Format: gateway.ReplyPlain}}))
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
		Must(t, s.Complete(ctx, j.ID, j.RequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
		if err := s.Complete(ctx, j.ID, j.RequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("duplicate completion accepted")
		}
		if err := s.Reject(ctx, j.ID, "denied"); !errors.Is(err, gateway.ErrConflict) {
			t.Fatal("terminal state changed")
		}
	})
}
