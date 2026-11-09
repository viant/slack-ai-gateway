package gateway_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storage/sqlite"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func database(t *testing.T) (*sqlite.Store, func() *sqlite.Store) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "gateway.sqlite")
	s, err := sqlite.Open(ctx, path, true)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	t.Cleanup(func() {
		if s != nil {
			storetest.Must(t, s.Close())
		}
	})
	return s, func() *sqlite.Store {
		storetest.Must(t, s.Close())
		s = nil
		var err error
		s, err = sqlite.Open(ctx, path, false)
		storetest.Must(t, err)
		storetest.Must(t, s.CheckSchema(ctx))
		storetest.Must(t, s.Recover(ctx))
		return s
	}
}

func policy() gateway.SharedChannelPolicy {
	return gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "shared"}
}

func intake(s gateway.Store) gateway.Intake {
	return gateway.Intake{Store: s, Route: gateway.Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"}}
}

func accept(t *testing.T, s gateway.Store, m gateway.Message) gateway.AcceptResult {
	t.Helper()
	r, err := intake(s).Accept(context.Background(), m)
	storetest.Must(t, err)
	if !r.Accepted {
		t.Fatal("expected accepted event")
	}
	return r
}

func work(t *testing.T, w gateway.Worker) {
	t.Helper()
	ok, err := w.Step(context.Background())
	storetest.Must(t, err)
	if !ok {
		t.Fatal("expected work")
	}
}

func TestOfflineConversationAcrossRestarts(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	alice := storetest.Message("a", "1.000001", "alice")
	bob := storetest.Message("b", "1.000001", "bob")
	carol := storetest.Message("c", "2.000001", "carol")
	for _, m := range []gateway.Message{alice, bob, carol} {
		accept(t, s, m)
	}
	if !accept(t, s, alice).Duplicate {
		t.Fatal("duplicate not acknowledged")
	}
	s = reopen() // ACK has happened; no worker or network call has run yet.
	backend := &fakeAgently{}
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
	for range 3 {
		work(t, w)
	}
	created, queries := backend.snapshot()
	if len(created) != 2 || len(queries) != 3 {
		t.Fatalf("create=%d query=%d", len(created), len(queries))
	}
	if queries[0].Query.ConversationID != queries[1].Query.ConversationID || queries[0].Query.ConversationID == queries[2].Query.ConversationID {
		t.Fatal("thread/conversation mapping incorrect")
	}
	for _, q := range queries {
		if q.Principal != "shared" || q.Query.MessageID == "" || q.Query.AgentID != "steward" {
			t.Fatal("wrong identity or query correlation")
		}
	}
	for _, p := range created {
		if p != "shared" {
			t.Fatal("conversation created under Slack author")
		}
	}
	s = reopen() // Results exist, Slack delivery has not started.
	sender := &fakeSlack{}
	delivery := gateway.DeliveryWorker{Store: s, Sender: sender}
	for range 3 {
		ok, err := delivery.Step(ctx)
		storetest.Must(t, err)
		if !ok {
			t.Fatal("missing persisted reply")
		}
	}
	msgs := sender.snapshot()
	if len(msgs) != 3 || msgs[0].RootTS != alice.RootTS || msgs[1].RootTS != alice.RootTS || msgs[2].RootTS != carol.RootTS {
		t.Fatal("incorrect destination threads")
	}
	s = reopen()
	w.Store = s
	delivery.Store = s
	if !accept(t, s, bob).Duplicate {
		t.Fatal("dedup not durable")
	}
	if ok, err := w.Step(ctx); err != nil || ok {
		t.Fatal("duplicate triggered query")
	}
	if ok, err := delivery.Step(ctx); err != nil || ok {
		t.Fatal("duplicate triggered reply")
	}
	_, queries = backend.snapshot()
	if len(queries) != 3 {
		t.Fatal("query replayed")
	}
}

func TestIntakeFilteringAndNoBindingForHelp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*gateway.Message)
	}{
		{"no_mention", func(m *gateway.Message) { m.Mentioned = false }},
		{"bot", func(m *gateway.Message) { m.Bot = true }},
		{"edited", func(m *gateway.Message) { m.Edited = true }},
		{"deleted", func(m *gateway.Message) { m.Deleted = true }},
		{"external_channel", func(m *gateway.Message) { m.ExternalShared = true }},
		{"foreign_app", func(m *gateway.Message) { m.AppID = "OTHER" }},
		{"foreign_workspace", func(m *gateway.Message) { m.WorkspaceID = "OTHER" }},
		{"foreign_channel", func(m *gateway.Message) { m.ChannelID = "OTHER" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := database(t)
			m := storetest.Message("a", "one", "alice")
			tc.change(&m)
			r, err := intake(s).Accept(context.Background(), m)
			storetest.Must(t, err)
			if !r.Ignored || r.Accepted {
				t.Fatal("event not ignored")
			}
			j, err := s.ClaimJob(context.Background())
			storetest.Must(t, err)
			if j != nil {
				t.Fatal("ignored event persisted")
			}
		})
	}
	t.Run("empty_with_attachment", func(t *testing.T) {
		ctx := context.Background()
		s, _ := database(t)
		m := storetest.Message("a", "one", "alice")
		m.Text = "  "
		m.HasAttachments = true
		accept(t, s, m)
		backend := &fakeAgently{}
		work(t, gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"})
		created, queries := backend.snapshot()
		if len(created) != 0 || len(queries) != 0 {
			t.Fatal("empty text created a conversation/query")
		}
		b, err := s.GetBinding(ctx, m.ThreadKey)
		storetest.Must(t, err)
		if b != nil {
			t.Fatal("help created a binding")
		}
		d, err := s.ClaimDelivery(ctx)
		storetest.Must(t, err)
		if d == nil || !strings.Contains(d.Text, "Attachments") {
			t.Fatal("attachment limitation missing")
		}
	})
}

func TestSharedPolicyAndConfigurationChange(t *testing.T) {
	ctx := context.Background()
	p := policy()
	m := storetest.Message("a", "one", "alice")
	d, err := p.Authorize(ctx, m, nil)
	storetest.Must(t, err)
	b := &gateway.Binding{ThreadKey: m.ThreadKey, Decision: d, ConversationID: "existing"}
	m.AuthorID = "bob"
	again, err := p.Authorize(ctx, m, b)
	storetest.Must(t, err)
	if again != d {
		t.Fatal("author changed execution identity")
	}
	p.PrincipalID = "different"
	if _, err = p.Authorize(ctx, m, b); !errors.Is(err, gateway.ErrDenied) {
		t.Fatal("owner change accepted")
	}
	p = gateway.SharedChannelPolicy{}
	if _, err = p.Authorize(ctx, m, nil); !errors.Is(err, gateway.ErrDenied) {
		t.Fatal("empty policy accepted")
	}
	s, _ := database(t)
	backend := &fakeAgently{}
	accept(t, s, storetest.Message("a", "one", "alice"))
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
	work(t, w)
	accept(t, s, storetest.Message("b", "one", "bob"))
	p = policy()
	p.PrincipalID = "changed"
	w.Policy = p
	work(t, w)
	_, queries := backend.snapshot()
	if len(queries) != 1 {
		t.Fatal("changed account used for existing conversation")
	}
}

func TestAmbiguousExternalOperationsAreNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name             string
		backend          *fakeAgently
		creates, queries int
	}{
		{"credentials", &fakeAgently{credentialsErr: errors.New("unavailable")}, 0, 0},
		{"creation", &fakeAgently{createErr: errors.New("timeout")}, 1, 0},
		{"query", &fakeAgently{queryErr: errors.New("timeout")}, 1, 1},
		{"queued", &fakeAgently{state: "queued"}, 1, 1},
		{"waiting", &fakeAgently{state: "waiting_for_user"}, 1, 1},
		{"wrong_turn", &fakeAgently{wrongTurn: true}, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, reopen := database(t)
			accept(t, s, storetest.Message("a", "one", "alice"))
			accept(t, s, storetest.Message("b", "one", "bob"))
			w := gateway.Worker{Store: s, Policy: policy(), Clients: tc.backend, AgentID: "steward"}
			work(t, w)
			s = reopen()
			w.Store = s
			if ok, err := w.Step(context.Background()); err != nil || ok {
				t.Fatal("unresolved operation was replayed")
			}
			created, queries := tc.backend.snapshot()
			if len(created) != tc.creates || len(queries) != tc.queries {
				t.Fatal("unexpected backend calls")
			}
		})
	}
}

func TestFailedDeliveryDoesNotRepeatQuery(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	backend := &fakeAgently{}
	accept(t, s, storetest.Message("a", "one", "alice"))
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
	work(t, w)
	sender := &fakeSlack{err: errors.New("unknown result")}
	delivery := gateway.DeliveryWorker{Store: s, Sender: sender}
	if ok, err := delivery.Step(ctx); !ok || err == nil {
		t.Fatal("expected ambiguous send")
	}
	s = reopen()
	w.Store = s
	delivery.Store = s
	if ok, err := w.Step(ctx); err != nil || ok {
		t.Fatal("delivery failure replayed query")
	}
	if ok, err := delivery.Step(ctx); err != nil || ok {
		t.Fatal("unknown send blindly repeated")
	}
	_, queries := backend.snapshot()
	if len(queries) != 1 || len(sender.snapshot()) != 1 {
		t.Fatal("side effects repeated")
	}
}

type alternatePolicy struct{}

func (alternatePolicy) Authorize(_ context.Context, m gateway.Message, b *gateway.Binding) (gateway.Decision, error) {
	d := gateway.Decision{Mode: "test_policy", PrincipalID: "test-execution", OwnerID: "test-owner"}
	if b != nil && b.Decision != d {
		return gateway.Decision{}, gateway.ErrDenied
	}
	return d, nil
}

func TestAlternativePolicyDoesNotRequireWorkerChanges(t *testing.T) {
	s, _ := database(t)
	m := storetest.Message("a", "one", "alice")
	accept(t, s, m)
	backend := &fakeAgently{}
	work(t, gateway.Worker{Store: s, Policy: alternatePolicy{}, Clients: backend, AgentID: "test-agent"})
	b, err := s.GetBinding(context.Background(), m.ThreadKey)
	storetest.Must(t, err)
	if b.Mode != "test_policy" || b.OwnerID != "test-owner" || b.PrincipalID != "test-execution" {
		t.Fatal("worker assumes shared identity")
	}
}

func TestWorkersRunDifferentThreadsConcurrently(t *testing.T) {
	s, _ := database(t)
	release := make(chan struct{})
	backend := &fakeAgently{started: make(chan gateway.Query, 4), release: release}
	for i := range 4 {
		root := fmt.Sprintf("thread-%d", i)
		accept(t, s, storetest.Message(fmt.Sprintf("a-%d", i), root, "alice"))
		accept(t, s, storetest.Message(fmt.Sprintf("b-%d", i), root, "bob"))
	}
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := w.Step(context.Background()); err != nil || !ok {
				t.Errorf("worker: %v, %v", ok, err)
			}
		}()
	}
	defer wg.Wait()
	defer close(release)
	seen := map[string]bool{}
	for range 4 {
		select {
		case q := <-backend.started:
			if seen[q.ConversationID] {
				t.Fatal("parallel queries in same conversation")
			}
			seen[q.ConversationID] = true
		case <-time.After(5 * time.Second):
			t.Fatal("independent threads did not run concurrently")
		}
	}
}

type unavailableStore struct{ gateway.Store }

func (unavailableStore) Enqueue(context.Context, gateway.Message) (bool, error) {
	return false, errors.New("database unavailable")
}

func TestIntakeDoesNotAcknowledgeFailedPersistence(t *testing.T) {
	r, err := intake(unavailableStore{}).Accept(context.Background(), storetest.Message("a", "one", "alice"))
	if err == nil || r.Accepted || r.Duplicate || r.Ignored {
		t.Fatal("failed write treated as accepted")
	}
}

func TestTerminalFailuresUseControlledReplies(t *testing.T) {
	for _, state := range []string{"failed", "canceled"} {
		t.Run(state, func(t *testing.T) {
			s, _ := database(t)
			m := storetest.Message("a", "one", "alice")
			m.HasAttachments = true
			accept(t, s, m)
			backend := &fakeAgently{state: state}
			work(t, gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"})
			d, err := s.ClaimDelivery(context.Background())
			storetest.Must(t, err)
			if d == nil || !strings.Contains(d.Text, "did not complete") || !strings.Contains(d.Text, "Attachments were not read") || strings.Contains(d.Text, "TEST RESPONSE") {
				t.Fatal("failure did not produce a controlled reply")
			}
		})
	}
}
