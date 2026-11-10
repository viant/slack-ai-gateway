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

type multiDirectory struct {
	channelErr               error
	private, external, guest bool
	im                       bool
	peer                     string
	fresh                    []bool
	channels                 []string
	users                    []string
}

func (d *multiDirectory) Channel(_ context.Context, w, c string, fresh bool) (gateway.ChannelInfo, error) {
	d.fresh = append(d.fresh, fresh)
	d.channels = append(d.channels, c)
	return gateway.ChannelInfo{WorkspaceID: w, ID: c, Member: true, Private: d.private, ExternalShared: d.external, IM: d.im, PeerID: d.peer}, d.channelErr
}
func (d *multiDirectory) User(_ context.Context, w, u string) (gateway.UserInfo, error) {
	d.users = append(d.users, u)
	return gateway.UserInfo{WorkspaceID: w, ID: u, Guest: d.guest}, nil
}

func multiPolicy(t *testing.T, channels []string, denyChannels, denyUsers []string, d *multiDirectory) gateway.ConfiguredPolicy {
	t.Helper()
	mode := "channel_members"
	scope := gateway.ChannelScope{Mode: "selected", Allowlist: channels, Denylist: denyChannels}
	if channels == nil {
		scope.Mode = "workspace"
	}
	r, err := gateway.NewChannelPolicyResolver(scope, gateway.AccessRules{Defaults: &gateway.UserRule{Mode: &mode}, GlobalDenylist: &denyUsers})
	storetest.Must(t, err)
	return gateway.ConfiguredPolicy{Route: gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: r}, PrincipalID: "shared", Directory: d}
}

func TestMultiChannelConversationAndDeliveryAcrossRestart(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	directory := &multiDirectory{}
	p := multiPolicy(t, []string{"C", "C2"}, nil, nil, directory)
	in := gateway.Intake{Store: s, Route: p.Route}
	var messages []gateway.Message
	for i, author := range []string{"alice", "bob", "alice"} {
		m := storetest.Message(string(rune('a'+i)), "1.001", author)
		if i == 2 {
			m.ChannelID = "C2"
		}
		messages = append(messages, m)
		r, err := in.Accept(ctx, m)
		storetest.Must(t, err)
		if !r.Accepted {
			t.Fatal("missing accepted event")
		}
	}
	r, err := in.Accept(ctx, messages[0])
	storetest.Must(t, err)
	if !r.Duplicate {
		t.Fatal("dedup lost")
	}
	if len(directory.channels) != 0 {
		t.Fatal("ACK path performed metadata lookup")
	}
	s = reopen()
	storetest.Must(t, s.CheckRunBindings(ctx, p.Route, "shared"))
	backend := &fakeAgently{}
	w := gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "general_helper"}
	for range 3 {
		work(t, w)
	}
	created, queries := backend.snapshot()
	if len(created) != 2 || len(queries) != 3 || queries[0].Query.ConversationID != queries[1].Query.ConversationID || queries[0].Query.ConversationID == queries[2].Query.ConversationID {
		t.Fatal("channel/thread isolation or shared ownership broken")
	}
	for _, q := range queries {
		if q.Principal != "shared" || q.Query.AgentID != "general_helper" {
			t.Fatal("hardcoded steward or author principal")
		}
	}
	s = reopen()
	sender := &fakeSlack{}
	delivery := gateway.DeliveryWorker{Store: s, Sender: sender, Policy: p}
	for range 3 {
		ok, err := delivery.Step(ctx)
		storetest.Must(t, err)
		if !ok {
			t.Fatal("missing delivery")
		}
	}
	posts := sender.snapshot()
	if len(posts) != 3 || posts[0].ChannelID != "C" || posts[1].ChannelID != "C" || posts[2].ChannelID != "C2" {
		t.Fatal("cross-channel reply")
	}
	s = reopen()
	in.Store = s
	r, err = in.Accept(ctx, messages[2])
	storetest.Must(t, err)
	if !r.Duplicate {
		t.Fatal("restart dedup")
	}
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(status.Bindings) != 2 || status.Jobs[0].AuthorID != "alice" || status.Jobs[1].AuthorID != "bob" || status.Deliveries[0].ChannelID != "C2" {
		t.Fatal("multi-channel diagnostics missing")
	}
}

func TestWorkspaceRoutingAndUnconfiguredChannelIgnore(t *testing.T) {
	s, _ := database(t)
	p := multiPolicy(t, nil, []string{"DENIED"}, nil, &multiDirectory{})
	in := gateway.Intake{Store: s, Route: p.Route}
	for _, channel := range []string{"NEW", "DENIED"} {
		m := storetest.Message(channel, "1", "alice")
		m.ChannelID = channel
		r, err := in.Accept(context.Background(), m)
		storetest.Must(t, err)
		if r.Accepted != (channel == "NEW") || r.Ignored != (channel == "DENIED") {
			t.Fatal("workspace route incorrect")
		}
	}
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Mode: "workspace"}, gateway.AccessRules{})
	storetest.Must(t, err)
	in.Route.Resolver = r
	m := storetest.Message("no-policy", "2", "alice")
	m.ChannelID = "NEW"
	result, err := in.Accept(context.Background(), m)
	storetest.Must(t, err)
	if !result.Ignored {
		t.Fatal("route without policy opened")
	}
}

func TestPolicyChangeAfterRestartRejectsQueuedButNotOtherChannels(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	for _, channel := range []string{"C", "C2"} {
		m := storetest.Message(channel, "1", "alice")
		m.ChannelID = channel
		_, err := s.Enqueue(ctx, m)
		storetest.Must(t, err)
	}
	s = reopen()
	p := multiPolicy(t, []string{"C2"}, nil, nil, &multiDirectory{})
	storetest.Must(t, s.CheckRunBindings(ctx, p.Route, "shared"))
	backend := &fakeAgently{}
	w := gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "steward"}
	work(t, w)
	work(t, w)
	created, queries := backend.snapshot()
	if len(created) != 1 || len(queries) != 1 {
		t.Fatal("removed channel executed query")
	}
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if status.Jobs[1].State != "rejected" || status.Jobs[1].Reason != "policy_denied" || status.Jobs[0].State != "completed" {
		t.Fatal("incorrect queue disposition")
	}
}

func TestPolicyDenialOnExistingTurnQuarantinesWithoutQueryReplay(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	accept(t, s, storetest.Message("a", "one", "alice"))
	backend := &fakeAgently{state: "queued"}
	p := multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{})
	w := gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Millisecond}}
	work(t, w)
	s = reopen()
	time.Sleep(3 * time.Millisecond)
	w.Store = s
	w.Policy = multiPolicy(t, []string{"C"}, nil, []string{"alice"}, &multiDirectory{})
	work(t, w)
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	_, queries := backend.snapshot()
	if len(queries) != 1 || status.Jobs[0].State != "needs_review" || status.Jobs[0].Reason != "policy_denied" || len(status.Deliveries) != 0 {
		t.Fatal("denied turn replayed or incorrectly rejected")
	}
}

func TestPolicyMetadataFailureBeforeQueryHasNoBackendSideEffects(t *testing.T) {
	for _, failure := range []error{gateway.ErrMetadataUnavailable, gateway.ErrSlackAuth} {
		t.Run(failure.Error(), func(t *testing.T) {
			s, _ := database(t)
			accept(t, s, storetest.Message("a", "one", "alice"))
			backend := &fakeAgently{}
			p := multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{channelErr: failure})
			worked, err := (gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "steward"}).Step(context.Background())
			if !worked || (failure == gateway.ErrSlackAuth && !errors.Is(err, failure)) || (failure == gateway.ErrMetadataUnavailable && err != nil) {
				t.Fatal(err)
			}
			created, queries := backend.snapshot()
			if len(created) != 0 || len(queries) != 0 {
				t.Fatal("query ran on unknown policy")
			}
			status, err := s.Status(context.Background(), 10)
			storetest.Must(t, err)
			if status.Jobs[0].State != "needs_review" || len(status.Deliveries) != 0 {
				t.Fatal("metadata failure not quarantined")
			}
		})
	}
}

func TestOutboundRechecksOriginalAuthorAndChannelBeforePost(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		channels, deniedUsers []string
		directory             multiDirectory
		state                 string
		fatal                 error
	}{
		{"author_revoked", []string{"C"}, []string{"alice"}, multiDirectory{}, "failed", nil},
		{"channel_revoked", []string{}, nil, multiDirectory{}, "failed", nil},
		{"private", []string{"C"}, nil, multiDirectory{private: true}, "failed", nil},
		{"external", []string{"C"}, nil, multiDirectory{external: true}, "failed", nil},
		{"metadata", []string{"C"}, nil, multiDirectory{channelErr: gateway.ErrMetadataUnavailable}, "pending", gateway.ErrMetadataUnavailable},
		{"credentials", []string{"C"}, nil, multiDirectory{channelErr: gateway.ErrSlackAuth}, "pending", gateway.ErrSlackAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			s, reopen := database(t)
			accept(t, s, storetest.Message("a", "one", "alice"))
			work(t, gateway.Worker{Store: s, Policy: multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{}), Clients: &fakeAgently{}, AgentID: "steward"})
			s = reopen()
			p := multiPolicy(t, tc.channels, nil, tc.deniedUsers, &tc.directory)
			storetest.Must(t, s.CheckRunBindings(ctx, p.Route, "shared"))
			sender := &fakeSlack{}
			w := gateway.DeliveryWorker{Store: s, Sender: sender, Policy: p}
			worked, err := w.Step(ctx)
			if !worked {
				t.Fatal("reply not checked")
			}
			if tc.fatal != nil {
				if !errors.Is(err, tc.fatal) {
					t.Fatal(err)
				}
			} else {
				var issue *gateway.DeliveryIssue
				if !errors.As(err, &issue) || issue.Reason != "policy_denied" {
					t.Fatal(err)
				}
			}
			if len(sender.snapshot()) != 0 {
				t.Fatal("forbidden or unknown-policy reply posted")
			}
			status, statusErr := s.Status(ctx, 10)
			storetest.Must(t, statusErr)
			if status.Deliveries[0].State != tc.state {
				t.Fatalf("state %s", status.Deliveries[0].State)
			}
			if len(tc.directory.fresh) > 0 && !tc.directory.fresh[0] {
				t.Fatal("post used stale channel info")
			}
			if tc.state == "pending" {
				// Repair metadata and restart: retry the exact part, not the query.
				s = reopen()
				p.Directory = &multiDirectory{}
				w = gateway.DeliveryWorker{Store: s, Sender: sender, Policy: p}
				worked, err = w.Step(ctx)
				storetest.Must(t, err)
				if !worked || len(sender.snapshot()) != 1 {
					t.Fatal("safe retry lost")
				}
			}
		})
	}
}

type recordingDeliveryPolicy struct {
	authors []string
	fresh   []bool
}

func (p *recordingDeliveryPolicy) AuthorizeDelivery(_ context.Context, d gateway.DeliveryContext, fresh bool) error {
	p.authors = append(p.authors, d.Message.AuthorID)
	p.fresh = append(p.fresh, fresh)
	return nil
}

func TestDeliveryPolicyFreshPerJobAndOriginalAuthorForHelp(t *testing.T) {
	ctx := context.Background()
	s, _ := database(t)
	m := storetest.Message("a", "one", "alice")
	m.Text = strings.Repeat("x", 16000)
	accept(t, s, m)
	help := storetest.Message("b", "two", "bob")
	help.Text = ""
	accept(t, s, help)
	w := gateway.Worker{Store: s, Policy: multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{}), Clients: &fakeAgently{}, AgentID: "steward"}
	work(t, w)
	work(t, w)
	p := &recordingDeliveryPolicy{}
	sender := &fakeSlack{}
	delivery := gateway.DeliveryWorker{Store: s, Sender: sender, Policy: p}
	for {
		worked, err := delivery.Step(ctx)
		storetest.Must(t, err)
		if !worked {
			break
		}
	}
	if len(p.authors) < 3 || !p.fresh[0] || p.fresh[1] || !p.fresh[len(p.fresh)-1] || p.authors[0] != "alice" || p.authors[len(p.authors)-1] != "bob" {
		t.Fatal("fresh check or original author lost")
	}
	if b, err := s.GetBinding(ctx, help.ThreadKey); err != nil || b != nil {
		t.Fatal("help created binding")
	}
}
