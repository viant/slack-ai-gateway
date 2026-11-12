package gateway_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func dmPolicy(t *testing.T, directory *multiDirectory, mode string) gateway.ConfiguredPolicy {
	t.Helper()
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Mode: "selected", Allowlist: []string{}}, gateway.AccessRules{DirectMessages: &gateway.UserRule{Mode: &mode}})
	storetest.Must(t, err)
	return gateway.ConfiguredPolicy{Route: gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: r}, PrincipalID: "shared", Directory: directory}
}

func dmMessage(event, root string) gateway.Message {
	m := storetest.Message(event, root, "alice")
	m.ChannelID, m.Surface, m.Mentioned = "D", gateway.SurfaceDirectMessage, false
	return m
}

func TestDirectMessageThreadsPersistAcrossRestart(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	directory := &multiDirectory{im: true, peer: "alice"}
	p := dmPolicy(t, directory, "channel_members")
	in := gateway.Intake{Store: s, Route: p.Route}
	messages := []gateway.Message{dmMessage("root", "1"), dmMessage("reply", "1"), dmMessage("new-root", "2"), dmMessage("unknown-thread-reply", "3")}
	for _, m := range messages {
		r, err := in.Accept(ctx, m)
		storetest.Must(t, err)
		if !r.Accepted {
			t.Fatal("DM without mention was ignored")
		}
	}
	if len(directory.channels) != 0 {
		t.Fatal("network lookup before ACK")
	}
	s = reopen()
	storetest.Must(t, s.CheckRunBindings(ctx, p.Route, "shared"))
	backend := &fakeAgently{}
	w := gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "general_helper"}
	for range messages {
		work(t, w)
	}
	created, queries := backend.snapshot()
	if len(created) != 3 || len(queries) != 4 || queries[0].Query.ConversationID != queries[1].Query.ConversationID || queries[1].Query.ConversationID == queries[2].Query.ConversationID || queries[2].Query.ConversationID == queries[3].Query.ConversationID {
		t.Fatal("DM thread mapping or new root isolation failed")
	}
	for _, q := range queries {
		if q.Principal != "shared" {
			t.Fatal("Slack author became execution principal")
		}
	}
	if len(directory.users) != 0 {
		t.Fatal("channel_members requires no users.info")
	}
	s = reopen()
	sender := &fakeSlack{}
	delivery := gateway.DeliveryWorker{Store: s, Sender: sender, Policy: p}
	for range messages {
		worked, err := delivery.Step(ctx)
		storetest.Must(t, err)
		if !worked {
			t.Fatal("missing DM reply after restart")
		}
	}
	for i, post := range sender.snapshot() {
		if post.ChannelID != "D" || post.RootTS != messages[i].RootTS {
			t.Fatal("wrong DM destination")
		}
	}
	in.Store = s
	r, err := in.Accept(ctx, messages[0])
	storetest.Must(t, err)
	if !r.Duplicate {
		t.Fatal("DM dedup lost")
	}
}

func TestDirectMessageStatusAndDeliveryRecheckPeerAfterRestart(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	directory := &multiDirectory{im: true, peer: "alice"}
	p := dmPolicy(t, directory, "channel_members")
	_, err := (gateway.Intake{Store: s, Route: p.Route}).Accept(ctx, dmMessage("dm", "1"))
	storetest.Must(t, err)
	now := time.Now()
	sender := &statusSlack{}
	status := &gateway.StatusWorker{Store: s, Sender: sender, Policy: p, Now: func() time.Time { return now }}
	statusStep(t, status, "urgent", true)
	if len(sender.calls) != 1 || sender.calls[0].Status.ChannelID != "D" {
		t.Fatal("initial DM status missing")
	}
	backend := &fakeAgently{}
	work(t, gateway.Worker{Store: s, Policy: p, Clients: backend, AgentID: "steward"})
	s = reopen()
	// SlackOutgoingMessage/status must reconstruct surface and original author from durable data.
	directory.peer = "bob"
	status.Store = s
	now = now.Add(time.Minute)
	statusStep(t, status, "urgent", true)
	if len(sender.calls) != 1 {
		t.Fatal("status sent to a different DM peer")
	}
	final := &fakeSlack{}
	worked, err := (&gateway.DeliveryWorker{Store: s, Sender: final, Policy: p}).Step(ctx)
	var issue *gateway.DeliveryIssue
	if !worked || !errors.As(err, &issue) || issue.Reason != "policy_denied" || len(final.snapshot()) != 0 {
		t.Fatalf("unauthorized delivery: %v", err)
	}
	_, queries := backend.snapshot()
	if len(queries) != 1 {
		t.Fatal("query replayed")
	}
	for _, fresh := range directory.fresh[2:] {
		if !fresh {
			t.Fatal("outbound peer check used stale metadata")
		}
	}
}

func TestDirectMessagePolicyUsesRolesAndRejectsWrongMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		directory  multiDirectory
		denied     bool
	}{
		{"guest_allowed", "channel_members", multiDirectory{im: true, peer: "alice", guest: true}, false},
		{"guest_denied", "members_only", multiDirectory{im: true, peer: "alice", guest: true}, true},
		{"member_allowed", "members_only", multiDirectory{im: true, peer: "alice"}, false},
		{"channel_not_dm", "channel_members", multiDirectory{peer: "alice"}, true},
		{"wrong_peer", "channel_members", multiDirectory{im: true, peer: "bob"}, true},
		{"external", "channel_members", multiDirectory{im: true, peer: "alice", external: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := dmPolicy(t, &tc.directory, tc.mode)
			_, err := p.Authorize(context.Background(), dmMessage("dm", "1"), nil)
			if tc.denied && !errors.Is(err, gateway.ErrDenied) || !tc.denied && err != nil {
				t.Fatalf("authorization: %v", err)
			}
			if tc.mode == "channel_members" && len(tc.directory.users) != 0 {
				t.Fatal("unnecessary user lookup")
			}
		})
	}
}
