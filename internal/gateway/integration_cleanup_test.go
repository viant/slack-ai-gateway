package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type cleanupDirectory struct {
	peer  string
	fresh []bool
}

func (d *cleanupDirectory) Channel(_ context.Context, workspace, channel string, fresh bool) (ChannelInfo, error) {
	d.fresh = append(d.fresh, fresh)
	return ChannelInfo{WorkspaceID: workspace, ID: channel, Member: true, IM: d.peer != "", PeerID: d.peer}, nil
}

func (*cleanupDirectory) User(_ context.Context, workspace, user string) (UserInfo, error) {
	return UserInfo{WorkspaceID: workspace, ID: user}, nil
}

func cleanupPolicy(t *testing.T, d *cleanupDirectory, denied []string) ConfiguredPolicy {
	t.Helper()
	mode := "workspace_members_and_guests"
	r, err := NewChannelPolicyResolver(ChannelScope{Enabled: true, Scope: "selected", Allowlist: []string{"C"}}, AccessRules{
		Users: &UserRule{Scope: mode, Denylist: denied}, DirectMessages: &DirectMessageRule{Enabled: true, Users: &UserRule{Scope: mode, Denylist: denied}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ConfiguredPolicy{Route: Route{AppID: "A", WorkspaceID: "T", Resolver: r}, PrincipalID: "new-principal", Directory: d}
}

func cleanupContext() DeliveryContext {
	m := Message{ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}, AppID: "A", AuthorID: "alice", MessageTS: "1.000001", Mentioned: true}
	return DeliveryContext{Message: m, ConversationBinding: &ConversationBinding{ThreadKey: m.ThreadKey, Decision: Decision{Mode: "shared_channel", PrincipalID: "old-principal", OwnerID: "old-principal"}, State: "closed"},
		IntegrationPrincipalID: "old-principal", Kind: DeliveryIntegrationNotice, TerminationReason: "integration_changed"}
}

func TestConfigurationClosureUsesHistoricalPrincipalAndCurrentSlackPolicy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*DeliveryContext)
		denied []string
		wantOK bool
	}{
		{name: "closure", wantOK: true},
		{name: "removed", change: func(c *DeliveryContext) { c.TerminationReason = "integration_removed" }, wantOK: true},
		{name: "normal_response", change: func(c *DeliveryContext) { c.Kind = DeliveryResponse }},
		{name: "active_assignment", change: func(c *DeliveryContext) { c.IntegrationActive = true }},
		{name: "untrusted_reason", change: func(c *DeliveryContext) { c.TerminationReason = "arbitrary" }},
		{name: "missing_old_principal", change: func(c *DeliveryContext) { c.IntegrationPrincipalID = "" }},
		{name: "wrong_old_principal", change: func(c *DeliveryContext) { c.IntegrationPrincipalID = "third-principal" }},
		{name: "current_access_revoked", denied: []string{"alice"}},
		{name: "wrong_bot", change: func(c *DeliveryContext) { c.Message.AppID = "OTHER" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &cleanupDirectory{}
			p := cleanupPolicy(t, d, tc.denied)
			c := cleanupContext()
			if tc.change != nil {
				tc.change(&c)
			}
			err := p.AuthorizeDelivery(context.Background(), c, true)
			if tc.wantOK && err != nil || !tc.wantOK && !errors.Is(err, ErrDenied) {
				t.Fatalf("cleanup authorization=%v, wantOK=%v", err, tc.wantOK)
			}
			if c.Message.AuthorID != "alice" || p.PrincipalID != "new-principal" {
				t.Fatal("cleanup changed the author or running policy principal")
			}
			if tc.wantOK && (len(d.fresh) != 1 || !d.fresh[0]) {
				t.Fatal("closure did not freshly validate Slack metadata")
			}
		})
	}
}

func TestConfigurationClosureStillChecksDirectMessagePeer(t *testing.T) {
	for _, peer := range []string{"alice", "bob"} {
		t.Run(peer, func(t *testing.T) {
			c := cleanupContext()
			c.Message.ChannelID, c.Message.Surface, c.Message.Mentioned = "D", SurfaceDirectMessage, false
			c.ConversationBinding.ThreadKey = c.Message.ThreadKey
			p := cleanupPolicy(t, &cleanupDirectory{peer: peer}, nil)
			err := p.AuthorizeDelivery(context.Background(), c, true)
			if peer == "alice" && err != nil || peer != "alice" && !errors.Is(err, ErrDenied) {
				t.Fatalf("peer %s: %v", peer, err)
			}
		})
	}
}

type cleanupStatusStore struct {
	StatusStore
	status  *GatewayJobProgress
	kinds   []string
	results []StatusResult
}

func (s *cleanupStatusStore) ClaimStatus(_ context.Context, _ time.Time, kind string) (*GatewayJobProgress, error) {
	s.kinds = append(s.kinds, kind)
	if kind != "integration_terminal" {
		return nil, nil
	}
	status := s.status
	s.status = nil
	return status, nil
}

func (*cleanupStatusStore) GetStatusContext(context.Context, GatewayJobProgress) (*DeliveryContext, error) {
	c := cleanupContext()
	return &c, nil
}

func (s *cleanupStatusStore) FinishStatus(_ context.Context, _ GatewayJobProgress, result StatusResult, _ time.Time) error {
	s.results = append(s.results, result)
	return nil
}

type cleanupDeliveryStore struct {
	Store
	claims    int
	delivered bool
}

func (s *cleanupDeliveryStore) ClaimDelivery(context.Context) (*SlackOutgoingMessage, error) {
	s.claims++
	if s.delivered {
		return nil, nil
	}
	return &SlackOutgoingMessage{ID: 2, JobID: 2, Text: "new response", Kind: DeliveryResponse}, nil
}

func (s *cleanupDeliveryStore) Delivered(context.Context, int64, string) error {
	s.delivered = true
	return nil
}

type cleanupSlack struct {
	order []string
	text  string
}

func (s *cleanupSlack) Send(context.Context, SlackOutgoingMessage) (string, error) {
	s.order = append(s.order, "response")
	return "3.000001", nil
}

func (s *cleanupSlack) SendStatus(_ context.Context, status GatewayJobProgress, text string) (StatusReceipt, error) {
	s.order = append(s.order, "closure")
	s.text = text
	return StatusReceipt{SlackTS: status.SlackTS}, nil
}

type cleanupRecoveringBackend struct{}

func (cleanupRecoveringBackend) State() BackendState { return BackendRecovering }

func TestConfigurationClosurePreemptsBackendRecoveryWithSharedPacing(t *testing.T) {
	now := time.Now()
	original := cleanupContext()
	statuses := &cleanupStatusStore{status: &GatewayJobProgress{JobID: 1, ThreadKey: original.Message.ThreadKey,
		MessageTS: original.Message.MessageTS, SlackTS: "2.000001", Phase: "integration_changed", Terminal: true, Submitted: true, CreatedAt: now}}
	deliveries := &cleanupDeliveryStore{}
	slack := &cleanupSlack{}
	statusWorker := &StatusWorker{Store: statuses, Sender: slack, Policy: cleanupPolicy(t, &cleanupDirectory{}, nil), Backend: cleanupRecoveringBackend{}, Now: func() time.Time { return now }}
	w := &OutboundWorker{Status: statusWorker, SlackOutgoingMessage: &DeliveryWorker{Store: deliveries, Sender: slack}, Interval: time.Second, Now: func() time.Time { return now }}
	worked, err := w.Step(context.Background())
	if err != nil || !worked || deliveries.claims != 0 || len(slack.order) != 1 || slack.order[0] != "closure" || statuses.kinds[0] != "integration_terminal" {
		t.Fatalf("configuration closure did not preempt other work: worked=%v err=%v claims=%d kinds=%v", worked, err, deliveries.claims, statuses.kinds)
	}
	if !strings.Contains(slack.text, "configuration changed") || strings.Contains(slack.text, "checking your existing request") || statuses.results[0].State != "settled" {
		t.Fatalf("closure was replaced with backend recovery: %q / %+v", slack.text, statuses.results)
	}
	worked, err = w.Step(context.Background())
	if worked || err != nil || len(slack.order) != 1 {
		t.Fatal("closure bypassed outbound pacing")
	}
	now = now.Add(time.Second)
	worked, err = w.Step(context.Background())
	if !worked || err != nil || !deliveries.delivered || len(slack.order) != 2 || slack.order[1] != "response" {
		t.Fatalf("normal delivery did not resume after closure: worked=%v err=%v", worked, err)
	}
}
