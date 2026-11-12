package gateway

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func resolver(t *testing.T, scope ChannelScope, rules AccessRules) *ChannelPolicyResolver {
	t.Helper()
	r, err := NewChannelPolicyResolver(scope, rules)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEffectivePolicyInheritanceAndGlobalCaps(t *testing.T) {
	r := resolver(t, ChannelScope{Mode: "selected", Allowlist: []string{"C1", "C2", "C3", "C4"}}, AccessRules{
		GlobalAllowlist: ptr([]string{"alice", "bob", "blocked"}), GlobalDenylist: ptr([]string{"blocked"}),
		Defaults: &UserRule{Mode: ptr("channel_members"), Allowlist: ptr([]string{"alice"}), Denylist: ptr([]string{"bob"})},
		Channels: map[string]UserRule{
			"C2": {Mode: ptr("members_only"), Allowlist: ptr([]string{"bob", "outsider", "blocked"}), Denylist: ptr([]string{})},
			"C3": {Allowlist: ptr([]string{})},
			"C4": {Denylist: ptr([]string{"alice"})},
		},
	})
	for _, tc := range []struct {
		channel, user string
		want          bool
	}{
		{"C1", "alice", true}, {"C1", "bob", false}, {"C2", "bob", true}, {"C2", "alice", false},
		{"C2", "outsider", false}, {"C2", "blocked", false}, {"C3", "alice", false}, {"C4", "alice", false}, {"OTHER", "alice", false},
	} {
		if got := r.Resolve(tc.channel).AllowsUser(tc.user); got != tc.want {
			t.Fatalf("%s/%s: %v", tc.channel, tc.user, got)
		}
	}
	if r.Resolve("C1").UserMode != "channel_members" || r.Resolve("C2").UserMode != "members_only" {
		t.Fatal("mode inheritance")
	}
	if !reflect.DeepEqual(r.Resolve("C2").Allowlist, []string{"bob", "blocked"}) {
		t.Fatal("global cap is not an intersection")
	}
	if r.Resolve("C3").Allowlist == nil {
		t.Fatal("explicit empty lost")
	}
}

func TestNoImplicitGrantAndChannelRouting(t *testing.T) {
	for _, mode := range []string{"selected", "workspace"} {
		scope := ChannelScope{Mode: mode, Denylist: []string{"CDENIED"}}
		if mode == "selected" {
			scope.Allowlist = []string{"C1", "C2", "CDENIED"}
		}
		r := resolver(t, scope, AccessRules{GlobalAllowlist: ptr([]string{"alice"}), Channels: map[string]UserRule{
			"C1": {Mode: ptr("channel_members")}, "CDENIED": {Mode: ptr("channel_members")},
		}})
		if !r.Resolve("C1").AllowsUser("alice") || r.Resolve("C2").AllowsUser("alice") || r.Resolve("CDENIED").AllowsUser("alice") {
			t.Fatal("implicit grant or denied channel opened")
		}
		if r.Resolve("C2").Present {
			t.Fatal("global opened a policy")
		}
	}
	r := resolver(t, ChannelScope{Mode: "workspace"}, AccessRules{Defaults: &UserRule{Mode: ptr("channel_members")}})
	if !r.Resolve("ANY").AllowsUser("guest") || r.Resolve("ANY").Allowlist != nil {
		t.Fatal("workspace defaults")
	}
	closed := resolver(t, ChannelScope{Mode: "selected", Allowlist: []string{}}, AccessRules{Defaults: &UserRule{Mode: ptr("channel_members")}})
	if closed.Resolve("ANY").AllowsUser("alice") {
		t.Fatal("empty selected scope opened")
	}
	cap := resolver(t, ChannelScope{Mode: "workspace"}, AccessRules{GlobalAllowlist: ptr([]string{}), Defaults: &UserRule{Mode: ptr("channel_members")}})
	if cap.Resolve("ANY").AllowsUser("alice") {
		t.Fatal("empty global allowlist opened")
	}
}

func TestPolicyValidationAndImmutableCopies(t *testing.T) {
	for _, tc := range []struct {
		scope ChannelScope
		rules AccessRules
	}{
		{ChannelScope{Mode: "bad"}, AccessRules{}},
		{ChannelScope{Mode: "selected"}, AccessRules{}},
		{ChannelScope{Mode: "workspace", Allowlist: []string{}}, AccessRules{}},
		{ChannelScope{Mode: "selected", Allowlist: []string{"C", "C"}}, AccessRules{}},
		{ChannelScope{Mode: "workspace"}, AccessRules{Defaults: &UserRule{Allowlist: ptr([]string{"alice"})}}},
		{ChannelScope{Mode: "workspace"}, AccessRules{Defaults: &UserRule{Mode: ptr("employees")}}},
		{ChannelScope{Mode: "selected", Allowlist: []string{"C"}}, AccessRules{Channels: map[string]UserRule{"OTHER": {Mode: ptr("channel_members")}}}},
		{ChannelScope{Mode: "workspace"}, AccessRules{Channels: map[string]UserRule{"C": {}}}},
		{ChannelScope{Mode: "workspace"}, AccessRules{GlobalDenylist: ptr([]string{" alice"})}},
	} {
		if _, err := NewChannelPolicyResolver(tc.scope, tc.rules); err == nil {
			t.Fatalf("invalid policy accepted: %+v", tc.scope)
		}
	}
	ids := []string{"C"}
	mode := "channel_members"
	users := []string{"alice"}
	r := resolver(t, ChannelScope{Mode: "selected", Allowlist: ids}, AccessRules{Defaults: &UserRule{Mode: &mode, Allowlist: &users}})
	ids[0] = "OTHER"
	mode = "members_only"
	users[0] = "bob"
	resolved := r.Resolve("C")
	resolved.Allowlist[0] = "mallory"
	scope := r.Scope()
	scope.Allowlist[0] = "OTHER"
	if !r.Resolve("C").AllowsUser("alice") || r.Resolve("C").UserMode != "channel_members" {
		t.Fatal("resolver mutated")
	}
}

type policyDirectory struct {
	channel                 ChannelInfo
	user                    UserInfo
	channelErr, userErr     error
	channelCalls, userCalls int
	fresh                   bool
}

func (d *policyDirectory) Channel(_ context.Context, w, c string, fresh bool) (ChannelInfo, error) {
	d.channelCalls++
	d.fresh = fresh
	return d.channel, d.channelErr
}
func (d *policyDirectory) User(_ context.Context, w, u string) (UserInfo, error) {
	d.userCalls++
	return d.user, d.userErr
}

func TestConfiguredPolicyChannelAndUserFacts(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		alter      func(*policyDirectory, *Message, *ConversationBinding)
		want       error
	}{
		{"regular", "members_only", nil, nil},
		{"guest_channel_members", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Guest = true }, nil},
		{"guest_members_only", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Guest = true }, ErrDenied},
		{"single_guest", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.SingleChannelGuest = true }, ErrDenied},
		{"deleted", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Deleted = true }, ErrDenied},
		{"bot", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Bot = true }, ErrDenied},
		{"wrong_user", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.ID = "bob" }, ErrDenied},
		{"foreign_workspace", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.WorkspaceID = "OTHER" }, ErrDenied},
		{"not_joined", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Member = false }, ErrDenied},
		{"archived", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Archived = true }, ErrDenied},
		{"external", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.ExternalShared = true }, ErrDenied},
		{"pending_external", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.PendingExternalShared = true }, ErrDenied},
		{"private_default_denied", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Private = true }, ErrDenied},
		{"dm", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.IM = true }, ErrDenied},
		{"group_dm", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.MPIM = true }, ErrDenied},
		{"channel_identity", "channel_members", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.ID = "OTHER" }, ErrDenied},
		{"binding_identity", "channel_members", func(_ *policyDirectory, _ *Message, b *ConversationBinding) { b.OwnerID = "bob" }, ErrDenied},
		{"binding_thread", "channel_members", func(_ *policyDirectory, _ *Message, b *ConversationBinding) { b.RootTS = "OTHER" }, ErrDenied},
		{"raw_external", "channel_members", func(_ *policyDirectory, m *Message, _ *ConversationBinding) { m.ExternalShared = true }, ErrDenied},
		{"metadata_unavailable", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channelErr = ErrMetadataUnavailable }, ErrMetadataUnavailable},
		{"slack_auth", "members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.userErr = ErrSlackAuth }, ErrSlackAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := resolver(t, ChannelScope{Mode: "workspace"}, AccessRules{Defaults: &UserRule{Mode: ptr(tc.mode)}})
			m := Message{AppID: "A", ThreadKey: ThreadKey{"T", "C", "1.001"}, AuthorID: "alice", Mentioned: true}
			b := ConversationBinding{ThreadKey: m.ThreadKey, Decision: Decision{"shared_channel", "shared", "shared"}}
			d := &policyDirectory{channel: ChannelInfo{WorkspaceID: "T", ID: "C", Member: true}, user: UserInfo{WorkspaceID: "T", ID: "alice"}}
			if tc.alter != nil {
				tc.alter(d, &m, &b)
			}
			p := ConfiguredPolicy{Route: Route{AppID: "A", WorkspaceID: "T", Resolver: r}, PrincipalID: "shared", Directory: d}
			decision, err := p.Authorize(context.Background(), m, &b)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
			if err == nil && decision != b.Decision {
				t.Fatal("Slack author became execution principal")
			}
			if tc.mode == "channel_members" && d.userCalls != 0 {
				t.Fatal("unnecessary users.info")
			}
			if err == nil {
				if err = p.AuthorizeDelivery(context.Background(), DeliveryContext{Message: m, ConversationBinding: &b}, true); err != nil || !d.fresh {
					t.Fatal("outbound did not force fresh metadata")
				}
			}
		})
	}
	private := resolver(t, ChannelScope{Mode: "workspace", AllowPrivate: true}, AccessRules{Defaults: &UserRule{Mode: ptr("channel_members")}})
	p := ConfiguredPolicy{Route: Route{AppID: "A", WorkspaceID: "T", Resolver: private}, PrincipalID: "shared", Directory: &policyDirectory{channel: ChannelInfo{WorkspaceID: "T", ID: "C", Private: true, Member: true}}}
	if _, err := p.Authorize(context.Background(), Message{AppID: "A", ThreadKey: ThreadKey{"T", "C", "1"}, AuthorID: "alice"}, nil); err != nil {
		t.Fatal(err)
	}
}
