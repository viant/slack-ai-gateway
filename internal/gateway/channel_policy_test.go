package gateway

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func resolver(t *testing.T, scope ChannelScope, rules AccessRules) *ChannelPolicyResolver {
	t.Helper()
	r, err := NewChannelPolicyResolver(scope, rules)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestEffectivePolicyUsesCompleteChannelOverrides(t *testing.T) {
	r := resolver(t, ChannelScope{Enabled: true, Scope: "selected", Allowlist: []string{"C1", "C2", "C3", "C4", "OFF"}, Denylist: []string{"DENIED"}}, AccessRules{
		Users: &UserRule{Scope: "workspace_members_only", Allowlist: []string{"alice"}, Denylist: []string{"bob"}},
		ChannelOverrides: map[string]ChannelRule{
			"C2":      {Enabled: true, Users: &UserRule{Scope: "workspace_members_and_guests", Allowlist: []string{"bob", "outsider", "blocked"}, Denylist: []string{"blocked"}}},
			"C3":      {Enabled: true, Users: &UserRule{Scope: "workspace_members_only", Allowlist: []string{}}},
			"C4":      {Enabled: true, Users: &UserRule{Scope: "workspace_members_only", Denylist: []string{"alice"}}},
			"OUTSIDE": {Enabled: true, AllowPrivate: true, Users: &UserRule{Scope: "workspace_members_and_guests"}},
			"DENIED":  {Enabled: true, Users: &UserRule{Scope: "workspace_members_only"}},
			"OFF":     {Enabled: false},
		},
	})
	for _, tc := range []struct {
		channel, user string
		want          bool
	}{
		{"C1", "alice", true}, {"C1", "bob", false}, {"C2", "bob", true}, {"C2", "alice", false},
		{"C2", "outsider", true}, {"C2", "blocked", false}, {"C3", "alice", false},
		{"C4", "alice", false}, {"C4", "bob", true}, {"OUTSIDE", "guest", true},
		{"DENIED", "alice", true}, {"OFF", "alice", false}, {"OTHER", "alice", false}, {"", "alice", false},
	} {
		if got := r.Resolve(tc.channel).AllowsUser(tc.user); got != tc.want {
			t.Fatalf("%s/%s: %v", tc.channel, tc.user, got)
		}
	}
	if r.Resolve("C1").UserScope != "workspace_members_only" || r.Resolve("C2").UserScope != "workspace_members_and_guests" {
		t.Fatal("override did not replace user scope")
	}
	if !reflect.DeepEqual(r.Resolve("C2").Allowlist, []string{"bob", "outsider", "blocked"}) || r.Resolve("C3").Allowlist == nil {
		t.Fatal("override list or explicit empty list changed")
	}
	if !r.Resolve("OUTSIDE").AllowPrivate || r.Resolve("C2").AllowPrivate || r.Resolve("C1").RuleSource != "general" || r.Resolve("OUTSIDE").RuleSource != "channel_override" || r.Resolve("OFF").RuleSource != "disabled" {
		t.Fatal("wrong privacy or source of effective rule")
	}
	if !reflect.DeepEqual(r.ChannelIDs(), []string{"C1", "C2", "C3", "C4", "DENIED", "OFF", "OUTSIDE"}) {
		t.Fatal("diagnostic channel IDs omit filters or overrides")
	}
}

func TestNoImplicitGrantAndChannelRouting(t *testing.T) {
	for _, scope := range []string{"selected", "workspace_channels_with_bot"} {
		r := resolver(t, ChannelScope{Enabled: true, Scope: scope, Allowlist: []string{"C1", "DENIED"}, Denylist: []string{"DENIED"}}, AccessRules{Users: &UserRule{Scope: "workspace_members_only"}})
		if !r.Resolve("C1").AllowsUser("alice") || r.Resolve("OTHER").AllowsUser("alice") || r.Resolve("DENIED").AllowsUser("alice") {
			t.Fatal("channel intersection or deny precedence incorrect")
		}
	}
	for _, scope := range []string{"selected", "workspace_channels_with_bot"} {
		r := resolver(t, ChannelScope{Enabled: true, Scope: scope, Allowlist: []string{}}, AccessRules{
			Users:            &UserRule{Scope: "workspace_members_only"},
			ChannelOverrides: map[string]ChannelRule{"ONLY": {Enabled: true, Users: &UserRule{Scope: "workspace_members_only"}}},
		})
		if r.Resolve("OTHER").AllowsUser("alice") || !r.Resolve("ONLY").AllowsUser("alice") {
			t.Fatal("empty general allowlist did not select only overrides")
		}
	}
	r := resolver(t, ChannelScope{Enabled: true, Scope: "workspace_channels_with_bot"}, AccessRules{Users: &UserRule{Scope: "workspace_members_and_guests"}})
	if !r.Resolve("ANY").AllowsUser("guest") || r.Resolve("ANY").Allowlist != nil {
		t.Fatal("omitted workspace allowlist changed")
	}
	closed := resolver(t, ChannelScope{}, AccessRules{
		ChannelOverrides: map[string]ChannelRule{"C": {Enabled: true, Users: &UserRule{Scope: "workspace_members_only"}}},
		DirectMessages:   &DirectMessageRule{Enabled: true, Users: &UserRule{Scope: "workspace_members_only"}},
	})
	if p := closed.Resolve("C"); p.Enabled || p.RouteAllowed || p.AllowsUser("alice") || p.RuleSource != "disabled" {
		t.Fatal("override bypassed channel master switch")
	}
	if !closed.ResolveDirectMessages().AllowsUser("alice") {
		t.Fatal("channel master switch disabled independent DM")
	}
	blocked := resolver(t, ChannelScope{Enabled: true, Scope: "workspace_channels_with_bot"}, AccessRules{
		Users:          &UserRule{Scope: "workspace_members_only", Allowlist: []string{}},
		DirectMessages: &DirectMessageRule{Users: &UserRule{Scope: "workspace_members_and_guests"}},
	})
	if blocked.Resolve("ANY").AllowsUser("alice") || blocked.ResolveDirectMessages().AllowsUser("alice") {
		t.Fatal("empty user allowlist or disabled DM opened")
	}
}

func TestPolicyValidationAndImmutableCopies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		scope ChannelScope
		rules AccessRules
	}{
		{"unknown_scope", ChannelScope{Scope: "bad"}, AccessRules{}},
		{"missing_scope", ChannelScope{Enabled: true}, AccessRules{}},
		{"missing_selected_list", ChannelScope{Enabled: true, Scope: "selected"}, AccessRules{Users: &UserRule{Scope: "workspace_members_only"}}},
		{"duplicate_channels", ChannelScope{Allowlist: []string{"C", "C"}}, AccessRules{}},
		{"invalid_channel_id", ChannelScope{Denylist: []string{" C"}}, AccessRules{}},
		{"missing_users", ChannelScope{Enabled: true, Scope: "workspace_channels_with_bot"}, AccessRules{}},
		{"missing_user_scope", ChannelScope{}, AccessRules{Users: &UserRule{Allowlist: []string{"alice"}}}},
		{"old_user_scope", ChannelScope{}, AccessRules{Users: &UserRule{Scope: "channel_members"}}},
		{"invalid_user_id", ChannelScope{}, AccessRules{Users: &UserRule{Scope: "workspace_members_only", Denylist: []string{" alice"}}}},
		{"duplicate_user_id", ChannelScope{}, AccessRules{Users: &UserRule{Scope: "workspace_members_only", Allowlist: []string{"alice", "alice"}}}},
		{"enabled_override_missing_users", ChannelScope{}, AccessRules{ChannelOverrides: map[string]ChannelRule{"C": {Enabled: true}}}},
		{"invalid_override_id", ChannelScope{}, AccessRules{ChannelOverrides: map[string]ChannelRule{" C": {}}}},
		{"disabled_override_invalid_users", ChannelScope{}, AccessRules{ChannelOverrides: map[string]ChannelRule{"C": {Users: &UserRule{}}}}},
		{"enabled_dm_missing_users", ChannelScope{}, AccessRules{DirectMessages: &DirectMessageRule{Enabled: true}}},
		{"disabled_dm_invalid_users", ChannelScope{}, AccessRules{DirectMessages: &DirectMessageRule{Users: &UserRule{Scope: "bad"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewChannelPolicyResolver(tc.scope, tc.rules); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
	ids, denied, users := []string{"C"}, []string{"BLOCKED"}, []string{"alice"}
	general := &UserRule{Scope: "workspace_members_and_guests", Allowlist: users}
	override := &UserRule{Scope: "workspace_members_only", Allowlist: []string{"bob"}, Denylist: []string{"blocked"}}
	dm := &DirectMessageRule{Enabled: true, Users: &UserRule{Scope: "workspace_members_only", Allowlist: []string{}}}
	rules := AccessRules{Users: general, ChannelOverrides: map[string]ChannelRule{"OTHER": {Enabled: true, Users: override}}, DirectMessages: dm}
	r := resolver(t, ChannelScope{Enabled: true, Scope: "selected", Allowlist: ids, Denylist: denied}, rules)
	ids[0], denied[0], users[0], general.Scope = "NEW", "C", "mallory", "workspace_members_only"
	override.Allowlist[0], override.Denylist[0], dm.Enabled, dm.Users.Scope = "mallory", "bob", false, "workspace_members_and_guests"
	delete(rules.ChannelOverrides, "OTHER")
	resolved := r.Resolve("C")
	resolved.Allowlist[0] = "mallory"
	resolved = r.Resolve("OTHER")
	resolved.Allowlist[0], resolved.Denylist[0] = "mallory", "bob"
	scope := r.Scope()
	scope.Allowlist[0], scope.Denylist[0] = "OTHER", "C"
	if !r.Resolve("C").AllowsUser("alice") || r.Resolve("C").UserScope != "workspace_members_and_guests" || !r.Resolve("OTHER").AllowsUser("bob") {
		t.Fatal("resolver mutated through inputs or resolved lists")
	}
	if p := r.ResolveDirectMessages(); !p.Enabled || p.UserScope != "workspace_members_only" || p.Allowlist == nil || p.AllowsUser("alice") {
		t.Fatal("DM rule or explicit empty allowlist mutated")
	}
}

func TestRouteSurfaceEnablementAndIdentity(t *testing.T) {
	r := resolver(t, ChannelScope{}, AccessRules{DirectMessages: &DirectMessageRule{Enabled: true, Users: &UserRule{Scope: "workspace_members_only"}}})
	route := Route{AppID: "A", WorkspaceID: "T", Resolver: r}
	m := Message{AppID: "A", ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "D"}, Surface: SurfaceDirectMessage}
	if !route.Allows(m) || route.AllowsChannel("T", "D") {
		t.Fatal("DM inherited channel routing")
	}
	for _, changed := range []Message{
		{AppID: "OTHER", ThreadKey: m.ThreadKey, Surface: m.Surface},
		{AppID: "A", ThreadKey: ThreadKey{WorkspaceID: "OTHER", ChannelID: "D"}, Surface: m.Surface},
		{AppID: "A", ThreadKey: m.ThreadKey, Surface: MessageSurface("unsupported")},
	} {
		if route.Allows(changed) {
			t.Fatal("foreign or unsupported surface admitted")
		}
	}
	if (Route{AppID: "A", WorkspaceID: "T"}).Allows(m) {
		t.Fatal("route without a resolver admitted DM")
	}
	route.Resolver = resolver(t, ChannelScope{}, AccessRules{DirectMessages: &DirectMessageRule{Users: &UserRule{Scope: "workspace_members_only"}}})
	if route.Allows(m) {
		t.Fatal("prepared users enabled disabled DM")
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
		{"regular", "workspace_members_only", nil, nil},
		{"guest_channel_members", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Guest = true }, nil},
		{"single_guest_allowed", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.SingleChannelGuest = true }, nil},
		{"guest_members_only", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Guest = true }, ErrDenied},
		{"single_guest", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.SingleChannelGuest = true }, ErrDenied},
		{"deleted", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Deleted = true }, ErrDenied},
		{"bot", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Bot = true }, ErrDenied},
		{"wrong_user", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.ID = "bob" }, ErrDenied},
		{"foreign_workspace", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.WorkspaceID = "OTHER" }, ErrDenied},
		{"deleted_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Deleted = true }, ErrDenied},
		{"bot_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.Bot = true }, ErrDenied},
		{"wrong_user_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.ID = "bob" }, ErrDenied},
		{"foreign_workspace_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.user.WorkspaceID = "OTHER" }, ErrDenied},
		{"not_joined", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Member = false }, ErrDenied},
		{"archived", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Archived = true }, ErrDenied},
		{"external", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.ExternalShared = true }, ErrDenied},
		{"pending_external", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.PendingExternalShared = true }, ErrDenied},
		{"private_default_denied", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.Private = true }, ErrDenied},
		{"dm", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.IM = true }, ErrDenied},
		{"group_dm", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.MPIM = true }, ErrDenied},
		{"channel_identity", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channel.ID = "OTHER" }, ErrDenied},
		{"binding_identity", "workspace_members_and_guests", func(_ *policyDirectory, _ *Message, b *ConversationBinding) { b.OwnerID = "bob" }, ErrDenied},
		{"binding_thread", "workspace_members_and_guests", func(_ *policyDirectory, _ *Message, b *ConversationBinding) { b.RootTS = "OTHER" }, ErrDenied},
		{"raw_external", "workspace_members_and_guests", func(_ *policyDirectory, m *Message, _ *ConversationBinding) { m.ExternalShared = true }, ErrDenied},
		{"metadata_unavailable", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.channelErr = ErrMetadataUnavailable }, ErrMetadataUnavailable},
		{"slack_auth", "workspace_members_only", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.userErr = ErrSlackAuth }, ErrSlackAuth},
		{"user_metadata_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.userErr = ErrMetadataUnavailable }, ErrMetadataUnavailable},
		{"user_auth_in_guest_scope", "workspace_members_and_guests", func(d *policyDirectory, _ *Message, _ *ConversationBinding) { d.userErr = ErrSlackAuth }, ErrSlackAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := resolver(t, ChannelScope{Enabled: true, Scope: "workspace_channels_with_bot"}, AccessRules{Users: &UserRule{Scope: tc.mode}})
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
			if err == nil && d.userCalls != 1 {
				t.Fatal("workspace account was not verified")
			}
			if err == nil {
				if err = p.AuthorizeDelivery(context.Background(), DeliveryContext{Message: m, ConversationBinding: &b}, true); err != nil || !d.fresh {
					t.Fatal("outbound did not force fresh metadata")
				}
			}
		})
	}
	private := resolver(t, ChannelScope{Enabled: true, Scope: "workspace_channels_with_bot", AllowPrivate: true}, AccessRules{Users: &UserRule{Scope: "workspace_members_and_guests"}})
	p := ConfiguredPolicy{Route: Route{AppID: "A", WorkspaceID: "T", Resolver: private}, PrincipalID: "shared", Directory: &policyDirectory{channel: ChannelInfo{WorkspaceID: "T", ID: "C", Private: true, Member: true}, user: UserInfo{WorkspaceID: "T", ID: "alice"}}}
	if _, err := p.Authorize(context.Background(), Message{AppID: "A", ThreadKey: ThreadKey{"T", "C", "1"}, AuthorID: "alice"}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteOverrideRetainsSlackBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		alter  func(*policyDirectory)
		denied bool
	}{
		{name: "private_guest_outside_general_allowlist_and_in_general_denylist"},
		{name: "bot_not_member", alter: func(d *policyDirectory) { d.channel.Member = false }, denied: true},
		{name: "archived", alter: func(d *policyDirectory) { d.channel.Archived = true }, denied: true},
		{name: "external", alter: func(d *policyDirectory) { d.channel.ExternalShared = true }, denied: true},
		{name: "pending_external", alter: func(d *policyDirectory) { d.channel.PendingExternalShared = true }, denied: true},
		{name: "wrong_workspace", alter: func(d *policyDirectory) { d.channel.WorkspaceID = "OTHER" }, denied: true},
		{name: "deleted_author", alter: func(d *policyDirectory) { d.user.Deleted = true }, denied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := resolver(t, ChannelScope{Enabled: true, Scope: "selected", Allowlist: []string{}, Denylist: []string{"C"}}, AccessRules{
				Users:            &UserRule{Scope: "workspace_members_only", Denylist: []string{"alice"}},
				ChannelOverrides: map[string]ChannelRule{"C": {Enabled: true, AllowPrivate: true, Users: &UserRule{Scope: "workspace_members_and_guests"}}},
			})
			d := &policyDirectory{channel: ChannelInfo{WorkspaceID: "T", ID: "C", Private: true, Member: true}, user: UserInfo{WorkspaceID: "T", ID: "alice", Guest: true}}
			if tc.alter != nil {
				tc.alter(d)
			}
			p := ConfiguredPolicy{Route: Route{AppID: "A", WorkspaceID: "T", Resolver: r}, PrincipalID: "shared", Directory: d}
			m := Message{AppID: "A", ThreadKey: ThreadKey{"T", "C", "1"}, AuthorID: "alice", Mentioned: true}
			decision, err := p.Authorize(context.Background(), m, nil)
			if tc.denied && !errors.Is(err, ErrDenied) || !tc.denied && err != nil {
				t.Fatalf("authorization=%v, denied=%t", err, tc.denied)
			}
			if err == nil {
				b := &ConversationBinding{ThreadKey: m.ThreadKey, Decision: decision}
				if err := p.AuthorizeDelivery(context.Background(), DeliveryContext{Message: m, ConversationBinding: b}, true); err != nil || !d.fresh {
					t.Fatalf("override outbound authorization=%v, fresh=%t", err, d.fresh)
				}
			}
		})
	}
}
