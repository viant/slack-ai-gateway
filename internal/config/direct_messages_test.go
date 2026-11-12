package config

import (
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"testing"
)

func TestDirectMessagePolicyIsExplicitAndIndependent(t *testing.T) {
	for _, tc := range []struct {
		name, access   string
		route, allowed bool
	}{
		{"absent", "channels: {enabled: false}", false, false},
		{"disabled", "direct_messages: {enabled: false}", false, false},
		{"enabled", "direct_messages: {enabled: true, users: {scope: workspace_members_only}}", true, true},
		{"channel_lists_do_not_apply", "channels: {enabled: true, scope: selected, allowlist: [], users: {scope: workspace_members_only, allowlist: []}}, direct_messages: {enabled: true, users: {scope: workspace_members_and_guests}}", true, true},
		{"empty_allowlist", "direct_messages: {enabled: true, users: {scope: workspace_members_only, allowlist: []}}", true, false},
		{"local_allowlist", "direct_messages: {enabled: true, users: {scope: workspace_members_only, allowlist: [alice]}}", true, true},
		{"local_deny", "direct_messages: {enabled: true, users: {scope: workspace_members_only, allowlist: [alice], denylist: [alice]}}", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadPolicyYAML(t, "access: {"+tc.access+"}")
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.Route()
			if err != nil {
				t.Fatal(err)
			}
			p := r.Resolver.ResolveDirectMessages()
			if p.AllowsUser("alice") != tc.allowed {
				t.Fatalf("DM policy: %+v", p)
			}
			m := gateway.Message{AppID: "A", Surface: gateway.SurfaceDirectMessage, ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "D"}}
			if r.Allows(m) != tc.route {
				t.Fatal("DM route differs from explicit enabled flag")
			}
		})
	}
	for _, body := range []string{
		"direct_messages: null", "direct_messages: {}", "direct_messages: {enabled: true}",
		"direct_messages: {enabled: true, users: null}", "direct_messages: {enabled: false, users: {}}",
		"direct_messages: {enabled: true, users: {scope: null}}",
		"direct_messages: {enabled: true, users: {scope: workspace_members_only, allowlist: null}}",
		"direct_messages: {enabled: true, users: {scope: workspace_members_only, allow_list: [alice]}}",
	} {
		if _, err := loadPolicyYAML(t, "access: {"+body+"}"); err == nil {
			t.Fatalf("invalid DM policy accepted: %s", body)
		}
	}
}
