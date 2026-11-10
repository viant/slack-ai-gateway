package config

import (
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestDirectMessagePolicyIsExplicitAndIndependent(t *testing.T) {
	base := "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C]}}\n"
	for _, tc := range []struct {
		name, access string
		allowed      bool
	}{
		{"absent", "defaults: {users: {mode: channel_members}}", false},
		{"enabled_without_channel_defaults", "direct_messages: {users: {mode: channel_members}}", true},
		{"channel_lists_do_not_apply", "defaults: {users: {mode: members_only, allowlist: []}}, direct_messages: {users: {mode: channel_members}}", true},
		{"empty_allowlist", "direct_messages: {users: {mode: channel_members, allowlist: []}}", false},
		{"local_allowlist", "direct_messages: {users: {mode: channel_members, allowlist: [alice]}}", true},
		{"global_intersection", "global: {users: {allowlist: [bob]}}, direct_messages: {users: {mode: channel_members, allowlist: [alice]}}", false},
		{"global_deny", "global: {users: {denylist: [alice]}}, direct_messages: {users: {mode: channel_members}}", false},
		{"local_deny", "direct_messages: {users: {mode: channel_members, denylist: [alice]}}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadPolicyYAML(t, base+"access: {"+tc.access+"}\n")
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.Route()
			if err != nil {
				t.Fatal(err)
			}
			if p := r.Resolver.ResolveDirectMessages(); p.AllowsUser("alice") != tc.allowed {
				t.Fatalf("unexpected DM policy: %+v", p)
			}
			m := gateway.Message{AppID: "A", Surface: gateway.SurfaceDirectMessage, ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "D"}}
			if r.Allows(m) != (tc.name != "absent") {
				t.Fatal("DM route inherited channel configuration")
			}
		})
	}
	for _, body := range []string{
		"defaults: {users: {mode: channel_members}}, direct_messages: {users: {allowlist: [alice]}}",
		"direct_messages: null", "direct_messages: {}", "direct_messages: {users: null}",
		"direct_messages: {users: {mode: null}}", "direct_messages: {users: {mode: channel_members, allowlist: null}}",
		"direct_messages: {users: {mode: channel_members, allow_list: [alice]}}",
	} {
		c, err := loadPolicyYAML(t, base+"access: {"+body+"}\n")
		if err == nil {
			err = c.ValidateSlack()
		}
		if err == nil {
			t.Fatalf("invalid DM policy accepted: %s", body)
		}
	}
}
