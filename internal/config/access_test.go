package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func loadPolicyYAML(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("storage: {driver: sqlite, path: gateway.sqlite}\ntransport: socket\naccess_mode: shared_channel\n"+body), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err == nil {
		configureOfflineSecrets(&c)
	}
	return c, err
}

func TestAccessYAMLPresenceAndLegacyCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		allowed    bool
		empty      bool
	}{
		{"legacy", "slack: {app_id: A, workspace_id: T, channel_id: C}\n", true, false},
		{"legacy_explicit_empty_access", "slack: {app_id: A, workspace_id: T, channel_id: C}\naccess: {}\n", false, false},
		{"new_without_policy", "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C]}}\n", false, false},
		{"new_defaults", "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C]}}\naccess: {defaults: {users: {mode: channel_members}}}\n", true, false},
		{"explicit_empty_users", "slack: {app_id: A, workspace_id: T, channels: {mode: workspace}}\naccess: {defaults: {users: {mode: channel_members, allowlist: []}}}\n", false, true},
		{"global_alone", "slack: {app_id: A, workspace_id: T, channels: {mode: workspace}}\naccess: {global: {users: {allowlist: [alice]}}}\n", false, false},
		{"channel_only", "slack: {app_id: A, workspace_id: T, channels: {mode: workspace}}\naccess: {channel_overrides: {C: {users: {mode: channel_members}}}}\n", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadPolicyYAML(t, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.ValidateSlack(); err != nil {
				t.Fatal(err)
			}
			r, err := c.Route()
			if err != nil {
				t.Fatal(err)
			}
			p := r.Resolver.Resolve("C")
			if p.AllowsUser("alice") != tc.allowed {
				t.Fatalf("policy: %+v", p)
			}
			if tc.empty && p.Allowlist == nil {
				t.Fatal("[] lost")
			}
			if tc.name == "legacy" && !p.AllowPrivate {
				t.Fatal("legacy private route changed")
			}
		})
	}
}

func TestSecurityYAMLRejectsNullUnknownAliasesAndInvalidRules(t *testing.T) {
	base := "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C]}}\n"
	for _, body := range []string{
		base + "access: null\n",
		base + "access: {defaults: null}\n",
		base + "access: {defaults: {users: null}}\n",
		base + "access: {defaults: {users: {mode: null}}}\n",
		base + "access: {defaults: {users: {mode: channel_members, allowlist: null}}}\n",
		base + "access: {defaults: {users: {mode: channel_members, denylist: null}}}\n",
		base + "access: {global: {users: {allowlist: null}}}\n",
		base + "access: {channel_overrides: {C: null}}\n",
		base + "access: {defaults: {users: {mode: channel_members, allow_list: [secret-marker]}}}\n",
		base + "access: {defaults: {users: {mode: channel_members, allowlist: [secret-marker, secret-marker]}}}\n",
		base + "access: {channel_overrides: {C: {users: {allowlist: [alice]}}}}\n",
		base + "access: {channel_overrides: {OTHER: {users: {mode: channel_members}}}}\n",
		base + "access: {defaults: {users: {mode: members_only}}, channel_overrides: {C: {users: {}}}}\n",
		"slack: {app_id: A, workspace_id: T, channel_id: C, channels: {mode: workspace}}\n",
		"slack: {app_id: A, workspace_id: T, channels: null}\n",
		"slack: {app_id: A, workspace_id: T, channels: {mode: workspace, allowlist: []}}\n",
		"slack: {app_id: A, workspace_id: T, channels: {mode: selected}}\n",
		"slack: {app_id: A, workspace_id: T, channels: {mode: workspace, allow_private: null}}\n",
		"slack: {app_id: A, workspace_id: T, channels: {mode: workspace, denylist: [null]}}\n",
		"access: &policy {defaults: {users: {mode: channel_members}}}\nslack: {app_id: A, workspace_id: T, channels: *policy}\n",
		"slack: &slack {app_id: A, workspace_id: T, channels: {mode: workspace}}\naccess: {<<: *slack}\n",
		"slack: &slack {app_id: A, workspace_id: T, channels: null}\n<<: *slack\n",
	} {
		c, err := loadPolicyYAML(t, body)
		if err == nil {
			err = c.ValidateSlack()
		}
		if err == nil {
			t.Fatalf("invalid config accepted: %s", body)
		}
		if strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("configuration value leaked")
		}
	}
}

func TestDocumentedAccessExamplesValidate(t *testing.T) {
	data, err := os.ReadFile("../../docs/access-policy.md")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(string(data), "```yaml\n")
	if len(parts) != 5 {
		t.Fatal("expected four access examples")
	}
	for _, part := range parts[1:] {
		body, _, ok := strings.Cut(part, "```")
		if !ok {
			t.Fatal("unclosed example")
		}
		c, err := loadPolicyYAML(t, body)
		if err != nil {
			t.Fatal(err)
		}
		if err = c.ValidateSlack(); err != nil {
			t.Fatal(err)
		}
	}
	c, err := Load("../../examples/gateway.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateRun(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"../../internal/integration/testdata/channels.yaml", "../../internal/integration/testdata/direct-messages.yaml"} {
		t.Run(path, func(t *testing.T) {
			smoke, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = smoke.ValidateSlack(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
