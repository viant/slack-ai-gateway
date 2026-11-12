package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Assemble integration fragments into the only supported public YAML format.
// The production loader is used for every example, including list presence.
func testIntegrationYAML(body string) string {
	return "storage: {driver: sqlite, path: gateway.sqlite}\nintegrations:\n  - name: steward\n    transport: socket\n    backend:\n      type: agently\n      base_url: http://127.0.0.1:9191\n      agent_id: steward\n      principal_id: shared\n      auth: {mode: oob_session, secret: {URL: /unused/oob.json, Key: 'blowfish://default'}}\n" + "    " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n    ") + "\n"
}

func loadRuntimeConfig(path string) (Config, error) {
	c, err := Load(path)
	if err != nil {
		return Config{}, err
	}
	resolved, err := c.Topology()
	if err != nil {
		return Config{}, err
	}
	return resolved[0].Config, nil
}

func loadPolicyYAML(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	body = "slack:\n  app_id: A\n  workspace_id: T\n  app_token_secret: {URL: /unused/app-token.enc, Key: 'blowfish://default'}\n  bot_token_secret: {URL: /unused/bot-token.enc, Key: 'blowfish://default'}\n" + body
	if err := os.WriteFile(path, []byte(testIntegrationYAML(body)), 0600); err != nil {
		t.Fatal(err)
	}
	return loadRuntimeConfig(path)
}

func TestAccessSurfaceAndListPresence(t *testing.T) {
	for _, tc := range []struct {
		name, body     string
		allowed, empty bool
	}{
		{"omitted", "", false, false},
		{"empty_access", "access: {}", false, false},
		{"disabled", "access: {channels: {enabled: false}}", false, false},
		{"general", "access: {channels: {enabled: true, scope: selected, allowlist: [C], users: {scope: workspace_members_only}}}", true, false},
		{"empty_users", "access: {channels: {enabled: true, scope: workspace_channels_with_bot, users: {scope: workspace_members_only, allowlist: []}}}", false, true},
		{"empty_channels", "access: {channels: {enabled: true, scope: selected, allowlist: [], users: {scope: workspace_members_only}}}", false, false},
		{"override_only", "access: {channels: {enabled: true, scope: selected, allowlist: [], users: {scope: workspace_members_only}, channel_overrides: {C: {enabled: true, users: {scope: workspace_members_and_guests}}}}}", true, false},
		{"override_bypasses_channel_lists", "access: {channels: {enabled: true, scope: selected, allowlist: [OTHER], denylist: [C], users: {scope: workspace_members_only, allowlist: []}, channel_overrides: {C: {enabled: true, users: {scope: workspace_members_and_guests}}}}}", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := loadPolicyYAML(t, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			route, err := c.Route()
			if err != nil {
				t.Fatal(err)
			}
			p := route.Resolver.Resolve("C")
			if p.AllowsUser("alice") != tc.allowed {
				t.Fatalf("policy: %+v", p)
			}
			if tc.empty && p.Allowlist == nil {
				t.Fatal("explicit [] was lost")
			}
		})
	}
}

func TestSecurityYAMLRejectsNullUnknownAliasesAndInvalidRules(t *testing.T) {
	for _, body := range []string{
		"access: null", "access: {channels: null}", "access: {channels: {}}",
		"access: {channels: {enabled: null}}", "access: {channels: {enabled: true}}",
		"access: {channels: {enabled: true, scope: selected, users: {scope: workspace_members_only}}}",
		"access: {channels: {enabled: true, scope: workspace, users: {scope: workspace_members_only}}}",
		"access: {channels: {enabled: false, users: {}}}",
		"access: {channels: {enabled: false, users: {scope: members_only}}}",
		"access: {channels: {enabled: false, users: {scope: workspace_members_only, allowlist: null}}}",
		"access: {channels: {enabled: false, allow_private: null}}",
		"access: {channels: {enabled: false, denylist: [null]}}",
		"access: {channels: {enabled: false, channel_overrides: {C: {}}}}",
		"access: {channels: {enabled: false, channel_overrides: {C: {enabled: true}}}}",
		"access: {channels: {enabled: false, channel_overrides: {C: null}}}",
		"access: {channels: {enabled: false, users: {scope: workspace_members_only, allow_list: [secret-marker]}}}",
		"access: {channels: {enabled: false, users: {scope: workspace_members_only, allowlist: [secret-marker, secret-marker]}}}",
		"access: {global: {users: {allowlist: [alice]}}}",
		"access: {defaults: {users: {mode: channel_members}}}",
		"access: {channel_overrides: {C: {users: {scope: workspace_members_only}}}}",
		"access: &p {channels: {enabled: false}}\nunused: *p",
		"access: {channels: &p {enabled: false}, direct_messages: {<<: *p}}",
	} {
		_, err := loadPolicyYAML(t, body)
		if err == nil {
			t.Fatalf("invalid config accepted: %s", body)
		}
		if strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("configuration value leaked")
		}
	}
}

func TestDocumentedAccessExamplesValidate(t *testing.T) {
	paths, err := filepath.Glob("../../examples/access/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 10 {
		t.Fatal("expected full access example and nine standalone variants")
	}
	paths = append(paths, "../../examples/gateway.example.yaml", "../../examples/gateway.multi.example.yaml")
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.ValidateRun(); err != nil {
				t.Fatal(err)
			}
		})
	}
	// Fragments in the guide use the same full native loader and validation.
	data, err := os.ReadFile("../../docs/access-policy.md")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, part := range strings.Split(string(data), "```yaml\n")[1:] {
		body, _, ok := strings.Cut(part, "```")
		if !ok {
			t.Fatal("unclosed example")
		}
		if strings.HasPrefix(strings.TrimSpace(body), "access:") {
			if _, err = loadPolicyYAML(t, body); err != nil {
				t.Fatal(err)
			}
		} else {
			path := filepath.Join(t.TempDir(), "documented.yaml")
			if err = os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = c.ValidateRun(); err != nil {
				t.Fatal(err)
			}
		}
		count++
	}
	if count < 10 {
		t.Fatal("expected full example and nine access fragments")
	}
}

func TestObsoleteAccessKeysGiveRedactedMigrationGuidance(t *testing.T) {
	for _, body := range []string{
		"access: {global: {users: {allowlist: [secret-marker]}}}",
		"access: {defaults: {users: {mode: channel_members}}}",
		"access: {channel_overrides: {secret-marker: {users: {mode: members_only}}}}",
		"access: {channels: {enabled: true, scope: selected, allowlist: [], users: {mode: members_only}}}",
		"access: {channels: {enabled: false, channel_overrides: {C: {enabled: false, users: {mode: members_only}}}}}",
		"access: {direct_messages: {enabled: true, users: {mode: channel_members}}}",
	} {
		_, err := loadPolicyYAML(t, body)
		if err == nil || !strings.Contains(err.Error(), "obsolete access configuration; use integrations[].access") {
			t.Fatalf("missing obsolete configuration guidance: %v", err)
		}
		if strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("legacy configuration value leaked")
		}
	}
}
