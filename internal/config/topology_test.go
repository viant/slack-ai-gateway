package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func topologyExample(t *testing.T) Config {
	t.Helper()
	c, err := Load(filepath.Join("..", "..", "examples", "gateway.multi.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTopologyResolvesMultipleInstallationsAndBackendsOffline(t *testing.T) {
	c := topologyExample(t)
	resolved, err := c.Topology()
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved) != 2 || resolved[0].Name != "steward" || resolved[0].Backend.AgentID != "steward" || resolved[1].Name != "support" || resolved[1].Backend.Type != "agui" || resolved[1].Config.Agently.AgentID != "" {
		t.Fatalf("incorrect topology: %#v", resolved)
	}
	if err = c.ValidateRun(); err != nil {
		t.Fatal(err)
	}
	if resolved[0].Config.Slack.AppID == resolved[1].Config.Slack.AppID || resolved[0].Config.Agently.PrincipalID == resolved[1].Config.Agently.PrincipalID {
		t.Fatal("Slack or execution identities were combined")
	}
	c.Integrations[1].Backend = c.Integrations[0].Backend
	c.Integrations[1].Backend.AgentID = "another-agent"
	resolved, err = c.Topology()
	if err != nil || resolved[0].Backend.BaseURL != resolved[1].Backend.BaseURL || resolved[0].Backend.AgentID == resolved[1].Backend.AgentID {
		t.Fatalf("shared backend: %v", err)
	}
}

func TestTopologyRejectsAmbiguousAndUnresolvedRoutes(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"missing_name", func(c *Config) { c.Integrations[0].Name = "" }},
		{"missing_backend", func(c *Config) { c.Integrations[0].Backend = Backend{} }},
		{"missing_slack", func(c *Config) { c.Integrations[0].Slack = SlackSettings{} }},
		{"duplicate_integration", func(c *Config) { c.Integrations[1].Name = c.Integrations[0].Name }},
		{"shared_bot_not_active_yet", func(c *Config) { c.Integrations[1].Slack = c.Integrations[0].Slack }},
		{"socket_app_partition", func(c *Config) { c.Integrations[1].Slack.AppID = c.Integrations[0].Slack.AppID }},
		{"backend_type", func(c *Config) { c.Integrations[0].Backend.Type = "unknown" }},
		{"generic_auth_omitted", func(c *Config) { c.Integrations[1].Backend.Auth.Mode = "" }},
		{"missing_native_agent", func(c *Config) { c.Integrations[0].Backend.AgentID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := topologyExample(t)
			tc.edit(&c)
			if _, err := c.Topology(); err == nil {
				t.Fatal("ambiguous or invalid topology accepted")
			}
		})
	}
}

func TestTopologyLoaderPreservesNativeStrictSecurityAndStatusPresence(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "examples", "gateway.multi.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, from, to string }{
		{"null_access", "    access:\n      channels:", "    access: null\n    unused_access:\n      channels:"},
		{"null_slack", "    slack:\n      app_id:", "    slack: null\n    unused_slack:\n      app_id:"},
		{"status_seconds_string", "refresh_interval_sec: 30", "refresh_interval_sec: '30'"},
		{"null_mode", "mode: hybrid", "mode: null"},
		{"unknown_nested_field", "principal_id: support-service", "principal_id: support-service\n      unknown: true"},
		{"null_topology", "integrations:\n", "integrations: null\nunused_integrations:\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "gateway.yaml")
			if err := os.WriteFile(path, []byte(strings.Replace(string(body), tc.from, tc.to, 1)), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("invalid mapping accepted")
			}
		})
	}
}

func TestExplicitEmptyIntegrationsAreDifferentFromAbsentAndNull(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"empty", "integrations: []\n", true},
		{"absent", "", false},
		{"null", "integrations: null\n", false},
		{"obsolete_dictionary", "slack_installations: []\n", false},
		{"empty_mixed_with_legacy", "integrations: []\ntransport: socket\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "gateway.yaml")
			body := "storage: {driver: sqlite, path: gateway.sqlite}\n" + tc.body
			if err := os.WriteFile(p, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if err == nil {
				var resolved []ResolvedIntegration
				resolved, err = c.Topology()
				if err == nil && len(resolved) != 0 {
					t.Fatalf("expected no active integrations: %#v", resolved)
				}
			}
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
		})
	}
}

func TestLoaderRejectsAllLegacyConfigurationFields(t *testing.T) {
	for _, body := range []string{
		"transport: socket\n", "access_mode: shared_channel\n", "slack: {app_id: A}\n",
		"agently: {base_url: http://127.0.0.1:9191}\n", "access: {}\n",
		"integrations: [{name: steward, access_mode: shared_channel}]\n",
		"integrations: [{name: steward, slack: {channel_id: C}}]\n",
		"integrations: [{name: steward, slack: {channels: {mode: selected, allowlist: [C]}}}]\n",
	} {
		path := filepath.Join(t.TempDir(), "legacy.yaml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}

		_, err := Load(path)
		if err == nil {
			t.Fatalf("legacy configuration accepted: %s", body)
		}
		if !strings.Contains(err.Error(), "obsolete access configuration; use integrations[].access") {
			t.Fatalf("migration guidance missing: %v", err)
		}
	}
}
