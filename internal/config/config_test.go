package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSlackTokenReferencesAndSmokeValidation(t *testing.T) {
	c := Config{Storage: Storage{Driver: "sqlite", Path: "slack-smoke.sqlite"}, Transport: "socket", AccessMode: "shared_channel"}
	c.Slack.AppID = "A"
	c.Slack.WorkspaceID = "T"
	c.Slack.ChannelID = "C"
	if err := c.ValidateSlack(); err != nil {
		t.Fatal(err)
	}
	if c.ValidateRun() == nil {
		t.Fatal("production accepted missing backend")
	}
	t.Setenv("SLACK_APP_TOKEN", "xapp-secret-marker")
	t.Setenv("SLACK_BOT_TOKEN", "xoxb-secret-marker")
	tokens, err := c.LoadSlackTokens()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", tokens, tokens), "secret-marker") {
		t.Fatal("credential formatting leaks secrets")
	}
	c.Slack.AppTokenEnv = "CUSTOM_APP"
	c.Slack.BotTokenEnv = "CUSTOM_BOT"
	t.Setenv("CUSTOM_APP", "xapp-custom")
	t.Setenv("CUSTOM_BOT", "xoxb-custom")
	tokens, err = c.LoadSlackTokens()
	if err != nil || tokens.App != "xapp-custom" {
		t.Fatal("custom reference ignored")
	}
	c.Slack.AppTokenEnv = "xapp-secret-marker"
	if err = c.ValidateSlack(); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("raw token accepted as env name or leaked")
	}
	c.Slack.AppTokenEnv = "MISSING"
	t.Setenv("MISSING", "")
	if _, err = c.LoadSlackTokens(); err == nil {
		t.Fatal("missing token accepted")
	}
}

func TestStorageRequiresExplicitSupportedDriver(t *testing.T) {
	for _, tc := range []struct {
		name    string
		storage Storage
		valid   bool
	}{
		{"missing", Storage{Path: "gateway.sqlite"}, false},
		{"sqlite", Storage{Driver: "sqlite", Path: "gateway.sqlite"}, true},
		{"path_missing", Storage{Driver: "sqlite"}, false},
		{"memory", Storage{Driver: "sqlite", Path: ":memory:"}, false},
		{"uri", Storage{Driver: "sqlite", Path: "file:test?mode=memory"}, false},
		{"mysql_no_fallback", Storage{Driver: "mysql", Path: "fallback.sqlite", DSNEnv: "MYSQL_DSN"}, false},
		{"mysql", Storage{Driver: "mysql", DSNEnv: "MYSQL_DSN"}, true},
		{"mysql_missing_reference", Storage{Driver: "mysql"}, false},
		{"mysql_raw_dsn", Storage{Driver: "mysql", DSNEnv: "user:secret-marker@tcp(localhost)/database"}, false},
		{"unknown", Storage{Driver: "other", Path: "gateway.sqlite"}, false},
		{"mixed", Storage{Driver: "sqlite", Path: "gateway.sqlite", DSNEnv: "MYSQL_DSN"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (Config{Storage: tc.storage}).ValidateStorage()
			if (err == nil) != tc.valid {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}

func TestMySQLDSNIsLoadedOnlyOnDemand(t *testing.T) {
	c := Config{Storage: Storage{Driver: "mysql", DSNEnv: "TEST_MYSQL_DSN"}}
	t.Setenv("TEST_MYSQL_DSN", "")
	if err := c.ValidateStorage(); err != nil {
		t.Fatalf("offline validation read a secret: %v", err)
	}
	if _, err := c.MySQLDSN(); err == nil {
		t.Fatal("missing DSN accepted for storage open")
	}
	dsn := "test:secret-marker@tcp(127.0.0.1:3309)/agently_steward_slack?parseTime=true"
	t.Setenv("TEST_MYSQL_DSN", dsn)
	actual, err := c.MySQLDSN()
	if err != nil || actual != dsn {
		t.Fatal("DSN value was changed or rejected")
	}
	c.Storage.DSNEnv = dsn
	if _, err = c.MySQLDSN(); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("invalid reference accepted or exposed")
	}
}

func TestLoadStrictAndConfigRelativePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("storage:\n  driver: sqlite\n  path: data.sqlite\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Storage.Path != filepath.Join(dir, "data.sqlite") {
		t.Fatal("relative path not anchored to config")
	}
	for _, body := range []string{"unknown: secret-marker\n", "storage: [secret-marker]\n", "storage: {}\n---\nextra: secret-marker\n", ""} {
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path)
		if err == nil {
			t.Fatal("invalid config accepted")
		}
		if strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("config error exposed a value")
		}
	}
}

func TestRunConfiguration(t *testing.T) {
	c := Config{Storage: Storage{Driver: "sqlite", Path: "gateway.sqlite"}, Transport: "socket", AccessMode: "shared_channel"}
	c.Slack.AppID = "A"
	c.Slack.WorkspaceID = "T"
	c.Slack.ChannelID = "C"
	c.Agently.AgentID = "steward"
	c.Agently.PrincipalID = "shared"
	c.Agently.BaseURL = "http://127.0.0.1:9191"
	c.Agently.Auth.Mode = "oob_session"
	if err := c.ValidateRun(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"http_future", func(c *Config) { c.Transport = "http" }},
		{"per_user_future", func(c *Config) { c.AccessMode = "per_user" }},
		{"missing_owner", func(c *Config) { c.Agently.PrincipalID = "" }},
		{"missing_auth", func(c *Config) { c.Agently.Auth.Mode = "" }},
		{"insecure_remote", func(c *Config) { c.Agently.BaseURL = "http://example.invalid" }},
		{"smoke_database", func(c *Config) { c.Storage.Path = "slack-smoke.sqlite" }},
		{"invalid_secret_reference", func(c *Config) { c.Agently.Auth.SecretsURLEnv = "secret-marker!" }},
		{"missing_host", func(c *Config) { c.Agently.BaseURL = "http://:9191" }},
		{"url_credentials", func(c *Config) { c.Agently.BaseURL = "https://user:secret-marker@example.invalid" }},
		{"url_query", func(c *Config) { c.Agently.BaseURL = "https://example.invalid?token=secret-marker" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := c
			tc.change(&changed)
			err := changed.ValidateRun()
			if err == nil {
				t.Fatal("invalid run config accepted")
			}
			if strings.Contains(err.Error(), "secret-marker") {
				t.Fatal("secret exposed")
			}
		})
	}
}

func TestWhitespacePathIsNotConvertedToValidAbsolutePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("storage:\n  driver: sqlite\n  path: '   '\n"), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateStorage(); err == nil {
		t.Fatal("whitespace-only path accepted")
	}
}

func TestStatusRefreshIntervalSeconds(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		seconds     int64
	}{
		{"omitted", "", 30}, {"one", "1", 1}, {"configured", "75", 75}, {"largest_duration", "9223372036", 9223372036},
		{"zero", "0", 0}, {"negative", "-1", 0}, {"fraction", "1.5", 0}, {"float_integer", "30.0", 0},
		{"duration_string", "30s", 0}, {"quoted", "\"30\"", 0}, {"boolean", "true", 0}, {"null", "null", 0},
		{"duration_overflow", "9223372037", 0}, {"integer_overflow", "9223372036854775808", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "storage:\n  driver: sqlite\n  path: test.sqlite\ntransport: socket\naccess_mode: shared_channel\nslack:\n  app_id: A\n  workspace_id: T\n  channel_id: C\n"
			if tc.value != "" {
				body += "  status:\n    refresh_interval_sec: " + tc.value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if err == nil {
				err = c.ValidateSlack()
			}
			if tc.seconds == 0 {
				if err == nil {
					t.Fatal("invalid interval accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.StatusRefreshInterval() != time.Duration(tc.seconds)*time.Second {
				t.Fatal("wrong seconds/default")
			}
		})
	}
	for _, key := range []string{"refresh_interval", "streaming"} {
		path := filepath.Join(t.TempDir(), "bad.yaml")
		if err := os.WriteFile(path, []byte("slack:\n  status:\n    "+key+": 30\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatal("unapproved option accepted")
		}
	}
}

func TestStatusPresentationModes(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{"", "message"}, {"message", "message"}, {"agent_session", "agent_session"}, {"hybrid", "hybrid"},
		{"unknown", ""}, {"null", ""}, {"''", ""}, {"true", ""}, {"42", ""},
	} {
		t.Run(tc.value, func(t *testing.T) {
			body := "storage:\n  driver: sqlite\n  path: test.sqlite\ntransport: socket\naccess_mode: shared_channel\nslack:\n  app_id: A\n  workspace_id: T\n  channel_id: C\n  status:\n    refresh_interval_sec: 15\n"
			if tc.value != "" {
				body += "    mode: " + tc.value + "\n"
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if err == nil {
				err = c.ValidateSlack()
			}
			if tc.want == "" {
				if err == nil {
					t.Fatal("invalid mode accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(c.StatusMode()) != tc.want || c.StatusRefreshInterval() != 15*time.Second {
				t.Fatal("wrong mode or interval")
			}
		})
	}
}
