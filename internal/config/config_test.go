package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viant/scy"
)

func syntheticTokenResource(t *testing.T, value string) *scy.Resource {
	t.Helper()
	resource := &scy.Resource{URL: "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "token.enc")), Key: "blowfish://default"}
	if err := scy.New().Store(context.Background(), scy.NewSecret(value, resource)); err != nil {
		t.Fatal("cannot prepare synthetic token")
	}
	return resource
}

func configureOfflineSecrets(c *Config) {
	c.Slack.AppTokenSecret = &scy.Resource{URL: "/unused/app-token.enc", Key: "blowfish://default"}
	c.Slack.BotTokenSecret = &scy.Resource{URL: "/unused/bot-token.enc", Key: "blowfish://default"}
	c.Agently.Auth.Secret = &scy.Resource{URL: "/unused/oob.json", Key: "blowfish://default"}
}

func TestSlackTokenResourcesAndSmokeValidation(t *testing.T) {
	c := Config{Storage: Storage{Driver: "sqlite", Path: "slack-smoke.sqlite"}, Transport: "socket", AccessMode: "shared_channel"}
	c.Slack.AppID, c.Slack.WorkspaceID, c.Slack.ChannelID = "A", "T", "C"
	configureOfflineSecrets(&c)
	if err := c.ValidateSlack(); err != nil {
		t.Fatal(err)
	}
	if c.ValidateRun() == nil {
		t.Fatal("production accepted missing backend")
	}
	t.Setenv("SLACK_APP_TOKEN", "xapp-old-env-marker")
	t.Setenv("SLACK_BOT_TOKEN", "xoxb-old-env-marker")
	c.Slack.AppTokenSecret = syntheticTokenResource(t, "xapp-secret-marker")
	c.Slack.BotTokenSecret = syntheticTokenResource(t, "xoxb-secret-marker")
	tokens, err := c.LoadSlackTokens(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tokens.App != "xapp-secret-marker" || tokens.Bot != "xoxb-secret-marker" {
		t.Fatal("resource values were not used")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", tokens, tokens), "secret-marker") {
		t.Fatal("credential formatting leaks secrets")
	}
	c.Slack.AppTokenSecret = nil
	if _, err = c.LoadSlackTokens(context.Background()); err == nil {
		t.Fatal("missing resource fell back to environment")
	}
	c.Slack.AppTokenSecret = syntheticTokenResource(t, "invalid-token-secret-marker")
	if _, err = c.LoadSlackTokens(context.Background()); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("invalid token accepted or exposed")
	}
}

func TestSlackTokenResourceFailuresAreRedacted(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprint(broken), func(t *testing.T) {
			c := Config{Storage: Storage{Driver: "sqlite", Path: "gateway.sqlite"}, Transport: "socket", AccessMode: "shared_channel"}
			c.Slack.AppID, c.Slack.WorkspaceID, c.Slack.ChannelID = "A", "T", "C"
			configureOfflineSecrets(&c)
			location := filepath.Join(t.TempDir(), "sensitive-location-marker.enc")
			c.Slack.AppTokenSecret = &scy.Resource{URL: "file://" + filepath.ToSlash(location), Key: "blowfish://default"}
			if broken {
				if err := os.WriteFile(location, []byte("bad"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := c.LoadSlackTokens(context.Background())
			if err == nil || strings.Contains(err.Error(), "sensitive-location-marker") || strings.Contains(err.Error(), "blowfish") {
				t.Fatal("secret resource failure missing or exposed")
			}
		})
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
		{"mysql_no_fallback", Storage{Driver: "mysql", Path: "fallback.sqlite", DSN: "test:password@tcp(localhost)/database"}, false},
		{"mysql_plain", Storage{Driver: "mysql", DSN: "test:password@tcp(localhost)/database"}, true},
		{"mysql_missing_dsn", Storage{Driver: "mysql"}, false},
		{"mysql_template", Storage{Driver: "mysql", DSN: "${Username}:${Password}@tcp(localhost)/database", Secret: &scy.Resource{URL: "/unused/mysql.json", Key: "blowfish://default"}}, true},
		{"mysql_invalid_secret", Storage{Driver: "mysql", DSN: "${Username}:${Password}@tcp(localhost)/database", Secret: &scy.Resource{}}, false},
		{"unknown", Storage{Driver: "other", Path: "gateway.sqlite"}, false},
		{"mixed_dsn", Storage{Driver: "sqlite", Path: "gateway.sqlite", DSN: "test:password@tcp(localhost)/database"}, false},
		{"mixed_secret", Storage{Driver: "sqlite", Path: "gateway.sqlite", Secret: &scy.Resource{URL: "/unused/mysql.json"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (Config{Storage: tc.storage}).ValidateStorage()
			if (err == nil) != tc.valid {
				t.Fatalf("validation: %v", err)
			}
		})
	}
}

func TestMySQLValidationDoesNotLoadSecretOrUseEnvironment(t *testing.T) {
	t.Setenv("AGENTLY_SLACK_MYSQL_DSN", "old-env-secret-marker")
	c := Config{Storage: Storage{Driver: "mysql", DSN: "${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true", Secret: &scy.Resource{URL: "aws://secretmanager/us-east-1/secret/synthetic-unavailable", Key: "blowfish://default"}}}
	if err := c.ValidateStorage(); err != nil {
		t.Fatalf("offline validation accessed secret: %v", err)
	}
	if strings.Contains(c.Storage.DSN, "old-env-secret-marker") {
		t.Fatal("environment replaced configured DSN")
	}
	c.Storage.DSN = ""
	if c.ValidateStorage() == nil {
		t.Fatal("missing DSN fell back to environment")
	}
}

func TestLegacySecretSourceFieldsAreRejected(t *testing.T) {
	for _, body := range []string{
		"slack: {app_token_env: SLACK_APP_TOKEN}\n",
		"slack: {bot_token_env: SLACK_BOT_TOKEN}\n",
		"agently: {auth: {secrets_url_env: AGENTLY_OOB_SECRETS}}\n",
		"storage: {driver: mysql, dsn_env: AGENTLY_SLACK_MYSQL_DSN}\n",
	} {
		path := filepath.Join(t.TempDir(), "legacy.yaml")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil {
			t.Fatal("legacy secret source accepted")
		}
	}
}

func TestSecretResourceYAMLIsOfflineAndPreserved(t *testing.T) {
	body := `storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(mysql.invalid:3306)/gateway?parseTime=true'
  secret:
    URL: aws://secretmanager/us-east-1/secret/synthetic-mysql
    Key: blowfish://default
transport: socket
access_mode: shared_channel
slack:
  app_id: A
  workspace_id: T
  channel_id: C
  app_token_secret:
    URL: aws://secretmanager/us-east-1/secret/synthetic-app
    Key: blowfish://default
  bot_token_secret:
    URL: aws://secretmanager/us-east-1/secret/synthetic-bot
    Key: blowfish://default
agently:
  base_url: https://agently.invalid
  agent_id: example
  principal_id: example-user
  auth:
    mode: oob_session
    secret:
      URL: aws://secretmanager/us-east-1/secret/synthetic-oob
      Key: blowfish://default
`
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.ValidateRun(); err != nil {
		t.Fatal(err)
	}
	if c.Storage.DSN != "${Username}:${Password}@tcp(mysql.invalid:3306)/gateway?parseTime=true" {
		t.Fatal("DSN template was changed")
	}
	for _, resource := range []*scy.Resource{c.Storage.Secret, c.Slack.AppTokenSecret, c.Slack.BotTokenSecret, c.Agently.Auth.Secret} {
		if resource == nil || !strings.HasPrefix(resource.URL, "aws://secretmanager/") || resource.Key != "blowfish://default" {
			t.Fatal("native secret configuration was not preserved")
		}
	}
	files, err := os.ReadDir(filepath.Dir(path))
	if err != nil || len(files) != 1 {
		t.Fatal("offline validation created runtime files")
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
	configureOfflineSecrets(&c)
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
		{"invalid_secret_reference", func(c *Config) { c.Agently.Auth.Secret = &scy.Resource{} }},
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
			body := "storage:\n  driver: sqlite\n  path: test.sqlite\ntransport: socket\naccess_mode: shared_channel\nslack:\n  app_id: A\n  workspace_id: T\n  channel_id: C\n  app_token_secret: {URL: /unused/app.enc, Key: 'blowfish://default'}\n  bot_token_secret: {URL: /unused/bot.enc, Key: 'blowfish://default'}\n"
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
			body := "storage:\n  driver: sqlite\n  path: test.sqlite\ntransport: socket\naccess_mode: shared_channel\nslack:\n  app_id: A\n  workspace_id: T\n  channel_id: C\n  app_token_secret: {URL: /unused/app.enc, Key: 'blowfish://default'}\n  bot_token_secret: {URL: /unused/bot.enc, Key: 'blowfish://default'}\n  status:\n    refresh_interval_sec: 15\n"
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
