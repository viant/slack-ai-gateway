package config

import (
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	slackpresentation "github.com/viant/agently-core/sdk/slack"
	"github.com/viant/agently-slack-gateway/internal/gateway"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage    Storage `yaml:"storage"`
	Transport  string  `yaml:"transport"`
	AccessMode string  `yaml:"access_mode"`
	Access     *Access `yaml:"access"`
	Slack      struct {
		AppID       string    `yaml:"app_id"`
		WorkspaceID string    `yaml:"workspace_id"`
		ChannelID   string    `yaml:"channel_id"`
		Channels    *Channels `yaml:"channels"`
		Status      Status    `yaml:"status"`
		Response    Response  `yaml:"response"`
		AppTokenEnv string    `yaml:"app_token_env"`
		BotTokenEnv string    `yaml:"bot_token_env"`
	} `yaml:"slack"`
	Agently struct {
		BaseURL     string `yaml:"base_url"`
		AgentID     string `yaml:"agent_id"`
		PrincipalID string `yaml:"principal_id"`
		Auth        struct {
			Mode          string `yaml:"mode"`
			SecretsURLEnv string `yaml:"secrets_url_env"`
		} `yaml:"auth"`
	} `yaml:"agently"`
}

// Response selects presentation of completed responses only.
type Response struct {
	Mode slackpresentation.ResponseMode `yaml:"mode"`
}

func (c Config) ResponseMode() slackpresentation.ResponseMode {
	if c.Slack.Response.Mode == "" {
		return slackpresentation.ResponseRaw
	}
	return c.Slack.Response.Mode
}

// Status controls periodic presentation only, independently of backend liveness
// and retry timers. A pointer distinguishes an omitted value from explicit zero.
type Status struct {
	Mode               *string `yaml:"mode"`
	RefreshIntervalSec *int64  `yaml:"refresh_interval_sec"`
}

// StatusMode preserves message presentation when the setting is omitted.
func (c Config) StatusMode() gateway.StatusMode {
	if c.Slack.Status.Mode == nil {
		return gateway.StatusMessage
	}
	return gateway.StatusMode(*c.Slack.Status.Mode)
}

func (c Config) StatusRefreshInterval() time.Duration {
	if c.Slack.Status.RefreshIntervalSec == nil {
		return gateway.DefaultStatusRefresh
	}
	return time.Duration(*c.Slack.Status.RefreshIntervalSec) * time.Second
}

type Channels struct {
	Mode         string    `yaml:"mode"`
	Allowlist    *[]string `yaml:"allowlist"`
	Denylist     []string  `yaml:"denylist"`
	AllowPrivate bool      `yaml:"allow_private"`
}

type Users struct {
	Mode      *string   `yaml:"mode"`
	Allowlist *[]string `yaml:"allowlist"`
	Denylist  *[]string `yaml:"denylist"`
}

type Policy struct {
	Users Users `yaml:"users"`
}

type Global struct {
	Users struct {
		Allowlist *[]string `yaml:"allowlist"`
		Denylist  *[]string `yaml:"denylist"`
	} `yaml:"users"`
}

type Access struct {
	Global           *Global           `yaml:"global"`
	Defaults         *Policy           `yaml:"defaults"`
	DirectMessages   *Policy           `yaml:"direct_messages"`
	ChannelOverrides map[string]Policy `yaml:"channel_overrides"`
}

func (c Config) Route() (gateway.Route, error) {
	if c.Slack.ChannelID != "" && c.Slack.Channels != nil {
		return gateway.Route{}, fmt.Errorf("slack.channel_id and slack.channels are mutually exclusive")
	}
	scope := gateway.ChannelScope{}
	if c.Slack.Channels != nil {
		s := c.Slack.Channels
		scope = gateway.ChannelScope{Mode: s.Mode, Denylist: s.Denylist, AllowPrivate: s.AllowPrivate}
		if s.Allowlist != nil {
			scope.Allowlist = append([]string{}, (*s.Allowlist)...)
		}
	} else if c.Slack.ChannelID != "" {
		// Legacy routing was not restricted to public channels.
		scope = gateway.ChannelScope{Mode: "selected", Allowlist: []string{c.Slack.ChannelID}, AllowPrivate: true}
	} else {
		return gateway.Route{}, fmt.Errorf("Slack requires channel_id or channels")
	}
	rules := gateway.AccessRules{Channels: make(map[string]gateway.UserRule)}
	convert := func(u Users) gateway.UserRule {
		return gateway.UserRule{Mode: u.Mode, Allowlist: u.Allowlist, Denylist: u.Denylist}
	}
	if c.Access != nil {
		if c.Access.Global != nil {
			rules.GlobalAllowlist, rules.GlobalDenylist = c.Access.Global.Users.Allowlist, c.Access.Global.Users.Denylist
		}
		if c.Access.Defaults != nil {
			d := convert(c.Access.Defaults.Users)
			rules.Defaults = &d
		}
		if c.Access.DirectMessages != nil {
			d := convert(c.Access.DirectMessages.Users)
			rules.DirectMessages = &d
		}
		for id, p := range c.Access.ChannelOverrides {
			rules.Channels[id] = convert(p.Users)
		}
	} else if c.Slack.Channels == nil {
		mode := "channel_members"
		rules.Channels[c.Slack.ChannelID] = gateway.UserRule{Mode: &mode}
	}
	resolver, err := gateway.NewChannelPolicyResolver(scope, rules)
	if err != nil {
		return gateway.Route{}, err
	}
	return gateway.Route{AppID: c.Slack.AppID, WorkspaceID: c.Slack.WorkspaceID, ChannelID: c.Slack.ChannelID, Resolver: resolver}, nil
}

type Storage struct {
	Driver string `yaml:"driver"`
	Path   string `yaml:"path"`
	DSNEnv string `yaml:"dsn_env"`
}

func Load(path string) (Config, error) {
	var c Config
	f, err := os.Open(path)
	if err != nil {
		return c, fmt.Errorf("cannot open configuration file")
	}
	defer f.Close()
	// Inspect YAML tags for security values and the integer-seconds status setting.
	// The second pass still uses KnownFields, including nested policy structs.
	var tree yaml.Node
	if yaml.NewDecoder(f).Decode(&tree) != nil || len(tree.Content) != 1 {
		return c, fmt.Errorf("invalid YAML configuration")
	}
	root := tree.Content[0]
	if err = validateNoAliases(root); err != nil {
		return c, err
	}
	if root.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(root.Content); i += 2 {
			key, value := root.Content[i].Value, root.Content[i+1]
			if key == "access" {
				if err = validateSecurityNode(value); err != nil {
					return c, err
				}
			}
			if key == "slack" && value.Kind == yaml.MappingNode {
				for j := 0; j+1 < len(value.Content); j += 2 {
					if value.Content[j].Value == "status" {
						node := value.Content[j+1]
						if node.Kind != yaml.MappingNode {
							return c, fmt.Errorf("slack.status must be a mapping")
						}
						for k := 0; k+1 < len(node.Content); k += 2 {
							if node.Content[k].Value == "mode" && node.Content[k+1].Tag != "!!str" {
								return c, fmt.Errorf("slack.status.mode must be message, agent_session or hybrid")
							}
							if node.Content[k].Value == "refresh_interval_sec" && node.Content[k+1].Tag != "!!int" {
								return c, fmt.Errorf("slack.status.refresh_interval_sec must be a positive integer number of seconds")
							}
						}
					}
					if value.Content[j].Value == "channels" {
						if err = validateSecurityNode(value.Content[j+1]); err != nil {
							return c, err
						}
					}
				}
			}
		}
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return c, fmt.Errorf("cannot read configuration")
	}
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if err = d.Decode(&c); err != nil {
		return c, fmt.Errorf("invalid YAML configuration")
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("configuration must contain exactly one document")
	}
	// Relative SQLite paths are anchored to the config, not the caller's cwd.
	if strings.TrimSpace(c.Storage.Path) != "" && c.Storage.Path != ":memory:" && !strings.HasPrefix(c.Storage.Path, "file:") && !filepath.IsAbs(c.Storage.Path) {
		c.Storage.Path, err = filepath.Abs(filepath.Join(filepath.Dir(path), c.Storage.Path))
		if err != nil {
			return c, err
		}
	}
	return c, nil
}

func validateNoAliases(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode || node.Tag == "!!merge" {
		return fmt.Errorf("configuration must not contain YAML aliases or merges")
	}
	for _, child := range node.Content {
		if err := validateNoAliases(child); err != nil {
			return err
		}
	}
	return nil
}

func validateSecurityNode(node *yaml.Node) error {
	if node.Tag == "!!null" || node.Kind == yaml.AliasNode {
		return fmt.Errorf("access and channel configuration must not contain null or YAML aliases")
	}
	// YAML merges can bypass pointer-presence semantics and strict field checks.
	if node.Tag == "!!merge" {
		return fmt.Errorf("access and channel configuration must not contain YAML merges")
	}
	for _, child := range node.Content {
		if err := validateSecurityNode(child); err != nil {
			return err
		}
	}
	return nil
}

func (c Config) ValidateStorage() error {
	switch c.Storage.Driver {
	case "":
		return fmt.Errorf("storage driver is required; set storage.driver or --storage-driver explicitly")
	case "sqlite":
		if strings.TrimSpace(c.Storage.Path) == "" || c.Storage.Path == ":memory:" || strings.HasPrefix(c.Storage.Path, "file:") {
			return fmt.Errorf("sqlite requires storage.path pointing to a durable file")
		}
		if c.Storage.DSNEnv != "" {
			return fmt.Errorf("sqlite does not accept a MySQL DSN reference")
		}
	case "mysql":
		return fmt.Errorf("MySQL is planned for stage E and is not implemented; no fallback is available")
	default:
		return fmt.Errorf("unsupported storage driver")
	}
	return nil
}

func (c Config) ValidateRun() error {
	if err := c.ValidateSlack(); err != nil {
		return err
	}
	if filepath.Base(c.Storage.Path) == "slack-smoke.sqlite" {
		return fmt.Errorf("run requires a separate SQLite database, not slack-smoke.sqlite")
	}
	return c.ValidateAgently()
}

// ValidateAgently deliberately requires neither Slack nor storage configuration.
func (c Config) ValidateAgently() error {
	for _, value := range []string{c.Agently.AgentID, c.Agently.PrincipalID} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("Agently requires agent_id and principal_id")
		}
	}
	u, err := url.Parse(c.Agently.BaseURL)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("agently.base_url must be an HTTP(S) URL without embedded credentials, query or fragment")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("Agently requires HTTPS outside loopback")
	}
	if c.Agently.Auth.Mode != "oob_session" {
		return fmt.Errorf("agently.auth.mode must explicitly be oob_session")
	}
	if name := c.Agently.Auth.SecretsURLEnv; name != "" && !environmentName.MatchString(name) {
		return fmt.Errorf("agently.auth.secrets_url_env must be an environment variable name")
	}
	return nil
}

func (c Config) OOBSecretsReference() (string, error) {
	name := c.Agently.Auth.SecretsURLEnv
	if name == "" {
		name = "AGENTLY_OOB_SECRETS"
	}
	ref := strings.TrimSpace(os.Getenv(name))
	if ref == "" {
		return "", fmt.Errorf("Agently OOB secret reference environment variable is missing")
	}
	return ref, nil
}

// ValidateSlack is shared by the live test and the real-adapter runtime.
// It deliberately needs neither real Agently configuration nor secret values.
func (c Config) ValidateSlack() error {
	if err := c.ResponseMode().Validate(); err != nil {
		return fmt.Errorf("slack.response.mode: %w", err)
	}
	if c.StatusMode() != gateway.StatusMessage && c.StatusMode() != gateway.StatusAgentSession && c.StatusMode() != gateway.StatusHybrid {
		return fmt.Errorf("slack.status.mode must be message, agent_session or hybrid")
	}
	if sec := c.Slack.Status.RefreshIntervalSec; sec != nil && (*sec <= 0 || *sec > math.MaxInt64/int64(time.Second)) {
		return fmt.Errorf("slack.status.refresh_interval_sec must be positive and fit in a time.Duration")
	}
	if err := c.ValidateStorage(); err != nil {
		return err
	}
	if c.Transport != "socket" {
		return fmt.Errorf("only transport: socket is planned for v1")
	}
	if c.AccessMode != "shared_channel" {
		return fmt.Errorf("only access_mode: shared_channel is planned for v1")
	}
	for _, value := range []string{c.Slack.AppID, c.Slack.WorkspaceID} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, " \r\n\t") {
			return fmt.Errorf("Slack app and workspace identifiers are required without whitespace")
		}
	}
	if _, err := c.Route(); err != nil {
		return err
	}
	for _, name := range []string{c.Slack.AppTokenEnv, c.Slack.BotTokenEnv} {
		if name != "" && !environmentName.MatchString(name) {
			return fmt.Errorf("Slack token references must be environment variable names")
		}
	}
	return nil
}

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type SlackTokens struct{ App, Bot string }

func (SlackTokens) String() string   { return "[Slack credentials redacted]" }
func (SlackTokens) GoString() string { return "[Slack credentials redacted]" }

// LoadSlackTokens is invoked only by an explicitly started Slack runtime.
func (c Config) LoadSlackTokens() (SlackTokens, error) {
	if err := c.ValidateSlack(); err != nil {
		return SlackTokens{}, err
	}
	appEnv, botEnv := c.Slack.AppTokenEnv, c.Slack.BotTokenEnv
	if appEnv == "" {
		appEnv = "SLACK_APP_TOKEN"
	}
	if botEnv == "" {
		botEnv = "SLACK_BOT_TOKEN"
	}
	tokens := SlackTokens{App: os.Getenv(appEnv), Bot: os.Getenv(botEnv)}
	if !strings.HasPrefix(tokens.App, "xapp-") || !strings.HasPrefix(tokens.Bot, "xoxb-") || strings.ContainsAny(tokens.App+tokens.Bot, " \r\n\t") {
		return SlackTokens{}, fmt.Errorf("Slack token environment variables are missing or invalid")
	}
	return tokens, nil
}
