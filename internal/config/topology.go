package config

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/viant/agently-core/sdk"
	"github.com/viant/slack-ai-gateway/internal/agui"
)

// Backend describes the execution side of one complete integration. Credentials
// remain configuration resources; the database records only execution identity.
type Backend struct {
	Type        string      `yaml:"type"`
	BaseURL     string      `yaml:"base_url"`
	Endpoint    string      `yaml:"endpoint"`
	PrincipalID string      `yaml:"principal_id"`
	AgentID     string      `yaml:"agent_id"`
	Auth        BackendAuth `yaml:"auth"`
}

// Integration is self-contained. Name identifies the logical configuration;
// numeric database IDs belong to recorded assignments, not to this document.
type Integration struct {
	Name      string        `yaml:"name"`
	Transport string        `yaml:"transport"`
	Access    *Access       `yaml:"access"`
	Slack     SlackSettings `yaml:"slack"`
	Backend   Backend       `yaml:"backend"`
}

type ResolvedIntegration struct {
	Name    string
	Backend Backend
	Config  Config
}

// A nonnil empty list deliberately selects zero active integrations. The native
// YAML decoder preserves that distinction; the mapping check rejects null.
func (c Config) hasTopology() bool { return c.Integrations != nil }

func identifier(value string) bool {
	return value != "" && len(value) <= 128 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, " \r\n\t")
}

// ValidateIntegrationNames checks the logical identities before any runtime or
// backend-only operation can open storage or authenticate a backend.
func (c Config) ValidateIntegrationNames() error {
	if !c.hasTopology() {
		return fmt.Errorf("integrations must be explicitly configured")
	}
	names := make(map[string]bool, len(c.Integrations))
	for _, integration := range c.Integrations {
		if !identifier(integration.Name) || names[integration.Name] {
			return fmt.Errorf("integration names must be unique nonempty identifiers of at most 128 bytes")
		}
		names[integration.Name] = true
	}
	return nil
}

// Topology validates all integrations before storage or credentials are opened.
// Only an explicit integrations list can activate or deactivate assignments.
func (c Config) Topology() ([]ResolvedIntegration, error) {
	if err := c.ValidateIntegrationNames(); err != nil {
		return nil, err
	}
	if err := c.ValidateStorage(); err != nil {
		return nil, err
	}
	if filepath.Base(c.Storage.Path) == "slack-smoke.sqlite" {
		return nil, fmt.Errorf("run requires a separate SQLite database, not slack-smoke.sqlite")
	}
	resolved := make([]ResolvedIntegration, 0, len(c.Integrations))
	apps := map[string]bool{}
	for _, integration := range c.Integrations {
		backend := integration.Backend
		local := Config{Storage: c.Storage, Transport: integration.Transport, Access: integration.Access, Slack: integration.Slack,
			Agently: AgentlySettings{BaseURL: backend.BaseURL, AgentID: backend.AgentID, PrincipalID: backend.PrincipalID, Auth: backend.Auth}}
		if err := local.ValidateSlack(); err != nil {
			return nil, fmt.Errorf("integration %s: %w", integration.Name, err)
		}
		if err := backend.Validate(); err != nil {
			return nil, fmt.Errorf("integration %s: %w", integration.Name, err)
		}
		if apps[local.Slack.AppID] {
			return nil, fmt.Errorf("the current Socket Mode runtime requires a distinct Slack app_id for each active integration; shared-bot routing is not implemented")
		}
		apps[local.Slack.AppID] = true
		resolved = append(resolved, ResolvedIntegration{Name: integration.Name, Backend: backend, Config: local})
	}
	return resolved, nil
}

// Validate checks the configured backend contract without reading credentials.
func (b Backend) Validate() error {
	if strings.TrimSpace(b.PrincipalID) == "" {
		return fmt.Errorf("principal_id is required")
	}
	switch b.Type {
	case "agently":
		if b.Endpoint != "" {
			return fmt.Errorf("Agently uses base_url, not endpoint")
		}
		return (Config{Agently: AgentlySettings{BaseURL: b.BaseURL, AgentID: b.AgentID, PrincipalID: b.PrincipalID, Auth: b.Auth}}).ValidateAgently()
	case "agui":
		if b.BaseURL != "" {
			return fmt.Errorf("generic AG-UI uses endpoint, not base_url")
		}
		var tokens sdk.TokenProvider
		switch b.Auth.Mode {
		case "bearer":
			if b.Auth.Secret.Validate() != nil {
				return fmt.Errorf("bearer authentication requires a secret resource")
			}
			tokens = func(context.Context) (string, error) { return "", nil }
		case "none":
			if b.Auth.Secret != nil {
				return fmt.Errorf("AG-UI auth.mode none does not accept a secret")
			}
		default:
			return fmt.Errorf("AG-UI auth.mode must explicitly be bearer or none")
		}
		_, err := agui.NewFactory(b.Endpoint, nil, tokens)
		return err
	default:
		return fmt.Errorf("backend type must be agently or agui")
	}
}
