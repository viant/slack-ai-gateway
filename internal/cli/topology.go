package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/viant/agently-core/sdk"
	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/agui"
	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

type policyReport struct {
	IntegrationName                string                           `json:"integration_name,omitempty"`
	ResponseMode                   string                           `json:"response_mode"`
	StatusMode                     gateway.StatusMode               `json:"status_mode"`
	MessageRefreshIntervalSec      int64                            `json:"message_refresh_interval_sec"`
	AgentSessionRefreshIntervalSec int64                            `json:"agent_session_refresh_interval_sec"`
	Valid                          bool                             `json:"valid"`
	AppID                          string                           `json:"app_id"`
	WorkspaceID                    string                           `json:"workspace_id"`
	Scope                          gateway.ChannelScope             `json:"channels"`
	Policies                       []gateway.EffectiveChannelPolicy `json:"effective_policies"`
	DirectMessages                 gateway.EffectiveChannelPolicy   `json:"direct_messages"`
}

func staticPolicy(c config.Config, integrationName, channelID string) (policyReport, error) {
	route, err := c.Route()
	if err != nil {
		return policyReport{}, err
	}
	ids := route.Resolver.ChannelIDs()
	if channelID != "" {
		ids = []string{channelID}
	}
	policies := make([]gateway.EffectiveChannelPolicy, 0, len(ids))
	for _, id := range ids {
		policies = append(policies, route.Resolver.Resolve(id))
	}
	return policyReport{IntegrationName: integrationName, ResponseMode: string(c.ResponseMode()), StatusMode: c.StatusMode(), MessageRefreshIntervalSec: int64(c.StatusRefreshInterval().Seconds()), AgentSessionRefreshIntervalSec: int64(gateway.AgentSessionRefresh.Seconds()), Valid: true, AppID: route.AppID, WorkspaceID: route.WorkspaceID, Scope: route.Resolver.Scope(), Policies: policies, DirectMessages: route.Resolver.ResolveDirectMessages()}, nil
}

func validateTopology(c config.Config, selected, channel string, out io.Writer) error {
	topology, err := c.Topology()
	if err != nil {
		return err
	}
	reports := make([]policyReport, 0, len(topology))
	for _, integration := range topology {
		if selected != "" && selected != integration.Name {
			continue
		}
		report, err := staticPolicy(integration.Config, integration.Name, channel)
		if err != nil {
			return err
		}
		reports = append(reports, report)
	}
	if len(reports) == 0 && selected != "" {
		return fmt.Errorf("unknown integration")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	if selected != "" {
		return encoder.Encode(reports[0])
	}
	return encoder.Encode(struct {
		Valid        bool           `json:"valid"`
		Integrations []policyReport `json:"integrations"`
	}{true, reports})
}

func checkTopologyAgently(ctx context.Context, c config.Config, selected string, out io.Writer, deps dependencies) error {
	if err := c.ValidateIntegrationNames(); err != nil {
		return err
	}
	candidates := make([]config.Integration, 0, len(c.Integrations))
	for _, integration := range c.Integrations {
		if selected != "" && selected != integration.Name {
			continue
		}
		if integration.Backend.Type != "agently" {
			if selected != "" {
				return fmt.Errorf("check-agently requires an Agently integration")
			}
			continue
		}
		if err := integration.Backend.Validate(); err != nil {
			return fmt.Errorf("integration %s: %w", integration.Name, err)
		}
		candidates = append(candidates, integration)
	}
	if len(candidates) == 0 {
		return fmt.Errorf("no matching Agently integration")
	}
	for _, integration := range candidates {
		backend := integration.Backend
		if _, err := deps.login(ctx, backend.BaseURL, backend.PrincipalID, backend.AgentID, backend.Auth.Secret); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(out, "Integration %s: Agently session identity and configured agent verified. No conversation, query or Slack call was made. Tool permissions are not verified.\n", integration.Name); err != nil {
			return err
		}
	}
	return nil
}

func integrationIdentity(integration config.ResolvedIntegration) storage.Integration {
	backendURL := integration.Backend.BaseURL
	if integration.Backend.Type == "agui" {
		backendURL = integration.Backend.Endpoint
	}
	return storage.Integration{Name: integration.Name,
		AppID: integration.Config.Slack.AppID, WorkspaceID: integration.Config.Slack.WorkspaceID, BackendType: integration.Backend.Type, BackendURL: backendURL,
		BackendAgentID: integration.Backend.AgentID, PrincipalID: integration.Backend.PrincipalID}
}

func genericFactory(ctx context.Context, backend config.Backend) (gateway.AgentlyClientFactory, error) {
	var tokens sdk.TokenProvider
	if backend.Auth.Mode == "bearer" {
		resource := *backend.Auth.Secret
		tokens = func(ctx context.Context) (token string, err error) {
			defer func() {
				if recover() != nil {
					token, err = "", fmt.Errorf("cannot decode AG-UI bearer credentials")
				}
			}()
			ref := resource
			secret, err := scy.New().Load(ctx, &ref)
			if err != nil || secret == nil || strings.TrimSpace(secret.String()) == "" {
				return "", fmt.Errorf("cannot load AG-UI bearer credentials")
			}
			return secret.String(), nil
		}
		if _, err := tokens(ctx); err != nil {
			return nil, err
		}
	}
	return agui.NewFactory(backend.Endpoint, nil, tokens)
}

func runTopology(ctx context.Context, c config.Config, root storage.Store, out io.Writer, deps dependencies) error {
	topology, err := c.Topology()
	if err != nil {
		return err
	}
	identities := make([]storage.Integration, 0, len(topology))
	for _, integration := range topology {
		identities = append(identities, integrationIdentity(integration))
	}
	recorded, err := root.ReconcileIntegrations(ctx, identities)
	if err != nil {
		return err
	}
	if len(topology) == 0 {
		_, err = fmt.Fprintln(out, "No active integrations configured. Previous assignments were deactivated and unfinished work was closed locally.")
		return err
	}
	byName := make(map[string]storage.Integration, len(recorded))
	for _, identity := range recorded {
		byName[identity.Name] = identity
	}
	log := slog.New(slog.NewTextHandler(out, nil))
	group := runner.Group{Store: root, Log: log}
	for _, integration := range topology {
		identity, ok := byName[integration.Name]
		if !ok || identity.ID <= 0 {
			return fmt.Errorf("cannot resolve recorded integration assignment")
		}
		local, scope := integration.Config, root.Scope(identity.ID)
		integrationLog := log.With("integration_name", integration.Name, "integration_id", identity.ID, "backend_type", integration.Backend.Type, "active", identity.Active)
		route, err := local.Route()
		if err != nil {
			return err
		}
		if err = scope.CheckRunBindings(ctx, route, local.Agently.PrincipalID); err != nil {
			return err
		}
		tokens, err := local.LoadSlackTokens(ctx)
		if err != nil {
			integrationLog.Error("integration_unavailable", "reason", "slack_credentials_unavailable")
			if err = scope.FailCleanup(ctx, "slack_credentials_unavailable"); err != nil {
				return err
			}
			continue
		}
		var login func(context.Context) (gateway.AgentlyClientFactory, error)
		if integration.Backend.Type == "agently" {
			var credentialErr error
			if deps.validateSecret != nil {
				if credentialErr = deps.validateSecret(ctx, local.Agently.Auth.Secret); credentialErr != nil {
					integrationLog.Error("integration_backend_unavailable", "reason", "backend_credentials_unavailable")
				}
			}
			login = func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
				if credentialErr != nil {
					return nil, gateway.ErrBackendAuth
				}
				a := local.Agently
				return deps.login(ctx, a.BaseURL, a.PrincipalID, a.AgentID, a.Auth.Secret)
			}
		} else {
			factory, err := genericFactory(ctx, integration.Backend)
			if err != nil {
				integrationLog.Error("integration_backend_unavailable", "reason", "agui_configuration_or_credentials_unavailable")
				login = func(context.Context) (gateway.AgentlyClientFactory, error) { return nil, gateway.ErrBackendAuth }
			} else {
				login = func(context.Context) (gateway.AgentlyClientFactory, error) { return factory, nil }
			}
		}
		backend := agently.NewManager(login, func(ctx context.Context, state gateway.BackendState) error {
			return scope.FailUnstarted(ctx, gateway.BackendReason(state))
		}, integrationLog)
		source, sender, err := deps.connect(ctx, tokens, route, integrationLog)
		if err != nil {
			integrationLog.Error("integration_unavailable", "reason", "slack_preflight_failed")
			if err = scope.FailCleanup(ctx, "slack_transport_unavailable"); err != nil {
				return err
			}
			continue
		}
		var directory gateway.SlackDirectory
		if deps.directory != nil {
			directory = deps.directory(tokens, route)
		}
		runtime := &runner.Runtime{StatusMode: local.StatusMode(), StatusRefreshInterval: local.StatusRefreshInterval(), Store: scope, Route: route, Backend: backend,
			Source: source, Sender: sender, AgentID: local.Agently.AgentID, Log: integrationLog, ResponseMode: local.ResponseMode(), AgentlyBaseURL: local.Agently.BaseURL,
			Policy: gateway.ConfiguredPolicy{Route: route, PrincipalID: local.Agently.PrincipalID, Directory: directory}}
		group.Integrations = append(group.Integrations, runner.Integration{ID: integration.Name, Runtime: runtime})
	}
	if len(group.Integrations) == 0 {
		return fmt.Errorf("no Slack integration could start; verify connection and credential configuration")
	}
	return group.Run(ctx)
}
