package agently

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// Identity is immutable after login. Only a confirmed identity mismatch poisons
// the factory. A temporary 401 can be Core's token-refresh cooldown.
type sessionIdentity struct {
	principal, agent string
	invalid          atomic.Bool
}

type oauthDiscovery struct {
	ConfigURL      string   `json:"configURL"`
	Scopes         []string `json:"scopes"`
	CLIScopes      []string `json:"cliScopes"`
	WebUIScopes    []string `json:"webUIScopes"`
	MobileUIScopes []string `json:"mobileUIScopes"`
}

// Match Agently's OAuthScopesForHeadless: base scopes plus the first nonempty
// normalized surface group (CLI, Web UI, Mobile UI). This is configuration
// selection before authorization, never a retry with broader permissions.
func (c oauthDiscovery) headlessScopes() []string {
	scopes := normalizeScopes(c.Scopes)
	for _, group := range [][]string{c.CLIScopes, c.WebUIScopes, c.MobileUIScopes} {
		if surface := normalizeScopes(group); len(surface) != 0 {
			scopes = normalizeScopes(append(scopes, surface...))
			break
		}
	}
	if len(scopes) == 0 {
		return []string{"openid"}
	}
	return scopes
}

func normalizeScopes(scopes []string) []string {
	var result []string
	seen := make(map[string]bool, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" || seen[scope] {
			continue
		}
		seen[scope] = true
		result = append(result, scope)
	}
	return result
}

func (f *Factory) rejectIdentity() error {
	f.session.invalid.Store(true)
	return gateway.ErrBackendAuth
}

func (c *Client) verifyIdentity(ctx context.Context) error {
	f := c.factory
	if f.session.invalid.Load() {
		return gateway.ErrBackendAuth
	}
	u, _ := url.Parse(f.base + "/v1/api/auth/me")
	if len(f.http.Jar.Cookies(u)) == 0 {
		return &missingSession{}
	}
	var me struct {
		Subject  string `json:"subject"`
		Provider string `json:"provider"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/api/auth/me", nil, &me); err != nil {
		var api *APIError
		if errors.As(err, &api) && api.Status == http.StatusUnauthorized {
			return &missingSession{}
		}
		if errors.As(err, &api) && api.Status == http.StatusForbidden {
			return f.rejectIdentity()
		}
		return err
	}
	if me.Subject != f.session.principal || me.Provider != "session" {
		return f.rejectIdentity()
	}
	return nil
}

func (c *Client) conversationThread(ctx context.Context, id string) (string, error) {
	if c.factory.session != nil {
		if strings.HasPrefix(id, "slack-smoke-") {
			return "", c.factory.rejectIdentity()
		}
		if err := c.verifyIdentity(ctx); err != nil {
			return "", err
		}
	}
	var out struct {
		ID       string  `json:"id"`
		Owner    string  `json:"createdByUserId"`
		Agent    string  `json:"agentId"`
		ThreadID *string `json:"aguiThreadId"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/conversations/"+url.PathEscape(id)+"?includeTranscript=false", nil, &out); err != nil {
		return "", err
	}
	if c.factory.session != nil && (out.ID != id || out.Owner != c.principal || out.Agent != c.factory.session.agent) {
		return "", c.factory.rejectIdentity()
	}
	if out.ID != id {
		return "", gateway.ErrBackendProtocol
	}
	if out.ThreadID != nil {
		// AG-UI thread labels are opaque, not native IDs or URL path segments.
		if *out.ThreadID == "" {
			return "", gateway.ErrBackendProtocol
		}
		return *out.ThreadID, nil
	}
	return id, nil
}

// LoginOOB creates an in-memory BFF session and verifies the effective subject
// and agent listing. It does not create a conversation or invoke an agent.
// The server may persist authentication/session data in its own database.
func LoginOOB(ctx context.Context, base, principal, agent, secretsRef string) (*Factory, error) {
	return loginOOB(ctx, base, principal, agent, secretsRef, loadOAuthConfig, loadBasicSecret, nil)
}

// Verify reuses the current cookie session and checks a protected endpoint.
// /auth/me alone can succeed while Core temporarily cannot inject an OAuth token.
// The probe bypasses only Manager's generation guard, never authentication.
func (f *Factory) Verify(ctx context.Context) error {
	if f.session == nil {
		return gateway.ErrBackendAuth
	}
	probe := *f
	probe.guard = nil
	c := &Client{factory: &probe, principal: f.session.principal}
	if err := c.verifyIdentity(ctx); err != nil {
		return err
	}
	var metadata struct {
		AgentInfos []struct {
			ID string `json:"id"`
		} `json:"agentInfos"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/workspace/metadata", nil, &metadata); err != nil {
		var api *APIError
		if errors.As(err, &api) && api.Status == http.StatusUnauthorized {
			copy := *api
			copy.Permanent = false
			copy.RetryAfter = max(copy.RetryAfter, 30*time.Second)
			return &copy
		}
		return err
	}
	for _, info := range metadata.AgentInfos {
		if info.ID == f.session.agent {
			return nil
		}
	}
	return &authFailure{message: "configured Agently agent is not available in workspace metadata", permanent: true}
}

func loginOOB(ctx context.Context, base, principal, agent, ref string, loadConfig oauthConfigLoader, loadSecret basicSecretLoader, httpClient *http.Client) (*Factory, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if strings.TrimSpace(principal) == "" || strings.TrimSpace(agent) == "" {
		return nil, &authFailure{message: "Agently identity configuration is incomplete", permanent: true}
	}
	// Reuse URL/TLS/redirect validation, but explicitly select session mode.
	f, err := NewFactory(base, httpClient, unusedCredentials{})
	if err != nil {
		return nil, &authFailure{message: err.Error(), permanent: true}
	}
	f.tokens = nil
	f.http.Timeout = min(f.http.Timeout, 30*time.Second)
	f.http.Jar, _ = cookiejar.New(nil)
	c := &Client{factory: f, principal: principal}
	var config oauthDiscovery
	if err = c.do(ctx, http.MethodGet, "/v1/api/auth/oauth/config", nil, &config); err != nil {
		return nil, loginFailure("Agently OAuth configuration request failed", err, nil)
	}
	if config.ConfigURL == "" {
		return nil, &authFailure{message: "Agently OAuth configuration is missing", permanent: true}
	}
	oauthConfig, err := loadConfig(ctx, config.ConfigURL)
	if err != nil || oauthConfig == nil {
		return nil, &authFailure{message: "cannot load Agently OAuth configuration", permanent: true}
	}
	clone := *oauthConfig
	clone.Scopes = config.headlessScopes()
	secret, err := loadSecret(ctx, ref)
	if err != nil || secret == nil || secret.Username == "" || secret.Password == "" {
		return nil, &authFailure{message: "cannot load Agently OOB credentials", permanent: true}
	}
	// IdP requests use a separate client, never the Agently cookie jar.
	idpClient := *f.http
	idpClient.Jar = nil
	payload, err := authorizeOOB(ctx, &clone, secret, &idpClient)
	if err != nil {
		return nil, err
	}
	if err = c.do(ctx, http.MethodPost, "/v1/api/auth/session", payload, nil); err != nil {
		// Report only the endpoint and numeric status, never the response body,
		// credentials, headers or raw transport error. Do not retry this POST.
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status != 0 {
			return nil, loginFailure(fmt.Sprintf("Agently session creation failed (POST /v1/api/auth/session, HTTP %d)", apiErr.Status), err, nil)
		}
		return nil, loginFailure("Agently session creation failed (POST /v1/api/auth/session, HTTP status unavailable)", err, nil)
	}
	f.session = &sessionIdentity{principal: principal, agent: agent}
	if err = f.Verify(ctx); err != nil {
		return nil, loginFailure(err.Error(), err, f)
	}
	f.http.Timeout = 120 * time.Second
	return f, nil
}

// Only used to validate an HTTP client before selecting session mode. It can
// never supply bearer credentials or act as an anonymous fallback.
type unusedCredentials struct{}

func (unusedCredentials) Token(context.Context, string) (string, error) {
	return "", gateway.ErrBackendAuth
}
