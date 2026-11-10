// Package agently implements the narrow HTTP contract documented in
// docs/agently-contract.md. AG-UI streaming is owned by the pinned Core SDK.
package agently

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/viant/agently-core/sdk"
	"github.com/viant/agently-slack-gateway/internal/gateway"
)

const ContractVersion = "github.com/viant/agently-core v0.1.59-0.20261104144409-65d86b32d5d9, AG-UI 1.0; see docs/agently-contract.md"
const maxResponseBytes = 8 << 20

// TokenProvider selects credentials for the execution principal, not the Slack
// author. Called before every HTTP request to allow future credential refresh.
// This is the low-level bearer adapter boundary; CLI run uses LoginOOB instead.
type TokenProvider interface {
	Token(context.Context, string) (string, error)
}

type Factory struct {
	base    string
	http    *http.Client
	tokens  TokenProvider
	session *sessionIdentity
	// Installed before publication by Manager. It prevents a stale worker from
	// sending a request after the shared connection generation has changed.
	guard func() error
}

var _ gateway.AgentlyClientFactory = (*Factory)(nil)

func NewFactory(baseURL string, client *http.Client, tokens TokenProvider) (*Factory, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid Agently base URL")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme == "http" && u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errors.New("Agently requires HTTPS outside loopback")
	}
	if tokens == nil {
		return nil, errors.New("Agently credential provider is required")
	}
	if client == nil {
		client = &http.Client{}
	}
	c := *client
	if c.Timeout <= 0 || c.Timeout > 120*time.Second {
		c.Timeout = 120 * time.Second
	}
	// No redirects (including to login pages), cookie fallback or implicit identity.
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.Jar = nil
	return &Factory{base: strings.TrimRight(u.String(), "/"), http: &c, tokens: tokens}, nil
}

func (f *Factory) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if f.session != nil && (f.session.invalid.Load() || principal != f.session.principal) {
		return nil, f.rejectIdentity()
	}
	if strings.TrimSpace(principal) == "" {
		return nil, gateway.ErrDenied
	}
	return &Client{factory: f, principal: principal}, nil
}

type Client struct {
	factory   *Factory
	principal string
}

// APIError never includes response bodies, URLs, tokens, prompts or raw transport
// errors. Permanent response failures require operator review, not another POST.
type APIError struct {
	Status     int
	Permanent  bool
	RetryAfter time.Duration
}

func (e *APIError) Error() string { return "Agently API request failed" }
func (e *APIError) Unwrap() error {
	if e.Permanent {
		return gateway.ErrBackendProtocol
	}
	return nil
}

// request shares credential, origin and status handling between JSON support
// APIs and AG-UI observation. Neither path retries requests or follows redirects.
func (c *Client) request(ctx context.Context, method, path string, input any, accept string) (*http.Response, error) {
	token, err := c.token(ctx)
	if err != nil {
		return nil, err
	}
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return nil, &APIError{Permanent: true}
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.factory.base+path, body)
	if err != nil {
		return nil, &APIError{Permanent: true}
	}
	if c.factory.session != nil {
		// Core requires a matching origin for cookie-authenticated mutations.
		// Derive it from the trusted destination, excluding any base path.
		switch method {
		case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		default:
			req.Header.Set("Origin", req.URL.Scheme+"://"+req.URL.Host)
		}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Accept", accept)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient(false).Do(req)
	if err != nil {
		return nil, observationError(err)
	}
	return response, nil
}

// token and the classified transport are shared by JSON requests and the SDK.
// No backend response body or transport error crosses this boundary.
func (c *Client) token(ctx context.Context) (string, error) {
	if c.factory.session != nil {
		if c.factory.session.invalid.Load() {
			return "", gateway.ErrBackendAuth
		}
		return "", nil
	}
	// LoginOOB clears the bearer provider before discovery and session creation.
	if c.factory.tokens == nil {
		return "", nil
	}
	token, err := c.factory.tokens.Token(ctx, c.principal)
	if ctx.Err() != nil {
		return "", &APIError{}
	}
	if err != nil || strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n\t ") {
		return "", &APIError{Permanent: true}
	}
	return token, nil
}

type classifiedTransport struct {
	client *Client
	base   http.RoundTripper
}

func (t classifiedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if guard := t.client.factory.guard; guard != nil {
		if err := guard(); err != nil {
			return nil, err
		}
	}
	response, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, &APIError{}
	}
	c := t.client
	if c.factory.session != nil {
		if c.factory.session.invalid.Load() {
			response.Body.Close()
			return nil, gateway.ErrBackendAuth
		}
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := response.StatusCode
		response.Body.Close()
		transient := code >= 500 || code == 408 || code == 429 || (code == 404 && req.Method == http.MethodGet)
		return nil, &APIError{Status: code, Permanent: !transient, RetryAfter: retryAfter(response.Header.Get("Retry-After"), time.Now())}
	}
	// Content type belongs to the existing HTTP boundary; the SDK handles framing.
	if req.Header.Get("Accept") == "text/event-stream" {
		mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if err != nil || mediaType != "text/event-stream" {
			response.Body.Close()
			return nil, gateway.ErrBackendProtocol
		}
	}
	return response, nil
}
func (c *Client) httpClient(stream bool) *http.Client {
	client := *c.factory.http
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = classifiedTransport{client: c, base: base}
	if stream {
		// The durable execution deadline owns stream lifetime.
		client.Timeout = 0
	}
	return &client
}

func (c *Client) do(ctx context.Context, method, path string, input, output any) error {
	response, err := c.request(ctx, method, path, input, "application/json")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return &APIError{}
	}
	if len(data) > maxResponseBytes {
		return &APIError{Permanent: true}
	}
	if output == nil {
		return nil
	}
	// Match the SDK's optional Datly response envelope without retrying requests.
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(data, &envelope) == nil && len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		data = envelope.Data
	}
	if json.Unmarshal(data, output) != nil {
		return &APIError{Permanent: true}
	}
	return nil
}

func (c *Client) CreateConversation(ctx context.Context, agent string) (string, error) {
	if agent == "" {
		return "", gateway.ErrBackendProtocol
	}
	if c.factory.session != nil {
		if agent != c.factory.session.agent {
			return "", c.factory.rejectIdentity()
		}
		if err := c.verifyIdentity(ctx); err != nil {
			return "", err
		}
	}
	var out struct {
		ID string `json:"id"`
	}
	err := c.do(ctx, http.MethodPost, "/v1/conversations", struct {
		AgentID string `json:"agentId"`
	}{agent}, &out)
	if err != nil {
		return "", err
	}
	if !validID(out.ID) {
		return "", gateway.ErrBackendProtocol
	}
	return out.ID, nil
}

func (c *Client) Query(ctx context.Context, q gateway.Query, observe func(gateway.Turn) error) (gateway.Turn, error) {
	if !validID(q.ConversationID) || !validID(q.MessageID) || q.AgentID == "" {
		return gateway.Turn{}, gateway.ErrBackendProtocol
	}
	if c.factory.session != nil {
		if q.AgentID != c.factory.session.agent {
			return gateway.Turn{}, c.factory.rejectIdentity()
		}
	}
	threadID, err := c.conversationThread(ctx, q.ConversationID)
	if err != nil {
		return gateway.Turn{}, err
	}
	return c.observeRun(ctx, q.ConversationID, threadID, q.MessageID, &q, observe)
}

func validID(id string) bool {
	return id != "" && id != "." && id != ".." && !strings.ContainsAny(id, "/\\?#\r\n\t ")
}

func (c *Client) ReadTurn(ctx context.Context, conversationID, turnID string, observe func(gateway.Turn) error) (gateway.Turn, error) {
	if !validID(conversationID) || !validID(turnID) {
		return gateway.Turn{}, gateway.ErrBackendProtocol
	}
	threadID, err := c.conversationThread(ctx, conversationID)
	if err != nil {
		return gateway.Turn{}, err
	}
	// Reattach to the durable journal without sending any prompt.
	return c.observeRun(ctx, conversationID, threadID, turnID, nil, observe)
}

func (c *Client) readNativeTurn(ctx context.Context, conversationID, requestID, nativeTurnID string) (gateway.Turn, error) {
	var out sdk.ConversationStateResponse
	if err := c.do(ctx, http.MethodGet, "/v1/conversations/"+url.PathEscape(conversationID)+"/transcript", nil, &out); err != nil {
		return gateway.Turn{}, err
	}
	if out.Conversation == nil || out.Conversation.ConversationID != conversationID {
		return gateway.Turn{}, gateway.ErrBackendProtocol
	}
	var selected *sdk.TurnState
	for _, turn := range out.Conversation.Turns {
		if turn == nil || turn.TurnID != nativeTurnID {
			continue
		}
		if selected != nil {
			return gateway.Turn{}, gateway.ErrBackendProtocol
		}
		selected = turn
	}
	if selected == nil {
		return gateway.Turn{ID: requestID, State: "unknown"}, nil
	}
	// Keep the worker's durable logical request identity at the adapter boundary;
	// startedByMessageId and runId are not native turn identities.
	result := gateway.Turn{ID: requestID, State: string(selected.Status)}
	if selected.Elicitation != nil {
		switch selected.Elicitation.Status {
		case "accepted", "declined", "canceled", "failed": // resolved; turn status still governs publication
		default: // pending, absent or new/unknown interaction state: fail closed
			result.State = "waiting_for_user"
		}
	}
	switch result.State {
	case "queued", "running", "waiting_for_user", "failed", "canceled":
		return result, nil
	case "completed":
		// Native ownership classification excludes execution narration, internal
		// messages, tool results and the final already represented by Content.
		result.StandaloneMessages = sdk.SelectStandaloneAssistantMessages(selected)
		if selected.Assistant != nil && selected.Assistant.Final != nil {
			result.Content = selected.Assistant.Final.Content
			return result, nil
		}
		if selected.Execution == nil {
			return result, nil
		}
		for n := len(selected.Execution.Pages) - 1; n >= 0; n-- {
			p := selected.Execution.Pages[n]
			if p != nil && p.FinalResponse && p.TurnID == nativeTurnID {
				result.Content = p.Content
				break
			}
		}
		return result, nil
	default:
		return gateway.Turn{}, gateway.ErrBackendProtocol
	}
}
