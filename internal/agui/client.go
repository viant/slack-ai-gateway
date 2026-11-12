// Package agui adapts a protocol-only AG-UI endpoint to the gateway's durable
// execution workflow. Native Core SDK code owns the transport and projection.
package agui

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	wire "github.com/viant/agently-core/protocol/agui"
	"github.com/viant/agently-core/runtime/aguistate"
	"github.com/viant/agently-core/sdk"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type Factory struct {
	client *sdk.HTTPClient
}

var _ gateway.AgentlyClientFactory = (*Factory)(nil)

// NewFactory configures one backend connection. The token callback is evaluated
// by the native SDK for each request; credentials never enter persisted state.
func NewFactory(endpoint string, httpClient *http.Client, tokens sdk.TokenProvider) (*Factory, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, errors.New("invalid AG-UI endpoint")
	}
	ip := net.ParseIP(u.Hostname())
	loopback := u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
	if u.Scheme == "http" && !loopback {
		return nil, errors.New("AG-UI requires HTTPS outside loopback")
	}
	if tokens == nil && !loopback {
		return nil, errors.New("AG-UI credentials are required outside loopback")
	}
	// The pinned SDK currently fixes this path. An endpoint option belongs in
	// that SDK; do not emulate it through a URL-rewriting RoundTripper here.
	if u.EscapedPath() != "/v1/ag-ui/run" || u.RawQuery != "" {
		return nil, errors.New("configured AG-UI endpoint requires native SDK path support")
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	client := *httpClient
	client.Timeout = 0 // The worker's durable query deadline owns stream lifetime.
	client.Jar = nil
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	core, err := sdk.NewHTTP(u.Scheme+"://"+u.Host, sdk.WithHTTPClient(&client), sdk.WithTokenProvider(tokens))
	if err != nil {
		return nil, errors.New("cannot initialize AG-UI client")
	}
	return &Factory{client: core}, nil
}

func (f *Factory) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if strings.TrimSpace(principal) == "" {
		return nil, gateway.ErrDenied
	}
	return &Client{factory: f}, nil
}

type Client struct {
	factory *Factory
}

// Base AG-UI accepts a caller-owned thread ID and has no separate create API.
func (c *Client) CreateConversation(ctx context.Context, _ string) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	return uuid.NewString(), nil
}

// ReadTurn never submits another run: ordinary AG-UI defines no durable attach
// or canonical-turn lookup equivalent to the Agently backend's extension.
func (c *Client) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, gateway.ErrBackendProtocol
}

func (c *Client) Query(ctx context.Context, q gateway.Query, observe func(gateway.Turn) error) (gateway.Turn, error) {
	unknown := gateway.Turn{ID: q.MessageID, State: "unknown"}
	projection, err := aguistate.New(q.ProtocolState, q.ProtocolMessages)
	if err != nil {
		return unknown, gateway.ErrBackendProtocol
	}
	previous := make(map[string]bool, len(projection.Messages))
	for _, message := range projection.Messages {
		id, _ := message["id"].(string)
		previous[id] = true
	}
	projection.Messages = append(projection.Messages, aguistate.Object{"id": q.MessageID, "role": "user", "content": q.Text})
	state, messages, err := projection.Snapshot()
	if err != nil {
		return unknown, gateway.ErrBackendProtocol
	}
	var history []wire.Message
	if err = json.Unmarshal(messages, &history); err != nil {
		return unknown, gateway.ErrBackendProtocol
	}
	// Activity messages are presentation material, which the protocol excludes
	// from run input. Retain them in the durable snapshot returned to storage.
	inputMessages := make([]wire.Message, 0, len(history))
	for _, message := range history {
		if message.Role != "activity" {
			inputMessages = append(inputMessages, message)
		}
	}
	input := &wire.RunAgentInput{ThreadID: q.ConversationID, RunID: q.MessageID, ProtocolVersion: wire.ProtocolVersion, State: state, Messages: inputMessages}
	stream, err := c.factory.client.RunAGUI(ctx, input, nil)
	if err != nil {
		return unknown, gateway.ErrBackendProtocol
	}
	defer stream.Close()
	owned := map[string]bool{}
	started := false
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			return unknown, gateway.ErrBackendProtocol
		}
		var frame struct {
			Type, ThreadID, RunID, MessageID, Role string
			SubagentRunID                          *string `json:"subagentRunId"`
			Outcome                                struct{ Type string }
		}
		if err = event.Decode(&frame); err != nil {
			return unknown, gateway.ErrBackendProtocol
		}
		if _, err = projection.Apply(event.Data); err != nil {
			return unknown, gateway.ErrBackendProtocol
		}
		root := frame.SubagentRunID == nil
		if root && frame.Type == "RUN_STARTED" && frame.ThreadID == q.ConversationID && frame.RunID == q.MessageID {
			// A producer may replay earlier runs before the requested run. Those
			// messages join history, but do not become this question's answer.
			for _, message := range projection.Messages {
				id, _ := message["id"].(string)
				previous[id] = true
			}
			started = true
			if observe != nil {
				if err = observe(gateway.Turn{ID: q.MessageID, State: "running"}); err != nil {
					return unknown, err
				}
			}
		}
		if started && root && frame.Type == "TEXT_MESSAGE_START" && frame.Role == "assistant" {
			owned[frame.MessageID] = true
		}
		terminal := root && started && (frame.Type == "RUN_FINISHED" || frame.Type == "RUN_ERROR")
		if !terminal {
			continue
		}
		if frame.Type == "RUN_FINISHED" && (frame.ThreadID != q.ConversationID || frame.RunID != q.MessageID) {
			return unknown, gateway.ErrBackendProtocol
		}
		state, messages, err = projection.Snapshot()
		if err != nil {
			return unknown, gateway.ErrBackendProtocol
		}
		result := gateway.Turn{ID: q.MessageID, State: "completed", ProtocolState: state, ProtocolMessages: messages}
		if frame.Type == "RUN_ERROR" {
			result.State = "failed"
		} else {
			switch frame.Outcome.Type {
			case "interrupt":
				result.State = "waiting_for_user"
			case "cancelled":
				result.State = "canceled"
			case "", "success":
			default:
				return unknown, gateway.ErrBackendProtocol
			}
		}
		if result.State == "completed" {
			var parts []string
			for _, message := range projection.Messages {
				id, _ := message["id"].(string)
				if message["role"] != "assistant" || previous[id] && !owned[id] {
					continue
				}
				if _, child := message["subagentRunId"]; child {
					continue
				}
				if text, ok := message["content"].(string); ok && text != "" {
					parts = append(parts, text)
				}
			}
			result.Content = strings.Join(parts, "\n\n")
		}
		return result, nil
	}
}
