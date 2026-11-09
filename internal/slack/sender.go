package slack

import (
	"context"
	"errors"
	"strings"
	"time"

	sdk "github.com/slack-go/slack"
	"github.com/viant/agently-slack-gateway/internal/gateway"
)

type postAPI interface {
	PostMessageContext(context.Context, string, ...sdk.MsgOption) (string, string, error)
}

type updateAPI interface {
	UpdateMessageContext(context.Context, string, string, ...sdk.MsgOption) (string, string, string, error)
}

type Sender struct {
	api   postAPI
	route gateway.Route
}

var _ gateway.StatusSender = (*Sender)(nil)

type sessionAPI interface {
	SetAgentSessionStatusContext(context.Context, sdk.AgentSessionSetStatusParameters) (*sdk.AgentSessionSetStatusResponse, error)
}

func (s *Sender) SendStatus(ctx context.Context, status gateway.RequestStatus, text string) (gateway.StatusReceipt, error) {
	if status.Target != gateway.StatusTargetAgentSession {
		ts, err := s.sendMessageStatus(ctx, status, text)
		return gateway.StatusReceipt{SlackTS: ts}, err
	}
	if !s.route.Allows(gateway.Message{AppID: s.route.AppID, ThreadKey: status.ThreadKey, Surface: status.Surface}) || status.RootTS == "" {
		return gateway.StatusReceipt{}, &gateway.SendError{Kind: gateway.SendRejected}
	}
	api, ok := s.api.(sessionAPI)
	if !ok {
		return gateway.StatusReceipt{}, &gateway.SendError{Kind: gateway.SendRejected}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := api.SetAgentSessionStatusContext(ctx, sdk.AgentSessionSetStatusParameters{Status: status.SessionState, ChannelID: status.ChannelID, ThreadTS: status.RootTS, InitiatorUserID: status.InitiatorUserID})
	if err != nil {
		return gateway.StatusReceipt{}, classifySendError(err)
	}
	if response == nil {
		return gateway.StatusReceipt{}, &gateway.SendError{Kind: gateway.SendUnknown}
	}
	warning := ""
	for _, w := range response.ResponseMetadata.Warnings {
		if w == "missing_agent_session_stopped_event_subscription" {
			warning = w
		}
	}
	return gateway.StatusReceipt{Acknowledged: true, AgentStatus: response.AgentStatus, SessionStatus: response.Status, Warning: warning}, nil
}

func (s *Sender) sendMessageStatus(ctx context.Context, status gateway.RequestStatus, text string) (string, error) {
	if status.SlackTS == "" {
		return s.Send(ctx, gateway.Delivery{ThreadKey: status.ThreadKey, Text: text, Format: gateway.ReplyPlain})
	}
	if !s.route.AllowsChannel(status.WorkspaceID, status.ChannelID) || status.RootTS == "" || text == "" {
		return "", &gateway.SendError{Kind: gateway.SendRejected}
	}
	api, ok := s.api.(updateAPI)
	if !ok {
		return "", &gateway.SendError{Kind: gateway.SendRejected}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	channel, ts, _, err := api.UpdateMessageContext(ctx, status.ChannelID, status.SlackTS,
		sdk.MsgOptionText(text, false), sdk.MsgOptionDisableMarkdown(), sdk.MsgOptionParse(false),
		sdk.MsgOptionDisableLinkUnfurl(), sdk.MsgOptionDisableMediaUnfurl())
	if err != nil {
		return "", classifySendError(err)
	}
	if channel != status.ChannelID || ts != status.SlackTS {
		return "", &gateway.SendError{Kind: gateway.SendUnknown}
	}
	return ts, nil
}

func (s *Sender) Send(ctx context.Context, d gateway.Delivery) (string, error) {
	if !s.route.AllowsChannel(d.WorkspaceID, d.ChannelID) || d.RootTS == "" || d.Text == "" {
		return "", &gateway.SendError{Kind: gateway.SendRejected}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	options := []sdk.MsgOption{sdk.MsgOptionTS(d.RootTS), sdk.MsgOptionDisableMarkdown(), sdk.MsgOptionParse(false), sdk.MsgOptionDisableLinkUnfurl(), sdk.MsgOptionDisableMediaUnfurl()}
	switch d.Format {
	case gateway.ReplyMarkdown:
		// The block carries unchanged standard Markdown. Escaped text remains useful
		// in notifications/screen readers, without expanding Slack mentions.
		// Reuse the plain renderer so entities stay intact. Slack truncates text
		// above 40,000 characters; the complete answer stays in the Markdown block.
		plain := gateway.RenderText(d.Text)
		var fallback strings.Builder
		for _, part := range plain[:min(len(plain), 10)] {
			fallback.WriteString(part.Text)
		}
		options = append(options, sdk.MsgOptionText(fallback.String(), false), sdk.MsgOptionBlocks(sdk.NewMarkdownBlock("", d.Text)))
	case gateway.ReplyPlain:
		options = append(options, sdk.MsgOptionText(d.Text, false))
	default:
		return "", &gateway.SendError{Kind: gateway.SendRejected}
	}
	channel, ts, err := s.api.PostMessageContext(ctx, d.ChannelID, options...)
	if err != nil {
		return "", classifySendError(err)
	}
	if channel != d.ChannelID || ts == "" {
		return "", &gateway.SendError{Kind: gateway.SendUnknown}
	}
	return ts, nil
}

func classifySendError(err error) *gateway.SendError {
	var limit *sdk.RateLimitedError
	if errors.As(err, &limit) && limit.RetryAfter > 0 {
		return &gateway.SendError{Kind: gateway.SendRateLimited, RetryAfter: limit.RetryAfter}
	}
	var response sdk.SlackErrorResponse
	if errors.As(err, &response) {
		switch response.Err {
		case "invalid_auth", "not_authed", "token_revoked", "account_inactive", "token_expired":
			return &gateway.SendError{Kind: gateway.SendCredentials}
		case "channel_not_found", "not_in_channel", "is_archived", "missing_scope", "no_permission", "restricted_action", "ekm_access_denied", "msg_too_long", "no_text", "invalid_arguments", "invalid_blocks", "message_not_found", "cant_update_message", "edit_window_closed", "feature_disabled":
			return &gateway.SendError{Kind: gateway.SendRejected, Code: response.Err}
		}
	}
	// This includes 5xx, internal_error, fatal_error, invalid/missing Retry-After,
	// malformed success responses, cancellation and unknown future API errors.
	return &gateway.SendError{Kind: gateway.SendUnknown}
}
