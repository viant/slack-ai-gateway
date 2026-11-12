package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/slack-go/slack/socketmode"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type session interface {
	RunContext(context.Context) error
	AckCtx(context.Context, string, any) error
	events() <-chan socketmode.Event
}
type sdkSession struct{ *socketmode.Client }

func (s sdkSession) events() <-chan socketmode.Event { return s.Events }

type Source struct {
	session   session
	route     gateway.Route
	botUserID string
	log       *slog.Logger
}

func (s *Source) Run(ctx context.Context, sink gateway.EventSink, ready func()) error {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.session.RunContext(ctx) }()
	defer func() { cancel(); <-done }()
	verified := false
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			// Retain the completion for the deferred join.
			done <- err
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("Slack socket stopped; verify connection and app credentials")
		case event, open := <-s.session.events():
			if !open {
				return fmt.Errorf("Slack event stream closed")
			}
			switch event.Type {
			case socketmode.EventTypeConnecting, socketmode.EventTypeConnected:
				verified = false
				s.log.Info("slack_connection", "state", string(event.Type))
			case socketmode.EventTypeHello:
				if event.Request == nil || event.Request.ConnectionInfo.AppID != s.route.AppID {
					return fmt.Errorf("Slack socket application mismatch")
				}
				verified = true
				ready()
				s.log.Info("slack_connection", "state", "ready")
			case socketmode.EventTypeInvalidAuth:
				return fmt.Errorf("Slack app authentication failed")
			case socketmode.EventTypeConnectionError, socketmode.EventTypeIncomingError, socketmode.EventTypeErrorWriteFailed:
				// Never log Data: SDK errors may contain credentials or raw payloads.
				s.log.Warn("slack_transport_error", "kind", string(event.Type))
			case socketmode.EventTypeErrorBadMessage:
				s.log.Warn("slack_ignored", "reason", "malformed_or_unsupported_payload")
				// The SDK can reject an unsupported inner event before exposing a
				// Request. ACK only when the envelope itself can be decoded.
				if bad, ok := event.Data.(*socketmode.ErrorBadMessage); ok && verified {
					var req socketmode.Request
					if json.Unmarshal(bad.Message, &req) == nil {
						s.ack(ctx, req.EnvelopeID)
					}
				}
			default:
				if !verified || event.Request == nil {
					continue
				}
				if err := s.handle(ctx, *event.Request, sink); err != nil {
					return err
				}
			}
		}
	}
}

func (s *Source) handle(ctx context.Context, req socketmode.Request, sink gateway.EventSink) error {
	if req.EnvelopeID == "" {
		return nil
	}
	start := time.Now()
	if req.Type == socketmode.RequestTypeEventsAPI {
		m, reason := normalize(req.Payload, s.route, s.botUserID)
		if reason != "" {
			s.log.Info("slack_ignored", "reason", reason)
		} else {
			acceptCtx, cancel := context.WithTimeout(ctx, time.Second)
			result, err := sink.Accept(acceptCtx, m)
			cancel()
			if err != nil {
				return fmt.Errorf("%w; envelope not acknowledged", gateway.ErrIntakePersistence)
			}
			if !result.Accepted && !result.Duplicate && !result.Ignored {
				return fmt.Errorf("%w: intake returned no disposition; envelope not acknowledged", gateway.ErrIntakePersistence)
			}
			s.log.Info("slack_intake", "event_id", m.EventID, "workspace_id", m.WorkspaceID, "channel_id", m.ChannelID, "author_id", m.AuthorID, "duplicate", result.Duplicate, "ignored", result.Ignored)
		}
	}
	s.ack(ctx, req.EnvelopeID)
	s.log.Info("slack_envelope_handled", "duration_ms", time.Since(start).Milliseconds())
	return nil
}

func (s *Source) ack(ctx context.Context, id string) {
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if s.session.AckCtx(ctx, id, nil) != nil {
		s.log.Warn("slack_ack_failed")
	}
}
