package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	DefaultStatusRefresh = 30 * time.Second
	StatusRetry          = 30 * time.Second
	AgentSessionRefresh  = 10 * time.Minute
)

type StatusMode string

const (
	StatusMessage      StatusMode = "message"
	StatusAgentSession StatusMode = "agent_session"
	StatusHybrid       StatusMode = "hybrid"
)

// StatusTarget identifies one concrete operation, independently of the question's
// persisted presentation mode. Hybrid questions publish to both targets.
type StatusTarget string

const (
	StatusTargetMessage      StatusTarget = "message"
	StatusTargetAgentSession StatusTarget = "agent_session"
)

// StatusReceipt separates a message timestamp from a session acknowledgement.
// AgentStatus is our app's state; SessionStatus may include other apps' work.
type StatusReceipt struct {
	SlackTS                             string
	Acknowledged                        bool
	AgentStatus, SessionStatus, Warning string
}

// GatewayJobProgress is a claimed snapshot. Destination and author must be checked
// against the durable job before any Slack operation, including updates.
type GatewayJobProgress struct {
	IntegrationID   int64
	JobID           int64
	Mode            StatusMode
	Target          StatusTarget
	Surface         MessageSurface
	InitiatorUserID string
	SessionState    string
	ThreadKey
	MessageTS     string
	SlackTS       string
	Phase         string
	Revision      int64
	CreatedAt     time.Time
	ObservedState string
	ObservedAt    time.Time
	Terminal      bool
	Submitted     bool
}

type StatusSender interface {
	SendStatus(context.Context, GatewayJobProgress, string) (StatusReceipt, error)
}

type StatusResult struct {
	SlackTS                             string
	Acknowledged                        bool
	AgentStatus, SessionStatus, Warning string
	State                               string // ready, settled, failed, delivery_unknown
	Reason                              string
	RetryAt                             time.Time
	RateLimitedUntil                    time.Time
	NextRefreshAt                       time.Time
}

type StatusStore interface {
	RecordObservation(context.Context, int64, string, time.Time) error
	// kind is urgent, heartbeat, backend_recovering, backend_blocked, or
	// integration_terminal, or terminal (legacy authentication shutdown only).
	ClaimStatus(context.Context, time.Time, string) (*GatewayJobProgress, error)
	GetStatusContext(context.Context, GatewayJobProgress) (*DeliveryContext, error)
	FinishStatus(context.Context, GatewayJobProgress, StatusResult, time.Time) error
}

func StatusText(s GatewayJobProgress, now time.Time) string {
	if ConversationClosed(s.Phase) {
		return "This Slack thread is closed for further requests. Start a new thread to continue."
	}
	text := map[string]string{
		"received":            "Received your request. Waiting to start.",
		"queued":              "Your request is queued behind an earlier request in this thread.",
		"blocked":             "This request is blocked by an earlier request that needs operator attention.",
		"contacting":          "Contacting Viant Steward…",
		"running":             "Viant Steward is working on your request.",
		"backend_queued":      "Viant Steward has queued your request.",
		"backend_recovering":  "Connection to Viant Steward is temporarily unavailable. We are checking your existing request; it will not be submitted again.",
		"backend_blocked":     "Connection to Viant Steward requires administrator attention. Your existing request will not be submitted again.",
		"not_submitted":       "This question was not submitted to Viant Steward. Please try again after the connection is restored.",
		"checking":            "Checking your existing request; it will not be submitted again.",
		"delivering":          "The response is ready. Delivering it to this thread…",
		"delivered":           "The response has been posted below.",
		"completed":           "Completed. The response has been posted below.",
		"failed":              "Viant Steward could not complete this request.",
		"canceled":            "Viant Steward canceled this request.",
		"needs_review":        "This request needs operator attention. It will not be retried automatically.",
		"delivery_uncertain":  "The response could not be fully confirmed as delivered. Operator attention is required.",
		"integration_changed": IntegrationNotice("integration_changed"),
		"integration_removed": IntegrationNotice("integration_removed"),
	}[s.Phase]
	if text == "" {
		text = "Checking the request status."
	}
	elapsed := max(time.Duration(0), now.Sub(s.CreatedAt)).Truncate(time.Second)
	return RenderText(fmt.Sprintf("%s Elapsed: %s.", text, elapsed))[0].Text
}

type StatusWorker struct {
	Store           Store
	Sender          StatusSender
	Policy          DeliveryPolicy
	Backend         BackendAvailability
	Now             func() time.Time
	RefreshInterval time.Duration
	OnStatus        func(GatewayJobProgress, StatusResult, time.Time)
}

func (w *StatusWorker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *StatusWorker) Step(ctx context.Context, kind string) (bool, error) {
	var prepared *PreparedOutbound
	err := InThread(ctx, w.Store, func(local Store) error {
		var prepareErr error
		prepared, prepareErr = w.Prepare(ctx, local, kind)
		return prepareErr
	})
	if err != nil || prepared == nil {
		return false, err
	}
	return true, commitOutbound(ctx, w.Store, prepared.Send(ctx))
}

// Prepare performs only native claims/context reads. Policy and Slack calls are
// deferred until Send, after the owner and its DB executor have been released.
func (w *StatusWorker) Prepare(ctx context.Context, local Store, kind string) (*PreparedOutbound, error) {
	if w.Backend != nil && kind != "terminal" && kind != "integration_terminal" {
		switch w.Backend.State() {
		case BackendStarting, BackendRecovering:
			kind = "backend_recovering"
		case BackendBlocked:
			kind = "backend_blocked"
		}
	}
	claimed, err := local.ClaimStatus(ctx, w.now(), kind)
	if err != nil || claimed == nil {
		return nil, err
	}
	s := *claimed
	metadata, err := local.GetStatusContext(ctx, s)
	if err != nil {
		prepareErr := err
		return &PreparedOutbound{send: func(context.Context) OutboundResult {
			return w.statusNotSent(s, prepareErr)
		}}, nil
	}
	return &PreparedOutbound{send: func(sendCtx context.Context) OutboundResult {
		if err := sendCtx.Err(); err != nil {
			return w.statusNotSent(s, err)
		}
		var policyErr error
		if w.Policy == nil {
			policyErr = ErrMetadataUnavailable
		} else {
			// Preserve the original author and verify every status operation.
			policyErr = w.Policy.AuthorizeDelivery(sendCtx, *metadata, true)
		}
		if policyErr != nil {
			if sendCtx.Err() != nil {
				return w.statusNotSent(s, sendCtx.Err())
			}
			result := StatusResult{State: "ready", Reason: "policy_unavailable", RetryAt: w.now().Add(StatusRetry)}
			var cause error = ErrMetadataUnavailable
			if errors.Is(policyErr, ErrDenied) {
				result.State, result.Reason, cause = "failed", "policy_denied", nil
			} else if errors.Is(policyErr, ErrSlackAuth) {
				cause = ErrSlackAuth
			}
			return w.statusResult(s, result, cause)
		}
		if err := sendCtx.Err(); err != nil {
			return w.statusNotSent(s, err)
		}
		text := ""
		visible := s
		if w.Backend != nil && s.Submitted && !s.Terminal && s.Phase != "delivering" {
			switch w.Backend.State() {
			case BackendStarting, BackendRecovering:
				visible.Phase = "backend_recovering"
			case BackendBlocked:
				visible.Phase = "backend_blocked"
			}
		}
		if s.Target != StatusTargetAgentSession {
			text = StatusText(visible, w.now())
		}
		receipt, sendErr := w.Sender.SendStatus(sendCtx, s, text)
		result := StatusResult{State: "ready", SlackTS: receipt.SlackTS, Acknowledged: receipt.Acknowledged,
			AgentStatus: receipt.AgentStatus, SessionStatus: receipt.SessionStatus, Warning: receipt.Warning}
		var fatal error
		valid := receipt.SlackTS != "" && (s.SlackTS == "" || receipt.SlackTS == s.SlackTS)
		if s.Target == StatusTargetAgentSession {
			valid = receipt.Acknowledged && receipt.SlackTS == ""
		}
		if sendErr == nil && valid {
			if s.Terminal {
				result.State = "settled"
			}
		} else {
			result.SlackTS, result.Acknowledged, result.AgentStatus, result.SessionStatus, result.Warning = "", false, "", "", ""
			result.Reason, result.RetryAt = "send_unknown", w.now().Add(StatusRetry)
			if s.Target != StatusTargetAgentSession && s.SlackTS == "" {
				result.State = "delivery_unknown"
			}
			var classified *SendError
			if errors.As(sendErr, &classified) {
				switch classified.Kind {
				case SendRateLimited:
					if classified.RetryAfter > 0 {
						result.State, result.Reason, result.RetryAt = "ready", "rate_limited", w.now().Add(classified.RetryAfter)
						result.RateLimitedUntil = result.RetryAt
					}
				case SendCredentials:
					result.State, result.Reason, fatal = "ready", "slack_auth_rejected", ErrSlackAuth
				case SendRejected:
					result.State, result.Reason = "failed", "slack_rejected"
					if classified.Code != "" {
						result.Reason = classified.Code
					}
				}
			}
		}
		return w.statusResult(s, result, fatal)
	}}, nil
}

func (w *StatusWorker) statusNotSent(s GatewayJobProgress, cause error) OutboundResult {
	return w.statusResult(s, StatusResult{State: "ready", Reason: "send_not_started", RetryAt: w.now()}, cause)
}

func (w *StatusWorker) statusResult(s GatewayJobProgress, result StatusResult, cause error) OutboundResult {
	return OutboundResult{Err: cause, NotBefore: result.RateLimitedUntil, commit: func(ctx context.Context, local Store) error {
		now := w.now()
		if result.SlackTS != "" || result.Acknowledged {
			interval := w.RefreshInterval
			if interval <= 0 {
				interval = DefaultStatusRefresh
			}
			if s.Target == StatusTargetAgentSession {
				interval = AgentSessionRefresh
			}
			result.NextRefreshAt = now.Add(interval)
		}
		if err := local.FinishStatus(ctx, s, result, now); err != nil {
			return err
		}
		if w.OnStatus != nil {
			w.OnStatus(s, result, now)
		}
		return nil
	}}
}

// OutboundWorker owns every Slack write. Urgent status changes alternate with
// final parts; heartbeats run only when neither higher priority group is ready.
// The Store gates both groups using durable workspace Retry-After deadlines.
type OutboundWorker struct {
	SlackOutgoingMessage *DeliveryWorker
	Status               *StatusWorker
	Interval             time.Duration
	Now                  func() time.Time
	nextSend             time.Time
	preferStatus         bool
}

func (w *OutboundWorker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// NextReadyAt is read by the dispatch owner only between Step calls.
func (w *OutboundWorker) NextReadyAt() time.Time { return w.nextSend }

func (w *OutboundWorker) Step(ctx context.Context) (bool, error) {
	if w.now().Before(w.nextSend) {
		return false, nil
	}
	store := w.SlackOutgoingMessage.Store
	var prepared *PreparedOutbound
	err := InThread(ctx, store, func(local Store) error {
		var prepareErr error
		prepared, prepareErr = w.Prepare(ctx, local)
		return prepareErr
	})
	if err != nil || prepared == nil {
		return false, err
	}
	result := prepared.Send(ctx)
	w.nextSend = result.NotBefore
	return true, commitOutbound(ctx, store, result)
}

// Prepare retains existing priorities/fairness using only local native work.
// The process bot coordinator owns pacing while the returned operation runs.
func (w *OutboundWorker) Prepare(ctx context.Context, local Store) (*PreparedOutbound, error) {
	prepared, err := w.Status.Prepare(ctx, local, "integration_terminal")
	if prepared != nil || err != nil {
		return w.paced(prepared), err
	}
	if w.preferStatus {
		prepared, err = w.Status.Prepare(ctx, local, "urgent")
		if prepared != nil || err != nil {
			w.preferStatus = false
			return w.paced(prepared), err
		}
	}
	prepared, err = w.SlackOutgoingMessage.Prepare(ctx, local)
	if prepared != nil || err != nil {
		w.preferStatus = true
		return w.paced(prepared), err
	}
	prepared, err = w.Status.Prepare(ctx, local, "urgent")
	if prepared != nil || err != nil {
		w.preferStatus = false
		return w.paced(prepared), err
	}
	prepared, err = w.Status.Prepare(ctx, local, "heartbeat")
	return w.paced(prepared), err
}

func (w *OutboundWorker) paced(prepared *PreparedOutbound) *PreparedOutbound {
	if prepared == nil {
		return nil
	}
	return &PreparedOutbound{send: func(ctx context.Context) OutboundResult {
		result := prepared.Send(ctx)
		if next := w.now().Add(w.Interval); result.NotBefore.Before(next) {
			result.NotBefore = next
		}
		return result
	}}
}

// TerminalStep is used only after normal workers have joined during a backend
// authentication shutdown. It cannot claim queries, final replies or heartbeats.
func (w *OutboundWorker) TerminalStep(ctx context.Context) (bool, error) {
	if delay := w.nextSend.Sub(w.now()); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	worked, err := w.Status.Step(ctx, "terminal")
	if worked {
		w.nextSend = w.now().Add(w.Interval)
	}
	return worked, err
}
