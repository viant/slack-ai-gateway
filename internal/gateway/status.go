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
	ObservationMaxAge    = 60 * time.Second
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
		"checking":            "Checking the request status.",
		"delivering":          "The response is ready. Delivering it to this thread…",
		"delivered":           "The response has been posted below.",
		"completed":           "Completed. The response has been posted below.",
		"failed":              "Viant Steward could not complete this request.",
		"canceled":            "Viant Steward canceled this request.",
		"needs_review":        "This request needs operator attention. It will not be retried automatically.",
		"delivery_uncertain":  "The response could not be fully confirmed as delivered. Operator attention is required.",
		"integration_changed": IntegrationNotice("integration_changed"),
		"integration_removed": IntegrationNotice("integration_removed"),
		"conversation_closed": "This conversation is closed. Start a new thread to continue.",
	}[s.Phase]
	if text == "" {
		text = "Checking the request status."
	}
	if s.Phase == "checking" && s.ObservedState != "" {
		text += fmt.Sprintf(" Last confirmed state: %s at %s.", s.ObservedState, s.ObservedAt.UTC().Format(time.RFC3339))
	}
	elapsed := max(time.Duration(0), now.Sub(s.CreatedAt)).Truncate(time.Second)
	return RenderText(fmt.Sprintf("%s Elapsed: %s.", text, elapsed))[0].Text
}

type StatusWorker struct {
	Store           StatusStore
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
	if w.Backend != nil && kind != "terminal" && kind != "integration_terminal" {
		switch w.Backend.State() {
		case BackendStarting, BackendRecovering:
			kind = "backend_recovering"
		case BackendBlocked:
			kind = "backend_blocked"
		}
	}
	s, err := w.Store.ClaimStatus(ctx, w.now(), kind)
	if err != nil || s == nil {
		return false, err
	}
	metadata, err := w.Store.GetStatusContext(ctx, *s)
	if err != nil {
		return true, err
	}
	// Never substitute the shared execution principal for the original author.
	// Recheck for every operation, without the final-part verification shortcut.
	var policyErr error
	if w.Policy == nil {
		policyErr = ErrMetadataUnavailable
	} else {
		policyErr = w.Policy.AuthorizeDelivery(ctx, *metadata, true)
	}
	if err := policyErr; err != nil {
		result := StatusResult{State: "ready", Reason: "policy_unavailable", RetryAt: w.now().Add(StatusRetry)}
		if errors.Is(err, ErrDenied) {
			result.State, result.Reason = "failed", "policy_denied"
		}
		saveErr := w.finish(ctx, *s, result)
		if saveErr != nil {
			return true, saveErr
		}
		if errors.Is(err, ErrDenied) {
			return true, nil
		}
		if errors.Is(err, ErrSlackAuth) {
			return true, ErrSlackAuth
		}
		return true, ErrMetadataUnavailable
	}
	text := ""
	visible := *s
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
	receipt, sendErr := w.Sender.SendStatus(ctx, *s, text)
	result := StatusResult{State: "ready", SlackTS: receipt.SlackTS, Acknowledged: receipt.Acknowledged, AgentStatus: receipt.AgentStatus, SessionStatus: receipt.SessionStatus, Warning: receipt.Warning}
	ts := receipt.SlackTS
	var fatal error
	valid := ts != "" && (s.SlackTS == "" || ts == s.SlackTS)
	if s.Target == StatusTargetAgentSession {
		valid = receipt.Acknowledged && ts == ""
	}
	if sendErr == nil && valid {
		if s.Terminal {
			result.State = "settled"
		}
	} else {
		result.SlackTS, result.Acknowledged, result.AgentStatus, result.SessionStatus, result.Warning = "", false, "", "", ""
		result.Reason = "send_unknown"
		// Retrying an update to a known timestamp is safe. A first POST is not.
		result.RetryAt = w.now().Add(StatusRetry)
		if s.Target != StatusTargetAgentSession && s.SlackTS == "" {
			result.State = "delivery_unknown"
		}
		var classified *SendError
		if errors.As(sendErr, &classified) {
			switch classified.Kind {
			case SendRateLimited:
				if classified.RetryAfter > 0 {
					result.State, result.Reason = "ready", "rate_limited"
					result.RetryAt = w.now().Add(classified.RetryAfter)
					result.RateLimitedUntil = result.RetryAt
				}
			case SendCredentials:
				result.State, result.Reason = "ready", "slack_auth_rejected"
				fatal = ErrSlackAuth
			case SendRejected:
				result.State, result.Reason = "failed", "slack_rejected"
				if classified.Code != "" {
					result.Reason = classified.Code
				}
			}
		}
	}
	if err := w.finish(ctx, *s, result); err != nil {
		return true, err
	}
	return true, fatal
}

func (w *StatusWorker) finish(ctx context.Context, s GatewayJobProgress, result StatusResult) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
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
	if err := w.Store.FinishStatus(saveCtx, s, result, now); err != nil {
		return err
	}
	if w.OnStatus != nil {
		w.OnStatus(s, result, now)
	}
	return nil
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

func (w *OutboundWorker) Step(ctx context.Context) (worked bool, err error) {
	if w.now().Before(w.nextSend) {
		return false, nil
	}
	defer func() {
		if worked {
			w.nextSend = w.now().Add(w.Interval)
		}
	}()
	worked, err = w.Status.Step(ctx, "integration_terminal")
	if worked || err != nil {
		return
	}
	if w.preferStatus {
		worked, err = w.Status.Step(ctx, "urgent")
		if worked || err != nil {
			w.preferStatus = false
			return
		}
	}
	worked, err = w.SlackOutgoingMessage.Step(ctx)
	if worked || err != nil {
		w.preferStatus = true
		return
	}
	worked, err = w.Status.Step(ctx, "urgent")
	if worked || err != nil {
		w.preferStatus = false
		return
	}
	return w.Status.Step(ctx, "heartbeat")
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
