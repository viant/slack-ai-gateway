package gateway

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SendErrorKind string

const (
	SendRateLimited SendErrorKind = "rate_limited"
	SendRejected    SendErrorKind = "rejected"
	SendCredentials SendErrorKind = "credentials"
	SendUnknown     SendErrorKind = "unknown"
)

// SendError never retains upstream errors, bodies, URLs, credentials or content.
type SendError struct {
	Code       string // bounded, known API error code only
	Kind       SendErrorKind
	RetryAfter time.Duration
}

func (e *SendError) Error() string { return "Slack delivery: " + string(e.Kind) }

// DeliveryIssue denotes a durably quarantined delivery, not a fatal runtime
// error. Other threads can continue. Store errors and credential failures stop
// the runtime instead of leaving an invisible claimed job behind.
type DeliveryIssue struct {
	ID     int64
	State  string
	Reason string
}

func (e *DeliveryIssue) Error() string { return fmt.Sprintf("delivery %d: %s", e.ID, e.State) }

type DeliveryWorker struct {
	Store  Store
	Sender SlackSender
	Policy DeliveryPolicy
	// Interval is zero for isolated worker tests; the runtime enforces >=1s.
	Interval    time.Duration
	OnDelivered func(SlackOutgoingMessage)
	nextSend    time.Time
	verifiedJob int64
	verifiedAt  time.Time
}

// Step is owned by a single goroutine. Rate limiting happens before the claim,
// so waiting does not turn a safe pending message into an uncertain send.
func (w *DeliveryWorker) Step(ctx context.Context) (bool, error) {
	if time.Now().Before(w.nextSend) {
		return false, nil
	}
	d, err := w.Store.ClaimDelivery(ctx)
	if err != nil || d == nil {
		return false, err
	}
	if w.Policy != nil {
		metadata, err := w.Store.GetDeliveryContext(ctx, *d)
		if err != nil {
			return true, err
		}
		fresh := w.verifiedJob != d.JobID || time.Since(w.verifiedAt) >= time.Minute
		if err := w.Policy.AuthorizeDelivery(ctx, *metadata, fresh); err != nil {
			saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer cancel()
			if errors.Is(err, ErrDenied) {
				if err := w.Store.FailDelivery(saveCtx, d.ID); err != nil {
					return true, err
				}
				return true, &DeliveryIssue{ID: d.ID, State: "failed", Reason: "policy_denied"}
			}
			// No chat.postMessage occurred. Keep the exact part pending and stop
			// the runtime instead of claiming that delivery was uncertain.
			if saveErr := w.Store.RetryDelivery(saveCtx, d.ID, time.Now()); saveErr != nil {
				return true, saveErr
			}
			if errors.Is(err, ErrSlackAuth) {
				return true, ErrSlackAuth
			}
			return true, ErrMetadataUnavailable
		}
		if fresh {
			w.verifiedJob, w.verifiedAt = d.JobID, time.Now()
		}
	}
	ts, sendErr := w.Sender.Send(ctx, *d)
	w.nextSend = time.Now().Add(w.Interval)
	// Persist the result even during cancellation. A crash/write failure still
	// leaves 'sending', which Recover conservatively quarantines on restart.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if sendErr == nil && ts != "" {
		if err := w.Store.Delivered(saveCtx, d.ID, ts); err != nil {
			return true, err
		}
		d.SlackTS = ts
		if w.OnDelivered != nil {
			w.OnDelivered(*d)
		}
		return true, nil
	}
	var classified *SendError
	if errors.As(sendErr, &classified) {
		switch classified.Kind {
		case SendRateLimited:
			if classified.RetryAfter > 0 {
				return true, w.Store.RetryDelivery(saveCtx, d.ID, time.Now().Add(classified.RetryAfter))
			}
		case SendCredentials:
			if err := w.Store.RetryDelivery(saveCtx, d.ID, time.Now()); err != nil {
				return true, err
			}
			return true, &SendError{Kind: SendCredentials}
		case SendRejected:
			if err := w.Store.FailDelivery(saveCtx, d.ID); err != nil {
				return true, err
			}
			return true, &DeliveryIssue{ID: d.ID, State: "failed"}
		}
	}
	if err := w.Store.DeliveryUnknown(saveCtx, d.ID); err != nil {
		return true, err
	}
	return true, &DeliveryIssue{ID: d.ID, State: "delivery_unknown"}
}
