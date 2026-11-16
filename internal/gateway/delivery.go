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

// Step composes the same prepare/send/commit stages used by the process
// scheduler. Only the native stages run under thread ownership.
func (w *DeliveryWorker) Step(ctx context.Context) (bool, error) {
	if time.Now().Before(w.nextSend) {
		return false, nil
	}
	var prepared *PreparedOutbound
	err := InThread(ctx, w.Store, func(local Store) error {
		var prepareErr error
		prepared, prepareErr = w.Prepare(ctx, local)
		return prepareErr
	})
	if err != nil || prepared == nil {
		return false, err
	}
	result := prepared.Send(ctx)
	w.nextSend = result.NotBefore
	return true, commitOutbound(ctx, w.Store, result)
}

// Prepare claims and captures authorization metadata. The returned closures do
// not retain local: a local native facade is valid only during this command.
func (w *DeliveryWorker) Prepare(ctx context.Context, local Store) (*PreparedOutbound, error) {
	claimed, err := local.ClaimDelivery(ctx)
	if err != nil || claimed == nil {
		return nil, err
	}
	d := *claimed
	var metadata *DeliveryContext
	if w.Policy != nil {
		metadata, err = local.GetDeliveryContext(ctx, d)
	}
	if err != nil {
		prepareErr := err
		return &PreparedOutbound{send: func(context.Context) OutboundResult {
			return deliveryNotSent(d, prepareErr)
		}}, nil
	}
	fresh := w.verifiedJob != d.JobID || time.Since(w.verifiedAt) >= time.Minute
	return &PreparedOutbound{send: func(sendCtx context.Context) OutboundResult {
		if err := sendCtx.Err(); err != nil {
			return deliveryNotSent(d, err)
		}
		if w.Policy != nil {
			if err := w.Policy.AuthorizeDelivery(sendCtx, *metadata, fresh); err != nil {
				if sendCtx.Err() != nil {
					return deliveryNotSent(d, sendCtx.Err())
				}
				if errors.Is(err, ErrDenied) {
					return OutboundResult{Err: &DeliveryIssue{ID: d.ID, State: "failed", Reason: "policy_denied"},
						commit: func(ctx context.Context, local Store) error { return local.FailDelivery(ctx, d.ID) }}
				}
				cause := ErrMetadataUnavailable
				if errors.Is(err, ErrSlackAuth) {
					cause = ErrSlackAuth
				}
				return deliveryNotSent(d, cause)
			}
			if fresh {
				w.verifiedJob, w.verifiedAt = d.JobID, time.Now()
			}
		}
		if err := sendCtx.Err(); err != nil {
			return deliveryNotSent(d, err)
		}
		ts, sendErr := w.Sender.Send(sendCtx, d)
		now := time.Now()
		result := OutboundResult{NotBefore: now.Add(w.Interval)}
		if sendErr == nil && ts != "" {
			result.commit = func(ctx context.Context, local Store) error {
				if err := local.Delivered(ctx, d.ID, ts); err != nil {
					return err
				}
				if w.OnDelivered != nil {
					delivered := d
					delivered.SlackTS = ts
					w.OnDelivered(delivered)
				}
				return nil
			}
			return result
		}
		var classified *SendError
		if errors.As(sendErr, &classified) {
			switch classified.Kind {
			case SendRateLimited:
				if classified.RetryAfter > 0 {
					retry := now.Add(classified.RetryAfter)
					if retry.After(result.NotBefore) {
						result.NotBefore = retry
					}
					result.commit = func(ctx context.Context, local Store) error { return local.RetryDelivery(ctx, d.ID, retry) }
					return result
				}
			case SendCredentials:
				result.Err = &SendError{Kind: SendCredentials}
				result.commit = func(ctx context.Context, local Store) error { return local.RetryDelivery(ctx, d.ID, now) }
				return result
			case SendRejected:
				result.Err = &DeliveryIssue{ID: d.ID, State: "failed"}
				result.commit = func(ctx context.Context, local Store) error { return local.FailDelivery(ctx, d.ID) }
				return result
			}
		}
		result.Err = &DeliveryIssue{ID: d.ID, State: "delivery_unknown"}
		result.commit = func(ctx context.Context, local Store) error { return local.DeliveryUnknown(ctx, d.ID) }
		return result
	}}, nil
}

func deliveryNotSent(d SlackOutgoingMessage, cause error) OutboundResult {
	retry := time.Now()
	return OutboundResult{Err: cause, commit: func(ctx context.Context, local Store) error {
		return local.RetryDelivery(ctx, d.ID, retry)
	}}
}
