package gateway

import (
	"context"
	"sync"
	"time"
)

func (w Worker) uncertaintyExpired(j *GatewayJob, now time.Time) bool {
	return !j.BackendObservationUncertainSince.IsZero() && !now.Before(j.BackendObservationUncertainSince.Add(w.Timing.defaults().MaxWait))
}

func (w Worker) recordObservation(ctx context.Context, j *GatewayJob, turn Turn) error {
	if turn.ID != j.GatewayRequestID {
		return ErrBackendProtocol
	}
	switch turn.State {
	case "unknown", "queued", "running", "completed", "failed", "canceled", "waiting_for_user":
	default:
		return ErrBackendProtocol
	}
	unknown := turn.State == "unknown"
	if unknown && !j.BackendObservationUncertainSince.IsZero() {
		return nil
	}
	if !unknown && turn.State == j.BackendObservedState && j.BackendObservationUncertainSince.IsZero() {
		return nil
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	if err := w.Store.RecordObservation(ctx, j.ID, turn.State, now); err != nil {
		return err
	}
	previous := j.BackendObservationUncertainSince
	if unknown {
		j.BackendObservationUncertainSince = now
	} else {
		j.BackendObservedState, j.BackendObservedAt = turn.State, now
		j.BackendObservationUncertainSince = time.Time{}
	}
	if w.Log != nil && previous.IsZero() != j.BackendObservationUncertainSince.IsZero() {
		attrs := []any{"job_id", j.ID, "gateway_request_id", j.GatewayRequestID, "uncertain", unknown, "observed_state", j.BackendObservedState}
		if unknown {
			attrs = append(attrs, "uncertain_since", now, "deadline", now.Add(w.Timing.defaults().MaxWait))
		} else {
			attrs = append(attrs, "uncertain_since", previous, "source", "current_backend_confirmation")
		}
		w.Log.Info("backend_observation", attrs...)
	}
	return nil
}

// Only the first current confirmation has a timeout. A confirmed quiet stream
// retains the runtime context for as long as the backend execution needs.
func (w Worker) observeAttempt(ctx context.Context, j *GatewayJob, conversation string, run func(context.Context, func(Turn) error) (Turn, error)) error {
	attempt, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	budget := w.Timing.defaults().Timeout
	if !w.finalDeadline.IsZero() {
		budget = min(budget, time.Until(w.finalDeadline))
	} else if !j.BackendObservationUncertainSince.IsZero() {
		budget = min(budget, time.Until(j.BackendObservationUncertainSince.Add(w.Timing.defaults().MaxWait)))
	}
	var first sync.Once
	timer := time.AfterFunc(max(0, budget), func() { first.Do(func() { cancel(context.DeadlineExceeded) }) })
	defer timer.Stop()
	observe := func(turn Turn) error {
		if turn.ID != j.GatewayRequestID {
			return ErrBackendProtocol
		}
		if turn.State != "unknown" {
			first.Do(func() { timer.Stop() })
		}
		if attempt.Err() != nil {
			return context.Cause(attempt)
		}
		return w.recordObservation(ctx, j, turn)
	}
	turn, err := run(attempt, observe)
	return w.settle(ctx, j, conversation, turn, err)
}

// Final verification may wait for the existing manager, without starting an
// authentication attempt or reserving a database connection while it waits.
func (w Worker) waitForBackend(ctx context.Context) error {
	if w.Backend == nil {
		return nil
	}
	wait, cancel := context.WithDeadline(ctx, w.finalDeadline)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		switch w.Backend.State() {
		case BackendReady:
			return nil
		case BackendBlocked:
			return ErrBackendAuth
		}
		select {
		case <-wait.Done():
			return wait.Err()
		case <-ticker.C:
		}
	}
}
