package gateway

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type observationStore struct {
	Store
	writes, completed, closed int
	next                      time.Time
	reason                    string
	fail                      bool
}

func (s *observationStore) RecordObservation(context.Context, int64, string, time.Time) error {
	if s.fail {
		return errors.New("fixture storage failure")
	}
	s.writes++
	return nil
}
func (s *observationStore) SchedulePoll(_ context.Context, _ int64, at time.Time) error {
	s.next = at
	return nil
}
func (s *observationStore) Complete(context.Context, int64, string, string, []ReplyPart, ...ProtocolSnapshot) error {
	s.completed++
	return nil
}
func (s *observationStore) CloseThread(_ context.Context, _ int64, reason string) (bool, error) {
	s.closed++
	s.reason = reason
	return true, nil
}

func observationJob() *GatewayJob {
	return &GatewayJob{ID: 1, GatewayRequestID: "run", BackendSubmissionStartedAt: time.Now().Add(-time.Hour)}
}

func TestObservationHealthyQuietExecutionHasNoLifetimeLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &observationStore{}
		j := observationJob()
		w := Worker{Store: s}
		err := w.observeAttempt(t.Context(), j, "conversation", func(ctx context.Context, observe func(Turn) error) (Turn, error) {
			for range 10000 {
				if err := observe(Turn{ID: "run", State: "running"}); err != nil {
					return Turn{}, err
				}
			}
			if s.writes != 1 {
				t.Fatalf("duplicate observations wrote %d times", s.writes)
			}
			time.Sleep(18 * time.Minute)
			if ctx.Err() != nil {
				t.Fatalf("healthy execution was interrupted: %v", ctx.Err())
			}
			return Turn{ID: "run", State: "completed", Content: "done"}, nil
		})
		if err != nil || s.completed != 1 || s.closed != 0 || s.writes != 2 {
			t.Fatalf("err=%v store=%+v", err, s)
		}
	})
}

func TestObservationFirstConfirmationTimeoutStartsUncertainty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &observationStore{}
		j := observationJob()
		w := Worker{Store: s}
		started := time.Now()
		err := w.observeAttempt(t.Context(), j, "conversation", func(ctx context.Context, _ func(Turn) error) (Turn, error) {
			<-ctx.Done()
			return Turn{}, ctx.Err()
		})
		if err != nil || time.Since(started) != 120*time.Second || j.BackendObservationUncertainSince.IsZero() || s.closed != 0 || s.next.IsZero() {
			t.Fatalf("err=%v elapsed=%v job=%+v store=%+v", err, time.Since(started), j, s)
		}
	})
}

func TestObservationRetriesPreserveDeadlineAndRecoverFreshWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &observationStore{}
		j := observationJob()
		j.BackendObservationUncertainSince = time.Now().Add(-14 * time.Minute)
		j.BackendObservedState = "running"
		original := j.BackendObservationUncertainSince
		w := Worker{Store: s}
		for range 3 {
			if err := w.recordObservation(t.Context(), j, Turn{ID: "run", State: "unknown"}); err != nil {
				t.Fatal(err)
			}
			// A recreated worker gets the same persisted timestamp after restart.
			w = Worker{Store: s}
		}
		if s.writes != 0 || !j.BackendObservationUncertainSince.Equal(original) {
			t.Fatal("retry or restart renewed uncertainty")
		}
		if err := w.observeAttempt(t.Context(), j, "conversation", func(ctx context.Context, _ func(Turn) error) (Turn, error) {
			<-ctx.Done()
			return Turn{}, ctx.Err()
		}); err != nil {
			t.Fatal(err)
		}
		if !s.next.Equal(original.Add(15*time.Minute)) || s.closed != 0 {
			t.Fatal("final check was skipped or deadline moved")
		}
		w.finalSince = original
		w.finalDeadline = time.Now().Add(120 * time.Second)
		if err := w.observeAttempt(t.Context(), j, "conversation", func(ctx context.Context, observe func(Turn) error) (Turn, error) {
			if err := observe(Turn{ID: "run", State: "running"}); err != nil {
				return Turn{}, err
			}
			time.Sleep(18 * time.Minute)
			if ctx.Err() != nil {
				t.Fatal("final probe kept its timeout after healthy confirmation")
			}
			return Turn{}, errors.New("new disconnected observer")
		}); err != nil {
			t.Fatal(err)
		}
		if !j.BackendObservationUncertainSince.After(original) || s.closed != 0 || s.writes != 2 {
			t.Fatalf("new outage reused old budget: job=%+v store=%+v", j, s)
		}
	})
}

func TestObservationFinalProbeAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown_closes", true: "shutdown_preserves"}[shutdown], func(t *testing.T) {
			s := &observationStore{}
			j := observationJob()
			j.BackendObservationUncertainSince = time.Now().Add(-20 * time.Minute)
			original := j.BackendObservationUncertainSince
			w := Worker{Store: s, finalSince: original, finalDeadline: time.Now().Add(time.Minute)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if shutdown {
				cancel()
			}
			if err := w.observeAttempt(ctx, j, "conversation", func(context.Context, func(Turn) error) (Turn, error) { return Turn{}, errors.New("lost") }); err != nil {
				t.Fatal(err)
			}
			if !j.BackendObservationUncertainSince.Equal(original) || s.writes != 0 {
				t.Fatal("uncertainty was overwritten")
			}
			if shutdown && (s.closed != 0 || s.next.IsZero()) || !shutdown && (s.closed != 1 || s.reason != "reconciliation_deadline") {
				t.Fatalf("wrong final disposition: %+v", s)
			}
		})
	}
}

func TestObservationFailedWriteDoesNotAdvanceLocalSnapshot(t *testing.T) {
	s := &observationStore{fail: true}
	j := observationJob()
	w := Worker{Store: s}
	if err := w.recordObservation(t.Context(), j, Turn{ID: "run", State: "unknown"}); err == nil || !j.BackendObservationUncertainSince.IsZero() {
		t.Fatal("failed write changed local uncertainty")
	}
	s.fail = false
	if err := w.recordObservation(t.Context(), j, Turn{ID: "run", State: "unknown"}); err != nil || s.writes != 1 {
		t.Fatal("failed observation was suppressed on retry")
	}
}

type finalPreparationStore struct {
	observationStore
	job *GatewayJob
}

func (s *finalPreparationStore) ClaimJob(context.Context) (*GatewayJob, error) { return s.job, nil }
func (s *finalPreparationStore) GetBinding(context.Context, ThreadKey) (*ConversationBinding, error) {
	return &ConversationBinding{ConversationID: "conversation"}, nil
}
func (s *finalPreparationStore) CloseThread(ctx context.Context, id int64, reason string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return s.observationStore.CloseThread(ctx, id, reason)
}

type finalPreparationPolicy struct{}

func (finalPreparationPolicy) Authorize(ctx context.Context, _ Message, _ *ConversationBinding) (Decision, error) {
	<-ctx.Done()
	return Decision{}, ctx.Err()
}

func TestObservationFinalBudgetIncludesAuthorization(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		job := observationJob()
		job.BackendObservationUncertainSince = time.Now().Add(-20 * time.Minute)
		s := &finalPreparationStore{job: job}
		w := Worker{Store: s, Policy: finalPreparationPolicy{}}
		started := time.Now()
		worked, err := w.Step(t.Context())
		if err != nil || !worked || time.Since(started) != 120*time.Second || s.closed != 1 || s.reason != "reconciliation_deadline" {
			t.Fatalf("final preparation exceeded its bounded window: worked=%t err=%v elapsed=%v store=%+v", worked, err, time.Since(started), s)
		}
	})
}
