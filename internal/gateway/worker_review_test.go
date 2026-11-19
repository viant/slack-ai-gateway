package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

type reviewStore struct {
	Store
	err           error
	jobID         int64
	reason        string
	calls         int
	alreadyClosed bool
}

func (s *reviewStore) RecordObservation(context.Context, int64, string, time.Time) error { return nil }

func (s *reviewStore) CloseThread(_ context.Context, jobID int64, reason string) (bool, error) {
	s.calls++
	if s.err != nil {
		return false, s.err
	}
	s.jobID, s.reason = jobID, reason
	return !s.alreadyClosed, nil
}

func TestThreadClosureNotifiesOnlyAfterPersistence(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "persisted"
		if fail {
			name = "persistence_failed"
		}
		t.Run(name, func(t *testing.T) {
			s := &reviewStore{}
			if fail {
				s.err = errors.New("storage unavailable")
			}
			j := &GatewayJob{ID: 42, GatewayRequestID: "request-1"}
			calls := 0
			w := Worker{Store: s, OnThreadClosed: func(jobID int64, requestID, reason string) {
				calls++
				if s.jobID != jobID || s.reason != reason || s.err != nil {
					t.Error("notification preceded persistence")
				}
				if jobID != j.ID || requestID != j.GatewayRequestID || reason != "backend_protocol_or_access_error" {
					t.Error("incorrect closure metadata")
				}
			}}
			err := w.closeThread(context.Background(), j, "backend_protocol_or_access_error")
			if !errors.Is(err, s.err) || s.calls != 1 {
				t.Fatalf("persistence result lost: %v (calls %d)", err, s.calls)
			}
			if (fail && calls != 0) || (!fail && calls != 1) {
				t.Fatalf("unexpected notification count: %d", calls)
			}
		})
	}
}

func TestThreadClosureDoesNotNotifyForAlreadyClosed(t *testing.T) {
	s := &reviewStore{alreadyClosed: true}
	w := Worker{Store: s, OnThreadClosed: func(int64, string, string) {
		t.Error("unchanged closure produced another notification")
	}}
	if err := w.closeThread(context.Background(), &GatewayJob{ID: 42}, "reconciliation_deadline"); err != nil {
		t.Fatal(err)
	}
	if s.calls != 1 {
		t.Fatal("closure did not consult durable state")
	}
}

func TestSettleNotifiesThreadClosureReasons(t *testing.T) {
	for _, tc := range []struct {
		name, turnID, state, reason string
		err                         error
	}{
		{"protocol", "m", "", "backend_protocol_or_access_error", ErrBackendProtocol},
		{"auth", "m", "", "backend_auth_or_identity_rejected", ErrBackendAuth},
		{"identity", "other", "completed", "turn_identity_mismatch", nil},
		{"interaction", "m", "waiting_for_user", "interaction_requires_review", nil},
		{"unsupported", "m", "future_state", "unsupported_turn_state", nil},
		{"deadline", "m", "unknown", "reconciliation_deadline", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &reviewStore{}
			calls := 0
			w := Worker{Store: s, OnThreadClosed: func(jobID int64, requestID, reason string) {
				calls++
				if jobID != 42 || requestID != "m" || reason != tc.reason {
					t.Error("incorrect closure metadata")
				}
			}}
			job := &GatewayJob{ID: 42, GatewayRequestID: "m"}
			if tc.name == "deadline" {
				job.BackendObservationUncertainSince = time.Now().Add(-16 * time.Minute)
				w.finalSince = job.BackendObservationUncertainSince
			}
			err := w.settle(context.Background(), job, "conversation", Turn{ID: tc.turnID, State: tc.state}, tc.err)
			if tc.err == ErrBackendAuth {
				if !errors.Is(err, ErrBackendAuth) {
					t.Fatal("fatal auth failure suppressed")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || s.calls != 1 || s.reason != tc.reason {
				t.Fatal("closure transition not reported exactly once")
			}
		})
	}
}
