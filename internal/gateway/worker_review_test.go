package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

type reviewStore struct {
	Store
	err    error
	jobID  int64
	reason string
	calls  int
}

func (s *reviewStore) RecordObservation(context.Context, int64, string, time.Time) error { return nil }

func (s *reviewStore) Review(_ context.Context, jobID int64, reason string) error {
	s.calls++
	if s.err != nil {
		return s.err
	}
	s.jobID, s.reason = jobID, reason
	return nil
}

func TestReviewNotifiesOnlyAfterPersistence(t *testing.T) {
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
			j := &Job{ID: 42, RequestID: "request-1"}
			calls := 0
			w := Worker{Store: s, OnReview: func(jobID int64, requestID, reason string) {
				calls++
				if s.jobID != jobID || s.reason != reason || s.err != nil {
					t.Error("notification preceded persistence")
				}
				if jobID != j.ID || requestID != j.RequestID || reason != "backend_protocol_or_access_error" {
					t.Error("incorrect review metadata")
				}
			}}
			err := w.review(context.Background(), j, "backend_protocol_or_access_error")
			if !errors.Is(err, s.err) || s.calls != 1 {
				t.Fatalf("persistence result lost: %v (calls %d)", err, s.calls)
			}
			if (fail && calls != 0) || (!fail && calls != 1) {
				t.Fatalf("unexpected notification count: %d", calls)
			}
		})
	}
}

func TestSettleNotifiesReviewReasons(t *testing.T) {
	for _, tc := range []struct {
		name, turnID, state, reason string
		err                         error
	}{
		{"protocol", "m", "", "backend_protocol_or_access_error", ErrBackendProtocol},
		{"auth", "m", "", "backend_auth_or_identity_rejected", ErrBackendAuth},
		{"identity", "other", "completed", "turn_identity_mismatch", nil},
		{"interaction", "m", "waiting_for_user", "interaction_requires_review", nil},
		{"unsupported", "m", "future_state", "unsupported_turn_state", nil},
		{"deadline", "m", "running", "reconciliation_deadline", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &reviewStore{}
			calls := 0
			w := Worker{Store: s, OnReview: func(jobID int64, requestID, reason string) {
				calls++
				if jobID != 42 || requestID != "m" || reason != tc.reason {
					t.Error("incorrect review metadata")
				}
			}}
			err := w.settle(context.Background(), &Job{ID: 42, RequestID: "m"}, "conversation", Turn{ID: tc.turnID, State: tc.state}, tc.err)
			if tc.err == ErrBackendAuth {
				if !errors.Is(err, ErrBackendAuth) {
					t.Fatal("fatal auth failure suppressed")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || s.calls != 1 || s.reason != tc.reason {
				t.Fatal("review transition not reported exactly once")
			}
		})
	}
}
