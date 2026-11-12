package gateway

import (
	"context"
	"testing"
	"time"
)

type pollStore struct {
	Store
	at     time.Time
	reason string
}

func (s *pollStore) SchedulePoll(_ context.Context, _ int64, at time.Time) error {
	s.at = at
	return nil
}
func (s *pollStore) Review(_ context.Context, _ int64, reason string) error {
	s.reason = reason
	return nil
}

func TestPollingDefaultsBackoffAndDeadline(t *testing.T) {
	d := (QueryTiming{}).defaults()
	if d.Timeout != 120*time.Second || d.PollInitial != 2*time.Second || d.PollMax != 15*time.Second || d.MaxWait != 15*time.Minute {
		t.Fatal("unexpected production timing")
	}
	for n, want := range []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second} {
		s := &pollStore{}
		w := Worker{Store: s}
		now := time.Now()
		if err := w.poll(context.Background(), &GatewayJob{BackendSubmissionStartedAt: now, PollAttempts: n}); err != nil {
			t.Fatal(err)
		}
		if got := s.at.Sub(now); got < want || got > want+100*time.Millisecond {
			t.Fatalf("attempt %d delay %v, want %v", n, got, want)
		}
	}
	s := &pollStore{}
	w := Worker{Store: s}
	started := time.Now().Add(-15*time.Minute + time.Second)
	if err := w.poll(context.Background(), &GatewayJob{BackendSubmissionStartedAt: started, PollAttempts: 100}); err != nil {
		t.Fatal(err)
	}
	if !s.at.Equal(started.Add(15 * time.Minute)) {
		t.Fatal("poll exceeds deadline")
	}
	s.at = time.Time{}
	if err := w.poll(context.Background(), &GatewayJob{BackendSubmissionStartedAt: time.Now().Add(-16 * time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if s.reason != "reconciliation_deadline" || !s.at.IsZero() {
		t.Fatal("expired query rescheduled")
	}
}
