package gateway

import (
	"context"
	"testing"
	"time"
)

// A failed readiness gate can return to its caller after another goroutine has
// restored readiness. Its old worker must not sweep newly admitted questions.
type staleBackend struct{ BackendLifecycle }

func (staleBackend) State() BackendState          { return BackendReady }
func (staleBackend) WithReady(func() error) error { return ErrBackendUnavailable }

type interruptedStore struct {
	Store
	job      *GatewayJob
	sweeps   int
	review   string
	rejected int64
	enqueued Message
}

func (s *interruptedStore) ClaimJob(context.Context) (*GatewayJob, error) { return s.job, nil }
func (s *interruptedStore) GetBinding(context.Context, ThreadKey) (*ConversationBinding, error) {
	return nil, nil
}
func (s *interruptedStore) FailUnstarted(context.Context, string) error { s.sweeps++; return nil }
func (s *interruptedStore) Reject(_ context.Context, id int64, _ string) error {
	s.rejected = id
	return ErrBackendUnavailable
}
func (s *interruptedStore) Review(_ context.Context, _ int64, reason string) error {
	s.review = reason
	return nil
}
func (s *interruptedStore) Enqueue(_ context.Context, message Message) (bool, error) {
	s.enqueued = message
	return true, nil
}

func TestWorkerDoesNotSweepNewQuestionsAfterRecovery(t *testing.T) {
	s := &interruptedStore{job: &GatewayJob{ID: 1, Message: Message{
		ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"},
		AppID:     "A", AuthorID: "alice", Text: "old question",
	}}}
	w := Worker{Store: s, Backend: staleBackend{}, Policy: SharedChannelPolicy{
		AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "shared",
	}}
	worked, err := w.Step(context.Background())
	if err != nil || !worked || s.sweeps != 0 || s.review != "" || s.rejected != s.job.ID {
		t.Fatalf("stale worker changed healthy work: worked=%t sweeps=%d review=%s err=%v", worked, s.sweeps, s.review, err)
	}
}

func TestIntakePreservesUnavailableDispositionAfterRecovery(t *testing.T) {
	s := &interruptedStore{}
	i := Intake{Store: s, Backend: staleBackend{}, Route: Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"}}
	result, err := i.Accept(context.Background(), Message{
		ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"},
		AppID:     "A", EventID: "event", MessageTS: "1", AuthorID: "alice", Mentioned: true, Text: "outage question",
	})
	if err != nil || !result.Accepted || s.enqueued.BackendReason != "backend_unavailable" {
		t.Fatalf("outage question was admitted after failed gate: result=%+v reason=%q err=%v", result, s.enqueued.BackendReason, err)
	}
}

func TestReconciliationDeadlineDoesNotRestartDuringBackendRecovery(t *testing.T) {
	s := &interruptedStore{job: &GatewayJob{ID: 1, GatewayRequestID: "existing-run", BackendSubmissionStartedAt: time.Now().Add(-16 * time.Minute)}}
	// No client or policy is available: an expired query is quarantined before
	// authentication/polling and keeps its original deadline across restarts.
	w := Worker{Store: s, Backend: staleBackend{}}
	worked, err := w.Step(context.Background())
	if err != nil || !worked || s.review != "reconciliation_deadline" || s.sweeps != 0 {
		t.Fatalf("expired query was extended: worked=%t review=%s err=%v", worked, s.review, err)
	}
}
