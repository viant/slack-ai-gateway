package runtime_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// The lifecycle is controlled independently of request and refresh deadlines.
// Runtime must consume its existing StateChanges notification to resume work.
type status027Lifecycle struct {
	mu      sync.RWMutex
	state   gateway.BackendState
	changes chan struct{}
	reads   atomic.Int32
	queries atomic.Int32
}

func (b *status027Lifecycle) State() gateway.BackendState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.state
}

func (b *status027Lifecycle) StateChanges() <-chan struct{} { return b.changes }
func (b *status027Lifecycle) Run(ctx context.Context) error { <-ctx.Done(); return nil }
func (b *status027Lifecycle) WithReady(fn func() error) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != gateway.BackendReady {
		return gateway.ErrBackendUnavailable
	}
	return fn()
}
func (b *status027Lifecycle) ready() {
	b.mu.Lock()
	b.state = gateway.BackendReady
	b.mu.Unlock()
	b.changes <- struct{}{}
}
func (b *status027Lifecycle) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	if b.State() != gateway.BackendReady {
		return nil, gateway.ErrBackendUnavailable
	}
	return b, nil
}
func (b *status027Lifecycle) CreateConversation(context.Context, string) (string, error) {
	return "", gateway.ErrBackendProtocol
}
func (b *status027Lifecycle) Query(context.Context, gateway.Query, func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{}, gateway.ErrBackendProtocol
}
func (b *status027Lifecycle) ReadTurn(ctx context.Context, _, request string, observe func(gateway.Turn) error) (gateway.Turn, error) {
	b.reads.Add(1)
	if err := observe(gateway.Turn{ID: request, State: "running"}); err != nil {
		return gateway.Turn{}, err
	}
	<-ctx.Done()
	return gateway.Turn{}, ctx.Err()
}

func TestStatus027ReadinessWakesCleanRecoveryBeforeRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, _ := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		message := storetest.Message("status-027-wake", "status-027-thread", "alice")
		_, err := s.Enqueue(ctx, message)
		storetest.Must(t, err)
		job, err := s.ClaimJob(ctx)
		storetest.Must(t, err)
		binding, err := s.BindJob(ctx, job.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
		storetest.Must(t, err)
		storetest.Must(t, s.BeginCreate(ctx, binding.ID))
		storetest.Must(t, s.SetConversation(ctx, binding.ID, "existing-status-027"))
		storetest.Must(t, s.BeginQuery(ctx, job.ID))
		now := time.Now().UTC().Truncate(time.Millisecond)
		storetest.Must(t, s.RecordObservation(ctx, job.ID, "running", now))
		storetest.Must(t, s.RecordObservation(ctx, job.ID, "unknown", now))
		status, err := s.ClaimStatus(ctx, now, "backend_recovering")
		storetest.Must(t, err)
		if status == nil || status.Phase != "backend_recovering" {
			t.Fatalf("missing recovery fixture: %+v", status)
		}
		storetest.Must(t, s.FinishStatus(ctx, *status, gateway.StatusResult{State: "ready", SlackTS: "27.000001", NextRefreshAt: now.Add(time.Hour)}, now))
		// Even the attachment is sleeping. A manager transition must wake both
		// lanes, rather than relying on a refresh or a request retry becoming due.
		storetest.Must(t, s.SchedulePoll(ctx, job.ID, now.Add(time.Hour)))
		b := &status027Lifecycle{state: gateway.BackendRecovering, changes: make(chan struct{}, 1)}
		r.Backend, r.StatusRefreshInterval = b, time.Hour
		connected := make(chan struct{})
		r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
			ready()
			close(connected)
			<-ctx.Done()
			return nil
		})
		statuses := make(chan gateway.GatewayJobProgress, 8)
		r.Sender = statusRuntimeSender{
			senderFunc: func(context.Context, gateway.SlackOutgoingMessage) (string, error) {
				t.Error("unfinished execution published a final response")
				return "", nil
			},
			status: func(_ context.Context, status gateway.GatewayJobProgress, _ string) (string, error) {
				statuses <- status
				return "27.000001", nil
			},
		}
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		defer func() { cancel(); storetest.Must(t, finished(t, done)) }()
		await(t, connected)
		synctest.Wait()
		select {
		case status := <-statuses:
			t.Fatalf("restart dirtied the acknowledged recovery status before refresh: %+v", status)
		default:
		}
		b.ready()
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			select {
			case status := <-statuses:
				if status.Phase != "checking" && status.Phase != "running" {
					t.Fatalf("readiness retained an obsolete outage phase: %+v", status)
				}
				if status.SlackTS != "27.000001" {
					t.Fatal("readiness created a second progress message")
				}
				if b.queries.Load() != 0 {
					t.Fatal("readiness resubmitted the existing prompt")
				}
				return
			case <-deadline.C:
				t.Fatalf("readiness did not wake the clean future publication: reads=%d", b.reads.Load())
			}
		}
	})
}
