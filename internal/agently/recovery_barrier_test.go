package agently

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestManagerCleanupRunsOutsideReadinessLockAndHasNoFiveSecondCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var callbacks atomic.Int32
		var m *Manager
		m = NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			return &readinessFactory{}, nil
		}, func(ctx context.Context, state gateway.BackendState) error {
			callbacks.Add(1)
			if m.State() != state {
				t.Error("cleanup could not inspect its unavailable state")
			}
			if err := m.WithReady(func() error { t.Error("unavailable cleanup admitted work"); return nil }); !errors.Is(err, gateway.ErrBackendUnavailable) {
				t.Errorf("cleanup readiness = %v", err)
			}
			if state == gateway.BackendRecovering {
				close(entered)
				<-release
				if err := ctx.Err(); err != nil {
					t.Errorf("whole fanout inherited a deadline: %v", err)
				}
			}
			return nil
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		m.probeInterval = time.Hour
		stop := startReadinessManager(t, m)
		defer stop()
		_, generation, _ := m.snapshot()
		failed := make(chan struct{})
		go func() { m.failed(generation, &APIError{Status: 503}); close(failed) }()
		<-entered
		if m.State() != gateway.BackendRecovering || m.ready(&readinessFactory{}) {
			t.Fatal("authentication bypassed outstanding cleanup")
		}
		time.Sleep(6 * time.Second)
		if m.State() != gateway.BackendRecovering {
			t.Fatal("unfinished cleanup became ready")
		}
		close(release)
		<-failed
		synctest.Wait()
		if m.State() != gateway.BackendReady || callbacks.Load() != 2 {
			t.Fatalf("cleanup did not release recovery: state=%s callbacks=%d", m.State(), callbacks.Load())
		}
	})
}

func TestManagerOutageEpochSurvivesRecoveryAndRejectsStaleFailures(t *testing.T) {
	m := NewManager(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.ready(genericFactory{})
	var before, after uint64
	if err := m.WithReadyEpoch(func(epoch uint64) error { before = epoch; return nil }); err != nil {
		t.Fatal(err)
	}
	_, generation, _ := m.snapshot()
	m.failed(generation, &APIError{Status: 503})
	if err := m.WithReadyEpoch(func(uint64) error { t.Fatal("outage admitted work"); return nil }); !errors.Is(err, gateway.ErrBackendUnavailable) {
		t.Fatal(err)
	}
	if !m.ready(genericFactory{}) {
		t.Fatal("finished cleanup prevented recovery")
	}
	if err := m.WithReadyEpoch(func(epoch uint64) error { after = epoch; return nil }); err != nil || after <= before {
		t.Fatalf("outage epoch was lost: before=%d after=%d err=%v", before, after, err)
	}
	m.failed(generation, &APIError{Status: 503})
	if err := m.WithReadyEpoch(func(epoch uint64) error {
		if epoch != after {
			t.Error("stale failure advanced a healthy epoch")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestManagerStaleAuthenticationCannotPublishReady(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		stale, current := &readinessFactory{}, &readinessFactory{}
		var calls atomic.Int32
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
				return stale, nil
			}
			return current, nil
		}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		m.probeInterval = time.Hour
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		<-entered
		m.mu.Lock()
		_ = m.unavailableLocked(ctx, gateway.BackendRecovering, nil)
		cleanup := m.cleanup
		m.mu.Unlock()
		if err := m.finishCleanup(ctx, cleanup); err != nil {
			t.Fatal(err)
		}
		close(release)
		synctest.Wait()
		factory, _, state := m.snapshot()
		if factory != current || state != gateway.BackendReady || calls.Load() != 2 {
			t.Fatalf("stale auth became ready: current=%t state=%s calls=%d", factory == current, state, calls.Load())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestManagerCancellationJoinsCleanupThroughItsTrackedCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			return &readinessFactory{}, nil
		}, func(ctx context.Context, state gateway.BackendState) error {
			if state == gateway.BackendRecovering {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		m.probeInterval = time.Hour
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- m.Run(ctx) }()
		synctest.Wait()
		_, generation, _ := m.snapshot()
		failed := make(chan struct{})
		go func() { m.failed(generation, &APIError{Status: 503}); close(failed) }()
		<-entered
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		<-failed
		m.mu.RLock()
		cleanup := m.cleanup
		m.mu.RUnlock()
		select {
		case <-cleanup.done:
		default:
			t.Fatal("tracked cleanup did not finish")
		}
	})
}
