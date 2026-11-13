package agently

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestManagerStateChangesFollowPersistedOutageAndReady(t *testing.T) {
	var m *Manager
	persisted := false
	m = NewManager(nil, func(context.Context, gateway.BackendState) error {
		select {
		case <-m.StateChanges():
			t.Fatal("scheduler was notified before the outage was persisted")
		default:
		}
		persisted = true
		return nil
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.mu.Lock()
	err := m.unavailableLocked(context.Background(), gateway.BackendBlocked)
	m.mu.Unlock()
	if err != nil || !persisted {
		t.Fatalf("outage persistence: persisted=%v err=%v", persisted, err)
	}
	select {
	case <-m.StateChanges():
		if m.State() != gateway.BackendBlocked {
			t.Fatal("outage hint did not expose the current state")
		}
	default:
		t.Fatal("persisted outage did not wake the scheduler")
	}
	m.ready(genericFactory{})
	select {
	case <-m.StateChanges():
		if m.State() != gateway.BackendReady {
			t.Fatal("ready hint did not expose the current state")
		}
	default:
		t.Fatal("recovery did not wake the scheduler")
	}
}

func TestManagerStateChangeHintsCoalesceWithoutBlockingReadiness(t *testing.T) {
	m := NewManager(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for range 100 {
		m.ready(genericFactory{})
	}
	if err := m.WithReady(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-m.StateChanges():
	default:
		t.Fatal("state changes were lost")
	}
	select {
	case <-m.StateChanges():
		t.Fatal("state notifications were not coalesced")
	default:
	}
}
