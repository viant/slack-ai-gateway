package runtime_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// These fakes isolate lifecycle ownership. The native storage/CLI tests cover
// the actual Datly transaction and integration scope contracts separately.
type lifecycleStore struct {
	gateway.Store
	recoveries atomic.Int32
}

func (s *lifecycleStore) Recover(context.Context) error {
	s.recoveries.Add(1)
	return nil
}
func (*lifecycleStore) ClaimJob(context.Context) (*gateway.GatewayJob, error) { return nil, nil }
func (*lifecycleStore) FailUnstarted(context.Context, string) error           { return nil }
func (*lifecycleStore) ClaimDelivery(context.Context) (*gateway.SlackOutgoingMessage, error) {
	return nil, nil
}
func (*lifecycleStore) ClaimStatus(context.Context, time.Time, string) (*gateway.GatewayJobProgress, error) {
	return nil, nil
}

func lifecycleRuntime(store gateway.Store) *runner.Runtime {
	return &runner.Runtime{Store: store, Clients: &backend{}, AgentID: "synthetic-agent",
		Route:  gateway.Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"},
		Policy: gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "shared"},
		Source: sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error { ready(); <-ctx.Done(); return nil }),
		Sender: senderFunc(func(context.Context, gateway.SlackOutgoingMessage) (string, error) { return "2.001", nil }),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestRuntimeLeavesAgentSelectionValidationToBackend(t *testing.T) {
	s := &lifecycleStore{}
	r := lifecycleRuntime(s)
	r.AgentID = "" // Protocol-only AG-UI does not require an external selector.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		cancel()
		<-ctx.Done()
		return nil
	})
	storetest.Must(t, r.Run(ctx))
	if s.recoveries.Load() != 1 {
		t.Fatal("direct runtime did not retain its standalone recovery owner")
	}
}

func TestGroupLifecycleUsesPreparedRootAndIsolatesLocalFailure(t *testing.T) {
	root, firstScope, secondScope := &lifecycleStore{}, &lifecycleStore{}, &lifecycleStore{}
	first, second := lifecycleRuntime(firstScope), lifecycleRuntime(secondScope)
	failed, running := make(chan struct{}), make(chan struct{})
	first.Source = sourceFunc(func(context.Context, gateway.EventSink, func()) error {
		close(failed)
		return gateway.ErrSlackAuth
	})
	second.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		close(running)
		<-ctx.Done()
		return nil
	})
	group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	await(t, failed)
	await(t, running)
	cancel()
	storetest.Must(t, finished(t, done))
	if root.recoveries.Load() != 0 || firstScope.recoveries.Load() != 0 || secondScope.recoveries.Load() != 0 {
		t.Fatal("group repeated recovery already owned by configuration reconciliation")
	}
}

func TestGroupLifecyclePreservesSharedFailureDuringLocalShutdown(t *testing.T) {
	root := &lifecycleStore{}
	firstStore := &failureAfterSourceStop{Store: &lifecycleStore{}, claimed: make(chan struct{})}
	first, second := lifecycleRuntime(firstStore), lifecycleRuntime(&lifecycleStore{})
	first.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		select {
		case <-firstStore.claimed:
			return gateway.ErrSlackAuth
		case <-ctx.Done():
			return nil
		}
	})
	group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	if err := finished(t, done); err == nil {
		t.Fatal("a common storage failure was mistaken for an isolated Slack failure")
	}
}

func TestGroupLifecycleTreatsSourcePersistenceFailureAsShared(t *testing.T) {
	for _, readyFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_ready", true: "after_ready"}[readyFirst], func(t *testing.T) {
			root := &lifecycleStore{}
			first, second := lifecycleRuntime(&lifecycleStore{}), lifecycleRuntime(&lifecycleStore{})
			first.Source = sourceFunc(func(_ context.Context, _ gateway.EventSink, ready func()) error {
				if readyFirst {
					ready()
				}
				return gateway.ErrIntakePersistence
			})
			group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- group.Run(ctx) }()
			if err := finished(t, done); err == nil || !strings.Contains(err.Error(), "intake persistence failed") {
				t.Fatalf("source intake failure was mistaken for a local transport failure: %v", err)
			}
		})
	}
}

type backendShutdownFailure struct{ backend }

func (*backendShutdownFailure) State() gateway.BackendState { return gateway.BackendReady }
func (*backendShutdownFailure) WithReady(fn func() error) error {
	return fn()
}
func (*backendShutdownFailure) Run(ctx context.Context) error {
	<-ctx.Done()
	return errors.New("synthetic backend storage callback failure")
}

func TestGroupLifecycleJoinsBackendBeforeClassifyingShutdown(t *testing.T) {
	root := &lifecycleStore{}
	first, second := lifecycleRuntime(&lifecycleStore{}), lifecycleRuntime(&lifecycleStore{})
	first.Backend = &backendShutdownFailure{}
	first.Source = sourceFunc(func(context.Context, gateway.EventSink, func()) error { return gateway.ErrSlackAuth })
	group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	if err := finished(t, done); err == nil || !strings.Contains(err.Error(), "backend connection recovery or storage failed") {
		t.Fatalf("shared backend storage callback failure was hidden during shutdown: %v", err)
	}
}

type canceledClaims struct {
	*failureAfterSourceStop
	canceled     chan struct{}
	canceledOnce sync.Once
}

func (s *canceledClaims) ClaimJob(ctx context.Context) (*gateway.GatewayJob, error) {
	s.once.Do(func() { close(s.claimed) })
	<-ctx.Done()
	s.canceledOnce.Do(func() { close(s.canceled) })
	return nil, ctx.Err()
}

func TestGroupLifecycleDoesNotPromoteCanceledClaimsToStorageFailure(t *testing.T) {
	root := &lifecycleStore{}
	claims := &canceledClaims{failureAfterSourceStop: &failureAfterSourceStop{Store: &lifecycleStore{}, claimed: make(chan struct{})}, canceled: make(chan struct{})}
	first, second := lifecycleRuntime(claims), lifecycleRuntime(&lifecycleStore{})
	first.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		select {
		case <-claims.claimed:
			return gateway.ErrSlackAuth
		case <-ctx.Done():
			return nil
		}
	})
	group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	await(t, claims.canceled)
	cancel()
	storetest.Must(t, finished(t, done))
}

type terminalStatusFailure struct{ *lifecycleStore }

func (*terminalStatusFailure) ClaimJob(context.Context) (*gateway.GatewayJob, error) {
	return nil, gateway.ErrBackendAuth
}
func (*terminalStatusFailure) ClaimStatus(_ context.Context, _ time.Time, kind string) (*gateway.GatewayJobProgress, error) {
	if kind == "terminal" {
		return nil, errors.New("synthetic terminal status persistence failure")
	}
	return nil, nil
}

func TestGroupLifecycleRetainsStorageFailureFromTerminalDrain(t *testing.T) {
	root := &lifecycleStore{}
	first, second := lifecycleRuntime(&terminalStatusFailure{lifecycleStore: &lifecycleStore{}}), lifecycleRuntime(&lifecycleStore{})
	group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	if err := finished(t, done); err == nil || !strings.Contains(err.Error(), "runtime worker or storage failed") {
		t.Fatalf("shared terminal status failure was hidden behind a backend rejection: %v", err)
	}
}
