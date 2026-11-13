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
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// These fakes isolate lifecycle ownership. The native storage/CLI tests cover
// the actual Datly transaction and integration scope contracts separately.
type runtimeTestStore interface {
	gateway.Store
	gateway.WorkPlanner
}

var lifecycleIdentity atomic.Int64

type lifecycleStore struct {
	gateway.Store
	recoveries atomic.Int32
	id         atomic.Int64
}

func (s *lifecycleStore) IntegrationID() int64 {
	if s.id.Load() == 0 {
		s.id.CompareAndSwap(0, lifecycleIdentity.Add(1))
	}
	return s.id.Load()
}
func (*lifecycleStore) WorkChanges() <-chan struct{} { return nil }
func (*lifecycleStore) WorkRevision(int64) uint64    { return 0 }
func (*lifecycleStore) WorkCandidates(context.Context, time.Time) ([]gateway.WorkCandidate, error) {
	var result []gateway.WorkCandidate
	for id := int64(1); id <= lifecycleIdentity.Load(); id++ {
		result = append(result, gateway.WorkCandidate{IntegrationID: id, Query: gateway.WorkHint{ProbeNow: true}, Outbound: gateway.WorkHint{ProbeNow: true}})
	}
	return result, nil
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
		Route:  storetest.Route("A", "T", "C"),
		Policy: storetest.Policy("A", "T", "shared", "C"),
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
	firstStore := &failureAfterSourceStop{runtimeTestStore: &lifecycleStore{}, claimed: make(chan struct{})}
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
	claims := &canceledClaims{failureAfterSourceStop: &failureAfterSourceStop{runtimeTestStore: &lifecycleStore{}, claimed: make(chan struct{})}, canceled: make(chan struct{})}
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

type shutdownDiscoveryFailure struct {
	*lifecycleStore
	entered chan struct{}
}

func (s *shutdownDiscoveryFailure) WorkCandidates(ctx context.Context, _ time.Time) ([]gateway.WorkCandidate, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, errors.New("private discovery failure during shutdown")
}

func TestRuntimeAndGroupRetainDiscoveryFailureAfterLocalShutdown(t *testing.T) {
	for _, grouped := range []bool{false, true} {
		for _, shutdown := range []string{"local", "normal", "existing_global"} {
			t.Run(map[bool]string{false: "standalone", true: "group"}[grouped]+"/"+shutdown, func(t *testing.T) {
				planner := &shutdownDiscoveryFailure{lifecycleStore: &lifecycleStore{}, entered: make(chan struct{})}
				r := lifecycleRuntime(planner)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if shutdown == "existing_global" {
					r.Backend = &backendShutdownFailure{}
				}
				r.Source = sourceFunc(func(sourceCtx context.Context, _ gateway.EventSink, ready func()) error {
					ready()
					<-planner.entered
					if shutdown == "normal" {
						cancel()
						<-sourceCtx.Done()
						return nil
					}
					return gateway.ErrSlackAuth
				})
				done := make(chan error, 1)
				go func() {
					if grouped {
						g := &runner.Group{Store: planner, Integrations: []runner.Integration{{ID: "first", Runtime: r}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
						done <- g.Run(ctx)
					} else {
						done <- r.Run(ctx)
					}
				}()
				err := finished(t, done)
				want := "runtime work discovery failed"
				if shutdown == "existing_global" {
					want = "backend connection recovery or storage failed"
				}
				if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(), "private") {
					t.Fatalf("shared error priority or redaction changed: %v, want %s", err, want)
				}
			})
		}
	}
}

type closedStateChangesBackend struct {
	backend
	changes chan struct{}
	reads   atomic.Int32
}

func (*closedStateChangesBackend) State() gateway.BackendState     { return gateway.BackendReady }
func (*closedStateChangesBackend) WithReady(fn func() error) error { return fn() }
func (*closedStateChangesBackend) Run(ctx context.Context) error   { <-ctx.Done(); return nil }
func (b *closedStateChangesBackend) StateChanges() <-chan struct{} {
	if b.reads.Add(1) == 1 {
		return b.changes
	}
	return nil
}

func TestRuntimeClosedBackendStateChangesStopsWatcher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		changes := make(chan struct{})
		close(changes)
		b := &closedStateChangesBackend{changes: changes}
		r := lifecycleRuntime(&lifecycleStore{})
		r.Backend = b
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		synctest.Wait()
		if b.reads.Load() != 1 {
			t.Fatalf("state channel was reacquired after closure: %d", b.reads.Load())
		}
		select {
		case err := <-done:
			t.Fatalf("notification closure stopped the runtime: %v", err)
		default:
		}
		cancel()
		storetest.Must(t, finished(t, done))
	})
}
