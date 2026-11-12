package runtime_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type recoveryOwner struct {
	gateway.Store
	recoveries atomic.Int32
}

func (s *recoveryOwner) Recover(ctx context.Context) error {
	s.recoveries.Add(1)
	return s.Store.Recover(ctx)
}

func TestGroupUsesPreparedRootAndIsolatesSlackSourceFailure(t *testing.T) {
	first, firstStore, _ := setup(t)
	second, secondStore, _ := setup(t)
	owner := &recoveryOwner{Store: firstStore.Scope(0)}
	firstScope := &recoveryOwner{Store: firstStore}
	secondScope := &recoveryOwner{Store: secondStore}
	first.Store, second.Store = firstScope, secondScope
	first.Source = sourceFunc(func(context.Context, gateway.EventSink, func()) error {
		return gateway.ErrSlackAuth
	})
	seedReply(t, secondStore, "second-installation", "second-thread")
	sent := make(chan struct{}, 1)
	second.Sender = senderFunc(func(context.Context, gateway.SlackOutgoingMessage) (string, error) {
		sent <- struct{}{}
		return "2.000001", nil
	})
	group := &runner.Group{Store: owner, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	await(t, sent)
	cancel()
	storetest.Must(t, finished(t, done))
	if owner.recoveries.Load() != 0 || firstScope.recoveries.Load() != 0 || secondScope.recoveries.Load() != 0 {
		t.Fatal("group repeated recovery already owned by configuration reconciliation")
	}
}

type failedClaims struct{ gateway.Store }

func (s failedClaims) ClaimJob(context.Context) (*gateway.GatewayJob, error) {
	return nil, errors.New("synthetic storage failure")
}

func TestGroupStorageFailureStopsOtherIntegrations(t *testing.T) {
	first, firstStore, _ := setup(t)
	second, _, _ := setup(t)
	first.Store = failedClaims{Store: firstStore}
	secondStopped := make(chan struct{})
	second.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		<-ctx.Done()
		close(secondStopped)
		return nil
	})
	group := &runner.Group{Store: firstStore.Scope(0), Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	done := make(chan error, 1)
	go func() { done <- group.Run(context.Background()) }()
	if err := finished(t, done); err == nil {
		t.Fatal("common storage failure was ignored")
	}
	await(t, secondStopped)
}

type failureAfterSourceStop struct {
	gateway.Store
	claimed chan struct{}
	once    sync.Once
}

func (s *failureAfterSourceStop) ClaimJob(ctx context.Context) (*gateway.GatewayJob, error) {
	s.once.Do(func() { close(s.claimed) })
	<-ctx.Done()
	return nil, errors.New("synthetic storage failure during join")
}

func TestGroupDoesNotHideStorageFailureBehindEarlierSlackFailure(t *testing.T) {
	first, firstStore, _ := setup(t)
	second, _, _ := setup(t)
	scope := &failureAfterSourceStop{Store: firstStore, claimed: make(chan struct{})}
	first.Store = scope
	first.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		select {
		case <-scope.claimed:
			return gateway.ErrSlackAuth
		case <-ctx.Done():
			return nil
		}
	})
	group := &runner.Group{Store: firstStore.Scope(0), Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	if err := finished(t, done); err == nil {
		t.Fatal("storage failure was hidden by the earlier Slack failure")
	}
}
