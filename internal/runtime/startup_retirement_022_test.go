package runtime_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type startupRetirement022Planner struct {
	nativeRuntimeStore
	forgotten chan gateway.PhysicalThreadKey
}

func (p startupRetirement022Planner) ForgetWorkThread(key gateway.PhysicalThreadKey) {
	p.Store.(interface {
		ForgetWorkThread(gateway.PhysicalThreadKey)
	}).ForgetWorkThread(key)
	select {
	case p.forgotten <- key:
	default:
	}
}

func TestGroup022RetiresSourceThatFailsBeforeReadiness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first, firstStore, second, _ := setupGroup(t)
		message := storetest.Message("before-ready-event", "before-ready-thread", "alice")
		_, err := firstStore.Enqueue(context.Background(), message)
		storetest.Must(t, err)
		first.Source = sourceFunc(func(context.Context, gateway.EventSink, func()) error { return gateway.ErrSlackAuth })
		forgotten := make(chan gateway.PhysicalThreadKey, 4)
		planner := startupRetirement022Planner{nativeRuntimeStore: nativeRuntimeStore{Store: firstStore.Scope(0)}, forgotten: forgotten}
		group := &runner.Group{Store: planner, Integrations: []runner.Integration{{ID: "first", Runtime: first}, {ID: "second", Runtime: second}}, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- group.Run(ctx) }()
		defer func() { cancel(); storetest.Must(t, finished(t, done)) }()
		select {
		case key := <-forgotten:
			if key != message.PhysicalThread() {
				t.Fatal("scheduler forgot another physical thread")
			}
		case err := <-done:
			done <- err
			t.Fatal("the healthy integration stopped with the failed startup source")
		case <-time.After(10 * time.Second):
			t.Fatal("failed startup source was never retired by the scheduler")
		}
		if revision := planner.ThreadRevision(message.PhysicalThread()); revision != 0 {
			t.Fatalf("pre-readiness failure retained thread revision %d", revision)
		}
	})
}
