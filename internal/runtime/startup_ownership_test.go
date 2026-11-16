package runtime_test

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type startupSweepStore struct {
	*lifecycleStore
	swept *atomic.Int32
}

func (s *startupSweepStore) FailUnstarted(context.Context, string) error {
	s.swept.Add(1)
	return nil
}

type startupReadyBackend struct{ gateway.BackendLifecycle }

func (startupReadyBackend) State() gateway.BackendState   { return gateway.BackendReady }
func (startupReadyBackend) Run(ctx context.Context) error { <-ctx.Done(); return nil }

func TestGroupCompletesEveryStartupSweepBeforeStartingSources(t *testing.T) {
	var swept, sources atomic.Int32
	root := &lifecycleStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group := &runner.Group{Store: root, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, name := range []string{"first", "second"} {
		store := &startupSweepStore{lifecycleStore: &lifecycleStore{}, swept: &swept}
		runtime := lifecycleRuntime(store)
		runtime.Backend = startupReadyBackend{}
		runtime.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
			if swept.Load() != 2 {
				t.Error("source admitted work before another integration's startup sweep")
			}
			ready()
			if sources.Add(1) == 2 {
				cancel()
			}
			<-ctx.Done()
			return nil
		})
		group.Integrations = append(group.Integrations, runner.Integration{ID: name, Runtime: runtime})
	}
	storetest.Must(t, group.Run(ctx))
	if swept.Load() != 2 || sources.Load() != 2 {
		t.Fatalf("startup stages: sweeps=%d sources=%d", swept.Load(), sources.Load())
	}
}
