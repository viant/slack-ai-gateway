package runtime_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type statusRuntimeSender struct {
	senderFunc
	status func(context.Context, gateway.GatewayJobProgress, string) (string, error)
}

func (s statusRuntimeSender) SendStatus(ctx context.Context, status gateway.GatewayJobProgress, text string) (gateway.StatusReceipt, error) {
	ts, err := s.status(ctx, status, text)
	return gateway.StatusReceipt{SlackTS: ts}, err
}

func TestMoreThanFourObservationsAndIndependentStatusAcknowledgment(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{}, 8)
	release := make(chan struct{})
	b.query = func(ctx context.Context, q gateway.Query) (gateway.Turn, error) {
		started <- struct{}{}
		select {
		case <-release:
			return gateway.Turn{ID: q.MessageID, State: "completed", Content: "reply"}, nil
		case <-ctx.Done():
			return gateway.Turn{}, ctx.Err()
		}
	}
	for i := 0; i < 4; i++ {
		_, err := s.Enqueue(ctx, storetest.Message(fmt.Sprint(i), fmt.Sprint(i), "alice"))
		storetest.Must(t, err)
	}
	statuses := make(chan gateway.GatewayJobProgress, 20)
	r.Sender = statusRuntimeSender{senderFunc: func(context.Context, gateway.SlackOutgoingMessage) (string, error) { return "4.000001", nil }, status: func(_ context.Context, status gateway.GatewayJobProgress, _ string) (string, error) {
		statuses <- status
		if status.SlackTS != "" {
			return status.SlackTS, nil
		}
		return "3.000001", nil
	}}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	for range 4 {
		await(t, started)
	}
	_, err := s.Enqueue(ctx, storetest.Message("fifth", "fifth", "bob"))
	storetest.Must(t, err)
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	for {
		select {
		case status := <-statuses:
			if status.RootTS != "fifth" {
				continue
			}
			if (status.Phase != "received" && status.Phase != "contacting") || r.Delivered() != 0 {
				t.Fatal("acknowledgment depends on a query worker or counts as final delivery")
			}
			await(t, started) // Fifth query started without releasing the first four.
			if b.queries.Load() != 5 {
				t.Fatal("long observations blocked admission")
			}
			cancel()
			close(release)
			storetest.Must(t, finished(t, done))
			return
		case <-timer.C:
			cancel()
			close(release)
			finished(t, done)
			t.Fatal("no acknowledgment while query workers were occupied")
		}
	}
}

func TestBackendAuthenticationShutdownAttemptsOnlyTerminalStatus(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, event := range []string{"first", "second"} {
		_, err := s.Enqueue(ctx, storetest.Message(event, "same-thread", "alice"))
		storetest.Must(t, err)
	}
	stopped := make(chan struct{})
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		<-ctx.Done()
		close(stopped)
		return nil
	})
	b.query = func(context.Context, gateway.Query) (gateway.Turn, error) {
		return gateway.Turn{}, gateway.ErrBackendAuth
	}
	terminal := make(chan gateway.GatewayJobProgress, 4)
	r.Sender = statusRuntimeSender{senderFunc: func(context.Context, gateway.SlackOutgoingMessage) (string, error) {
		t.Error("shutdown drained final replies")
		return "", nil
	}, status: func(_ context.Context, status gateway.GatewayJobProgress, _ string) (string, error) {
		if status.Phase == "needs_review" {
			terminal <- status
		}
		if status.SlackTS != "" {
			return status.SlackTS, nil
		}
		return "3.000001", nil
	}}
	err := r.Run(ctx)
	if err == nil || err.Error() != gateway.ErrBackendAuth.Error() {
		t.Fatalf("unexpected shutdown: %v", err)
	}
	await(t, stopped)
	select {
	case status := <-terminal:
		if !status.Terminal {
			t.Fatal("nonterminal auth notice")
		}
	default:
		t.Fatal("terminal auth status not attempted")
	}
	metadata, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if metadata.Counts["gateway_job.gateway_execution_state"]["pending"] != 1 || b.queries.Load() != 1 || r.Delivered() != 0 {
		t.Fatal("authentication shutdown drained queued work")
	}
}
