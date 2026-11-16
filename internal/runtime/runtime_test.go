package runtime_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type logWriterFunc func([]byte) (int, error)

func (f logWriterFunc) Write(p []byte) (int, error) { return f(p) }

type sourceFunc func(context.Context, gateway.EventSink, func()) error

func (f sourceFunc) Run(c context.Context, s gateway.EventSink, ready func()) error {
	return f(c, s, ready)
}

type senderFunc func(context.Context, gateway.SlackOutgoingMessage) (string, error)

func (f senderFunc) Send(c context.Context, d gateway.SlackOutgoingMessage) (string, error) {
	return f(c, d)
}

func (f senderFunc) SendStatus(_ context.Context, s gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	if s.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: s.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}

// All runtime tests use local SQLite and controlled, test-only adapters.
type backend struct {
	queries atomic.Int32
	query   func(context.Context, gateway.Query) (gateway.Turn, error)
}

func (b *backend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) { return b, nil }
func (b *backend) CreateConversation(context.Context, string) (string, error) {
	return "test-conversation", nil
}
func (b *backend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, errors.New("unexpected poll")
}
func (b *backend) Query(ctx context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	if b.query != nil {
		return b.query(ctx, q)
	}
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: "reply"}, nil
}

func setup(t *testing.T) (*runner.Runtime, storage.Store, *backend) {
	t.Helper()
	ctx := context.Background()
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "gateway.sqlite"), true)
	storetest.Must(t, err)
	t.Cleanup(func() { s.Close() })
	storetest.Must(t, s.Migrate(ctx))
	integrationID := storetest.RegisterDefault(t, s)
	scope := s.Scope(integrationID)
	r, b := runtimeForStore(scope)
	return r, scope, b
}

func runtimeForStore(scope storage.Store) (*runner.Runtime, *backend) {
	b := &backend{}
	r := &runner.Runtime{Store: scope, Clients: b, AgentID: "steward", Route: storetest.Route("A", "T", "C"),
		Policy: storetest.Policy("A", "T", "shared", "C"),
		Source: sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error { ready(); <-ctx.Done(); return nil }),
		Sender: senderFunc(func(context.Context, gateway.SlackOutgoingMessage) (string, error) { return "2.000001", nil }),
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil))}
	return r, b
}

func seedReply(t *testing.T, s storage.Store, event, root string) {
	t.Helper()
	ctx := context.Background()
	_, err := s.Enqueue(ctx, storetest.Message(event, root, "alice"))
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	if j == nil {
		t.Fatal("missing seed job")
	}
	storetest.Must(t, s.Complete(ctx, j.ID, "", "reply", []gateway.ReplyPart{{Text: "reply", Format: gateway.ReplyPlain}}))
}

func await(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runtime")
	}
}
func finished(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop")
		return nil
	}
}

func TestIdentityReadinessGatesAllWorkers(t *testing.T) {
	r, s, b := setup(t)
	seedReply(t, s, "delivery", "one")
	_, err := s.Enqueue(context.Background(), storetest.Message("query", "two", "bob"))
	storetest.Must(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	sent := make(chan struct{}, 1)
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		close(started)
		select {
		case <-release:
			ready()
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	})
	r.Sender = senderFunc(func(context.Context, gateway.SlackOutgoingMessage) (string, error) {
		sent <- struct{}{}
		return "2", nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	await(t, started)
	select {
	case <-sent:
		t.Fatal("delivery before verified hello")
	case <-time.After(150 * time.Millisecond):
	}
	if b.queries.Load() != 0 {
		t.Fatal("query before verified hello")
	}
	close(release)
	await(t, sent)
	cancel()
	storetest.Must(t, finished(t, done))
	if r.Delivered() != 1 {
		t.Fatal("delivery completion not recorded")
	}
}

func TestShutdownFinishesQueryButDoesNotDrainQueue(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, event := range []string{"first", "second"} {
		_, err := s.Enqueue(ctx, storetest.Message(event, "one", "alice"))
		storetest.Must(t, err)
	}
	queryStarted := make(chan struct{})
	var queryStartedOnce sync.Once
	release := make(chan struct{})
	sourceStopped := make(chan struct{})
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		<-ctx.Done()
		close(sourceStopped)
		return nil
	})
	b.query = func(ctx context.Context, q gateway.Query) (gateway.Turn, error) {
		queryStartedOnce.Do(func() { close(queryStarted) })
		select {
		case <-release:
		case <-ctx.Done():
			return gateway.Turn{}, ctx.Err()
		}
		return gateway.Turn{ID: q.MessageID, State: "completed", Content: "saved during shutdown"}, nil
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	await(t, queryStarted)
	cancel()
	await(t, sourceStopped)
	close(release)
	storetest.Must(t, finished(t, done))
	if b.queries.Load() != 1 {
		t.Fatalf("shutdown drained next query: got %d backend queries, want 1", b.queries.Load())
	}
	d, err := fixtureThread(s, "one").ClaimDelivery(context.Background())
	storetest.Must(t, err)
	if d == nil || d.Text != "saved during shutdown" {
		t.Fatal("in-flight result lost")
	}
	j, err := fixtureThread(s, "one").ClaimJob(context.Background())
	storetest.Must(t, err)
	if j == nil || j.EventID != "second" {
		t.Fatal("unstarted query not left pending")
	}
}

func TestShutdownPersistsInFlightDelivery(t *testing.T) {
	r, s, _ := setup(t)
	seedReply(t, s, "first", "one")
	seedReply(t, s, "second", "two")
	started := make(chan struct{})
	var startedRoot string
	var attempts atomic.Int32
	release := make(chan struct{})
	stopped := make(chan struct{})
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
		ready()
		<-ctx.Done()
		close(stopped)
		return nil
	})
	r.Sender = senderFunc(func(ctx context.Context, delivery gateway.SlackOutgoingMessage) (string, error) {
		if attempts.Add(1) == 1 {
			startedRoot = delivery.RootTS
			close(started)
		}
		select {
		case <-release:
			return "slack-ts", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	await(t, started)
	cancel()
	await(t, stopped)
	close(release)
	storetest.Must(t, finished(t, done))
	if r.Delivered() != 1 || attempts.Load() != 1 {
		t.Fatalf("in-flight send counts: delivered=%d attempts=%d", r.Delivered(), attempts.Load())
	}
	otherRoot := "two"
	if startedRoot == "two" {
		otherRoot = "one"
	} else if startedRoot != "one" {
		t.Fatalf("unexpected started thread %q", startedRoot)
	}
	status, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if len(status.SlackOutgoingMessages) != 2 {
		t.Fatalf("unexpected durable deliveries: %+v", status.SlackOutgoingMessages)
	}
	for _, delivery := range status.SlackOutgoingMessages {
		if delivery.ThreadTS == startedRoot {
			if delivery.State != "delivered" || delivery.SlackTS != "slack-ts" || delivery.Attempts != 1 {
				t.Fatalf("in-flight success was not durably preserved: %+v", delivery)
			}
		} else if delivery.ThreadTS != otherRoot || delivery.State != "pending" || delivery.SlackTS != "" || delivery.Attempts != 0 {
			t.Fatalf("shutdown changed unstarted delivery: %+v", delivery)
		}
	}
	d, err := fixtureThread(s, startedRoot).ClaimDelivery(context.Background())
	storetest.Must(t, err)
	if d != nil {
		t.Fatal("completed in-flight delivery was claimable again")
	}
	d, err = fixtureThread(s, otherRoot).ClaimDelivery(context.Background())
	storetest.Must(t, err)
	if d == nil || d.RootTS != otherRoot {
		t.Fatal("shutdown drained pending delivery")
	}
}

func TestCredentialRejectionStopsRuntimeWithoutLosingReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, _ := setup(t)
		seedReply(t, s, "first", "one")
		r.Sender = senderFunc(func(context.Context, gateway.SlackOutgoingMessage) (string, error) {
			return "", &gateway.SendError{Kind: gateway.SendCredentials}
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.Run(ctx); err == nil || !strings.Contains(err.Error(), "credentials rejected") {
			t.Fatalf("runtime did not surface credential failure: %v", err)
		}
		d, err := fixtureThread(s, "one").ClaimDelivery(context.Background())
		storetest.Must(t, err)
		if d == nil || d.Attempts != 2 {
			t.Fatal("definitely rejected reply not preserved")
		}
	})
}

func TestBackendIdentityRejectionStopsIntakeAndPreservesPendingWork(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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
	err := r.Run(ctx)
	if err == nil || err.Error() != gateway.ErrBackendAuth.Error() {
		t.Fatalf("missing fatal auth result: %v", err)
	}
	await(t, stopped)
	status, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if b.queries.Load() != 1 || len(status.SlackOutgoingMessages) != 0 || status.Counts["gateway_job.gateway_execution_state"]["needs_review"] != 1 || status.Counts["gateway_job.gateway_execution_state"]["pending"] != 1 {
		t.Fatal("unsafe auth failure recovery")
	}
	if status.GatewayJobs[1].Reason != "backend_auth_or_identity_rejected" {
		t.Fatal("missing redacted review reason")
	}
}

func TestQueryReviewLogsPersistedMetadataWithoutContent(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m := storetest.Message("review", "one", "alice")
	m.Text = "private-question-marker"
	_, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	b.query = func(context.Context, gateway.Query) (gateway.Turn, error) {
		return gateway.Turn{}, errors.Join(gateway.ErrBackendProtocol, errors.New("private-backend-error-marker"))
	}
	var logs bytes.Buffer
	r.Log = slog.New(slog.NewJSONHandler(logWriterFunc(func(p []byte) (int, error) {
		n, err := logs.Write(p)
		if bytes.Contains(p, []byte(`"msg":"query_needs_review"`)) {
			cancel()
		}
		return n, err
	}), nil))
	storetest.Must(t, r.Run(ctx)) // Joins all workers before logs are inspected.
	status, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if len(status.GatewayJobs) != 1 || status.GatewayJobs[0].State != "needs_review" || len(status.SlackOutgoingMessages) != 0 || b.queries.Load() != 1 || r.Delivered() != 0 {
		t.Fatal("review caused replay or delivery, or was not persisted")
	}
	var record struct {
		Level            string `json:"level"`
		Message          string `json:"msg"`
		JobID            int64  `json:"job_id"`
		GatewayRequestID string `json:"gateway_request_id"`
		Reason           string `json:"reason"`
	}
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if bytes.Contains(line, []byte(`"msg":"query_needs_review"`)) {
			storetest.Must(t, json.Unmarshal(line, &record))
		}
	}
	j := status.GatewayJobs[0]
	if record.Level != "WARN" || record.Message != "query_needs_review" || record.JobID != j.ID || record.GatewayRequestID != j.GatewayRequestID || record.Reason != "backend_protocol_or_access_error" || record.Reason != j.Reason {
		t.Fatalf("unexpected review log: %s", logs.String())
	}
	if strings.Contains(logs.String(), "private-") {
		t.Fatal("review log exposed question text or backend error")
	}
}

func TestUncertainDeliveryDoesNotBlockOtherThreads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, _ := setup(t)
		seedReply(t, s, "first", "one")
		seedReply(t, s, "second", "two")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		uncertainAttempt, sent := make(chan struct{}), make(chan struct{})
		var firstAttempts, secondAttempts atomic.Int32
		r.Sender = senderFunc(func(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
			if d.RootTS == "one" {
				if firstAttempts.Add(1) == 1 {
					close(uncertainAttempt)
				}
				return "", &gateway.SendError{Kind: gateway.SendUnknown}
			}
			if secondAttempts.Add(1) == 1 {
				close(sent)
			}
			return "2", nil
		})
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		// Physical threads are scheduled fairly; either one may reach Slack
		// first. Exercise both outcomes before stopping admission.
		await(t, uncertainAttempt)
		await(t, sent)
		cancel()
		storetest.Must(t, finished(t, done))
		if r.Delivered() != 1 || firstAttempts.Load() != 1 || secondAttempts.Load() != 1 {
			t.Fatalf("delivery counts: confirmed=%d uncertain_attempts=%d other_attempts=%d", r.Delivered(), firstAttempts.Load(), secondAttempts.Load())
		}
		status, err := s.Status(context.Background(), 10)
		storetest.Must(t, err)
		if len(status.SlackOutgoingMessages) != 2 {
			t.Fatalf("unexpected persisted deliveries: %+v", status.SlackOutgoingMessages)
		}
		for _, delivery := range status.SlackOutgoingMessages {
			want := "delivered"
			if delivery.ThreadTS == "one" {
				want = "delivery_unknown"
			}
			if delivery.State != want || delivery.Attempts != 1 {
				t.Fatalf("unexpected persisted delivery outcome: %+v", delivery)
			}
		}
		// The uncertain result is already durable; this still-open runtime Store
		// must not run global startup recovery after ownership was activated.
		if d, err := fixtureThread(s, "one").ClaimDelivery(context.Background()); err != nil || d != nil {
			t.Fatalf("unknown delivery automatically retried: delivery=%+v error=%v", d, err)
		}
	})
}

type brokenStore struct {
	nativeRuntimeStore
	recoverErr bool
}

func (s brokenStore) ForThread(key gateway.PhysicalThreadKey) gateway.Store {
	return brokenStore{nativeRuntimeStore: nativeRuntimeStore{Store: gateway.ForThread(s.Store, key).(storage.Store)}, recoverErr: s.recoverErr}
}

func (s brokenStore) Recover(ctx context.Context) error {
	if s.recoverErr {
		return errors.New("secret-marker")
	}
	return s.Store.Recover(ctx)
}
func (s brokenStore) ClaimJob(context.Context) (*gateway.GatewayJob, error) {
	return nil, errors.New("secret-marker")
}

func TestFatalErrorsAreVisibleAndRedacted(t *testing.T) {
	for _, kind := range []string{"recovery", "worker", "source"} {
		t.Run(kind, func(t *testing.T) {
			r, s, _ := setup(t)
			switch kind {
			case "recovery":
				r.Store = brokenStore{nativeRuntimeStore: nativeRuntimeStore{Store: s}, recoverErr: true}
			case "worker":
				r.Store = brokenStore{nativeRuntimeStore: nativeRuntimeStore{Store: s}}
				_, err := s.Enqueue(context.Background(), storetest.Message("worker-failure", "failure", "alice"))
				storetest.Must(t, err)
			case "source":
				r.Source = sourceFunc(func(context.Context, gateway.EventSink, func()) error { return errors.New("secret-marker") })
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.Run(ctx); err == nil || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}
