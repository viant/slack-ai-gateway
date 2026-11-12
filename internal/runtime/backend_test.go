package runtime_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func recoveryLog(states map[gateway.BackendState]chan struct{}) *slog.Logger {
	var mu sync.Mutex
	return slog.New(slog.NewTextHandler(logWriterFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		for state, signal := range states {
			if strings.Contains(string(p), "msg=agently_backend state="+string(state)) {
				close(signal)
				delete(states, state)
			}
		}
		return len(p), nil
	}), nil))
}

func TestBackendOutageKeepsSlackConnectedWithoutReplayingQuestions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, b := setup(t)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// A process restart must also close old, unsubmitted work.
		_, err := s.Enqueue(ctx, storetest.Message("before-restart", "old", "alice"))
		storetest.Must(t, err)
		recovering, ready := make(chan struct{}), make(chan struct{})
		r.Log = recoveryLog(map[gateway.BackendState]chan struct{}{
			gateway.BackendReady: ready, gateway.BackendRecovering: recovering,
		})
		release := make(chan struct{})
		var logins atomic.Int32
		r.Backend = agently.NewManager(func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
			if logins.Add(1) == 1 {
				return nil, &agently.APIError{Status: http.StatusServiceUnavailable}
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return b, nil
			}
		}, func(ctx context.Context, state gateway.BackendState) error {
			return s.FailUnstarted(ctx, gateway.BackendReason(state))
		}, r.Log)
		sinks := make(chan gateway.EventSink, 1)
		r.Source = sourceFunc(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
			ready()
			sinks <- sink
			<-ctx.Done()
			return nil
		})
		deliveries := make(chan gateway.SlackOutgoingMessage, 8)
		r.Sender = senderFunc(func(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
			deliveries <- d
			return "confirmed", nil
		})
		queries := make(chan gateway.Query, 2)
		b.query = func(_ context.Context, q gateway.Query) (gateway.Turn, error) {
			queries <- q
			return gateway.Turn{ID: q.MessageID, State: "completed", Content: "healthy response"}, nil
		}
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		var sink gateway.EventSink
		select {
		case sink = <-sinks:
		case err := <-done:
			t.Fatalf("Slack did not connect during outage: %v", err)
		}
		// Initial temporary login failure leaves Slack running in recovery.
		// A starting state is also unavailable; no timer or network is needed to
		// submit the question and persist its terminal local reply.
		message := storetest.Message("during-outage", "outage", "alice")
		result, err := sink.Accept(ctx, message)
		storetest.Must(t, err)
		if !result.Accepted || result.Duplicate {
			t.Fatal("outage question was not durably accepted")
		}
		duplicate, err := sink.Accept(ctx, message)
		storetest.Must(t, err)
		if !duplicate.Duplicate {
			t.Fatal("outage event deduplication was lost")
		}
		for range 2 {
			select {
			case d := <-deliveries:
				if !strings.Contains(d.Text, "backend is temporarily unavailable") || !strings.Contains(d.Text, "not submitted") || !strings.Contains(d.Text, "try again later") {
					t.Fatalf("unexpected outage reply: %s", d.Text)
				}
			case err := <-done:
				t.Fatalf("runtime stopped during outage: %v", err)
			case <-ctx.Done():
				t.Fatal("test canceled before outage reply")
			}
		}
		if b.queries.Load() != 0 {
			t.Fatal("unavailable backend received a question")
		}
		close(release)
		await(t, ready)
		fresh := storetest.Message("after-recovery", "healthy", "alice")
		fresh.Text = "new healthy question"
		_, err = sink.Accept(ctx, fresh)
		storetest.Must(t, err)
		select {
		case q := <-queries:
			if q.Text != fresh.Text {
				t.Fatalf("replayed an old question: %q", q.Text)
			}
		case <-ctx.Done():
			t.Fatal("healthy question was not executed")
		}
		select {
		case d := <-deliveries:
			if d.Text != "healthy response" {
				t.Fatalf("unexpected final answer: %s", d.Text)
			}
		case err := <-done:
			t.Fatalf("runtime stopped after recovery: %v", err)
		}
		cancel()
		storetest.Must(t, finished(t, done))
		if b.queries.Load() != 1 {
			t.Fatalf("expected only the new query, got %d", b.queries.Load())
		}
	})
}

type attachOnlyBackend struct {
	reads            atomic.Int32
	creates, queries atomic.Int32
}

func (b *attachOnlyBackend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return b, nil
}
func (b *attachOnlyBackend) CreateConversation(context.Context, string) (string, error) {
	b.creates.Add(1)
	return "unexpected", nil
}
func (b *attachOnlyBackend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{ID: q.MessageID, State: "completed"}, nil
}
func (b *attachOnlyBackend) ReadTurn(_ context.Context, _, request string, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.reads.Add(1)
	return gateway.Turn{ID: request, State: "completed", Content: "recovered result"}, nil
}

func TestBackendRecoveryAfterRestartOnlyAttachesStartedQuery(t *testing.T) {
	r, s, b := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := s.Enqueue(ctx, storetest.Message("submitted", "existing", "alice"))
	storetest.Must(t, err)
	b.query = func(_ context.Context, q gateway.Query) (gateway.Turn, error) {
		return gateway.Turn{ID: q.MessageID, State: "running"}, nil
	}
	worked, err := (gateway.Worker{Store: s, Policy: r.Policy, Clients: b, AgentID: "steward"}).Step(ctx)
	storetest.Must(t, err)
	if !worked || b.queries.Load() != 1 {
		t.Fatal("original query was not submitted")
	}
	attached := &attachOnlyBackend{}
	ready := make(chan struct{})
	r.Log = recoveryLog(map[gateway.BackendState]chan struct{}{gateway.BackendReady: ready})
	r.Backend = agently.NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
		return attached, nil
	}, func(ctx context.Context, state gateway.BackendState) error {
		return s.FailUnstarted(ctx, gateway.BackendReason(state))
	}, r.Log)
	r.Source = sourceFunc(func(ctx context.Context, _ gateway.EventSink, connected func()) error {
		select {
		case <-ready:
			connected()
		case <-ctx.Done():
			return nil
		}
		<-ctx.Done()
		return nil
	})
	sent := make(chan struct{}, 1)
	r.Sender = senderFunc(func(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
		if d.Text == "recovered result" {
			sent <- struct{}{}
		}
		return "recovered-ts", nil
	})
	stopped := make(chan struct{})
	var runErr error
	go func() {
		runErr = r.Run(ctx)
		close(stopped)
	}()
	defer func() {
		cancel()
		select {
		case <-stopped:
			storetest.Must(t, runErr)
		case <-time.After(35 * time.Second):
			t.Error("runtime did not stop before storage cleanup")
		}
	}()
	select {
	case <-sent:
	case <-stopped:
		t.Fatalf("runtime stopped before delivering the reconciled answer: %v", runErr)
	case <-ctx.Done():
		t.Fatalf("reconciled answer was not delivered: state=%s reads=%d deliveries=%d", r.Backend.State(), attached.reads.Load(), r.Delivered())
	}
	if attached.reads.Load() != 1 || attached.creates.Load() != 0 || attached.queries.Load() != 0 {
		t.Fatalf("restart replayed a mutation: reads=%d creates=%d queries=%d", attached.reads.Load(), attached.creates.Load(), attached.queries.Load())
	}
}

// These synchronous controls put the outage at an exact workflow boundary;
// the real SQLite/Datly store still owns every claim and terminal transition.
type controlledAvailability struct {
	gateway.BackendLifecycle
	state gateway.BackendState
}

func (b *controlledAvailability) State() gateway.BackendState { return b.state }
func (b *controlledAvailability) WithReady(fn func() error) error {
	if b.state != gateway.BackendReady {
		return gateway.ErrBackendUnavailable
	}
	return fn()
}

type outagePolicy func(context.Context, gateway.Message, *gateway.ConversationBinding) (gateway.Decision, error)

func (f outagePolicy) Authorize(ctx context.Context, m gateway.Message, b *gateway.ConversationBinding) (gateway.Decision, error) {
	return f(ctx, m, b)
}

func TestBackendOutageDuringPolicyDoesNotOverwriteLocalReply(t *testing.T) {
	for _, outcome := range []string{"denied", "metadata_unavailable", "empty_question"} {
		for _, recovered := range []bool{false, true} {
			name := outcome + "/recovering"
			if recovered {
				name = outcome + "/already_ready_again"
			}
			t.Run(name, func(t *testing.T) {
				r, s, b := setup(t)
				ctx := context.Background()
				m := storetest.Message("policy-outage", "one", "alice")
				if outcome == "empty_question" {
					m.Text = ""
				}
				_, err := s.Enqueue(ctx, m)
				storetest.Must(t, err)
				availability := &controlledAvailability{state: gateway.BackendReady}
				policy := outagePolicy(func(ctx context.Context, m gateway.Message, binding *gateway.ConversationBinding) (gateway.Decision, error) {
					availability.state = gateway.BackendRecovering
					storetest.Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
					if recovered {
						availability.state = gateway.BackendReady
					}
					switch outcome {
					case "denied":
						return gateway.Decision{}, gateway.ErrDenied
					case "metadata_unavailable":
						return gateway.Decision{}, errors.New("controlled metadata outage")
					default:
						return r.Policy.Authorize(ctx, m, binding)
					}
				})
				worked, err := (gateway.Worker{Store: s, Policy: policy, Clients: b, Backend: availability, AgentID: "steward"}).Step(ctx)
				storetest.Must(t, err)
				state, err := s.Status(ctx, 10)
				storetest.Must(t, err)
				if !worked || len(state.GatewayJobs) != 1 || state.GatewayJobs[0].State != "completed" || state.GatewayJobs[0].Reason != "backend_unavailable" || state.GatewayJobs[0].BackendSubmissionStartedAt != 0 || b.queries.Load() != 0 {
					t.Fatalf("stale policy result replaced outage disposition: worked=%t state=%+v queries=%d", worked, state.GatewayJobs, b.queries.Load())
				}
				d, err := s.ClaimDelivery(ctx)
				storetest.Must(t, err)
				if d == nil || d.Text != gateway.BackendNotice("backend_unavailable") {
					t.Fatalf("local outage reply changed: %+v", d)
				}
			})
		}
	}
}

type lateCreateClient struct {
	gateway.AgentlyClient
	create func(context.Context) (string, error)
}

func (b lateCreateClient) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return b, nil
}
func (b lateCreateClient) CreateConversation(ctx context.Context, _ string) (string, error) {
	return b.create(ctx)
}

func TestBackendOutageAfterCreateSuccessClosesOnlyUnsubmittedQuestion(t *testing.T) {
	r, s, b := setup(t)
	ctx := context.Background()
	_, err := s.Enqueue(ctx, storetest.Message("late-create", "one", "alice"))
	storetest.Must(t, err)
	availability := &controlledAvailability{state: gateway.BackendReady}
	client := lateCreateClient{AgentlyClient: b, create: func(ctx context.Context) (string, error) {
		availability.state = gateway.BackendRecovering
		storetest.Must(t, s.FailUnstarted(ctx, "backend_unavailable"))
		// The sweep must leave the uncertain create alone. Its HTTP response
		// then resolves the conversation identity while the backend is down.
		return "confirmed-late-conversation", nil
	}}
	worked, err := (gateway.Worker{Store: s, Policy: r.Policy, Clients: client, Backend: availability, AgentID: "steward"}).Step(ctx)
	storetest.Must(t, err)
	state, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if !worked || len(state.GatewayJobs) != 1 || state.GatewayJobs[0].State != "completed" || state.GatewayJobs[0].Reason != "backend_unavailable" || state.GatewayJobs[0].BackendSubmissionStartedAt != 0 || b.queries.Load() != 0 {
		t.Fatalf("late create left unsubmitted work stuck: worked=%t state=%+v queries=%d", worked, state.GatewayJobs, b.queries.Load())
	}
	binding, err := s.GetBinding(ctx, storetest.Message("", "one", "alice").ThreadKey)
	storetest.Must(t, err)
	if binding == nil || binding.ConversationID != "confirmed-late-conversation" {
		t.Fatalf("known remote identity was lost: %+v", binding)
	}
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != gateway.BackendNotice("backend_unavailable") {
		t.Fatalf("missing durable local failure reply: %+v", d)
	}
}
