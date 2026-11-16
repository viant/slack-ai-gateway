package runtime_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type recovery021Backend struct {
	creates, queries, reads atomic.Int32
	attached                chan [2]string
}

func (b *recovery021Backend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if principal != "shared" {
		return nil, fmt.Errorf("unexpected execution identity")
	}
	return b, nil
}

func (b *recovery021Backend) CreateConversation(context.Context, string) (string, error) {
	b.creates.Add(1)
	return "unexpected-new-conversation", nil
}

func (b *recovery021Backend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: "unexpected replay"}, nil
}

func (b *recovery021Backend) ReadTurn(_ context.Context, conversation, request string, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.reads.Add(1)
	b.attached <- [2]string{conversation, request}
	return gateway.Turn{ID: request, State: "completed", Content: "recovered original result"}, nil
}

func recovery021Receive[T any](t *testing.T, values <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(timeout):
		t.Fatal("timed out waiting for recovery fixture")
		var zero T
		return zero
	}
}

func recovery021Store(t *testing.T, driver string) storage.Store {
	t.Helper()
	ctx := context.Background()
	if driver == "mysql" {
		s, err := mysql.Open(ctx, storetest.MySQLDSN(t), nil)
		storetest.Must(t, err)
		t.Cleanup(func() { storetest.Must(t, s.Close()) })
		return s
	}
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "recovery.sqlite"), true)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	t.Cleanup(func() { storetest.Must(t, s.Close()) })
	return s
}

// Authentication is controlled locally; every job, hint, claim and delivery uses
// the native shared store and real Group/Manager/scheduler. No chat is retried.
func TestRemoteAuthRecoveryResumesOriginalRunAndIsolatesIntegrations(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			if driver == "mysql" && os.Getenv("SLACK_AI_GATEWAY_MYSQL_TEST_DSN") == "" {
				t.Skip("set SLACK_AI_GATEWAY_MYSQL_TEST_DSN for isolated MySQL recovery tests")
			}
			for _, retry := range []struct {
				name  string
				after time.Duration
			}{{name: "remote_retry_window"}, {name: "retry_after_ready_wakes_parked_lane", after: 77 * time.Second}} {
				t.Run(retry.name, func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						root := recovery021Store(t, driver)
						firstID, secondID := storetest.DefaultIntegration(), storetest.DefaultIntegration()
						firstID.Name, secondID.Name, secondID.AppID = "recovering", "healthy", "A_HEALTHY"
						rows, err := root.ReconcileIntegrations(ctx, []storage.Integration{firstID, secondID})
						storetest.Must(t, err)
						first, second := root.Scope(rows[0].ID), root.Scope(rows[1].ID)
						firstRuntime, seedBackend := runtimeForStore(first)
						secondRuntime, healthyBackend := runtimeForStore(second)
						secondRuntime.Route = storetest.Route(secondID.AppID, "T", "C")
						secondRuntime.Policy = storetest.Policy(secondID.AppID, "T", "shared", "C")

						submitted := make(chan gateway.Query, 1)
						seedBackend.query = func(_ context.Context, q gateway.Query) (gateway.Turn, error) {
							submitted <- q
							return gateway.Turn{ID: q.MessageID, State: "running"}, nil
						}
						_, err = first.Enqueue(ctx, storetest.Message("submitted", "original", "alice"))
						storetest.Must(t, err)
						worked, err := (gateway.Worker{Store: first, Policy: firstRuntime.Policy, Clients: seedBackend, AgentID: "steward"}).Step(ctx)
						storetest.Must(t, err)
						if !worked {
							t.Fatal("original request did not start")
						}
						original := recovery021Receive(t, submitted, time.Second)
						_, err = first.Enqueue(ctx, storetest.Message("review", "review", "alice"))
						storetest.Must(t, err)
						reviewJob, err := first.ClaimJob(ctx)
						storetest.Must(t, err)
						if reviewJob == nil || reviewJob.EventID != "review" {
							t.Fatal("missing independent review fixture")
						}
						storetest.Must(t, first.Review(ctx, reviewJob.ID, "reconciliation_deadline"))

						attached := &recovery021Backend{attached: make(chan [2]string, 4)}
						attempts := make(chan time.Time, 4)
						var logins atomic.Int32
						quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
						firstRuntime.Backend = agently.NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
							attempts <- time.Now()
							if logins.Add(1) == 1 {
								// This is an authentication-boundary refusal, not a query 403.
								return nil, &agently.APIError{Status: http.StatusForbidden, Permanent: true, RetryAfter: retry.after}
							}
							return attached, nil
						}, func(ctx context.Context, state gateway.BackendState) error {
							return first.FailUnstarted(ctx, gateway.BackendReason(state))
						}, quiet)
						secondRuntime.Backend = agently.NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
							return healthyBackend, nil
						}, func(ctx context.Context, state gateway.BackendState) error {
							return second.FailUnstarted(ctx, gateway.BackendReason(state))
						}, quiet)
						firstSink, secondSink := make(chan gateway.EventSink, 1), make(chan gateway.EventSink, 1)
						firstRuntime.Source = sourceFunc(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
							ready()
							firstSink <- sink
							<-ctx.Done()
							return nil
						})
						secondRuntime.Source = sourceFunc(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
							ready()
							secondSink <- sink
							<-ctx.Done()
							return nil
						})
						firstDeliveries, secondDeliveries := make(chan gateway.SlackOutgoingMessage, 8), make(chan gateway.SlackOutgoingMessage, 8)
						readyStatuses := make(chan time.Time, 4)
						firstRuntime.Sender = statusRuntimeSender{senderFunc: func(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
							firstDeliveries <- d
							return "2.000001", nil
						}, status: func(_ context.Context, status gateway.GatewayJobProgress, _ string) (string, error) {
							if status.RootTS == "original" && status.Phase == "checking" {
								readyStatuses <- time.Now()
							}
							if status.SlackTS != "" {
								return status.SlackTS, nil
							}
							return "3.000001", nil
						}}
						secondRuntime.Sender = senderFunc(func(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
							secondDeliveries <- d
							return "2.000002", nil
						})
						group := &runner.Group{Store: root, Integrations: []runner.Integration{{ID: "recovering", Runtime: firstRuntime}, {ID: "healthy", Runtime: secondRuntime}}, Log: quiet}
						done := make(chan error, 1)
						go func() { done <- group.Run(ctx) }()
						defer func() { cancel(); storetest.Must(t, finished(t, done)) }()
						failedAt := recovery021Receive(t, attempts, 5*time.Second)
						sink := recovery021Receive(t, firstSink, 5*time.Second)
						healthySink := recovery021Receive(t, secondSink, 5*time.Second)
						synctest.Wait()
						if firstRuntime.Backend.State() == gateway.BackendReady || secondRuntime.Backend.State() != gateway.BackendReady {
							t.Fatal("remote refusal affected the other integration's readiness")
						}
						unavailable := storetest.Message("not-submitted", "outage", "bob")
						accepted, err := sink.Accept(ctx, unavailable)
						storetest.Must(t, err)
						duplicate, err := sink.Accept(ctx, unavailable)
						storetest.Must(t, err)
						if !accepted.Accepted || accepted.Duplicate || !duplicate.Duplicate {
							t.Fatal("outage admission or event deduplication changed")
						}
						notice := recovery021Receive(t, firstDeliveries, 5*time.Second)
						if notice.Text != gateway.BackendNotice("backend_unavailable") || notice.RootTS != "outage" {
							t.Fatal("unsubmitted question did not receive its durable local notice")
						}
						healthyPrompt := storetest.Message("healthy-during-outage", "healthy", "alice")
						healthyPrompt.AppID = secondID.AppID
						_, err = healthySink.Accept(ctx, healthyPrompt)
						storetest.Must(t, err)
						if d := recovery021Receive(t, secondDeliveries, 5*time.Second); d.Text != "reply" || d.RootTS != "healthy" {
							t.Fatal("healthy integration did not answer during another integration's auth retry")
						}
						minimum, maximum := 60*time.Second, 75*time.Second
						if retry.after > maximum {
							minimum, maximum = retry.after, retry.after
						}
						time.Sleep(max(0, failedAt.Add(minimum-time.Second).Sub(time.Now())))
						synctest.Wait()
						if logins.Load() != 1 || attached.reads.Load() != 0 {
							t.Fatal("remote authentication retried before its minimum interval")
						}
						time.Sleep(maximum - minimum + time.Second + time.Millisecond)
						synctest.Wait()
						retriedAt := recovery021Receive(t, attempts, time.Second)
						if elapsed := retriedAt.Sub(failedAt); elapsed < minimum || elapsed > maximum || logins.Load() != 2 || firstRuntime.Backend.State() != gateway.BackendReady {
							t.Fatalf("remote authentication did not recover in its interval: elapsed=%s attempts=%d", elapsed, logins.Load())
						}
						if retry.after != 0 {
							// The last poll at 74s deferred the next read to 89s; the
							// next reconciliation is 80s. Only the existing ready wake
							// can publish this changed phase promptly at 77s.
							at := recovery021Receive(t, readyStatuses, time.Second)
							if at.Before(retriedAt) || at.Sub(retriedAt) > time.Millisecond {
								t.Fatal("ready did not wake the parked outbound lane before fallback reconciliation")
							}
						}
						// Stored poll deadlines still apply; readiness wakes the parked
						// scheduler but must not bypass its authoritative native claim.
						seen := recovery021Receive(t, attached.attached, 20*time.Second)
						if seen != [2]string{original.ConversationID, original.MessageID} {
							t.Fatal("recovery attached a different conversation or request")
						}
						if d := recovery021Receive(t, firstDeliveries, 5*time.Second); d.Text != "recovered original result" || d.RootTS != "original" {
							t.Fatal("original request did not complete after readiness returned")
						}
						synctest.Wait()
						if seedBackend.queries.Load() != 1 || attached.creates.Load() != 0 || attached.queries.Load() != 0 || attached.reads.Load() != 1 || healthyBackend.queries.Load() != 1 {
							t.Fatal("recovery replayed a prompt/create or blocked the healthy integration")
						}
						state, err := root.Status(ctx, 20)
						storetest.Must(t, err)
						if len(state.GatewayJobs) != 4 || len(state.Integrations) != 2 {
							t.Fatal("recovery changed integration assignments or duplicated durable jobs")
						}
						for _, job := range state.GatewayJobs {
							switch job.ThreadTS {
							case "review":
								if job.ID != reviewJob.ID || job.State != "needs_review" || job.Reason != "reconciliation_deadline" {
									t.Fatal("authentication recovery released historical review work")
								}
							case "outage":
								if job.State != "completed" || job.Reason != "backend_unavailable" || job.BackendSubmissionStartedAt != 0 {
									t.Fatal("locally rejected prompt was submitted after recovery")
								}
							case "original":
								if job.GatewayRequestID != original.MessageID || job.State != "completed" {
									t.Fatal("started request identity was reset")
								}
							}
						}
					})
				})
			}
		})
	}
}
