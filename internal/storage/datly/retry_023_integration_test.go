package datly_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func retry023Wait(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("native retry barrier was not reached")
	}
}

func TestWorkflow023RetryRetainsOwnerWhileIndependentThreadContinues(t *testing.T) {
	root, _, fault, id := retry022Open(t, "mysql")
	firstBlocked, retryBlocked := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseRetry := make(chan struct{}), make(chan struct{})
	var firstOnce, retryOnce sync.Once
	release := func() { firstOnce.Do(func() { close(releaseFirst) }); retryOnce.Do(func() { close(releaseRetry) }) }
	t.Cleanup(release)
	fault.onMatch = func(ctx context.Context, _ string, call int32, fails bool) error {
		var wait <-chan struct{}
		if fails {
			close(firstBlocked)
			wait = releaseFirst
		} else if call == 3 {
			close(retryBlocked)
			wait = releaseRetry
		}
		if wait != nil {
			select {
			case <-wait:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	fault.arm(1)
	original := storetest.Message("owner-retry-event", "owner-retry-root", "alice")
	original.StatusMode = gateway.StatusHybrid
	bound := gateway.ForThread(root.Scope(id), original.PhysicalThread())
	type result struct {
		inserted bool
		err      error
	}
	originalDone := make(chan result, 1)
	go func() { inserted, err := bound.Enqueue(t.Context(), original); originalDone <- result{inserted, err} }()
	retry023Wait(t, firstBlocked)
	followerEntered := make(chan struct{})
	followerDone := make(chan error, 1)
	follower, ok := bound.(gateway.AsyncThreadStore)
	if !ok {
		t.Fatal("native fixture does not support synchronous mailbox admission")
	}
	if !follower.TryInThread(t.Context(), func(gateway.Store) error { close(followerEntered); return nil }, func(err error) { followerDone <- err }) {
		t.Fatal("same-thread follower was not admitted to the occupied mailbox")
	}
	independent := storetest.Message("independent-retry-event", "independent-retry-root", "bob")
	independent.StatusMode = gateway.StatusHybrid
	inserted, err := root.Scope(id).Enqueue(t.Context(), independent)
	retry022Must(t, err)
	if !inserted {
		t.Fatal("independent physical thread did not make progress")
	}
	firstOnce.Do(func() { close(releaseFirst) })
	retry023Wait(t, retryBlocked)
	select {
	case <-followerEntered:
		t.Fatal("same-thread command entered between attempts")
	default:
	}
	if fault.activeRoots.Load() != 1 {
		t.Fatalf("previous root retained a transaction: %d", fault.activeRoots.Load())
	}
	retryOnce.Do(func() { close(releaseRetry) })
	select {
	case result := <-originalDone:
		retry022Must(t, result.err)
		if !result.inserted {
			t.Fatal("original intake was not committed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("retried command did not complete")
	}
	retry023Wait(t, followerEntered)
	storetest.Must(t, <-followerDone)
	if fault.roots.Load() != 3 || fault.activeRoots.Load() != 0 {
		t.Fatalf("unexpected concurrent transaction lifecycle: roots=%d active=%d", fault.roots.Load(), fault.activeRoots.Load())
	}
}

func TestWorkflow023CanceledNativeAttemptDoesNotStartAnother(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			root, _, fault, id := retry022Open(t, dialect)
			entered, rolledBack := make(chan struct{}), make(chan struct{}, 1)
			fault.onRollback = func() { rolledBack <- struct{}{} }
			fault.onMatch = func(ctx context.Context, _ string, _ int32, _ bool) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			fault.arm(1)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			message := storetest.Message("cancel-retry-event", "cancel-retry-root", "alice")
			message.StatusMode = gateway.StatusHybrid
			done := make(chan error, 1)
			go func() { _, err := root.Scope(id).Enqueue(ctx, message); done <- err }()
			retry023Wait(t, entered)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation lost: %T", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("canceled native command did not join")
			}
			retry023Wait(t, rolledBack)
			if fault.roots.Load() != 1 || fault.publicationCalls.Load() != 1 || fault.activeRoots.Load() != 0 {
				t.Fatalf("canceled attempt was replayed or leaked: roots=%d calls=%d active=%d", fault.roots.Load(), fault.publicationCalls.Load(), fault.activeRoots.Load())
			}
		})
	}
}

func TestWorkflow023RetryKeepsAbsoluteCommandDeadline(t *testing.T) {
	root, _, fault, id := retry022Open(t, "sqlite")
	// Warm native graph construction before measuring the shared caller budget.
	warm := storetest.Message("deadline-warm", "deadline-warm", "alice")
	warm.StatusMode = gateway.StatusHybrid
	_, err := root.Scope(id).Enqueue(t.Context(), warm)
	storetest.Must(t, err)
	var deadlines []time.Time
	rolledBack := make(chan struct{}, 2)
	fault.onRollback = func() { rolledBack <- struct{}{} }
	fault.onMatch = func(ctx context.Context, _ string, call int32, _ bool) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			return errors.New("native statement lost caller deadline")
		}
		deadlines = append(deadlines, deadline)
		if call == 2 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	fault.arm(1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	expected, _ := ctx.Deadline()
	started := time.Now()
	message := storetest.Message("deadline-retry-event", "deadline-retry-root", "alice")
	message.StatusMode = gateway.StatusHybrid
	_, err = root.Scope(id).Enqueue(ctx, message)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %T", err)
	}
	if len(deadlines) != 2 || !deadlines[0].Equal(expected) || !deadlines[1].Equal(expected) {
		t.Fatalf("retry renewed the command deadline: statements=%d", len(deadlines))
	}
	if time.Since(started) > 1500*time.Millisecond {
		t.Fatal("native retry exceeded caller budget plus cleanup allowance")
	}
	retry023Wait(t, rolledBack)
	retry023Wait(t, rolledBack)
	if fault.roots.Load() != 2 || fault.activeRoots.Load() != 0 {
		t.Fatalf("deadline cleanup did not finish: roots=%d active=%d", fault.roots.Load(), fault.activeRoots.Load())
	}
}

type retry023Backend struct{ creates, queries, reads atomic.Int32 }

func (b *retry023Backend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return b, nil
}
func (b *retry023Backend) CreateConversation(context.Context, string) (string, error) {
	b.creates.Add(1)
	return "owned-fake-conversation", nil
}
func (b *retry023Backend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: "owned fake response"}, nil
}
func (b *retry023Backend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	b.reads.Add(1)
	return gateway.Turn{}, errors.New("unexpected read")
}

type retry023Sender struct{ sends, statuses atomic.Int32 }

func (s *retry023Sender) Send(context.Context, gateway.SlackOutgoingMessage) (string, error) {
	s.sends.Add(1)
	return "123.000001", nil
}
func (s *retry023Sender) SendStatus(_ context.Context, p gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	s.statuses.Add(1)
	return gateway.StatusReceipt{Acknowledged: true, AgentStatus: p.SessionState, SessionStatus: p.SessionState}, nil
}

func TestWorkflow023RetryOfPersistenceDoesNotRepeatExternalCalls(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			for _, phase := range []string{"set_conversation", "complete", "delivered", "finish_status"} {
				t.Run(phase, func(t *testing.T) {
					root, db, fault, id := retry022Open(t, dialect)
					message := storetest.Message("external-retry-event", "external-retry-root", "alice")
					message.StatusMode = gateway.StatusAgentSession
					bound := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					_, err := bound.Enqueue(t.Context(), message)
					storetest.Must(t, err)
					backend, sender := &retry023Backend{}, &retry023Sender{}
					policy := storetest.Policy("A", "T", "shared", "C")
					worker := gateway.Worker{Store: bound, Clients: backend, AgentID: "steward", Policy: policy}
					switch phase {
					case "set_conversation":
						fault.match, fault.requiredColumn = "update conversation_binding", "backend_conversation_id"
					case "complete":
						fault.match, fault.requiredColumn = "insert into slack_outgoing_message", "slack_message_text"
					case "delivered":
						fault.match, fault.requiredColumn = "update slack_outgoing_message", "slack_message_id"
					case "finish_status":
						fault.match, fault.requiredColumn = "update slack_agent_session_publication", "slack_publication_sent_revision"
					}
					if phase == "set_conversation" || phase == "complete" {
						fault.arm(1)
					}
					if phase != "finish_status" {
						worked, err := worker.Step(t.Context())
						retry022Must(t, err)
						if !worked {
							t.Fatal("fake backend question was not executed")
						}
						if backend.creates.Load() != 1 || backend.queries.Load() != 1 || backend.reads.Load() != 0 {
							t.Fatalf("backend side effects repeated: creates=%d queries=%d reads=%d", backend.creates.Load(), backend.queries.Load(), backend.reads.Load())
						}
					}
					switch phase {
					case "delivered":
						fault.arm(1)
						delivery := gateway.DeliveryWorker{Store: bound, Sender: sender, Policy: policy}
						worked, err := delivery.Step(t.Context())
						retry022Must(t, err)
						if !worked || sender.sends.Load() != 1 {
							t.Fatal("retry repeated Slack response POST")
						}
						var state, messageID string
						storetest.Must(t, db.QueryRow("SELECT slack_delivery_state,slack_message_id FROM slack_outgoing_message").Scan(&state, &messageID))
						if state != "delivered" || messageID != "123.000001" {
							t.Fatal("known external success was not persisted")
						}
					case "finish_status":
						fault.arm(1)
						status := gateway.StatusWorker{Store: bound, Sender: sender, Policy: policy}
						worked, err := status.Step(t.Context(), "urgent")
						retry022Must(t, err)
						if !worked || sender.statuses.Load() != 1 {
							t.Fatal("retry repeated Slack native status call")
						}
						var revision, sent int
						storetest.Must(t, db.QueryRow("SELECT slack_publication_revision,slack_publication_sent_revision FROM slack_agent_session_publication").Scan(&revision, &sent))
						if revision != sent {
							t.Fatal("known native status acknowledgement was not persisted")
						}
					}
					if fault.publicationCalls.Load() != 2 {
						t.Fatalf("persistence fault did not exercise retry: phase=%s calls=%d", phase, fault.publicationCalls.Load())
					}
					if fault.activeRoots.Load() != 0 {
						t.Fatal("external receipt persistence leaked a transaction")
					}
				})
			}
		})
	}
}

func TestWorkflow023RetryCoversExclusiveStartupAndOfflineResolution(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			t.Run("startup_reconcile", func(t *testing.T) {
				root, db, fault, _ := retry022OpenState(t, dialect, false)
				before := root.WorkRevision(0)
				fault.match, fault.requiredColumn = "insert into integration", "backend_endpoint"
				fault.arm(1)
				other := storetest.DefaultIntegration()
				other.Name = "second"
				other.AppID = "B"
				rows, err := root.ReconcileIntegrations(t.Context(), []storage.Integration{storetest.DefaultIntegration(), other})
				retry022Must(t, err)
				if len(rows) != 2 || fault.publicationCalls.Load() != 2 || fault.roots.Load() != 2 {
					t.Fatalf("startup retry not exercised: rows=%d calls=%d roots=%d", len(rows), fault.publicationCalls.Load(), fault.roots.Load())
				}
				var count int
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM integration").Scan(&count))
				if count != 2 || root.WorkRevision(0) != before+1 {
					t.Fatal("startup replay duplicated identity or work notification")
				}
			})
			t.Run("offline_resolution", func(t *testing.T) {
				root, db, fault, id := retry022OpenState(t, dialect, false)
				message := storetest.Message("offline-retry-event", "offline-retry-root", "alice")
				bound := gateway.ForThread(root.Scope(id), message.PhysicalThread())
				_, err := bound.Enqueue(t.Context(), message)
				storetest.Must(t, err)
				job, err := bound.ClaimJob(t.Context())
				storetest.Must(t, err)
				storetest.Must(t, bound.Complete(t.Context(), job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
				part, err := bound.ClaimDelivery(t.Context())
				storetest.Must(t, err)
				storetest.Must(t, bound.DeliveryUnknown(t.Context(), part.ID))
				fault.match, fault.requiredColumn = "insert into slack_delivery_resolution", "resolution_action"
				fault.arm(1)
				retry022Must(t, root.ResolveDelivery(t.Context(), part.ID, "delivered", "300.000001", false))
				var count int
				var state string
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_delivery_resolution").Scan(&count))
				storetest.Must(t, db.QueryRow("SELECT slack_delivery_state FROM slack_outgoing_message WHERE id=?", part.ID).Scan(&state))
				if count != 1 || state != "delivered" || fault.publicationCalls.Load() != 2 || fault.roots.Load() != 2 {
					t.Fatalf("offline resolution replay not atomic: resolutions=%d state=%s calls=%d roots=%d", count, state, fault.publicationCalls.Load(), fault.roots.Load())
				}
			})
		})
	}
}

func TestWorkflow023UsesFinalAttemptTransactionEvidence(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			for _, scenario := range []struct {
				name                                                string
				failures, commitAt, beginAt, wantAttempts, wantRows int32
				wantState                                           string
			}{
				{name: "first_commit_unknown", commitAt: 1, wantAttempts: 1, wantRows: 1, wantState: "commit_unknown"},
				{name: "second_commit_unknown", failures: 1, commitAt: 2, wantAttempts: 2, wantRows: 1, wantState: "commit_unknown"},
				{name: "second_begin_failed", failures: 1, beginAt: 2, wantAttempts: 2, wantState: "none"},
				{name: "first_begin_failed", beginAt: 1, wantAttempts: 1, wantState: "none"},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					root, db, fault, id := retry022Open(t, dialect)
					before := root.WorkRevision(id)
					fault.commitFailureAt, fault.beginFailureAt = scenario.commitAt, scenario.beginAt
					if scenario.name == "second_begin_failed" {
						fault.beginFailure = errors.New("fixture second connection failure")
					}
					fault.arm(scenario.failures)
					message := storetest.Message("outcome-retry-event", "outcome-retry-root", "alice")
					message.StatusMode = gateway.StatusHybrid
					inserted, err := root.Scope(id).Enqueue(t.Context(), message)
					var issue *gateway.StorageError
					if inserted || !errors.As(err, &issue) || issue.Attempts != int(scenario.wantAttempts) || issue.TransactionState != scenario.wantState {
						t.Fatalf("final evidence inherited from previous attempt: inserted=%t error=%+v", inserted, issue)
					}
					if fault.roots.Load() != scenario.wantAttempts || fault.activeRoots.Load() != 0 {
						t.Fatalf("unexpected invocation count or open root: roots=%d active=%d", fault.roots.Load(), fault.activeRoots.Load())
					}
					if scenario.wantAttempts == 2 && issue.MySQLCode != 0 {
						t.Fatal("second failure inherited first attempt's contention code")
					}
					var jobs, publications int32
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&jobs))
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
					if jobs != scenario.wantRows || publications != scenario.wantRows || root.WorkRevision(id) != before {
						t.Fatalf("unknown/failed outcome was replayed or published: jobs=%d publications=%d", jobs, publications)
					}
					hints := root.TakeWorkHints()
					if scenario.wantState == "commit_unknown" && (len(hints) != 1 || hints[0].Thread != message.PhysicalThread()) {
						t.Fatal("uncertain commit lost targeted reconciliation")
					}
				})
			}
		})
	}
}

func TestWorkflow023OrdinarySuccessUsesOneAttempt(t *testing.T) {
	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			root, db, fault, id := retry022Open(t, "sqlite")
			before := root.WorkRevision(id)
			fault.arm(0)
			message := storetest.Message("ordinary-event", "ordinary-root", "alice")
			message.StatusMode = mode
			inserted, err := root.Scope(id).Enqueue(t.Context(), message)
			storetest.Must(t, err)
			if !inserted || fault.roots.Load() != 1 || fault.jobInserts.Load() != 1 || root.WorkRevision(id) != before+1 {
				t.Fatalf("ordinary success repeated native work: inserted=%t roots=%d jobs=%d", inserted, fault.roots.Load(), fault.jobInserts.Load())
			}
			expectedPublications := int32(1)
			if mode == gateway.StatusMessage {
				expectedPublications = 0
			}
			if fault.publicationCalls.Load() != expectedPublications {
				t.Fatal("ordinary success repeated native publication")
			}
			root.TakeWorkHints()
			inserted, err = root.Scope(id).Enqueue(t.Context(), message)
			storetest.Must(t, err)
			if inserted || fault.roots.Load() != 2 || fault.jobInserts.Load() != 1 || fault.publicationCalls.Load() != expectedPublications || root.WorkRevision(id) != before+1 || len(root.TakeWorkHints()) != 0 {
				t.Fatalf("ordinary duplicate repeated effects or native call: inserted=%t roots=%d jobs=%d", inserted, fault.roots.Load(), fault.jobInserts.Load())
			}
			var jobs, publications int32
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&jobs))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
			if jobs != 1 || publications != expectedPublications || fault.activeRoots.Load() != 0 {
				t.Fatal("ordinary call left unexpected durable work or transaction")
			}
		})
	}
}
