package slack

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slack-go/slack/socketmode"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func intakeDurabilityFixture(t *testing.T, driver string) (storage.Store, *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var root storage.Store
	var dsn string
	if driver == "mysql" {
		dsn = storetest.MySQLDSN(t)
		s, err := mysql.Open(ctx, dsn, nil)
		storetest.Must(t, err)
		root = s
	} else {
		path := filepath.Join(t.TempDir(), "intake.sqlite")
		s, err := sqlite.Open(ctx, path, true)
		storetest.Must(t, err)
		storetest.Must(t, s.Migrate(ctx))
		root, dsn = s, path+"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
	}
	t.Cleanup(func() { storetest.Must(t, root.Close()) })
	// SQL access here is fixture/assertion access, separate from Datly's pool.
	db, err := sql.Open(driver, dsn)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	return root, db
}

func durabilityRequest(app, event, envelope, thread string) socketmode.Request {
	return socketmode.Request{Type: socketmode.RequestTypeEventsAPI, EnvelopeID: envelope, Payload: payload(func(c, e map[string]any) {
		c["api_app_id"], c["event_id"] = app, event
		e["ts"], e["text"] = thread, "<@UBOT> "+event
	})}
}

type intakeDurabilityFact struct {
	id, integration int64
	request         string
}

func durabilityFact(t *testing.T, db *sql.DB, event string) (intakeDurabilityFact, bool) {
	t.Helper()
	var fact intakeDurabilityFact
	err := db.QueryRow("SELECT id,integration_id,gateway_request_id FROM gateway_job WHERE slack_event_id=?", event).Scan(&fact.id, &fact.integration, &fact.request)
	if errors.Is(err, sql.ErrNoRows) {
		return fact, false
	}
	storetest.Must(t, err)
	return fact, true
}

func TestSourceDeadlinePreservesNativeIntakeAndRetryIdentity(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []string{"expired_before_native_start", "committed_then_returned_deadline"} {
				t.Run(mode, func(t *testing.T) {
					root, db := intakeDurabilityFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					intake := gateway.Intake{Store: root.Scope(id), Route: testRoute, StatusMode: gateway.StatusHybrid}
					acks := []string{}
					source := Source{session: &fakeSession{ack: func(envelope string) error { acks = append(acks, envelope); return nil }}, route: testRoute, botUserID: "UBOT", log: quietLog()}
					var firstErr error
					var firstResult gateway.AcceptResult
					var original intakeDurabilityFact
					calls := 0
					sink := sinkFunc(func(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
						calls++
						if calls != 1 {
							return intake.Accept(ctx, m)
						}
						if mode == "expired_before_native_start" {
							<-ctx.Done()
							firstResult, firstErr = intake.Accept(ctx, m)
							return firstResult, firstErr
						}
						firstResult, firstErr = intake.Accept(ctx, m)
						if firstErr != nil {
							return firstResult, firstErr
						}
						var exists bool
						original, exists = durabilityFact(t, db, m.EventID)
						if !exists {
							t.Fatal("native successful admission had no durable job")
						}
						<-ctx.Done()
						// This models uncertainty returned to Source AFTER an actual
						// confirmed commit. It is not a manufactured native Outcome.
						return gateway.AcceptResult{}, errors.Join(ctx.Err(), sql.ErrTxDone)
					})
					ctx := context.Background()
					first := durabilityRequest("A", "deadline-E1", "first-envelope", "1.000001")
					storetest.Must(t, source.handle(ctx, first, sink))
					if len(acks) != 0 {
						t.Fatal("failed attempt was acknowledged")
					}
					if mode == "expired_before_native_start" {
						if !errors.Is(firstErr, context.DeadlineExceeded) {
							t.Fatalf("native canceled admission lost deadline category: %T", firstErr)
						}
						if _, exists := durabilityFact(t, db, "deadline-E1"); exists {
							t.Fatal("expired native admission persisted a job")
						}
					} else if firstErr != nil || !firstResult.Accepted || original.request == "" {
						t.Fatal("fixture did not establish its confirmed first commit")
					}
					storetest.Must(t, source.handle(ctx, durabilityRequest("A", "deadline-E2", "second-envelope", "2.000001"), sink))
					retry := first
					retry.EnvelopeID, retry.RetryAttempt = "retry-envelope", 1
					storetest.Must(t, source.handle(ctx, retry, sink))
					fact, exists := durabilityFact(t, db, "deadline-E1")
					if !exists || fact.id <= 0 || fact.request == "" || fact.integration != id {
						t.Fatal("retry lost its native job identity")
					}
					if mode == "committed_then_returned_deadline" && fact != original {
						t.Fatal("retry replaced the first committed identity")
					}
					retry.EnvelopeID, retry.RetryAttempt = "another-retry-envelope", 2
					storetest.Must(t, source.handle(ctx, retry, sink))
					again, exists := durabilityFact(t, db, "deadline-E1")
					if !exists || again != fact {
						t.Fatal("duplicate retry changed the durable request")
					}
					if fmt.Sprint(acks) != "[second-envelope retry-envelope another-retry-envelope]" {
						t.Fatalf("ACKs used the wrong envelope: %v", acks)
					}
					var jobs, publications, submissions, outgoing int
					storetest.Must(t, db.QueryRow("SELECT COUNT(*),COUNT(backend_submission_started_at) FROM gateway_job").Scan(&jobs, &submissions))
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&outgoing))
					if jobs != 2 || publications != 2 || submissions != 0 || outgoing != 0 {
						t.Fatalf("native graph duplicated or submitted work: jobs=%d publications=%d submissions=%d outgoing=%d", jobs, publications, submissions, outgoing)
					}
					candidates, err := root.WorkCandidates(ctx, time.Now())
					storetest.Must(t, err)
					found := false
					for _, candidate := range candidates {
						found = found || candidate.IntegrationID == id && candidate.Query.ProbeNow
					}
					if !found {
						t.Fatal("native discovery did not find the durable admitted work")
					}
				})
			}
		})
	}
}

func TestSourceOwnedDatabaseLockDeadlineClassification(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			root, db := intakeDurabilityFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			intake := gateway.Intake{Store: root.Scope(id), Route: testRoute}
			conn, err := db.Conn(context.Background())
			storetest.Must(t, err)
			defer conn.Close()
			lock, unlock := "BEGIN IMMEDIATE", "ROLLBACK"
			if driver == "mysql" {
				lock, unlock = "LOCK TABLES gateway_job WRITE", "UNLOCK TABLES"
			}
			_, err = conn.ExecContext(context.Background(), lock)
			storetest.Must(t, err)
			var once sync.Once
			released := make(chan error, 1)
			release := func() { once.Do(func() { _, e := conn.ExecContext(context.Background(), unlock); released <- e }) }
			defer release()
			acks := 0
			source := Source{session: &fakeSession{ack: func(string) error { acks++; return nil }}, route: testRoute, botUserID: "UBOT", log: quietLog()}
			var nativeErr error
			var budgetExpired bool
			sink := sinkFunc(func(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
				go func() { <-ctx.Done(); release() }()
				result, err := intake.Accept(ctx, m)
				nativeErr, budgetExpired = err, errors.Is(ctx.Err(), context.DeadlineExceeded)
				return result, err
			})
			err = source.handle(context.Background(), durabilityRequest("A", "locked-event", "locked-envelope", "3.000001"), sink)
			storetest.Must(t, <-released)
			if !budgetExpired {
				t.Fatal("fixture failed to hold the database through the native admission budget")
			}
			fact, exists := durabilityFact(t, db, "locked-event")
			if nativeErr == nil {
				if err != nil || acks != 1 || !exists || fact.request == "" {
					t.Fatal("successful native admission lost its normal durable ACK")
				}
			} else if errors.Is(nativeErr, context.DeadlineExceeded) {
				if err != nil || acks != 0 {
					t.Fatal("native local deadline stopped the source or acknowledged failure")
				}
			} else if !errors.Is(err, gateway.ErrIntakePersistence) || acks != 0 {
				t.Fatal("an opaque driver failure was broadened into the deadline exception")
			}
			t.Logf("native %s lock: deadline=%v canceled=%v tx_done=%v type=%T source_continues=%v durable_job=%v", driver, errors.Is(nativeErr, context.DeadlineExceeded), errors.Is(nativeErr, context.Canceled), errors.Is(nativeErr, sql.ErrTxDone), nativeErr, err == nil, exists)
			// A later transport delivery always uses ordinary native intake.
			retry := durabilityRequest("A", "locked-event", "lock-retry-envelope", "3.000001")
			retry.RetryAttempt = 1
			storetest.Must(t, source.handle(context.Background(), retry, intake))
			after, exists := durabilityFact(t, db, "locked-event")
			if !exists || after.request == "" || fact.id != 0 && after != fact {
				t.Fatal("native retry did not preserve a single durable identity")
			}
			var count int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE slack_event_id=?", "locked-event").Scan(&count))
			if count != 1 {
				t.Fatal("database-lock retry duplicated admission")
			}
		})
	}
}

type durabilityDelayedSource struct {
	source           *Source
	ready            chan struct{}
	entered, expired chan struct{}
}

func (s *durabilityDelayedSource) Run(ctx context.Context, sink gateway.EventSink, ready func()) error {
	delayed := false
	return s.source.Run(ctx, sinkFunc(func(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
		if s.entered != nil && !delayed && m.EventID == "group-slow" {
			delayed = true
			close(s.entered)
			<-ctx.Done()
			result, err := sink.Accept(ctx, m)
			close(s.expired)
			return result, err
		}
		return sink.Accept(ctx, m)
	}), func() { ready(); close(s.ready) })
}

type durabilityBackend struct {
	conversations atomic.Int32
	queries       atomic.Int32
}

func (b *durabilityBackend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return b, nil
}
func (b *durabilityBackend) CreateConversation(context.Context, string) (string, error) {
	return fmt.Sprintf("durability-conversation-%d", b.conversations.Add(1)), nil
}
func (b *durabilityBackend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: "reply " + q.Text}, nil
}
func (*durabilityBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, errors.New("unexpected durability-test backend observation")
}

type durabilitySender struct {
	sent chan gateway.SlackOutgoingMessage
}

func (s durabilitySender) Send(_ context.Context, out gateway.SlackOutgoingMessage) (string, error) {
	s.sent <- out
	return fmt.Sprintf("200.%06d", out.ID), nil
}
func (durabilitySender) SendStatus(_ context.Context, status gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	if status.Target == gateway.StatusTargetAgentSession {
		return gateway.StatusReceipt{Acknowledged: true, AgentStatus: status.SessionState, SessionStatus: status.SessionState}, nil
	}
	return gateway.StatusReceipt{SlackTS: "200.000001"}, nil
}

type durabilityDeadline struct {
	Message      string `json:"msg"`
	EventID      string `json:"event_id"`
	EnvelopeID   string `json:"envelope_id"`
	RetryAttempt int    `json:"retry_attempt"`
	DurationMS   int64  `json:"duration_ms"`
	Reason       string `json:"reason"`
	ACKState     string `json:"ack_state"`
}

// The simulated peer observes the same structured timeout that an operator can
// inspect. It never derives a retry from elapsed test time or a database row.
type durabilitySourceLog struct {
	mu       sync.Mutex
	records  []durabilityDeadline
	timeouts chan durabilityDeadline
	dropped  int
}

func (l *durabilitySourceLog) Write(p []byte) (int, error) {
	var record durabilityDeadline
	if err := json.Unmarshal(p, &record); err != nil {
		return 0, err
	}
	if record.Message != "slack_intake_timeout" {
		return len(p), nil
	}
	l.mu.Lock()
	l.records = append(l.records, record)
	l.mu.Unlock()
	select {
	case l.timeouts <- record:
	default:
		l.mu.Lock()
		l.dropped++
		l.mu.Unlock()
	}
	return len(p), nil
}

func (l *durabilitySourceLog) snapshot() (string, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	encoded, _ := json.Marshal(l.records)
	return string(encoded), l.dropped
}

func TestGroupSourceDeadlineKeepsBothIntegrationsRunning(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			root, db := intakeDurabilityFixture(t, driver)
			a, b := storetest.DefaultIntegration(), storetest.DefaultIntegration()
			a.Name, b.Name, b.AppID = "first", "second", "B"
			identities, err := root.ReconcileIntegrations(context.Background(), []storage.Integration{a, b})
			storetest.Must(t, err)
			group := runner.Group{Store: root, Log: quietLog()}
			streams := []chan socketmode.Event{make(chan socketmode.Event, 8), make(chan socketmode.Event, 8)}
			acks := []chan string{make(chan string, 8), make(chan string, 8)}
			sent := make(chan gateway.SlackOutgoingMessage, 8)
			backends := []*durabilityBackend{{}, {}}
			sources := make([]*durabilityDelayedSource, 2)
			sourceLog := &durabilitySourceLog{timeouts: make(chan durabilityDeadline, 32)}
			logger := slog.New(slog.NewJSONHandler(sourceLog, &slog.HandlerOptions{Level: slog.LevelWarn}))
			for i, identity := range identities {
				route := storetest.Route(identity.AppID, "T", "C")
				ack := acks[i]
				source := &Source{session: &fakeSession{stream: streams[i], ack: func(envelope string) error { ack <- envelope; return nil }}, route: route, botUserID: "UBOT", log: logger}
				sources[i] = &durabilityDelayedSource{source: source, ready: make(chan struct{})}
				if i == 0 {
					sources[i].entered, sources[i].expired = make(chan struct{}), make(chan struct{})
				}
				group.Integrations = append(group.Integrations, runner.Integration{ID: identity.Name, Runtime: &runner.Runtime{
					Store: root.Scope(identity.ID), Source: sources[i], Route: route,
					Policy: storetest.Policy(identity.AppID, "T", "shared", "C"), Clients: backends[i],
					Sender: durabilitySender{sent: sent}, AgentID: "steward", Log: quietLog(), StatusMode: gateway.StatusAgentSession,
				}})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			stopped := make(chan struct{})
			var runErr error
			go func() { runErr = group.Run(ctx); close(stopped) }()
			defer func() {
				cancel()
				select {
				case <-stopped:
					if runErr != nil {
						t.Errorf("group failed: %v", runErr)
					}
				case <-time.After(20 * time.Second):
					t.Error("group did not join its native workers")
				}
				records, dropped := sourceLog.snapshot()
				t.Logf("observed Source deadlines after join: %s", records)
				if dropped != 0 {
					t.Errorf("simulated peer lost %d timeout observations", dropped)
				}
			}()
			wait := func(signal <-chan struct{}) {
				t.Helper()
				select {
				case <-signal:
				case <-stopped:
					t.Fatalf("group stopped during intake: %v", runErr)
				case <-ctx.Done():
					t.Fatal("group fixture timed out")
				}
			}
			push := func(index int, req socketmode.Request) {
				streams[index] <- socketmode.Event{Type: socketmode.EventTypeEventsAPI, Request: &req}
			}
			for i, app := range []string{"A", "B"} {
				streams[i] <- socketmode.Event{Type: socketmode.EventTypeHello, Request: &socketmode.Request{ConnectionInfo: socketmode.ConnectionInfo{AppID: app}}}
				wait(sources[i].ready)
			}
			push(0, durabilityRequest("A", "group-slow", "slow-first-envelope", "10.000001"))
			wait(sources[0].entered)
			other := durabilityRequest("B", "group-other", "other-envelope", "20.000001")
			push(1, other)
			wait(sources[0].expired)
			select {
			case envelope := <-acks[0]:
				t.Fatalf("expired admission was acknowledged: %s", envelope)
			default:
			}
			if _, exists := durabilityFact(t, db, "group-slow"); exists {
				t.Fatal("expired group admission persisted work")
			}
			next := durabilityRequest("A", "group-next", "next-envelope", "11.000001")
			push(0, next)
			retry := durabilityRequest("A", "group-slow", "slow-retry-envelope", "10.000001")
			retry.RetryAttempt = 1
			push(0, retry)
			type pendingEvent struct {
				source  int
				request socketmode.Request
			}
			pending := map[string]pendingEvent{
				"group-other": {source: 1, request: other},
				"group-next":  {source: 0, request: next},
				"group-slow":  {source: 0, request: retry},
			}
			envelopeEvent := map[string]string{
				other.EnvelopeID: "group-other", next.EnvelopeID: "group-next", retry.EnvelopeID: "group-slow",
			}
			gotACK := make(map[string]bool)
			acceptACK := func(envelope string) {
				t.Helper()
				event := envelopeEvent[envelope]
				if event == "" {
					t.Fatalf("unexpected ACK, including the explicitly failed first attempt: %s", envelope)
				}
				gotACK[event] = true
			}
			for len(gotACK) < len(pending) {
				select {
				case envelope := <-acks[0]:
					acceptACK(envelope)
				case envelope := <-acks[1]:
					acceptACK(envelope)
				case failure := <-sourceLog.timeouts:
					if failure.Reason != "accept_budget_exceeded" || failure.ACKState != "not_sent" {
						t.Fatal("Source timeout lost its explicit unacknowledged classification")
					}
					attempt, known := pending[failure.EventID]
					if !known {
						t.Fatalf("Source reported an unknown fixture event: %s", failure.EventID)
					}
					if gotACK[failure.EventID] || failure.EnvelopeID != attempt.request.EnvelopeID {
						continue // An older attempt's log or ACK can arrive later.
					}
					if attempt.request.RetryAttempt >= 5 {
						t.Fatalf("five observed deadline retries did not admit event %s", failure.EventID)
					}
					// This test peer explicitly simulates Slack redelivery only
					// after observing a failed, unacknowledged attempt. It does not
					// claim a production Slack retry schedule or guarantee.
					attempt.request.RetryAttempt++
					attempt.request.EnvelopeID = fmt.Sprintf("%s-retry-%d", failure.EventID, attempt.request.RetryAttempt)
					pending[failure.EventID] = attempt
					envelopeEvent[attempt.request.EnvelopeID] = failure.EventID
					t.Logf("simulated peer redelivers observed timeout: event=%s envelope=%s retry=%d", failure.EventID, attempt.request.EnvelopeID, attempt.request.RetryAttempt)
					push(attempt.source, attempt.request)
				case <-stopped:
					t.Fatalf("one timeout stopped the shared group: %v", runErr)
				case <-ctx.Done():
					t.Fatal("missing native ACKs after the timeout")
				}
			}
			if !gotACK["group-other"] || !gotACK["group-next"] || !gotACK["group-slow"] {
				t.Fatalf("group did not acknowledge every fixture event: %v", gotACK)
			}
			for range 3 {
				select {
				case <-sent:
				case <-stopped:
					t.Fatalf("group stopped before its replies: %v", runErr)
				case <-ctx.Done():
					t.Fatal("one timeout prevented a real reply")
				}
			}
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for {
				var jobs, delivered int
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&jobs))
				storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message WHERE slack_delivery_state='delivered'").Scan(&delivered))
				if jobs == 3 && delivered == 3 {
					break
				}
				select {
				case <-ticker.C:
				case <-stopped:
					t.Fatalf("group stopped before durable delivery: %v", runErr)
				case <-ctx.Done():
					t.Fatalf("native deliveries did not settle: jobs=%d delivered=%d", jobs, delivered)
				}
			}
			if backends[0].queries.Load() != 2 || backends[1].queries.Load() != 1 {
				t.Fatal("deadline retry duplicated a backend submission or blocked its sibling")
			}
		})
	}
}
