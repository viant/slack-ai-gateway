package datly

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
	"github.com/viant/xdatly/handler"
	modernsqlite "modernc.org/sqlite"
)

type retry023CodeError int

func (e retry023CodeError) Error() string { return "fixture numeric error" }
func (e retry023CodeError) Code() int     { return int(e) }

type retry023SliceError struct {
	values []int
	cause  error
}

func (e retry023SliceError) Error() string { return "fixture noncomparable error" }
func (e retry023SliceError) Unwrap() error { return e.cause }

type retry023InterfaceError struct{ cause error }

func (e retry023InterfaceError) Error() string { return "fixture interface error" }
func (e retry023InterfaceError) Unwrap() error { return e.cause }

type retry023EmptyJoin struct{}

func (retry023EmptyJoin) Error() string   { return "fixture empty error group" }
func (retry023EmptyJoin) Unwrap() []error { return nil }

func retry023Attempt(err error) workflowAttempt {
	return workflowAttempt{err: err, completions: 1, outcome: handler.Outcome{
		Error: err, Transactions: []handler.TransactionOutcome{{Unit: 0, State: handler.TransactionRolledBack}},
	}}
}

func TestWorkflowRetry023RequiresCleanSingleRollbackAndTypedContention(t *testing.T) {
	deadlock := &mysql.MySQLError{Number: 1213}
	lockWait := &mysql.MySQLError{Number: 1205}
	wrapped := fmt.Errorf("fixture operation: %w", deadlock)
	joined := errors.Join(wrapped, fmt.Errorf("fixture nested operation: %w", lockWait))
	finalizer := &engine.FinalizationError{Route: "fixture", Err: deadlock}
	panicErr := new(dexec.PanicError)
	noncomparable := retry023SliceError{values: []int{1}, cause: deadlock}
	interfaceNoncomparable := retry023InterfaceError{cause: noncomparable}
	var typedNil *mysql.MySQLError

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"mysql deadlock", deadlock, true},
		{"mysql lock wait", lockWait, true},
		{"same canonical wrapper", wrapped, true},
		{"all joined branches are typed contention", joined, true},
		{"nil", nil, false},
		{"typed nil", typedNil, false},
		{"noncomparable wrapper around contention", noncomparable, false},
		{"comparable type with noncomparable interface value", interfaceNoncomparable, false},
		{"empty joined error", retry023EmptyJoin{}, false},
		{"only matching message", errors.New("Error 1213: deadlock; SQLITE_BUSY"), false},
		{"arbitrary numeric code", retry023CodeError(5), false},
		{"mysql other error", &mysql.MySQLError{Number: 1062}, false},
		{"mysql sqlstate alone", &mysql.MySQLError{Number: 0, SQLState: [5]byte{'4', '0', '0', '0', '1'}}, false},
		{"business conflict", gateway.ErrConflict, false},
		{"access rejection", gateway.ErrDenied, false},
		{"native validation", &handler.Validation{}, false},
		{"bad connection", driver.ErrBadConn, false},
		{"no rows", sql.ErrNoRows, false},
		{"cancelled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, false},
		{"contention joined with cleanup", errors.Join(deadlock, errors.New("fixture cleanup failure")), false},
		{"contention joined with cancellation", errors.Join(deadlock, context.Canceled), false},
		{"contention joined with constraint", errors.Join(deadlock, &mysql.MySQLError{Number: 1452}), false},
		{"canonical finalizer returning same contention", finalizer, false},
		{"contention joined with finalizer", errors.Join(deadlock, finalizer), false},
		{"panic", panicErr, false},
		{"contention joined with panic", errors.Join(deadlock, panicErr), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := canRetryWorkflow(context.Background(), retry023Attempt(tc.err)); got != tc.want {
				t.Fatalf("retry eligibility = %t, want %t", got, tc.want)
			}
		})
	}

	for _, state := range []handler.TransactionState{
		"", handler.TransactionNone, handler.TransactionCommitted,
		handler.TransactionCallerPending, handler.TransactionCommitUnknown,
		handler.TransactionRollbackUnknown, handler.TransactionUnknown,
		handler.TransactionPartial, handler.TransactionMixed, "future_state",
	} {
		t.Run("state="+string(state), func(t *testing.T) {
			a := retry023Attempt(deadlock)
			a.outcome.Transactions[0].State = state
			if canRetryWorkflow(context.Background(), a) {
				t.Fatal("non-rollback completion authorized retry")
			}
		})
	}

	t.Run("metadata and final error", func(t *testing.T) {
		cases := []struct {
			name   string
			change func(*workflowAttempt)
		}{
			{"no notification", func(a *workflowAttempt) { a.completions = 0 }},
			{"duplicate notification", func(a *workflowAttempt) { a.completions = 2 }},
			{"no unit", func(a *workflowAttempt) { a.outcome.Transactions = nil }},
			{"two rolled-back units", func(a *workflowAttempt) {
				a.outcome.Transactions = append(a.outcome.Transactions, handler.TransactionOutcome{Unit: 1, State: handler.TransactionRolledBack})
			}},
			{"no canonical error", func(a *workflowAttempt) { a.outcome.Error = nil }},
			{"no returned error", func(a *workflowAttempt) { a.err = nil }},
			{"equal driver values are not identity", func(a *workflowAttempt) { a.err = &mysql.MySQLError{Number: 1213} }},
			{"later transparent wrapper", func(a *workflowAttempt) { a.err = fmt.Errorf("fixture later wrapper: %w", deadlock) }},
			{"later contention join", func(a *workflowAttempt) { a.err = errors.Join(deadlock, lockWait) }},
			{"finalizer returns original driver object", func(a *workflowAttempt) { a.err = errors.Join(deadlock, finalizer) }},
			{"unit cleanup error", func(a *workflowAttempt) { a.outcome.Transactions[0].Error = errors.New("fixture cleanup failure") }},
			{"unit mixed error", func(a *workflowAttempt) {
				a.outcome.Transactions[0].Error = errors.Join(deadlock, context.DeadlineExceeded)
			}},
			{"unit finalizer", func(a *workflowAttempt) { a.outcome.Transactions[0].Error = finalizer }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				a := retry023Attempt(deadlock)
				tc.change(&a)
				if canRetryWorkflow(context.Background(), a) {
					t.Fatal("incomplete or additional failure evidence authorized retry")
				}
			})
		}
	})

	t.Run("unit preserves actual operation failure", func(t *testing.T) {
		a := retry023Attempt(deadlock)
		a.outcome.Transactions[0].Error = deadlock
		if !canRetryWorkflow(context.Background(), a) {
			t.Fatal("clean rollback was rejected because its unit retained the operation error")
		}
	})
	t.Run("cancelled caller", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if canRetryWorkflow(ctx, retry023Attempt(deadlock)) {
			t.Fatal("cancelled caller authorized retry")
		}
	})
}

// Obtain concrete modernc driver errors through owned connections. A generic
// Code() method is deliberately insufficient evidence for SQLite contention.
func TestWorkflowRetry023UsesRealSQLiteContentionCodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "codes.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	first, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, statement := range []string{"PRAGMA journal_mode=WAL", "CREATE TABLE fixture(id INTEGER PRIMARY KEY)"} {
		if _, err := first.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	writer, err := first.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback()
	if _, err := writer.ExecContext(ctx, "INSERT INTO fixture VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	_, busy := second.ExecContext(ctx, "INSERT INTO fixture VALUES(2)")
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.ExecContext(ctx, "INSERT INTO fixture VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	reader, err := first.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var id int
	if err := reader.QueryRowContext(ctx, "SELECT id FROM fixture").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := second.ExecContext(ctx, "INSERT INTO fixture VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	_, snapshotBusy := reader.ExecContext(ctx, "INSERT INTO fixture VALUES(3)")
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	rows, err := first.QueryContext(ctx, "SELECT id FROM fixture")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("fixture has no active cursor")
	}
	_, locked := first.ExecContext(ctx, "DROP TABLE fixture")
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	_, constraint := first.ExecContext(ctx, "INSERT INTO fixture VALUES(1)")
	for _, tc := range []struct {
		name    string
		err     error
		primary int
		want    bool
	}{
		{"busy", busy, 5, true}, {"busy snapshot", snapshotBusy, 5, true},
		{"locked", locked, 6, true}, {"constraint", constraint, 19, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var native *modernsqlite.Error
			if !errors.As(tc.err, &native) || native == nil || native.Code()&0xff != tc.primary {
				t.Fatalf("fixture did not produce concrete SQLite code %d", tc.primary)
			}
			if got := canRetryWorkflow(ctx, retry023Attempt(tc.err)); got != tc.want {
				t.Fatalf("SQLite code %d retry eligibility = %t, want %t", native.Code(), got, tc.want)
			}
		})
	}
}

func TestWorkflowRetry023SnapshotRetainsOriginalCommandFacts(t *testing.T) {
	at := time.Date(2026, 11, 16, 22, 0, 0, 123000000, time.UTC)
	key := gateway.PhysicalThreadKey{AppID: "A", WorkspaceID: "T", ChannelID: "D", RootTS: "1.000001"}
	in := &workflow.Input{
		Action: "complete", Scope: 7, IntegrationName: "fixture", PhysicalThread: &key,
		Integrations: []storage.Integration{{ID: 7, Name: "fixture", Active: true, AppID: "A"}},
		Protocol:     []gateway.ProtocolSnapshot{{Messages: json.RawMessage(`[{"text":"original"}]`), State: json.RawMessage(`{"step":1}`)}, {}},
		ID:           19, Message: gateway.Message{Text: "original prompt", AuthorID: "alice"},
		GatewayRequestID: "stable-run", Text: "original result", At: at,
		Parts:               []gateway.ReplyPart{{Text: "original part", Format: gateway.ReplyMarkdown}},
		Status:              gateway.GatewayJobProgress{JobID: 19, Revision: 0, Terminal: false, CreatedAt: at},
		StatusResult:        gateway.StatusResult{Acknowledged: false, RetryAt: at},
		AcceptDuplicateRisk: false, PreferMessage: false,
	}
	snapshot, err := captureWorkflowInput(in)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot == in || !reflect.DeepEqual(snapshot, in) {
		t.Fatal("snapshot changed command values or retained the original pointer")
	}
	in.Action, in.Scope, in.GatewayRequestID, in.Text = "changed", 99, "changed", "changed"
	in.PhysicalThread.RootTS = "changed"
	in.Integrations[0].Name = "changed"
	in.Protocol[0].Messages[0], in.Protocol[0].State[0] = '!', '!'
	in.Parts[0].Text = "changed"
	if snapshot.Action != "complete" || snapshot.Scope != 7 || snapshot.GatewayRequestID != "stable-run" || snapshot.Text != "original result" ||
		snapshot.PhysicalThread.RootTS != "1.000001" || snapshot.Integrations[0].Name != "fixture" ||
		string(snapshot.Protocol[0].Messages) != `[{"text":"original"}]` || string(snapshot.Protocol[0].State) != `{"step":1}` || snapshot.Parts[0].Text != "original part" {
		t.Fatal("first attempt mutated the retry snapshot")
	}
	retry, err := captureWorkflowInput(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(retry, snapshot) || retry.PhysicalThread == snapshot.PhysicalThread ||
		retry.Status.Terminal || retry.Status.Revision != 0 || retry.StatusResult.Acknowledged || retry.AcceptDuplicateRisk || retry.PreferMessage ||
		retry.Protocol[1].Messages != nil || retry.Protocol[1].State != nil || !retry.At.Equal(at) {
		t.Fatal("fresh attempt lost typed zero/nil values, timestamps or command identity")
	}
	retry.PhysicalThread.RootTS = "retry mutation"
	retry.Protocol[0].State[0] = '!'
	retry.Parts[0].Text = "retry mutation"
	if snapshot.PhysicalThread.RootTS != "1.000001" || string(snapshot.Protocol[0].State) != `{"step":1}` || snapshot.Parts[0].Text != "original part" {
		t.Fatal("fresh retry retained mutable references to its snapshot")
	}
	zero, err := captureWorkflowInput(&workflow.Input{})
	if err != nil || !reflect.DeepEqual(zero, &workflow.Input{}) {
		t.Fatal("empty command no longer preserves nil slices/pointers and zero values")
	}
}

func TestWorkflowRetry023SnapshotFailureDoesNotChangeOriginalInput(t *testing.T) {
	// Both JSON values are valid, but one is an interior allocation of the
	// other. Native cloning rejects unsupported alias shapes rather than
	// silently changing their relationship or using an incomplete snapshot.
	backing := json.RawMessage(`[{"a":1},{"b":2}]`)
	in := &workflow.Input{GatewayRequestID: "stable-run", Protocol: []gateway.ProtocolSnapshot{{Messages: backing, State: backing[1:8:8]}}}
	value, err := captureWorkflowInput(in)
	if err == nil || value != nil {
		t.Fatal("unsupported overlapping graph published a retry snapshot")
	}
	if in.GatewayRequestID != "stable-run" || string(in.Protocol[0].Messages) != `[{"a":1},{"b":2}]` || string(in.Protocol[0].State) != `{"a":1}` {
		t.Fatal("failed snapshot changed the original command")
	}
}

func TestWorkflowRetry023PreInvocationFailureHasNoCompletionOrReconciliation(t *testing.T) {
	backing := json.RawMessage(`[{"a":1},{"b":2}]`)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		input *workflow.Input
		cause error
	}{
		{"snapshot failure", context.Background(), &workflow.Input{Action: "complete", Protocol: []gateway.ProtocolSnapshot{{Messages: backing, State: backing[1:8:8]}}}, nil},
		{"pre-cancelled context", cancelled, &workflow.Input{Action: "enqueue", GatewayRequestID: "stable-run"}, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &Store{work: newWorkState()}
			key := ownerKey("pre-invocation")
			// A nil native server makes any accidental invocation fail loudly.
			// The inactive fixture owns only the normal work metadata; no
			// storage, worker or service is started for this boundary check.
			scoped := &Store{root: root, scope: 7, local: true, thread: &key}
			value, outcome, err := scoped.invokeNative(tc.ctx, tc.input)
			var issue *gateway.StorageError
			if !errors.As(err, &issue) || issue == nil || issue.Attempts != 0 || issue.CompletionObserved ||
				issue.IntegrationID != 7 || issue.Operation != tc.input.Action || issue.TransactionState != "" {
				t.Fatal("failure before the native invocation lost its storage diagnostic metadata")
			}
			if tc.cause != nil && !errors.Is(err, tc.cause) {
				t.Fatal("pre-invocation cancellation lost its original cause")
			}
			if value != nil || outcome.Error != nil || len(outcome.Transactions) != 0 {
				t.Fatal("failure before invocation fabricated a result or transaction completion")
			}
			if root.work.global != 0 || len(root.work.revisions) != 0 || len(root.work.threads) != 0 ||
				len(root.work.pending) != 0 || len(root.work.changes) != 0 {
				t.Fatal("failure before invocation queued reconciliation or changed work revisions")
			}
		})
	}
}

func TestWorkflowRetry023BackoffRespectsOriginalContext(t *testing.T) {
	t.Run("one bounded delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			if err := waitWorkflowRetry(context.Background()); err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(started); elapsed != 10*time.Millisecond {
				t.Fatalf("retry delay = %s, want 10ms", elapsed)
			}
		})
	})
	t.Run("cancel during delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			time.AfterFunc(time.Millisecond, cancel)
			started := time.Now()
			if err := waitWorkflowRetry(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled wait returned %v", err)
			}
			if elapsed := time.Since(started); elapsed != time.Millisecond {
				t.Fatalf("cancelled wait lasted %s", elapsed)
			}
		})
	})
	t.Run("deadline during delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			if err := waitWorkflowRetry(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expired original budget returned %v", err)
			}
		})
	})
}
