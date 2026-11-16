package datly_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	store "github.com/viant/slack-ai-gateway/internal/storage/datly"
	sqlitestore "github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	mysqlproduct "github.com/viant/sqlx/metadata/product/mysql"
	sqliteproduct "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/registry"
	modernsqlite "modernc.org/sqlite"
)

var retry022DriverNumber atomic.Uint64

type retry022Fault struct {
	armed                           atomic.Bool
	publicationFaults               atomic.Int32
	jobInserts                      atomic.Int32
	publicationCalls                atomic.Int32
	roots, activeRoots              atomic.Int32
	dialect, match                  string
	failure                         error
	commitFailureAt, beginFailureAt int32
	beginFailure                    error
	requiredColumn                  string
	onMatch                         func(context.Context, string, int32, bool) error
	onRollback                      func()
	mu                              sync.Mutex
	facts                           []map[string]any
}

func retry022SQL(statement string) string {
	return strings.ToLower(strings.TrimSpace(strings.NewReplacer("`", "", "\"", "").Replace(statement)))
}

func retry022Arguments(statement string, args []driver.NamedValue) map[string]any {
	result := map[string]any{}
	left, right := strings.Index(statement, "("), strings.Index(statement, ")")
	if left < 0 || right < left {
		return result
	}
	columns := strings.Split(statement[left+1:right], ",")
	for i, column := range columns {
		if i >= len(args) {
			break
		}
		value := args[i].Value
		if bytes, ok := value.([]byte); ok {
			value = string(bytes)
		}
		result[strings.TrimSpace(column)] = value
	}
	return result
}

func (f *retry022Fault) before(ctx context.Context, statement string, args []driver.NamedValue) error {
	if !f.armed.Load() {
		return nil
	}
	text := retry022SQL(statement)
	// The connection wrapper excludes native identity reservations in spare
	// transactions, including allocators that clone the complete row payload.
	// Count and fault actual business rows in the root transaction only.
	if strings.HasPrefix(text, "insert into gateway_job") && strings.Contains(text, "gateway_request_id") {
		f.jobInserts.Add(1)
		arguments := retry022Arguments(text, args)
		facts := map[string]any{}
		for _, key := range []string{"gateway_request_id", "integration_id", "slack_event_id", "slack_channel_id", "slack_thread_root_message_id", "slack_prompt_message_id", "slack_prompt_author_id", "prompt_text", "slack_progress_mode"} {
			facts[key] = arguments[key]
		}
		f.mu.Lock()
		f.facts = append(f.facts, facts)
		f.mu.Unlock()
	}
	match := f.match
	if match == "" {
		match = "insert into slack_agent_session_publication"
	}
	if !strings.HasPrefix(text, match) || f.requiredColumn != "" && !strings.Contains(text, f.requiredColumn) {
		return nil
	}
	if strings.HasPrefix(match, "insert into slack_agent_session_publication") && !strings.Contains(text, "slack_thread_root_message_id") {
		return nil
	}
	call := f.publicationCalls.Add(1)
	fails := f.publicationFaults.Add(-1) >= 0
	if f.onMatch != nil {
		if err := f.onMatch(ctx, text, call, fails); err != nil {
			return err
		}
	}
	if fails {
		if f.failure != nil {
			return f.failure
		}
		return &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "fixture coded contention"}
	}
	return nil
}

func (f *retry022Fault) arm(failures int32) {
	f.jobInserts.Store(0)
	f.publicationCalls.Store(0)
	f.roots.Store(0)
	f.activeRoots.Store(0)
	f.mu.Lock()
	f.facts = nil
	f.mu.Unlock()
	f.publicationFaults.Store(failures)
	f.armed.Store(true)
}

type retry022Driver struct {
	base  driver.Driver
	fault *retry022Fault
}
type retry022MySQLDriver struct{ retry022Driver }

func (d retry022Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.base.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &retry022Conn{Conn: conn, fault: d.fault}, nil
}

type retry022Conn struct {
	driver.Conn
	fault  *retry022Fault
	rootTx atomic.Bool
}

func (c *retry022Conn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	isRoot := c.fault.dialect != "mysql" || options.Isolation == driver.IsolationLevel(sql.LevelReadCommitted)
	root := c.fault.armed.Load() && isRoot
	var number int32
	if root {
		number = c.fault.roots.Add(1)
		if c.fault.beginFailureAt == number {
			if c.fault.beginFailure != nil {
				return nil, c.fault.beginFailure
			}
			return nil, &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "fixture begin failure"}
		}
	}
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.rootTx.Store(isRoot)
	if root {
		c.fault.activeRoots.Add(1)
	}
	return &retry022Tx{Tx: tx, conn: c, fault: c.fault, root: root, number: number}, nil
}

type retry022Tx struct {
	driver.Tx
	conn   *retry022Conn
	fault  *retry022Fault
	root   bool
	number int32
	once   sync.Once
}

func (t *retry022Tx) finish(rollback bool) {
	t.once.Do(func() {
		t.conn.rootTx.Store(false)
		if t.root {
			t.fault.activeRoots.Add(-1)
			if rollback && t.fault.onRollback != nil {
				t.fault.onRollback()
			}
		}
	})
}
func (t *retry022Tx) Commit() error {
	defer t.finish(false)
	err := t.Tx.Commit()
	if err == nil && t.root && t.fault.commitFailureAt == t.number {
		return errors.New("fixture commit result missing after durable commit")
	}
	return err
}
func (t *retry022Tx) Rollback() error { defer t.finish(true); return t.Tx.Rollback() }
func (c *retry022Conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return retry022Statement{Stmt: stmt, query: query, conn: c}, nil
}
func (c *retry022Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if preparer, ok := c.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := preparer.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return retry022Statement{Stmt: stmt, query: query, conn: c}, nil
	}
	return c.Prepare(query)
}
func (c *retry022Conn) before(ctx context.Context, query string, args []driver.NamedValue) error {
	if !c.rootTx.Load() {
		return nil
	}
	return c.fault.before(ctx, query, args)
}
func (c *retry022Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.before(ctx, query, args); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *retry022Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type retry022Statement struct {
	driver.Stmt
	query string
	conn  *retry022Conn
}

func retry022Named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, value := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return out
}
func (s retry022Statement) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.conn.before(context.Background(), s.query, retry022Named(args)); err != nil {
		return nil, err
	}
	return s.Stmt.Exec(args)
}
func (s retry022Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.before(ctx, s.query, args); err != nil {
		return nil, err
	}
	if executor, ok := s.Stmt.(driver.StmtExecContext); ok {
		return executor.ExecContext(ctx, args)
	}
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return s.Stmt.Exec(values)
}
func (s retry022Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if query, ok := s.Stmt.(driver.StmtQueryContext); ok {
		return query.QueryContext(ctx, args)
	}
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return s.Stmt.Query(values)
}

func retry022Open(t *testing.T, dialect string) (*store.Store, *sql.DB, *retry022Fault, int64) {
	t.Helper()
	return retry022OpenState(t, dialect, true)
}

func retry022OpenState(t *testing.T, dialect string, active bool) (*store.Store, *sql.DB, *retry022Fault, int64) {
	t.Helper()
	ctx := context.Background()
	var dsn string
	var native driver.Driver
	fault := &retry022Fault{dialect: dialect}
	name := fmt.Sprintf("gateway_retry_023_%s_%d", dialect, retry022DriverNumber.Add(1))
	if dialect == "mysql" {
		dsn = storetest.MySQLDSN(t)
		native = &mysql.MySQLDriver{}
		typ := reflect.TypeFor[retry022MySQLDriver]()
		storetest.Must(t, registry.RegisterDriver(*mysqlproduct.MySQL5(), typ.PkgPath(), typ.Name()))
		sql.Register(name, retry022MySQLDriver{retry022Driver{base: native, fault: fault}})
	} else {
		path := filepath.Join(t.TempDir(), "retry.sqlite")
		prepared, err := sqlitestore.Open(ctx, path, true)
		storetest.Must(t, err)
		storetest.Must(t, prepared.Migrate(ctx))
		storetest.Must(t, prepared.Close())
		dsn = path + "?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
		native = &modernsqlite.Driver{}
		typ := reflect.TypeFor[retry022Driver]()
		storetest.Must(t, registry.RegisterDriver(*sqliteproduct.SQLite3(), typ.PkgPath(), typ.Name()))
		sql.Register(name, retry022Driver{base: native, fault: fault})
	}
	root, err := store.Open(ctx, connector.Config{Name: "slack", Driver: name, DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 4})
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, root.Close()) })
	db, err := sql.Open(dialect, dsn)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	identities, err := root.ReconcileIntegrations(ctx, []storage.Integration{storetest.DefaultIntegration()})
	retry022Must(t, err)
	if len(identities) != 1 {
		t.Fatal("missing fixture identity")
	}
	if active {
		root.StartThreadOwnership()
	}
	root.TakeWorkHints()
	return root, db, fault, identities[0].ID
}

// The production Store owns replay around its fully completed native Workflow.
// A real driver error occurs after the job INSERT and before publication INSERT.
func TestWorkflow022NativeRetryKeepsIntakeAndPublicationAtomic(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			ctx := context.Background()
			root, db, fault, id := retry022Open(t, dialect)
			scoped := root.Scope(id)
			before := root.WorkRevision(id)
			fault.arm(1)
			message := storetest.Message("retry-022-event", "retry-022-root", "alice")
			message.StatusMode = gateway.StatusHybrid
			inserted, err := scoped.Enqueue(ctx, message)
			retry022Must(t, err)
			if !inserted || fault.jobInserts.Load() != 2 || fault.publicationCalls.Load() != 2 || fault.roots.Load() != 2 {
				t.Fatalf("composed replay not exercised: inserted=%t jobs=%d publications=%d roots=%d", inserted, fault.jobInserts.Load(), fault.publicationCalls.Load(), fault.roots.Load())
			}
			if root.WorkRevision(id) != before+1 {
				t.Fatal("rolled-back attempt emitted a commit notification")
			}
			facts := fault.facts
			if len(facts) != 2 || !reflect.DeepEqual(facts[0], facts[1]) {
				t.Fatalf("request facts changed between attempts: count=%d", len(facts))
			}
			requestID, ok := facts[0]["gateway_request_id"].(string)
			if !ok || len(requestID) != 32 {
				t.Fatal("fixture did not capture stable gateway request ID")
			}
			var jobs, publications, revision int
			var savedID string
			storetest.Must(t, db.QueryRow("SELECT COUNT(*),MAX(gateway_request_id) FROM gateway_job WHERE slack_event_id=?", message.EventID).Scan(&jobs, &savedID))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*),MAX(slack_publication_revision) FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", message.RootTS).Scan(&publications, &revision))
			if jobs != 1 || publications != 1 || revision != 1 || savedID != requestID {
				t.Fatalf("non-atomic intake: jobs=%d publications=%d revision=%d", jobs, publications, revision)
			}
			hints := root.TakeWorkHints()
			if len(hints) != 1 || hints[0].Thread != message.PhysicalThread() {
				t.Fatalf("unexpected committed work hints: %+v", hints)
			}
			inserted, err = scoped.Enqueue(ctx, message)
			storetest.Must(t, err)
			if inserted || root.WorkRevision(id) != before+1 || fault.jobInserts.Load() != 2 || fault.publicationCalls.Load() != 2 || len(root.TakeWorkHints()) != 0 {
				t.Fatal("transport duplicate repeated persistence/publication")
			}
		})
	}
}

func TestWorkflow023RetryExhaustionRollsBackBothAttempts(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			root, db, fault, id := retry022Open(t, dialect)
			before := root.WorkRevision(id)
			fault.arm(2)
			message := storetest.Message("exhausted-event", "exhausted-root", "alice")
			message.StatusMode = gateway.StatusHybrid
			inserted, err := root.Scope(id).Enqueue(context.Background(), message)
			var issue *gateway.StorageError
			if inserted || !errors.As(err, &issue) || issue.TransactionState != "rolled_back" || issue.Attempts != 2 {
				t.Fatalf("exhausted retry result: inserted=%t error=%T", inserted, err)
			}
			if fault.roots.Load() != 2 || fault.jobInserts.Load() != 2 || fault.publicationCalls.Load() != 2 {
				t.Fatalf("retry was not bounded: roots=%d jobs=%d publications=%d", fault.roots.Load(), fault.jobInserts.Load(), fault.publicationCalls.Load())
			}
			var jobs, publications int
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&jobs))
			storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
			if jobs != 0 || publications != 0 || root.WorkRevision(id) != before || len(root.TakeWorkHints()) != 0 {
				t.Fatalf("rolled-back retry left work: jobs=%d publications=%d", jobs, publications)
			}
		})
	}
}

func retry022Must(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	var issue *gateway.StorageError
	if errors.As(err, &issue) {
		var driverError *mysql.MySQLError
		if errors.As(issue.Cause, &driverError) {
			t.Fatalf("fixture native failure: state=%s cause=%T driver_number=%d sqlstate=%s", issue.TransactionState, issue.Cause, driverError.Number, driverError.SQLState)
		}
		t.Fatalf("fixture native failure: state=%s cause=%T", issue.TransactionState, issue.Cause)
	}
	t.Fatalf("fixture native failure: %T", err)
}
