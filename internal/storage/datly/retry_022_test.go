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
	"sync/atomic"
	"testing"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	store "github.com/viant/slack-ai-gateway/internal/storage/datly"
	sqlitestore "github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	sqliteproduct "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/sqlx/metadata/registry"
	modernsqlite "modernc.org/sqlite"
)

var retry022DriverNumber atomic.Uint64

type retry022Fault struct {
	armed             atomic.Bool
	publicationFaults atomic.Int32
	jobInserts        atomic.Int32
	publicationCalls  atomic.Int32
}

func (f *retry022Fault) before(statement string) error {
	if !f.armed.Load() {
		return nil
	}
	statement = strings.ToLower(strings.TrimSpace(strings.NewReplacer("`", "", "\"", "").Replace(statement)))
	if strings.HasPrefix(statement, "insert into gateway_job") {
		f.jobInserts.Add(1)
	}
	if strings.HasPrefix(statement, "insert into slack_agent_session_publication") {
		f.publicationCalls.Add(1)
		if f.publicationFaults.Add(-1) >= 0 {
			return &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "fixture coded contention"}
		}
	}
	return nil
}

type retry022Driver struct {
	base  driver.Driver
	fault *retry022Fault
}

func (d retry022Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.base.Open(dsn)
	if err != nil {
		return nil, err
	}
	return retry022Conn{Conn: conn, fault: d.fault}, nil
}

type retry022Conn struct {
	driver.Conn
	fault *retry022Fault
}

func (c retry022Conn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
}
func (c retry022Conn) Prepare(query string) (driver.Stmt, error) {
	statement, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return retry022Statement{Stmt: statement, query: query, fault: c.fault}, nil
}
func (c retry022Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.fault.before(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c retry022Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type retry022Statement struct {
	driver.Stmt
	query string
	fault *retry022Fault
}

func (s retry022Statement) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.fault.before(s.query); err != nil {
		return nil, err
	}
	return s.Stmt.Exec(args)
}

// This is the gateway's actual typed Workflow Input (without request markers),
// imperative child graphs, production migration and public Store.Enqueue.
// The error occurs after its job insert but before its publication insert.
func TestWorkflow022NativeRetryKeepsIntakeAndPublicationAtomic(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "retry.sqlite")
	prepared, err := sqlitestore.Open(ctx, path, true)
	storetest.Must(t, err)
	storetest.Must(t, prepared.Migrate(ctx))
	storetest.Must(t, prepared.Close())
	fault := &retry022Fault{}
	driverType := reflect.TypeFor[retry022Driver]()
	storetest.Must(t, registry.RegisterDriver(*sqliteproduct.SQLite3(), driverType.PkgPath(), driverType.Name()))
	name := fmt.Sprintf("gateway_native_retry_022_%d", retry022DriverNumber.Add(1))
	sql.Register(name, retry022Driver{base: &modernsqlite.Driver{}, fault: fault})
	dsn := path + "?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)"
	root, err := store.Open(ctx, connector.Config{Name: "slack", Driver: name, DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 4})
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, root.Close()) })
	db, err := sql.Open("sqlite", dsn)
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, db.Close()) })
	identities, err := root.ReconcileIntegrations(ctx, []storage.Integration{storetest.DefaultIntegration()})
	retry022Must(t, err)
	if len(identities) != 1 {
		t.Fatal("missing fixture identity")
	}
	id := identities[0].ID
	scoped := root.Scope(id)
	before := root.WorkRevision(id)
	fault.publicationFaults.Store(1)
	fault.armed.Store(true)
	message := storetest.Message("retry-022-event", "retry-022-root", "alice")
	message.StatusMode = gateway.StatusHybrid
	inserted, err := scoped.Enqueue(ctx, message)
	retry022Must(t, err)
	if !inserted {
		t.Fatal("retried intake did not commit")
	}
	if fault.jobInserts.Load() != 2 || fault.publicationCalls.Load() != 2 {
		t.Fatalf("did not exercise composed rollback and replay: jobs=%d publications=%d", fault.jobInserts.Load(), fault.publicationCalls.Load())
	}
	if root.WorkRevision(id) != before+1 {
		t.Fatal("rolled-back attempt emitted a commit notification")
	}
	var jobs, publications, revision int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE slack_event_id=?", message.EventID).Scan(&jobs))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*),MAX(slack_publication_revision) FROM slack_agent_session_publication WHERE slack_thread_root_message_id=?", message.RootTS).Scan(&publications, &revision))
	if jobs != 1 || publications != 1 || revision != 1 {
		t.Fatalf("non-atomic or duplicated intake: jobs=%d publications=%d revision=%d", jobs, publications, revision)
	}
	inserted, err = scoped.Enqueue(ctx, message)
	storetest.Must(t, err)
	if inserted || root.WorkRevision(id) != before+1 || fault.jobInserts.Load() != 2 || fault.publicationCalls.Load() != 2 {
		t.Fatal("transport duplicate repeated persistence/publication")
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
