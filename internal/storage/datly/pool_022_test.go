package datly_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	dr "github.com/viant/slack-ai-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/slack-ai-gateway/internal/datly/deliveries/write"
	opc "github.com/viant/slack-ai-gateway/internal/datly/operations/check"
	opn "github.com/viant/slack-ai-gateway/internal/datly/operations/notbefore"
	ops "github.com/viant/slack-ai-gateway/internal/datly/operations/status"
	opw "github.com/viant/slack-ai-gateway/internal/datly/operations/work"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	er "github.com/viant/slack-ai-gateway/internal/datly/read/event"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	tgw "github.com/viant/slack-ai-gateway/internal/datly/topology/integrations/write"
	tr "github.com/viant/slack-ai-gateway/internal/datly/topology/read"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	mysqlproduct "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/sqlx/metadata/registry"
	"github.com/viant/xdatly"
	xconnector "github.com/viant/xdatly/connector"
	"github.com/viant/xdatly/handler"
)

type pool022ProbeInput struct{}
type pool022ProbeOutput struct {
	Stats sql.DBStats
	db    *sql.DB
}
type pool022Probe struct{}

func (p *pool022Probe) Exec(ctx context.Context, session handler.Session, _ *pool022ProbeInput, out *pool022ProbeOutput) error {
	deps := struct {
		Connector xconnector.Provider `bind:"kind=connector,required"`
	}{}
	if err := session.Binder().Bind(ctx, &deps); err != nil {
		return err
	}
	db, err := deps.Connector.Connector(ctx, "slack")
	if err != nil {
		return err
	}
	out.db = db
	out.Stats = db.Stats()
	return nil
}

// The diagnostic capability belongs to this fixture only. The production
// standalone boundary deliberately does not expose a database handle.
type pool022ProbeComponent struct {
	Contract xdatly.Component[pool022ProbeInput, pool022ProbeOutput] `component:"Pool022Probe,path=/internal/test/pool022,method=GET,handler=NewProbe,connector=slack,internal=true"`
}

func (c pool022ProbeComponent) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name != "NewProbe" {
		return nil
	}
	return custom.Factory(func() handler.Contract[pool022ProbeInput, pool022ProbeOutput] { return &pool022Probe{} })
}

type pool022Gate struct {
	armed                       atomic.Bool
	roots, allocators           atomic.Int32
	discovery, faulted          atomic.Bool
	rootReady, allocatorReady   chan struct{}
	discoveryReady              chan struct{}
	releaseRoots, releaseIDs    chan struct{}
	releaseDiscovery            chan struct{}
	rootOnce, idOnce, queryOnce sync.Once
}

func newPool022Gate() *pool022Gate {
	return &pool022Gate{rootReady: make(chan struct{}, 4), allocatorReady: make(chan struct{}, 4), discoveryReady: make(chan struct{}, 1),
		releaseRoots: make(chan struct{}), releaseIDs: make(chan struct{}), releaseDiscovery: make(chan struct{})}
}
func (g *pool022Gate) release() {
	g.rootOnce.Do(func() { close(g.releaseRoots) })
	g.idOnce.Do(func() { close(g.releaseIDs) })
	g.queryOnce.Do(func() { close(g.releaseDiscovery) })
}
func pool022Wait(ctx context.Context, release <-chan struct{}) error {
	select {
	case <-release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type pool022Driver struct{ gate *pool022Gate }

func (d pool022Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := (&mysqldriver.MySQLDriver{}).Open(dsn)
	if err != nil {
		return nil, err
	}
	return &pool022Conn{Conn: conn, gate: d.gate}, nil
}

type pool022Conn struct {
	driver.Conn
	gate *pool022Gate
	inTx atomic.Bool
}

func (c *pool022Conn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	c.inTx.Store(true)
	if c.gate.armed.Load() {
		var release <-chan struct{}
		if options.Isolation == driver.IsolationLevel(sql.LevelReadCommitted) {
			if c.gate.roots.Add(1) <= 4 {
				c.gate.rootReady <- struct{}{}
				release = c.gate.releaseRoots
			}
		} else if c.gate.allocators.Add(1) <= 4 {
			c.gate.allocatorReady <- struct{}{}
			release = c.gate.releaseIDs
		}
		if release != nil {
			if err = pool022Wait(ctx, release); err != nil {
				_ = tx.Rollback()
				c.inTx.Store(false)
				return nil, err
			}
		}
	}
	return pool022Tx{Tx: tx, conn: c}, nil
}

func (c *pool022Conn) before(statement string) error {
	text := strings.ToLower(strings.TrimSpace(strings.NewReplacer("`", "", "\"", "").Replace(statement)))
	if c.gate.armed.Load() && strings.HasPrefix(text, "insert into slack_agent_session_publication") && c.gate.faulted.CompareAndSwap(false, true) {
		return &mysqldriver.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "fixture coded contention"}
	}
	return nil
}
func (c *pool022Conn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return pool022Statement{Stmt: stmt, query: query, conn: c}, nil
}
func (c *pool022Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.Conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return pool022Statement{Stmt: stmt, query: query, conn: c}, nil
}
func (c *pool022Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.before(query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}
func (c *pool022Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := c.beforeQuery(ctx, query); err != nil {
		return nil, err
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *pool022Conn) beforeQuery(ctx context.Context, query string) error {
	text := strings.ToLower(query)
	if c.gate.armed.Load() && !c.inTx.Load() && strings.Contains(text, "gateway_job") && strings.Contains(text, "union") && c.gate.discovery.CompareAndSwap(false, true) {
		c.gate.discoveryReady <- struct{}{}
		if err := pool022Wait(ctx, c.gate.releaseDiscovery); err != nil {
			return err
		}
	}
	return nil
}

type pool022Tx struct {
	driver.Tx
	conn *pool022Conn
}

func (tx pool022Tx) Commit() error   { defer tx.conn.inTx.Store(false); return tx.Tx.Commit() }
func (tx pool022Tx) Rollback() error { defer tx.conn.inTx.Store(false); return tx.Tx.Rollback() }

type pool022Statement struct {
	driver.Stmt
	query string
	conn  *pool022Conn
}

func (s pool022Statement) Exec(args []driver.Value) (driver.Result, error) {
	if err := s.conn.before(s.query); err != nil {
		return nil, err
	}
	return s.Stmt.Exec(args)
}
func (s pool022Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := s.conn.before(s.query); err != nil {
		return nil, err
	}
	return s.Stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}
func (s pool022Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := s.conn.beforeQuery(ctx, s.query); err != nil {
		return nil, err
	}
	return s.Stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
}

func TestNativePool022FourRootsTransientAllocatorsAndDiscovery(t *testing.T) {
	dsn := storetest.MySQLDSN(t) // Creates and drops only a fresh owned saigt_* database.
	gate := newPool022Gate()
	driverType := reflect.TypeFor[pool022Driver]()
	storetest.Must(t, registry.RegisterDriver(*mysqlproduct.MySQL5(), driverType.PkgPath(), driverType.Name()))
	driverName := fmt.Sprintf("gateway_pool_022_%d", retry022DriverNumber.Add(1))
	sql.Register(driverName, pool022Driver{gate: gate})
	server, err := standalone.New(t.Context(), standalone.Options{RequireLinked: true,
		Holders: []any{&workflow.Component{}, &br.ReaderComponent{}, &bw.WriterComponent{}, &dr.ReaderComponent{}, &dw.WriterComponent{}, &jr.ReaderComponent{}, &er.ReaderComponent{}, &jw.WriterComponent{}, &sr.ReaderComponent{}, &sw.WriterComponent{}, &ops.ReaderComponent{}, &opc.ReaderComponent{}, &opn.ReaderComponent{}, &opw.ReaderComponent{}, &tr.ReaderComponent{}, &tgw.WriterComponent{}, &pool022ProbeComponent{}},
		Config: &config.Config{Connector: "slack", Connectors: []connector.Config{{Name: "slack", Driver: driverName, DSN: dsn, MaxOpenConns: 10, MaxIdleConns: 4}},
			GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/slack-ai-gateway/internal/datly/...", "github.com/viant/slack-ai-gateway/internal/storage/datly/workflow", reflect.TypeFor[pool022ProbeComponent]().PkgPath()}}}})
	storetest.Must(t, err)
	t.Cleanup(func() { storetest.Must(t, server.Shutdown(context.Background())) })
	t.Cleanup(gate.release)
	storetest.Must(t, server.Reload(t.Context(), 1))
	workflowTarget := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[workflow.Component]().PkgPath(), Name: "Workflow"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/slack/storage"}}
	probeTarget := dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[pool022ProbeComponent]().PkgPath(), Name: "Pool022Probe"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/test/pool022"}}
	value, err := server.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: workflowTarget, Input: &workflow.Input{Action: "reconcile_integrations", Integrations: []storage.Integration{storetest.DefaultIntegration()}}})
	storetest.Must(t, err)
	id := value.(*workflow.Output).Integrations[0].ID
	value, err = server.InvokeComponent(t.Context(), dexec.ComponentRequest{Target: probeTarget, Input: &pool022ProbeInput{}})
	storetest.Must(t, err)
	db := value.(*pool022ProbeOutput).db
	if db == nil || db.Stats().MaxOpenConnections != 10 {
		t.Fatal("probe did not observe the actual configured native pool")
	}
	initial := db.Stats()
	gate.armed.Store(true)
	type result struct {
		committed bool
		err       error
	}
	results := make(chan result, 4)
	for i := range 4 {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			m := storetest.Message(fmt.Sprintf("pool-event-%d", i), fmt.Sprintf("pool-root-%d", i), "alice")
			m.StatusMode = gateway.StatusHybrid
			key := m.PhysicalThread()
			committed := false
			_, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: workflowTarget, Input: &workflow.Input{Action: "enqueue", Scope: id, PhysicalThread: &key, Message: m, GatewayRequestID: fmt.Sprintf("pool-request-%d", i)}, Completion: func(outcome handler.Outcome) { committed = outcome.CommitConfirmed() }})
			results <- result{committed, err}
		}()
	}
	wait := func(ch <-chan struct{}, count int) {
		t.Helper()
		for range count {
			select {
			case <-ch:
			case <-time.After(2 * time.Second):
				t.Fatal("native concurrency barrier did not become ready")
			}
		}
	}
	wait(gate.rootReady, 4)
	if stats := db.Stats(); stats.InUse != 4 {
		t.Fatalf("four actual native roots not held: %+v", stats)
	}
	gate.rootOnce.Do(func() { close(gate.releaseRoots) })
	wait(gate.allocatorReady, 4)
	if stats := db.Stats(); stats.InUse != 8 {
		t.Fatalf("four root and four transient transactions not held: %+v", stats)
	}
	discoveryDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		now := time.Now().UTC().Truncate(time.Millisecond)
		input := &opw.Input{}
		input.SetMode("page")
		input.SetNow(now)
		input.SetCutoff(now)
		_, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[opw.ReaderComponent]().PkgPath(), Name: "reader"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/gateway/operations/work"}}, Input: input})
		discoveryDone <- err
	}()
	wait(gate.discoveryReady, 1)
	occupied := db.Stats()
	if occupied.InUse != 9 || occupied.MaxOpenConnections != 10 || occupied.WaitCount != initial.WaitCount {
		t.Fatalf("native pool budget exceeded: initial=%+v occupied=%+v", initial, occupied)
	}
	gate.release()
	storetest.Must(t, <-discoveryDone)
	for range 4 {
		r := <-results
		storetest.Must(t, r.err)
		if !r.committed {
			t.Fatal("native terminal completion was not confirmed committed")
		}
	}
	var jobs, publications int
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&jobs))
	storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM slack_agent_session_publication").Scan(&publications))
	final := db.Stats()
	if jobs != 4 || publications != 4 || !gate.faulted.Load() || gate.roots.Load() != 5 || final.InUse != 0 || final.WaitCount != initial.WaitCount {
		t.Fatalf("native retry/pool cleanup mismatch: jobs=%d publications=%d fault=%t roots=%d initial=%+v final=%+v", jobs, publications, gate.faulted.Load(), gate.roots.Load(), initial, final)
	}
	t.Logf("actual native pool: roots4 + transient4 + discovery1 = %d in-use; max=%d; wait_delta=%d; terminal roots4 with one confirmed rollback replay", occupied.InUse, occupied.MaxOpenConnections, final.WaitCount-initial.WaitCount)
}
