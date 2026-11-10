package pilot

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	read "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/read"
	write "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/write"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	xdatly "github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
)

var errRollbackProbe = errors.New("rollback probe")

type PilotComponent struct {
	Contract xdatly.Component[PilotInput, PilotOutput] `component:"Pilot,path=/internal/gateway/pilot,method=POST,handler=NewPilot,connector=slack,internal=true" componentCallPolicy:"imperative"`
}
type PilotInput struct {
	Action        string
	Job           *write.Job
	ID            int
	Fail          bool
	ExpectedState string
}
type PilotOutput struct{ ID int }
type PilotHandler struct{}

func NewPilot() handler.Contract[PilotInput, PilotOutput] { return &PilotHandler{} }
func (PilotComponent) DatlyHandler(name string) func() (rh.TypedHandler, error) {
	if name != "NewPilot" {
		return nil
	}
	return custom.Factory(NewPilot)
}
func (*PilotHandler) Exec(ctx context.Context, session handler.Session, input *PilotInput, output *PilotOutput) error {
	deps := struct {
		Invoker handler.ComponentInvoker   `bind:"kind=component_invoker,required"`
		Starter handler.TransactionStarter `bind:"kind=transactionStarter,required"`
	}{}
	if err := session.Binder().Bind(ctx, &deps); err != nil {
		return err
	}
	if err := deps.Starter.Start(ctx); err != nil {
		return err
	}
	switch input.Action {
	case "write":
		mutation := &write.Input{}
		mutation.SetJobs([]*write.Job{input.Job})
		if input.ExpectedState != "" {
			mutation.SetExpectedState(input.ExpectedState)
		}
		_, err := deps.Invoker.Invoke(ctx, handler.ComponentRequest{Target: handler.ComponentTarget{Scope: reflect.TypeFor[write.WriterComponent]().PkgPath(), Name: "writer", Method: "PATCH", Path: "/internal/gateway/jobs"}, Input: mutation})
		if err != nil {
			return err
		}
		output.ID = input.Job.Id
	case "claim":
		query := &read.Input{}
		query.SetID(int64(input.ID))
		query.SetMode("key")
		lockCtx := (dexec.ReaderOptions{ForUpdate: []string{dexec.RootView}}).Context(ctx)
		result, err := deps.Invoker.Invoke(lockCtx, handler.ComponentRequest{Target: handler.ComponentTarget{Scope: reflect.TypeFor[read.ReaderComponent]().PkgPath(), Name: "reader", Method: "GET", Path: "/internal/gateway/jobs"}, Input: query})
		if err != nil {
			return err
		}
		rows := result.(*read.Output).Data
		if len(rows) == 0 || rows[0].State != "pending" {
			return nil
		}
		row := &write.Job{}
		row.SetId(rows[0].Id)
		row.SetState("processing")
		row.SetAttempts(rows[0].Attempts + 1)
		mutation := &write.Input{}
		mutation.SetJobs([]*write.Job{row})
		_, err = deps.Invoker.Invoke(ctx, handler.ComponentRequest{Target: handler.ComponentTarget{Scope: reflect.TypeFor[write.WriterComponent]().PkgPath(), Name: "writer", Method: "PATCH", Path: "/internal/gateway/jobs"}, Input: mutation})
		if err != nil {
			return err
		}
		output.ID = row.Id
	default:
		return fmt.Errorf("unknown pilot action")
	}
	if input.Fail {
		return errRollbackProbe
	}
	return nil
}

func TestNativeMySQLGraphPilot(t *testing.T) {
	dsn := storetest.MySQLDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	must(t, err)
	t.Cleanup(func() { db.Close() })
	prefix := fmt.Sprintf("pilot-%d", time.Now().UnixNano())

	server, err := standalone.New(ctx, standalone.Options{Config: &config.Config{BaseDir: t.TempDir(), Connector: "slack", Connectors: []connector.Config{{Name: "slack", Driver: "mysql", DSN: dsn, MaxOpenConns: 8, MaxIdleConns: 4}}, GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/agently-slack-gateway/internal/storage/datly/graph/...", "github.com/viant/agently-slack-gateway/internal/storage/datly/pilot"}}}, RequireLinked: true, Holders: []any{&read.ReaderComponent{}, &write.WriterComponent{}, &PilotComponent{}}})
	must(t, err)
	t.Cleanup(func() { server.Shutdown(context.Background()) })
	must(t, server.Reload(ctx, 1))
	invoke := func(input *PilotInput) (*PilotOutput, error) {
		value, e := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[PilotComponent]().PkgPath(), Name: "Pilot"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/gateway/pilot"}}, Input: input})
		if e != nil {
			return nil, e
		}
		return value.(*PilotOutput), nil
	}
	newJob := func(suffix string) *write.Job {
		now := int(time.Now().UnixMilli())
		return &write.Job{AppId: "pilot", WorkspaceId: prefix, ChannelId: "Cpilot", RootThreadTs: "1.000001", EventId: prefix + suffix, AuthorId: "Upilot", MessageTs: "1.000001", QueryText: "native graph pilot", RequestId: prefix + suffix, State: "pending", CreatedAt: now, UpdatedAt: now, MessageSurface: "channel", Status: &write.RequestStatus{State: "ready", Mode: "message", UpdatedAt: now}, Outbox: []*write.Outbox{{WorkspaceId: prefix, ChannelId: "Cpilot", RootThreadTs: "1.000001", Part: 0, Content: "first", State: "pending", CreatedAt: now, UpdatedAt: now, Format: "plain"}, {WorkspaceId: prefix, ChannelId: "Cpilot", RootThreadTs: "1.000001", Part: 1, Content: "second", State: "pending", CreatedAt: now, UpdatedAt: now, Format: "plain"}}}
	}
	row := newJob("-commit")
	created, err := invoke(&PilotInput{Action: "write", Job: row})
	must(t, err)
	if created.ID <= 0 || row.Id != created.ID || row.Status.JobId != row.Id || row.Outbox[0].JobId != row.Id || row.Outbox[0].Id <= 0 || row.Outbox[1].Id <= 0 {
		t.Fatalf("native identity/link propagation failed: job=%+v", row)
	}
	var count int
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_messages WHERE job_id=?", row.Id).Scan(&count))
	if count != 2 {
		t.Fatalf("outbox count=%d", count)
	}
	accepted := &write.Job{}
	accepted.SetId(row.Id)
	accepted.SetReason("guard accepted")
	_, err = invoke(&PilotInput{Action: "write", Job: accepted, ExpectedState: "pending"})
	must(t, err)
	var acceptedReason string
	must(t, db.QueryRowContext(ctx, "SELECT reason FROM inbox_jobs WHERE id=?", row.Id).Scan(&acceptedReason))
	if acceptedReason != "guard accepted" {
		t.Fatalf("native matching condition did not write: %q", acceptedReason)
	}
	guarded := &write.Job{}
	guarded.SetId(row.Id)
	guarded.SetState("rejected")
	_, err = invoke(&PilotInput{Action: "write", Job: guarded, ExpectedState: "needs_review"})
	if err == nil {
		t.Fatal("conditional update with wrong expected state succeeded")
	}
	var unchanged string
	must(t, db.QueryRowContext(ctx, "SELECT state FROM inbox_jobs WHERE id=?", row.Id).Scan(&unchanged))
	if unchanged != "pending" {
		t.Fatalf("guarded state=%s", unchanged)
	}
	rolled := newJob("-rollback")
	_, err = invoke(&PilotInput{Action: "write", Job: rolled, Fail: true})
	if err == nil {
		t.Fatal("rollback probe unexpectedly succeeded")
	}
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM inbox_jobs WHERE request_id=?", rolled.RequestId).Scan(&count))
	if count != 0 {
		t.Fatal("root persisted after rollback")
	}
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox_messages WHERE job_id=?", rolled.Id).Scan(&count))
	if count != 0 {
		t.Fatal("outbox persisted after rollback")
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	claims := 0
	errs := []error{}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := invoke(&PilotInput{Action: "claim", ID: row.Id})
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				errs = append(errs, e)
			} else if out.ID != 0 {
				claims++
			}
		}()
	}
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("claims: %v", errs)
	}
	if claims != 1 {
		t.Fatalf("exclusive claims=%d", claims)
	}
	var attempts int
	must(t, db.QueryRowContext(ctx, "SELECT attempts FROM inbox_jobs WHERE id=?", row.Id).Scan(&attempts))
	if attempts != 1 {
		t.Fatalf("attempts=%d", attempts)
	}
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
