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
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	read "github.com/viant/slack-ai-gateway/internal/datly/read"
	integrationwrite "github.com/viant/slack-ai-gateway/internal/datly/topology/integrations/write"
	write "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/storetest"
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
	GatewayJob    *write.GatewayJob
	Integration   *integrationwrite.Integration
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
	case "integration":
		mutation := &integrationwrite.Input{}
		mutation.SetRows([]*integrationwrite.Integration{input.Integration})
		_, err := deps.Invoker.Invoke(ctx, handler.ComponentRequest{Target: handler.ComponentTarget{Scope: reflect.TypeFor[integrationwrite.WriterComponent]().PkgPath(), Name: "writer", Method: "PATCH", Path: "/internal/gateway/topology/integrations"}, Input: mutation})
		if err != nil {
			return err
		}
		output.ID = input.Integration.Id
	case "write":
		mutation := &write.Input{}
		mutation.SetGatewayJobs([]*write.GatewayJob{input.GatewayJob})
		if input.ExpectedState != "" {
			mutation.SetExpectedState(input.ExpectedState)
		}
		_, err := deps.Invoker.Invoke(ctx, handler.ComponentRequest{Target: handler.ComponentTarget{Scope: reflect.TypeFor[write.WriterComponent]().PkgPath(), Name: "writer", Method: "PATCH", Path: "/internal/gateway/jobs"}, Input: mutation})
		if err != nil {
			return err
		}
		output.ID = input.GatewayJob.Id
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
		if len(rows) == 0 || rows[0].GatewayExecutionState != "pending" {
			return nil
		}
		row := &write.GatewayJob{}
		row.SetId(rows[0].Id)
		row.SetGatewayExecutionState("processing")
		row.SetGatewayExecutionClaimCount(rows[0].GatewayExecutionClaimCount + 1)
		mutation := &write.Input{}
		mutation.SetGatewayJobs([]*write.GatewayJob{row})
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
	prefix := fmt.Sprintf("pilot-%x", time.Now().UnixNano()&0xffffffffff)
	seedPilotTopology(t, db)

	server, err := standalone.New(ctx, standalone.Options{Config: &config.Config{BaseDir: t.TempDir(), Connector: "slack", Connectors: []connector.Config{{Name: "slack", Driver: "mysql", DSN: dsn, MaxOpenConns: 8, MaxIdleConns: 4}}, GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/slack-ai-gateway/internal/datly/...", "github.com/viant/slack-ai-gateway/internal/storage/datly/pilot"}}}, RequireLinked: true, Holders: []any{&read.ReaderComponent{}, &write.WriterComponent{}, &PilotComponent{}}})
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
	newJob := func(suffix string) *write.GatewayJob {
		now := time.Now().UTC().Truncate(time.Millisecond)
		return &write.GatewayJob{IntegrationId: 1, SlackChannelId: "Cpilot", SlackThreadRootMessageId: "1.000001", SlackEventId: prefix + suffix, SlackPromptAuthorId: "Upilot", SlackPromptMessageId: "1.000001", PromptText: storetest.Pointer("native graph pilot"), GatewayRequestId: prefix + suffix, GatewayExecutionState: "pending", CreatedAt: now, GatewayExecutionUpdatedAt: now, SlackConversationType: "channel", SlackProgressState: "ready", SlackProgressMode: "message", GatewayStatusUpdatedAt: now, SlackOutgoingMessages: []*write.SlackOutgoingMessage{{SlackMessageIndex: 0, SlackMessageText: storetest.Pointer("first"), SlackMessageKind: "response", SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now, SlackMessageFormat: "plain"}, {SlackMessageIndex: 1, SlackMessageText: storetest.Pointer("second"), SlackMessageKind: "response", SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now, SlackMessageFormat: "plain"}}}
	}
	row := newJob("-commit")
	created, err := invoke(&PilotInput{Action: "write", GatewayJob: row})
	must(t, err)
	if created.ID <= 0 || row.Id != created.ID || row.SlackOutgoingMessages[0].JobId != row.Id || row.SlackOutgoingMessages[0].Id <= 0 || row.SlackOutgoingMessages[1].Id <= 0 {
		t.Fatalf("native identity/link propagation failed: job=%+v", row)
	}
	var count int
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM slack_outgoing_message WHERE job_id=?", row.Id).Scan(&count))
	if count != 2 {
		t.Fatalf("outbox count=%d", count)
	}
	accepted := &write.GatewayJob{}
	accepted.SetId(row.Id)
	accepted.SetGatewayExecutionReason("guard accepted")
	_, err = invoke(&PilotInput{Action: "write", GatewayJob: accepted, ExpectedState: "pending"})
	must(t, err)
	var acceptedReason string
	must(t, db.QueryRowContext(ctx, "SELECT gateway_execution_reason FROM gateway_job WHERE id=?", row.Id).Scan(&acceptedReason))
	if acceptedReason != "guard accepted" {
		t.Fatalf("native matching condition did not write: %q", acceptedReason)
	}
	guarded := &write.GatewayJob{}
	guarded.SetId(row.Id)
	guarded.SetGatewayExecutionState("rejected")
	_, err = invoke(&PilotInput{Action: "write", GatewayJob: guarded, ExpectedState: "needs_review"})
	if err == nil {
		t.Fatal("conditional update with wrong expected state succeeded")
	}
	var unchanged string
	must(t, db.QueryRowContext(ctx, "SELECT gateway_execution_state FROM gateway_job WHERE id=?", row.Id).Scan(&unchanged))
	if unchanged != "pending" {
		t.Fatalf("guarded state=%s", unchanged)
	}
	rolled := newJob("-rollback")
	_, err = invoke(&PilotInput{Action: "write", GatewayJob: rolled, Fail: true})
	if err == nil {
		t.Fatal("rollback probe unexpectedly succeeded")
	}
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM gateway_job WHERE gateway_request_id=?", rolled.GatewayRequestId).Scan(&count))
	if count != 0 {
		t.Fatal("root persisted after rollback")
	}
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM slack_outgoing_message WHERE job_id=?", rolled.Id).Scan(&count))
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
	must(t, db.QueryRowContext(ctx, "SELECT gateway_execution_claim_count FROM gateway_job WHERE id=?", row.Id).Scan(&attempts))
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
