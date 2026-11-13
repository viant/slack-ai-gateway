package workflow

import (
	"context"
	"fmt"

	dexec "github.com/viant/datly/exec"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/xdatly/handler"
)

type Workflow struct{}

func NewWorkflow() handler.Contract[Input, Output] { return &Workflow{} }
func (Component) DatlyHandler(name string) func() (rh.TypedHandler, error) {
	if name != "NewWorkflow" {
		return nil
	}
	return custom.Factory(NewWorkflow)
}

type execution struct {
	invoke   handler.ComponentInvoker
	scope    int64
	name     string
	identity *storage.Integration
}

func (*Workflow) Exec(ctx context.Context, session handler.Session, in *Input, out *Output) error {
	if in == nil || out == nil || session == nil || session.Binder() == nil {
		return fmt.Errorf("incomplete storage invocation")
	}
	deps := struct {
		Invoker  handler.ComponentInvoker   `bind:"kind=component_invoker,required"`
		Starter  handler.TransactionStarter `bind:"kind=transactionStarter,required"`
		Reporter handler.MutationReporter   `bind:"kind=mutationReporter,required"`
	}{}
	if err := session.Binder().Bind(ctx, &deps); err != nil {
		return err
	}
	out.reporter, out.scope = deps.Reporter, in.Scope
	// MySQL native transient identity allocation uses a spare transaction.
	// Read-committed avoids holding empty-range locks across that allocation;
	// explicit record locks still protect every state transition. SQLite's
	// configured driver retains its IMMEDIATE transaction locking.
	if in.Action != "status" {
		ctx = dexec.WithTransactionIsolation(ctx, dexec.IsolationReadCommitted)
	}
	if err := deps.Starter.Start(ctx); err != nil {
		return err
	}
	x := &execution{invoke: deps.Invoker, scope: in.Scope, name: in.IntegrationName}
	if in.Scope != 0 {
		identity, err := x.readIdentity(ctx, in.Scope)
		if err != nil {
			return err
		}
		x.identity = identity
		out.appID, out.workspaceID = identity.AppID, identity.WorkspaceID
		if !identity.Active {
			switch in.Action {
			case "claim_job":
				return nil
			case "enqueue", "bind_job", "begin_create", "set_conversation", "begin_query", "schedule_poll", "complete", "observe", "review", "reject", "fail_unstarted":
				return gateway.ErrConflict
			}
		}
	} else {
		switch in.Action {
		case "reconcile_integrations", "status", "recover", "resolve_delivery":
		default:
			return fmt.Errorf("an explicit integration scope is required")
		}
	}
	switch in.Action {
	case "reconcile_integrations":
		return x.reconcileIntegrations(ctx, in.Integrations, out)
	case "existing_event":
		return x.existingEvent(ctx, in, out)
	case "enqueue":
		return x.enqueue(ctx, in, out)
	case "fail_unstarted":
		return x.failUnstarted(ctx, in, out)
	case "fail_cleanup":
		return x.failCleanup(ctx, in.Text)
	case "claim_job", "bind_job", "begin_query", "schedule_poll", "complete", "review", "reject":
		return x.jobs(ctx, in, out)
	case "get_binding", "begin_create", "set_conversation":
		return x.bindings(ctx, in, out)
	case "claim_delivery", "delivery_context", "delivered", "delivery_unknown", "retry_delivery", "fail_delivery", "resolve_delivery":
		return x.deliveries(ctx, in, out)
	case "claim_status":
		return x.claimStatus(ctx, in, out)
	case "finish_status":
		return x.finishStatus(ctx, in, out)
	case "observe":
		return x.recordObservation(ctx, in, out)
	case "status_context":
		return x.getStatusContext(ctx, in, out)
	case "recover":
		return x.recover(ctx, in, out)
	case "status", "check_bindings", "not_before":
		return x.operations(ctx, in, out)
	default:
		return fmt.Errorf("unknown storage operation")
	}
}

// FinalizeOutcome observes native execution only after the root has completed.
// In particular, an empty claim and a rolled-back child write cannot wake work.
func (*Workflow) FinalizeOutcome(_ context.Context, _ rh.Invocation, result any, outcome handler.Outcome) error {
	out, ok := result.(*Output)
	if !ok || out == nil || out.reporter == nil {
		return nil
	}
	report := out.reporter.MutationReport()
	out.reporter = nil
	if !outcome.CommitConfirmed() {
		return nil
	}
	for _, item := range report.Results {
		if item.Error == nil && item.Affected > 0 {
			out.changed = true
			break
		}
	}
	return nil
}
