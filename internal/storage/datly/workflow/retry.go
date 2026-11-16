package workflow

import (
	"context"

	rh "github.com/viant/datly/runtime/handler"
)

// Native Datly owns admission, rollback evidence, input capture and the bound.
// This workflow has local database effects only; HTTP is owned by runtime tasks.
func (*Workflow) SupportsTransactionRetry() bool { return true }

func (*Workflow) RetryTransaction(ctx context.Context, _ rh.Invocation, _ any, evidence rh.MutationOutcome) (bool, error) {
	return ctx.Err() == nil && evidence.Mutation.Contention && evidence.Attempt < evidence.RetryLimit, nil
}
