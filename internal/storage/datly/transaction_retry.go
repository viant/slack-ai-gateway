package datly

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xdatly/handler"
	modernsqlite "modernc.org/sqlite"
)

// workflowAttempt contains evidence from one completed invocation of our fixed,
// synchronous, database-only Workflow. It is not a retry policy for arbitrary
// Datly components or for the gateway's external calls.
type workflowAttempt struct {
	value       any
	outcome     handler.Outcome
	err         error
	completions int
}

func captureWorkflowInput(in *workflow.Input) (*workflow.Input, error) {
	if in == nil {
		return nil, fmt.Errorf("storage workflow input is required")
	}
	value, err := (xshape.Runtime{}).CloneValue(in)
	if err != nil {
		return nil, fmt.Errorf("capture storage workflow input: %w", err)
	}
	return value.(*workflow.Input), nil
}

func canRetryWorkflow(ctx context.Context, a workflowAttempt) bool {
	if ctx.Err() != nil || a.completions != 1 || len(a.outcome.Transactions) != 1 ||
		a.outcome.Transactions[0].State != handler.TransactionRolledBack || !sameWorkflowError(a.err, a.outcome.Error) {
		return false
	}
	if !pureWorkflowContention(a.outcome.Error) {
		return false
	}
	unitErr := a.outcome.Transactions[0].Error
	return unitErr == nil || pureWorkflowContention(unitErr)
}

// Outcome.Error precedes finalizers. A later error, even another occurrence of
// the same driver error, cannot authorize replay. Unknown wrappers decline retry
// rather than inferring safety from errors.Is, which also matches joined errors.
func sameWorkflowError(returned, canonical error) bool {
	return returned != nil && canonical != nil &&
		reflect.ValueOf(returned).Comparable() && reflect.ValueOf(canonical).Comparable() && returned == canonical
}

func pureWorkflowContention(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *engine.FinalizationError, *dexec.PanicError:
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		parts := joined.Unwrap()
		if len(parts) == 0 {
			return false
		}
		for _, part := range parts {
			if !pureWorkflowContention(part) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return pureWorkflowContention(wrapped.Unwrap())
	}
	switch native := err.(type) {
	case *mysql.MySQLError:
		return native != nil && (native.Number == 1213 || native.Number == 1205)
	case *modernsqlite.Error:
		return native != nil && (native.Code()&0xff == 5 || native.Code()&0xff == 6)
	default:
		return false
	}
}

func waitWorkflowRetry(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
