package datly

import (
	"context"
	"errors"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage/datly/workflow"
	"github.com/viant/xdatly/handler"
)

func (s *Store) invokeNative(ctx context.Context, in *workflow.Input) (any, handler.Outcome, error) {
	started := time.Now()
	if s.thread == nil && s.workOwner().active.Load() {
		release, err := s.reserveReadLane(ctx)
		if err != nil {
			return nil, handler.Outcome{}, err
		}
		defer release()
	}
	operation := ""
	if in != nil {
		operation = in.Action
	}
	if err := ctx.Err(); err != nil {
		return s.finishWorkflowAttempt(operation, workflowAttempt{err: err}, 0, started)
	}
	snapshot, err := captureWorkflowInput(in)
	if err != nil {
		return s.finishWorkflowAttempt(operation, workflowAttempt{err: err}, 0, started)
	}
	if err := ctx.Err(); err != nil {
		return s.finishWorkflowAttempt(operation, workflowAttempt{err: err}, 0, started)
	}
	attempt := s.invokeWorkflowAttempt(ctx, in)
	attempts := 1
	if canRetryWorkflow(ctx, attempt) {
		// Retain the same command owner, executor, read lane and caller budget.
		// The native transaction and finalizers have already finished here.
		attempt.value = nil
		if err := waitWorkflowRetry(ctx); err != nil {
			attempt.err = errors.Join(attempt.err, err)
		} else if retryInput, err := captureWorkflowInput(snapshot); err != nil {
			attempt.err = errors.Join(attempt.err, err)
		} else if err := ctx.Err(); err != nil {
			attempt.err = errors.Join(attempt.err, err)
		} else {
			attempts++
			attempt = s.invokeWorkflowAttempt(ctx, retryInput)
		}
	}
	return s.finishWorkflowAttempt(operation, attempt, attempts, started)
}

func (s *Store) invokeWorkflowAttempt(ctx context.Context, in *workflow.Input) (a workflowAttempt) {
	a.value, a.err = s.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in,
		Completion: func(report handler.Outcome) {
			a.outcome = report.Clone()
			a.completions++
		}})
	return a
}

func (s *Store) finishWorkflowAttempt(operation string, a workflowAttempt, attempts int, started time.Time) (any, handler.Outcome, error) {
	if a.err == nil {
		return a.value, a.outcome, nil
	}
	if a.outcome.State() == handler.TransactionRolledBack && (errors.Is(a.err, gateway.ErrConflict) || errors.Is(a.err, gateway.ErrBackendUnavailable) || errors.Is(a.err, gateway.ErrDenied)) {
		return a.value, a.outcome, a.err
	}
	observed := a.completions != 0
	issue := &gateway.StorageError{Operation: operation, IntegrationID: s.scope, Attempts: attempts,
		CompletionObserved: observed, Cause: a.err, WaitMillis: s.queueWait.Milliseconds(), ExecutionMillis: time.Since(started).Milliseconds()}
	if observed {
		issue.TransactionState = string(a.outcome.State())
	}
	var driver *mysql.MySQLError
	if errors.As(a.err, &driver) && driver != nil {
		issue.MySQLCode = driver.Number
		issue.SQLState = string(driver.SQLState[:])
		for _, c := range issue.SQLState {
			if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
				issue.SQLState = ""
				break
			}
		}
	}
	// A targeted read of durable truth is safe even when completion is unknown.
	// It does not authorize replay of any external side effect.
	if attempts != 0 && s.thread != nil && (!observed || a.outcome.State() == handler.TransactionUnknown || a.outcome.State() == handler.TransactionCommitUnknown || a.outcome.State() == handler.TransactionRollbackUnknown || a.outcome.State() == handler.TransactionMixed || a.outcome.State() == handler.TransactionPartial) {
		s.reconcileThreadHint(*s.thread)
	}
	return a.value, a.outcome, issue
}

func (s *Store) invokeReader(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	release, err := s.reserveReadLane(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.workOwner().server.InvokeComponent(ctx, request)
}

func (s *Store) reserveReadLane(ctx context.Context) (func(), error) {
	root := s.workOwner()
	select {
	case root.readLane <- struct{}{}:
		return func() { <-root.readLane }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Store) reconcileThreadHint(key gateway.PhysicalThreadKey) {
	state := s.workOwner().work
	state.mu.Lock()
	state.threads[key]++
	state.pending[key] = gateway.WorkCandidate{IntegrationID: s.scope, Thread: key, Query: gateway.WorkHint{ProbeNow: true}, Outbound: gateway.WorkHint{ProbeNow: true}}
	state.mu.Unlock()
	select {
	case state.changes <- struct{}{}:
	default:
	}
}
