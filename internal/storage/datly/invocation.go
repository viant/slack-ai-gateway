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
	var outcome handler.Outcome
	observed := false
	value, err := s.server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: in,
		Completion: func(report handler.Outcome) { outcome = report; observed = true }})
	if err == nil {
		return value, outcome, nil
	}
	if outcome.State() == handler.TransactionRolledBack && (errors.Is(err, gateway.ErrConflict) || errors.Is(err, gateway.ErrBackendUnavailable) || errors.Is(err, gateway.ErrDenied)) {
		return value, outcome, err
	}
	issue := &gateway.StorageError{Operation: in.Action, IntegrationID: s.scope,
		CompletionObserved: observed, Cause: err, WaitMillis: s.queueWait.Milliseconds(), ExecutionMillis: time.Since(started).Milliseconds()}
	if observed {
		issue.TransactionState = string(outcome.State())
	}
	var driver *mysql.MySQLError
	if errors.As(err, &driver) {
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
	if s.thread != nil && (!observed || outcome.State() == handler.TransactionUnknown || outcome.State() == handler.TransactionCommitUnknown || outcome.State() == handler.TransactionRollbackUnknown || outcome.State() == handler.TransactionMixed || outcome.State() == handler.TransactionPartial) {
		s.reconcileThreadHint(*s.thread)
	}
	return value, outcome, issue
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
