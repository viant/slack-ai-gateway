package gateway

import (
	"context"
	"time"
)

// PreparedOutbound owns an immutable claimed snapshot. Preparation performs only
// local storage work; Send performs policy/network work without a thread owner.
type PreparedOutbound struct {
	send func(context.Context) OutboundResult
}

func (p *PreparedOutbound) Send(ctx context.Context) OutboundResult { return p.send(ctx) }

// OutboundResult separates a known network outcome from its durable receipt.
// The bot permit remains held until Commit finishes. Err is reported afterward;
// a Commit error is a storage failure and takes precedence over Err.
type OutboundResult struct {
	NotBefore time.Time
	Err       error
	commit    func(context.Context, Store) error
}

func (r OutboundResult) Commit(ctx context.Context, local Store) error {
	if r.commit == nil {
		return nil
	}
	return r.commit(ctx, local)
}

func commitOutbound(ctx context.Context, store Store, result OutboundResult) error {
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := InThread(saveCtx, store, func(local Store) error { return result.Commit(saveCtx, local) }); err != nil {
		return err
	}
	return result.Err
}
