package workflow

import (
	"context"
	"time"

	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// Persistence uses UTC millisecond moments on both supported engines.
func timestamp(at time.Time) time.Time {
	return at.UTC().Truncate(time.Millisecond)
}

func timePointer(at time.Time) *time.Time {
	if at.IsZero() {
		return nil
	}
	value := timestamp(at)
	return &value
}

// touchBinding records actual conversation activity, within the caller's native
// transaction. Historical notices use the same physical-bot scope as delivery;
// the original request's binding and publication identity never change.
func (x *execution) touchBinding(ctx context.Context, id int, at time.Time) error {
	if id == 0 {
		return nil
	}
	query := &br.Input{}
	query.SetID(int64(id))
	if x.identity != nil {
		query.SetMode("physical_thread")
	}
	rows, err := x.readBindings(ctx, query, true)
	if err != nil {
		return err
	}
	if len(rows) != 1 || rows[0].Id != id {
		return gateway.ErrConflict
	}
	return x.updateBindingActivity(ctx, rows[0], at)
}

// The row is already locked by the owning workflow's native reader.
func (x *execution) updateBindingActivity(ctx context.Context, binding *br.ConversationBinding, at time.Time) error {
	at = timestamp(at)
	if !at.After(binding.LastActivityAt) {
		return nil
	}
	patch := &bw.ConversationBinding{}
	patch.SetId(binding.Id)
	patch.SetLastActivityAt(at)
	patch.SetUpdatedAt(at)
	return x.writeBindings(ctx, []*bw.ConversationBinding{patch})
}
