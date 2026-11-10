package workflow

import (
	"context"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	br "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/bindings/read"
	bw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/bindings/write"
	dr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/read"
	dw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/write"
	jr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/read"
	sr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/sessions/read"
	sw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/sessions/write"
	jw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/write"
)

// Recover is invoked once before workers/intake start, with exclusive database
// ownership. External operations are reconciled or quarantined, never replayed.
func (x *execution) recover(ctx context.Context, _ *Input, _ *Output) error {
	now := time.Now()
	ms := int(now.UnixMilli())
	bq := &br.Input{}
	bq.SetState("creating")
	bindings, err := x.readBindings(ctx, bq, true)
	if err != nil {
		return err
	}
	changes := make([]*bw.Binding, 0, len(bindings))
	for _, b := range bindings {
		r := &bw.Binding{}
		r.SetId(b.Id)
		r.SetState("needs_review")
		r.SetUpdatedAt(ms)
		changes = append(changes, r)
	}
	if len(changes) > 0 {
		if err = x.writeBindings(ctx, changes); err != nil {
			return err
		}
	}
	jq := &jr.Input{}
	jq.SetMode("recover")
	jobs, err := x.readJobs(ctx, jq, true)
	if err != nil {
		return err
	}
	updates := make([]*jw.Job, 0, len(jobs))
	for _, j := range jobs {
		r := jobUpdate(j)
		if (j.Binding != nil && j.Binding.State == "needs_review") || (j.TurnId != "" && (j.QueryStartedAt == 0 || j.TurnId != j.RequestId)) {
			r.SetState("needs_review")
			r.SetReason("interrupted_external_operation")
		} else if j.TurnId == j.RequestId && j.QueryStartedAt > 0 {
			r.SetState("querying")
			r.SetPollReady(1)
		} else if j.State == "processing" {
			r.SetState("pending")
		}
		updates = append(updates, r)
	}
	if len(updates) > 0 {
		if err = x.writeJobs(ctx, updates); err != nil {
			return err
		}
	}
	dq := &dr.Input{}
	dq.SetMode("recover")
	deliveries, err := x.readDeliveries(ctx, dq, true)
	if err != nil {
		return err
	}
	dchanges := make([]*dw.Delivery, 0, len(deliveries))
	for _, d := range deliveries {
		r := &dw.Delivery{}
		r.SetId(d.Id)
		r.SetState("delivery_unknown")
		r.SetUpdatedAt(ms)
		dchanges = append(dchanges, r)
	}
	if len(dchanges) > 0 {
		if err = x.writeDeliveries(ctx, dchanges); err != nil {
			return err
		}
	}
	jq = &jr.Input{}
	jq.SetMode("recover_status")
	jobs, err = x.readJobs(ctx, jq, true)
	if err != nil {
		return err
	}
	updates = nil
	for _, j := range jobs {
		if j.Status == nil {
			continue
		}
		s := &jw.RequestStatus{}
		s.SetJobId(j.Id)
		state := "ready"
		if j.Status.SlackTs == "" {
			state = "delivery_unknown"
		}
		s.SetState(state)
		s.SetReason("interrupted_status_operation")
		s.SetNextAttemptAt(0)
		s.SetUpdatedAt(ms)
		r := jobUpdate(j)
		r.SetStatus(s)
		updates = append(updates, r)
	}
	if len(updates) > 0 {
		if err = x.writeJobs(ctx, updates); err != nil {
			return err
		}
	}
	sq := &sr.Input{}
	sq.SetMode("recover")
	sessions, err := x.readSessions(ctx, sq, true)
	if err != nil {
		return err
	}
	schanges := make([]*sw.Publication, 0, len(sessions))
	for _, s := range sessions {
		r := &sw.Publication{}
		r.SetWorkspaceId(s.WorkspaceId)
		r.SetChannelId(s.ChannelId)
		r.SetRootThreadTs(s.RootThreadTs)
		r.SetState("ready")
		r.SetReason("interrupted_status_operation")
		r.SetNextAttemptAt(max(s.NextAttemptAt, s.RateLimitedUntil, int(now.Add(gateway.StatusRetry).UnixMilli())))
		r.SetUpdatedAt(ms)
		schanges = append(schanges, r)
	}
	if len(schanges) > 0 {
		return x.writeSessions(ctx, schanges)
	}
	return nil
}
