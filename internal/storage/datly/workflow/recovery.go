package workflow

import (
	"context"
	"time"

	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	dr "github.com/viant/slack-ai-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/slack-ai-gateway/internal/datly/deliveries/write"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// Recover is invoked once before workers/intake start, with exclusive database
// ownership. External operations are reconciled or quarantined, never replayed.
func (x *execution) recover(ctx context.Context, _ *Input, out *Output) error {
	now := timestamp(time.Now())
	for _, state := range []string{"creating", "needs_review"} {
		bq := &br.Input{}
		bq.SetState(state)
		bindings, err := x.readBindings(ctx, bq, true)
		if err != nil {
			return err
		}
		if err := x.closeBindings(ctx, bindings, now); err != nil {
			return err
		}
	}
	jq := &jr.Input{}
	jq.SetMode("recover")
	jobs, err := x.readJobs(ctx, jq, true)
	if err != nil {
		return err
	}
	type closure struct {
		job    *jr.GatewayJob
		reason string
	}
	closures := map[gateway.PhysicalThreadKey]closure{}
	for _, j := range jobs {
		reason := ""
		switch {
		case j.GatewayExecutionState == "needs_review":
			reason = j.GatewayExecutionReason
			if reason == "" {
				reason = "interrupted_external_operation"
			}
		case j.ConversationBinding != nil && j.ConversationBinding.GatewayBindingState == "closed":
			reason = "interrupted_external_operation"
		case j.GatewayExecutionState == "querying" && j.BackendSubmissionStartedAt == nil:
			reason = "interrupted_external_operation"
		}
		if reason != "" {
			key := gateway.PhysicalThreadKey{AppID: j.SlackAppId, WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId}
			if _, exists := closures[key]; !exists {
				closures[key] = closure{j, reason}
			}
		}
	}
	for _, c := range closures {
		if err := x.closeJobThread(ctx, c.job, c.reason, now, out); err != nil {
			return err
		}
	}
	updates := make([]*jw.GatewayJob, 0, len(jobs))
	for _, j := range jobs {
		key := gateway.PhysicalThreadKey{AppID: j.SlackAppId, WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId}
		if _, closed := closures[key]; closed {
			continue
		}
		r := jobUpdate(j)
		if j.BackendSubmissionStartedAt != nil {
			r.SetGatewayExecutionState("querying")
			r.SetBackendPollReady(1)
		} else if j.GatewayExecutionState == "processing" {
			r.SetGatewayExecutionState("pending")
		}
		updates = append(updates, r)
	}
	if len(updates) > 0 {
		if err = x.writeJobs(ctx, updates); err != nil {
			return err
		}
		for _, j := range jobs {
			key := gateway.PhysicalThreadKey{AppID: j.SlackAppId, WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId}
			if _, closed := closures[key]; !closed {
				x.affectJob(j)
			}
		}
	}
	dq := &dr.Input{}
	dq.SetMode("recover")
	deliveries, err := x.readDeliveries(ctx, dq, true)
	if err != nil {
		return err
	}
	dchanges := make([]*dw.SlackOutgoingMessage, 0, len(deliveries))
	for _, d := range deliveries {
		r := &dw.SlackOutgoingMessage{}
		r.SetId(d.Id)
		r.SetSlackDeliveryState("delivery_unknown")
		r.SetUpdatedAt(now)
		dchanges = append(dchanges, r)
	}
	if len(dchanges) > 0 {
		if err = x.writeDeliveries(ctx, dchanges); err != nil {
			return err
		}
		for _, d := range deliveries {
			x.affectProgress(gateway.PhysicalThreadKey{AppID: d.SlackAppId, WorkspaceID: d.SlackWorkspaceId, ChannelID: d.SlackChannelId, RootTS: d.SlackThreadRootMessageId})
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
		r := jobUpdate(j)
		state := "ready"
		if j.SlackProgressMessageId == "" {
			state = "delivery_unknown"
		}
		r.SetSlackProgressState(state)
		r.SetSlackProgressReason("interrupted_status_operation")
		r.SetSlackProgressNextAttemptAt(nil)
		r.SetGatewayStatusUpdatedAt(now)
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
	schanges := make([]*sw.SlackAgentSessionPublication, 0, len(sessions))
	for _, s := range sessions {
		x.affectProgress(gateway.PhysicalThreadKey{AppID: s.SlackAppId, WorkspaceID: s.SlackWorkspaceId, ChannelID: s.SlackChannelId, RootTS: s.SlackThreadRootMessageId})
		if s.SlackPublicationState != "sending" {
			continue
		}
		r := &sw.SlackAgentSessionPublication{}
		r.SetId(s.Id)
		r.SetSlackPublicationState("ready")
		r.SetSlackPublicationReason("interrupted_status_operation")
		retryAt := timestamp(now.Add(gateway.StatusRetry))
		for _, scheduled := range []*time.Time{s.SlackPublicationNextAttemptAt, s.SlackPublicationRateLimitedUntil} {
			if scheduled != nil && scheduled.After(retryAt) {
				retryAt = *scheduled
			}
		}
		r.SetSlackPublicationNextAttemptAt(timePointer(retryAt))
		r.SetUpdatedAt(now)
		schanges = append(schanges, r)
	}
	if len(schanges) > 0 {
		return x.writeSessions(ctx, schanges)
	}
	return nil
}
