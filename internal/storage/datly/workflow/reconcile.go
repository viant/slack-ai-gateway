package workflow

import (
	"context"
	"time"

	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
)

type physicalThread struct{ app, workspace, channel, root string }
type noticeSource struct {
	job       *jr.GatewayJob
	reason    string
	available bool
}

func botKey(app, workspace string) string { return app + "\x00" + workspace }

func finalProgressPhase(phase string) bool {
	switch phase {
	case "completed", "delivered", "failed", "canceled", "not_submitted", "needs_review", "delivery_uncertain", "conversation_closed", "integration_changed", "integration_removed":
		return true
	}
	return false
}

func (x *execution) closeInactive(ctx context.Context, previous, current []storage.Integration, now time.Time) error {
	names := map[string]bool{}
	bots := map[string]bool{}
	for _, v := range current {
		names[v.Name] = true
		bots[botKey(v.AppID, v.WorkspaceID)] = true
	}
	notices := map[physicalThread]noticeSource{}
	for _, identity := range previous {
		if identity.Active {
			continue
		}
		reason := "integration_removed"
		if names[identity.Name] {
			reason = "integration_changed"
		}
		available := bots[botKey(identity.AppID, identity.WorkspaceID)]
		bq := &br.Input{}
		bq.SetMode("all")
		bq.SetIntegrationID(int(identity.ID))
		bindings, err := x.readBindings(ctx, bq, true)
		if err != nil {
			return err
		}
		bp := []*bw.ConversationBinding{}
		for _, b := range bindings {
			if b.GatewayBindingState != "closed" {
				r := &bw.ConversationBinding{}
				r.SetId(b.Id)
				r.SetGatewayBindingState("closed")
				r.SetClosedAt(timePointer(now))
				r.SetUpdatedAt(now)
				bp = append(bp, r)
			}
		}
		if len(bp) > 0 {
			if err = x.writeBindings(ctx, bp); err != nil {
				return err
			}
		}
		q := &jr.Input{}
		q.SetMode("termination")
		q.SetIntegrationID(int(identity.ID))
		jobs, err := x.readJobs(ctx, q, true)
		if err != nil {
			return err
		}
		patches := []*jw.GatewayJob{}
		for _, j := range jobs {
			unfinished := j.GatewayExecutionState != "completed" && j.GatewayExecutionState != "rejected"
			responseUnfinished := false
			for _, d := range j.SlackOutgoingMessages {
				if d.SlackMessageKind == gateway.DeliveryResponse && d.SlackDeliveryState != "delivered" {
					responseUnfinished = true
				}
			}
			terminalReason := j.GatewayExecutionReason
			if unfinished || responseUnfinished {
				terminalReason = reason
			}
			r := jobUpdate(j)
			changed := false
			if unfinished {
				r.SetGatewayExecutionState("rejected")
				if j.GatewayExecutionFinishedAt == nil {
					r.SetGatewayExecutionFinishedAt(timePointer(now))
				}
				r.SetGatewayExecutionReason(reason)
				r.SetBackendPollReady(0)
				r.SetBackendNextPollAt(nil)
				changed = true
			} else if responseUnfinished && !gateway.IntegrationTermination(j.GatewayExecutionReason) {
				r.SetGatewayExecutionReason(reason)
				changed = true
			}
			parts := []*jw.SlackOutgoingMessage{}
			for _, d := range j.SlackOutgoingMessages {
				if d.SlackDeliveryState != "pending" {
					continue
				}
				if d.SlackMessageKind == gateway.DeliveryResponse || !available {
					p := &jw.SlackOutgoingMessage{}
					p.SetId(d.Id)
					p.SetSlackDeliveryState("failed")
					p.SetUpdatedAt(now)
					parts = append(parts, p)
				}
			}
			if len(parts) > 0 {
				r.SetSlackOutgoingMessages(parts)
				changed = true
			}
			if !gateway.IntegrationTermination(j.SlackProgressPhase) && (unfinished || responseUnfinished || j.SlackProgressMessageId != "" && !finalProgressPhase(j.SlackProgressPhase)) {
				s := r
				s.SetSlackProgressPhase(reason)
				s.SetSlackProgressRevision(j.SlackProgressRevision + 1)
				s.SetGatewayStatusUpdatedAt(now)
				s.SetSlackProgressRefreshAt(nil)
				if j.SlackProgressMessageId == "" {
					if j.SlackProgressState == "delivery_unknown" {
						s.SetSlackProgressState("delivery_unknown")
					} else {
						s.SetSlackProgressState("skipped")
					}
					s.SetSlackProgressReason("no_status_needed")
				} else if available {
					s.SetSlackProgressState("ready")
					s.SetSlackProgressReason("")
				} else {
					s.SetSlackProgressState("failed")
					s.SetSlackProgressReason("slack_transport_unavailable")
				}
				changed = true
			} else if !available && gateway.IntegrationTermination(j.SlackProgressPhase) && j.SlackProgressState == "ready" {
				s := r
				s.SetSlackProgressState("failed")
				s.SetSlackProgressReason("slack_transport_unavailable")
				s.SetGatewayStatusUpdatedAt(now)
				changed = true
			}
			if changed {
				patches = append(patches, r)
			}
			if unfinished || responseUnfinished {
				k := physicalThread{identity.AppID, identity.WorkspaceID, j.SlackChannelId, j.SlackThreadRootMessageId}
				old, exists := notices[k]
				if !exists || j.Id > old.job.Id {
					notices[k] = noticeSource{j, terminalReason, available}
				}
			}
		}
		if len(patches) > 0 {
			if err = x.writeJobs(ctx, patches); err != nil {
				return err
			}
		}
		sq := &sr.Input{}
		sq.SetMode("all")
		sq.SetIntegrationID(int(identity.ID))
		pubs, err := x.readSessions(ctx, sq, true)
		if err != nil {
			return err
		}
		sp := []*sw.SlackAgentSessionPublication{}
		for _, p := range pubs {
			if int64(p.InitialIntegrationId) != identity.ID {
				continue
			}
			if p.SlackPublicationState == "failed" || p.SlackPublicationState == "skipped" {
				continue
			}
			if p.SlackPublicationState == "settled" && p.SlackAgentDesiredStatus == "active" && p.SlackPublicationSentRevision >= p.SlackPublicationRevision {
				continue
			}
			if available && p.SlackAgentDesiredStatus == "active" && gateway.IntegrationTermination(p.SlackPublicationReason) {
				continue
			}
			r := sessionPatch(p)
			r.SetSlackAgentDesiredStatus("active")
			r.SetSlackPublicationReason(reason)
			r.SetSlackPublicationRevision(p.SlackPublicationRevision + 1)
			r.SetSlackPublicationRefreshAt(nil)
			r.SetUpdatedAt(now)
			if !available {
				r.SetSlackPublicationState("failed")
				r.SetSlackPublicationReason("slack_transport_unavailable")
			} else if p.SlackPublicationAttempts == 0 && p.SlackPublicationFirstSentAt == nil {
				r.SetSlackPublicationState("skipped")
			} else {
				r.SetSlackPublicationState("ready")
			}
			sp = append(sp, r)
		}
		if len(sp) > 0 {
			if err = x.writeSessions(ctx, sp); err != nil {
				return err
			}
		}
	}
	for key, source := range notices {
		q := &jr.Input{}
		q.SetMode("physical_thread")
		q.SetAppID(key.app)
		q.SetWorkspaceID(key.workspace)
		q.SetChannelID(key.channel)
		q.SetSlackThreadRootMessageID(key.root)
		jobs, err := x.readJobs(ctx, q, false)
		if err != nil {
			return err
		}
		exists := false
		part := -1
		for _, j := range jobs {
			for _, d := range j.SlackOutgoingMessages {
				if d.SlackMessageKind == gateway.DeliveryIntegrationNotice {
					exists = true
				}
				if j.Id == source.job.Id && d.SlackMessageIndex > part {
					part = d.SlackMessageIndex
				}
			}
		}
		if exists {
			continue
		}
		state := "pending"
		if !source.available {
			state = "failed"
		}
		r := jobUpdate(source.job)
		notice := gateway.IntegrationNotice(source.reason)
		r.SetSlackOutgoingMessages([]*jw.SlackOutgoingMessage{{JobId: source.job.Id, SlackMessageIndex: part + 1, SlackMessageKind: gateway.DeliveryIntegrationNotice, SlackMessageText: &notice, SlackMessageFormat: "plain", SlackDeliveryState: state, CreatedAt: now, UpdatedAt: now}})
		if err = x.writeJobs(ctx, []*jw.GatewayJob{r}); err != nil {
			return err
		}
	}
	return nil
}

// failCleanup closes only historical terminal publications for an active bot
// whose configured transport could not be started. It does not touch live work.
func (x *execution) failCleanup(ctx context.Context, reason string) error {
	if x.identity == nil {
		return gateway.ErrConflict
	}
	if reason != "slack_credentials_unavailable" && reason != "slack_transport_unavailable" {
		return gateway.ErrConflict
	}
	now := timestamp(time.Now())
	q := &jr.Input{}
	q.SetMode("inactive_bot")
	q.SetAppID(x.identity.AppID)
	q.SetWorkspaceID(x.identity.WorkspaceID)
	// The native reader limits this cleanup capability to the configured bot.
	out, err := readGraph[jr.Output](ctx, x.invoke, graphTarget[jr.ReaderComponent]("reader", "GET", "jobs"), q, true)
	if err != nil {
		return err
	}
	patches := []*jw.GatewayJob{}
	for _, j := range out.Data {
		if j.IntegrationActive != 0 || j.SlackAppId != x.identity.AppID || j.SlackWorkspaceId != x.identity.WorkspaceID {
			continue
		}
		r := jobUpdate(j)
		changed := false
		parts := []*jw.SlackOutgoingMessage{}
		for _, d := range j.SlackOutgoingMessages {
			if d.SlackMessageKind == gateway.DeliveryIntegrationNotice && d.SlackDeliveryState == "pending" {
				p := &jw.SlackOutgoingMessage{}
				p.SetId(d.Id)
				p.SetSlackDeliveryState("failed")
				p.SetUpdatedAt(now)
				parts = append(parts, p)
			}
		}
		if len(parts) > 0 {
			r.SetSlackOutgoingMessages(parts)
			changed = true
		}
		if gateway.IntegrationTermination(j.SlackProgressPhase) && j.SlackProgressState == "ready" {
			s := r
			s.SetSlackProgressState("failed")
			s.SetSlackProgressReason(reason)
			s.SetGatewayStatusUpdatedAt(now)
			changed = true
		}
		if changed {
			patches = append(patches, r)
		}
	}
	if len(patches) > 0 {
		if err = x.writeJobs(ctx, patches); err != nil {
			return err
		}
	}
	sq := &sr.Input{}
	sq.SetMode("all")
	pubs, err := x.readSessions(ctx, sq, true)
	if err != nil {
		return err
	}
	ps := []*sw.SlackAgentSessionPublication{}
	for _, p := range pubs {
		if gateway.IntegrationTermination(p.SlackPublicationReason) && p.SlackPublicationState == "ready" {
			r := sessionPatch(p)
			r.SetSlackPublicationState("failed")
			r.SetSlackPublicationReason(reason)
			r.SetUpdatedAt(now)
			ps = append(ps, r)
		}
	}
	if len(ps) > 0 {
		return x.writeSessions(ctx, ps)
	}
	return nil
}
