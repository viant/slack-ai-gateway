package workflow

import (
	"context"
	"strings"
	"time"

	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func finishedJob(j *jr.GatewayJob) bool {
	return j.GatewayExecutionState == "completed" || j.GatewayExecutionState == "rejected"
}

func (x *execution) closeThread(ctx context.Context, in *Input, out *Output) error {
	if strings.TrimSpace(in.Text) == "" {
		return gateway.ErrConflict
	}
	j, err := x.job(ctx, in.ID, true)
	if err != nil {
		return err
	}
	if finishedJob(j) {
		return nil
	}
	now := in.At
	if now.IsZero() {
		now = time.Now()
	}
	return x.closeJobThread(ctx, j, in.Text, timestamp(now), out)
}

// closeJobThread changes only the local workflow. The backend may still have
// executed the request. The existing transaction owns the binding, queued jobs,
// final progress projection and one root-owned notice together.
func (x *execution) closeJobThread(ctx context.Context, source *jr.GatewayJob, cause string, now time.Time, out *Output) error {
	if finishedJob(source) {
		return nil
	}
	key := gateway.PhysicalThreadKey{AppID: source.SlackAppId, WorkspaceID: source.SlackWorkspaceId, ChannelID: source.SlackChannelId, RootTS: source.SlackThreadRootMessageId}
	bindings := &br.Input{}
	bindings.SetMode("physical_thread")
	bindings.SetAppID(key.AppID)
	bindings.SetWorkspaceID(key.WorkspaceID)
	bindings.SetChannelID(key.ChannelID)
	bindings.SetSlackThreadRootMessageID(key.RootTS)
	rows, err := x.readBindings(ctx, bindings, true)
	if err != nil {
		return err
	}
	if err := x.closeBindings(ctx, rows, now); err != nil {
		return err
	}
	input := &jr.Input{}
	input.SetMode("thread_unfinished")
	input.SetPhysicalThread(true)
	input.SetAppID(key.AppID)
	input.SetWorkspaceID(key.WorkspaceID)
	input.SetChannelID(key.ChannelID)
	input.SetSlackThreadRootMessageID(key.RootTS)
	jobs, err := x.readJobs(ctx, input, true)
	if err != nil {
		return err
	}
	reason := gateway.ConversationClosureReason(cause)
	notice := gateway.ConversationClosureNotice(reason, source.BackendSubmissionStartedAt != nil, source.GatewayExecutionOutcome)
	patches := make([]*jw.GatewayJob, 0, len(jobs))
	found := false
	for _, j := range jobs {
		if finishedJob(j) {
			continue
		}
		patch := jobUpdate(j)
		patch.SetGatewayExecutionState("rejected")
		patch.SetGatewayExecutionReason("conversation_closed")
		patch.SetGatewayExecutionFinishedAt(timePointer(now))
		patch.SetGatewayExecutionUpdatedAt(now)
		patch.SetGatewayStatusUpdatedAt(now)
		patch.SetBackendPollReady(0)
		patch.SetBackendNextPollAt(nil)
		patch.SetSlackProgressRefreshAt(nil)
		if j.SlackProgressState == "settled" && j.SlackProgressMessageId != "" && j.SlackProgressPhase != "conversation_closed" {
			// Historical review messages are settled but still need their final
			// closure update. Sending publications retain their receipt fence.
			patch.SetSlackProgressState("ready")
		}
		out.ClosedJobs++
		if j.BackendSubmissionStartedAt == nil {
			out.UnsubmittedJobs++
		}
		if j.Id == source.Id {
			found = true
			patch.SetGatewayExecutionReason(reason)
			patch.SetGatewayExecutionResultText(&notice)
			parts := make([]*jw.SlackOutgoingMessage, 0, len(j.SlackOutgoingMessages)+1)
			index := 0
			for _, d := range j.SlackOutgoingMessages {
				index = max(index, d.SlackMessageIndex+1)
				if d.SlackDeliveryState == "pending" {
					stale := &jw.SlackOutgoingMessage{}
					stale.SetId(d.Id)
					stale.SetSlackDeliveryState("failed")
					stale.SetUpdatedAt(now)
					parts = append(parts, stale)
				}
			}
			parts = append(parts, &jw.SlackOutgoingMessage{JobId: j.Id, SlackMessageIndex: index, SlackMessageKind: gateway.DeliveryResponse, SlackMessageText: &notice, SlackMessageFormat: string(gateway.ReplyPlain), SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now})
			patch.SetSlackOutgoingMessages(parts)
		}
		patches = append(patches, patch)
	}
	if !found {
		return gateway.ErrConflict
	}
	if err := x.writeJobs(ctx, patches); err != nil {
		return err
	}
	if err := x.retainClosurePublication(ctx, key, source.Id, now); err != nil {
		return err
	}
	out.ThreadClosed = true
	x.affectProgress(key)
	return nil
}

// A message-only question can close an open native cycle. Retain it as the
// latest lifecycle fact while leaving the original publication author/source
// and any in-flight receipt untouched.
func (x *execution) retainClosurePublication(ctx context.Context, key gateway.PhysicalThreadKey, jobID int, now time.Time) error {
	input := &sr.Input{}
	input.SetMode("key")
	input.SetPhysicalThread(true)
	input.SetAppID(key.AppID)
	input.SetWorkspaceID(key.WorkspaceID)
	input.SetChannelID(key.ChannelID)
	input.SetSlackThreadRootMessageID(key.RootTS)
	rows, err := x.readSessions(ctx, input, true)
	if err != nil || len(rows) == 0 {
		return err
	}
	if len(rows) != 1 {
		return gateway.ErrConflict
	}
	a := rows[0]
	if a.LatestStatusJobId == jobID {
		return nil
	}
	patch := sessionPatch(a)
	patch.SetLatestStatusJobId(jobID)
	patch.SetUpdatedAt(now)
	return x.writeSessions(ctx, []*sw.SlackAgentSessionPublication{patch})
}

func (x *execution) closeBindings(ctx context.Context, rows []*br.ConversationBinding, now time.Time) error {
	var changes []*bw.ConversationBinding
	for _, b := range rows {
		if b.GatewayBindingState == "closed" {
			continue
		}
		patch := &bw.ConversationBinding{}
		patch.SetId(b.Id)
		patch.SetGatewayBindingState("closed")
		if b.ClosedAt == nil {
			patch.SetClosedAt(timePointer(now))
		}
		patch.SetUpdatedAt(now)
		patch.SetLastActivityAt(now)
		changes = append(changes, patch)
	}
	if len(changes) == 0 {
		return nil
	}
	return x.writeBindings(ctx, changes)
}
