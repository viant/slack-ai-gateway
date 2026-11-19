package workflow

import (
	"context"
	"time"

	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// Progress is invalidated by the operation that changes its source facts, in
// that operation's native transaction. Sleeping publications do not need a
// second monitor or periodic claim just to notice a dependent transition.
func (x *execution) affectProgress(key gateway.PhysicalThreadKey) {
	if !key.Valid() {
		return
	}
	if x.progress == nil {
		x.progress = map[gateway.PhysicalThreadKey]struct{}{}
	}
	x.progress[key] = struct{}{}
}

func (x *execution) affectJob(j *jr.GatewayJob) {
	x.affectProgress(gateway.PhysicalThreadKey{AppID: j.SlackAppId, WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId})
}

func (x *execution) refreshProgress(ctx context.Context, _ *Input, _ *Output) error {
	for key := range x.progress {
		if err := x.refreshMessageProgress(ctx, key); err != nil {
			return err
		}
		if err := x.refreshSessionProgress(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (x *execution) refreshMessageProgress(ctx context.Context, key gateway.PhysicalThreadKey) error {
	in := &jr.Input{}
	in.SetMode("progress_dependencies")
	in.SetPhysicalThread(true)
	in.SetAppID(key.AppID)
	in.SetWorkspaceID(key.WorkspaceID)
	in.SetChannelID(key.ChannelID)
	in.SetSlackThreadRootMessageID(key.RootTS)
	jobs, err := x.readJobs(ctx, in, false)
	if err != nil {
		return err
	}
	now := timestamp(time.Now())
	var changes []*jw.GatewayJob
	for _, j := range jobs {
		p := projectMessageStatus(j, now)
		// Claims and poll scheduling change execution bookkeeping, not the
		// manager's availability. Its next status claim owns that overlay;
		// preserving it here avoids inventing an urgent checking transition.
		if backendStatusKind(j.SlackProgressPhase) && p.Submitted && p.jobState != "completed" && !p.Terminal && p.Phase != "delivering" {
			continue
		}
		if p.Phase == j.SlackProgressPhase {
			continue
		}
		patch := statusPatch(j.Id)
		patch.SetSlackProgressPhase(p.Phase)
		patch.SetGatewayStatusUpdatedAt(now)
		// Message revisions also fence the active send. Preserve that fence
		// until its receipt; finishStatus then leaves the newer phase dirty.
		if j.SlackProgressState != "sending" {
			patch.SetSlackProgressRevision(j.SlackProgressRevision + 1)
			patch.SetSlackProgressState("ready")
		}
		changes = append(changes, patch)
	}
	if len(changes) == 0 {
		return nil
	}
	return x.writeJobs(ctx, changes)
}

func nativeCycleClosed(a *sr.SlackAgentSessionPublication) bool {
	return a.SlackPublicationState == "skipped" ||
		a.SlackPublicationState == "settled" && a.SlackAgentDesiredStatus == "active" && a.SlackPublicationSentRevision >= a.SlackPublicationRevision
}

func (x *execution) refreshSessionProgress(ctx context.Context, key gateway.PhysicalThreadKey) error {
	in := &sr.Input{}
	in.SetMode("key")
	in.SetPhysicalThread(true)
	in.SetAppID(key.AppID)
	in.SetWorkspaceID(key.WorkspaceID)
	in.SetChannelID(key.ChannelID)
	in.SetSlackThreadRootMessageID(key.RootTS)
	rows, err := x.readSessions(ctx, in, false)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	if len(rows) != 1 {
		return gateway.ErrConflict
	}
	a := rows[0]
	if a.SlackPublicationState == "failed" || nativeCycleClosed(a) {
		return nil
	}
	p := projectSession(a)
	if p.SessionState == a.SlackAgentDesiredStatus && p.JobID == int64(a.StatusSourceJobId) {
		return nil
	}
	patch := sessionPatch(a)
	patch.SetSlackAgentDesiredStatus(p.SessionState)
	patch.SetSlackPublicationRevision(a.SlackPublicationRevision + 1)
	patch.SetUpdatedAt(timestamp(time.Now()))
	if a.SlackPublicationState != "sending" {
		patch.SetStatusSourceJobId(int(p.JobID))
		patch.SetSlackPublicationState("ready")
	}
	return x.writeSessions(ctx, []*sw.SlackAgentSessionPublication{patch})
}
