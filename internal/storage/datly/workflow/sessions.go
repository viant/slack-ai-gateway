package workflow

import (
	"context"
	"time"

	sessionsread "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sessionswrite "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func sessionKeyInput(key gateway.ThreadKey) *sessionsread.Input {
	input := &sessionsread.Input{}
	input.SetMode("key")
	input.SetWorkspaceID(key.WorkspaceID)
	input.SetChannelID(key.ChannelID)
	input.SetSlackThreadRootMessageID(key.RootTS)
	return input
}

func sessionPatch(a *sessionsread.SlackAgentSessionPublication) *sessionswrite.SlackAgentSessionPublication {
	patch := &sessionswrite.SlackAgentSessionPublication{}
	patch.SetId(a.Id)
	return patch
}

func (e *execution) session(ctx context.Context, key gateway.ThreadKey, lock bool) (*sessionsread.SlackAgentSessionPublication, error) {
	rows, err := e.readSessions(ctx, sessionKeyInput(key), lock)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, gateway.ErrConflict
	}
	return rows[0], nil
}

// Include every job in the thread's activity while retaining a native-enabled
// question as the source of author identity. A blocker dominates queued work.
func projectSession(a *sessionsread.SlackAgentSessionPublication) gateway.GatewayJobProgress {
	key := gateway.ThreadKey{WorkspaceID: a.SlackWorkspaceId, ChannelID: a.SlackChannelId, RootTS: a.SlackThreadRootMessageId}
	p := gateway.GatewayJobProgress{Target: gateway.StatusTargetAgentSession, ThreadKey: key, SessionState: "active", Terminal: true}
	priority := 0
	authRejected := false
	var source *sessionsread.GatewayJobSessionProjection
	for _, j := range a.GatewayJobs {
		if j == nil {
			continue
		}
		state, reason := deref(j.GatewayExecutionState), deref(j.GatewayExecutionReason)
		pending, bad := deref(j.Pending), deref(j.Bad)
		rank := 0
		live := deref(j.IntegrationActive) != 0 && j.BindingState != "closed" && reason != "conversation_closed" && !gateway.IntegrationTermination(reason)
		if live && (state != "completed" && state != "rejected" || pending > 0) {
			rank = 1
		}
		if live && (state == "needs_review" || j.BindingState == "needs_review" || bad > 0 || (state != "completed" && state != "rejected" && j.BackendObservedState == "waiting_for_user")) {
			rank = 2
		}
		if rank == 2 && reason == "backend_auth_or_identity_rejected" {
			authRejected = true
		}
		if live && (j.SlackProgressMode == string(gateway.StatusAgentSession) || j.SlackProgressMode == string(gateway.StatusHybrid)) && (rank >= priority || source == nil) {
			source = j
		}
		if rank > priority {
			priority = rank
		}
	}
	if source == nil {
		// A closed-thread reply is local work, not a new native execution.
		// Cleanup retains its stored source so a later author's permissions
		// cannot authorize publication on behalf of the original question.
		for _, j := range a.GatewayJobs {
			if j != nil && deref(j.Id) == a.StatusSourceJobId {
				source = j
				break
			}
		}
	}
	if source != nil {
		p.JobID, p.MessageTS, p.Surface, p.CreatedAt = int64(deref(source.Id)), deref(source.SlackPromptMessageId), gateway.MessageSurface(deref(source.SlackConversationType)), deref(source.CreatedAt)
		p.IntegrationID = int64(deref(source.IntegrationId))
		p.Mode, p.Phase = gateway.StatusMode(source.SlackProgressMode), deref(source.GatewayExecutionReason)
	}
	switch priority {
	case 1:
		p.SessionState, p.Terminal = "processing", false
	case 2:
		p.SessionState = "suspended"
	}
	if authRejected {
		p.Phase = "backend_auth_or_identity_rejected"
	}
	if p.Terminal && gateway.IntegrationTermination(a.SlackPublicationReason) {
		p.Phase = a.SlackPublicationReason
	}
	return p
}

func (e *execution) claimAgentSession(ctx context.Context, now time.Time, kind string) (*gateway.GatewayJobProgress, error) {
	now = timestamp(now)
	input := &sessionsread.Input{}
	input.SetMode("due")
	rows, err := e.readSessions(ctx, input, true)
	if err != nil {
		return nil, err
	}
	var patches []*sessionswrite.SlackAgentSessionPublication
	var chosen *gateway.GatewayJobProgress
	for _, a := range rows {
		if a.SlackPublicationState == "sending" || a.SlackPublicationState == "failed" || a.SlackPublicationState == "skipped" {
			continue
		}
		p := projectSession(a)
		// One bot may eventually serve several integrations. All their jobs
		// contribute to the projection, but its original source policy and
		// scoped context must publish it through the owning integration.
		closing := gateway.IntegrationTermination(p.Phase)
		if e.scope != 0 && p.IntegrationID != e.scope && !closing {
			continue
		}
		if kind == "integration_terminal" && !closing {
			continue
		}
		if p.SessionState == "active" && a.SlackPublicationAttempts == 0 && a.SlackPublicationFirstSentAt == nil {
			patch := sessionPatch(a)
			patch.SetSlackPublicationState("skipped")
			patch.SetSlackPublicationReason("no_status_needed")
			patch.SetUpdatedAt(timestamp(now))
			patches = append(patches, patch)
			continue
		}
		revision := a.SlackPublicationRevision
		changedProjection := a.SlackAgentDesiredStatus != p.SessionState || int64(a.StatusSourceJobId) != p.JobID
		if changedProjection {
			revision++
		}
		urgent := changedProjection || a.SlackPublicationSentRevision < revision
		if kind == "terminal" && (p.Phase != "backend_auth_or_identity_rejected" || p.SessionState != "suspended") {
			continue
		}
		if kind == "heartbeat" && (urgent || p.SessionState != "processing" || deref(a.SlackPublicationRefreshAt).After(now)) {
			continue
		}
		if kind != "heartbeat" && !urgent {
			continue
		}
		if deref(a.SlackPublicationNextAttemptAt).After(now) {
			continue
		}
		cooldown, err := e.notBefore(ctx, a.SlackWorkspaceId)
		if err != nil {
			return nil, err
		}
		if cooldown.After(now) {
			continue
		}
		revision++
		p.Revision, p.InitiatorUserID = int64(revision), a.SlackInitiatorUserId
		if !closing {
			p.Phase = p.SessionState
		}
		patch := sessionPatch(a)
		patch.SetStatusSourceJobId(int(p.JobID))
		patch.SetSlackAgentDesiredStatus(p.SessionState)
		patch.SetSlackPublicationRevision(revision)
		patch.SetSlackPublicationClaimedRevision(revision)
		patch.SetSlackPublicationState("sending")
		patch.SetSlackPublicationAttempts(a.SlackPublicationAttempts + 1)
		patch.SetUpdatedAt(timestamp(now))
		patches = append(patches, patch)
		chosen = &p
		break
	}
	if len(patches) > 0 {
		if err = e.writeSessions(ctx, patches); err != nil {
			return nil, err
		}
	}
	return chosen, nil
}

func (e *execution) finishAgentSession(ctx context.Context, status gateway.GatewayJobProgress, r gateway.StatusResult, now time.Time) error {
	now = timestamp(now)
	switch r.State {
	case "ready", "settled", "failed":
	default:
		return gateway.ErrConflict
	}
	if r.SlackTS != "" {
		return gateway.ErrConflict
	}
	a, err := e.session(ctx, status.ThreadKey, true)
	if err != nil {
		return err
	}
	if a.SlackPublicationState != "sending" || int64(a.SlackPublicationClaimedRevision) != status.Revision || int64(a.StatusSourceJobId) != status.JobID {
		return gateway.ErrConflict
	}
	p := projectSession(a)
	dirty := int64(a.SlackPublicationRevision) != status.Revision || p.SessionState != status.SessionState || p.JobID != status.JobID
	revision := a.SlackPublicationRevision
	if p.SessionState != a.SlackAgentDesiredStatus || p.JobID != int64(a.StatusSourceJobId) {
		revision++
	}
	state := r.State
	patch := sessionPatch(a)
	if r.Acknowledged {
		state = "settled"
		patch.SetSlackPublicationSentRevision(int(status.Revision))
		patch.SetSlackPublicationLastSentAt(timePointer(now))
		if a.SlackPublicationFirstSentAt == nil {
			patch.SetSlackPublicationFirstSentAt(timePointer(now))
		}
		patch.SetSlackAgentObservedStatus(r.AgentStatus)
		patch.SetSlackSessionObservedStatus(r.SessionStatus)
		patch.SetSlackPublicationWarning(r.Warning)
		patch.SetSlackPublicationRefreshAt(timePointer(r.NextRefreshAt))
	}
	if dirty && (r.Acknowledged || r.State != "failed") {
		state = "ready"
	}
	patch.SetStatusSourceJobId(int(p.JobID))
	patch.SetSlackAgentDesiredStatus(p.SessionState)
	patch.SetSlackPublicationRevision(revision)
	patch.SetSlackPublicationState(state)
	patch.SetSlackPublicationNextAttemptAt(timePointer(r.RetryAt))
	patch.SetSlackPublicationRateLimitedUntil(timePointer(r.RateLimitedUntil))
	patch.SetSlackPublicationReason(r.Reason)
	patch.SetUpdatedAt(timestamp(now))
	return e.writeSessions(ctx, []*sessionswrite.SlackAgentSessionPublication{patch})
}
