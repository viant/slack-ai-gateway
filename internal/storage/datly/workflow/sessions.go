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
	input.SetRootThreadTS(key.RootTS)
	return input
}

func sessionPatch(a *sessionsread.Publication) *sessionswrite.Publication {
	patch := &sessionswrite.Publication{}
	patch.SetWorkspaceId(a.WorkspaceId)
	patch.SetChannelId(a.ChannelId)
	patch.SetRootThreadTs(a.RootThreadTs)
	return patch
}

func (e *execution) session(ctx context.Context, key gateway.ThreadKey, lock bool) (*sessionsread.Publication, error) {
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
func projectSession(a *sessionsread.Publication) gateway.RequestStatus {
	key := gateway.ThreadKey{WorkspaceID: a.WorkspaceId, ChannelID: a.ChannelId, RootTS: a.RootThreadTs}
	p := gateway.RequestStatus{Target: gateway.StatusTargetAgentSession, ThreadKey: key, SessionState: "active", Terminal: true}
	priority := 0
	authRejected := false
	for _, j := range a.Jobs {
		if j == nil {
			continue
		}
		state, reason := deref(j.State), deref(j.Reason)
		pending, bad := deref(j.Pending), deref(j.Bad)
		rank := 0
		if state != "completed" && state != "rejected" || pending > 0 {
			rank = 1
		}
		if state == "needs_review" || j.BindingState == "needs_review" || bad > 0 || (state != "completed" && state != "rejected" && j.ObservedState == "waiting_for_user") {
			rank = 2
		}
		if rank == 2 && reason == "backend_auth_or_identity_rejected" {
			authRejected = true
		}
		if (j.Mode == string(gateway.StatusAgentSession) || j.Mode == string(gateway.StatusHybrid)) && (rank >= priority || p.JobID == 0) {
			p.JobID, p.MessageTS, p.Surface, p.CreatedAt = int64(deref(j.Id)), deref(j.MessageTs), gateway.MessageSurface(deref(j.MessageSurface)), time.UnixMilli(int64(deref(j.CreatedAt)))
			p.Mode, p.Phase = gateway.StatusMode(j.Mode), reason
		}
		if rank > priority {
			priority = rank
		}
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
	return p
}

func (e *execution) claimAgentSession(ctx context.Context, now time.Time, kind string) (*gateway.RequestStatus, error) {
	input := &sessionsread.Input{}
	input.SetMode("due")
	rows, err := e.readSessions(ctx, input, true)
	if err != nil {
		return nil, err
	}
	var patches []*sessionswrite.Publication
	var chosen *gateway.RequestStatus
	for _, a := range rows {
		if a.State == "sending" || a.State == "failed" || a.State == "skipped" {
			continue
		}
		if a.State == "settled" && a.DesiredStatus == "active" && a.SentRevision >= a.Revision {
			continue
		}
		p := projectSession(a)
		if p.SessionState == "active" && a.Attempts == 0 && a.FirstSentAt == 0 {
			patch := sessionPatch(a)
			patch.SetState("skipped")
			patch.SetReason("no_status_needed")
			patch.SetUpdatedAt(int(now.UnixMilli()))
			patches = append(patches, patch)
			continue
		}
		revision := a.Revision
		changedProjection := a.DesiredStatus != p.SessionState || int64(a.SourceJobId) != p.JobID
		if changedProjection {
			revision++
		}
		urgent := changedProjection || a.SentRevision < revision
		if kind == "terminal" && (p.Phase != "backend_auth_or_identity_rejected" || p.SessionState != "suspended") {
			continue
		}
		if kind == "heartbeat" && (urgent || p.SessionState != "processing" || int64(a.RefreshAt) > now.UnixMilli()) {
			continue
		}
		if kind != "heartbeat" && !urgent {
			continue
		}
		if int64(a.NextAttemptAt) > now.UnixMilli() {
			continue
		}
		cooldown, err := e.notBefore(ctx, a.WorkspaceId)
		if err != nil {
			return nil, err
		}
		if cooldown.After(now) {
			continue
		}
		revision++
		p.Revision, p.InitiatorUserID, p.Phase = int64(revision), a.InitiatorUserId, p.SessionState
		patch := sessionPatch(a)
		patch.SetSourceJobId(int(p.JobID))
		patch.SetDesiredStatus(p.SessionState)
		patch.SetRevision(revision)
		patch.SetClaimedRevision(revision)
		patch.SetState("sending")
		patch.SetAttempts(a.Attempts + 1)
		patch.SetUpdatedAt(int(now.UnixMilli()))
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

func (e *execution) finishAgentSession(ctx context.Context, status gateway.RequestStatus, r gateway.StatusResult, now time.Time) error {
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
	if a.State != "sending" || int64(a.ClaimedRevision) != status.Revision || int64(a.SourceJobId) != status.JobID {
		return gateway.ErrConflict
	}
	p := projectSession(a)
	dirty := int64(a.Revision) != status.Revision || p.SessionState != status.SessionState || p.JobID != status.JobID
	revision := a.Revision
	if p.SessionState != a.DesiredStatus || p.JobID != int64(a.SourceJobId) {
		revision++
	}
	state := r.State
	patch := sessionPatch(a)
	if r.Acknowledged {
		state = "settled"
		patch.SetSentRevision(int(status.Revision))
		patch.SetLastSentAt(int(now.UnixMilli()))
		if a.FirstSentAt == 0 {
			patch.SetFirstSentAt(int(now.UnixMilli()))
		}
		patch.SetAgentStatus(r.AgentStatus)
		patch.SetSessionStatus(r.SessionStatus)
		patch.SetWarning(r.Warning)
		patch.SetRefreshAt(statusMillis(r.NextRefreshAt))
	}
	if dirty && (r.Acknowledged || r.State != "failed") {
		state = "ready"
	}
	patch.SetSourceJobId(int(p.JobID))
	patch.SetDesiredStatus(p.SessionState)
	patch.SetRevision(revision)
	patch.SetState(state)
	patch.SetNextAttemptAt(statusMillis(r.RetryAt))
	patch.SetRateLimitedUntil(statusMillis(r.RateLimitedUntil))
	patch.SetReason(r.Reason)
	patch.SetUpdatedAt(int(now.UnixMilli()))
	return e.writeSessions(ctx, []*sessionswrite.Publication{patch})
}
