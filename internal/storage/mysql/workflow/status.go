package workflow

import (
	"context"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	graphread "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/read"
	graphwrite "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/write"
)

// The projection preserves the SQLite backend's interpretation of persisted
// execution and delivery facts. It does not infer backend progress from time.
type statusProjection struct {
	gateway.RequestStatus
	jobState, outcome, previousPhase string
	uncertain                        bool
	refreshAt, sentRevision          int64
	pending, delivered, bad          int
	earlier, blocked                 bool
}

func (p statusProjection) phase(now time.Time) (string, bool) {
	switch p.jobState {
	case "rejected":
		return "", true
	case "needs_review":
		return "needs_review", true
	case "completed":
		if p.bad > 0 {
			return "delivery_uncertain", true
		}
		if p.pending > 0 {
			return "delivering", false
		}
		if p.outcome == "failed" || p.outcome == "canceled" {
			return p.outcome, true
		}
		if p.delivered > 0 && p.outcome == "completed" {
			return "completed", true
		}
		return "delivered", true
	case "pending":
		if p.blocked {
			return "blocked", false
		}
		if p.earlier {
			return "queued", false
		}
		return "received", false
	}
	if p.uncertain || (!p.ObservedAt.IsZero() && now.Sub(p.ObservedAt) > gateway.ObservationMaxAge) {
		return "checking", false
	}
	switch p.ObservedState {
	case "running":
		return "running", false
	case "queued":
		return "backend_queued", false
	case "failed", "canceled":
		return p.ObservedState, true
	}
	return "contacting", false
}

func statusMillis(at time.Time) int {
	if at.IsZero() {
		return 0
	}
	return int(at.UnixMilli())
}

func statusPatch(id int) (*graphwrite.Job, *graphwrite.RequestStatus) {
	job := &graphwrite.Job{}
	job.SetId(id)
	status := &graphwrite.RequestStatus{}
	status.SetJobId(id)
	job.SetStatus(status)
	return job, status
}

func (e *execution) recordObservation(ctx context.Context, in *Input, _ *Output) error {
	switch in.Text {
	case "unknown", "queued", "running", "completed", "failed", "canceled", "waiting_for_user":
	default:
		return gateway.ErrConflict
	}
	input := &graphread.Input{}
	input.SetMode("key")
	input.SetID(in.ID)
	rows, err := e.readJobs(ctx, input, true)
	if err != nil {
		return err
	}
	// Missing status rows represent legacy work and are never backfilled.
	if len(rows) == 0 || rows[0].Status == nil {
		return nil
	}
	job, status := statusPatch(rows[0].Id)
	status.SetUpdatedAt(int(in.At.UnixMilli()))
	if in.Text == "unknown" {
		status.SetObservationUncertain(1)
	} else {
		status.SetObservedState(in.Text)
		status.SetObservedAt(int(in.At.UnixMilli()))
		status.SetObservationUncertain(0)
		if in.Text == "completed" || in.Text == "failed" || in.Text == "canceled" {
			status.SetOutcome(in.Text)
		}
	}
	return e.writeJobs(ctx, []*graphwrite.Job{job})
}

func (e *execution) claimStatus(ctx context.Context, in *Input, out *Output) error {
	if in.Kind != "urgent" && in.Kind != "heartbeat" && in.Kind != "terminal" {
		return gateway.ErrConflict
	}
	claims := []func(context.Context, time.Time, string) (*gateway.RequestStatus, error){e.claimAgentSession, e.claimMessageStatus}
	if in.PreferMessage {
		claims[0], claims[1] = claims[1], claims[0]
	}
	for _, claim := range claims {
		status, err := claim(ctx, in.At, in.Kind)
		if err != nil {
			return err
		}
		if status != nil {
			out.Status = status
			return nil
		}
	}
	return nil
}

func projectMessageStatus(j *graphread.Job, now time.Time) statusProjection {
	s := j.Status
	p := statusProjection{
		RequestStatus: gateway.RequestStatus{
			JobID:     int64(j.Id),
			ThreadKey: gateway.ThreadKey{WorkspaceID: j.WorkspaceId, ChannelID: j.ChannelId, RootTS: j.RootThreadTs},
			MessageTS: j.MessageTs, Mode: gateway.StatusMode(s.Mode), Target: gateway.StatusTargetMessage,
			Surface: gateway.MessageSurface(j.MessageSurface), SlackTS: s.SlackTs,
			Revision: int64(s.Revision), CreatedAt: time.UnixMilli(int64(j.CreatedAt)), ObservedState: s.ObservedState,
		},
		jobState: j.State, outcome: s.Outcome, previousPhase: s.Phase,
		uncertain: s.ObservationUncertain != 0, refreshAt: int64(s.RefreshAt), sentRevision: int64(s.SentRevision),
		earlier: j.Earlier != 0, blocked: j.Blocked != 0,
	}
	if s.ObservedAt != 0 {
		p.ObservedAt = time.UnixMilli(int64(s.ObservedAt))
	}
	for _, d := range j.Outbox {
		switch d.State {
		case "pending", "sending":
			p.pending++
		case "delivered":
			p.delivered++
		case "failed", "delivery_unknown":
			p.bad++
		}
	}
	p.Phase, p.Terminal = p.phase(now)
	return p
}

func (e *execution) claimMessageStatus(ctx context.Context, now time.Time, kind string) (*gateway.RequestStatus, error) {
	input := &graphread.Input{}
	input.SetMode("status")
	input.SetNow(now.UnixMilli())
	rows, err := e.readJobs(ctx, input, true)
	if err != nil {
		return nil, err
	}
	var patches []*graphwrite.Job
	var chosen *gateway.RequestStatus
	for _, j := range rows {
		if j.Status == nil || (j.Status.State != "ready" && j.Status.State != "settled") {
			continue
		}
		if kind == "terminal" && j.Reason != "backend_auth_or_identity_rejected" {
			continue
		}
		p := projectMessageStatus(j, now)
		if p.jobState == "rejected" || (p.SlackTS == "" && p.jobState == "completed") {
			job, status := statusPatch(j.Id)
			status.SetState("skipped")
			status.SetReason("no_status_needed")
			status.SetUpdatedAt(int(now.UnixMilli()))
			patches = append(patches, job)
			continue
		}
		urgent := p.SlackTS == "" || p.Phase != p.previousPhase || p.Revision > p.sentRevision
		if kind == "terminal" && (!p.Terminal || !urgent) {
			continue
		}
		if kind == "urgent" && !urgent {
			continue
		}
		if kind == "heartbeat" && (urgent || p.Terminal || p.refreshAt > now.UnixMilli()) {
			continue
		}
		p.Revision++
		job, status := statusPatch(j.Id)
		status.SetState("sending")
		status.SetPhase(p.Phase)
		status.SetRevision(int(p.Revision))
		status.SetAttempts(j.Status.Attempts + 1)
		status.SetUpdatedAt(int(now.UnixMilli()))
		patches = append(patches, job)
		chosen = &p.RequestStatus
		break
	}
	if len(patches) > 0 {
		if err = e.writeJobs(ctx, patches); err != nil {
			return nil, err
		}
	}
	return chosen, nil
}

func (e *execution) getStatusContext(ctx context.Context, in *Input, out *Output) error {
	s := in.Status
	j, err := e.job(ctx, s.JobID, false)
	if err != nil {
		return err
	}
	if j.WorkspaceId != s.WorkspaceID || j.ChannelId != s.ChannelID || j.RootThreadTs != s.RootTS || j.MessageTs != s.MessageTS || j.Status == nil || j.Status.Mode != string(s.Mode) {
		return gateway.ErrConflict
	}
	switch s.Target {
	case gateway.StatusTargetMessage:
		if (s.Mode != gateway.StatusMessage && s.Mode != gateway.StatusHybrid) || j.Status.State != "sending" || int64(j.Status.Revision) != s.Revision || j.Status.SlackTs != s.SlackTS {
			return gateway.ErrConflict
		}
	case gateway.StatusTargetAgentSession:
		if s.Mode != gateway.StatusAgentSession && s.Mode != gateway.StatusHybrid {
			return gateway.ErrConflict
		}
		publication, err := e.session(ctx, s.ThreadKey, false)
		if err != nil {
			return err
		}
		if publication.State != "sending" || int64(publication.ClaimedRevision) != s.Revision || int64(publication.SourceJobId) != s.JobID {
			return gateway.ErrConflict
		}
	default:
		return gateway.ErrConflict
	}
	out.Context, err = contextFromJob(j)
	return err
}

func (e *execution) finishStatus(ctx context.Context, in *Input, out *Output) error {
	s, r, now := in.Status, in.StatusResult, in.At
	if s.Target == gateway.StatusTargetAgentSession {
		return e.finishAgentSession(ctx, s, r, now)
	}
	switch r.State {
	case "ready", "settled", "failed", "delivery_unknown":
	default:
		return gateway.ErrConflict
	}
	if r.SlackTS != "" && s.SlackTS != "" && r.SlackTS != s.SlackTS {
		return gateway.ErrConflict
	}
	j, err := e.job(ctx, s.JobID, true)
	if err != nil {
		return err
	}
	previous := j.Status
	if previous == nil || previous.State != "sending" || int64(previous.Revision) != s.Revision {
		return gateway.ErrConflict
	}
	job, status := statusPatch(j.Id)
	status.SetState(r.State)
	if r.SlackTS != "" {
		status.SetSlackTs(r.SlackTS)
		status.SetSentRevision(int(s.Revision))
		if previous.FirstSentAt == 0 {
			status.SetFirstSentAt(int(now.UnixMilli()))
		}
		status.SetLastSentAt(int(now.UnixMilli()))
	}
	if !r.NextRefreshAt.IsZero() && r.NextRefreshAt.UnixMilli() > 0 {
		status.SetRefreshAt(statusMillis(r.NextRefreshAt))
	}
	status.SetNextAttemptAt(statusMillis(r.RetryAt))
	status.SetRateLimitedUntil(statusMillis(r.RateLimitedUntil))
	status.SetReason(r.Reason)
	status.SetUpdatedAt(int(now.UnixMilli()))
	return e.writeJobs(ctx, []*graphwrite.Job{job})
}
