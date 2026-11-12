package workflow

import (
	"context"
	"time"

	graphread "github.com/viant/slack-ai-gateway/internal/datly/read"
	graphwrite "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// The projection preserves the SQLite backend's interpretation of persisted
// execution and delivery facts. It does not infer backend progress from time.
type statusProjection struct {
	gateway.GatewayJobProgress
	jobState, outcome, previousPhase, reason string
	uncertain                                bool
	refreshAt                                time.Time
	sentRevision                             int64
	pending, delivered, bad                  int
	earlier, blocked                         bool
}

func (p statusProjection) phase(now time.Time) (string, bool) {
	if gateway.IntegrationTermination(p.reason) || p.reason == "conversation_closed" {
		return p.reason, true
	}
	switch p.jobState {
	case "rejected":
		return "", true
	case "needs_review":
		return "needs_review", true
	case "completed":
		if p.bad > 0 {
			return "delivery_uncertain", true
		}
		if localBackendFailure(p.reason) {
			return "not_submitted", true
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

func statusPatch(id int) *graphwrite.GatewayJob {
	job := &graphwrite.GatewayJob{}
	job.SetId(id)
	return job
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
	if len(rows) == 0 {
		return nil
	}
	if rows[0].GatewayExecutionState == "completed" && localBackendFailure(rows[0].GatewayExecutionReason) {
		// A stale worker observation cannot convert a local rejection into a
		// successful remote execution.
		return gateway.ErrBackendUnavailable
	}
	status := statusPatch(rows[0].Id)
	status.SetGatewayStatusUpdatedAt(timestamp(in.At))
	if in.Text == "unknown" {
		status.SetBackendObservationUncertain(1)
	} else {
		status.SetBackendObservedState(in.Text)
		status.SetBackendObservedAt(timePointer(in.At))
		status.SetBackendObservationUncertain(0)
		if in.Text == "completed" || in.Text == "failed" || in.Text == "canceled" {
			status.SetGatewayExecutionOutcome(in.Text)
		}
	}
	return e.writeJobs(ctx, []*graphwrite.GatewayJob{status})
}

func (e *execution) claimStatus(ctx context.Context, in *Input, out *Output) error {
	if backendStatusKind(in.Kind) {
		var err error
		out.Status, err = e.claimMessageStatus(ctx, in.At, in.Kind)
		return err
	}
	if in.Kind != "urgent" && in.Kind != "heartbeat" && in.Kind != "terminal" && in.Kind != "integration_terminal" {
		return gateway.ErrConflict
	}
	claims := []func(context.Context, time.Time, string) (*gateway.GatewayJobProgress, error){e.claimAgentSession, e.claimMessageStatus}
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

func projectMessageStatus(j *graphread.GatewayJob, now time.Time) statusProjection {
	s := j
	p := statusProjection{
		GatewayJobProgress: gateway.GatewayJobProgress{
			JobID: int64(j.Id), IntegrationID: int64(j.IntegrationId),
			ThreadKey: gateway.ThreadKey{WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId},
			MessageTS: j.SlackPromptMessageId, Mode: gateway.StatusMode(s.SlackProgressMode), Target: gateway.StatusTargetMessage, Submitted: j.BackendSubmissionStartedAt != nil,
			Surface: gateway.MessageSurface(j.SlackConversationType), SlackTS: s.SlackProgressMessageId,
			Revision: int64(s.SlackProgressRevision), CreatedAt: j.CreatedAt, ObservedState: s.BackendObservedState,
		},
		jobState: j.GatewayExecutionState, outcome: s.GatewayExecutionOutcome, previousPhase: s.SlackProgressPhase, reason: j.GatewayExecutionReason,
		uncertain: s.BackendObservationUncertain != 0, refreshAt: deref(s.SlackProgressRefreshAt), sentRevision: int64(s.SlackProgressSentRevision),
		earlier: j.Earlier != 0, blocked: j.Blocked != 0,
	}
	if j.IntegrationActive == 0 && gateway.IntegrationTermination(s.SlackProgressPhase) {
		p.reason = s.SlackProgressPhase
	}
	if s.BackendObservedAt != nil {
		p.ObservedAt = *s.BackendObservedAt
	}
	for _, d := range j.SlackOutgoingMessages {
		switch d.SlackDeliveryState {
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

func (e *execution) claimMessageStatus(ctx context.Context, now time.Time, kind string) (*gateway.GatewayJobProgress, error) {
	now = timestamp(now)
	input := &graphread.Input{}
	input.SetMode("status")
	if backendStatusKind(kind) {
		input.SetMode("backend_status")
	}
	input.SetNow(now)
	rows, err := e.readJobs(ctx, input, true)
	if err != nil {
		return nil, err
	}
	var patches []*graphwrite.GatewayJob
	var chosen *gateway.GatewayJobProgress
	for _, j := range rows {
		if j.SlackProgressState != "ready" && j.SlackProgressState != "settled" {
			continue
		}
		if kind == "terminal" && j.GatewayExecutionReason != "backend_auth_or_identity_rejected" {
			continue
		}
		p := projectMessageStatus(j, now)
		closing := gateway.IntegrationTermination(p.Phase)
		if kind == "integration_terminal" && !closing {
			continue
		}
		if backendStatusKind(kind) && !p.Terminal && p.jobState != "completed" {
			if !p.Submitted {
				continue
			}
			p.Phase = kind
		}
		if (p.jobState == "rejected" && !closing) || (p.SlackTS == "" && (p.jobState == "completed" || closing)) {
			status := statusPatch(j.Id)
			status.SetSlackProgressState("skipped")
			status.SetSlackProgressReason("no_status_needed")
			status.SetGatewayStatusUpdatedAt(timestamp(now))
			patches = append(patches, status)
			continue
		}
		urgent := p.SlackTS == "" || p.Phase != p.previousPhase || p.Revision > p.sentRevision
		if backendStatusKind(kind) && !urgent && (p.Terminal || p.refreshAt.After(now)) {
			continue
		}
		if (kind == "terminal" || kind == "integration_terminal") && (!p.Terminal || !urgent) {
			continue
		}
		if kind == "urgent" && !urgent {
			continue
		}
		if kind == "heartbeat" && (urgent || p.Terminal || p.refreshAt.After(now)) {
			continue
		}
		p.Revision++
		status := statusPatch(j.Id)
		status.SetSlackProgressState("sending")
		status.SetSlackProgressPhase(p.Phase)
		status.SetSlackProgressRevision(int(p.Revision))
		status.SetSlackProgressAttempts(j.SlackProgressAttempts + 1)
		status.SetGatewayStatusUpdatedAt(timestamp(now))
		patches = append(patches, status)
		chosen = &p.GatewayJobProgress
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
	j, err := e.publicationJob(ctx, s.JobID, false)
	if err != nil {
		return err
	}
	if j.SlackWorkspaceId != s.WorkspaceID || j.SlackChannelId != s.ChannelID || j.SlackThreadRootMessageId != s.RootTS || j.SlackPromptMessageId != s.MessageTS || j.SlackProgressMode != string(s.Mode) {
		return gateway.ErrConflict
	}
	switch s.Target {
	case gateway.StatusTargetMessage:
		// The claimed outage phase owns the first fallback publication even
		// when execution completes before its Slack message is sent.
		fallback := s.Mode == gateway.StatusAgentSession && (j.SlackProgressMessageId != "" ||
			(j.BackendSubmissionStartedAt != nil && backendStatusKind(s.Phase) && j.SlackProgressPhase == s.Phase))
		if (s.Mode != gateway.StatusMessage && s.Mode != gateway.StatusHybrid && !fallback) || j.SlackProgressState != "sending" || int64(j.SlackProgressRevision) != s.Revision || j.SlackProgressMessageId != s.SlackTS {
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
		if publication.SlackPublicationState != "sending" || int64(publication.SlackPublicationClaimedRevision) != s.Revision || int64(publication.StatusSourceJobId) != s.JobID {
			return gateway.ErrConflict
		}
	default:
		return gateway.ErrConflict
	}
	out.Context, err = contextFromJob(j)
	if err == nil && gateway.IntegrationTermination(s.Phase) && j.IntegrationActive == 0 {
		out.Context.Kind = gateway.DeliveryIntegrationNotice
		out.Context.TerminationReason = s.Phase
	}
	return err
}

func (e *execution) finishStatus(ctx context.Context, in *Input, out *Output) error {
	s, r, now := in.Status, in.StatusResult, timestamp(in.At)
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
	j, err := e.publicationJob(ctx, s.JobID, true)
	if err != nil {
		return err
	}
	previous := j
	if previous.SlackProgressState != "sending" || int64(previous.SlackProgressRevision) != s.Revision {
		return gateway.ErrConflict
	}
	status := statusPatch(j.Id)
	status.SetSlackProgressState(r.State)
	if r.SlackTS != "" {
		status.SetSlackProgressMessageId(r.SlackTS)
		status.SetSlackProgressSentRevision(int(s.Revision))
		if previous.SlackProgressFirstSentAt == nil {
			status.SetSlackProgressFirstSentAt(timePointer(now))
		}
		status.SetSlackProgressLastSentAt(timePointer(now))
	}
	if !r.NextRefreshAt.IsZero() {
		status.SetSlackProgressRefreshAt(timePointer(r.NextRefreshAt))
	}
	status.SetSlackProgressNextAttemptAt(timePointer(r.RetryAt))
	status.SetSlackProgressRateLimitedUntil(timePointer(r.RateLimitedUntil))
	status.SetSlackProgressReason(r.Reason)
	status.SetGatewayStatusUpdatedAt(timestamp(now))
	return e.writeJobs(ctx, []*graphwrite.GatewayJob{status})
}
