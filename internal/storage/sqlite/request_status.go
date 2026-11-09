package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// Unknown is an observation failure, not a confirmed backend state. Preserve
// the last confirmation so a refreshed Slack message never invents progress.
func (s *Store) RecordObservation(ctx context.Context, id int64, state string, at time.Time) error {
	switch state {
	case "unknown":
		_, err := s.db.ExecContext(ctx, `UPDATE request_status SET observation_uncertain=1,updated_at=? WHERE job_id=?`, at.UnixMilli(), id)
		return err
	case "queued", "running", "completed", "failed", "canceled", "waiting_for_user":
	default:
		return gateway.ErrConflict
	}
	outcome := ""
	if state == "completed" || state == "failed" || state == "canceled" {
		outcome = state
	}
	// A missing row is an intentional legacy job, not a reason to backfill.
	_, err := s.db.ExecContext(ctx, `UPDATE request_status SET observed_state=?,observed_at=?,observation_uncertain=0,
 outcome=CASE WHEN ?='' THEN outcome ELSE ? END,updated_at=? WHERE job_id=?`, state, at.UnixMilli(), outcome, outcome, at.UnixMilli(), id)
	return err
}

type statusProjection struct {
	gateway.RequestStatus
	jobState, outcome, previousPhase string
	uncertain                        bool
	refreshAt                        int64
	sentRevision                     int64
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
		// Help replies without an Agently call must not assert backend success.
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

// Claims project current durable facts, not a backlog of status events. Query
// workers need not be available, and a restart emits only the latest snapshot.
func (s *Store) ClaimStatus(ctx context.Context, now time.Time, kind string) (*gateway.RequestStatus, error) {
	if kind != "urgent" && kind != "heartbeat" && kind != "terminal" {
		return nil, gateway.ErrConflict
	}
	// Alternate concrete targets when both have work. This is scheduling only;
	// delivery facts, cooldowns and recovery remain durable in the two tables.
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	claims := []func(context.Context, time.Time, string) (*gateway.RequestStatus, error){s.claimAgentSession, s.claimMessageStatus}
	if s.preferMessageStatus {
		claims[0], claims[1] = claims[1], claims[0]
	}
	for _, claim := range claims {
		status, err := claim(ctx, now, kind)
		if err != nil || status != nil {
			if status != nil {
				s.preferMessageStatus = status.Target == gateway.StatusTargetAgentSession
			}
			return status, err
		}
	}
	return nil, nil
}

func (s *Store) claimMessageStatus(ctx context.Context, now time.Time, kind string) (*gateway.RequestStatus, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT s.job_id,j.workspace_id,j.channel_id,j.root_thread_ts,j.message_ts,
 s.mode,j.message_surface,s.slack_ts,s.phase,s.revision,s.sent_revision,j.created_at,s.observed_state,s.observed_at,s.outcome,s.observation_uncertain,s.refresh_at,j.state,
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('pending','sending')),
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state='delivered'),
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('failed','delivery_unknown')),
 EXISTS(SELECT 1 FROM inbox_jobs e WHERE e.workspace_id=j.workspace_id AND e.channel_id=j.channel_id AND e.root_thread_ts=j.root_thread_ts AND e.id<j.id AND e.state NOT IN ('completed','rejected')),
 EXISTS(SELECT 1 FROM inbox_jobs e WHERE e.workspace_id=j.workspace_id AND e.channel_id=j.channel_id AND e.root_thread_ts=j.root_thread_ts AND e.id<j.id AND e.state='needs_review')
 FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id
 WHERE s.mode IN ('message','hybrid') AND (s.state='ready' OR (s.state='settled' AND s.phase='delivery_uncertain' AND NOT EXISTS
 (SELECT 1 FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('failed','delivery_unknown'))))
 AND (?!='terminal' OR j.reason='backend_auth_or_identity_rejected')
	 AND s.next_attempt_at<=?
 AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.workspace_id=j.workspace_id AND o.state='pending' AND o.next_attempt_at>?)
 AND NOT EXISTS (SELECT 1 FROM request_status limited JOIN inbox_jobs lj ON lj.id=limited.job_id WHERE lj.workspace_id=j.workspace_id AND limited.rate_limited_until>?)
 AND NOT EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=j.workspace_id AND a.rate_limited_until>?)
 ORDER BY s.last_sent_at,s.job_id`, kind, now.UnixMilli(), now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return nil, err
	}
	var chosen *gateway.RequestStatus
	var skip []int64
	for rows.Next() {
		var p statusProjection
		var created, observed int64
		err = rows.Scan(&p.JobID, &p.WorkspaceID, &p.ChannelID, &p.RootTS, &p.MessageTS, &p.Mode, &p.Surface, &p.SlackTS, &p.previousPhase, &p.Revision, &p.sentRevision, &created, &p.ObservedState, &observed, &p.outcome, &p.uncertain, &p.refreshAt, &p.jobState, &p.pending, &p.delivered, &p.bad, &p.earlier, &p.blocked)
		if err != nil {
			break
		}
		p.Target = gateway.StatusTargetMessage
		p.CreatedAt = time.UnixMilli(created)
		if observed != 0 {
			p.ObservedAt = time.UnixMilli(observed)
		}
		p.Phase, p.Terminal = p.phase(now)
		// No acknowledgment for a final reply already ready before the first POST.
		if p.jobState == "rejected" || (p.SlackTS == "" && p.jobState == "completed") {
			skip = append(skip, p.JobID)
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
		chosen = &p.RequestStatus
		break
	}
	scanErr := rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	for _, id := range skip {
		if _, err := tx.ExecContext(ctx, `UPDATE request_status SET state='skipped',reason='no_status_needed',updated_at=? WHERE job_id=?`, now.UnixMilli(), id); err != nil {
			return nil, err
		}
	}
	if chosen != nil {
		err = changed(tx.ExecContext(ctx, `UPDATE request_status SET state='sending',phase=?,revision=?,attempts=attempts+1,updated_at=? WHERE job_id=? AND state IN ('ready','settled')`, chosen.Phase, chosen.Revision, now.UnixMilli(), chosen.JobID))
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return chosen, nil
}

// Statuses may precede bindings and outbox rows. Resolve from the original job,
// checking both the supplied destination and any already persisted identity.
func (s *Store) GetStatusContext(ctx context.Context, status gateway.RequestStatus) (*gateway.DeliveryContext, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var bindingID int64
	var decision gateway.Decision
	result := &gateway.DeliveryContext{}
	m := &result.Message
	err = tx.QueryRowContext(ctx, `SELECT j.app_id,j.workspace_id,j.channel_id,j.root_thread_ts,j.author_id,j.message_ts,j.message_surface,
 COALESCE(j.binding_id,0),j.access_mode,j.principal_id,j.owner_id
 FROM inbox_jobs j JOIN request_status s ON s.job_id=j.id
 WHERE j.id=? AND j.workspace_id=? AND j.channel_id=? AND j.root_thread_ts=? AND j.message_ts=?
 AND s.mode=? AND ((?='message' AND s.mode IN ('message','hybrid') AND s.state='sending' AND s.revision=? AND s.slack_ts=?) OR
 (?='agent_session' AND s.mode IN ('agent_session','hybrid') AND EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=j.workspace_id AND a.channel_id=j.channel_id AND a.root_thread_ts=j.root_thread_ts AND a.state='sending' AND a.claimed_revision=? AND a.source_job_id=j.id)))`, status.JobID, status.WorkspaceID, status.ChannelID, status.RootTS, status.MessageTS, status.Mode, status.Target, status.Revision, status.SlackTS, status.Target, status.Revision).Scan(&m.AppID, &m.WorkspaceID, &m.ChannelID, &m.RootTS, &m.AuthorID, &m.MessageTS, &m.Surface, &bindingID, &decision.Mode, &decision.PrincipalID, &decision.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, gateway.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	m.Mentioned = m.MessageSurface() == gateway.SurfaceChannel
	if bindingID != 0 {
		result.Binding, err = scanBinding(tx.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM conversation_bindings WHERE id=?", bindingID))
		if err != nil {
			return nil, err
		}
		if result.Binding.ThreadKey != m.ThreadKey || result.Binding.Decision != decision {
			return nil, gateway.ErrConflict
		}
	} else if decision != (gateway.Decision{}) {
		return nil, gateway.ErrConflict
	}
	return result, nil
}

func millis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func (s *Store) FinishStatus(ctx context.Context, status gateway.RequestStatus, result gateway.StatusResult, now time.Time) error {
	if status.Target == gateway.StatusTargetAgentSession {
		return s.finishAgentSession(ctx, status, result, now)
	}
	switch result.State {
	case "ready", "settled", "failed", "delivery_unknown":
	default:
		return gateway.ErrConflict
	}
	if result.SlackTS != "" && status.SlackTS != "" && result.SlackTS != status.SlackTS {
		return gateway.ErrConflict
	}
	return changed(s.db.ExecContext(ctx, `UPDATE request_status SET state=?,
 slack_ts=CASE WHEN ?='' THEN slack_ts ELSE ? END,
 sent_revision=CASE WHEN ?='' THEN sent_revision ELSE ? END,
 first_sent_at=CASE WHEN first_sent_at=0 AND ?!='' THEN ? ELSE first_sent_at END,
 last_sent_at=CASE WHEN ?='' THEN last_sent_at ELSE ? END,
 refresh_at=CASE WHEN ?>0 THEN ? ELSE refresh_at END,next_attempt_at=?,rate_limited_until=?,reason=?,updated_at=?
 WHERE job_id=? AND state='sending' AND revision=?`, result.State, result.SlackTS, result.SlackTS, result.SlackTS, status.Revision, result.SlackTS, now.UnixMilli(), result.SlackTS, now.UnixMilli(), millis(result.NextRefreshAt), millis(result.NextRefreshAt), millis(result.RetryAt), millis(result.RateLimitedUntil), result.Reason, now.UnixMilli(), status.JobID, status.Revision))
}
