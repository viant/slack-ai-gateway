package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

func presentationMode(mode gateway.StatusMode) gateway.StatusMode {
	if mode == "" {
		return gateway.StatusMessage
	}
	return mode
}

type sessionProjection struct {
	gateway.RequestStatus
}

// Project existing execution and delivery facts. The publication row is not a
// second backend observer. A blocker wins over any amount of queued work.
func projectSession(ctx context.Context, tx *sql.Tx, key gateway.ThreadKey) (sessionProjection, error) {
	p := sessionProjection{RequestStatus: gateway.RequestStatus{Target: gateway.StatusTargetAgentSession, ThreadKey: key, SessionState: "active", Terminal: true}}
	rows, err := tx.QueryContext(ctx, `SELECT j.id,j.message_ts,j.message_surface,j.created_at,COALESCE(s.mode,'message'),j.state,COALESCE(s.observed_state,''),j.reason,
 COALESCE(b.state,''),
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('pending','sending')),
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('delivery_unknown','failed'))
 FROM inbox_jobs j LEFT JOIN request_status s ON s.job_id=j.id LEFT JOIN conversation_bindings b ON b.id=j.binding_id
 WHERE j.workspace_id=? AND j.channel_id=? AND j.root_thread_ts=? ORDER BY j.id`, key.WorkspaceID, key.ChannelID, key.RootTS)
	if err != nil {
		return p, err
	}
	defer rows.Close()
	priority := 0
	authRejected := false
	for rows.Next() {
		var id, created int64
		var ts, surface, mode, state, observed, reason, binding string
		var pending, bad int
		if err = rows.Scan(&id, &ts, &surface, &created, &mode, &state, &observed, &reason, &binding, &pending, &bad); err != nil {
			return p, err
		}
		rank := 0
		if state != "completed" && state != "rejected" || pending > 0 {
			rank = 1
		}
		if state == "needs_review" || binding == "needs_review" || bad > 0 || (state != "completed" && state != "rejected" && observed == "waiting_for_user") {
			rank = 2
		}
		if rank == 2 && reason == "backend_auth_or_identity_rejected" {
			authRejected = true
		}
		// Only a native question may authorize this publication. Include all jobs in
		// aggregation, but never substitute another question's author or principal.
		if (mode == string(gateway.StatusAgentSession) || mode == string(gateway.StatusHybrid)) && (rank >= priority || p.JobID == 0) {
			p.JobID, p.MessageTS, p.Surface, p.CreatedAt = id, ts, gateway.MessageSurface(surface), time.UnixMilli(created)
			p.Mode, p.Phase = gateway.StatusMode(mode), reason
		}
		if rank > priority {
			priority = rank
		}
	}
	if err = rows.Err(); err != nil {
		return p, err
	}
	switch priority {
	case 1:
		p.SessionState, p.Terminal = "processing", false
	case 2:
		p.SessionState = "suspended"
	}
	if authRejected {
		// Terminal shutdown must also settle an open native cycle when the
		// blocked question uses message mode after a configuration change.
		p.Phase = "backend_auth_or_identity_rejected"
	}
	return p, nil
}

type sessionPublication struct {
	gateway.ThreadKey
	sourceID, revision, sent, claimed, attempts, next, refresh, first, last, limited int64
	initiator, desired, agent, state                                                 string
}

func readPublication(ctx context.Context, tx *sql.Tx, key gateway.ThreadKey) (sessionPublication, error) {
	a := sessionPublication{ThreadKey: key}
	err := tx.QueryRowContext(ctx, `SELECT source_job_id,revision,sent_revision,claimed_revision,attempts,next_attempt_at,refresh_at,first_sent_at,last_sent_at,rate_limited_until,initiator_user_id,desired_status,agent_status,state FROM agent_session_publications WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?`, key.WorkspaceID, key.ChannelID, key.RootTS).Scan(&a.sourceID, &a.revision, &a.sent, &a.claimed, &a.attempts, &a.next, &a.refresh, &a.first, &a.last, &a.limited, &a.initiator, &a.desired, &a.agent, &a.state)
	return a, err
}

func (s *Store) claimAgentSession(ctx context.Context, now time.Time, kind string) (*gateway.RequestStatus, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT workspace_id,channel_id,root_thread_ts FROM agent_session_publications WHERE state NOT IN ('sending','failed','skipped') ORDER BY last_sent_at,latest_job_id`)
	if err != nil {
		return nil, err
	}
	var keys []gateway.ThreadKey
	for rows.Next() {
		var k gateway.ThreadKey
		if err = rows.Scan(&k.WorkspaceID, &k.ChannelID, &k.RootTS); err != nil {
			break
		}
		keys = append(keys, k)
	}
	scanErr := rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	var chosen *gateway.RequestStatus
	for _, key := range keys {
		a, err := readPublication(ctx, tx, key)
		if err != nil {
			return nil, err
		}
		// A confirmed active publication ends this cycle. Message-only intake
		// leaves it alone; native/hybrid intake increments revision and sets ready.
		if a.state == "settled" && a.desired == "active" && a.sent >= a.revision {
			continue
		}
		p, err := projectSession(ctx, tx, key)
		if err != nil {
			return nil, err
		}
		if p.SessionState == "active" && a.attempts == 0 && a.first == 0 {
			_, err = tx.ExecContext(ctx, `UPDATE agent_session_publications SET state='skipped',reason='no_status_needed',updated_at=? WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?`, now.UnixMilli(), key.WorkspaceID, key.ChannelID, key.RootTS)
			if err != nil {
				return nil, err
			}
			continue
		}
		changedProjection := a.desired != p.SessionState || a.sourceID != p.JobID
		if changedProjection {
			a.revision++
			a.desired = p.SessionState
			a.sourceID = p.JobID
		}
		urgent := changedProjection || a.sent < a.revision
		if kind == "terminal" && (p.Phase != "backend_auth_or_identity_rejected" || p.SessionState != "suspended") {
			continue
		}
		if kind == "heartbeat" && (urgent || p.SessionState != "processing" || a.refresh > now.UnixMilli()) {
			continue
		}
		if kind != "heartbeat" && !urgent {
			continue
		}
		if a.next > now.UnixMilli() {
			continue
		}
		var cooldown int64
		err = tx.QueryRowContext(ctx, `SELECT MAX(
 COALESCE((SELECT MAX(next_attempt_at) FROM outbox_messages WHERE workspace_id=? AND state='pending'),0),
 COALESCE((SELECT MAX(s.rate_limited_until) FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id WHERE j.workspace_id=?),0),
 COALESCE((SELECT MAX(rate_limited_until) FROM agent_session_publications WHERE workspace_id=?),0))`, key.WorkspaceID, key.WorkspaceID, key.WorkspaceID).Scan(&cooldown)
		if err != nil {
			return nil, err
		}
		if cooldown > now.UnixMilli() {
			continue
		}
		a.revision++
		p.Revision, p.InitiatorUserID = a.revision, a.initiator
		p.Phase = p.SessionState
		_, err = tx.ExecContext(ctx, `UPDATE agent_session_publications SET source_job_id=?,desired_status=?,revision=?,claimed_revision=?,state='sending',attempts=attempts+1,updated_at=? WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?`, p.JobID, p.SessionState, a.revision, a.revision, now.UnixMilli(), key.WorkspaceID, key.ChannelID, key.RootTS)
		if err != nil {
			return nil, err
		}
		chosen = &p.RequestStatus
		break
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return chosen, nil
}

func (s *Store) finishAgentSession(ctx context.Context, status gateway.RequestStatus, r gateway.StatusResult, now time.Time) error {
	switch r.State {
	case "ready", "settled", "failed":
	default:
		return gateway.ErrConflict
	}
	if r.SlackTS != "" {
		return gateway.ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	a, err := readPublication(ctx, tx, status.ThreadKey)
	if err != nil {
		return err
	}
	if a.state != "sending" || a.claimed != status.Revision || a.sourceID != status.JobID {
		return gateway.ErrConflict
	}
	p, err := projectSession(ctx, tx, status.ThreadKey)
	if err != nil {
		return err
	}
	dirty := a.revision != status.Revision || p.SessionState != status.SessionState || p.JobID != status.JobID
	if p.SessionState != a.desired || p.JobID != a.sourceID {
		a.revision++
	}
	state := r.State
	if r.Acknowledged {
		state = "settled"
		a.sent = status.Revision
		a.last = now.UnixMilli()
		if a.first == 0 {
			a.first = a.last
		}
		a.agent = r.AgentStatus
	}
	if dirty && (r.Acknowledged || r.State != "failed") {
		state = "ready"
	}
	_, err = tx.ExecContext(ctx, `UPDATE agent_session_publications SET source_job_id=?,desired_status=?,revision=?,sent_revision=?,state=?,agent_status=?,
 session_status=CASE WHEN ? THEN ? ELSE session_status END,warning=CASE WHEN ? THEN ? ELSE warning END,
 first_sent_at=?,last_sent_at=?,refresh_at=CASE WHEN ? THEN ? ELSE refresh_at END,next_attempt_at=?,rate_limited_until=?,reason=?,updated_at=?
 WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?`, p.JobID, p.SessionState, a.revision, a.sent, state, a.agent, r.Acknowledged, r.SessionStatus, r.Acknowledged, r.Warning, a.first, a.last, r.Acknowledged, millis(r.NextRefreshAt), millis(r.RetryAt), millis(r.RateLimitedUntil), r.Reason, now.UnixMilli(), status.WorkspaceID, status.ChannelID, status.RootTS)
	if err != nil {
		return err
	}
	return tx.Commit()
}
