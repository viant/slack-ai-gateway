package sqlite

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

// Status deliberately has no prompt, result, reply text or credential fields.
// It is a consistent snapshot; rows are newest first, counts include all rows.
type Status struct {
	Limit           int                       `json:"limit_per_table"`
	Counts          map[string]map[string]int `json:"counts"`
	Jobs            []JobStatus               `json:"jobs"`
	Bindings        []BindingStatus           `json:"bindings"`
	Deliveries      []DeliveryStatus          `json:"deliveries"`
	AgentSessions   []AgentSessionMetadata    `json:"agent_sessions"`
	RequestStatuses []RequestStatusMetadata   `json:"request_statuses"`
}

// AgentSessionMetadata describes publication only, without upstream payloads.
type AgentSessionMetadata struct {
	Mode             string `json:"mode"`
	Target           string `json:"target"`
	WorkspaceID      string `json:"workspace_id"`
	ChannelID        string `json:"channel_id"`
	ThreadTS         string `json:"thread_ts"`
	SourceJobID      int64  `json:"source_job_id"`
	InitiatorUserID  string `json:"initiator_user_id"`
	DesiredStatus    string `json:"desired_status"`
	AgentStatus      string `json:"agent_status"`
	SessionStatus    string `json:"session_status"`
	State            string `json:"state"`
	Revision         int64  `json:"revision"`
	SentRevision     int64  `json:"sent_revision"`
	Attempts         int    `json:"attempts"`
	NextAttemptAt    int64  `json:"next_attempt_at_ms"`
	RefreshAt        int64  `json:"refresh_at_ms"`
	RateLimitedUntil int64  `json:"rate_limited_until_ms"`
	FirstSentAt      int64  `json:"first_sent_at_ms"`
	LastSentAt       int64  `json:"last_sent_at_ms"`
	Reason           string `json:"reason"`
	Warning          string `json:"warning"`
}

// Metadata only: no rendered status or upstream response bodies.
type RequestStatusMetadata struct {
	Mode                 string `json:"mode"`
	Target               string `json:"target,omitempty"`
	JobID                int64  `json:"job_id"`
	SlackTS              string `json:"slack_ts"`
	Phase                string `json:"phase"`
	State                string `json:"state"`
	ObservedState        string `json:"observed_state"`
	ObservedAt           int64  `json:"observed_at_ms"`
	ObservationUncertain bool   `json:"observation_uncertain"`
	Outcome              string `json:"backend_outcome"`
	Revision             int64  `json:"revision"`
	SentRevision         int64  `json:"sent_revision"`
	Attempts             int    `json:"attempts"`
	NextAttemptAt        int64  `json:"next_attempt_at_ms"`
	RefreshAt            int64  `json:"refresh_at_ms"`
	RateLimitedUntil     int64  `json:"rate_limited_until_ms"`
	FirstSentAt          int64  `json:"first_sent_at_ms"`
	LastSentAt           int64  `json:"last_sent_at_ms"`
	AckLatencyMS         *int64 `json:"ack_latency_ms"`
	Reason               string `json:"reason"`
}
type JobStatus struct {
	ID             int64  `json:"id"`
	WorkspaceID    string `json:"workspace_id"`
	ChannelID      string `json:"channel_id"`
	ThreadTS       string `json:"thread_ts"`
	AuthorID       string `json:"author_id"`
	BindingID      int64  `json:"binding_id"`
	RequestID      string `json:"request_id"`
	TurnID         string `json:"turn_id"`
	State          string `json:"state"`
	Reason         string `json:"reason"`
	PollAttempts   int    `json:"poll_attempts"`
	QueryStartedAt int64  `json:"query_started_at_ms"`
	NextAttemptAt  int64  `json:"next_attempt_at_ms"`
}
type BindingStatus struct {
	ID             int64  `json:"id"`
	WorkspaceID    string `json:"workspace_id"`
	ChannelID      string `json:"channel_id"`
	ThreadTS       string `json:"thread_ts"`
	ConversationID string `json:"conversation_id"`
	Mode           string `json:"access_mode"`
	PrincipalID    string `json:"principal_id"`
	OwnerID        string `json:"owner_id"`
	State          string `json:"state"`
}
type DeliveryStatus struct {
	ID            int64  `json:"id"`
	JobID         int64  `json:"job_id"`
	WorkspaceID   string `json:"workspace_id"`
	ChannelID     string `json:"channel_id"`
	ThreadTS      string `json:"thread_ts"`
	Part          int    `json:"part"`
	State         string `json:"state"`
	SlackTS       string `json:"slack_ts"`
	Attempts      int    `json:"attempts"`
	NextAttemptAt int64  `json:"next_attempt_at_ms"`
}

// CheckRunBindings is read-only. Reject copied/renamed smoke databases and
// accidental reuse under a different route or execution identity before sends.
func (s *Store) CheckRunBindings(ctx context.Context, route gateway.Route, principal string) error {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM inbox_jobs
		WHERE app_id <> ? OR workspace_id <> ? OR (? AND channel_id <> ?)
		OR (access_mode <> '' AND (access_mode <> 'shared_channel' OR principal_id <> ? OR owner_id <> ?))`,
		route.AppID, route.WorkspaceID, route.Resolver == nil, route.ChannelID, principal, principal).Scan(&count)
	if err != nil || count != 0 {
		return fmt.Errorf("Gateway database contains incompatible event routing or cannot be verified")
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM conversation_bindings
		WHERE conversation_id LIKE 'slack-smoke-%' OR access_mode <> 'shared_channel'
		OR workspace_id <> ? OR (? AND channel_id <> ?) OR principal_id <> ? OR owner_id <> ?`,
		route.WorkspaceID, route.Resolver == nil, route.ChannelID, principal, principal).Scan(&count)
	if err != nil {
		return fmt.Errorf("cannot verify Gateway database identity")
	}
	if count != 0 {
		return fmt.Errorf("Gateway database contains smoke or incompatible conversation bindings; use a separate real-integration SQLite database")
	}
	return nil
}

func (s *Store) Status(ctx context.Context, limit int) (*Status, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("status limit must be between 1 and 1000")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result := &Status{Limit: limit, Counts: make(map[string]map[string]int), Jobs: []JobStatus{}, Bindings: []BindingStatus{}, Deliveries: []DeliveryStatus{}}
	result.RequestStatuses = []RequestStatusMetadata{}
	result.AgentSessions = []AgentSessionMetadata{}
	for _, table := range []string{"inbox_jobs", "conversation_bindings", "outbox_messages", "request_status", "agent_session_publications"} {
		rows, err := tx.QueryContext(ctx, "SELECT state,COUNT(*) FROM "+table+" GROUP BY state")
		if err != nil {
			return nil, err
		}
		counts := make(map[string]int)
		for rows.Next() {
			var state string
			var count int
			if err := rows.Scan(&state, &count); err != nil {
				rows.Close()
				return nil, err
			}
			counts[state] = count
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		result.Counts[table] = counts
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,workspace_id,channel_id,root_thread_ts,author_id,COALESCE(binding_id,0),request_id,turn_id,state,reason,poll_attempts,query_started_at,next_attempt_at FROM inbox_jobs ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var j JobStatus
		if err := rows.Scan(&j.ID, &j.WorkspaceID, &j.ChannelID, &j.ThreadTS, &j.AuthorID, &j.BindingID, &j.RequestID, &j.TurnID, &j.State, &j.Reason, &j.PollAttempts, &j.QueryStartedAt, &j.NextAttemptAt); err != nil {
			rows.Close()
			return nil, err
		}
		result.Jobs = append(result.Jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, "SELECT "+bindingColumns+" FROM conversation_bindings ORDER BY id DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var b BindingStatus
		if err := rows.Scan(&b.ID, &b.WorkspaceID, &b.ChannelID, &b.ThreadTS, &b.ConversationID, &b.Mode, &b.PrincipalID, &b.OwnerID, &b.State); err != nil {
			rows.Close()
			return nil, err
		}
		result.Bindings = append(result.Bindings, b)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT id,job_id,workspace_id,channel_id,root_thread_ts,part,state,slack_ts,attempts,next_attempt_at FROM outbox_messages ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d DeliveryStatus
		if err := rows.Scan(&d.ID, &d.JobID, &d.WorkspaceID, &d.ChannelID, &d.ThreadTS, &d.Part, &d.State, &d.SlackTS, &d.Attempts, &d.NextAttemptAt); err != nil {
			rows.Close()
			return nil, err
		}
		result.Deliveries = append(result.Deliveries, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT s.mode,s.job_id,s.slack_ts,s.phase,s.state,s.observed_state,s.observed_at,s.observation_uncertain,s.outcome,s.revision,s.sent_revision,s.attempts,s.next_attempt_at,s.refresh_at,s.rate_limited_until,s.first_sent_at,s.last_sent_at,s.reason,j.created_at FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id ORDER BY s.job_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var s RequestStatusMetadata
		var created int64
		if err := rows.Scan(&s.Mode, &s.JobID, &s.SlackTS, &s.Phase, &s.State, &s.ObservedState, &s.ObservedAt, &s.ObservationUncertain, &s.Outcome, &s.Revision, &s.SentRevision, &s.Attempts, &s.NextAttemptAt, &s.RefreshAt, &s.RateLimitedUntil, &s.FirstSentAt, &s.LastSentAt, &s.Reason, &created); err != nil {
			rows.Close()
			return nil, err
		}
		if s.FirstSentAt != 0 {
			latency := max(int64(0), s.FirstSentAt-created)
			s.AckLatencyMS = &latency
		}
		if s.Mode == string(gateway.StatusMessage) || s.Mode == string(gateway.StatusHybrid) {
			s.Target = string(gateway.StatusTargetMessage)
		}
		result.RequestStatuses = append(result.RequestStatuses, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.QueryContext(ctx, `SELECT s.mode,a.workspace_id,a.channel_id,a.root_thread_ts,a.source_job_id,a.initiator_user_id,a.desired_status,a.agent_status,a.session_status,a.state,a.revision,a.sent_revision,a.attempts,a.next_attempt_at,a.refresh_at,a.rate_limited_until,a.first_sent_at,a.last_sent_at,a.reason,a.warning FROM agent_session_publications a JOIN request_status s ON s.job_id=a.source_job_id ORDER BY a.latest_job_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var a AgentSessionMetadata
		a.Target = string(gateway.StatusTargetAgentSession)
		if err = rows.Scan(&a.Mode, &a.WorkspaceID, &a.ChannelID, &a.ThreadTS, &a.SourceJobID, &a.InitiatorUserID, &a.DesiredStatus, &a.AgentStatus, &a.SessionStatus, &a.State, &a.Revision, &a.SentRevision, &a.Attempts, &a.NextAttemptAt, &a.RefreshAt, &a.RateLimitedUntil, &a.FirstSentAt, &a.LastSentAt, &a.Reason, &a.Warning); err != nil {
			rows.Close()
			return nil, err
		}
		result.AgentSessions = append(result.AgentSessions, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}

	return result, nil
}

var slackTimestamp = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

// ResolveDelivery is an offline operator assertion, not a remote Slack check.
// It updates one uncertain part and its audit record atomically, never a job.
func (s *Store) ResolveDelivery(ctx context.Context, id int64, action, ts string, acceptDuplicateRisk bool) error {
	if id <= 0 {
		return gateway.ErrConflict
	}
	switch action {
	case "delivered":
		if !slackTimestamp.MatchString(ts) || acceptDuplicateRisk {
			return gateway.ErrConflict
		}
	case "retry":
		if !acceptDuplicateRisk || ts != "" {
			return gateway.ErrConflict
		}
	default:
		return gateway.ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state := "delivered"
	if action == "retry" {
		state = "pending"
	}
	now := time.Now().UnixMilli()
	if err = changed(tx.ExecContext(ctx, `UPDATE outbox_messages SET state=?,slack_ts=?,next_attempt_at=0,updated_at=? WHERE id=? AND state='delivery_unknown'`, state, ts, now, id)); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO delivery_resolutions(delivery_id,action,slack_ts,duplicate_risk_accepted,created_at) VALUES(?,?,?,?,?)`, id, action, ts, acceptDuplicateRisk, now); err != nil {
		return err
	}
	return tx.Commit()
}
