package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

const jobColumns = "id,app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,COALESCE(binding_id,0),access_mode,principal_id,owner_id,request_id,turn_id,state,result_text,reason,query_started_at,poll_attempts,message_surface"
const bindingColumns = "id,workspace_id,channel_id,root_thread_ts,COALESCE(conversation_id,''),access_mode,principal_id,owner_id,state"
const deliveryColumns = "id,job_id,workspace_id,channel_id,root_thread_ts,part,content,format,state,slack_ts,attempts"

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (*gateway.Job, error) {
	j := &gateway.Job{}
	var started int64
	err := row.Scan(&j.ID, &j.AppID, &j.WorkspaceID, &j.ChannelID, &j.RootTS, &j.EventID, &j.AuthorID, &j.MessageTS, &j.Text, &j.HasAttachments, &j.BindingID, &j.Mode, &j.PrincipalID, &j.OwnerID, &j.RequestID, &j.TurnID, &j.State, &j.Result, &j.Reason, &started, &j.PollAttempts, &j.Surface)
	if started != 0 {
		j.QueryStartedAt = time.UnixMilli(started)
	}
	j.Mentioned = j.MessageSurface() == gateway.SurfaceChannel
	return j, err
}

func scanBinding(row scanner) (*gateway.Binding, error) {
	b := &gateway.Binding{}
	err := row.Scan(&b.ID, &b.WorkspaceID, &b.ChannelID, &b.RootTS, &b.ConversationID, &b.Mode, &b.PrincipalID, &b.OwnerID, &b.State)
	return b, err
}

func scanDelivery(row scanner) (*gateway.Delivery, error) {
	d := &gateway.Delivery{}
	err := row.Scan(&d.ID, &d.JobID, &d.WorkspaceID, &d.ChannelID, &d.RootTS, &d.Part, &d.Text, &d.Format, &d.State, &d.SlackTS, &d.Attempts)
	return d, err
}

func changed(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return gateway.ErrConflict
	}
	return nil
}

func (s *Store) Enqueue(ctx context.Context, m gateway.Message) (bool, error) {
	if m.AppID == "" || m.EventID == "" || m.WorkspaceID == "" || m.ChannelID == "" || m.RootTS == "" || m.AuthorID == "" || m.MessageTS == "" {
		return false, fmt.Errorf("incomplete event")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return false, err
	}
	now := time.Now().UnixMilli()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, `INSERT INTO inbox_jobs
	(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,message_surface,request_id,state,created_at,updated_at)
	VALUES(?,?,?,?,?,?,?,?,?,?,?,'pending',?,?) ON CONFLICT(app_id,workspace_id,event_id) DO NOTHING`,
		m.AppID, m.WorkspaceID, m.ChannelID, m.RootTS, m.EventID, m.AuthorID, m.MessageTS, m.Text, m.HasAttachments, m.MessageSurface(), hex.EncodeToString(id[:]), now, now)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 1 {
		jobID, err := r.LastInsertId()
		if err != nil {
			return false, err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO request_status(job_id,updated_at,mode) VALUES(?,?,?)", jobID, now, presentationMode(m.StatusMode)); err != nil {
			return false, err
		}
		if m.StatusMode == gateway.StatusAgentSession || m.StatusMode == gateway.StatusHybrid {
			_, err = tx.ExecContext(ctx, `INSERT INTO agent_session_publications(workspace_id,channel_id,root_thread_ts,latest_job_id,source_job_id,initiator_user_id,updated_at)
 VALUES(?,?,?,?,?,?,?) ON CONFLICT(workspace_id,channel_id,root_thread_ts) DO UPDATE SET
 latest_job_id=excluded.latest_job_id, revision=revision+1,
 state=CASE WHEN state='sending' THEN state ELSE 'ready' END,reason='',updated_at=excluded.updated_at`, m.WorkspaceID, m.ChannelID, m.RootTS, jobID, jobID, m.AuthorID, now)
			if err != nil {
				return false, err
			}
		}
	}
	return n == 1, tx.Commit()
}

func (s *Store) GetBinding(ctx context.Context, k gateway.ThreadKey) (*gateway.Binding, error) {
	b, err := scanBinding(s.db.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM conversation_bindings WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?", k.WorkspaceID, k.ChannelID, k.RootTS))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return b, err
}

// GetDeliveryContext reads the original author and binding in one snapshot.
// Destination fields on a claimed delivery are assertions, never authority.
func (s *Store) GetDeliveryContext(ctx context.Context, d gateway.Delivery) (*gateway.DeliveryContext, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var bindingID int64
	var decision gateway.Decision
	result := &gateway.DeliveryContext{}
	m := &result.Message
	err = tx.QueryRowContext(ctx, `SELECT j.app_id,j.workspace_id,j.channel_id,j.root_thread_ts,j.author_id,j.message_surface,
		COALESCE(j.binding_id,0),j.access_mode,j.principal_id,j.owner_id
		FROM outbox_messages AS o JOIN inbox_jobs AS j ON j.id=o.job_id
		WHERE o.id=? AND o.job_id=? AND o.workspace_id=? AND o.channel_id=? AND o.root_thread_ts=?
		AND o.workspace_id=j.workspace_id AND o.channel_id=j.channel_id AND o.root_thread_ts=j.root_thread_ts`,
		d.ID, d.JobID, d.WorkspaceID, d.ChannelID, d.RootTS).Scan(&m.AppID, &m.WorkspaceID, &m.ChannelID, &m.RootTS, &m.AuthorID, &m.Surface,
		&bindingID, &decision.Mode, &decision.PrincipalID, &decision.OwnerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, gateway.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	m.Mentioned = m.MessageSurface() == gateway.SurfaceChannel
	if bindingID != 0 {
		result.Binding, err = scanBinding(tx.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM conversation_bindings WHERE id=?", bindingID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, gateway.ErrConflict
		}
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

func (s *Store) ClaimJob(ctx context.Context) (*gateway.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	j, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM inbox_jobs AS j
	WHERE (state='pending' OR (state='querying' AND poll_ready=1)) AND next_attempt_at<=? AND NOT EXISTS (
	 SELECT 1 FROM inbox_jobs AS earlier WHERE earlier.workspace_id=j.workspace_id AND earlier.channel_id=j.channel_id
	 AND earlier.root_thread_ts=j.root_thread_ts AND earlier.id<j.id AND earlier.state NOT IN ('completed','rejected')
	) ORDER BY id LIMIT 1`, time.Now().UnixMilli()))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = changed(tx.ExecContext(ctx, "UPDATE inbox_jobs SET state='processing',poll_ready=0,attempts=attempts+1,updated_at=? WHERE id=? AND (state='pending' OR (state='querying' AND poll_ready=1))", time.Now().UnixMilli(), j.ID)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	j.State = "processing"
	return j, nil
}

func (s *Store) BindJob(ctx context.Context, id int64, d gateway.Decision) (*gateway.Binding, error) {
	if d.Mode == "" || d.PrincipalID == "" || d.OwnerID == "" {
		return nil, gateway.ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	j, err := scanJob(tx.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM inbox_jobs WHERE id=? AND state='processing'", id))
	if err != nil {
		return nil, err
	}
	now := time.Now().UnixMilli()
	_, err = tx.ExecContext(ctx, `INSERT INTO conversation_bindings (workspace_id,channel_id,root_thread_ts,access_mode,principal_id,owner_id,state,created_at,updated_at)
	VALUES(?,?,?,?,?,?,'reserved',?,?) ON CONFLICT(workspace_id,channel_id,root_thread_ts) DO NOTHING`, j.WorkspaceID, j.ChannelID, j.RootTS, d.Mode, d.PrincipalID, d.OwnerID, now, now)
	if err != nil {
		return nil, err
	}
	b, err := scanBinding(tx.QueryRowContext(ctx, "SELECT "+bindingColumns+" FROM conversation_bindings WHERE workspace_id=? AND channel_id=? AND root_thread_ts=?", j.WorkspaceID, j.ChannelID, j.RootTS))
	if err != nil {
		return nil, err
	}
	if b.Decision != d || (b.State != "reserved" && b.State != "ready") {
		return nil, gateway.ErrConflict
	}
	if err = changed(tx.ExecContext(ctx, "UPDATE inbox_jobs SET binding_id=?,access_mode=?,principal_id=?,owner_id=?,updated_at=? WHERE id=? AND state='processing'", b.ID, d.Mode, d.PrincipalID, d.OwnerID, now, id)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *Store) BeginCreate(ctx context.Context, id int64) error {
	return changed(s.db.ExecContext(ctx, "UPDATE conversation_bindings SET state='creating',updated_at=? WHERE id=? AND state='reserved' AND conversation_id IS NULL", time.Now().UnixMilli(), id))
}

func (s *Store) SetConversation(ctx context.Context, id int64, conversationID string) error {
	if conversationID == "" {
		return gateway.ErrConflict
	}
	return changed(s.db.ExecContext(ctx, "UPDATE conversation_bindings SET conversation_id=?,state='ready',updated_at=? WHERE id=? AND state='creating'", conversationID, time.Now().UnixMilli(), id))
}

func (s *Store) BeginQuery(ctx context.Context, id int64) error {
	now := time.Now().UnixMilli()
	return changed(s.db.ExecContext(ctx, `UPDATE inbox_jobs SET state='querying',turn_id=request_id,query_started_at=?,poll_ready=0,updated_at=? WHERE id=? AND state='processing' AND turn_id=''
	AND binding_id IN (SELECT id FROM conversation_bindings WHERE state='ready' AND conversation_id IS NOT NULL)`, now, now, id))
}

// Scheduling releases the worker, but not the thread. poll_ready distinguishes
// a queued read from a query still executing on another worker.
func (s *Store) SchedulePoll(ctx context.Context, id int64, at time.Time) error {
	return changed(s.db.ExecContext(ctx, `UPDATE inbox_jobs SET state='querying',poll_ready=1,poll_attempts=poll_attempts+1,next_attempt_at=?,updated_at=?
	WHERE id=? AND state IN ('processing','querying') AND poll_ready=0 AND turn_id=request_id AND query_started_at>0`, at.UnixMilli(), time.Now().UnixMilli(), id))
}

func (s *Store) Complete(ctx context.Context, id int64, turnID, result string, parts []gateway.ReplyPart) error {
	if len(parts) == 0 {
		return fmt.Errorf("outbox requires at least one part")
	}
	for _, p := range parts {
		if p.Text == "" || (p.Format != gateway.ReplyPlain && p.Format != gateway.ReplyMarkdown) {
			return fmt.Errorf("outbox part requires text and a supported format")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	j, err := scanJob(tx.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM inbox_jobs WHERE id=?", id))
	if err != nil {
		return err
	}
	if j.State != "processing" && j.State != "querying" {
		return gateway.ErrConflict
	}
	if j.TurnID != "" && turnID != j.RequestID {
		return gateway.ErrConflict
	}
	now := time.Now().UnixMilli()
	if err = changed(tx.ExecContext(ctx, "UPDATE inbox_jobs SET state='completed',result_text=?,turn_id=?,updated_at=? WHERE id=? AND state=?", result, turnID, now, id, j.State)); err != nil {
		return err
	}
	for index, text := range parts {
		_, err = tx.ExecContext(ctx, `INSERT INTO outbox_messages (job_id,workspace_id,channel_id,root_thread_ts,part,content,format,state,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,'pending',?,?)`, id, j.WorkspaceID, j.ChannelID, j.RootTS, index, text.Text, text.Format, now, now)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) Review(ctx context.Context, id int64, reason string) error {
	return changed(s.db.ExecContext(ctx, "UPDATE inbox_jobs SET state='needs_review',reason=?,updated_at=? WHERE id=? AND state IN ('processing','querying')", reason, time.Now().UnixMilli(), id))
}

func (s *Store) Reject(ctx context.Context, id int64, reason string) error {
	return changed(s.db.ExecContext(ctx, "UPDATE inbox_jobs SET state='rejected',reason=?,updated_at=? WHERE id=? AND state='processing'", reason, time.Now().UnixMilli(), id))
}

func (s *Store) ClaimDelivery(ctx context.Context) (*gateway.Delivery, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	d, err := scanDelivery(tx.QueryRowContext(ctx, `SELECT `+deliveryColumns+` FROM outbox_messages AS m WHERE state='pending' AND next_attempt_at<=?
	AND NOT EXISTS (SELECT 1 FROM outbox_messages AS limited WHERE limited.workspace_id=m.workspace_id AND limited.state='pending' AND limited.next_attempt_at>?)
	AND NOT EXISTS (SELECT 1 FROM request_status limited JOIN inbox_jobs j ON j.id=limited.job_id WHERE j.workspace_id=m.workspace_id AND limited.rate_limited_until>?)
 AND NOT EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=m.workspace_id AND a.rate_limited_until>?)
	AND NOT EXISTS (SELECT 1 FROM outbox_messages AS earlier WHERE earlier.workspace_id=m.workspace_id AND earlier.channel_id=m.channel_id
	AND earlier.root_thread_ts=m.root_thread_ts AND earlier.id<m.id AND earlier.state!='delivered') ORDER BY id LIMIT 1`, now, now, now, now))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = changed(tx.ExecContext(ctx, "UPDATE outbox_messages SET state='sending',attempts=attempts+1,updated_at=? WHERE id=? AND state='pending'", time.Now().UnixMilli(), d.ID)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	d.State = "sending"
	d.Attempts++
	return d, nil
}

func (s *Store) Delivered(ctx context.Context, id int64, ts string) error {
	if ts == "" {
		return gateway.ErrConflict
	}
	return changed(s.db.ExecContext(ctx, "UPDATE outbox_messages SET state='delivered',slack_ts=?,updated_at=? WHERE id=? AND state='sending'", ts, time.Now().UnixMilli(), id))
}

func (s *Store) DeliveryUnknown(ctx context.Context, id int64) error {
	return changed(s.db.ExecContext(ctx, "UPDATE outbox_messages SET state='delivery_unknown',updated_at=? WHERE id=? AND state='sending'", time.Now().UnixMilli(), id))
}

// Retry is used only for a confirmed rejection (429 or invalid credentials),
// never for an uncertain network result. Pending retry timestamps also gate
// new messages from this workspace, including messages created after the 429.
func (s *Store) RetryDelivery(ctx context.Context, id int64, at time.Time) error {
	return changed(s.db.ExecContext(ctx, "UPDATE outbox_messages SET state='pending',next_attempt_at=?,updated_at=? WHERE id=? AND state='sending'", at.UnixMilli(), time.Now().UnixMilli(), id))
}

func (s *Store) FailDelivery(ctx context.Context, id int64) error {
	return changed(s.db.ExecContext(ctx, "UPDATE outbox_messages SET state='failed',updated_at=? WHERE id=? AND state='sending'", time.Now().UnixMilli(), id))
}

func (s *Store) DeliveryNotBefore(ctx context.Context, workspace string) (time.Time, error) {
	var ms int64
	err := s.db.QueryRowContext(ctx, `SELECT MAX(COALESCE((SELECT MAX(next_attempt_at) FROM outbox_messages WHERE workspace_id=? AND state='pending'),0), COALESCE((SELECT MAX(s.rate_limited_until) FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id WHERE j.workspace_id=?),0), COALESCE((SELECT MAX(rate_limited_until) FROM agent_session_publications WHERE workspace_id=?),0))`, workspace, workspace, workspace).Scan(&ms)
	return time.UnixMilli(ms), err
}

// Recover resumes uncertain queries as read-only reconciliation. Creation and
// delivery remain quarantined because there is no safe remote lookup contract.
func (s *Store) Recover(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		"UPDATE conversation_bindings SET state='needs_review',updated_at=? WHERE state='creating'",
		"UPDATE inbox_jobs SET state='needs_review',reason='interrupted_external_operation',updated_at=? WHERE state IN ('processing','querying') AND (binding_id IN (SELECT id FROM conversation_bindings WHERE state='needs_review') OR (turn_id!='' AND (query_started_at=0 OR turn_id!=request_id)))",
		"UPDATE inbox_jobs SET state='querying',poll_ready=1,updated_at=? WHERE state IN ('processing','querying') AND turn_id=request_id AND query_started_at>0",
		"UPDATE inbox_jobs SET state='pending',updated_at=? WHERE state='processing'",
		"UPDATE outbox_messages SET state='delivery_unknown',updated_at=? WHERE state='sending'",
		"UPDATE request_status SET state=CASE WHEN slack_ts='' THEN 'delivery_unknown' ELSE 'ready' END,reason='interrupted_status_operation',next_attempt_at=0,updated_at=? WHERE state='sending'",
	} {
		if _, err = tx.ExecContext(ctx, query, time.Now().UnixMilli()); err != nil {
			return err
		}
	}
	now := time.Now()
	if _, err = tx.ExecContext(ctx, `UPDATE agent_session_publications SET state='ready',reason='interrupted_status_operation',next_attempt_at=MAX(next_attempt_at,rate_limited_until,?),updated_at=? WHERE state='sending'`, now.Add(gateway.StatusRetry).UnixMilli(), now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}
