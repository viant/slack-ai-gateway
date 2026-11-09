package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	br "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/bindings/read"
	bw "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/bindings/write"
	jr "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/read"
	sr "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/sessions/read"
	sw "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/sessions/write"
	jw "github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/write"
)

func (x *execution) enqueue(ctx context.Context, in *Input, out *Output) error {
	m := in.Message
	if m.AppID == "" || m.EventID == "" || m.WorkspaceID == "" || m.ChannelID == "" || m.RootTS == "" || m.AuthorID == "" || m.MessageTS == "" {
		return fmt.Errorf("incomplete event")
	}
	q := &jr.Input{}
	q.SetMode("key")
	q.SetAppID(m.AppID)
	q.SetWorkspaceID(m.WorkspaceID)
	q.SetEventID(m.EventID)
	rows, err := x.readJobs(ctx, q, false)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return nil
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		return err
	}
	now := int(time.Now().UnixMilli())
	mode := m.StatusMode
	if mode == "" {
		mode = gateway.StatusMessage
	}
	row := &jw.Job{AppId: m.AppID, WorkspaceId: m.WorkspaceID, ChannelId: m.ChannelID, RootThreadTs: m.RootTS, EventId: m.EventID, AuthorId: m.AuthorID, MessageTs: m.MessageTS, QueryText: m.Text, HasAttachments: boolInt(m.HasAttachments), RequestId: hex.EncodeToString(token[:]), State: "pending", CreatedAt: now, UpdatedAt: now, MessageSurface: string(m.MessageSurface()), Status: &jw.RequestStatus{State: "ready", Mode: string(mode), UpdatedAt: now}}
	if err = x.writeJobs(ctx, []*jw.Job{row}); err != nil {
		return err
	}
	if mode == gateway.StatusAgentSession || mode == gateway.StatusHybrid {
		query := &sr.Input{}
		query.SetMode("key")
		query.SetWorkspaceID(m.WorkspaceID)
		query.SetChannelID(m.ChannelID)
		query.SetRootThreadTS(m.RootTS)
		publications, e := x.readSessions(ctx, query, true)
		if e != nil {
			return e
		}
		var publication *sw.Publication
		if len(publications) == 0 {
			publication = &sw.Publication{WorkspaceId: m.WorkspaceID, ChannelId: m.ChannelID, RootThreadTs: m.RootTS, LatestJobId: row.Id, SourceJobId: row.Id, InitiatorUserId: m.AuthorID, DesiredStatus: "processing", Revision: 1, State: "ready", UpdatedAt: now}
		} else {
			p := publications[0]
			publication = &sw.Publication{}
			publication.SetWorkspaceId(p.WorkspaceId)
			publication.SetChannelId(p.ChannelId)
			publication.SetRootThreadTs(p.RootThreadTs)
			publication.SetLatestJobId(row.Id)
			publication.SetRevision(p.Revision + 1)
			if p.State != "sending" {
				publication.SetState("ready")
			}
			publication.SetReason("")
			publication.SetUpdatedAt(now)
		}
		if err = x.writeSessions(ctx, []*sw.Publication{publication}); err != nil {
			return err
		}
	}
	out.Inserted = true
	return nil
}

func (x *execution) jobs(ctx context.Context, in *Input, out *Output) error {
	if in.Action == "claim_job" {
		q := &jr.Input{}
		q.SetMode("claim")
		q.SetNow(time.Now().UnixMilli())
		rows, e := x.readJobs(ctx, q, true)
		if e != nil || len(rows) == 0 {
			return e
		}
		j := rows[0]
		if j.State != "pending" && (j.State != "querying" || j.PollReady != 1) {
			return nil
		}
		row := jobUpdate(j)
		row.SetState("processing")
		row.SetPollReady(0)
		row.SetAttempts(j.Attempts + 1)
		if e = x.writeJobs(ctx, []*jw.Job{row}); e != nil {
			return e
		}
		out.Job = jobValue(j)
		out.Job.State = "processing"
		return nil
	}
	j, err := x.job(ctx, in.ID, true)
	if err != nil {
		return err
	}
	row := jobUpdate(j)
	switch in.Action {
	case "bind_job":
		return x.bindJob(ctx, in, out, j)
	case "begin_query":
		if j.State != "processing" || j.TurnId != "" || j.Binding == nil || j.Binding.State != "ready" || j.Binding.ConversationId == nil {
			return gateway.ErrConflict
		}
		row.SetState("querying")
		row.SetTurnId(j.RequestId)
		row.SetQueryStartedAt(int(time.Now().UnixMilli()))
		row.SetPollReady(0)
	case "schedule_poll":
		if (j.State != "processing" && j.State != "querying") || j.PollReady != 0 || j.TurnId != j.RequestId || j.QueryStartedAt <= 0 {
			return gateway.ErrConflict
		}
		row.SetState("querying")
		row.SetPollReady(1)
		row.SetPollAttempts(j.PollAttempts + 1)
		row.SetNextAttemptAt(int(in.At.UnixMilli()))
	case "complete":
		if len(in.Parts) == 0 {
			return fmt.Errorf("outbox requires at least one part")
		}
		if (j.State != "processing" && j.State != "querying") || (j.TurnId != "" && in.TurnID != j.RequestId) {
			return gateway.ErrConflict
		}
		parts := make([]*jw.Outbox, len(in.Parts))
		now := int(time.Now().UnixMilli())
		for i, p := range in.Parts {
			if p.Text == "" || (p.Format != gateway.ReplyPlain && p.Format != gateway.ReplyMarkdown) {
				return fmt.Errorf("outbox part requires text and a supported format")
			}
			parts[i] = &jw.Outbox{JobId: j.Id, WorkspaceId: j.WorkspaceId, ChannelId: j.ChannelId, RootThreadTs: j.RootThreadTs, Part: i, Content: p.Text, Format: string(p.Format), State: "pending", CreatedAt: now, UpdatedAt: now}
		}
		row.SetState("completed")
		row.SetResultText(in.Text)
		row.SetTurnId(in.TurnID)
		row.SetOutbox(parts)
	case "review":
		if j.State != "processing" && j.State != "querying" {
			return gateway.ErrConflict
		}
		row.SetState("needs_review")
		row.SetReason(in.Text)
	case "reject":
		if j.State != "processing" {
			return gateway.ErrConflict
		}
		row.SetState("rejected")
		row.SetReason(in.Text)
	default:
		return gateway.ErrConflict
	}
	return x.writeJobs(ctx, []*jw.Job{row})
}
func jobUpdate(j *jr.Job) *jw.Job {
	r := &jw.Job{}
	r.SetId(j.Id)
	r.SetUpdatedAt(int(time.Now().UnixMilli()))
	return r
}

func (x *execution) bindJob(ctx context.Context, in *Input, out *Output, j *jr.Job) error {
	d := in.Decision
	if d.Mode == "" || d.PrincipalID == "" || d.OwnerID == "" || j.State != "processing" {
		return gateway.ErrConflict
	}
	query := &br.Input{}
	query.SetWorkspaceID(j.WorkspaceId)
	query.SetChannelID(j.ChannelId)
	query.SetRootThreadTS(j.RootThreadTs)
	rows, err := x.readBindings(ctx, query, true)
	if err != nil {
		return err
	}
	var b *gateway.Binding
	if len(rows) == 0 {
		now := int(time.Now().UnixMilli())
		row := &bw.Binding{WorkspaceId: j.WorkspaceId, ChannelId: j.ChannelId, RootThreadTs: j.RootThreadTs, AccessMode: d.Mode, PrincipalId: d.PrincipalID, OwnerId: d.OwnerID, State: "reserved", CreatedAt: now, UpdatedAt: now}
		if err = x.writeBindings(ctx, []*bw.Binding{row}); err != nil {
			return err
		}
		b = &gateway.Binding{ID: int64(row.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: j.WorkspaceId, ChannelID: j.ChannelId, RootTS: j.RootThreadTs}, Decision: d, State: "reserved"}
	} else {
		r := rows[0]
		b = &gateway.Binding{ID: int64(r.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: r.WorkspaceId, ChannelID: r.ChannelId, RootTS: r.RootThreadTs}, Decision: gateway.Decision{Mode: r.AccessMode, PrincipalID: r.PrincipalId, OwnerID: r.OwnerId}, State: r.State, ConversationID: deref(r.ConversationId)}
	}
	if b.Decision != d || (b.State != "reserved" && b.State != "ready") {
		return gateway.ErrConflict
	}
	row := jobUpdate(j)
	id := int(b.ID)
	row.SetBindingId(&id)
	row.SetAccessMode(d.Mode)
	row.SetPrincipalId(d.PrincipalID)
	row.SetOwnerId(d.OwnerID)
	if err = x.writeJobs(ctx, []*jw.Job{row}); err != nil {
		return err
	}
	out.Binding = b
	return nil
}
func (x *execution) bindings(ctx context.Context, in *Input, out *Output) error {
	q := &br.Input{}
	if in.Action == "get_binding" {
		q.SetWorkspaceID(in.Key.WorkspaceID)
		q.SetChannelID(in.Key.ChannelID)
		q.SetRootThreadTS(in.Key.RootTS)
	} else {
		q.SetID(in.ID)
	}
	rows, err := x.readBindings(ctx, q, in.Action != "get_binding")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		if in.Action == "get_binding" {
			return nil
		}
		return gateway.ErrConflict
	}
	b := rows[0]
	if in.Action == "get_binding" {
		out.Binding = &gateway.Binding{ID: int64(b.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: b.WorkspaceId, ChannelID: b.ChannelId, RootTS: b.RootThreadTs}, Decision: gateway.Decision{Mode: b.AccessMode, PrincipalID: b.PrincipalId, OwnerID: b.OwnerId}, ConversationID: deref(b.ConversationId), State: b.State}
		return nil
	}
	row := &bw.Binding{}
	row.SetId(b.Id)
	row.SetUpdatedAt(int(time.Now().UnixMilli()))
	switch in.Action {
	case "begin_create":
		if b.State != "reserved" || b.ConversationId != nil {
			return gateway.ErrConflict
		}
		row.SetState("creating")
	case "set_conversation":
		if in.Text == "" || b.State != "creating" {
			return gateway.ErrConflict
		}
		row.SetState("ready")
		row.SetConversationId(&in.Text)
	default:
		return gateway.ErrConflict
	}
	return x.writeBindings(ctx, []*bw.Binding{row})
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func deref[T any](v *T) (z T) {
	if v != nil {
		return *v
	}
	return z
}
func jobValue(j *jr.Job) *gateway.Job {
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: j.WorkspaceId, ChannelID: j.ChannelId, RootTS: j.RootThreadTs}, AppID: j.AppId, EventID: j.EventId, AuthorID: j.AuthorId, MessageTS: j.MessageTs, Text: j.QueryText, HasAttachments: j.HasAttachments != 0, Surface: gateway.MessageSurface(j.MessageSurface)}
	m.Mentioned = m.MessageSurface() == gateway.SurfaceChannel
	out := &gateway.Job{ID: int64(j.Id), Message: m, BindingID: int64(deref(j.BindingId)), Decision: gateway.Decision{Mode: j.AccessMode, PrincipalID: j.PrincipalId, OwnerID: j.OwnerId}, RequestID: j.RequestId, TurnID: j.TurnId, State: j.State, Result: j.ResultText, Reason: j.Reason, PollAttempts: j.PollAttempts}
	if j.QueryStartedAt != 0 {
		out.QueryStartedAt = time.UnixMilli(int64(j.QueryStartedAt))
	}
	return out
}
func contextFromJob(j *jr.Job) (*gateway.DeliveryContext, error) {
	job := jobValue(j)
	out := &gateway.DeliveryContext{Message: gateway.Message{ThreadKey: job.ThreadKey, AppID: j.AppId, AuthorID: j.AuthorId, MessageTS: j.MessageTs, Surface: job.Surface, Mentioned: job.Mentioned}}
	if j.BindingId != nil {
		b := j.Binding
		if b == nil || b.Id != *j.BindingId {
			return nil, gateway.ErrConflict
		}
		out.Binding = &gateway.Binding{ID: int64(b.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: b.WorkspaceId, ChannelID: b.ChannelId, RootTS: b.RootThreadTs}, Decision: gateway.Decision{Mode: b.AccessMode, PrincipalID: b.PrincipalId, OwnerID: b.OwnerId}, State: b.State, ConversationID: deref(b.ConversationId)}
		if out.Binding.ThreadKey != out.Message.ThreadKey || out.Binding.Decision != job.Decision {
			return nil, gateway.ErrConflict
		}
	} else if job.Decision != (gateway.Decision{}) {
		return nil, gateway.ErrConflict
	}
	return out, nil
}

// existingEvent reads the unique transport identity after a confirmed native
// rollback. A collision on any other unique field must remain an error.
func (x *execution) existingEvent(ctx context.Context, in *Input, out *Output) error {
	m := in.Message
	q := &jr.Input{}
	q.SetMode("key")
	q.SetAppID(m.AppID)
	q.SetWorkspaceID(m.WorkspaceID)
	q.SetEventID(m.EventID)
	rows, err := x.readJobs(ctx, q, false)
	if err != nil {
		return err
	}
	if len(rows) == 1 {
		out.Job = jobValue(rows[0])
	}
	return nil
}
