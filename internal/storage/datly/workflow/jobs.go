package workflow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	br "github.com/viant/slack-ai-gateway/internal/datly/bindings/read"
	bw "github.com/viant/slack-ai-gateway/internal/datly/bindings/write"
	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	er "github.com/viant/slack-ai-gateway/internal/datly/read/event"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func (x *execution) enqueue(ctx context.Context, in *Input, out *Output) error {
	m := in.Message
	if x.identity == nil || m.AppID != x.identity.AppID || m.WorkspaceID != x.identity.WorkspaceID {
		return gateway.ErrConflict
	}
	if m.AppID == "" || m.EventID == "" || m.WorkspaceID == "" || m.ChannelID == "" || m.RootTS == "" || m.AuthorID == "" || m.MessageTS == "" {
		return fmt.Errorf("incomplete event")
	}
	exists, err := x.eventExists(ctx, m.EventID)
	if err != nil {
		return err
	}
	if exists {
		out.Duplicate = true
		return nil
	}
	var token [16]byte
	if _, err = rand.Read(token[:]); err != nil {
		return err
	}
	now := timestamp(time.Now())
	mode := m.StatusMode
	if mode == "" {
		mode = gateway.StatusMessage
	}
	row := &jw.GatewayJob{IntegrationId: int(x.scope), SlackChannelId: m.ChannelID, SlackThreadRootMessageId: m.RootTS, SlackEventId: m.EventID, SlackPromptAuthorId: m.AuthorID, SlackPromptMessageId: m.MessageTS, PromptText: &m.Text, PromptHasAttachments: boolInt(m.HasAttachments), GatewayRequestId: hex.EncodeToString(token[:]), GatewayExecutionState: "pending", CreatedAt: now, GatewayExecutionUpdatedAt: now, SlackConversationType: string(m.MessageSurface()), SlackProgressState: "ready", SlackProgressMode: string(mode), GatewayStatusUpdatedAt: now}
	stale, err := x.staleThread(ctx, m)
	if err != nil {
		return err
	}
	if stale {
		text := gateway.IntegrationNotice("integration_changed")
		row.GatewayExecutionState = "completed"
		row.GatewayExecutionFinishedAt = timePointer(now)
		row.GatewayExecutionReason = "conversation_closed"
		row.GatewayExecutionResultText = &text
		row.SlackProgressState = "settled"
		row.SlackProgressPhase = "conversation_closed"
		row.SlackOutgoingMessages = []*jw.SlackOutgoingMessage{{SlackMessageIndex: 0, SlackMessageKind: gateway.DeliveryResponse, SlackMessageText: &text, SlackMessageFormat: "plain", SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now}}
	} else if m.BackendReason != "" {
		if !localBackendFailure(m.BackendReason) {
			return gateway.ErrConflict
		}
		closeUnstarted(row, m.BackendReason, now)
		// New graph nodes are full inserts, unlike the sparse patches used by
		// FailUnstarted for existing jobs.
		row.Has = nil
	}
	if err = x.writeJobs(ctx, []*jw.GatewayJob{row}); err != nil {
		return err
	}
	if row.GatewayExecutionState == "pending" && (mode == gateway.StatusAgentSession || mode == gateway.StatusHybrid) {
		query := &sr.Input{}
		query.SetMode("key")
		query.SetWorkspaceID(m.WorkspaceID)
		query.SetChannelID(m.ChannelID)
		query.SetSlackThreadRootMessageID(m.RootTS)
		publications, e := x.readSessions(ctx, query, true)
		if e != nil {
			return e
		}
		var publication *sw.SlackAgentSessionPublication
		if len(publications) == 0 {
			publication = &sw.SlackAgentSessionPublication{InitialIntegrationId: int(x.scope), SlackChannelId: m.ChannelID, SlackThreadRootMessageId: m.RootTS, LatestStatusJobId: row.Id, StatusSourceJobId: row.Id, SlackInitiatorUserId: m.AuthorID, SlackAgentDesiredStatus: "processing", SlackPublicationRevision: 1, SlackPublicationState: "ready", CreatedAt: now, UpdatedAt: now}
		} else {
			p := publications[0]
			publication = &sw.SlackAgentSessionPublication{}
			publication.SetId(p.Id)
			publication.SetLatestStatusJobId(row.Id)
			publication.SetSlackPublicationRevision(p.SlackPublicationRevision + 1)
			if p.SlackPublicationState != "sending" {
				publication.SetSlackPublicationState("ready")
			}
			publication.SetSlackPublicationReason("")
			publication.SetUpdatedAt(now)
		}
		if err = x.writeSessions(ctx, []*sw.SlackAgentSessionPublication{publication}); err != nil {
			return err
		}
	}
	if !stale {
		q := &br.Input{}
		q.SetWorkspaceID(m.WorkspaceID)
		q.SetChannelID(m.ChannelID)
		q.SetSlackThreadRootMessageID(m.RootTS)
		bindings, err := x.readBindings(ctx, q, true)
		if err != nil {
			return err
		}
		if len(bindings) == 1 {
			if err := x.updateBindingActivity(ctx, bindings[0], now); err != nil {
				return err
			}
		}
	}
	out.Inserted = true
	return nil
}

func (x *execution) jobs(ctx context.Context, in *Input, out *Output) error {
	if in.Action == "claim_job" {
		q := &jr.Input{}
		q.SetMode("claim")
		q.SetNow(timestamp(time.Now()))
		rows, e := x.readJobs(ctx, q, true)
		if e != nil || len(rows) == 0 {
			return e
		}
		j := rows[0]
		if j.GatewayExecutionState != "pending" && (j.GatewayExecutionState != "querying" || j.BackendPollReady != 1) {
			return nil
		}
		row := jobUpdate(j)
		row.SetGatewayExecutionState("processing")
		row.SetBackendPollReady(0)
		row.SetGatewayExecutionClaimCount(j.GatewayExecutionClaimCount + 1)
		if e = x.writeJobs(ctx, []*jw.GatewayJob{row}); e != nil {
			return e
		}
		out.GatewayJob = jobValue(j)
		out.GatewayJob.State = "processing"
		return nil
	}
	j, err := x.job(ctx, in.ID, true)
	if err != nil {
		return err
	}
	if j.GatewayExecutionState == "completed" && localBackendFailure(j.GatewayExecutionReason) {
		return gateway.ErrBackendUnavailable
	}
	row := jobUpdate(j)
	switch in.Action {
	case "bind_job":
		return x.bindJob(ctx, in, out, j)
	case "begin_query":
		if j.GatewayExecutionState != "processing" || j.BackendSubmissionStartedAt != nil || j.ConversationBinding == nil || j.ConversationBinding.GatewayBindingState != "ready" || j.ConversationBinding.BackendConversationId == nil {
			return gateway.ErrConflict
		}
		row.SetGatewayExecutionState("querying")
		row.SetBackendSubmissionStartedAt(timePointer(time.Now()))
		row.SetBackendPollReady(0)
	case "schedule_poll":
		if (j.GatewayExecutionState != "processing" && j.GatewayExecutionState != "querying") || j.BackendPollReady != 0 || j.BackendSubmissionStartedAt == nil {
			return gateway.ErrConflict
		}
		row.SetGatewayExecutionState("querying")
		row.SetBackendPollReady(1)
		row.SetBackendPollAttempts(j.BackendPollAttempts + 1)
		row.SetBackendNextPollAt(timePointer(in.At))
	case "complete":
		if len(in.Parts) == 0 {
			return fmt.Errorf("outbox requires at least one part")
		}
		if (j.GatewayExecutionState != "processing" && j.GatewayExecutionState != "querying") || (j.BackendSubmissionStartedAt != nil && in.GatewayRequestID != j.GatewayRequestId) {
			return gateway.ErrConflict
		}
		parts := make([]*jw.SlackOutgoingMessage, len(in.Parts))
		now := timestamp(time.Now())
		for i, p := range in.Parts {
			if p.Text == "" || (p.Format != gateway.ReplyPlain && p.Format != gateway.ReplyMarkdown) {
				return fmt.Errorf("outbox part requires text and a supported format")
			}
			parts[i] = &jw.SlackOutgoingMessage{JobId: j.Id, SlackMessageIndex: i, SlackMessageKind: gateway.DeliveryResponse, SlackMessageText: &p.Text, SlackMessageFormat: string(p.Format), SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now}
		}
		row.SetGatewayExecutionState("completed")
		row.SetGatewayExecutionFinishedAt(timePointer(now))
		row.SetGatewayExecutionResultText(&in.Text)
		row.SetSlackOutgoingMessages(parts)
		if len(in.Protocol) > 0 {
			if j.ConversationBinding == nil {
				return gateway.ErrConflict
			}
			binding := &bw.ConversationBinding{}
			binding.SetId(j.ConversationBinding.Id)
			binding.SetBackendProtocolMessages(string(in.Protocol[0].Messages))
			binding.SetBackendProtocolState(string(in.Protocol[0].State))
			binding.SetUpdatedAt(now)
			if err := x.writeBindings(ctx, []*bw.ConversationBinding{binding}); err != nil {
				return err
			}
		}
	case "review":
		if j.GatewayExecutionState != "processing" && j.GatewayExecutionState != "querying" {
			return gateway.ErrConflict
		}
		row.SetGatewayExecutionState("needs_review")
		row.SetGatewayExecutionReason(in.Text)
	case "reject":
		if localBackendFailure(in.Text) {
			if (j.GatewayExecutionState != "pending" && j.GatewayExecutionState != "processing") || j.BackendSubmissionStartedAt != nil ||
				(j.ConversationBinding != nil && (j.ConversationBinding.GatewayBindingState == "creating" || j.ConversationBinding.GatewayBindingState == "needs_review")) {
				return gateway.ErrConflict
			}
			closeUnstarted(row, in.Text, timestamp(time.Now()))
			break
		}
		if j.GatewayExecutionState != "processing" {
			return gateway.ErrConflict
		}
		row.SetGatewayExecutionState("rejected")
		row.SetGatewayExecutionFinishedAt(timePointer(time.Now()))
		row.SetGatewayExecutionReason(in.Text)
	default:
		return gateway.ErrConflict
	}
	if err := x.writeJobs(ctx, []*jw.GatewayJob{row}); err != nil {
		return err
	}
	if in.Action == "complete" || in.Action == "reject" {
		return x.touchBinding(ctx, deref(j.ConversationBindingId), time.Now())
	}
	return nil
}
func jobUpdate(j *jr.GatewayJob) *jw.GatewayJob {
	r := &jw.GatewayJob{}
	r.SetId(j.Id)
	r.SetGatewayExecutionUpdatedAt(timestamp(time.Now()))
	return r
}

func (x *execution) bindJob(ctx context.Context, in *Input, out *Output, j *jr.GatewayJob) error {
	d := in.Decision
	if d.Mode == "" || d.PrincipalID == "" || d.OwnerID == "" || j.GatewayExecutionState != "processing" {
		return gateway.ErrConflict
	}
	query := &br.Input{}
	query.SetWorkspaceID(j.SlackWorkspaceId)
	query.SetChannelID(j.SlackChannelId)
	query.SetSlackThreadRootMessageID(j.SlackThreadRootMessageId)
	rows, err := x.readBindings(ctx, query, true)
	if err != nil {
		return err
	}
	var b *gateway.ConversationBinding
	if len(rows) == 0 {
		now := timestamp(time.Now())
		row := &bw.ConversationBinding{IntegrationId: j.IntegrationId, BackendProtocolMessages: "[]", BackendProtocolState: "{}", SlackChannelId: j.SlackChannelId, SlackThreadRootMessageId: j.SlackThreadRootMessageId, GatewayAccessMode: d.Mode, BackendPrincipalId: d.PrincipalID, GatewayBindingOwnerId: d.OwnerID, GatewayBindingState: "reserved", LastActivityAt: now, CreatedAt: now, UpdatedAt: now}
		if err = x.writeBindings(ctx, []*bw.ConversationBinding{row}); err != nil {
			return err
		}
		b = &gateway.ConversationBinding{ID: int64(row.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId}, Decision: d, State: "reserved"}
	} else {
		r := rows[0]
		b = &gateway.ConversationBinding{ID: int64(r.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: r.SlackWorkspaceId, ChannelID: r.SlackChannelId, RootTS: r.SlackThreadRootMessageId}, Decision: gateway.Decision{Mode: r.GatewayAccessMode, PrincipalID: r.BackendPrincipalId, OwnerID: r.GatewayBindingOwnerId}, State: r.GatewayBindingState, ConversationID: deref(r.BackendConversationId), ProtocolMessages: json.RawMessage(r.BackendProtocolMessages), ProtocolState: json.RawMessage(r.BackendProtocolState)}
	}
	if b.Decision != d || (b.State != "reserved" && b.State != "ready") {
		return gateway.ErrConflict
	}
	row := jobUpdate(j)
	id := int(b.ID)
	row.SetConversationBindingId(&id)
	if err = x.writeJobs(ctx, []*jw.GatewayJob{row}); err != nil {
		return err
	}
	out.ConversationBinding = b
	return nil
}
func (x *execution) bindings(ctx context.Context, in *Input, out *Output) error {
	q := &br.Input{}
	if in.Action == "get_binding" {
		q.SetWorkspaceID(in.Key.WorkspaceID)
		q.SetChannelID(in.Key.ChannelID)
		q.SetSlackThreadRootMessageID(in.Key.RootTS)
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
		out.ConversationBinding = &gateway.ConversationBinding{ID: int64(b.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: b.SlackWorkspaceId, ChannelID: b.SlackChannelId, RootTS: b.SlackThreadRootMessageId}, Decision: gateway.Decision{Mode: b.GatewayAccessMode, PrincipalID: b.BackendPrincipalId, OwnerID: b.GatewayBindingOwnerId}, ConversationID: deref(b.BackendConversationId), State: b.GatewayBindingState, ProtocolMessages: json.RawMessage(b.BackendProtocolMessages), ProtocolState: json.RawMessage(b.BackendProtocolState)}
		return nil
	}
	row := &bw.ConversationBinding{}
	row.SetId(b.Id)
	row.SetUpdatedAt(timestamp(time.Now()))
	switch in.Action {
	case "begin_create":
		if b.GatewayBindingState != "reserved" || b.BackendConversationId != nil {
			return gateway.ErrConflict
		}
		row.SetGatewayBindingState("creating")
	case "set_conversation":
		if in.Text == "" || b.GatewayBindingState != "creating" {
			return gateway.ErrConflict
		}
		row.SetGatewayBindingState("ready")
		row.SetBackendConversationId(&in.Text)
	default:
		return gateway.ErrConflict
	}
	return x.writeBindings(ctx, []*bw.ConversationBinding{row})
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
func jobValue(j *jr.GatewayJob) *gateway.GatewayJob {
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: j.SlackWorkspaceId, ChannelID: j.SlackChannelId, RootTS: j.SlackThreadRootMessageId}, AppID: j.SlackAppId, EventID: j.SlackEventId, AuthorID: j.SlackPromptAuthorId, MessageTS: j.SlackPromptMessageId, Text: deref(j.PromptText), HasAttachments: j.PromptHasAttachments != 0, Surface: gateway.MessageSurface(j.SlackConversationType)}
	m.Mentioned = m.MessageSurface() == gateway.SurfaceChannel
	out := &gateway.GatewayJob{ID: int64(j.Id), Message: m, ConversationBindingID: int64(deref(j.ConversationBindingId)), GatewayRequestID: j.GatewayRequestId, State: j.GatewayExecutionState, Result: deref(j.GatewayExecutionResultText), Reason: j.GatewayExecutionReason, PollAttempts: j.BackendPollAttempts}
	if j.ConversationBinding != nil {
		out.Decision = gateway.Decision{Mode: j.ConversationBinding.GatewayAccessMode, PrincipalID: j.ConversationBinding.BackendPrincipalId, OwnerID: j.ConversationBinding.GatewayBindingOwnerId}
	}
	if j.BackendSubmissionStartedAt != nil {
		out.BackendSubmissionStartedAt = deref(j.BackendSubmissionStartedAt)
	}
	return out
}
func contextFromJob(j *jr.GatewayJob) (*gateway.DeliveryContext, error) {
	job := jobValue(j)
	out := &gateway.DeliveryContext{Message: gateway.Message{ThreadKey: job.ThreadKey, AppID: j.SlackAppId, AuthorID: j.SlackPromptAuthorId, MessageTS: j.SlackPromptMessageId, Surface: job.Surface, Mentioned: job.Mentioned}, IntegrationActive: j.IntegrationActive != 0, IntegrationPrincipalID: j.IntegrationPrincipalId, TerminationReason: j.GatewayExecutionReason}
	if j.ConversationBindingId != nil {
		b := j.ConversationBinding
		if b == nil || b.Id != *j.ConversationBindingId || b.IntegrationId != j.IntegrationId {
			return nil, gateway.ErrConflict
		}
		out.ConversationBinding = &gateway.ConversationBinding{ID: int64(b.Id), ThreadKey: gateway.ThreadKey{WorkspaceID: b.SlackWorkspaceId, ChannelID: b.SlackChannelId, RootTS: b.SlackThreadRootMessageId}, Decision: gateway.Decision{Mode: b.GatewayAccessMode, PrincipalID: b.BackendPrincipalId, OwnerID: b.GatewayBindingOwnerId}, State: b.GatewayBindingState, ConversationID: deref(b.BackendConversationId), ProtocolMessages: json.RawMessage(b.BackendProtocolMessages), ProtocolState: json.RawMessage(b.BackendProtocolState)}
		if out.ConversationBinding.ThreadKey != out.Message.ThreadKey {
			return nil, gateway.ErrConflict
		}
	}
	return out, nil
}

// existingEvent reads the unique transport identity after a confirmed native
// rollback. A collision on any other unique field must remain an error.
func (x *execution) existingEvent(ctx context.Context, in *Input, out *Output) error {
	m := in.Message
	if x.identity == nil || m.AppID != x.identity.AppID || m.WorkspaceID != x.identity.WorkspaceID {
		return gateway.ErrConflict
	}
	var err error
	out.Duplicate, err = x.eventExists(ctx, m.EventID)
	return err
}

func (x *execution) eventExists(ctx context.Context, id string) (bool, error) {
	in := &er.Input{}
	in.SetEventID(id)
	out, err := readGraph[er.Output](ctx, x.invoke, graphTarget[er.ReaderComponent]("reader", "GET", "events/exists"), in, false)
	if err != nil {
		return false, err
	}
	return len(out.Data) == 1 && out.Data[0].Found != 0, nil
}

func (x *execution) staleThread(ctx context.Context, m gateway.Message) (bool, error) {
	q := &jr.Input{}
	q.SetMode("stale_thread")
	q.SetAppID(m.AppID)
	q.SetWorkspaceID(m.WorkspaceID)
	q.SetChannelID(m.ChannelID)
	q.SetSlackThreadRootMessageID(m.RootTS)
	rows, err := x.readJobs(ctx, q, false)
	if err != nil {
		return false, err
	}
	if len(rows) > 0 {
		return true, nil
	}
	bq := &br.Input{}
	bq.SetMode("physical_thread")
	bq.SetAppID(m.AppID)
	bq.SetWorkspaceID(m.WorkspaceID)
	bq.SetChannelID(m.ChannelID)
	bq.SetSlackThreadRootMessageID(m.RootTS)
	bindings, err := x.readBindings(ctx, bq, false)
	if err != nil {
		return false, err
	}
	for _, b := range bindings {
		if b.GatewayBindingState == "closed" {
			return true, nil
		}
	}
	return false, nil
}
