package workflow

import (
	"context"
	"regexp"
	"time"

	dr "github.com/viant/slack-ai-gateway/internal/datly/deliveries/read"
	dw "github.com/viant/slack-ai-gateway/internal/datly/deliveries/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

var slackTimestamp = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

func (x *execution) deliveries(ctx context.Context, in *Input, out *Output) error {
	q := &dr.Input{}
	q.SetMode("key")
	if in.Action == "claim_delivery" {
		q.SetMode("claim")
		q.SetNow(timestamp(time.Now()))
	} else if in.Action == "delivery_context" {
		q.SetID(in.SlackOutgoingMessage.ID)
	} else {
		q.SetID(in.ID)
	}
	rows, err := x.readDeliveries(ctx, q, in.Action != "delivery_context")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		if in.Action == "claim_delivery" {
			return nil
		}
		return gateway.ErrConflict
	}
	d := rows[0]
	if in.Action == "delivery_context" {
		asserted := in.SlackOutgoingMessage
		k := gateway.ThreadKey{WorkspaceID: d.SlackWorkspaceId, ChannelID: d.SlackChannelId, RootTS: d.SlackThreadRootMessageId}
		if int64(d.JobId) != asserted.JobID || k != asserted.ThreadKey || d.GatewayJob == nil {
			return gateway.ErrConflict
		}
		job, e := x.publicationJob(ctx, int64(d.JobId), false)
		if e != nil {
			return e
		}
		out.Context, err = contextFromJob(job)
		if err == nil {
			out.Context.Kind = d.SlackMessageKind
		}
		return err
	}
	now := timestamp(time.Now())
	row := &dw.SlackOutgoingMessage{}
	row.SetId(d.Id)
	row.SetUpdatedAt(now)
	switch in.Action {
	case "claim_delivery":
		if d.SlackDeliveryState != "pending" {
			return nil
		}
		if d.GatewayJob == nil {
			return gateway.ErrConflict
		}
		row.SetSlackDeliveryState("sending")
		row.SetSlackDeliveryAttempts(d.SlackDeliveryAttempts + 1)
		out.SlackOutgoingMessage = &gateway.SlackOutgoingMessage{ID: int64(d.Id), JobID: int64(d.JobId), ThreadKey: gateway.ThreadKey{WorkspaceID: d.SlackWorkspaceId, ChannelID: d.SlackChannelId, RootTS: d.SlackThreadRootMessageId}, Surface: gateway.MessageSurface(d.GatewayJob.SlackConversationType), Part: d.SlackMessageIndex, Kind: d.SlackMessageKind, Text: deref(d.SlackMessageText), Format: gateway.ReplyFormat(d.SlackMessageFormat), State: "sending", SlackTS: d.SlackMessageId, Attempts: d.SlackDeliveryAttempts + 1}
	case "delivered":
		if d.SlackDeliveryState != "sending" || in.Text == "" {
			return gateway.ErrConflict
		}
		row.SetSlackDeliveryState("delivered")
		row.SetSlackMessageId(in.Text)
	case "delivery_unknown":
		if d.SlackDeliveryState != "sending" {
			return gateway.ErrConflict
		}
		row.SetSlackDeliveryState("delivery_unknown")
	case "retry_delivery":
		if d.SlackDeliveryState != "sending" {
			return gateway.ErrConflict
		}
		row.SetSlackDeliveryState("pending")
		row.SetSlackDeliveryNextAttemptAt(timePointer(in.At))
	case "fail_delivery":
		if d.SlackDeliveryState != "sending" {
			return gateway.ErrConflict
		}
		row.SetSlackDeliveryState("failed")
	case "resolve_delivery":
		if in.ID <= 0 || d.SlackDeliveryState != "delivery_unknown" {
			return gateway.ErrConflict
		}
		switch in.Kind {
		case "delivered":
			if !slackTimestamp.MatchString(in.Text) || in.AcceptDuplicateRisk {
				return gateway.ErrConflict
			}
			row.SetSlackDeliveryState("delivered")
		case "retry":
			if d.IntegrationActive == 0 && d.SlackMessageKind != gateway.DeliveryIntegrationNotice {
				return gateway.ErrConflict
			}
			if !in.AcceptDuplicateRisk || in.Text != "" {
				return gateway.ErrConflict
			}
			row.SetSlackDeliveryState("pending")
		default:
			return gateway.ErrConflict
		}
		row.SetSlackMessageId(in.Text)
		row.SetSlackDeliveryNextAttemptAt(nil)
		row.SetSlackDeliveryResolutions([]*dw.SlackDeliveryResolution{{OutgoingMessageId: d.Id, ResolutionAction: in.Kind, SlackMessageId: in.Text, DuplicateDeliveryRiskAccepted: boolInt(in.AcceptDuplicateRisk), CreatedAt: now}})
	default:
		return gateway.ErrConflict
	}
	if err = x.writeDeliveries(ctx, []*dw.SlackOutgoingMessage{row}); err != nil {
		return err
	}
	if in.Action == "delivered" || in.Action == "resolve_delivery" && in.Kind == "delivered" {
		if d.GatewayJob == nil {
			return gateway.ErrConflict
		}
		return x.touchBinding(ctx, deref(d.GatewayJob.ConversationBindingId), now)
	}
	return nil
}
