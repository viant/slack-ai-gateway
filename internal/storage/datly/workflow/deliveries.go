package workflow

import (
	"context"
	"regexp"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	dr "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/read"
	dw "github.com/viant/agently-slack-gateway/internal/storage/datly/graph/deliveries/write"
)

var slackTimestamp = regexp.MustCompile(`^[0-9]+\.[0-9]+$`)

func (x *execution) deliveries(ctx context.Context, in *Input, out *Output) error {
	q := &dr.Input{}
	q.SetMode("key")
	if in.Action == "claim_delivery" {
		q.SetMode("claim")
		q.SetNow(time.Now().UnixMilli())
	} else if in.Action == "delivery_context" {
		q.SetID(in.Delivery.ID)
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
		asserted := in.Delivery
		k := gateway.ThreadKey{WorkspaceID: d.WorkspaceId, ChannelID: d.ChannelId, RootTS: d.RootThreadTs}
		if int64(d.JobId) != asserted.JobID || k != asserted.ThreadKey || d.Job == nil || d.Job.WorkspaceId != d.WorkspaceId || d.Job.ChannelId != d.ChannelId || d.Job.RootThreadTs != d.RootThreadTs {
			return gateway.ErrConflict
		}
		job, e := x.job(ctx, int64(d.JobId), false)
		if e != nil {
			return e
		}
		out.Context, err = contextFromJob(job)
		return err
	}
	row := &dw.Delivery{}
	row.SetId(d.Id)
	row.SetUpdatedAt(int(time.Now().UnixMilli()))
	switch in.Action {
	case "claim_delivery":
		if d.State != "pending" {
			return nil
		}
		row.SetState("sending")
		row.SetAttempts(d.Attempts + 1)
		out.Delivery = &gateway.Delivery{ID: int64(d.Id), JobID: int64(d.JobId), ThreadKey: gateway.ThreadKey{WorkspaceID: d.WorkspaceId, ChannelID: d.ChannelId, RootTS: d.RootThreadTs}, Part: d.Part, Text: d.Content, Format: gateway.ReplyFormat(d.Format), State: "sending", SlackTS: d.SlackTs, Attempts: d.Attempts + 1}
	case "delivered":
		if d.State != "sending" || in.Text == "" {
			return gateway.ErrConflict
		}
		row.SetState("delivered")
		row.SetSlackTs(in.Text)
	case "delivery_unknown":
		if d.State != "sending" {
			return gateway.ErrConflict
		}
		row.SetState("delivery_unknown")
	case "retry_delivery":
		if d.State != "sending" {
			return gateway.ErrConflict
		}
		row.SetState("pending")
		row.SetNextAttemptAt(int(in.At.UnixMilli()))
	case "fail_delivery":
		if d.State != "sending" {
			return gateway.ErrConflict
		}
		row.SetState("failed")
	case "resolve_delivery":
		if in.ID <= 0 || d.State != "delivery_unknown" {
			return gateway.ErrConflict
		}
		switch in.Kind {
		case "delivered":
			if !slackTimestamp.MatchString(in.Text) || in.AcceptDuplicateRisk {
				return gateway.ErrConflict
			}
			row.SetState("delivered")
		case "retry":
			if !in.AcceptDuplicateRisk || in.Text != "" {
				return gateway.ErrConflict
			}
			row.SetState("pending")
		default:
			return gateway.ErrConflict
		}
		row.SetSlackTs(in.Text)
		row.SetNextAttemptAt(0)
		row.SetResolutions([]*dw.Resolution{{DeliveryId: d.Id, Action: in.Kind, SlackTs: in.Text, DuplicateRiskAccepted: boolInt(in.AcceptDuplicateRisk), CreatedAt: int(time.Now().UnixMilli())}})
	default:
		return gateway.ErrConflict
	}
	return x.writeDeliveries(ctx, []*dw.Delivery{row})
}
