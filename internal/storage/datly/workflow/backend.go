package workflow

import (
	"context"
	"time"

	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func localBackendFailure(reason string) bool {
	return reason == "backend_unavailable" || reason == "backend_blocked"
}

func backendStatusKind(kind string) bool {
	return kind == "backend_recovering" || kind == "backend_blocked"
}

// The normal nested writer owns both the terminal job and its local reply.
// A failed write rolls back the whole invocation, including new transport intake.
func closeUnstarted(row *jw.GatewayJob, reason string, now time.Time) {
	notice := gateway.BackendNotice(reason)
	row.SetGatewayExecutionState("completed")
	row.SetGatewayExecutionFinishedAt(timePointer(now))
	row.SetGatewayExecutionReason(reason)
	row.SetGatewayExecutionResultText(&notice)
	row.SetBackendPollReady(0)
	row.SetGatewayExecutionUpdatedAt(now)
	row.SetSlackOutgoingMessages([]*jw.SlackOutgoingMessage{{JobId: row.Id, SlackMessageIndex: 0, SlackMessageKind: gateway.DeliveryResponse, SlackMessageText: &notice, SlackMessageFormat: string(gateway.ReplyPlain), SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now}})
	row.SetGatewayExecutionOutcome("failed")
	row.SetGatewayStatusUpdatedAt(now)
}

func (x *execution) failUnstarted(ctx context.Context, in *Input, _ *Output) error {
	if !localBackendFailure(in.Text) {
		return gateway.ErrConflict
	}
	q := &jr.Input{}
	q.SetMode("unstarted")
	jobs, err := x.readJobs(ctx, q, true)
	if err != nil {
		return err
	}
	updates := make([]*jw.GatewayJob, 0, len(jobs))
	bindings := map[int]bool{}
	now := timestamp(time.Now())
	for _, j := range jobs {
		// Recheck the native, locked row before any mutation. A claimed worker
		// cannot overwrite this terminal transition with BeginQuery/Complete.
		if (j.GatewayExecutionState != "pending" && j.GatewayExecutionState != "processing") || j.BackendSubmissionStartedAt != nil || (j.ConversationBinding != nil && (j.ConversationBinding.GatewayBindingState == "creating" || j.ConversationBinding.GatewayBindingState == "needs_review")) {
			continue
		}
		row := jobUpdate(j)
		closeUnstarted(row, in.Text, now)
		updates = append(updates, row)
		if id := deref(j.ConversationBindingId); id != 0 {
			bindings[id] = true
		}
	}
	if len(updates) == 0 {
		return nil
	}
	if err := x.writeJobs(ctx, updates); err != nil {
		return err
	}
	for id := range bindings {
		if err := x.touchBinding(ctx, id, now); err != nil {
			return err
		}
	}
	return nil
}
