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
func closeUnstarted(row *jw.Job, reason string, now int) {
	notice := gateway.BackendNotice(reason)
	row.SetState("completed")
	row.SetReason(reason)
	row.SetResultText(notice)
	row.SetPollReady(0)
	row.SetUpdatedAt(now)
	row.SetOutbox([]*jw.Outbox{{JobId: row.Id, WorkspaceId: row.WorkspaceId, ChannelId: row.ChannelId, RootThreadTs: row.RootThreadTs, Part: 0, Content: notice, Format: string(gateway.ReplyPlain), State: "pending", CreatedAt: now, UpdatedAt: now}})
	if row.Status == nil {
		s := &jw.RequestStatus{}
		s.SetJobId(row.Id)
		row.SetStatus(s)
	}
	row.Status.SetOutcome("failed")
	row.Status.SetUpdatedAt(now)
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
	updates := make([]*jw.Job, 0, len(jobs))
	now := int(time.Now().UnixMilli())
	for _, j := range jobs {
		// Recheck the native, locked row before any mutation. A claimed worker
		// cannot overwrite this terminal transition with BeginQuery/Complete.
		if (j.State != "pending" && j.State != "processing") || j.TurnId != "" || (j.Binding != nil && (j.Binding.State == "creating" || j.Binding.State == "needs_review")) {
			continue
		}
		row := jobUpdate(j)
		// Outbox transport scope is carried explicitly; sparse job fields stay
		// untouched and the generated writer propagates the parent identity.
		row.WorkspaceId, row.ChannelId, row.RootThreadTs = j.WorkspaceId, j.ChannelId, j.RootThreadTs
		closeUnstarted(row, in.Text, now)
		if j.Status == nil {
			// Preserve the established rule that legacy rows receive no status
			// backfill. The ordinary reply outbox still publishes the notice.
			row.Status = nil
			row.Has.Status = false
		}
		updates = append(updates, row)
	}
	if len(updates) == 0 {
		return nil
	}
	return x.writeJobs(ctx, updates)
}
