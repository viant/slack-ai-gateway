package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	jr "github.com/viant/agently-slack-gateway/internal/datly/read"
	sr "github.com/viant/agently-slack-gateway/internal/datly/sessions/read"
	sw "github.com/viant/agently-slack-gateway/internal/datly/sessions/write"
	jw "github.com/viant/agently-slack-gateway/internal/datly/write"
	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/xdatly/handler"
)

type statusInvoker func(context.Context, handler.ComponentRequest) (any, error)

func (f statusInvoker) Invoke(ctx context.Context, r handler.ComponentRequest) (any, error) {
	return f(ctx, r)
}
func statusPointer[T any](v T) *T { return &v }

func TestLocalBackendFailureNeverProjectsRemoteSuccess(t *testing.T) {
	for _, reason := range []string{"backend_unavailable", "backend_blocked"} {
		for _, delivery := range []string{"pending", "delivered"} {
			p := projectMessageStatus(&jr.Job{State: "completed", Reason: reason, Status: &jr.RequestStatus{Outcome: "failed"}, Outbox: []*jr.Outbox{{State: delivery}}}, time.Now())
			if p.Phase != "not_submitted" || !p.Terminal || p.Submitted {
				t.Fatalf("local failure projected as remote completion: %+v", p)
			}
		}
	}
}

func TestObservationUsesSparseGraphWritesAndPreservesLegacyRows(t *testing.T) {
	now := time.Unix(100, 0)
	for _, state := range []string{"unknown", "running", "completed"} {
		t.Run(state, func(t *testing.T) {
			var written []*jw.Job
			x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
				switch r.Target.Method {
				case "GET":
					in := r.Input.(*jr.Input)
					if in.ID != 7 {
						t.Fatal("wrong observation scope")
					}
					return &jr.Output{Data: []*jr.Job{{Id: 7, Status: &jr.RequestStatus{ObservedState: "running", Outcome: "completed"}}}}, nil
				case "PATCH":
					written = r.Input.(*jw.Input).Jobs
					return &jw.Output{}, nil
				}
				t.Fatal("unexpected graph")
				return nil, nil
			})}
			if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: state, At: now}, &Output{}); err != nil {
				t.Fatal(err)
			}
			if len(written) != 1 || written[0].Status == nil {
				t.Fatal("observation missing")
			}
			s := written[0].Status
			if s.Has.ObservedAt != (state != "unknown") || s.Has.Outcome != (state == "completed") || !s.Has.ObservationUncertain {
				t.Fatal("observation overwrites unrelated facts")
			}
		})
	}
	writes := 0
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			writes++
		}
		return &jr.Output{Data: []*jr.Job{{Id: 7}}}, nil
	})}
	if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: "unknown", At: now}, &Output{}); err != nil || writes != 0 {
		t.Fatal("legacy status row backfilled")
	}
	if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: "invented", At: now}, &Output{}); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("unrecognized observation accepted")
	}
}

func TestMessageClaimSkipsFastAnswerAndProjectsBlockedWork(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*jw.Job
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*jw.Input).Jobs
			return &jw.Output{}, nil
		}
		input := r.Input.(*jr.Input)
		if input.Mode != "status" || input.Now != now.UnixMilli() {
			t.Fatal("unbounded status read")
		}
		return &jr.Output{Data: []*jr.Job{
			{Id: 1, State: "completed", Status: &jr.RequestStatus{Mode: "message", State: "ready"}},
			{Id: 2, WorkspaceId: "T", ChannelId: "C", RootThreadTs: "root", State: "pending", Blocked: 1, Earlier: 1, Status: &jr.RequestStatus{Mode: "message", State: "ready", Revision: 2}},
		}}, nil
	})}
	got, err := x.claimMessageStatus(context.Background(), now, "urgent")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JobID != 2 || got.Phase != "blocked" || got.Terminal || got.Revision != 3 {
		t.Fatalf("wrong status snapshot: %+v", got)
	}
	if len(written) != 2 || written[0].Status.State != "skipped" || written[1].Status.State != "sending" {
		t.Fatal("claim and skipped intention not written together")
	}
}

func TestMessageFinishRejectsStaleRevisionBeforeWriting(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*jw.Job
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*jw.Input).Jobs
			return &jw.Output{}, nil
		}
		return &jr.Output{Data: []*jr.Job{{Id: 7, Status: &jr.RequestStatus{State: "sending", Revision: 3, SlackTs: "1.001"}}}}, nil
	})}
	in := &Input{Status: gateway.RequestStatus{JobID: 7, Revision: 2, SlackTS: "1.001"}, StatusResult: gateway.StatusResult{State: "ready", SlackTS: "1.001"}, At: now}
	if err := x.finishStatus(context.Background(), in, &Output{}); !errors.Is(err, gateway.ErrConflict) || written != nil {
		t.Fatal("stale receipt persisted")
	}
	in.Status.Revision = 3
	if err := x.finishStatus(context.Background(), in, &Output{}); err != nil {
		t.Fatal(err)
	}
	s := written[0].Status
	if s.SentRevision != 3 || s.FirstSentAt != int(now.UnixMilli()) || s.LastSentAt != int(now.UnixMilli()) {
		t.Fatal("message receipt facts missing")
	}
}

func TestSessionFinishReprojectsIntakeDuringInflightPublication(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*sw.Publication
	a := &sr.Publication{WorkspaceId: "T", ChannelId: "C", RootThreadTs: "root", SourceJobId: 7, State: "sending", DesiredStatus: "processing", Revision: 4, ClaimedRevision: 3, Jobs: []*sr.SessionJob{
		{Id: statusPointer(7), State: statusPointer("completed"), Mode: "hybrid", MessageTs: statusPointer("7.001"), CreatedAt: statusPointer(1)},
		{Id: statusPointer(8), State: statusPointer("pending"), Mode: "hybrid", MessageTs: statusPointer("8.001"), CreatedAt: statusPointer(2)},
	}}
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*sw.Input).Rows
			return &sw.Output{}, nil
		}
		return &sr.Output{Data: []*sr.Publication{a}}, nil
	})}
	status := gateway.RequestStatus{JobID: 7, ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "root"}, Revision: 3, SessionState: "processing", Target: gateway.StatusTargetAgentSession}
	result := gateway.StatusResult{State: "ready", Acknowledged: true, AgentStatus: "processing", SessionStatus: "processing"}
	if err := x.finishAgentSession(context.Background(), status, result, now); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 {
		t.Fatal("missing session receipt")
	}
	p := written[0]
	if p.State != "ready" || p.SourceJobId != 8 || p.Revision != 5 || p.SentRevision != 3 || p.Has.ClaimedRevision {
		t.Fatal("in-flight intake lost or acknowledgement adopted newer revision")
	}
}
