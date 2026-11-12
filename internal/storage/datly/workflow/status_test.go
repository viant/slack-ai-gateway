package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	sr "github.com/viant/slack-ai-gateway/internal/datly/sessions/read"
	sw "github.com/viant/slack-ai-gateway/internal/datly/sessions/write"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
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
			p := projectMessageStatus(&jr.GatewayJob{GatewayExecutionState: "completed", GatewayExecutionReason: reason, GatewayExecutionOutcome: "failed", SlackOutgoingMessages: []*jr.SlackOutgoingMessage{{SlackDeliveryState: delivery}}}, time.Now())
			if p.Phase != "not_submitted" || !p.Terminal || p.Submitted {
				t.Fatalf("local failure projected as remote completion: %+v", p)
			}
		}
	}
}

func TestObservationUsesSparseGraphWritesAndIgnoresMissingRequests(t *testing.T) {
	now := time.Unix(100, 0)
	for _, state := range []string{"unknown", "running", "completed"} {
		t.Run(state, func(t *testing.T) {
			var written []*jw.GatewayJob
			x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
				switch r.Target.Method {
				case "GET":
					in := r.Input.(*jr.Input)
					if in.ID != 7 {
						t.Fatal("wrong observation scope")
					}
					return &jr.Output{Data: []*jr.GatewayJob{{Id: 7, BackendObservedState: "running", GatewayExecutionOutcome: "completed"}}}, nil
				case "PATCH":
					written = r.Input.(*jw.Input).GatewayJobs
					return &jw.Output{}, nil
				}
				t.Fatal("unexpected graph")
				return nil, nil
			})}
			if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: state, At: now}, &Output{}); err != nil {
				t.Fatal(err)
			}
			if len(written) != 1 || written[0] == nil {
				t.Fatal("observation missing")
			}
			s := written[0]
			if s.Has.BackendObservedAt != (state != "unknown") || s.Has.GatewayExecutionOutcome != (state == "completed") || !s.Has.BackendObservationUncertain {
				t.Fatal("observation overwrites unrelated facts")
			}
		})
	}
	writes := 0
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			writes++
		}
		return &jr.Output{}, nil
	})}
	if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: "unknown", At: now}, &Output{}); err != nil || writes != 0 {
		t.Fatal("missing request was updated")
	}
	if err := x.recordObservation(context.Background(), &Input{ID: 7, Text: "invented", At: now}, &Output{}); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("unrecognized observation accepted")
	}
}

func TestMessageClaimSkipsFastAnswerAndProjectsBlockedWork(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*jw.GatewayJob
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*jw.Input).GatewayJobs
			return &jw.Output{}, nil
		}
		input := r.Input.(*jr.Input)
		if input.Mode != "status" || !input.Now.Equal(timestamp(now)) {
			t.Fatal("unbounded status read")
		}
		return &jr.Output{Data: []*jr.GatewayJob{
			{Id: 1, GatewayExecutionState: "completed", SlackProgressMode: "message", SlackProgressState: "ready"},
			{Id: 2, SlackWorkspaceId: "T", SlackChannelId: "C", SlackThreadRootMessageId: "root", GatewayExecutionState: "pending", Blocked: 1, Earlier: 1, SlackProgressMode: "message", SlackProgressState: "ready", SlackProgressRevision: 2},
		}}, nil
	})}
	got, err := x.claimMessageStatus(context.Background(), now, "urgent")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.JobID != 2 || got.Phase != "blocked" || got.Terminal || got.Revision != 3 {
		t.Fatalf("wrong status snapshot: %+v", got)
	}
	if len(written) != 2 || written[0].SlackProgressState != "skipped" || written[1].SlackProgressState != "sending" {
		t.Fatal("claim and skipped intention not written together")
	}
}

func TestMessageFinishRejectsStaleRevisionBeforeWriting(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*jw.GatewayJob
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*jw.Input).GatewayJobs
			return &jw.Output{}, nil
		}
		return &jr.Output{Data: []*jr.GatewayJob{{Id: 7, SlackProgressState: "sending", SlackProgressRevision: 3, SlackProgressMessageId: "1.001"}}}, nil
	})}
	in := &Input{Status: gateway.GatewayJobProgress{JobID: 7, Revision: 2, SlackTS: "1.001"}, StatusResult: gateway.StatusResult{State: "ready", SlackTS: "1.001"}, At: now}
	if err := x.finishStatus(context.Background(), in, &Output{}); !errors.Is(err, gateway.ErrConflict) || written != nil {
		t.Fatal("stale receipt persisted")
	}
	in.Status.Revision = 3
	if err := x.finishStatus(context.Background(), in, &Output{}); err != nil {
		t.Fatal(err)
	}
	s := written[0]
	if s.SlackProgressSentRevision != 3 || s.SlackProgressFirstSentAt == nil || !s.SlackProgressFirstSentAt.Equal(now) || s.SlackProgressLastSentAt == nil || !s.SlackProgressLastSentAt.Equal(now) {
		t.Fatal("message receipt facts missing")
	}
}

func TestSessionFinishReprojectsIntakeDuringInflightPublication(t *testing.T) {
	now := time.Unix(100, 0)
	var written []*sw.SlackAgentSessionPublication
	a := &sr.SlackAgentSessionPublication{Id: 42, InitialIntegrationId: 1, SlackAppId: "A", SlackWorkspaceId: "T", SlackChannelId: "C", SlackThreadRootMessageId: "root", StatusSourceJobId: 7, LatestStatusJobId: 8, SlackPublicationState: "sending", SlackAgentDesiredStatus: "processing", SlackPublicationRevision: 4, SlackPublicationClaimedRevision: 3, GatewayJobs: []*sr.GatewayJobSessionProjection{
		{Id: statusPointer(7), IntegrationId: statusPointer(1), IntegrationActive: statusPointer(1), GatewayExecutionState: statusPointer("completed"), SlackProgressMode: "hybrid", SlackPromptMessageId: statusPointer("7.001"), CreatedAt: statusPointer(time.UnixMilli(1))},
		{Id: statusPointer(8), IntegrationId: statusPointer(1), IntegrationActive: statusPointer(1), GatewayExecutionState: statusPointer("pending"), SlackProgressMode: "hybrid", SlackPromptMessageId: statusPointer("8.001"), CreatedAt: statusPointer(time.UnixMilli(2))},
	}}
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*sw.Input).Rows
			return &sw.Output{}, nil
		}
		return &sr.Output{Data: []*sr.SlackAgentSessionPublication{a}}, nil
	})}
	status := gateway.GatewayJobProgress{JobID: 7, ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "root"}, Revision: 3, SessionState: "processing", Target: gateway.StatusTargetAgentSession}
	result := gateway.StatusResult{State: "ready", Acknowledged: true, AgentStatus: "processing", SessionStatus: "processing"}
	if err := x.finishAgentSession(context.Background(), status, result, now); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 {
		t.Fatal("missing session receipt")
	}
	p := written[0]
	if p.Id != 42 || !p.Has.Id || p.SlackPublicationState != "ready" || p.StatusSourceJobId != 8 || p.SlackPublicationRevision != 5 || p.SlackPublicationSentRevision != 3 || p.Has.SlackPublicationClaimedRevision {
		t.Fatal("in-flight intake lost or acknowledgement adopted newer revision")
	}
}

func TestObservationRejectsStaleRemoteSuccessAfterLocalFailure(t *testing.T) {
	for _, reason := range []string{"backend_unavailable", "backend_blocked"} {
		t.Run(reason, func(t *testing.T) {
			writes := 0
			x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
				if r.Target.Method == "PATCH" {
					writes++
					return &jw.Output{}, nil
				}
				return &jr.Output{Data: []*jr.GatewayJob{{Id: 7, GatewayExecutionState: "completed", GatewayExecutionReason: reason, GatewayExecutionOutcome: "failed", SlackProgressState: "settled"}}}, nil
			})}
			err := x.recordObservation(context.Background(), &Input{ID: 7, Text: "completed", At: time.Now()}, &Output{})
			if !errors.Is(err, gateway.ErrBackendUnavailable) || writes != 0 {
				t.Fatalf("stale success crossed the local execution boundary: err=%v writes=%d", err, writes)
			}
		})
	}
}

func TestStatusReceiptKeepsNativeUTCMillisecondsAndSparseOwnership(t *testing.T) {
	now := time.Unix(100, 123456789).In(time.FixedZone("fixture", 7200))
	var written []*jw.GatewayJob
	x := &execution{invoke: statusInvoker(func(_ context.Context, r handler.ComponentRequest) (any, error) {
		if r.Target.Method == "PATCH" {
			written = r.Input.(*jw.Input).GatewayJobs
			return &jw.Output{}, nil
		}
		return &jr.Output{Data: []*jr.GatewayJob{{Id: 7, SlackProgressState: "sending", SlackProgressRevision: 3}}}, nil
	})}
	in := &Input{Status: gateway.GatewayJobProgress{JobID: 7, Revision: 3}, StatusResult: gateway.StatusResult{State: "settled", SlackTS: "1.001"}, At: now}
	if err := x.finishStatus(context.Background(), in, &Output{}); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 {
		t.Fatal("missing native receipt patch")
	}
	row := written[0]
	for _, at := range []*time.Time{row.SlackProgressFirstSentAt, row.SlackProgressLastSentAt, &row.GatewayStatusUpdatedAt} {
		if at == nil || !at.Equal(timestamp(now)) || at.Location() != time.UTC || at.Nanosecond()%int(time.Millisecond) != 0 {
			t.Fatalf("receipt moment was not normalized: %+v", at)
		}
	}
	if !row.Has.SlackProgressNextAttemptAt || row.SlackProgressNextAttemptAt != nil || !row.Has.SlackProgressRateLimitedUntil || row.SlackProgressRateLimitedUntil != nil {
		t.Fatal("absent deadlines were not explicitly cleared")
	}
	if row.Has.GatewayExecutionState || row.Has.GatewayExecutionUpdatedAt || row.Has.CreatedAt || row.Has.GatewayExecutionResultText {
		t.Fatal("status receipt overwrote unrelated request ownership")
	}
}
