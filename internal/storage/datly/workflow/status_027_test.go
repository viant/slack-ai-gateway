package workflow

import (
	"context"
	"testing"
	"time"

	jr "github.com/viant/slack-ai-gateway/internal/datly/read"
	jw "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/xdatly/handler"
)

func status027Job(now time.Time) *jr.GatewayJob {
	return &jr.GatewayJob{
		Id: 7, IntegrationActive: 1, GatewayExecutionState: "querying",
		BackendSubmissionStartedAt: statusPointer(now.Add(-time.Minute)), BackendObservedState: "running",
		BackendObservationUncertainSince: statusPointer(now.Add(-time.Second)),
		SlackProgressMode:                "message", SlackProgressState: "ready", SlackProgressPhase: "backend_recovering",
		SlackProgressMessageId: "7.001", SlackProgressRevision: 8, SlackProgressSentRevision: 8,
		SlackProgressRefreshAt: statusPointer(now.Add(10 * time.Second)),
	}
}

func status027Execution(t *testing.T, job *jr.GatewayJob, written *[]*jw.GatewayJob) *execution {
	t.Helper()
	return &execution{invoke: statusInvoker(func(_ context.Context, request handler.ComponentRequest) (any, error) {
		switch request.Target.Method {
		case "GET":
			return &jr.Output{Data: []*jr.GatewayJob{job}}, nil
		case "PATCH":
			*written = append(*written, request.Input.(*jw.Input).GatewayJobs...)
			return &jw.Output{}, nil
		default:
			t.Fatalf("unexpected status graph method %q", request.Target.Method)
			return nil, nil
		}
	})}
}

func TestStatus027CompletedObservationWaitsForDelivery(t *testing.T) {
	now := time.Unix(100, 0)
	for _, state := range []string{"processing", "querying"} {
		t.Run(state, func(t *testing.T) {
			job := status027Job(now)
			job.GatewayExecutionState, job.BackendObservedState = state, "completed"
			job.BackendObservationUncertainSince = nil
			projected := projectMessageStatus(job, now)
			if projected.Phase != "delivering" || projected.Terminal {
				t.Fatalf("confirmed response before outbox was not awaiting delivery: %+v", projected)
			}
			job.GatewayExecutionState, job.GatewayExecutionOutcome = "completed", "completed"
			job.SlackOutgoingMessages = []*jr.SlackOutgoingMessage{{SlackDeliveryState: "delivered"}, {SlackDeliveryState: "pending"}}
			projected = projectMessageStatus(job, now)
			if projected.Phase != "delivering" || projected.Terminal {
				t.Fatalf("one acknowledged part ended the response: %+v", projected)
			}
			job.SlackOutgoingMessages[1].SlackDeliveryState = "delivered"
			projected = projectMessageStatus(job, now)
			if projected.Phase != "completed" || !projected.Terminal {
				t.Fatalf("all acknowledged parts did not complete the response: %+v", projected)
			}
		})
	}
}

func TestStatus027OperationalRefreshRetainsBackendStatus(t *testing.T) {
	now := time.Unix(100, 0)
	for _, phase := range []string{"backend_recovering", "backend_blocked"} {
		for _, state := range []string{"processing", "querying"} {
			t.Run(phase+"/"+state, func(t *testing.T) {
				job := status027Job(now)
				job.SlackProgressPhase, job.GatewayExecutionState = phase, state
				var written []*jw.GatewayJob
				x := status027Execution(t, job, &written)
				for range 5 {
					if err := x.refreshMessageProgress(context.Background(), gateway.PhysicalThreadKey{}); err != nil {
						t.Fatal(err)
					}
				}
				if len(written) != 0 {
					t.Fatalf("operational refresh dirtied acknowledged backend status: %+v", written[0])
				}
			})
		}
	}
}

func TestStatus027CompletionPreemptsBackendOverlay(t *testing.T) {
	now := time.Unix(100, 0)
	for _, backendPhase := range []string{"backend_recovering", "backend_blocked"} {
		for _, observation := range []string{"completed", "failed", "canceled"} {
			t.Run(backendPhase+"/"+observation, func(t *testing.T) {
				job := status027Job(now)
				job.SlackProgressPhase, job.BackendObservedState = backendPhase, observation
				job.BackendObservationUncertainSince = nil
				want := observation
				if observation == "completed" {
					want = "delivering"
				}
				var written []*jw.GatewayJob
				x := status027Execution(t, job, &written)
				if err := x.refreshMessageProgress(context.Background(), gateway.PhysicalThreadKey{}); err != nil {
					t.Fatal(err)
				}
				if len(written) != 1 || written[0].SlackProgressPhase != want {
					t.Fatalf("confirmed outcome did not invalidate backend overlay: %+v", written)
				}
				written = nil
				claimed, err := x.claimMessageStatus(context.Background(), now, backendPhase)
				if err != nil || claimed == nil || claimed.Phase != want || claimed.Terminal != (observation != "completed") {
					t.Fatalf("backend availability hid known outcome: %+v, %v", claimed, err)
				}
			})
		}
	}
}

func TestStatus027HealthyClaimClearsBackendStatusBeforeRefresh(t *testing.T) {
	now := time.Unix(100, 0)
	for _, state := range []string{"unknown", "running", "queued", "completed"} {
		t.Run(state, func(t *testing.T) {
			job := status027Job(now)
			want := "checking"
			if state != "unknown" {
				job.BackendObservedState, job.BackendObservationUncertainSince = state, nil
				want = map[string]string{"running": "running", "queued": "backend_queued", "completed": "delivering"}[state]
			}
			var written []*jw.GatewayJob
			x := status027Execution(t, job, &written)
			claimed, err := x.claimMessageStatus(context.Background(), now, "urgent")
			if err != nil || claimed == nil || claimed.Phase != want {
				t.Fatalf("healthy manager left prior backend overlay until heartbeat: %+v, %v", claimed, err)
			}
		})
	}
}

func TestStatus027RecoveryReceiptPreservesCompletedIntent(t *testing.T) {
	now := time.Unix(100, 0)
	job := status027Job(now)
	job.SlackProgressState, job.BackendObservedState = "sending", "completed"
	job.BackendObservationUncertainSince = nil
	var written []*jw.GatewayJob
	x := status027Execution(t, job, &written)
	status := gateway.GatewayJobProgress{JobID: 7, Phase: "backend_recovering", Revision: 8, SlackTS: "7.001"}
	result := gateway.StatusResult{State: "ready", SlackTS: "7.001", NextRefreshAt: now.Add(10 * time.Second)}
	if err := x.finishStatus(context.Background(), &Input{Status: status, StatusResult: result, At: now}, &Output{}); err != nil {
		t.Fatal(err)
	}
	if len(written) != 1 {
		t.Fatalf("missing recovery receipt: %+v", written)
	}
	patch := written[0]
	if patch.SlackProgressPhase != "delivering" || patch.SlackProgressState != "ready" || patch.SlackProgressRevision != 9 || patch.SlackProgressSentRevision != 8 {
		t.Fatalf("recovery receipt lost newer completed intent or its revision fence: %+v", patch)
	}
}

func TestStatus027NativeOnlyKnownOutcomeDoesNotCreateFallback(t *testing.T) {
	now := time.Unix(100, 0)
	for _, state := range []string{"completed", "failed", "canceled"} {
		t.Run(state, func(t *testing.T) {
			job := status027Job(now)
			job.SlackProgressMode, job.SlackProgressMessageId = "agent_session", ""
			job.BackendObservedState, job.BackendObservationUncertainSince = state, nil
			var written []*jw.GatewayJob
			x := status027Execution(t, job, &written)
			claimed, err := x.claimMessageStatus(context.Background(), now, "backend_recovering")
			if err != nil || claimed != nil || len(written) != 0 {
				t.Fatalf("known native-only outcome created an ordinary message fallback: %+v, %v, writes=%d", claimed, err, len(written))
			}
		})
	}
}
