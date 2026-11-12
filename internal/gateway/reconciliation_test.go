package gateway_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-core/sdk"
	slackpresentation "github.com/viant/agently-core/sdk/slack"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type testCredentials struct{}

func (testCredentials) Token(_ context.Context, p string) (string, error) {
	if p != "shared" {
		return "", errors.New("wrong principal")
	}
	return "offline-test", nil
}

func TestHTTPQueryReconciliationAcrossRestart(t *testing.T) {
	for _, mode := range []string{"sync", "queued", "lost_post_result", "empty_stream", "observer_canceled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			observerCtx, stopObserver := context.WithCancel(ctx)
			defer stopObserver()
			s, reopen := database(t)
			m := storetest.Message("a", "123.456", "alice")
			accept(t, s, m)
			var creates, queries, attaches, reads atomic.Int64
			var finished atomic.Bool
			if mode == "sync" {
				finished.Store(true)
			}
			var mu sync.Mutex
			var requestID string
			queryDone := make(chan struct{})
			writeIdentity := func(out http.ResponseWriter, runID string) {
				out.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(out, "id: opaque\ndata: {\"type\":\"RUN_STARTED\",\"threadId\":\"wire-thread\",\"runId\":%q,\"protocolVersion\":\"1.0\",\"metadata\":{\"agently\":{\"identityVersion\":\"1\",\"nativeTurnId\":%q}}}\n\n", runID, "native-"+runID)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/conversations":
					creates.Add(1)
					fmt.Fprint(out, `{"Id":"c"}`)
				case "GET /v1/conversations/c":
					if r.URL.RawQuery != "includeTranscript=false" {
						t.Error("unexpected metadata query")
					}
					fmt.Fprint(out, `{"id":"c","aguiThreadId":"wire-thread"}`)
				case "POST /v1/ag-ui/run":
					var in struct {
						ThreadID string `json:"threadId"`
						RunID    string `json:"runId"`
						Messages []struct {
							ID      string `json:"id"`
							Role    string `json:"role"`
							Content string `json:"content"`
						} `json:"messages"`
						Forwarded struct {
							Agently struct {
								Version   string            `json:"version"`
								Operation string            `json:"operation"`
								RequestID string            `json:"requestId"`
								Target    map[string]string `json:"target"`
								Payload   map[string]any    `json:"payload"`
							} `json:"agently"`
						} `json:"forwardedProps"`
					}
					decoder := json.NewDecoder(r.Body)
					decoder.DisallowUnknownFields()
					if err := decoder.Decode(&in); err != nil {
						t.Error(err)
						return
					}
					ext := in.Forwarded.Agently
					if r.Header.Get("Accept") != "text/event-stream" || in.ThreadID != "wire-thread" || in.RunID == "" || ext.Version != "1" || ext.RequestID != in.RunID {
						t.Error("invalid AG-UI envelope")
					}
					if ext.Operation == "run.attach" {
						attaches.Add(1)
						mu.Lock()
						id := requestID
						mu.Unlock()
						if in.RunID != id || len(in.Messages) != 0 || len(ext.Payload) != 0 || len(ext.Target) != 1 || ext.Target["threadId"] != "wire-thread" {
							t.Error("attachment changed or replayed the original question")
						}
						writeIdentity(out, in.RunID)
						return
					}
					queries.Add(1)
					defer close(queryDone)
					if ext.Operation != "chat" || len(ext.Target) != 0 || len(ext.Payload) != 2 || ext.Payload["agentId"] != "steward" || ext.Payload["useServerState"] != true || len(in.Messages) != 1 || in.Messages[0].ID != in.RunID || in.Messages[0].Role != "user" || in.Messages[0].Content != m.Text {
						t.Error("chat input changed or omitted server history")
					}
					mu.Lock()
					requestID = in.RunID
					mu.Unlock()
					// Assert correlation and conversation are durable BEFORE this POST.
					status, err := s.Status(ctx, 10)
					if err != nil {
						t.Error(err)
						return
					}
					if len(status.GatewayJobs) != 1 || status.GatewayJobs[0].GatewayRequestID != in.RunID || status.GatewayJobs[0].BackendSubmissionStartedAt == 0 || status.ConversationBindings[0].ConversationID != "c" || status.ConversationBindings[0].OwnerID != "shared" {
						t.Error("side effect preceded durable correlation")
					}
					if mode == "lost_post_result" {
						out.WriteHeader(503)
						return
					}
					if mode == "observer_canceled" {
						// Cancel only after the admitted POST and durable identity
						// exist; database admission time must not determine this test.
						stopObserver()
						<-r.Context().Done()
						return
					}
					if mode == "empty_stream" {
						out.Header().Set("Content-Type", "text/event-stream")
						return
					}
					writeIdentity(out, in.RunID)
				case "GET /v1/conversations/c/transcript":
					reads.Add(1)
					mu.Lock()
					id := "native-" + requestID
					mu.Unlock()
					state := "queued"
					if finished.Load() {
						state = "completed"
					}
					fmt.Fprintf(out, `{"conversation":{"conversationId":"c","turns":[{"turnId":%q,"startedByMessageId":"user-message-1","user":{"messageId":"user-message-1"},"status":%q,"assistant":{"final":{"content":"verified answer"}}},{"turnId":"unrelated","status":"completed","assistant":{"final":{"content":"WRONG"}}}]}}`, id, state)
				default:
					t.Errorf("unexpected endpoint %s %s", r.Method, r.URL.Path)
					out.WriteHeader(404)
				}
			}))
			defer srv.Close()
			factory, err := agently.NewFactory(srv.URL, srv.Client(), testCredentials{})
			storetest.Must(t, err)
			w := gateway.Worker{Store: s, Policy: policy(), Clients: factory, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Millisecond, Timeout: time.Second}}
			if mode == "observer_canceled" {
				// Stopping the observer does not cancel or resubmit durable execution.
				worked, err := w.Step(observerCtx)
				storetest.Must(t, err)
				if !worked {
					t.Fatal("query not admitted")
				}
			} else {
				work(t, w)
			}
			select { // Handler no longer accesses s when we close/reopen it.
			case <-queryDone:
			case <-time.After(5 * time.Second):
				t.Fatal("chat was never submitted or its handler did not finish")
			}
			if mode != "sync" {
				status, err := s.Status(ctx, 10)
				storetest.Must(t, err)
				if status.GatewayJobs[0].State != "querying" || len(status.SlackOutgoingMessages) != 0 {
					t.Fatal("nonterminal query published a reply")
				}
			}
			s = reopen()
			w.Store = s
			if !accept(t, s, m).Duplicate {
				t.Fatal("duplicate after restart")
			}
			finished.Store(true)
			if mode != "sync" {
				time.Sleep(3 * time.Millisecond)
				work(t, w)
			}
			if ok, err := w.Step(ctx); ok || err != nil {
				t.Fatal("completed query replayed")
			}
			sender := &fakeSlack{}
			delivery := gateway.DeliveryWorker{Store: s, Sender: sender}
			if ok, err := delivery.Step(ctx); !ok || err != nil {
				t.Fatalf("delivery: %v %v", ok, err)
			}
			msgs := sender.snapshot()
			if len(msgs) != 1 || msgs[0].Text != "verified answer" || msgs[0].RootTS != m.RootTS {
				t.Fatalf("wrong reply: %+v", msgs)
			}
			if creates.Load() != 1 || queries.Load() != 1 || reads.Load() < 1 {
				t.Fatalf("repeated POSTs or no reads: %d %d %d", creates.Load(), queries.Load(), reads.Load())
			}
			wantAttaches := int64(1)
			if mode == "sync" {
				wantAttaches = 0
			}
			if attaches.Load() != wantAttaches {
				t.Fatalf("attachments = %d, want %d", attaches.Load(), wantAttaches)
			}
		})
	}
}

func TestHTTPPresentationPersistsSlackDeduplicationAcrossRestart(t *testing.T) {
	for _, mode := range []slackpresentation.ResponseMode{
		slackpresentation.ResponseRaw, slackpresentation.ResponseCLICompatible, slackpresentation.ResponseSummary,
		slackpresentation.ResponseNormalizedSummary, slackpresentation.ResponseReportText,
	} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			s, reopen := database(t)
			message := storetest.Message("presentation", "123.456", "alice")
			accept(t, s, message)
			const report = "```forge-report\n" + `{"version":1,"id":"brief","sequence":1,"mode":"start","grammar":"report-document-v1","title":"Example report","blocks":[{"id":"summary","kind":"markdownBlock","markdown":"Authored conclusion."}]}` + "\n```\n" +
				"```forge-report\n" + `{"version":1,"id":"brief","sequence":2,"mode":"commit"}` + "\n```"
			var nativeID atomic.Value
			var queries atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
				switch r.Method + " " + r.URL.Path {
				case "POST /v1/conversations":
					fmt.Fprint(out, `{"Id":"c"}`)
				case "GET /v1/conversations/c":
					fmt.Fprint(out, `{"id":"c","aguiThreadId":"wire-thread"}`)
				case "POST /v1/ag-ui/run":
					queries.Add(1)
					var in struct {
						RunID string `json:"runId"`
					}
					if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
						t.Error(err)
						out.WriteHeader(400)
						return
					}
					id := "native-" + in.RunID
					nativeID.Store(id)
					out.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(out, "data: {\"type\":\"RUN_STARTED\",\"threadId\":\"wire-thread\",\"runId\":%q,\"protocolVersion\":\"1.0\",\"metadata\":{\"agently\":{\"identityVersion\":\"1\",\"nativeTurnId\":%q}}}\n\n", in.RunID, id)
				case "GET /v1/conversations/c/transcript":
					turn := &sdk.TurnState{TurnID: nativeID.Load().(string), Status: "completed", Messages: []*sdk.TurnMessageState{
						{MessageID: "findings", Role: "assistant", Content: "### Key findings\nPublic findings."},
						{MessageID: "findings", Role: "assistant", Content: "Later duplicate."},
						{MessageID: "internal", Role: "assistant", Mode: "router", Content: "Internal routing."},
						{MessageID: "narration", Role: "assistant", Content: "Execution narration."},
						{MessageID: "final", Role: "assistant", Content: report},
					}, Assistant: &sdk.AssistantState{
						Narration: &sdk.AssistantMessageState{MessageID: "narration", Content: "Execution narration."},
						Final:     &sdk.AssistantMessageState{MessageID: "final", Content: report},
					}}
					if err := json.NewEncoder(out).Encode(&sdk.ConversationStateResponse{Conversation: &sdk.ConversationState{
						ConversationID: "c", Turns: []*sdk.TurnState{turn},
					}}); err != nil {
						t.Error(err)
					}
				default:
					t.Errorf("unexpected endpoint %s %s", r.Method, r.URL.Path)
					out.WriteHeader(404)
				}
			}))
			defer srv.Close()
			factory, err := agently.NewFactory(srv.URL, srv.Client(), testCredentials{})
			storetest.Must(t, err)
			work(t, gateway.Worker{Store: s, Policy: policy(), Clients: factory, AgentID: "steward", ResponseMode: mode, AgentlyBaseURL: srv.URL})
			s = reopen() // Deliver stored presentation without another API call.
			status, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			if len(status.GatewayJobs) != 1 || status.GatewayJobs[0].State != "completed" {
				t.Fatal("query was not completed before restart")
			}
			sender := &fakeSlack{}
			delivery := gateway.DeliveryWorker{Store: s, Sender: sender}
			worked, err := delivery.Step(ctx)
			storetest.Must(t, err)
			messages := sender.snapshot()
			if !worked || len(messages) != 1 {
				t.Fatalf("missing persisted delivery: %+v", messages)
			}
			got := messages[0]
			prefix := "[View full response in Agently](" + srv.URL + "/conversation/c)\n\n### Key findings\nPublic findings.\n\n"
			if !strings.HasPrefix(got.Text, prefix) || strings.Count(got.Text, "Public findings.") != 1 || strings.Contains(got.Text, "Later duplicate.") || strings.Contains(got.Text, "Internal routing.") || strings.Contains(got.Text, "Execution narration.") {
				t.Fatalf("wrong public presentation: %q", got.Text)
			}
			if got.RootTS != message.RootTS || got.Format != gateway.ReplyMarkdown || queries.Load() != 1 {
				t.Fatal("delivery changed destination, format or resubmitted query")
			}
			if mode == slackpresentation.ResponseRaw {
				if !strings.HasSuffix(got.Text, report) {
					t.Fatal("raw final changed")
				}
			} else if strings.Contains(got.Text, "forge-report") || strings.Count(got.Text, "Authored conclusion.") != 1 {
				t.Fatalf("report presentation lost text or leaked authoring: %q", got.Text)
			}
		})
	}
}

type reconcilingBackend struct {
	fakeAgently
	readCalls int
	readState string
	readError error
}
type reconcilingClient struct {
	*fakeClient
	backend *reconcilingBackend
}

func (f *reconcilingBackend) ForPrincipal(_ context.Context, p string) (gateway.AgentlyClient, error) {
	return &reconcilingClient{fakeClient: &fakeClient{backend: &f.fakeAgently, principal: p}, backend: f}, nil
}
func (c *reconcilingClient) ReadTurn(_ context.Context, _, id string, _ func(gateway.Turn) error) (gateway.Turn, error) {
	c.backend.readCalls++
	return gateway.Turn{ID: id, State: c.backend.readState, Content: "read answer"}, c.backend.readError
}

func TestPollingReviewAndDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason string
		readErr             error
	}{
		{"running", "running", "", nil},
		{"unknown", "unknown", "", nil},
		{"transient", "", "", errors.New("temporary secret-marker")},
		{"completed", "completed", "", nil},
		{"failed", "failed", "", nil},
		{"canceled", "canceled", "", nil},
		{"waiting", "waiting_for_user", "interaction_requires_review", nil},
		{"permanent", "", "backend_protocol_or_access_error", gateway.ErrBackendProtocol},
		{"unsupported", "future_state", "unsupported_turn_state", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, reopen := database(t)
			backend := &reconcilingBackend{fakeAgently: fakeAgently{state: "queued"}, readState: tc.state, readError: tc.readErr}
			accept(t, s, storetest.Message("a", "one", "alice"))
			w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Millisecond}}
			work(t, w)
			s = reopen()
			w.Store = s
			time.Sleep(3 * time.Millisecond)
			work(t, w)
			status, err := s.Status(context.Background(), 10)
			storetest.Must(t, err)
			j := status.GatewayJobs[0]
			if tc.reason != "" {
				if j.State != "needs_review" || j.Reason != tc.reason || len(status.SlackOutgoingMessages) != 0 {
					t.Fatalf("bad review: %+v", status)
				}
			} else if tc.state == "completed" || tc.state == "failed" || tc.state == "canceled" {
				if j.State != "completed" || len(status.SlackOutgoingMessages) != 1 {
					t.Fatal("terminal result not delivered")
				}
			} else {
				if j.State != "querying" || j.PollAttempts != 2 || len(status.SlackOutgoingMessages) != 0 {
					t.Fatal("transient read not rescheduled")
				}
			}
			_, queries := backend.snapshot()
			if len(queries) != 1 || backend.readCalls != 1 {
				t.Fatal("unexpected query/read count")
			}
		})
	}
	t.Run("deadline_does_not_reset_after_restart", func(t *testing.T) {
		s, reopen := database(t)
		backend := &reconcilingBackend{fakeAgently: fakeAgently{state: "queued"}, readState: "running"}
		accept(t, s, storetest.Message("a", "one", "alice"))
		w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Millisecond, MaxWait: 200 * time.Millisecond}}
		work(t, w)
		s = reopen()
		w.Store = s
		time.Sleep(250 * time.Millisecond)
		work(t, w)
		status, err := s.Status(context.Background(), 10)
		storetest.Must(t, err)
		if status.GatewayJobs[0].Reason != "reconciliation_deadline" || backend.readCalls != 0 {
			t.Fatal("deadline ignored or reset")
		}
		if len(status.SlackOutgoingMessages) != 0 {
			t.Fatal("timeout incorrectly published as completion")
		}
	})
}

func TestCanceledWorkerPersistsReconciliation(t *testing.T) {
	s, reopen := database(t)
	backend := &reconcilingBackend{
		fakeAgently: fakeAgently{started: make(chan gateway.Query, 1), release: make(chan struct{})},
		readState:   "completed",
	}
	accept(t, s, storetest.Message("a", "one", "alice"))
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward", Timing: gateway.QueryTiming{PollInitial: time.Millisecond}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := w.Step(ctx)
		done <- err
	}()
	select {
	case <-backend.started:
	case <-time.After(5 * time.Second):
		t.Fatal("query did not start")
	}
	cancel()
	storetest.Must(t, <-done)
	status, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if status.GatewayJobs[0].State != "querying" || status.GatewayJobs[0].PollAttempts != 1 || len(status.SlackOutgoingMessages) != 0 {
		t.Fatal("cancellation prevented durable reconciliation")
	}
	s = reopen()
	w.Store = s
	time.Sleep(3 * time.Millisecond)
	work(t, w)
	_, queries := backend.snapshot()
	if len(queries) != 1 || backend.readCalls != 1 {
		t.Fatal("canceled query was resubmitted")
	}
}

func TestDeliveryResolutionNeverRepeatsQuery(t *testing.T) {
	for _, action := range []string{"delivered", "retry"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			s, reopen := database(t)
			backend := &fakeAgently{}
			accept(t, s, storetest.Message("a", "one", "alice"))
			w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
			work(t, w)
			sender := &fakeSlack{err: errors.New("lost result")}
			dw := gateway.DeliveryWorker{Store: s, Sender: sender}
			if ok, err := dw.Step(ctx); !ok || err == nil {
				t.Fatal("expected uncertain send")
			}
			s = reopen()
			w.Store = s
			dw.Store = s
			status, err := s.Status(ctx, 10)
			storetest.Must(t, err)
			id := status.SlackOutgoingMessages[0].ID
			ts := ""
			if action == "delivered" {
				ts = "123.00001"
			}
			storetest.Must(t, s.ResolveDelivery(ctx, id, action, ts, action == "retry"))
			if ok, err := w.Step(ctx); ok || err != nil {
				t.Fatal("resolution replayed query")
			}
			sender.err = nil
			ok, err := dw.Step(ctx)
			storetest.Must(t, err)
			if ok != (action == "retry") {
				t.Fatal("incorrect resolution")
			}
			_, queries := backend.snapshot()
			if len(queries) != 1 {
				t.Fatal("query repeated")
			}
			messages := sender.snapshot()
			if action == "retry" && (len(messages) != 2 || messages[0].Text != messages[1].Text || messages[0].ID != messages[1].ID) {
				t.Fatal("persisted part changed")
			}
			if strings.Contains(status.GatewayJobs[0].Reason, "lost result") {
				t.Fatal("raw error persisted")
			}
		})
	}
}
