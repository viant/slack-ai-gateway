package slack

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	slackgo "github.com/slack-go/slack"
	convstore "github.com/viant/agently-core/app/store/conversation"
	conversationmodel "github.com/viant/agently-core/model/conversation"
	"github.com/viant/agently-core/sdk"
	presentation "github.com/viant/agently-core/sdk/slack"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type presentationCredentials struct{}

func (presentationCredentials) Token(context.Context, string) (string, error) {
	return "offline-test", nil
}

// Exercise the real HTTP SDK -> canonical turn -> worker -> SQLite -> native
// Slack sender path. Both HTTP peers are local test servers; no live calls.
func TestResponsePresentationThroughHTTPAndDurableOutbox(t *testing.T) {
	for _, mode := range []presentation.ResponseMode{presentation.ResponseRaw, presentation.ResponseCLICompatible, presentation.ResponseSummary, presentation.ResponseNormalizedSummary, presentation.ResponseReportText} {
		for _, recoverQuery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/recovery=%v", mode, recoverQuery), func(t *testing.T) {
				ctx := context.Background()
				path := filepath.Join(t.TempDir(), "gateway.sqlite")
				s, err := sqlite.Open(ctx, path, true)
				storetest.Must(t, err)
				defer func() { storetest.Must(t, s.Close()) }()
				storetest.Must(t, s.Migrate(ctx))
				reopen := func() {
					storetest.Must(t, s.Close())
					s, err = sqlite.Open(ctx, path, false)
					storetest.Must(t, err)
					storetest.Must(t, s.CheckSchema(ctx))
					storetest.Must(t, s.Recover(ctx))
				}
				content := strings.Repeat("Public conclusion. 🙂\n", 700) + "\n```forge-report\n" +
					`{"version":1,"id":"example","sequence":1,"mode":"start","grammar":"report-document-v1","title":"Example report","blocks":[{"id":"summary","kind":"markdownBlock","markdown":"Authored summary."},{"id":"details","kind":"markdownBlock","markdown":"Authored detail."}]}` +
					"\n```\n```forge-report\n" + `{"version":1,"id":"example","sequence":2,"mode":"commit"}` + "\n```"
				var finished atomic.Bool
				finished.Store(!recoverQuery)
				var request atomic.Value
				var creates, chats, attaches atomic.Int64
				backend := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
					switch r.Method + " " + r.URL.Path {
					case "POST /app/v1/conversations":
						creates.Add(1)
						fmt.Fprint(out, `{"Id":"example"}`)
					case "GET /app/v1/conversations/example":
						fmt.Fprint(out, `{"id":"example","aguiThreadId":"wire-thread"}`)
					case "POST /app/v1/ag-ui/run":
						var in struct {
							RunID     string `json:"runId"`
							Forwarded struct {
								Agently struct {
									Operation string `json:"operation"`
								} `json:"agently"`
							} `json:"forwardedProps"`
						}
						if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
							t.Error(err)
							return
						}
						if in.Forwarded.Agently.Operation == "chat" {
							chats.Add(1)
							request.Store(in.RunID)
						} else if in.Forwarded.Agently.Operation == "run.attach" {
							attaches.Add(1)
						} else {
							t.Errorf("unexpected operation %q", in.Forwarded.Agently.Operation)
						}
						out.Header().Set("Content-Type", "text/event-stream")
						fmt.Fprintf(out, "data: {\"type\":\"RUN_STARTED\",\"threadId\":\"wire-thread\",\"runId\":%q,\"protocolVersion\":\"1.0\",\"metadata\":{\"agently\":{\"identityVersion\":\"1\",\"nativeTurnId\":%q}}}\n\n", in.RunID, "native-"+in.RunID)
					case "GET /app/v1/conversations/example/transcript":
						id, _ := request.Load().(string)
						state := presentationCanonicalState("native-"+id, content, finished.Load())
						if err := json.NewEncoder(out).Encode(sdk.ConversationStateResponse{Conversation: state}); err != nil {
							t.Error(err)
						}

					default:
						t.Errorf("unexpected endpoint %s %s", r.Method, r.URL.Path)
						out.WriteHeader(404)
					}
				}))
				defer backend.Close()
				baseURL := backend.URL + "/app/"
				factory, err := agently.NewFactory(baseURL, backend.Client(), presentationCredentials{})
				storetest.Must(t, err)
				m := storetest.Message("example-event", "1.000001", "example-user")
				_, err = (gateway.Intake{Store: s, Route: testRoute}).Accept(ctx, m)
				storetest.Must(t, err)
				policy := gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "shared"}
				worker := gateway.Worker{Store: s, Clients: factory, Policy: policy, AgentID: "example", ResponseMode: mode, AgentlyBaseURL: baseURL,
					Timing: gateway.QueryTiming{PollInitial: time.Millisecond, Timeout: time.Second}}
				step := func() {
					worked, err := worker.Step(ctx)
					storetest.Must(t, err)
					if !worked {
						t.Fatal("expected work")
					}
				}
				step()
				if recoverQuery {
					status, err := s.Status(ctx, 10)
					storetest.Must(t, err)
					if len(status.Deliveries) != 0 {
						t.Fatal("nonterminal query published a reply")
					}
					reopen()
					worker.Store = s
					finished.Store(true)
					time.Sleep(3 * time.Millisecond)
					step()
				}
				want, err := presentation.PresentResponse(presentation.ResponseInput{Content: content, StandaloneMessages: []*sdk.TurnMessageState{{Content: "### Public findings\nIndependent public findings. ą🙂"}}, Mode: mode, ConversationURL: backend.URL + "/app/conversation/example"})
				storetest.Must(t, err)
				if !strings.HasPrefix(want.Text, "[View full response in Agently]("+backend.URL+"/app/conversation/example)\n\n### Public findings") || strings.Count(want.Text, "Independent public findings.") != 1 || strings.Count(want.Text, "Public conclusion.") != 700 {
					t.Fatal("link, public findings or final missing/duplicated")
				}
				for _, hidden := range []string{"EXECUTION_ONLY", "INTERNAL_ONLY", "TOOL_ONLY", "WRONG TURN"} {
					if strings.Contains(want.Text, hidden) {
						t.Fatal("nonpublic content exposed")
					}
				}
				parts := gateway.RenderMarkdown(want.Text)
				if len(parts) < 2 {
					t.Fatal("long answer did not exercise multipart delivery")
				}
				// Inspect the durable source directly; operational status intentionally
				// excludes result text and must continue to do so.
				db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
				storetest.Must(t, err)
				var stored string
				storetest.Must(t, db.QueryRowContext(ctx, "SELECT result_text FROM inbox_jobs WHERE event_id=?", m.EventID).Scan(&stored))
				storetest.Must(t, db.Close())
				if stored != content {
					t.Fatal("presentation replaced canonical result")
				}
				// A claim returned to pending before any POST is a safe retry. Its
				// persisted content survives restart and a different presentation mode.
				claimed, err := s.ClaimDelivery(ctx)
				storetest.Must(t, err)
				if claimed == nil || claimed.Text != parts[0].Text {
					t.Fatal("wrong persisted presentation")
				}
				storetest.Must(t, s.RetryDelivery(ctx, claimed.ID, time.Now()))
				reopen()
				worker.Store, worker.ResponseMode = s, presentation.ResponseReportText
				if mode == presentation.ResponseReportText {
					worker.ResponseMode = presentation.ResponseRaw
				}
				if worked, err := worker.Step(ctx); worked || err != nil {
					t.Fatalf("completed query repeated: %v %v", worked, err)
				}

				var mu sync.Mutex
				var sent []string
				slackServer := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/chat.postMessage" {
						t.Errorf("unexpected Slack path: %s", r.URL.Path)
					}
					if err := r.ParseForm(); err != nil {
						t.Error(err)
						return
					}
					if r.Form.Get("channel") != "C" || r.Form.Get("thread_ts") != m.RootTS || r.Form.Get("text") == "" {
						t.Error("missing delivery destination/fallback")
					}
					var blocks []struct{ Type, Text string }
					if err := json.Unmarshal([]byte(r.Form.Get("blocks")), &blocks); err != nil {
						t.Error(err)
						return
					}
					if len(blocks) != 1 || blocks[0].Type != "markdown" {
						t.Error("native markdown block changed")
						return
					}
					mu.Lock()
					sent = append(sent, blocks[0].Text)
					n := len(sent)
					mu.Unlock()
					fmt.Fprintf(out, `{"ok":true,"channel":"C","ts":"2.%06d"}`, n)
				}))
				defer slackServer.Close()
				sender := &Sender{api: slackgo.New("xoxb-test", slackgo.OptionAPIURL(slackServer.URL+"/"), slackgo.OptionHTTPClient(slackServer.Client())), route: testRoute}
				delivery := gateway.DeliveryWorker{Store: s, Sender: sender, Policy: policy}
				for range parts {
					worked, err := delivery.Step(ctx)
					storetest.Must(t, err)
					if !worked {
						t.Fatal("delivery missing")
					}
				}
				if worked, err := delivery.Step(ctx); worked || err != nil {
					t.Fatal("unexpected extra delivery")
				}
				mu.Lock()
				defer mu.Unlock()
				if len(sent) != len(parts) {
					t.Fatal("wrong number of Slack posts")
				}
				joined := strings.Join(sent, "\n")
				if !strings.HasPrefix(sent[0], "[View full response in Agently]("+backend.URL+"/app/conversation/example)") || strings.Count(joined, backend.URL+"/app/conversation/example") != 1 || strings.Count(joined, "Independent public findings.") != 1 || strings.Count(joined, "Public conclusion.") != 700 {
					t.Fatal("delivered envelope or canonical messages changed")
				}
				for _, hidden := range []string{"EXECUTION_ONLY", "INTERNAL_ONLY", "TOOL_ONLY", "WRONG TURN"} {
					if strings.Contains(joined, hidden) {
						t.Fatal("delivered nonpublic content")
					}
				}
				for i, text := range sent {
					if text != parts[i].Text {
						t.Fatalf("delivery %d was reformatted", i)
					}
				}
				if creates.Load() != 1 || chats.Load() != 1 || (attaches.Load() != 0) != recoverQuery {
					t.Fatalf("repeated/missing Agently calls: create=%d chat=%d attach=%d", creates.Load(), chats.Load(), attaches.Load())
				}
			})
		}
	}
}

// Build the HTTP response through the native producer rather than a second
// hand-maintained definition of canonical ownership or message ordering.
func presentationCanonicalState(turnID, final string, completed bool) *sdk.ConversationState {
	task, internal := "task", "router"
	narration, note, hidden, tool := "EXECUTION_ONLY", "### Public findings\nIndependent public findings. ą🙂", "INTERNAL_ONLY", "TOOL_ONLY"
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	one, two := 1, 2
	status := "running"
	if completed {
		status = "succeeded"
	}
	messages := []*conversationmodel.MessageView{
		{Id: "model-first", Role: "assistant", Mode: &task, Content: &narration, CreatedAt: now, Iteration: &one, ModelCall: &conversationmodel.ModelCallView{MessageId: "model-first", Status: "completed"}, ToolMessage: []*conversationmodel.ToolMessageView{{Id: "tool", Content: &tool, ToolCall: &conversationmodel.ToolCallView{OpId: "call", ToolName: "example/read", Status: "completed"}}}},
		{Id: "public-note", Role: "assistant", Mode: &task, Content: &note, CreatedAt: now.Add(time.Second)},
		{Id: "internal-note", Role: "assistant", Mode: &internal, Content: &hidden, CreatedAt: now.Add(2 * time.Second)},
		{Id: "model-final", Role: "assistant", Mode: &task, Content: &final, CreatedAt: now.Add(3 * time.Second), Iteration: &two, ModelCall: &conversationmodel.ModelCallView{MessageId: "model-final", Status: status}},
	}
	state := sdk.BuildCanonicalState("example", convstore.Transcript{{Id: turnID, ConversationId: "example", Status: status, Message: messages}})
	state.Turns = append(state.Turns, &sdk.TurnState{TurnID: "unrelated", Status: sdk.TurnStatusCompleted,
		Messages:  []*sdk.TurnMessageState{{MessageID: "foreign-note", Role: "assistant", Content: "WRONG TURN"}},
		Assistant: &sdk.AssistantState{Final: &sdk.AssistantMessageState{MessageID: "foreign-final", Content: "WRONG TURN"}},
	})
	return state
}
