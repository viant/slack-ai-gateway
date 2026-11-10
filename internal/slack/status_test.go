package slack

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/slack-go/slack"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestStatusUpdateHTTPContractAndClassification(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		code       int
		retry      string
		kind       gateway.SendErrorKind
	}{
		{"success", `{"ok":true,"channel":"C","ts":"2.000001","text":"updated"}`, 200, "", ""},
		{"deleted", `{"ok":false,"error":"message_not_found"}`, 200, "", gateway.SendRejected},
		{"not_owned", `{"ok":false,"error":"cant_update_message"}`, 200, "", gateway.SendRejected},
		{"auth", `{"ok":false,"error":"token_revoked"}`, 200, "", gateway.SendCredentials},
		{"limit", `{}`, 429, "7", gateway.SendRateLimited},
		{"server", `private-body`, 503, "", gateway.SendUnknown},
		{"malformed", `private-body`, 200, "", gateway.SendUnknown},
		{"wrong_ts", `{"ok":true,"channel":"C","ts":"999.000001"}`, 200, "", gateway.SendUnknown},
		{"wrong_channel", `{"ok":true,"channel":"OTHER","ts":"2.000001"}`, 200, "", gateway.SendUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				r.ParseForm()
				if r.URL.Path != "/chat.update" || r.Form.Get("channel") != "C" || r.Form.Get("ts") != "2.000001" || r.Form.Get("thread_ts") != "" || r.Form.Get("text") != "status &amp;" || r.Form.Get("mrkdwn") != "false" || r.Form.Get("parse") != "none" {
					t.Error("unsafe update parameters")
				}
				if tc.retry != "" {
					w.Header().Set("Retry-After", tc.retry)
				}
				w.WriteHeader(tc.code)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client())), route: testRoute}
			status := gateway.RequestStatus{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}, SlackTS: "2.000001"}
			ts, err := sender.SendStatus(context.Background(), status, "status &amp;")
			if tc.kind == "" {
				if err != nil || ts.SlackTS != status.SlackTS {
					t.Fatal("update success lost")
				}
			} else {
				var classified *gateway.SendError
				if !errors.As(err, &classified) || classified.Kind != tc.kind {
					t.Fatalf("wrong update classification: %v", err)
				}
				if tc.kind == gateway.SendRateLimited && classified.RetryAfter != 7*time.Second {
					t.Fatal("lost Retry-After")
				}
			}
			if calls.Load() != 1 {
				t.Fatal("SDK retried an update")
			}
			status.ChannelID = "OTHER"
			if _, err = sender.SendStatus(context.Background(), status, "status"); err == nil || calls.Load() != 1 {
				t.Fatal("update bypassed channel guard")
			}
		})
	}
}

func TestStatusFirstPostIsThreaded(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		r.ParseForm()
		if r.URL.Path != "/chat.postMessage" || r.Form.Get("thread_ts") != "1.000001" || r.Form.Get("reply_broadcast") == "true" {
			t.Error("status not posted privately within thread")
		}
		io.WriteString(w, `{"ok":true,"channel":"C","ts":"2.000001"}`)
	}))
	defer server.Close()
	sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/")), route: testRoute}
	ts, err := sender.SendStatus(context.Background(), gateway.RequestStatus{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}}, "Received your request.")
	if err != nil || ts.SlackTS != "2.000001" || calls.Load() != 1 {
		t.Fatal("initial status failed")
	}
}

// Use the real versioned SDK, with HTTP redirected only at its native API URL.
func TestAgentSessionSDKContract(t *testing.T) {
	for _, mode := range []gateway.StatusMode{gateway.StatusAgentSession, gateway.StatusHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			for _, surface := range []gateway.MessageSurface{gateway.SurfaceChannel, gateway.SurfaceDirectMessage} {
				t.Run(string(surface), func(t *testing.T) {
					for _, tc := range []struct {
						name, body string
						code       int
						kind       gateway.SendErrorKind
					}{
						{"own_and_aggregate", `{"ok":true,"agent_status":"active","status":"processing","response_metadata":{"warnings":["missing_agent_session_stopped_event_subscription"]}}`, 200, ""},
						{"native_lenient_success", `{"ok":true}`, 200, ""},
						{"feature_disabled", `{"ok":false,"error":"feature_disabled"}`, 200, gateway.SendRejected},
						{"missing_scope", `{"ok":false,"error":"missing_scope"}`, 200, gateway.SendRejected},
						{"auth", `{"ok":false,"error":"invalid_auth"}`, 200, gateway.SendCredentials},
						{"rate_limit", `{}`, 429, gateway.SendRateLimited},
						{"ambiguous", `{"ok":false,"error":"internal_error"}`, 200, gateway.SendUnknown},
						{"malformed", `secret-body`, 200, gateway.SendUnknown},
					} {
						t.Run(tc.name, func(t *testing.T) {
							channel := "C"
							if surface == gateway.SurfaceDirectMessage {
								channel = "D"
							}
							calls := 0
							server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								calls++
								r.ParseForm()
								if r.URL.Path != "/agents.sessions.setStatus" || r.Form.Get("channel_id") != channel || r.Form.Get("thread_ts") != "1.000001" || r.Form.Get("status") != "active" || r.Form.Get("initiator_user_id") != "alice" || r.Form.Get("text") != "" || r.Form.Get("title") != "" {
									t.Error("wrong native parameters")
								}
								w.Header().Set("Retry-After", "37")
								w.WriteHeader(tc.code)
								io.WriteString(w, tc.body)
							}))
							defer server.Close()
							route := testRoute
							if surface == gateway.SurfaceDirectMessage {
								route = dmRoute(t)
							}
							sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/")), route: route}
							status := gateway.RequestStatus{Mode: mode, Target: gateway.StatusTargetAgentSession, Surface: surface, SessionState: "active", InitiatorUserID: "alice", ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: channel, RootTS: "1.000001"}, MessageTS: "9.000001"}
							receipt, err := sender.SendStatus(context.Background(), status, "")
							if tc.kind == "" {
								if err != nil || !receipt.Acknowledged || receipt.SlackTS != "" {
									t.Fatalf("ack lost: %+v %v", receipt, err)
								}
								if tc.name == "own_and_aggregate" && (receipt.AgentStatus != "active" || receipt.SessionStatus != "processing" || receipt.Warning != "missing_agent_session_stopped_event_subscription") {
									t.Fatal("SDK metadata lost")
								}
							} else {
								var classified *gateway.SendError
								if !errors.As(err, &classified) || classified.Kind != tc.kind || receipt.Acknowledged {
									t.Fatalf("wrong classification: %v", err)
								}
								if tc.kind == gateway.SendRateLimited && classified.RetryAfter != 37*time.Second {
									t.Fatal("Retry-After lost")
								}
								if tc.name == "feature_disabled" && classified.Code != tc.name {
									t.Fatal("bounded error code lost")
								}
							}
							if calls != 1 {
								t.Fatal("unexpected SDK retry")
							}
							status.WorkspaceID = "OTHER"
							if _, err = sender.SendStatus(context.Background(), status, ""); err == nil || calls != 1 {
								t.Fatal("cross-workspace status allowed")
							}
						})
					}
				})
			}
		})
	}
}

func TestHybridMessageTargetUsesExistingSDKMessageMethods(t *testing.T) {
	for _, timestamp := range []string{"", "2.000001"} {
		t.Run("existing_ts_"+timestamp, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				r.ParseForm()
				wantPath := "/chat.update"
				if timestamp == "" {
					wantPath = "/chat.postMessage"
					if r.Form.Get("thread_ts") != "1.000001" {
						t.Error("hybrid message posted outside the original thread")
					}
				}
				if r.URL.Path != wantPath || r.Form.Get("channel") != "C" || r.Form.Get("text") != "Agently is working. Elapsed: 3s." || r.Form.Get("ts") != timestamp {
					t.Error("hybrid message target used wrong SDK method or identifiers")
				}
				io.WriteString(w, `{"ok":true,"channel":"C","ts":"2.000001"}`)
			}))
			defer server.Close()
			sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/")), route: testRoute}
			receipt, err := sender.SendStatus(context.Background(), gateway.RequestStatus{Mode: gateway.StatusHybrid, Target: gateway.StatusTargetMessage, ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}, SlackTS: timestamp}, "Agently is working. Elapsed: 3s.")
			if err != nil || receipt.SlackTS != "2.000001" || calls != 1 {
				t.Fatalf("message receipt lost: %+v %v", receipt, err)
			}
		})
	}
}
