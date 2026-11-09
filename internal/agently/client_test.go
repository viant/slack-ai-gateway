package agently

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
)

type tokenFunc func(context.Context, string) (string, error)

func (f tokenFunc) Token(ctx context.Context, p string) (string, error) { return f(ctx, p) }

func testClient(t *testing.T, handler http.HandlerFunc) gateway.AgentlyClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	f, err := NewFactory(srv.URL, srv.Client(), tokenFunc(func(_ context.Context, principal string) (string, error) {
		if principal != "shared" {
			t.Errorf("unexpected principal %q", principal)
		}
		return "test-credential", nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.ForPrincipal(context.Background(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSessionMutationOriginWithHTTPSAndBasePath(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			var origin string
			var calls atomic.Int64
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.URL.Path != "/agently/v1/operation" || r.Method != method {
					t.Error("wrong destination or method")
				}
				want := origin
				if method == http.MethodGet {
					want = ""
				}
				if r.Header.Get("Origin") != want {
					t.Errorf("Origin = %q, want %q", r.Header.Get("Origin"), want)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer srv.Close()
			origin = srv.URL
			factory, err := NewFactory(srv.URL+"/agently", srv.Client(), unusedCredentials{})
			if err != nil {
				t.Fatal(err)
			}
			factory.session = &sessionIdentity{principal: "shared", agent: "steward"}
			client := &Client{factory: factory, principal: "shared"}
			if err := client.do(context.Background(), method, "/v1/operation", nil, nil); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("request not sent exactly once")
			}
		})
	}
}

func TestWireContractAndExactTurn(t *testing.T) {
	var creates, queries, reads atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-credential" || r.Header.Get("Cookie") != "" {
			t.Error("wrong authentication")
		}
		if r.Header.Get("Origin") != "" {
			t.Error("cookie origin header added to bearer request")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/conversations":
			creates.Add(1)
			var in map[string]any
			if json.NewDecoder(r.Body).Decode(&in) != nil || len(in) != 1 || in["agentId"] != "steward" {
				t.Errorf("bad create contract: %v", in)
			}
			fmt.Fprint(w, `{"Id":"conversation-1","CreatedByUserId":"backend-user"}`)
		case "GET /v1/conversations/conversation-1":
			fmt.Fprint(w, `{"id":"conversation-1"}`)
		case "POST /v1/ag-ui/run":
			queries.Add(1)
			checkRunInput(t, r, "conversation-1", "request-1", "chat", "steward", "hello")
			writeRunStarted(w, "conversation-1", "request-1", "native-turn-1")
		case "GET /v1/conversations/conversation-1/transcript":
			reads.Add(1)
			if r.URL.RawQuery != "" {
				t.Error("unexpected transcript enrichment")
			}
			fmt.Fprint(w, `{"schemaVersion":"1","conversation":{"conversationId":"conversation-1","turns":[
			 {"turnId":"native-turn-1","startedByMessageId":"user-message-1","user":{"messageId":"user-message-1","content":"hello"},"status":"completed","assistant":{"final":{"messageId":"answer-1","content":"correct final"},"narration":{"content":"internal narration"}}},
			 {"turnId":"newer-unrelated","status":"completed","assistant":{"final":{"content":"wrong final"}}}
			]}}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	})
	ctx := context.Background()
	id, err := c.CreateConversation(ctx, "steward")
	if err != nil || id != "conversation-1" {
		t.Fatalf("create: %q %v", id, err)
	}
	turn, err := c.Query(ctx, gateway.Query{ConversationID: id, MessageID: "request-1", AgentID: "steward", Text: "hello"}, nil)
	if err != nil || turn.ID != "request-1" || turn.State != "completed" || turn.Content != "correct final" {
		t.Fatalf("query: %+v %v", turn, err)
	}
	if creates.Load() != 1 || queries.Load() != 1 || reads.Load() != 1 {
		t.Fatal("unexpected requests")
	}
}

func TestCanonicalNativeTurnStatesAndResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, conversation, turns, state, content string
		invalid                                   bool
	}{
		{"missing_turn", "c", `[]`, "unknown", "", false},
		{"queued", "c", `[{"turnId":"m","status":"queued"}]`, "queued", "", false},
		{"running_not_final", "c", `[{"turnId":"m","status":"running","assistant":{"final":{"content":"premature"}}}]`, "running", "", false},
		{"empty_completed", "c", `[{"turnId":"m","status":"completed"}]`, "completed", "", false},
		{"failed", "c", `[{"turnId":"m","status":"failed","errorMessage":"internal error"}]`, "failed", "", false},
		{"canceled", "c", `[{"turnId":"m","status":"canceled"}]`, "canceled", "", false},
		{"interaction", "c", `[{"turnId":"m","status":"waiting_for_user"}]`, "waiting_for_user", "", false},
		{"elicitation", "c", `[{"turnId":"m","status":"completed","elicitation":{"status":"pending"}}]`, "waiting_for_user", "", false},
		{"unknown_elicitation", "c", `[{"turnId":"m","status":"completed","elicitation":{"status":"future_state"}}]`, "waiting_for_user", "", false},
		{"empty_elicitation", "c", `[{"turnId":"m","status":"completed","elicitation":{}}]`, "waiting_for_user", "", false},
		{"resolved_elicitation", "c", `[{"turnId":"m","status":"completed","elicitation":{"status":"accepted"}}]`, "completed", "", false},
		{"final_page", "c", `[{"turnId":"m","status":"completed","execution":{"pages":[{"turnId":"m","content":"narration"},{"turnId":"m","finalResponse":true,"content":"final"},{"turnId":"other","finalResponse":true,"content":"wrong"}]}}]`, "completed", "final", false},
		{"empty_final_does_not_use_page", "c", `[{"turnId":"m","status":"completed","assistant":{"final":{"content":""}},"execution":{"pages":[{"turnId":"m","finalResponse":true,"content":"not the canonical final"}]}}]`, "completed", "", false},
		{"unknown_status", "c", `[{"turnId":"m","status":"success"}]`, "", "", true},
		{"duplicate_turn", "c", `[{"turnId":"m","status":"running"},{"turnId":"m","status":"completed"}]`, "", "", true},
		{"separate_starter_message", "c", `[{"turnId":"m","status":"completed","startedByMessageId":"user-1","user":{"messageId":"user-1"},"assistant":{"final":{"content":"final"}}}]`, "completed", "final", false},
		{"starter_without_user_projection", "c", `[{"turnId":"m","status":"completed","startedByMessageId":"user-1"}]`, "completed", "", false},
		{"legacy_starter_equals_turn", "c", `[{"turnId":"m","status":"completed","startedByMessageId":"m"}]`, "completed", "", false},
		{"running_with_separate_starter", "c", `[{"turnId":"m","status":"running","startedByMessageId":"user-1","assistant":{"final":{"content":"premature"}}}]`, "running", "", false},
		{"starter_is_not_turn_identity", "c", `[{"turnId":"other","status":"completed","startedByMessageId":"m","assistant":{"final":{"content":"wrong"}}}]`, "unknown", "", false},
		{"wrong_conversation", "other", `[]`, "", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("read caused mutation")
				}
				fmt.Fprintf(w, `{"conversation":{"conversationId":%q,"turns":%s}}`, tc.conversation, tc.turns)
			})
			turn, err := c.(*Client).readNativeTurn(context.Background(), "c", "logical-request", "m")
			if tc.invalid {
				if !errors.Is(err, gateway.ErrBackendProtocol) {
					t.Fatalf("expected protocol error, got %v", err)
				}
				return
			}
			if err != nil || turn.ID != "logical-request" || turn.State != tc.state || turn.Content != tc.content {
				t.Fatalf("turn %+v, %v", turn, err)
			}
		})
	}
}

func TestCanonicalPublicMessagesRequireCompletedTurn(t *testing.T) {
	for _, status := range []string{"queued", "running", "waiting_for_user", "failed", "canceled", "completed"} {
		t.Run(status, func(t *testing.T) {
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprintf(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"m","status":%q,"messages":[{"messageId":"note","role":"assistant","content":"Public findings."},{"messageId":"note","role":"assistant","content":"Later findings."},{"messageId":"final","role":"assistant","content":"Final answer."}],"assistant":{"final":{"messageId":"final","content":"Final answer."}}}]}}`, status)
			})
			turn, err := c.(*Client).readNativeTurn(context.Background(), "c", "logical-request", "m")
			if err != nil {
				t.Fatal(err)
			}
			if status != "completed" {
				if turn.Content != "" || len(turn.StandaloneMessages) != 0 {
					t.Fatal("non-completed response exposed public content")
				}
				return
			}
			if turn.Content != "Final answer." || len(turn.StandaloneMessages) != 2 || turn.StandaloneMessages[0].MessageID != "note" || turn.StandaloneMessages[1].MessageID != "note" || turn.StandaloneMessages[0].Content != "Public findings." || turn.StandaloneMessages[1].Content != "Later findings." {
				t.Fatal("completed response lost findings or duplicated final")
			}
		})
	}
}

func TestQueryNeverTrustsHTTP200Alone(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		invalid    bool
	}{
		{"no_terminal_turn", `{"conversation":{"conversationId":"c","turns":[]}}`, false},
		{"unrelated_final", `{"conversation":{"conversationId":"c","turns":[{"turnId":"m","status":"completed","assistant":{"final":{"content":"wrong native identity"}}}]}}`, false},
		{"missing_conversation", `{"content":"not a terminal status"}`, true},
		{"null_conversation", `{"conversation":null}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var reads atomic.Int64
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/conversations/c":
					fmt.Fprint(w, `{"id":"c"}`)
				case "/v1/ag-ui/run":
					writeRunStarted(w, "c", "m", "native")
					// Even streamed text is not a canonical final answer.
					fmt.Fprint(w, "data: {\"type\":\"TEXT_MESSAGE_CONTENT\",\"delta\":\"unverified\"}\n\ndata: {\"type\":\"RUN_FINISHED\"}\n\n")
				case "/v1/conversations/c/transcript":
					reads.Add(1)
					fmt.Fprint(w, tc.body)
				default:
					t.Error("unexpected request")
				}
			})
			turn, err := c.Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "m", AgentID: "a", Text: "x"}, nil)
			if tc.invalid {
				if !errors.Is(err, gateway.ErrBackendProtocol) || reads.Load() != 1 {
					t.Fatalf("unsafe response: %v", err)
				}
				return
			}
			if err != nil || turn.ID != "m" {
				t.Fatalf("query: %+v %v", turn, err)
			}
			if turn.State != "unknown" || reads.Load() != 1 {
				t.Fatal("HTTP 200 incorrectly considered completion")
			}
		})
	}
}

func TestFailuresAreRedactedAndNeverRetried(t *testing.T) {
	for _, code := range []int{302, 400, 401, 403, 404, 408, 429, 500, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var calls atomic.Int64
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == "/v1/conversations/c" {
					fmt.Fprint(w, `{"id":"c"}`)
					return
				}
				calls.Add(1)
				w.Header().Set("Location", "/login?secret-marker")
				w.WriteHeader(code)
				fmt.Fprint(w, "secret-marker server trace")
			})
			_, err := c.Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "m", AgentID: "a", Text: "secret-marker"}, nil)
			if err == nil || strings.Contains(err.Error(), "secret-marker") || calls.Load() != 1 {
				t.Fatalf("unsafe retry/error: %v, %d", err, calls.Load())
			}
			permanent := code < 500 && code != 408 && code != 429
			if errors.Is(err, gateway.ErrBackendProtocol) != permanent {
				t.Fatalf("incorrect classification for %d", code)
			}
		})
	}
	for _, body := range []string{"secret-marker", `{"id":"c"} {}`, strings.Repeat("x", maxResponseBytes+1)} {
		c := testClient(t, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, body) })
		if _, err := c.CreateConversation(context.Background(), "a"); !errors.Is(err, gateway.ErrBackendProtocol) || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("malformed response: %v", err)
		}
	}
}

func TestTimeoutAndCredentials(t *testing.T) {
	var calls atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.Query(ctx, gateway.Query{ConversationID: "c", MessageID: "m", AgentID: "a"}, nil)
	if err == nil || errors.Is(err, gateway.ErrBackendProtocol) || calls.Load() != 1 {
		t.Fatalf("timeout must be reconciled: %v", err)
	}
	provider := tokenFunc(func(context.Context, string) (string, error) { return "", errors.New("secret-marker") })
	f, err := NewFactory("http://127.0.0.1:1", nil, provider)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForPrincipal(context.Background(), ""); err == nil {
		t.Fatal("missing principal accepted")
	}
	c, _ = f.ForPrincipal(context.Background(), "shared")
	_, err = c.ReadTurn(context.Background(), "c", "m", nil)
	if !errors.Is(err, gateway.ErrBackendProtocol) || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("credential failure exposed or ignored")
	}
	for _, base := range []string{"http://example.com", "https://user:secret@example.com", "https://example.com?secret", "://bad"} {
		if _, err := NewFactory(base, nil, provider); err == nil {
			t.Fatal("unsafe base URL accepted")
		}
	}
}

func TestCredentialProviderIsConsultedForEveryRequest(t *testing.T) {
	var supplied, received atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := received.Add(1)
		if r.Header.Get("Authorization") != fmt.Sprintf("Bearer credential-%d", n) {
			t.Error("stale credential reused")
		}
		switch r.URL.Path {
		case "/v1/conversations/c":
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/ag-ui/run":
			checkRunInput(t, r, "c", "m", "run.attach", "", "")
			writeRunStarted(w, "c", "m", "native")
		case "/v1/conversations/c/transcript":
			fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[]}}`)
		default:
			t.Error("unexpected request")
		}
	}))
	defer srv.Close()
	f, err := NewFactory(srv.URL, srv.Client(), tokenFunc(func(_ context.Context, p string) (string, error) {
		if p != "shared" {
			t.Error("execution principal changed")
		}
		return fmt.Sprintf("credential-%d", supplied.Add(1)), nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.ForPrincipal(context.Background(), "shared")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := c.ReadTurn(context.Background(), "c", "m", nil); err != nil {
			t.Fatal(err)
		}
	}
	if supplied.Load() != 6 || received.Load() != 6 {
		t.Fatal("credential provider was not called per request")
	}
}

func TestCredentialCancellationIsNotAccessRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f, err := NewFactory("http://127.0.0.1:1", nil, tokenFunc(func(ctx context.Context, _ string) (string, error) {
		cancel()
		return "", ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.ForPrincipal(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ReadTurn(ctx, "c", "m", nil); err == nil || errors.Is(err, gateway.ErrBackendProtocol) {
		t.Fatalf("credential cancellation must allow reconciliation: %v", err)
	}
}
