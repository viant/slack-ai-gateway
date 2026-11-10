package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	sdk "github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
)

var testRoute = gateway.Route{AppID: "A", WorkspaceID: "T", ChannelID: "C"}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func payload(change func(map[string]any, map[string]any)) []byte {
	e := map[string]any{"type": "app_mention", "user": "U1", "channel": "C", "ts": "1.000001", "text": "<@UBOT> why &lt;x&gt; &amp; &amp;lt;?"}
	c := map[string]any{"type": "event_callback", "api_app_id": "A", "team_id": "T", "event_id": "E1", "is_ext_shared_channel": false, "event": e}
	if change != nil {
		change(c, e)
	}
	b, _ := json.Marshal(c)
	return b
}

func TestNormalization(t *testing.T) {
	m, reason := normalize(payload(nil), testRoute, "UBOT")
	if reason != "" || m.Text != "why <x> & &lt;?" || m.RootTS != "1.000001" || !m.Mentioned || m.AuthorID != "U1" {
		t.Fatalf("incorrect normalization: %#v %s", m, reason)
	}
	m, reason = normalize(payload(func(c, e map[string]any) { e["thread_ts"] = "0.000001"; e["files"] = []any{map[string]any{"id": "F"}} }), testRoute, "UBOT")
	if reason != "" || m.RootTS != "0.000001" || !m.HasAttachments {
		t.Fatal("thread or attachment lost")
	}
	for _, tc := range []struct {
		name   string
		change func(map[string]any, map[string]any)
	}{
		{"app", func(c, e map[string]any) { c["api_app_id"] = "OTHER" }},
		{"workspace", func(c, e map[string]any) { c["team_id"] = "OTHER" }},
		{"channel", func(c, e map[string]any) { e["channel"] = "OTHER" }},
		{"external", func(c, e map[string]any) { c["is_ext_shared_channel"] = true }},
		{"missing_security_field", func(c, e map[string]any) { delete(c, "is_ext_shared_channel") }},
		{"event_id", func(c, e map[string]any) { delete(c, "event_id") }},
		{"user", func(c, e map[string]any) { delete(e, "user") }},
		{"bot", func(c, e map[string]any) { e["bot_id"] = "BOTHER" }},
		{"self", func(c, e map[string]any) { e["user"] = "UBOT" }},
		{"edit", func(c, e map[string]any) { e["edited"] = map[string]any{"ts": "2"} }},
		{"delete", func(c, e map[string]any) { e["subtype"] = "message_deleted" }},
		{"unknown_subtype", func(c, e map[string]any) { e["subtype"] = "future" }},
		{"display_name", func(c, e map[string]any) { e["text"] = "@Steward hello" }},
		{"escaped_mention", func(c, e map[string]any) { e["text"] = "&lt;@UBOT&gt; hello" }},
		{"other_mention", func(c, e map[string]any) { e["text"] = "<@UOTHER> hello" }},
		{"unsupported", func(c, e map[string]any) { e["type"] = "message" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, reason := normalize(payload(tc.change), testRoute, "UBOT"); reason == "" {
				t.Fatal("unsafe event accepted")
			}
		})
	}
}

func TestNormalizationSupportsWorkspaceAndSelectedChannels(t *testing.T) {
	mode := "channel_members"
	for _, scope := range []gateway.ChannelScope{
		{Mode: "workspace", Denylist: []string{"DENIED"}},
		{Mode: "selected", Allowlist: []string{"C", "C2"}},
	} {
		resolver, err := gateway.NewChannelPolicyResolver(scope, gateway.AccessRules{Defaults: &gateway.UserRule{Mode: &mode}})
		if err != nil {
			t.Fatal(err)
		}
		route := gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: resolver}
		for _, channel := range []string{"C", "C2", "NEW", "DENIED"} {
			m, reason := normalize(payload(func(c, e map[string]any) { e["channel"] = channel }), route, "UBOT")
			want := channel == "C" || channel == "C2" || (scope.Mode == "workspace" && channel == "NEW")
			if (reason == "") != want || (want && m.ChannelID != channel) {
				t.Fatal("multi-channel normalization failed")
			}
		}
	}
}

type sinkFunc func(context.Context, gateway.Message) (gateway.AcceptResult, error)

func (f sinkFunc) Accept(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
	return f(ctx, m)
}

type fakeSession struct {
	stream chan socketmode.Event
	ack    func(string) error
}

func (f *fakeSession) RunContext(ctx context.Context) error             { <-ctx.Done(); return ctx.Err() }
func (f *fakeSession) AckCtx(_ context.Context, id string, _ any) error { return f.ack(id) }
func (f *fakeSession) events() <-chan socketmode.Event                  { return f.stream }

func testStore(t *testing.T) *sqlite.Store {
	t.Helper()
	s, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "gateway.sqlite"), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestACKAfterCommitAndForDuplicate(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	acks := []string{}
	session := &fakeSession{ack: func(id string) error {
		m, _ := normalize(payload(nil), testRoute, "UBOT")
		inserted, err := s.Enqueue(ctx, m)
		if err != nil || inserted {
			t.Fatal("ACK preceded durable inbox commit")
		}
		acks = append(acks, id)
		return nil
	}}
	source := Source{session: session, route: testRoute, botUserID: "UBOT", log: quietLog()}
	for _, id := range []string{"envelope-1", "different-envelope-same-event"} {
		err := source.handle(ctx, socketmode.Request{Type: "events_api", EnvelopeID: id, Payload: payload(nil)}, gateway.Intake{Store: s, Route: testRoute})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(acks) != 2 || acks[1] != "different-envelope-same-event" {
		t.Fatal("wrong ACK identity")
	}
	j, err := s.ClaimJob(ctx)
	if err != nil || j == nil {
		t.Fatal("missing job")
	}
	j, err = s.ClaimJob(ctx)
	if err != nil || j != nil {
		t.Fatal("duplicate job")
	}
}

func TestACKFailureAndIgnoredPayloads(t *testing.T) {
	acks := 0
	var logs bytes.Buffer
	source := Source{session: &fakeSession{ack: func(string) error { acks++; return errors.New("secret-marker") }}, route: testRoute, botUserID: "UBOT", log: slog.New(slog.NewTextHandler(&logs, nil))}
	req := socketmode.Request{Type: "events_api", EnvelopeID: "env", Payload: payload(nil)}
	err := source.handle(context.Background(), req, sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
		return gateway.AcceptResult{}, errors.New("secret-marker")
	}))
	if err == nil || acks != 0 || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("storage failure acknowledged or leaked")
	}
	err = source.handle(context.Background(), req, sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
		return gateway.AcceptResult{Accepted: true}, nil
	}))
	if err != nil || acks != 1 || !strings.Contains(logs.String(), "slack_ack_failed") || strings.Contains(logs.String(), "secret-marker") {
		t.Fatal("ACK error not safely observed")
	}
	req.Payload = payload(func(c, e map[string]any) { e["text"] = "without mention" })
	err = source.handle(context.Background(), req, sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
		t.Fatal("ignored event reached intake")
		return gateway.AcceptResult{}, nil
	}))
	if err != nil || acks != 2 {
		t.Fatal("ignored event not acknowledged")
	}
}

func TestACKBurstDoesNotWaitForBackend(t *testing.T) {
	s := testStore(t)
	acks := 0
	source := Source{session: &fakeSession{ack: func(string) error { acks++; return nil }}, route: testRoute, botUserID: "UBOT", log: quietLog()}
	intake := gateway.Intake{Store: s, Route: testRoute}
	var longest time.Duration
	for i := range 64 {
		data := payload(func(c, e map[string]any) { c["event_id"] = fmt.Sprintf("E%d", i); e["ts"] = fmt.Sprintf("1.%06d", i) })
		start := time.Now()
		if err := source.handle(context.Background(), socketmode.Request{Type: "events_api", EnvelopeID: fmt.Sprintf("env%d", i), Payload: data}, intake); err != nil {
			t.Fatal(err)
		}
		longest = max(longest, time.Since(start))
	}
	if acks != 64 {
		t.Fatal("missing ACK")
	}
	if longest >= time.Second {
		t.Fatalf("local ACK target exceeded: %s", longest)
	}
	t.Logf("64 persisted events with no backend; longest local handling: %s", longest)
}

func TestWrongHelloNeverBecomesReady(t *testing.T) {
	stream := make(chan socketmode.Event, 1)
	stream <- socketmode.Event{Type: socketmode.EventTypeHello, Request: &socketmode.Request{ConnectionInfo: socketmode.ConnectionInfo{AppID: "OTHER"}}}
	source := Source{session: &fakeSession{stream: stream, ack: func(string) error { t.Error("unexpected ACK"); return nil }}, route: testRoute, botUserID: "UBOT", log: quietLog()}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := source.Run(ctx, sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
		t.Error("unexpected intake")
		return gateway.AcceptResult{}, nil
	}), func() { t.Error("wrong app became ready") })
	if err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("wrong hello accepted: %v", err)
	}
}

type failingPost struct {
	calls int
	err   error
}

func (f *failingPost) PostMessageContext(context.Context, string, ...sdk.MsgOption) (string, string, error) {
	f.calls++
	return "", "", f.err
}

func TestSenderAmbiguousTransportErrorsAndRouteGuard(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, context.Canceled, errors.New("secret-marker")} {
		api := &failingPost{err: err}
		sender := Sender{api: api, route: testRoute}
		d := gateway.Delivery{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"}, Text: "reply", Format: gateway.ReplyPlain}
		_, err := sender.Send(context.Background(), d)
		var classified *gateway.SendError
		if !errors.As(err, &classified) || classified.Kind != gateway.SendUnknown || api.calls != 1 {
			t.Fatalf("uncertain send retried or misclassified: %v", err)
		}
		d.ChannelID = "OTHER"
		_, err = sender.Send(context.Background(), d)
		if !errors.As(err, &classified) || classified.Kind != gateway.SendRejected || api.calls != 1 {
			t.Fatal("route guard failed")
		}
	}
}

func TestSenderHTTPContractAndClassification(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		retry      string
		kind       gateway.SendErrorKind
	}{
		{"success", `{"ok":true,"channel":"C","ts":"2.000001"}`, 200, "", ""},
		{"limit", `{"ok":false}`, 429, "12", gateway.SendRateLimited},
		{"invalid_retry", `{}`, 429, "invalid", gateway.SendUnknown},
		{"auth", `{"ok":false,"error":"invalid_auth"}`, 200, "", gateway.SendCredentials},
		{"scope", `{"ok":false,"error":"missing_scope"}`, 200, "", gateway.SendRejected},
		{"blocks", `{"ok":false,"error":"invalid_blocks"}`, 200, "", gateway.SendRejected},
		{"channel", `{"ok":false,"error":"not_in_channel"}`, 200, "", gateway.SendRejected},
		{"internal", `{"ok":false,"error":"internal_error"}`, 200, "", gateway.SendUnknown},
		{"fatal", `{"ok":false,"error":"fatal_error"}`, 200, "", gateway.SendUnknown},
		{"server", `secret-marker`, 503, "", gateway.SendUnknown},
		{"invalid_json", `secret-marker`, 200, "", gateway.SendUnknown},
		{"missing_ts", `{"ok":true,"channel":"C"}`, 200, "", gateway.SendUnknown},
		{"wrong_destination", `{"ok":true,"channel":"OTHER","ts":"2"}`, 200, "", gateway.SendUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				r.ParseForm()
				if r.URL.Path != "/chat.postMessage" || r.Form.Get("channel") != "C" || r.Form.Get("thread_ts") != "1.000001" || r.Form.Get("text") != "&lt;!here&gt; &amp;" || r.Form.Get("mrkdwn") != "false" || r.Form.Get("parse") != "none" || r.Form.Get("unfurl_links") != "false" || r.Form.Get("unfurl_media") != "false" || r.Form.Get("reply_broadcast") == "true" || r.Form.Get("link_names") == "1" {
					t.Error("incorrect send parameters")
				}
				if tc.retry != "" {
					w.Header().Set("Retry-After", tc.retry)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer server.Close()
			sender := Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client())), route: testRoute}
			ts, err := sender.Send(context.Background(), gateway.Delivery{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}, Text: "&lt;!here&gt; &amp;", Format: gateway.ReplyPlain})
			if tc.kind == "" {
				if err != nil || ts == "" {
					t.Fatal("success lost")
				}
			} else {
				var classified *gateway.SendError
				if !errors.As(err, &classified) || classified.Kind != tc.kind {
					t.Fatalf("wrong classification: %v", err)
				}
				if tc.kind == gateway.SendRateLimited && classified.RetryAfter != 12*time.Second {
					t.Fatal("Retry-After lost")
				}
				if strings.Contains(err.Error(), "secret-marker") {
					t.Fatal("upstream content leaked")
				}
			}
			if calls.Load() != 1 {
				t.Fatal("SDK retried a write")
			}
		})
	}
}

func TestNativeMarkdownPayloadAndAccessibleFallback(t *testing.T) {
	for _, text := range []string{
		"# Heading\n\n**Bold** & [link](https://example.org) <@U123>\n\n```go\nfmt.Println(\"🙂\")\n```",
		strings.Repeat("&", gateway.MaxMarkdownCharacters),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			var blocks []struct{ Type, Text string }
			if err := json.Unmarshal([]byte(r.Form.Get("blocks")), &blocks); err != nil {
				t.Error(err)
			}
			if len(blocks) != 1 || blocks[0].Type != "markdown" || blocks[0].Text != text {
				t.Error("native Markdown block missing or rewritten")
			}
			fallback := r.Form.Get("text")
			if fallback == "" || strings.ContainsAny(fallback, "<>") || utf8.RuneCountInString(fallback) > 40000 {
				t.Error("missing, unescaped or oversized accessibility text")
			}
			if r.Form.Get("thread_ts") != "1.000001" || r.Form.Get("mrkdwn") != "false" || r.Form.Get("parse") != "none" || r.Form.Get("unfurl_links") != "false" || r.Form.Get("unfurl_media") != "false" || r.Form.Get("markdown_text") != "" {
				t.Error("native block changed safe delivery options")
			}
			io.WriteString(w, `{"ok":true,"channel":"C","ts":"2.000001"}`)
		}))
		sender := Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client())), route: testRoute}
		_, err := sender.Send(context.Background(), gateway.Delivery{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.000001"}, Text: text, Format: gateway.ReplyMarkdown})
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPreflight(t *testing.T) {
	for _, body := range []string{`{"ok":true,"team_id":"T","user_id":"UBOT","bot_id":"B123"}`, `{"ok":true,"team_id":"OTHER","user_id":"UBOT","bot_id":"B123"}`, `{"ok":true,"team_id":"T","user_id":"UHUMAN"}`, `{"ok":false,"error":"secret-marker"}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		id, err := preflight(context.Background(), sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/")), testRoute)
		server.Close()
		if strings.Contains(body, `"bot_id":"B123"`) && !strings.Contains(body, "OTHER") {
			if err != nil || id != "UBOT" {
				t.Fatal("must use bot UserID, not BotID")
			}
		} else if err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("unsafe preflight")
		}
	}
}

// Exercises the actual pinned SDK, including hello, envelope ACK and reconnect.
// All HTTP and WebSocket traffic is confined to this local test server.
func TestSocketSDKReconnectAndDurableDedup(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	acked := make(chan string, 4)
	var connections atomic.Int32
	// The real SDK sends Slack's Origin, not this loopback server's address.
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://api.slack.com" }}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apps.connections.open" {
			if r.Header.Get("Authorization") != "Bearer xapp-test" {
				t.Error("app token not used for socket open")
			}
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": "ws" + strings.TrimPrefix(server.URL, "http") + "/socket"})
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		conn.WriteJSON(map[string]any{"type": "hello", "connection_info": map[string]any{"app_id": "A"}})
		id := "env-first"
		if n > 1 {
			id = "env-retry"
		}
		conn.WriteJSON(map[string]any{"type": "events_api", "envelope_id": id, "payload": json.RawMessage(payload(nil))})
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var ack map[string]any
		if err := conn.ReadJSON(&ack); err != nil {
			return
		}
		m, _ := normalize(payload(nil), testRoute, "UBOT")
		inserted, err := s.Enqueue(ctx, m)
		if err != nil || inserted {
			t.Error("wire ACK before commit")
		}
		acked <- ack["envelope_id"].(string)
		if n == 1 {
			conn.WriteJSON(map[string]any{"type": "disconnect", "reason": "refresh_requested"})
			return
		}
		// Wait for client shutdown; a read unblocks when the SDK closes the socket.
		conn.ReadMessage()
	}))
	defer server.Close()
	client := sdk.New("xoxb-test", sdk.OptionAppLevelToken("xapp-test"), sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client()))
	source := Source{session: sdkSession{socketmode.New(client)}, route: testRoute, botUserID: "UBOT", log: quietLog()}
	var ready atomic.Int32
	done := make(chan error, 1)
	go func() { done <- source.Run(ctx, gateway.Intake{Store: s, Route: testRoute}, func() { ready.Add(1) }) }()
	for _, expected := range []string{"env-first", "env-retry"} {
		select {
		case got := <-acked:
			if got != expected {
				t.Errorf("ACK %s instead of %s", got, expected)
			}
		case <-ctx.Done():
			t.Error("socket test timed out")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if ready.Load() < 2 {
		t.Fatal("hello not revalidated on reconnect")
	}
	j, err := s.ClaimJob(context.Background())
	if err != nil || j == nil {
		t.Fatal("missing durable event")
	}
	if j, err = s.ClaimJob(context.Background()); err != nil || j != nil {
		t.Fatal("reconnect duplicate persisted twice")
	}
}
