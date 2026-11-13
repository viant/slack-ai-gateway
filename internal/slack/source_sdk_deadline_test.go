package slack

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	sdk "github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestSocketSourceAcceptDeadlineKeepsConnectionAndReceivesRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const burst = 32
	var connections atomic.Int32
	wireACKs := make(chan string, burst+2)
	serverErrors := make(chan error, 1)
	ackLatency := make(chan time.Duration, 1)
	report := func(err error) {
		select {
		case serverErrors <- err:
		default:
		}
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://api.slack.com" }}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apps.connections.open" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": "ws" + strings.TrimPrefix(server.URL, "http") + "/socket"})
			return
		}
		if r.URL.Path != "/socket" {
			http.NotFound(w, r)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			report(err)
			return
		}
		defer conn.Close()
		connections.Add(1)
		_ = conn.SetReadDeadline(time.Now().Add(12 * time.Second))
		if err := conn.WriteJSON(map[string]any{"type": "hello", "connection_info": map[string]any{"app_id": "A"}}); err != nil {
			report(err)
			return
		}
		frame := func(envelopeID, eventID string, retry int) map[string]any {
			return map[string]any{"type": "events_api", "envelope_id": envelopeID, "retry_attempt": retry,
				"payload": json.RawMessage(payload(func(c, _ map[string]any) { c["event_id"] = eventID }))}
		}
		write := func(envelopeID, eventID string, retry int) bool {
			if err := conn.WriteJSON(frame(envelopeID, eventID, retry)); err != nil {
				report(err)
				return false
			}
			return true
		}
		readACK := func() bool {
			var ack struct {
				EnvelopeID string `json:"envelope_id"`
			}
			if err := conn.ReadJSON(&ack); err != nil {
				report(err)
				return false
			}
			wireACKs <- ack.EnvelopeID
			return true
		}
		if !write("failed-first", "E1", 0) {
			return
		}
		secondSent := time.Now()
		if !write("second", "E2", 0) || !readACK() {
			return
		}
		ackLatency <- time.Since(secondSent)
		// This server explicitly simulates redelivery. It makes no guarantee
		// about Slack's retry schedule or whether any production event is retried.
		if !write("first-retry", "E1", 1) || !readACK() {
			return
		}
		for i := range burst {
			if !write(fmt.Sprintf("burst-%d", i), fmt.Sprintf("B%d", i), 0) {
				return
			}
		}
		for range burst {
			if !readACK() {
				return
			}
		}
		_, _, _ = conn.ReadMessage() // unblocks when the native client is canceled
	}))
	api := sdk.New("xoxb-test", sdk.OptionAppLevelToken("xapp-test"), sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client()))
	source := Source{session: sdkSession{socketmode.New(api)}, route: testRoute, botUserID: "UBOT", log: quietLog()}
	calls := make(map[string]int)
	var ready atomic.Int32
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		done <- source.Run(ctx, sinkFunc(func(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
			calls[m.EventID]++
			if m.EventID == "E1" && calls[m.EventID] == 1 {
				<-ctx.Done()
				return gateway.AcceptResult{}, ctx.Err()
			}
			return gateway.AcceptResult{Accepted: true}, nil
		}), func() { ready.Add(1) })
	}()
	defer func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Source/SDK did not join")
		}
		server.Close()
	}()
	want := []string{"second", "first-retry"}
	for i := range burst {
		want = append(want, fmt.Sprintf("burst-%d", i))
	}
	for _, expected := range want {
		select {
		case got := <-wireACKs:
			if got != expected {
				t.Fatalf("wire ACK %q want %q; failed attempt must remain unacknowledged", got, expected)
			}
		case err := <-serverErrors:
			t.Fatal(err)
		case err := <-done:
			t.Fatalf("Source stopped after one timeout: %v", err)
		case <-ctx.Done():
			t.Fatal("Source/SDK did not deliver the expected wire ACKs")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-joined
	if connections.Load() != 1 || ready.Load() != 1 || calls["E1"] != 2 || calls["E2"] != 1 || len(calls) != burst+2 {
		t.Fatalf("unexpected reconnect/replay: connections=%d ready=%d calls=%v", connections.Load(), ready.Load(), calls)
	}
	for i := range burst {
		if calls[fmt.Sprintf("B%d", i)] != 1 {
			t.Fatal("burst envelope was locally replayed")
		}
	}
	// Includes the first event's timeout and waiting inside the native buffers.
	// Record the complete wire path without a fragile millisecond CI assertion.
	t.Logf("E2 frame-to-wire-ACK after failed E1: %s; one connection, %d later burst ACKs", <-ackLatency, burst)
}

func TestSourceACKQueueFailureDoesNotBecomeIntakeFailure(t *testing.T) {
	client := socketmode.New(sdk.New("xoxb-test-not-a-real-token"))
	for i := range 20 {
		if err := client.AckCtx(context.Background(), fmt.Sprintf("queued-%d", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	// No RunContext/socket exists: success above proves queueing, not wire ACK.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	var logs bytes.Buffer
	source := Source{session: sdkSession{client}, route: testRoute, botUserID: "UBOT", log: slog.New(slog.NewTextHandler(&logs, nil))}
	err := source.handle(ctx, socketmode.Request{Type: socketmode.RequestTypeEventsAPI, EnvelopeID: "queue-full", Payload: payload(nil)},
		sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
			return gateway.AcceptResult{Accepted: true}, nil
		}))
	if err != nil || !strings.Contains(logs.String(), "slack_ack_failed") {
		t.Fatalf("successful admission with full ACK queue changed classification: err=%v logs=%s", err, logs.String())
	}
	if err := client.AckCtx(ctx, "still-full", nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("native full queue did not respect context: %v", err)
	}
}

func TestSocketSourceDisconnectAfterCommitDeduplicatesRedelivery(t *testing.T) {
	s := testStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	type admission struct {
		call          int32
		accepted      bool
		duplicate     bool
		ownDeadline   bool
		errorDeadline bool
		txDone        bool
		hasError      bool
		duration      time.Duration
	}
	dispositions := make(chan admission)
	diagnostics := make(chan string, 8)
	disconnected := make(chan struct{})
	wireACK := make(chan string, 1)
	serverErrors := make(chan error, 1)
	var connections, calls atomic.Int32
	var firstAccepted atomic.Bool
	var committedEnvelope atomic.Value
	committedEnvelope.Store("")
	report := func(err error) {
		select {
		case serverErrors <- err:
		default:
		}
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return r.Header.Get("Origin") == "https://api.slack.com" }}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apps.connections.open" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "url": "ws" + strings.TrimPrefix(server.URL, "http") + "/socket"})
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			report(err)
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		_ = conn.SetReadDeadline(time.Now().Add(12 * time.Second))
		if err := conn.WriteJSON(map[string]any{"type": "hello", "connection_info": map[string]any{"app_id": "A"}}); err != nil {
			report(err)
			return
		}
		// A one-second application budget can expire under instrumentation.
		// The peer explicitly simulates at most three attempts per connection;
		// do not require a cold native graph to finish on its first attempt.
		for attempt := 1; attempt <= 3; attempt++ {
			id := fmt.Sprintf("connection-%d-attempt-%d", n, attempt)
			if err := conn.WriteJSON(map[string]any{"type": "events_api", "envelope_id": id,
				"retry_attempt": attempt - 1, "payload": json.RawMessage(payload(nil))}); err != nil {
				report(err)
				return
			}
			var observed admission
			select {
			case observed = <-dispositions:
			case <-ctx.Done():
				return
			}
			record := fmt.Sprintf("connection=%d attempt=%d native_call=%d accepted=%v duplicate=%v own_deadline=%v error_deadline=%v tx_done=%v has_error=%v duration=%s",
				n, attempt, observed.call, observed.accepted, observed.duplicate, observed.ownDeadline, observed.errorDeadline, observed.txDone, observed.hasError, observed.duration)
			select {
			case diagnostics <- record:
			case <-ctx.Done():
				return
			}
			if observed.ownDeadline {
				continue // No local replay: the next iteration is a peer redelivery.
			}
			if observed.hasError || !observed.accepted {
				report(fmt.Errorf("native admission did not return a successful or own-budget disposition: connection=%d attempt=%d", n, attempt))
				return
			}
			if n == 1 {
				// Accepted also covers an authoritative duplicate after an earlier
				// uncertain write. Both prove a durable event before disconnect.
				committedEnvelope.Store(id)
				_ = conn.Close()
				close(disconnected)
				return
			}
			if !observed.duplicate {
				report(errors.New("redelivery after a proven durable event was inserted again"))
				return
			}
			for {
				var ack struct {
					EnvelopeID string `json:"envelope_id"`
				}
				if err := conn.ReadJSON(&ack); err != nil {
					report(err)
					return
				}
				if ack.EnvelopeID == id {
					wireACK <- id
					_, _, _ = conn.ReadMessage()
					return
				}
				// The successful old attempt's queued ACK may reach the new
				// socket. An ACK for any failed attempt would be incorrect.
				if ack.EnvelopeID != committedEnvelope.Load().(string) {
					report(fmt.Errorf("unexpected wire ACK for an uncommitted attempt: %s", ack.EnvelopeID))
					return
				}
			}
		}
		report(fmt.Errorf("three native admission attempts exceeded the unchanged budget on connection %d", n))
	}))
	api := sdk.New("xoxb-test", sdk.OptionAppLevelToken("xapp-test"), sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client()))
	source := Source{session: sdkSession{socketmode.New(api)}, route: testRoute, botUserID: "UBOT", log: quietLog()}
	intake := gateway.Intake{Store: s, Route: testRoute}
	done := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		done <- source.Run(ctx, sinkFunc(func(acceptCtx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
			started := time.Now()
			result, err := intake.Accept(acceptCtx, m)
			observed := admission{call: calls.Add(1), accepted: result.Accepted, duplicate: result.Duplicate,
				errorDeadline: errors.Is(err, context.DeadlineExceeded), txDone: errors.Is(err, sql.ErrTxDone), hasError: err != nil, duration: time.Since(started)}
			observed.ownDeadline = observed.errorDeadline && acceptCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil
			select {
			case dispositions <- observed:
			case <-ctx.Done():
			}
			if err == nil && result.Accepted && firstAccepted.CompareAndSwap(false, true) {
				select {
				case <-disconnected:
				case <-ctx.Done():
				}
			}
			return result, err
		}), func() {})
	}()
	defer func() {
		cancel()
		select {
		case <-joined:
		case <-time.After(5 * time.Second):
			t.Error("Source/SDK did not join")
		}
		server.Close()
		for {
			select {
			case record := <-diagnostics:
				t.Log(record)
			default:
				return
			}
		}
	}()
	select {
	case <-wireACK:
	case err := <-serverErrors:
		t.Fatal(err)
	case err := <-done:
		t.Fatalf("source failed before redelivery ACK: %v", err)
	case <-ctx.Done():
		t.Fatal("redelivery after committed-but-disconnected attempt was not acknowledged")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	<-joined
	if connections.Load() != 2 || calls.Load() < 2 {
		t.Fatalf("reconnect/redelivery path was not exercised: connections=%d calls=%d", connections.Load(), calls.Load())
	}
	j, err := s.ClaimJob(context.Background())
	if err != nil || j == nil {
		t.Fatal("committed event was lost")
	}
	if j, err := s.ClaimJob(context.Background()); err != nil || j != nil {
		t.Fatal("redelivery created a second durable event")
	}
}
