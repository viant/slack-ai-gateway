package agently

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-core/protocol/agui"
	"github.com/viant/agently-core/runtime/streaming"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// The producer descriptor comes from the exact pinned native translator. The
// client below consumes it through Core's SDK SSE transport, not a fake reader.
func nativeTurnFrame026(t *testing.T, eventType streaming.EventType) string {
	t.Helper()
	producer := agui.NewTranslator("c", "m")
	if err := producer.SetNativeIdentity("native"); err != nil {
		t.Fatal(err)
	}
	producer.Start()
	for _, event := range producer.Translate(&streaming.Event{Type: eventType, ConversationID: "c", TurnID: "native"}) {
		body, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var header struct{ Type, ActivityType string }
		if err = json.Unmarshal(body, &header); err != nil {
			t.Fatal(err)
		}
		if header.Type == "ACTIVITY_SNAPSHOT" && header.ActivityType == "agently.turn" {
			return fmt.Sprintf("data: %s\n\n", body)
		}
	}
	t.Fatal("native producer did not emit a turn descriptor")
	return ""
}

func TestAGUI026LiveProgressWaitsForTurnAndSuppressesDuplicateWork(t *testing.T) {
	queued := nativeTurnFrame026(t, streaming.EventTypeTurnQueued)
	running := nativeTurnFrame026(t, streaming.EventTypeTurnStarted)
	finish := make(chan struct{})
	initialRead := make(chan struct{})
	framesSent := make(chan struct{})
	var completed atomic.Bool
	var reads atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/conversations/c":
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/ag-ui/run":
			writeRunStarted(w, "c", "m", "native")
			w.(http.Flusher).Flush()
			select {
			case <-initialRead:
			case <-r.Context().Done():
				return
			}
			fmt.Fprint(w, queued)
			for range 1000 {
				fmt.Fprint(w, running)
				fmt.Fprint(w, "data: {\"type\":\"TEXT_MESSAGE_CONTENT\",\"messageId\":\"assistant\",\"delta\":\"text\"}\n\n")
			}
			fmt.Fprint(w, "data: {\"type\":\"RUN_FINISHED\",\"subagentRunId\":\"child\"}\n\n")
			w.(http.Flusher).Flush()
			close(framesSent)
			select {
			case <-finish:
			case <-r.Context().Done():
				return
			}
			completed.Store(true)
			fmt.Fprint(w, "data: {\"type\":\"RUN_FINISHED\",\"threadId\":\"c\",\"runId\":\"m\"}\n\n")
		case "/v1/conversations/c/transcript":
			if reads.Add(1) == 1 {
				fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[]}}`)
				close(initialRead)
				return
			}
			if !completed.Load() {
				t.Error("live progress or child event triggered a canonical read")
			}
			fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"native","status":"completed","assistant":{"final":{"content":"canonical final"}}}]}}`)
		}
	})
	c.(*Client).factory.http.Timeout = 30 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	states := make(chan string, 4)
	done := make(chan error, 1)
	go func() {
		turn, err := c.Query(ctx, gateway.Query{ConversationID: "c", MessageID: "m", AgentID: "a", Text: "hello"}, func(turn gateway.Turn) error {
			select {
			case states <- turn.State:
				return nil
			default:
				return errors.New("duplicate observer work")
			}
		})
		if err == nil && turn.Content != "canonical final" {
			err = fmt.Errorf("noncanonical result: %+v", turn)
		}
		done <- err
	}()
	for _, want := range []string{"queued", "running"} {
		select {
		case got := <-states:
			if got != want {
				t.Fatalf("observed %q, want %q", got, want)
			}
		case <-ctx.Done():
			t.Fatal("native progress did not reach observer")
		}
	}
	<-framesSent
	select {
	case err := <-done:
		t.Fatalf("healthy quiet stream ended: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if reads.Load() != 1 || len(states) != 0 {
		t.Fatalf("per-fragment work: canonical reads=%d pending observations=%d", reads.Load(), len(states))
	}
	close(finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("root terminal did not return result")
	}
	if reads.Load() != 2 || <-states != "completed" || len(states) != 0 {
		t.Fatal("terminal observation was duplicated or not confirmed canonically")
	}
}

func TestAGUI026ReplayedProgressCannotConfirmUnknown(t *testing.T) {
	queued := nativeTurnFrame026(t, streaming.EventTypeTurnQueued)
	running := nativeTurnFrame026(t, streaming.EventTypeTurnStarted)
	finish := make(chan struct{})
	framesSent := make(chan struct{})
	var completed atomic.Bool
	var reads, observations atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/conversations/c":
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/ag-ui/run":
			checkRunInput(t, r, "c", "m", "run.attach", "", "")
			writeRunStarted(w, "c", "m", "native")
			for range 1000 {
				fmt.Fprint(w, queued, running)
			}
			w.(http.Flusher).Flush()
			close(framesSent)
			select {
			case <-finish:
			case <-r.Context().Done():
				return
			}
			completed.Store(true)
			fmt.Fprint(w, "data: {\"type\":\"RUN_FINISHED\",\"threadId\":\"c\",\"runId\":\"m\"}\n\n")
		case "/v1/conversations/c/transcript":
			reads.Add(1)
			if !completed.Load() {
				fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[]}}`)
				return
			}
			fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"native","status":"completed","assistant":{"final":{"content":"final"}}}]}}`)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.ReadTurn(ctx, "c", "m", func(turn gateway.Turn) error {
			observations.Add(1)
			if turn.State != "completed" {
				return fmt.Errorf("historical state confirmed observation: %s", turn.State)
			}
			return nil
		})
		done <- err
	}()
	<-framesSent
	select {
	case err := <-done:
		t.Fatalf("initial unknown/replay ended attachment: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if reads.Load() != 3 || observations.Load() != 0 {
		t.Fatalf("replay caused unbounded reads or false confirmation: reads=%d observations=%d", reads.Load(), observations.Load())
	}
	close(finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("canonical completion did not finish attachment")
	}
	if reads.Load() != 4 || observations.Load() != 1 {
		t.Fatal("terminal root was not confirmed exactly once")
	}
}

func TestAGUI026ContradictoryAttachmentHintReturnsCanonicalState(t *testing.T) {
	running := nativeTurnFrame026(t, streaming.EventTypeTurnStarted)
	var reads atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/conversations/c":
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/ag-ui/run":
			writeRunStarted(w, "c", "m", "native")
			fmt.Fprint(w, running)
		case "/v1/conversations/c/transcript":
			reads.Add(1)
			fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"native","status":"queued"}]}}`)
		}
	})
	var states []string
	turn, err := c.ReadTurn(context.Background(), "c", "m", func(turn gateway.Turn) error {
		states = append(states, turn.State)
		return nil
	})
	if err != nil || turn.State != "queued" || reads.Load() != 2 || len(states) != 1 || states[0] != "queued" {
		t.Fatalf("contradictory hint must reuse scheduler attachment: turn=%+v err=%v reads=%d states=%v", turn, err, reads.Load(), states)
	}
}

func TestAGUI026ReplayDoesNotRegressCanonicalRunning(t *testing.T) {
	queued := nativeTurnFrame026(t, streaming.EventTypeTurnQueued)
	finish := make(chan struct{})
	framesSent := make(chan struct{})
	var completed atomic.Bool
	var reads, observations atomic.Int64
	c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/conversations/c":
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/ag-ui/run":
			writeRunStarted(w, "c", "m", "native")
			for range 1000 {
				fmt.Fprint(w, queued)
			}
			w.(http.Flusher).Flush()
			close(framesSent)
			select {
			case <-finish:
			case <-r.Context().Done():
				return
			}
			completed.Store(true)
			fmt.Fprint(w, "data: {\"type\":\"RUN_FINISHED\",\"threadId\":\"c\",\"runId\":\"m\"}\n\n")
		case "/v1/conversations/c/transcript":
			reads.Add(1)
			state := "running"
			if completed.Load() {
				state = "completed"
			}
			fmt.Fprintf(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"native","status":%q,"assistant":{"final":{"content":"final"}}}]}}`, state)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		turn, err := c.ReadTurn(ctx, "c", "m", func(turn gateway.Turn) error {
			observations.Add(1)
			if turn.State != "running" && turn.State != "completed" {
				return fmt.Errorf("replay regressed confirmed state to %s", turn.State)
			}
			return nil
		})
		if err == nil && turn.State != "completed" {
			err = fmt.Errorf("historical hint ended healthy attachment: %+v", turn)
		}
		done <- err
	}()
	<-framesSent
	select {
	case err := <-done:
		t.Fatalf("replay detached healthy running root: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if reads.Load() != 1 || observations.Load() != 1 {
		t.Fatalf("replay caused health polling: reads=%d observations=%d", reads.Load(), observations.Load())
	}
	close(finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("root terminal did not end attachment")
	}
	if reads.Load() != 2 || observations.Load() != 2 {
		t.Fatal("unexpected work after canonical completion")
	}
}

func TestAGUI026TurnSnapshotsRequireRootIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, extra, native, version string
		wantRunning                  bool
	}{
		{name: "root", native: "native", version: "1", wantRunning: true},
		{name: "other_native_turn", native: "other", version: "1"},
		{name: "other_thread", extra: `"threadId":"foreign",`, native: "native", version: "1"},
		{name: "other_run", extra: `"runId":"foreign",`, native: "native", version: "1"},
		{name: "subagent", extra: `"subagentRunId":"child",`, native: "native", version: "1"},
		{name: "unknown_version", native: "native", version: "2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var finished atomic.Bool
			var reads atomic.Int64
			c := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/conversations/c":
					fmt.Fprint(w, `{"id":"c"}`)
				case "/v1/ag-ui/run":
					writeRunStarted(w, "c", "m", "native")
					fmt.Fprintf(w, "data: {\"type\":\"ACTIVITY_SNAPSHOT\",%s\"activityType\":\"agently.turn\",\"content\":{\"version\":%q,\"nativeTurnId\":%q,\"status\":\"running\"}}\n\n", tc.extra, tc.version, tc.native)
					fmt.Fprint(w, "data: {\"type\":\"RUN_FINISHED\",\"threadId\":\"c\",\"runId\":\"m\"}\n\n")
				case "/v1/conversations/c/transcript":
					if reads.Add(1) == 1 {
						fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[]}}`)
						return
					}
					finished.Store(true)
					fmt.Fprint(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":"native","status":"completed","assistant":{"final":{"content":"final"}}}]}}`)
				}
			})
			var running bool
			_, err := c.Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "m", AgentID: "a", Text: "hello"}, func(turn gateway.Turn) error {
				running = running || turn.State == "running"
				return nil
			})
			if err != nil || running != tc.wantRunning || !finished.Load() {
				t.Fatalf("identity boundary: err=%v running=%t terminalRead=%t", err, running, finished.Load())
			}
		})
	}
}
