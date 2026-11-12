package agui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	wire "github.com/viant/agently-core/protocol/agui"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func event(w http.ResponseWriter, value any) {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(w, "data: %s\n\n", data)
}

func begin(w http.ResponseWriter, in wire.RunAgentInput) {
	w.Header().Set("Content-Type", "text/event-stream")
	event(w, map[string]any{"type": "RUN_STARTED", "threadId": in.ThreadID, "runId": in.RunID, "protocolVersion": "1.0"})
}

func text(w http.ResponseWriter, id, content string, child bool) {
	start := map[string]any{"type": "TEXT_MESSAGE_START", "messageId": id, "role": "assistant", "metadata": map[string]any{"opaque": map[string]any{"keep": false, "integer": json.Number("9007199254740993")}}}
	delta := map[string]any{"type": "TEXT_MESSAGE_CONTENT", "messageId": id, "delta": content}
	end := map[string]any{"type": "TEXT_MESSAGE_END", "messageId": id}
	if child {
		for _, e := range []map[string]any{start, delta, end} {
			e["subagentRunId"] = "child"
		}
	}
	event(w, start)
	event(w, delta)
	event(w, end)
}

func finish(w http.ResponseWriter, in wire.RunAgentInput, outcome any) {
	e := map[string]any{"type": "RUN_FINISHED", "threadId": in.ThreadID, "runId": in.RunID}
	if outcome != nil {
		e["outcome"] = outcome
	}
	event(w, e)
}

func client(t *testing.T, server *httptest.Server) gateway.AgentlyClient {
	t.Helper()
	f, err := NewFactory(server.URL+"/v1/ag-ui/run", server.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.ForPrincipal(context.Background(), "test-principal")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNativeProtocolHistoryStateAndRootOutput(t *testing.T) {
	var count atomic.Int64
	long := strings.Repeat("Mysz w piekarni. 🐭 ", 700)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/ag-ui/run" || r.Method != http.MethodPost || r.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("unexpected native request path/method/accept")
		}
		var in wire.RunAgentInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			return
		}
		n := count.Add(1)
		if len(in.Tools) != 0 || len(in.ForwardedProps) != 0 || len(in.Resume) != 0 {
			t.Error("generic input offered tools or a producer-specific extension")
		}
		if n == 1 && len(in.Messages) != 1 {
			t.Errorf("first history=%d", len(in.Messages))
		}
		if n == 2 {
			if !strings.Contains(string(in.State), `"counter":2`) || !strings.Contains(string(in.State), `9007199254740993`) {
				t.Errorf("state was not preserved: %s", in.State)
			}
			foundAnswer := false
			for _, m := range in.Messages {
				if m.Role == "activity" {
					t.Error("activity returned to producer")
				}
				if m.ID == "answer" {
					foundAnswer = true
					var metadata struct {
						Opaque map[string]json.RawMessage `json:"opaque"`
					}
					if err := json.Unmarshal(m.Metadata, &metadata); err != nil {
						t.Error(err)
					}
					encoded, err := json.Marshal(m)
					if err != nil || string(metadata.Opaque["integer"]) != "9007199254740993" || string(metadata.Opaque["keep"]) != "false" || !strings.Contains(string(encoded), `"integer":9007199254740993`) {
						t.Errorf("native wire.Message lost opaque metadata precision/presence: metadata=%s err=%v", m.Metadata, err)
					}
				}
			}
			if !foundAnswer {
				t.Error("followup omitted the assistant message carrying opaque metadata")
			}
			if len(in.Messages) != 4 || in.Messages[len(in.Messages)-1].ID != "followup" {
				t.Errorf("followup did not retain full protocol history: %+v", in.Messages)
			}
		}
		begin(w, in)
		event(w, json.RawMessage(`{"type":"STATE_SNAPSHOT","snapshot":{"counter":0,"opaque":{"number":9007199254740993}}}`))
		event(w, json.RawMessage(`{"type":"STATE_DELTA","delta":[{"op":"replace","path":"/counter","value":2}]}`))
		if n == 1 {
			event(w, map[string]any{"type": "SUBAGENT_STARTED", "subagentRunId": "child", "name": "helper"})
			text(w, "child-answer", "PRIVATE CHILD TEXT", true)
			event(w, map[string]any{"type": "SUBAGENT_FINISHED", "subagentRunId": "child"})
			event(w, map[string]any{"type": "ACTIVITY_SNAPSHOT", "messageId": "activity", "activityType": "progress", "content": map[string]any{"phase": "complete"}})
			text(w, "answer", long, false)
		} else {
			text(w, "summary", "A mouse helps the baker.", false)
		}
		finish(w, in, nil)
	}))
	defer server.Close()
	c := client(t, server)
	thread, err := c.CreateConversation(context.Background(), "agent")
	if err != nil || thread == "" {
		t.Fatalf("local conversation: %q %v", thread, err)
	}
	if count.Load() != 0 {
		t.Fatal("local thread creation called the backend")
	}
	observed := 0
	result, err := c.Query(context.Background(), gateway.Query{ConversationID: thread, MessageID: "story", Text: "Write a story."}, func(turn gateway.Turn) error {
		observed++
		if turn.State != "running" || turn.Content != "" {
			t.Error("partial text was presented as a completed reply")
		}
		return nil
	})
	if err != nil || result.State != "completed" || result.Content != long || observed != 1 {
		t.Fatalf("story state=%q length=%d observed=%d err=%v", result.State, len(result.Content), observed, err)
	}
	if !strings.Contains(string(result.ProtocolMessages), `"activityType":"progress"`) || !strings.Contains(string(result.ProtocolMessages), `"keep":false`) {
		t.Fatal("durable projection lost activity or opaque metadata")
	}
	summary, err := c.Query(context.Background(), gateway.Query{ConversationID: thread, MessageID: "followup", Text: "Summarize.", ProtocolMessages: result.ProtocolMessages, ProtocolState: result.ProtocolState}, nil)
	if err != nil || summary.Content != "A mouse helps the baker." || count.Load() != 2 {
		t.Fatalf("followup=%+v err=%v calls=%d", summary, err, count.Load())
	}
}

func TestUncertainRunDoesNotReplayOrPublishPartialText(t *testing.T) {
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		var in wire.RunAgentInput
		_ = json.NewDecoder(r.Body).Decode(&in)
		begin(w, in)
		text(w, "answer", "Partial answer", false)
		// A clean HTTP EOF does not prove that execution finished.
	}))
	defer server.Close()
	c := client(t, server)
	result, err := c.Query(context.Background(), gateway.Query{ConversationID: "thread", MessageID: "run", Text: "Question"}, nil)
	if !errors.Is(err, gateway.ErrBackendProtocol) || result.State != "unknown" || result.Content != "" {
		t.Fatalf("uncertain result=%+v err=%v", result, err)
	}
	_, err = c.ReadTurn(context.Background(), "thread", "run", nil)
	if !errors.Is(err, gateway.ErrBackendProtocol) || count.Load() != 1 {
		t.Fatalf("read-only reconciliation replayed a prompt: calls=%d err=%v", count.Load(), err)
	}
}

func TestHistoricalReplayAndAuthoritativeSnapshotDoNotBecomeCurrentAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in wire.RunAgentInput
		_ = json.NewDecoder(r.Body).Decode(&in)
		old := in
		old.RunID = "earlier-run"
		begin(w, old)
		text(w, "historical-answer", "HISTORICAL TEXT", false)
		finish(w, old, nil)
		begin(w, in)
		event(w, map[string]any{"type": "MESSAGES_SNAPSHOT", "messages": []any{
			map[string]any{"id": "historical-answer", "role": "assistant", "content": "HISTORICAL TEXT"},
			map[string]any{"id": in.RunID, "role": "user", "content": "Question"},
			map[string]any{"id": "snapshot-answer", "role": "assistant", "content": "Current answer"},
		}})
		finish(w, in, nil)
	}))
	defer server.Close()
	result, err := client(t, server).Query(context.Background(), gateway.Query{ConversationID: "thread", MessageID: "current-run", Text: "Question"}, nil)
	if err != nil || result.State != "completed" || result.Content != "Current answer" {
		t.Fatalf("historical/snapshot output=%+v err=%v", result, err)
	}
	if !strings.Contains(string(result.ProtocolMessages), "HISTORICAL TEXT") {
		t.Fatal("authoritative history was lost")
	}
}

func TestNativeTerminalOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		outcome     any
		runError    bool
	}{
		{name: "cancelled", state: "canceled", outcome: map[string]any{"type": "cancelled"}},
		{name: "interrupt", state: "waiting_for_user", outcome: map[string]any{"type": "interrupt", "interrupts": []any{map[string]any{"id": "decision", "reason": "approval", "message": "Confirm?"}}}},
		{name: "error", state: "failed", runError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var in wire.RunAgentInput
				_ = json.NewDecoder(r.Body).Decode(&in)
				n := calls.Add(1)
				begin(w, in)
				if n == 2 {
					if !strings.Contains(string(in.State), `"integer":9007199254740993`) || len(in.Messages) != 3 {
						t.Errorf("confirmed terminal snapshot lost state/history: state=%s messages=%d", in.State, len(in.Messages))
					}
					text(w, "next-answer", "Next answer", false)
					finish(w, in, nil)
					return
				}
				event(w, json.RawMessage(`{"type":"STATE_SNAPSHOT","snapshot":{"integer":9007199254740993}}`))
				text(w, "partial-answer", "Not a successful answer", false)
				if tc.runError {
					event(w, map[string]any{"type": "RUN_ERROR", "message": "synthetic backend detail"})
				} else {
					finish(w, in, tc.outcome)
				}
			}))
			defer server.Close()
			c := client(t, server)
			result, err := c.Query(context.Background(), gateway.Query{ConversationID: "thread", MessageID: "run", Text: "Question"}, nil)
			if err != nil || result.State != tc.state || result.Content != "" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if !strings.Contains(string(result.ProtocolState), "9007199254740993") || !strings.Contains(string(result.ProtocolMessages), "partial-answer") {
				t.Fatal("confirmed terminal did not return its native protocol snapshot")
			}
			if tc.state == "failed" || tc.state == "canceled" {
				followup, followupErr := c.Query(context.Background(), gateway.Query{ConversationID: "thread", MessageID: "followup", Text: "Next question", ProtocolMessages: result.ProtocolMessages, ProtocolState: result.ProtocolState}, nil)
				if followupErr != nil || followup.Content != "Next answer" || calls.Load() != 2 {
					t.Fatalf("confirmed terminal continuation=%+v calls=%d err=%v", followup, calls.Load(), followupErr)
				}
			}
		})
	}
}

func TestFactoryCredentialBoundary(t *testing.T) {
	for _, endpoint := range []string{"http://example.com/v1/ag-ui/run", "https://user:secret@example.com/v1/ag-ui/run", "https://example.com/v1/ag-ui/run#fragment"} {
		if _, err := NewFactory(endpoint, nil, nil); err == nil {
			t.Fatalf("accepted invalid credential boundary %q", endpoint)
		}
	}
	if _, err := NewFactory("https://example.com/v1/ag-ui/run", nil, nil); err == nil {
		t.Fatal("accepted unauthenticated remote endpoint")
	}
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer synthetic-token" {
			t.Error("native SDK did not request credentials")
		}
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "synthetic-secret-response")
	}))
	defer server.Close()
	f, err := NewFactory(server.URL+"/v1/ag-ui/run", server.Client(), func(context.Context) (string, error) {
		calls.Add(1)
		return "synthetic-token", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c, err := f.ForPrincipal(context.Background(), "principal")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Query(context.Background(), gateway.Query{ConversationID: "thread", MessageID: "run", Text: "Question"}, nil)
	if !errors.Is(err, gateway.ErrBackendProtocol) || calls.Load() != 1 || strings.Contains(err.Error(), "synthetic") {
		t.Fatalf("unsafe native error/token handling: calls=%d err=%v", calls.Load(), err)
	}
}
