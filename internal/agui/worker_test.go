package agui

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	wire "github.com/viant/agently-core/protocol/agui"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type workerDatabase struct {
	root     *sqlite.Store
	scoped   storage.Store
	path     string
	identity storage.Integration
}

func workerFixture(t *testing.T, endpoint string) *workerDatabase {
	t.Helper()
	f := &workerDatabase{path: filepath.Join(t.TempDir(), "generic.sqlite"), identity: storage.Integration{
		Name:  "test",
		AppID: "A", WorkspaceID: "T", BackendType: "agui", BackendURL: endpoint, PrincipalID: "shared",
	}}
	var err error
	f.root, err = sqlite.Open(context.Background(), f.path, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if f.root != nil {
			if err := f.root.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	if err = f.root.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.register(t)
	return f
}

func (f *workerDatabase) register(t *testing.T) {
	t.Helper()
	registered, err := f.root.ReconcileIntegrations(context.Background(), []storage.Integration{f.identity})
	if err != nil {
		t.Fatal(err)
	}
	f.scoped = f.root.Scope(registered[0].ID)
}

func (f *workerDatabase) reopen(t *testing.T) {
	t.Helper()
	if err := f.root.Close(); err != nil {
		t.Fatal(err)
	}
	f.root = nil
	var err error
	f.root, err = sqlite.Open(context.Background(), f.path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.root.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.register(t)
	if err = f.root.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func (f *workerDatabase) inspect(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func workerMessage(eventID, root, timestamp, prompt string) gateway.Message {
	return gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: root},
		AppID: "A", EventID: eventID, AuthorID: "U", MessageTS: timestamp, Text: prompt, Mentioned: true}
}

func enqueueWorkerMessage(t *testing.T, store gateway.Store, message gateway.Message) {
	t.Helper()
	result, err := (gateway.Intake{Store: store, Route: storetest.Route("A", "T", "C")}).Accept(context.Background(), message)
	if err != nil || !result.Accepted || result.Duplicate {
		t.Fatalf("intake=%+v err=%v", result, err)
	}
}

func workerFor(t *testing.T, store gateway.Store, server *httptest.Server) gateway.Worker {
	t.Helper()
	factory, err := NewFactory(server.URL+"/v1/ag-ui/run", server.Client(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return gateway.Worker{Store: store, Clients: factory, Policy: storetest.Policy("A", "T", "shared", "C")}
}

func workerStep(t *testing.T, worker gateway.Worker, expected bool) {
	t.Helper()
	worked, err := worker.Step(context.Background())
	if err != nil || worked != expected {
		t.Fatalf("worked=%t want=%t err=%v", worked, expected, err)
	}
}

func TestWorkerPersistsGenericHistoryOutboxAndReviewAcrossRestart(t *testing.T) {
	var calls atomic.Int64
	story := strings.Repeat("The mouse helped the baker. 🐭\n", 700)
	storyJSON := string(mustJSON(t, story))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in wire.RunAgentInput
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			return
		}
		n := calls.Add(1)
		if n == 2 {
			if len(in.Messages) != 3 || in.Messages[0].Role != "user" || in.Messages[1].Role != "assistant" || string(in.Messages[1].Content) != storyJSON {
				t.Error("second run did not receive the complete persisted first turn")
			}
			if !strings.Contains(string(in.State), `9007199254740993`) || !strings.Contains(string(in.State), `"counter":2`) {
				t.Errorf("second run lost durable state: %s", in.State)
			}
		}
		begin(w, in)
		event(w, json.RawMessage(`{"type":"STATE_SNAPSHOT","snapshot":{"counter":2,"opaque":{"number":9007199254740993}}}`))
		if n == 1 {
			text(w, "story-answer", story, false)
		} else if n == 2 {
			text(w, "summary-answer", "The mouse helped the baker.", false)
		} else {
			text(w, "other-answer", "This remains partial when the stream is truncated.", false)
		}
		if n != 3 {
			finish(w, in, nil)
		}
	}))
	defer server.Close()
	f := workerFixture(t, server.URL+"/v1/ag-ui/run")
	rootTS := "100.000001"
	message := workerMessage("first", rootTS, rootTS, "Write a long story.")
	enqueueWorkerMessage(t, f.scoped, message)
	workerStep(t, workerFor(t, f.scoped, server), true)
	binding, err := f.scoped.GetBinding(context.Background(), message.ThreadKey)
	if err != nil || binding == nil || !strings.Contains(string(binding.ProtocolState), "9007199254740993") {
		t.Fatalf("persisted protocol binding=%+v err=%v", binding, err)
	}
	firstMessages, firstState := string(binding.ProtocolMessages), string(binding.ProtocolState)
	inspection := f.inspect(t)
	rows, err := inspection.Query("SELECT slack_message_text FROM slack_outgoing_message ORDER BY slack_message_index")
	if err != nil {
		t.Fatal(err)
	}
	var assembled strings.Builder
	parts := 0
	for rows.Next() {
		var part string
		if err = rows.Scan(&part); err != nil {
			t.Fatal(err)
		}
		assembled.WriteString(part)
		parts++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	_ = rows.Close()
	if parts < 2 || assembled.String() != story {
		t.Fatalf("multipart outbox lost response: parts=%d got=%d want=%d", parts, assembled.Len(), len(story))
	}
	f.reopen(t)
	if calls.Load() != 1 {
		t.Fatal("recovery submitted a prompt")
	}
	binding, err = f.scoped.GetBinding(context.Background(), message.ThreadKey)
	if err != nil || string(binding.ProtocolMessages) != firstMessages || string(binding.ProtocolState) != firstState {
		t.Fatal("protocol snapshot did not survive restart")
	}
	enqueueWorkerMessage(t, f.scoped, workerMessage("followup", rootTS, "101.000001", "Summarize the story."))
	workerStep(t, workerFor(t, f.scoped, server), true)
	if calls.Load() != 2 {
		t.Fatal("followup was not executed exactly once")
	}
	binding, err = f.scoped.GetBinding(context.Background(), message.ThreadKey)
	if err != nil || !strings.Contains(string(binding.ProtocolMessages), "summary-answer") {
		t.Fatal("followup snapshot was not persisted")
	}
	var summary string
	if err = inspection.QueryRow("SELECT gateway_execution_result_text FROM gateway_job WHERE slack_event_id='followup'").Scan(&summary); err != nil || summary != "The mouse helped the baker." {
		t.Fatalf("followup result=%q err=%v", summary, err)
	}
	previousMessages := string(binding.ProtocolMessages)
	enqueueWorkerMessage(t, f.scoped, workerMessage("truncated", rootTS, "102.000001", "Start another story."))
	workerStep(t, workerFor(t, f.scoped, server), true)
	var disposition string
	if err = inspection.QueryRow("SELECT gateway_execution_state FROM gateway_job WHERE slack_event_id='truncated'").Scan(&disposition); err != nil || disposition != "needs_review" {
		t.Fatalf("truncated state=%q err=%v", disposition, err)
	}
	binding, err = f.scoped.GetBinding(context.Background(), message.ThreadKey)
	if err != nil || string(binding.ProtocolMessages) != previousMessages {
		t.Fatal("partial result overwrote the confirmed protocol snapshot")
	}
	enqueueWorkerMessage(t, f.scoped, workerMessage("blocked-followup", rootTS, "103.000001", "Continue."))
	workerStep(t, workerFor(t, f.scoped, server), false)
	f.reopen(t)
	workerStep(t, workerFor(t, f.scoped, server), false)
	if calls.Load() != 3 {
		t.Fatalf("uncertain prompt was replayed: calls=%d", calls.Load())
	}
	enqueueWorkerMessage(t, f.scoped, workerMessage("independent", "200.000001", "200.000001", "An independent question."))
	workerStep(t, workerFor(t, f.scoped, server), true)
	if calls.Load() != 4 {
		t.Fatal("review blocked an independent thread")
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestWorkerRollsBackProtocolSnapshotWithFailedOutbox(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var in wire.RunAgentInput
		_ = json.NewDecoder(r.Body).Decode(&in)
		begin(w, in)
		event(w, json.RawMessage(`{"type":"STATE_SNAPSHOT","snapshot":{"confirmed":true}}`))
		text(w, "answer", strings.Repeat("A", gateway.MaxMarkdownCharacters+1), false)
		finish(w, in, nil)
	}))
	defer server.Close()
	f := workerFixture(t, server.URL+"/v1/ag-ui/run")
	db := f.inspect(t)
	if _, err := db.Exec("CREATE TRIGGER reject_protocol_part BEFORE INSERT ON slack_outgoing_message WHEN NEW.slack_message_index=1 BEGIN SELECT RAISE(ABORT,'synthetic outbox failure'); END"); err != nil {
		t.Fatal(err)
	}
	enqueueWorkerMessage(t, f.scoped, workerMessage("atomic", "100.000001", "100.000001", "Write a long answer."))
	worked, err := workerFor(t, f.scoped, server).Step(context.Background())
	if !worked || err == nil {
		t.Fatalf("injected outbox failure: worked=%t err=%v", worked, err)
	}
	var disposition, messages, state string
	var result sql.NullString
	var parts int
	if err = db.QueryRow("SELECT gateway_execution_state,gateway_execution_result_text FROM gateway_job WHERE slack_event_id='atomic'").Scan(&disposition, &result); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT backend_protocol_messages,backend_protocol_state FROM conversation_binding").Scan(&messages, &state); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM slack_outgoing_message").Scan(&parts); err != nil {
		t.Fatal(err)
	}
	if disposition != "querying" || result.Valid || messages != "[]" || state != "{}" || parts != 0 || calls.Load() != 1 {
		t.Fatalf("partial transaction: state=%q result=%+v messages=%q protocol=%q parts=%d calls=%d", disposition, result, messages, state, parts, calls.Load())
	}
	if _, err = db.Exec("DROP TRIGGER reject_protocol_part"); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	workerStep(t, workerFor(t, f.scoped, server), true)
	if calls.Load() != 1 {
		t.Fatal("recovery replayed the completed but uncommitted generic run")
	}
	if err = db.QueryRow("SELECT gateway_execution_state FROM gateway_job WHERE slack_event_id='atomic'").Scan(&disposition); err != nil || disposition != "needs_review" {
		t.Fatalf("unknown completion state=%q err=%v", disposition, err)
	}
}
