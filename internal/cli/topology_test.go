package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"gopkg.in/yaml.v3"
)

func writeTopology(t *testing.T, tokens bool) string {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "..", "examples", "gateway.multi.example.yaml"))
	storetest.Must(t, err)
	var document map[string]any
	storetest.Must(t, yaml.Unmarshal(body, &document))
	if tokens {
		for _, value := range document["integrations"].([]any) {
			installation := value.(map[string]any)["slack"].(map[string]any)
			for field, token := range map[string]string{"app_token_secret": "xapp-synthetic", "bot_token_secret": "xoxb-synthetic"} {
				resource := &scy.Resource{URL: "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "token.enc")), Key: "blowfish://default"}
				storetest.Must(t, scy.New().Store(context.Background(), scy.NewSecret(token, resource)))
				installation[field] = map[string]any{"URL": resource.URL, "Key": resource.Key}
			}
		}
	}
	encoded, err := yaml.Marshal(document)
	storetest.Must(t, err)
	path := filepath.Join(t.TempDir(), "config.yaml")
	storetest.Must(t, os.WriteFile(path, encoded, 0600))
	return path
}

func TestValidateMultipleIntegrationsAndSelectedPolicyOffline(t *testing.T) {
	path := writeTopology(t, false)
	for _, selected := range []string{"", "support"} {
		var out bytes.Buffer
		args := []string{"validate-config", "--config", path}
		if selected != "" {
			args = append(args, "--integration", selected)
		}
		storetest.Must(t, execute(context.Background(), args, &out, dependencies{}))
		var document map[string]any
		storetest.Must(t, json.Unmarshal(out.Bytes(), &document))
		if selected == "" {
			if len(document["integrations"].([]any)) != 2 {
				t.Fatal("missing integration policy reports")
			}
		} else if document["integration_name"] != selected || document["app_id"] != "A_SUPPORT" {
			t.Fatal("wrong selected policy")
		}
		if strings.Contains(out.String(), "agently.example.com") || strings.Contains(out.String(), "token.enc") {
			t.Fatal("unnecessary endpoint or credential-reference disclosure")
		}
	}
	files, err := os.ReadDir(filepath.Dir(path))
	storetest.Must(t, err)
	if len(files) != 1 {
		t.Fatal("offline validation opened or created storage")
	}
}

type integrationStateWriter struct {
	mu    sync.Mutex
	ready map[string]chan struct{}
	logs  bytes.Buffer
}

func (w *integrationStateWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, _ = w.logs.Write(p)
	if strings.Contains(string(p), "state=ready") {
		for id, ready := range w.ready {
			if strings.Contains(string(p), "integration_name="+id) {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
		}
	}
	return len(p), nil
}

type integrationBackend struct{ agent string }

func (b integrationBackend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return b, nil
}
func (b integrationBackend) CreateConversation(context.Context, string) (string, error) {
	return "same-native-id-in-different-integrations", nil
}
func (b integrationBackend) Query(_ context.Context, query gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	if query.AgentID != b.agent {
		return gateway.Turn{}, fmt.Errorf("incorrect synthetic agent selection")
	}
	return gateway.Turn{ID: query.MessageID, State: "completed", Content: "reply-from-" + b.agent}, nil
}
func (b integrationBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, fmt.Errorf("unexpected synthetic attachment")
}

type integrationSender struct {
	want string
	sent *atomic.Int32
	stop context.CancelFunc
}

func (s integrationSender) Send(_ context.Context, delivery gateway.SlackOutgoingMessage) (string, error) {
	if delivery.Text != s.want {
		return "", fmt.Errorf("response crossed integration boundary")
	}
	if s.sent.Add(1) == 2 {
		s.stop()
	}
	return "2.000001", nil
}
func (integrationSender) SendStatus(_ context.Context, status gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	if status.Target == gateway.StatusTargetAgentSession {
		return gateway.StatusReceipt{Acknowledged: true, AgentStatus: status.SessionState}, nil
	}
	if status.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: status.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}

func TestRunUsesOneDatabaseAndKeepsDifferentIntegrationsSeparate(t *testing.T) {
	path := writeTopology(t, true)
	body, err := os.ReadFile(path)
	storetest.Must(t, err)
	var document map[string]any
	storetest.Must(t, yaml.Unmarshal(body, &document))
	integrations := document["integrations"].([]any)
	integrations[1].(map[string]any)["backend"] = map[string]any{"type": "agently", "base_url": "http://127.0.0.1:9192", "principal_id": "second-principal", "agent_id": "support-agent", "auth": map[string]any{"mode": "oob_session", "secret": map[string]any{"URL": "/unused/synthetic-oob", "Key": "blowfish://default"}}}
	for _, value := range integrations {
		installation := value.(map[string]any)["slack"].(map[string]any)
		installation["channels"] = map[string]any{"mode": "selected", "allowlist": []string{"C"}}
		installation["status"] = map[string]any{"mode": "message"}
	}
	body, err = yaml.Marshal(document)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(path, body, 0600))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	states := &integrationStateWriter{ready: map[string]chan struct{}{"steward": make(chan struct{}), "support": make(chan struct{})}}
	var sent atomic.Int32
	deps := dependencies{
		login: func(_ context.Context, _, _, agent string, _ *scy.Resource) (gateway.AgentlyClientFactory, error) {
			return integrationBackend{agent: agent}, nil
		},
		directory: func(config.SlackTokens, gateway.Route) gateway.SlackDirectory { return &routedDirectory{} },
		connect: func(_ context.Context, _ config.SlackTokens, route gateway.Route, _ *slog.Logger) (runner.Source, gateway.SlackSender, error) {
			id, agent := "steward", "steward"
			base := integrations[0].(map[string]any)["backend"].(map[string]any)["base_url"].(string)
			if route.AppID == "A_SUPPORT" {
				id, agent = "support", "support-agent"
				base = integrations[1].(map[string]any)["backend"].(map[string]any)["base_url"].(string)
			}
			want := "[View full response in Agently](" + base + "/conversation/same-native-id-in-different-integrations)\n\nreply-from-" + agent
			return idleSource(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
				ready()
				select {
				case <-states.ready[id]:
				case <-ctx.Done():
					return nil
				}
				message := storetest.Message(id+"-event", "same-thread", "alice")
				message.AppID, message.WorkspaceID = route.AppID, route.WorkspaceID
				if _, err := sink.Accept(ctx, message); err != nil {
					return err
				}
				<-ctx.Done()
				return nil
			}), integrationSender{want: want, sent: &sent, stop: cancel}, nil
		},
	}
	storetest.Must(t, execute(ctx, []string{"run", "--config", path}, io.Writer(states), deps))
	if sent.Load() != 2 {
		states.mu.Lock()
		logs := states.logs.String()
		states.mu.Unlock()
		t.Fatalf("expected both integrations to deliver: %d; logs=%s", sent.Load(), logs)
	}
	root, err := sqlite.Open(context.Background(), filepath.Join(filepath.Dir(path), "gateway-multi.sqlite"), false)
	storetest.Must(t, err)
	defer root.Close()
	for _, id := range []string{"steward", "support"} {
		status, err := root.ScopeName(id).Status(context.Background(), 10)
		storetest.Must(t, err)
		if len(status.GatewayJobs) != 1 || len(status.ConversationBindings) != 1 || len(status.SlackOutgoingMessages) != 1 || status.SlackOutgoingMessages[0].State != "delivered" {
			t.Fatalf("incorrect persisted integration %s: %#v", id, status)
		}
	}
}

func TestExplicitEmptyIntegrationsValidateAndRunWithoutCredentials(t *testing.T) {
	path := writeConfig(t, "storage: {driver: sqlite, path: empty.sqlite}\nintegrations: []\n")
	var out bytes.Buffer
	storetest.Must(t, execute(context.Background(), []string{"validate-config", "--config", path}, &out, dependencies{}))
	var report struct {
		Valid        bool           `json:"valid"`
		Integrations []policyReport `json:"integrations"`
	}
	storetest.Must(t, json.Unmarshal(out.Bytes(), &report))
	if !report.Valid || report.Integrations == nil || len(report.Integrations) != 0 {
		t.Fatalf("incorrect empty configuration report: %s", out.String())
	}
	out.Reset()
	storetest.Must(t, execute(context.Background(), []string{"run", "--config", path}, &out, dependencies{}))
	if !strings.Contains(out.String(), "No active integrations configured") {
		t.Fatal("empty run did not explain local shutdown")
	}
}
