package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
	"gopkg.in/yaml.v3"
)

const agentlyTestConfig = `backend:
  type: agently
  base_url: http://127.0.0.1:9191
  agent_id: steward
  principal_id: agently_steward
  auth:
    mode: oob_session
    secret: {URL: /unused/synthetic-secret-reference, Key: 'blowfish://default'}
`

func TestValidateConfigIsOfflineAndReportsEffectivePolicy(t *testing.T) {
	t.Setenv("SLACK_APP_TOKEN", "")
	t.Setenv("SLACK_BOT_TOKEN", "")
	t.Setenv("AGENTLY_OOB_SECRETS", "/unused/ignored-env-secret-marker")
	path := writeConfig(t, `storage: {driver: sqlite, path: nonexistent.sqlite}
transport: socket
slack: {app_id: A, workspace_id: T}
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    users: {scope: workspace_members_only, denylist: [blocked]}
    channel_overrides:
      C: {enabled: true, users: {scope: workspace_members_only, allowlist: []}}
`+agentlyTestConfig)
	for _, id := range []string{"C", "OTHER"} {
		var out bytes.Buffer
		// Nil dependencies panic if validation unexpectedly logs in or connects.
		storetest.Must(t, execute(context.Background(), []string{"validate-config", "--config", path, "--integration", "steward", "--channel-id", id}, &out, dependencies{}))
		var report struct {
			Valid        bool                             `json:"valid"`
			ResponseMode string                           `json:"response_mode"`
			Policies     []gateway.EffectiveChannelPolicy `json:"effective_policies"`
		}
		storetest.Must(t, json.Unmarshal(out.Bytes(), &report))
		if !report.Valid || report.ResponseMode != "raw" || len(report.Policies) != 1 || report.Policies[0].ChannelID != id {
			t.Fatal("incorrect report")
		}
		if report.Policies[0].AllowsUser("alice") != (id == "OTHER") || report.Policies[0].AllowsUser("blocked") {
			t.Fatal("effective policy differs")
		}
		files, err := os.ReadDir(filepath.Dir(path))
		storetest.Must(t, err)
		if len(files) != 1 {
			t.Fatal("validation created storage or lock")
		}
		if strings.Contains(out.String(), "secrets") || strings.Contains(out.String(), "base_url") {
			t.Fatal("unnecessary configuration disclosure")
		}
		if !strings.Contains(out.String(), `"scope": "workspace_channels_with_bot"`) || !strings.Contains(out.String(), `"user_scope": "workspace_members_only"`) || !strings.Contains(out.String(), `"rule_source"`) || strings.Contains(out.String(), `"user_mode"`) {
			t.Fatal("policy diagnostics use obsolete field names")
		}

	}
}

type routedBackend struct{ queries atomic.Int64 }

func (b *routedBackend) ForPrincipal(_ context.Context, p string) (gateway.AgentlyClient, error) {
	if p != "agently_steward" {
		return nil, gateway.ErrBackendAuth
	}
	return b, nil
}
func (b *routedBackend) CreateConversation(context.Context, string) (string, error) {
	return "offline-conversation", nil
}
func (b *routedBackend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: "offline reply"}, nil
}
func (b *routedBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, errors.New("unexpected read")
}

type routedDirectory struct{ fresh atomic.Int64 }

func (d *routedDirectory) Channel(_ context.Context, w, c string, f bool) (gateway.ChannelInfo, error) {
	if f {
		d.fresh.Add(1)
	}
	return gateway.ChannelInfo{WorkspaceID: w, ID: c, Member: true}, nil
}
func (*routedDirectory) User(_ context.Context, workspace, user string) (gateway.UserInfo, error) {
	return gateway.UserInfo{WorkspaceID: workspace, ID: user}, nil
}

type routedSender struct {
	calls        atomic.Int64
	statuses     atomic.Int64
	cancel       context.CancelFunc
	wantResponse string
}

func (s *routedSender) Send(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
	if d.ChannelID != "C" && d.ChannelID != "C2" {
		return "", errors.New("wrong route")
	}
	if s.wantResponse != "" && d.Text != s.wantResponse {
		return "", errors.New("configured presentation did not reach delivery")
	}
	if s.calls.Add(1) == 2 {
		s.cancel()
	}
	return "2.001", nil
}

func TestRunWiresMultiChannelPolicyForQueriesAndReplies(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		path := writeConfigWithTokens(t, `storage: {driver: sqlite, path: real.sqlite}
transport: socket
slack: {app_id: A, workspace_id: T, response: {mode: summary}}
access:
  channels:
    enabled: true
    scope: selected
    allowlist: [C, C2]
    users: {scope: workspace_members_only, denylist: [mallory]}
`+agentlyTestConfig)
		storetest.Must(t, Execute(context.Background(), []string{"migrate", "--config", path}, &bytes.Buffer{}))
		// Both replies and their statuses share one-second pacing. This is a wiring
		// check, so leave room for race-instrumented SQLite rather than timing it.
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		backend := &routedBackend{}
		backendReady := make(chan struct{})
		out := &backendStateWriter{state: "ready", reached: backendReady}
		directory := &routedDirectory{}
		sender := &routedSender{cancel: cancel, wantResponse: "[View full response in Agently](http://127.0.0.1:9191/conversation/offline-conversation)\n\noffline reply"}
		deps := dependencies{
			login: func(context.Context, string, string, string, *scy.Resource) (gateway.AgentlyClientFactory, error) {
				return backend, nil
			},
			directory: func(config.SlackTokens, gateway.Route) gateway.SlackDirectory { return directory },
			connect: func(_ context.Context, _ config.SlackTokens, route gateway.Route, _ *slog.Logger) (runner.Source, gateway.SlackSender, error) {
				if route.Resolver == nil {
					t.Fatal("runtime used legacy single-channel route")
				}
				return idleSource(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
					ready()
					select {
					case <-backendReady:
					case <-ctx.Done():
						return nil
					}
					for i, author := range []string{"alice", "bob", "mallory"} {
						m := storetest.Message(author, author, author)
						if i != 0 {
							m.ChannelID = "C2"
						}
						_, err := sink.Accept(ctx, m)
						if err != nil {
							return err
						}
					}
					<-ctx.Done()
					return nil
				}), sender, nil
			},
		}

		runStarted := time.Now()
		if err := execute(ctx, []string{"run", "--config", path}, out, deps); err != nil {
			t.Fatalf("runtime failed after %s: %v\nruntime logs:\n%s", time.Since(runStarted), err, out.String())
		}
		if backend.queries.Load() != 2 || sender.calls.Load() != 2 || directory.fresh.Load() != 2+sender.statuses.Load() {
			t.Fatalf("queries=%d sends=%d statuses=%d fresh=%d", backend.queries.Load(), sender.calls.Load(), sender.statuses.Load(), directory.fresh.Load())
		}
		s, err := sqlite.Open(context.Background(), filepath.Join(filepath.Dir(path), "real.sqlite"), false)
		storetest.Must(t, err)
		defer s.Close()
		status, err := s.Status(context.Background(), 10)
		storetest.Must(t, err)
		if status.Counts["gateway_job.gateway_execution_state"]["rejected"] != 1 || status.Counts["slack_outgoing_message.slack_delivery_state"]["delivered"] != 2 {
			t.Fatal("policy not applied to durable runtime")
		}

	})
}

func TestCheckAgentlyNeedsNoStorageOrSlack(t *testing.T) {
	t.Setenv("AGENTLY_OOB_SECRETS", "/unused/ignored-env-secret-marker")
	path := writeConfig(t, agentlyTestConfig)
	var out bytes.Buffer
	called := 0
	deps := dependencies{login: func(_ context.Context, base, principal, agent string, ref *scy.Resource) (gateway.AgentlyClientFactory, error) {
		called++
		if base != "http://127.0.0.1:9191" || principal != "agently_steward" || agent != "steward" || (ref == nil || ref.URL != "/unused/synthetic-secret-reference" || ref.Key != "blowfish://default") {
			t.Fatal("incorrect login selection")
		}
		return nil, nil // check only consumes the validated login outcome
	}}
	storetest.Must(t, execute(context.Background(), []string{"check-agently", "--config", path}, &out, deps))
	if called != 1 || !strings.Contains(out.String(), "No conversation, query or Slack") || strings.Contains(out.String(), "synthetic-secret") {
		t.Fatal("unsafe check output")
	}
	files, err := os.ReadDir(filepath.Dir(path))
	storetest.Must(t, err)
	if len(files) != 1 {
		t.Fatal("check-agently created storage")
	}
}

type idleSource func(context.Context, gateway.EventSink, func()) error

func (f idleSource) Run(ctx context.Context, sink gateway.EventSink, ready func()) error {
	return f(ctx, sink, ready)
}

type unusedSender struct{}

func (unusedSender) Send(context.Context, gateway.SlackOutgoingMessage) (string, error) {
	return "", errors.New("unexpected send")
}

type unusedBackend struct{}

func (unusedBackend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return nil, errors.New("unexpected query")
}

// Waiting on a logged transition keeps wiring tests independent of scheduler
// timing while exercising the production Manager and runtime together.
type backendStateWriter struct {
	state   string
	reached chan struct{}
	once    sync.Once
	mu      sync.Mutex
	logs    bytes.Buffer
}

func (w *backendStateWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	_, _ = w.logs.Write(p)
	w.mu.Unlock()
	if strings.Contains(string(p), "msg=agently_backend") && strings.Contains(string(p), "state="+w.state) {
		w.once.Do(func() { close(w.reached) })
	}
	return len(p), nil
}

func (w *backendStateWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.logs.String()
}

func TestRunConnectsSlackDuringBackendFailureAndClosesUnstartedWork(t *testing.T) {
	for _, blocked := range []bool{true, false} {
		t.Run(map[bool]string{true: "blocked", false: "temporarily_unavailable"}[blocked], func(t *testing.T) {
			path := writeConfigWithTokens(t, `storage:
  driver: sqlite
  path: real.sqlite
transport: socket
access: {channels: {enabled: true, scope: selected, allowlist: [C], users: {scope: workspace_members_only}}}
slack:
  app_id: A
  workspace_id: T
`+agentlyTestConfig)

			// Provisioning is outside the bounded runtime exercise. Under race
			// instrumentation native SQLite startup can consume several seconds.
			setupStarted := time.Now()
			ctx := context.Background()
			storetest.Must(t, Execute(ctx, []string{"migrate", "--config", path}, &bytes.Buffer{}))
			dbPath := filepath.Join(filepath.Dir(path), "real.sqlite")
			s, err := sqlite.Open(ctx, dbPath, false)
			storetest.Must(t, err)
			configured, err := config.Load(path)
			storetest.Must(t, err)
			topology, err := configured.Topology()
			storetest.Must(t, err)
			assignments, err := s.ReconcileIntegrations(ctx, []storage.Integration{integrationIdentity(topology[0])})
			storetest.Must(t, err)
			scope := s.Scope(assignments[0].ID)
			_, err = scope.Enqueue(ctx, storetest.Message("event", "root", "alice"))
			storetest.Must(t, err)
			_, err = scope.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Close())
			setupElapsed := time.Since(setupStarted)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			connected := false
			attempted := make(chan struct{})
			var attemptedOnce sync.Once
			state := "starting"
			if blocked {
				state = "blocked"
			}
			reached := make(chan struct{})
			out := &backendStateWriter{state: state, reached: reached}
			deps := dependencies{
				login: func(context.Context, string, string, string, *scy.Resource) (gateway.AgentlyClientFactory, error) {
					attemptedOnce.Do(func() { close(attempted) })
					if blocked {
						return nil, gateway.ErrBackendAuth
					}
					return nil, &agently.APIError{Status: 503}
				},
				connect: func(_ context.Context, tokens config.SlackTokens, route gateway.Route, _ *slog.Logger) (runner.Source, gateway.SlackSender, error) {
					if route.AppID != "A" {
						t.Fatal("wrong Slack route")
					}
					connected = true
					return idleSource(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
						// Do not start outbound workers: inspect the durable notice.
						select {
						case <-reached:
						case <-ctx.Done():
							return nil
						}
						select {
						case <-attempted:
						case <-ctx.Done():
							return nil
						}
						_, acceptErr := sink.Accept(ctx, storetest.Message("during-outage", "new-root", "alice"))
						cancel()
						return acceptErr
					}), unusedSender{}, nil
				},
			}

			t.Logf("setup completed in %s; bounded runtime starts after provisioning", setupElapsed)
			runStarted := time.Now()
			err = execute(ctx, []string{"run", "--config", path}, out, deps)
			if err != nil {
				t.Fatalf("runtime failed after %s (setup %s): %v\nruntime logs:\n%s", time.Since(runStarted), setupElapsed, err, out.String())
			}
			if !connected {
				t.Fatal("Slack not connected during backend failure")
			}
			s, err = sqlite.Open(context.Background(), dbPath, false)
			storetest.Must(t, err)
			defer s.Close()
			status, err := s.Status(context.Background(), 10)
			storetest.Must(t, err)
			if len(status.GatewayJobs) != 2 || len(status.SlackOutgoingMessages) != 2 {
				t.Fatalf("unexpected recovery after %s (setup %s, context error %v): %+v\nruntime logs:\n%s", time.Since(runStarted), setupElapsed, ctx.Err(), status.GatewayJobs, out.String())
			}
			for _, job := range status.GatewayJobs {
				if job.State != "completed" || job.BackendSubmissionStartedAt != 0 {
					t.Fatalf("unstarted work could replay: %+v", job)
				}
			}
		})
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	var document map[string]any
	storetest.Must(t, yaml.Unmarshal([]byte(body), &document))

	// Most wiring tests author one integration fragment alongside storage. Build
	// the complete public document before passing it to the production loader.
	integration := map[string]any{"name": "steward"}
	for _, key := range []string{"transport", "slack", "access", "backend"} {
		if value, ok := document[key]; ok {
			integration[key] = value
			delete(document, key)
		}
	}
	if len(integration) > 1 {
		document["integrations"] = []any{integration}
	}
	if values, ok := document["integrations"].([]any); ok {
		for _, value := range values {
			item := value.(map[string]any)
			if slack, ok := item["slack"].(map[string]any); ok {
				slack["app_token_secret"] = map[string]any{"URL": "/unused/app-token.enc", "Key": "blowfish://default"}
				slack["bot_token_secret"] = map[string]any{"URL": "/unused/bot-token.enc", "Key": "blowfish://default"}
			}
		}
	}

	encoded, err := yaml.Marshal(document)
	storetest.Must(t, err)
	path := filepath.Join(t.TempDir(), "config.yaml")
	storetest.Must(t, os.WriteFile(path, encoded, 0600))
	return path
}

func writeConfigWithTokens(t *testing.T, body string) string {
	t.Helper()
	path := writeConfig(t, body)
	encoded, err := os.ReadFile(path)
	storetest.Must(t, err)
	var document map[string]any
	storetest.Must(t, yaml.Unmarshal(encoded, &document))
	slack := document["integrations"].([]any)[0].(map[string]any)["slack"].(map[string]any)
	for field, value := range map[string]string{"app_token_secret": "xapp-synthetic", "bot_token_secret": "xoxb-synthetic"} {
		resource := &scy.Resource{URL: "file://" + filepath.ToSlash(filepath.Join(t.TempDir(), "token.enc")), Key: "blowfish://default"}
		storetest.Must(t, scy.New().Store(context.Background(), scy.NewSecret(value, resource)))
		slack[field] = map[string]any{"URL": resource.URL, "Key": resource.Key}
	}
	encoded, err = yaml.Marshal(document)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(path, encoded, 0600))
	return path
}

func TestOfflineStatusAndDeliveryResolution(t *testing.T) {
	ctx := context.Background()
	path := writeConfig(t, "storage:\n  driver: sqlite\n  path: gateway.sqlite\n")
	var out bytes.Buffer
	storetest.Must(t, Execute(ctx, []string{"migrate", "--config", path}, &out))
	s, err := sqlite.Open(ctx, filepath.Join(filepath.Dir(path), "gateway.sqlite"), false)
	storetest.Must(t, err)
	integrationID := storetest.RegisterDefault(t, s)
	m := storetest.Message("a", "one", "alice")
	m.Text = "secret-query-marker"
	_, err = s.Scope(integrationID).Enqueue(ctx, m)
	storetest.Must(t, err)
	j, err := s.Scope(integrationID).ClaimJob(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Scope(integrationID).Complete(ctx, j.ID, "", "secret-result-marker", []gateway.ReplyPart{{Text: "secret-response-marker", Format: gateway.ReplyPlain}}))
	d, err := s.Scope(integrationID).ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Scope(integrationID).DeliveryUnknown(ctx, d.ID))
	storetest.Must(t, s.Close())
	out.Reset()
	storetest.Must(t, Execute(ctx, []string{"status", "--config", path}, &out))
	if strings.Contains(out.String(), "secret-") || !strings.Contains(out.String(), "delivery_unknown") {
		t.Fatal("unsafe or incomplete diagnostics")
	}
	// Offline scoping uses the recorded identity, independently of whether the
	// integration still appears in an active runtime configuration.
	out.Reset()
	storetest.Must(t, Execute(ctx, []string{"status", "--config", path, "--integration", "default"}, &out))
	if strings.Contains(out.String(), "secret-") || !strings.Contains(out.String(), "delivery_unknown") {
		t.Fatal("unsafe or incomplete scoped diagnostics")
	}
	if err := Execute(ctx, []string{"resolve-delivery", "--config", path, "--delivery-id", "1", "--action", "retry"}, &out); err == nil {
		t.Fatal("retry without confirmation accepted")
	}
	storetest.Must(t, Execute(ctx, []string{"resolve-delivery", "--config", path, "--delivery-id", "1", "--action", "retry", "--confirm-duplicate-risk"}, &out))
	if !strings.Contains(out.String(), "No Slack message or Agently query was sent") {
		t.Fatal("resolution side effects unclear")
	}
	s, err = sqlite.Open(ctx, filepath.Join(filepath.Dir(path), "gateway.sqlite"), false)
	storetest.Must(t, err)
	defer s.Close()
	d, err = s.Scope(integrationID).ClaimDelivery(ctx)
	storetest.Must(t, err)
	if d == nil || d.Text != "secret-response-marker" {
		t.Fatal("resolution failed to preserve persisted reply")
	}
}

func TestMigrateNeedsOnlyStorageAndExplicitDriver(t *testing.T) {
	ctx := context.Background()
	path := writeConfig(t, "storage:\n  path: gateway.sqlite\n")
	var out bytes.Buffer
	if err := Execute(ctx, []string{"migrate", "--config", path}, &out); err == nil || !strings.Contains(err.Error(), "driver is required") {
		t.Fatalf("expected explicit driver error: %v", err)
	}
	for range 2 {
		if err := Execute(ctx, []string{"migrate", "--config", path, "--storage-driver", "sqlite"}, &out); err != nil {
			t.Fatal(err)
		}
	}
	s, err := sqlite.Open(ctx, filepath.Join(filepath.Dir(path), "gateway.sqlite"), false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.CheckSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "schema is current") {
		t.Fatal("missing migration result")
	}
}

func TestMySQLAndUnknownDriverNeverFallback(t *testing.T) {
	for _, driver := range []string{"mysql", "unknown"} {
		path := writeConfig(t, "storage:\n  path: fallback.sqlite\n")
		if err := Execute(context.Background(), []string{"migrate", "--config", path, "--storage-driver", driver}, &bytes.Buffer{}); err == nil {
			t.Fatal("unsupported backend accepted")
		}
		if _, err := os.Stat(filepath.Join(filepath.Dir(path), "fallback.sqlite")); !os.IsNotExist(err) {
			t.Fatal("fallback database created")
		}
	}
}

func TestMySQLValidationAndMigrationDoNotOpenStorage(t *testing.T) {
	t.Setenv("AGENTLY_SLACK_MYSQL_DSN", "ignored-env-secret-marker")
	path := writeConfig(t, "storage: {driver: mysql, dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true', secret: {URL: /unused/mysql-secret-marker.json, Key: 'blowfish://default'}}\n"+runStorageTestConfig)
	deps := dependencies{openMySQL: func(context.Context, string, *scy.Resource) (storage.Store, error) {
		t.Fatal("offline operation opened MySQL")
		return nil, nil
	}}
	storetest.Must(t, execute(context.Background(), []string{"validate-config", "--config", path}, &bytes.Buffer{}, deps))
	err := execute(context.Background(), []string{"migrate", "--config", path}, &bytes.Buffer{}, deps)
	if err == nil || !strings.Contains(err.Error(), "Endly") {
		t.Fatalf("MySQL migration did not direct explicit provisioning: %v", err)
	}
	files, err := os.ReadDir(filepath.Dir(path))
	storetest.Must(t, err)
	if len(files) != 1 {
		t.Fatal("offline MySQL operation created local storage")
	}
}

type operatorStore struct {
	gateway.Store
	snapshot *storage.Status
	closed   bool
	checked  bool
	resolved bool
}

func (*operatorStore) IntegrationID() int64         { return 1 }
func (*operatorStore) WorkChanges() <-chan struct{} { return nil }
func (*operatorStore) WorkRevision(int64) uint64    { return 0 }
func (*operatorStore) WorkCandidates(context.Context, time.Time) ([]gateway.WorkCandidate, error) {
	return nil, nil
}

func (s *operatorStore) ReconcileIntegrations(_ context.Context, wanted []storage.Integration) ([]storage.Integration, error) {
	rows := append([]storage.Integration(nil), wanted...)
	for i := range rows {
		rows[i].ID = int64(i + 1)
		rows[i].Active = true
	}
	return rows, nil
}
func (s *operatorStore) Scope(int64) storage.Store                 { return s }
func (s *operatorStore) ScopeName(string) storage.Store            { return s }
func (s *operatorStore) FailCleanup(context.Context, string) error { return nil }
func (s *operatorStore) Close() error                              { s.closed = true; return nil }
func (s *operatorStore) CheckRunBindings(context.Context, gateway.Route, string) error {
	s.checked = true
	return nil
}
func (s *operatorStore) Status(context.Context, int) (*storage.Status, error) {
	if s.snapshot != nil {
		return s.snapshot, nil
	}
	return &storage.Status{Counts: map[string]map[string]int{"gateway_job.gateway_execution_state": {"completed": 1}}}, nil
}
func (s *operatorStore) ResolveDelivery(context.Context, int64, string, string, bool) error {
	s.resolved = true
	return nil
}

func TestMySQLCommandsUseSelectedStorageAndCloseIt(t *testing.T) {
	dsn := "${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true"
	t.Setenv("AGENTLY_SLACK_MYSQL_DSN", "ignored-old-env-secret-marker")
	t.Setenv("SLACK_APP_TOKEN", "")
	t.Setenv("SLACK_BOT_TOKEN", "")
	for _, command := range []string{"status", "resolve-delivery", "run"} {
		t.Run(command, func(t *testing.T) {
			path := writeConfig(t, "storage: {driver: mysql, dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true', secret: {URL: /unused/mysql-secret-marker.json, Key: 'blowfish://default'}}\n"+runStorageTestConfig)
			s := &operatorStore{}
			called := 0
			deps := dependencies{openMySQL: func(_ context.Context, actual string, resource *scy.Resource) (storage.Store, error) {
				called++
				if actual != dsn || resource == nil || resource.URL != "/unused/mysql-secret-marker.json" || resource.Key != "blowfish://default" {
					t.Fatal("wrong DSN reference")
				}
				return s, nil
			}}
			var out bytes.Buffer
			err := execute(context.Background(), []string{command, "--config", path}, &out, deps)
			if command == "run" {
				if err == nil || !strings.Contains(err.Error(), "Slack") || !s.checked {
					t.Fatalf("run bypassed identity/credentials boundary: %v", err)
				}
			} else {
				storetest.Must(t, err)
			}
			if called != 1 || !s.closed || s.resolved != (command == "resolve-delivery") {
				t.Fatal("wrong storage lifecycle")
			}
			if strings.Contains(out.String(), "secret-marker") || strings.Contains(out.String(), "initialized") {
				t.Fatal("MySQL credentials exposed or initialization attempted")
			}
		})
	}
}

func TestMySQLPlainDSNNeedsNoSecret(t *testing.T) {
	dsn := "test:synthetic-password@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true"
	path := writeConfig(t, fmt.Sprintf("storage: {driver: mysql, dsn: %q}\n", dsn))
	s := &operatorStore{}
	called := false
	deps := dependencies{openMySQL: func(_ context.Context, actual string, resource *scy.Resource) (storage.Store, error) {
		called = true
		if actual != dsn || resource != nil {
			t.Fatal("plain DSN unexpectedly required a secret")
		}
		return s, nil
	}}
	var out bytes.Buffer
	storetest.Must(t, execute(context.Background(), []string{"status", "--config", path}, &out, deps))
	if !called || !s.closed || strings.Contains(out.String(), "synthetic-password") {
		t.Fatal("unsafe direct DSN storage lifecycle")
	}
}

func TestMySQLStorageFailureIsRedactedAndNeverFallsBack(t *testing.T) {
	t.Setenv("AGENTLY_SLACK_MYSQL_DSN", "ignored-env-secret-marker")
	path := writeConfig(t, "storage: {driver: mysql, dsn: 'test:secret-marker@tcp(127.0.0.1:3309)/slack_ai_gateway'}\n")
	deps := dependencies{openMySQL: func(context.Context, string, *scy.Resource) (storage.Store, error) {
		return nil, errors.New("driver error with secret-marker")
	}}
	err := execute(context.Background(), []string{"status", "--config", path}, &bytes.Buffer{}, deps)
	if err == nil || strings.Contains(err.Error(), "secret-marker") || !strings.Contains(err.Error(), "MySQL") {
		t.Fatalf("unsafe or missing MySQL storage error: %v", err)
	}
	files, readErr := os.ReadDir(filepath.Dir(path))
	storetest.Must(t, readErr)
	if len(files) != 1 {
		t.Fatal("failed MySQL open created fallback storage")
	}
}

const runStorageTestConfig = `transport: socket
access:
  channels:
    enabled: true
    scope: selected
    allowlist: [C]
    users: {scope: workspace_members_only}
slack:
  app_id: A
  workspace_id: T
` + agentlyTestConfig

func TestRunInitializesMissingDatabaseAndRequiresRealCredentials(t *testing.T) {
	t.Setenv("SLACK_APP_TOKEN", "")
	t.Setenv("SLACK_BOT_TOKEN", "")
	ctx := context.Background()
	path := writeConfig(t, "storage: {driver: sqlite, path: gateway.sqlite}\n"+runStorageTestConfig)
	args := []string{"run", "--config", path}
	dbPath := filepath.Join(filepath.Dir(path), "gateway.sqlite")
	for attempt := range 2 {
		var out bytes.Buffer
		if err := Execute(ctx, args, &out); err == nil || !strings.Contains(err.Error(), "Slack") {
			t.Fatalf("expected real-backend boundary after initializing storage: %v", err)
		}
		if initialized := strings.Contains(out.String(), "Gateway SQLite database initialized at "+dbPath); initialized != (attempt == 0) {
			t.Fatalf("unexpected initialization notice on attempt %d: %s", attempt, out.String())
		}
		s, err := sqlite.Open(ctx, dbPath, false)
		storetest.Must(t, err)
		storetest.Must(t, s.CheckSchema(ctx))
		storetest.Must(t, s.Close())
	}
	info, err := os.Stat(dbPath)
	storetest.Must(t, err)
	if info.Mode().Perm()&0077 != 0 {
		t.Fatal("new database is readable by other users")
	}
}

func TestRunDoesNotMigrateExistingDatabase(t *testing.T) {
	ctx := context.Background()
	for _, kind := range []string{"empty", "incompatible", "foreign"} {
		t.Run(kind, func(t *testing.T) {
			path := writeConfig(t, "storage: {driver: sqlite, path: gateway.sqlite}\n"+runStorageTestConfig)
			dbPath := filepath.Join(filepath.Dir(path), "gateway.sqlite")
			s, err := sqlite.Open(ctx, dbPath, true)
			storetest.Must(t, err)
			storetest.Must(t, s.Close())
			db, err := sql.Open("sqlite", dbPath)
			storetest.Must(t, err)
			defer db.Close()
			wantError := "schema is not current"
			switch kind {
			case "incompatible":
				body, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
				storetest.Must(t, err)
				_, err = db.ExecContext(ctx, string(body))
				storetest.Must(t, err)
				_, err = db.ExecContext(ctx, "INSERT INTO schema_migrations(version,checksum,applied_at) VALUES(1,?,1)", "pre-release-schema")
				storetest.Must(t, err)
				wantError = "incompatible migration history"
			case "foreign":
				_, err = db.ExecContext(ctx, "CREATE TABLE foreign_data(id INTEGER); INSERT INTO foreign_data VALUES(42)")
				storetest.Must(t, err)
				wantError = "not an empty gateway database"
			}
			var before, after int
			storetest.Must(t, db.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&before))
			var out bytes.Buffer
			if err = Execute(ctx, []string{"run", "--config", path}, &out); err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("existing %s database not rejected: %v", kind, err)
			}
			if strings.Contains(out.String(), "initialized") {
				t.Fatal("existing database reported as initialized")
			}
			storetest.Must(t, db.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&after))
			if after != before {
				t.Fatal("run changed an existing schema")
			}
			if kind == "foreign" {
				var id int
				storetest.Must(t, db.QueryRowContext(ctx, "SELECT id FROM foreign_data").Scan(&id))
				if id != 42 {
					t.Fatal("foreign data changed")
				}
			} else if kind == "incompatible" {
				if err = Execute(ctx, []string{"migrate", "--config", path}, &bytes.Buffer{}); err == nil {
					t.Fatal("explicit initialization accepted an incompatible schema")
				}
				storetest.Must(t, db.QueryRowContext(ctx, "PRAGMA schema_version").Scan(&after))
				if after != before {
					t.Fatal("initialization changed an incompatible schema")
				}
			} else {
				storetest.Must(t, db.Close())
				storetest.Must(t, Execute(ctx, []string{"migrate", "--config", path}, &bytes.Buffer{}))
				s, err = sqlite.Open(ctx, dbPath, false)
				storetest.Must(t, err)
				storetest.Must(t, s.CheckSchema(ctx))
				storetest.Must(t, s.Close())
			}
		})
	}
}

func TestRunRequiresExistingParentDirectory(t *testing.T) {
	path := writeConfig(t, "storage: {driver: sqlite, path: missing/gateway.sqlite}\n"+runStorageTestConfig)
	err := Execute(context.Background(), []string{"run", "--config", path}, &bytes.Buffer{})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing-directory error: %v", err)
	}
	if _, err = os.Stat(filepath.Join(filepath.Dir(path), "missing")); !os.IsNotExist(err) {
		t.Fatalf("run created parent directory: %v", err)
	}
}

func TestStorageInspectionDoesNotCreateDatabase(t *testing.T) {
	for _, command := range []string{"status", "resolve-delivery"} {
		t.Run(command, func(t *testing.T) {
			path := writeConfig(t, "storage: {driver: sqlite, path: missing.sqlite}\n")
			err := Execute(context.Background(), []string{command, "--config", path}, &bytes.Buffer{})
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("expected missing-database error: %v", err)
			}
			files, err := os.ReadDir(filepath.Dir(path))
			storetest.Must(t, err)
			if len(files) != 1 {
				t.Fatal("inspection created database or lock")
			}
		})
	}
}

func TestArgumentValidation(t *testing.T) {
	for _, args := range [][]string{nil, {"unknown"}, {"migrate"}, {"migrate", "--token", "secret-marker"}, {"migrate", "--config", "missing", "extra"}} {
		err := Execute(context.Background(), args, &bytes.Buffer{})
		if err == nil {
			t.Fatal("invalid command accepted")
		}
		if strings.Contains(err.Error(), "secret-marker") {
			t.Fatal("argument exposed")
		}
	}
}

func TestExplicitFlagOverridesConfig(t *testing.T) {
	path := writeConfig(t, "storage:\n  driver: mysql\n  path: gateway.sqlite\n")
	if err := Execute(context.Background(), []string{"migrate", "--config", path, "--storage-driver", "sqlite"}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if err := Execute(context.Background(), []string{"migrate", "--config", path, "--storage-driver="}, &bytes.Buffer{}); err == nil {
		t.Fatal("empty explicit driver silently ignored")
	}
}

func (sender *routedSender) SendStatus(_ context.Context, s gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	sender.statuses.Add(1)
	if s.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: s.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}
func (unusedSender) SendStatus(_ context.Context, s gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	if s.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: s.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}

func TestBackendCredentialFailureStillStartsSlackTransport(t *testing.T) {
	path := writeConfigWithTokens(t, `storage: {driver: sqlite, path: credential-failure.sqlite}
transport: socket
slack: {app_id: A, workspace_id: T}
`+agentlyTestConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blocked := make(chan struct{})
	out := &backendStateWriter{state: "blocked", reached: blocked}
	connected := false
	deps := dependencies{
		validateSecret: func(context.Context, *scy.Resource) error {
			return errors.New("synthetic sensitive credential failure")
		},
		login: func(context.Context, string, string, string, *scy.Resource) (gateway.AgentlyClientFactory, error) {
			t.Fatal("invalid credentials reached backend login")
			return nil, nil
		},
		connect: func(context.Context, config.SlackTokens, gateway.Route, *slog.Logger) (runner.Source, gateway.SlackSender, error) {
			connected = true
			return idleSource(func(ctx context.Context, _ gateway.EventSink, ready func()) error {
				ready()
				select {
				case <-blocked:
					cancel()
				case <-ctx.Done():
				}
				return nil
			}), unusedSender{}, nil
		},
	}
	storetest.Must(t, execute(ctx, []string{"run", "--config", path}, out, deps))
	if !connected {
		t.Fatal("backend credential failure suppressed the Slack transport needed for cleanup")
	}
}

// Exercise the actual CLI encoder: the merged diagnostic contract keeps the
// execution and progress dimensions independent and does not expose old arrays.
func TestStatusJSONMergedRequestMetadata(t *testing.T) {
	path := writeConfig(t, "storage: {driver: mysql, dsn: 'test:unused@tcp(127.0.0.1:3309)/fixture'}\n")
	zero := int64(0)
	s := &operatorStore{snapshot: &storage.Status{
		Limit:        2,
		Integrations: []storage.IntegrationMetadata{},
		GatewayJobs: []storage.GatewayJobMetadata{
			{ID: 2, State: "completed", Reason: "", Outcome: "completed", ProgressState: "sending", ProgressReason: "retrying", ObservedAt: 1234, AckLatencyMS: &zero},
			{ID: 1, State: "pending", ProgressState: "ready"},
		},
		ConversationBindings: []storage.ConversationBindingMetadata{}, SlackOutgoingMessages: []storage.SlackOutgoingMessageMetadata{}, SlackAgentSessionPublications: []storage.SlackAgentSessionPublicationMetadata{},
		Counts: map[string]map[string]int{
			"gateway_job.gateway_execution_state": {"pending": 1, "completed": 1},
			"gateway_job.slack_progress_state":    {"ready": 1, "sending": 1},
		},
	}}
	deps := dependencies{openMySQL: func(context.Context, string, *scy.Resource) (storage.Store, error) { return s, nil }}
	var out bytes.Buffer
	storetest.Must(t, execute(context.Background(), []string{"status", "--config", path, "--limit", "2"}, &out, deps))
	var result map[string]json.RawMessage
	storetest.Must(t, json.Unmarshal(out.Bytes(), &result))
	for _, name := range []string{"integrations", "gateway_jobs", "conversation_bindings", "slack_outgoing_messages", "slack_agent_session_publications"} {
		if value, ok := result[name]; !ok || string(value) == "null" {
			t.Fatalf("missing or null collection %s: %s", name, out.String())
		}
	}
	for _, name := range []string{"jobs", "requests", "request_statuses", "bindings", "deliveries", "agent_sessions"} {
		if _, ok := result[name]; ok {
			t.Errorf("legacy collection %s is still exposed", name)
		}
	}
	var requests []map[string]any
	storetest.Must(t, json.Unmarshal(result["gateway_jobs"], &requests))
	if len(requests) != 2 || requests[0]["gateway_execution_state"] != "completed" || requests[0]["slack_progress_state"] != "sending" || requests[0]["slack_progress_reason"] != "retrying" || requests[0]["gateway_execution_outcome"] != "completed" {
		t.Fatalf("execution/progress metadata was conflated: %+v", requests)
	}
	if requests[0]["backend_observed_at_ms"] != float64(1234) || requests[0]["slack_progress_ack_latency_ms"] != float64(0) || requests[1]["slack_progress_ack_latency_ms"] != nil || requests[1]["backend_observed_at_ms"] != float64(0) {
		t.Fatalf("diagnostic time/latency contract changed: %+v", requests)
	}
	for _, forbidden := range []string{"prompt_text", "gateway_execution_result_text", "slack_message_text", "backend_protocol_messages", "backend_protocol_state", "backend_outcome"} {
		if _, ok := requests[0][forbidden]; ok {
			t.Errorf("unexpected diagnostic field %s", forbidden)
		}
	}
}

func TestCheckAgentlyValidatesSelectedBackendAndNamesBeforeLogin(t *testing.T) {
	for _, tc := range []struct{ name, body, selected string }{
		{"missing_name", "integrations: [{backend: {type: agently}}]", ""},
		{"duplicate_name", "integrations: [{name: steward, backend: {type: agently}}, {name: steward, backend: {type: agently}}]", ""},
		{"unknown_selection", agentlyTestConfig, "other"},
		{"unexpected_endpoint", agentlyTestConfig + "  endpoint: https://unexpected.example.test/run\n", ""},
		{"empty_integrations", "integrations: []", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.body)
			args := []string{"check-agently", "--config", path}
			if tc.selected != "" {
				args = append(args, "--integration", tc.selected)
			}
			deps := dependencies{login: func(context.Context, string, string, string, *scy.Resource) (gateway.AgentlyClientFactory, error) {
				t.Fatal("invalid configuration reached backend login")
				return nil, nil
			}}
			if err := execute(context.Background(), args, &bytes.Buffer{}, deps); err == nil {
				t.Fatal("invalid backend-only configuration accepted")
			}
		})
	}
}
