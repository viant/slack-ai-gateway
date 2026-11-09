package cli

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/config"
	"github.com/viant/agently-slack-gateway/internal/gateway"
	runner "github.com/viant/agently-slack-gateway/internal/runtime"
	"github.com/viant/agently-slack-gateway/internal/storage/sqlite"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/agently-slack-gateway/migrations"
)

const agentlyTestConfig = `agently:
  base_url: http://127.0.0.1:9191
  agent_id: steward
  principal_id: agently_steward
  auth:
    mode: oob_session
    secrets_url_env: TEST_GATEWAY_OOB_REF
`

func TestValidateConfigIsOfflineAndReportsEffectivePolicy(t *testing.T) {
	t.Setenv("SLACK_APP_TOKEN", "")
	t.Setenv("SLACK_BOT_TOKEN", "")
	t.Setenv("TEST_GATEWAY_OOB_REF", "")
	path := writeConfig(t, `storage: {driver: sqlite, path: nonexistent.sqlite}
transport: socket
access_mode: shared_channel
slack: {app_id: A, workspace_id: T, channels: {mode: workspace}}
access:
  global: {users: {denylist: [blocked]}}
  defaults: {users: {mode: channel_members}}
  channel_overrides:
    C: {users: {allowlist: []}}
`+agentlyTestConfig)
	for _, id := range []string{"C", "OTHER"} {
		var out bytes.Buffer
		// Nil dependencies panic if validation unexpectedly logs in or connects.
		storetest.Must(t, execute(context.Background(), []string{"validate-config", "--config", path, "--channel-id", id}, &out, dependencies{}))
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
func (*routedDirectory) User(context.Context, string, string) (gateway.UserInfo, error) {
	return gateway.UserInfo{}, errors.New("unexpected users.info")
}

type routedSender struct {
	calls        atomic.Int64
	statuses     atomic.Int64
	cancel       context.CancelFunc
	wantResponse string
}

func (s *routedSender) Send(_ context.Context, d gateway.Delivery) (string, error) {
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
	t.Setenv("TEST_GATEWAY_OOB_REF", "/unused/synthetic-secret-reference")
	t.Setenv("SLACK_APP_TOKEN", "xapp-synthetic")
	t.Setenv("SLACK_BOT_TOKEN", "xoxb-synthetic")
	path := writeConfig(t, `storage: {driver: sqlite, path: real.sqlite}
transport: socket
access_mode: shared_channel
slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C, C2]}, response: {mode: summary}}
access:
  global: {users: {denylist: [mallory]}}
  defaults: {users: {mode: channel_members}}
`+agentlyTestConfig)
	storetest.Must(t, Execute(context.Background(), []string{"migrate", "--config", path}, &bytes.Buffer{}))
	// Both replies and their statuses share one-second pacing. This is a wiring
	// check, so leave room for race-instrumented SQLite rather than timing it.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	backend := &routedBackend{}
	directory := &routedDirectory{}
	sender := &routedSender{cancel: cancel, wantResponse: "[View full response in Agently](http://127.0.0.1:9191/conversation/offline-conversation)\n\noffline reply"}
	deps := dependencies{
		login: func(context.Context, string, string, string, string) (gateway.AgentlyClientFactory, error) {
			return backend, nil
		},
		directory: func(config.SlackTokens, gateway.Route) gateway.SlackDirectory { return directory },
		connect: func(_ context.Context, _ config.SlackTokens, route gateway.Route, _ *slog.Logger) (runner.Source, gateway.SlackSender, error) {
			if route.Resolver == nil {
				t.Fatal("runtime used legacy single-channel route")
			}
			return idleSource(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
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
				ready()
				<-ctx.Done()
				return nil
			}), sender, nil
		},
	}
	storetest.Must(t, execute(ctx, []string{"run", "--config", path}, &bytes.Buffer{}, deps))
	if backend.queries.Load() != 2 || sender.calls.Load() != 2 || directory.fresh.Load() != 2+sender.statuses.Load() {
		t.Fatalf("queries=%d sends=%d statuses=%d fresh=%d", backend.queries.Load(), sender.calls.Load(), sender.statuses.Load(), directory.fresh.Load())
	}
	s, err := sqlite.Open(context.Background(), filepath.Join(filepath.Dir(path), "real.sqlite"), false)
	storetest.Must(t, err)
	defer s.Close()
	status, err := s.Status(context.Background(), 10)
	storetest.Must(t, err)
	if status.Counts["inbox_jobs"]["rejected"] != 1 || status.Counts["outbox_messages"]["delivered"] != 2 {
		t.Fatal("policy not applied to durable runtime")
	}
}

func TestCheckAgentlyNeedsNoStorageOrSlack(t *testing.T) {
	t.Setenv("TEST_GATEWAY_OOB_REF", "/unused/synthetic-secret-reference")
	path := writeConfig(t, agentlyTestConfig)
	var out bytes.Buffer
	called := 0
	deps := dependencies{login: func(_ context.Context, base, principal, agent, ref string) (gateway.AgentlyClientFactory, error) {
		called++
		if base != "http://127.0.0.1:9191" || principal != "agently_steward" || agent != "steward" || ref != "/unused/synthetic-secret-reference" {
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

func (unusedSender) Send(context.Context, gateway.Delivery) (string, error) {
	return "", errors.New("unexpected send")
}

type unusedBackend struct{}

func (unusedBackend) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return nil, errors.New("unexpected query")
}

func TestRunPreflightBeforeSlackAndQueueRecovery(t *testing.T) {
	for _, fail := range []bool{true, false} {
		t.Run(map[bool]string{true: "rejected", false: "verified"}[fail], func(t *testing.T) {
			t.Setenv("TEST_GATEWAY_OOB_REF", "/unused/synthetic-secret-reference")
			t.Setenv("SLACK_APP_TOKEN", "xapp-synthetic")
			t.Setenv("SLACK_BOT_TOKEN", "xoxb-synthetic")
			path := writeConfig(t, `storage:
  driver: sqlite
  path: real.sqlite
transport: socket
access_mode: shared_channel
slack:
  app_id: A
  workspace_id: T
  channel_id: C
`+agentlyTestConfig)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			storetest.Must(t, Execute(ctx, []string{"migrate", "--config", path}, &bytes.Buffer{}))
			dbPath := filepath.Join(filepath.Dir(path), "real.sqlite")
			s, err := sqlite.Open(ctx, dbPath, false)
			storetest.Must(t, err)
			_, err = s.Enqueue(ctx, storetest.Message("event", "root", "alice"))
			storetest.Must(t, err)
			_, err = s.ClaimJob(ctx) // failed preflight must not recover this claim
			storetest.Must(t, err)
			storetest.Must(t, s.Close())
			loggedIn, connected := false, false
			deps := dependencies{
				login: func(context.Context, string, string, string, string) (gateway.AgentlyClientFactory, error) {
					if fail {
						return nil, gateway.ErrBackendAuth
					}
					loggedIn = true
					return unusedBackend{}, nil
				},
				connect: func(_ context.Context, tokens config.SlackTokens, route gateway.Route, _ *slog.Logger) (runner.Source, gateway.SlackSender, error) {
					if !loggedIn || route.AppID != "A" {
						t.Fatal("Slack before verified Agently identity")
					}
					connected = true
					return idleSource(func(ctx context.Context, _ gateway.EventSink, _ func()) error { cancel(); <-ctx.Done(); return nil }), unusedSender{}, nil
				},
			}
			err = execute(ctx, []string{"run", "--config", path}, &bytes.Buffer{}, deps)
			if fail && (!errors.Is(err, gateway.ErrBackendAuth) || connected) {
				t.Fatalf("preflight was bypassed: %v", err)
			}
			if !fail {
				storetest.Must(t, err)
				if !connected {
					t.Fatal("runtime not connected")
				}
			}
			s, err = sqlite.Open(context.Background(), dbPath, false)
			storetest.Must(t, err)
			defer s.Close()
			status, err := s.Status(context.Background(), 10)
			storetest.Must(t, err)
			want := "pending"
			if fail {
				want = "processing"
			}
			if status.Jobs[0].State != want || len(status.Deliveries) != 0 {
				t.Fatalf("unexpected recovery: %+v", status.Jobs)
			}
		})
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestOfflineStatusAndDeliveryResolution(t *testing.T) {
	ctx := context.Background()
	path := writeConfig(t, "storage:\n  driver: sqlite\n  path: gateway.sqlite\n")
	var out bytes.Buffer
	storetest.Must(t, Execute(ctx, []string{"migrate", "--config", path}, &out))
	s, err := sqlite.Open(ctx, filepath.Join(filepath.Dir(path), "gateway.sqlite"), false)
	storetest.Must(t, err)
	m := storetest.Message("a", "one", "alice")
	m.Text = "secret-query-marker"
	_, err = s.Enqueue(ctx, m)
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.Complete(ctx, j.ID, "", "secret-result-marker", []gateway.ReplyPart{{Text: "secret-response-marker", Format: gateway.ReplyPlain}}))
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	storetest.Must(t, s.DeliveryUnknown(ctx, d.ID))
	storetest.Must(t, s.Close())
	out.Reset()
	storetest.Must(t, Execute(ctx, []string{"status", "--config", path}, &out))
	if strings.Contains(out.String(), "secret-") || !strings.Contains(out.String(), "delivery_unknown") {
		t.Fatal("unsafe or incomplete diagnostics")
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
	d, err = s.ClaimDelivery(ctx)
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

const runStorageTestConfig = `transport: socket
access_mode: shared_channel
slack:
  app_id: A
  workspace_id: T
  channel_id: C
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
		if err := Execute(ctx, args, &out); err == nil || !strings.Contains(err.Error(), "Slack token environment variables") {
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

func (sender *routedSender) SendStatus(_ context.Context, s gateway.RequestStatus, _ string) (gateway.StatusReceipt, error) {
	sender.statuses.Add(1)
	if s.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: s.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}
func (unusedSender) SendStatus(_ context.Context, s gateway.RequestStatus, _ string) (gateway.StatusReceipt, error) {
	if s.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: s.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "3.000001"}, nil
}
