//go:build slack_live

package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	gatewayruntime "github.com/viant/slack-ai-gateway/internal/runtime"
	slackadapter "github.com/viant/slack-ai-gateway/internal/slack"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
)

// TestSlackThreadClosure uses a private disposable database and a controlled
// backend. Only the three exact marker commands enter the real runtime. Nothing
// connects to Slack until the explicit live opt-in and configuration checks pass.
func TestSlackThreadClosure(t *testing.T) {
	if os.Getenv("SLACK_AI_GATEWAY_LIVE") != "1" {
		t.Skip("set SLACK_AI_GATEWAY_LIVE=1 to explicitly enable real Slack writes")
	}
	marker := os.Getenv("SLACK_AI_GATEWAY_CLOSURE_MARKER")
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{6,48}$`).MatchString(marker) {
		t.Fatal("SLACK_AI_GATEWAY_CLOSURE_MARKER must contain 6-48 ASCII letters, digits, underscores or hyphens")
	}
	path, integration := os.Getenv("SLACK_AI_GATEWAY_CONFIG"), os.Getenv("SLACK_AI_GATEWAY_INTEGRATION")
	if path == "" || integration == "" {
		t.Fatal("SLACK_AI_GATEWAY_CONFIG and SLACK_AI_GATEWAY_INTEGRATION are required")
	}
	c, err := loadSmokeConfig(path, integration)
	if err != nil {
		t.Fatal(err)
	}
	// Never open, migrate, reconcile or recover the configured application DB.
	c.Storage = config.Storage{Driver: "sqlite", Path: filepath.Join(t.TempDir(), "slack-smoke.sqlite")}
	policy, err := smokePolicy(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	tokens, err := c.LoadSlackTokens(ctx)
	if err != nil {
		t.Fatal("cannot load configured Slack test credentials")
	}
	store, err := sqlite.Open(ctx, c.Storage.Path, true)
	if err != nil {
		t.Fatal("cannot create private Slack closure test storage")
	}
	defer store.Close()
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	route := policy.Route
	assignments, err := store.ReconcileIntegrations(ctx, []storage.Integration{{
		Name: "closure-live", AppID: route.AppID, WorkspaceID: route.WorkspaceID,
		BackendType: "smoke", BackendURL: "https://smoke.example.test",
		BackendAgentID: "slack-smoke", PrincipalID: smokePrincipal,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	source, sender, err := slackadapter.Connect(ctx, tokens, route, log)
	if err != nil {
		t.Fatal("cannot connect the configured Slack test application")
	}
	policy.Directory = gateway.NewCachedDirectory(slackadapter.NewDirectory(tokens.Bot, route.WorkspaceID))
	backend := &closureLiveBackend{marker: marker}
	scoped := store.Scope(assignments[0].ID)
	var ready sync.Once
	runtime := gatewayruntime.Runtime{
		Store: scoped, Source: closureLiveSource{Source: source, marker: marker, onReady: func() {
			ready.Do(func() { t.Logf("LIVE_CLOSURE_READY marker=%s", marker) })
		}}, Route: route, Policy: policy, Clients: backend, Sender: sender,
		AgentID: "slack-smoke", Log: log, StatusMode: gateway.StatusHybrid,
		StatusRefreshInterval: 30 * time.Second,
	}
	group := gatewayruntime.Group{Store: store, Integrations: []gatewayruntime.Integration{{ID: "closure-live", Runtime: &runtime}}, Log: log}
	done := make(chan error, 1)
	go func() { done <- group.Run(ctx) }()
	stopped := false
	defer func() {
		cancel()
		if !stopped {
			select {
			case <-done:
			case <-time.After(25 * time.Second):
				t.Error("live closure runtime did not finish shutdown")
			}
		}
	}()
	t.Logf("Send three commands in order: main message GW024-CLOSE %s; reply in that thread GW024-FOLLOWUP %s; NEW main message GW024-NEWROOT %s. No Agently or model calls are made.", marker, marker, marker)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	seen := map[int64]bool{}
	for {
		select {
		case err := <-done:
			stopped = true
			if err != nil {
				t.Fatalf("live closure runtime failed: %v", err)
			}
			t.Fatal("live runtime stopped before all three verification steps completed")
		case <-ctx.Done():
			t.Fatalf("live closure verification timed out: queries=%d deliveries=%d", backend.queries.Load(), runtime.Delivered())
		case <-ticker.C:
			view, err := scoped.Status(ctx, 10)
			if err != nil {
				t.Fatal("cannot inspect private live test storage")
			}
			for _, job := range view.GatewayJobs {
				if !seen[job.ID] {
					seen[job.ID] = true
					t.Logf("LIVE_CLOSURE_JOB id=%d root=%s", job.ID, job.ThreadTS)
				}
			}
			if backend.queries.Load() > 2 || len(view.GatewayJobs) > 3 {
				t.Fatal("a closed-thread follow-up or duplicate reached backend execution")
			}
			if len(view.GatewayJobs) != 3 || len(view.SlackOutgoingMessages) != 3 || runtime.Delivered() != 3 {
				continue
			}
			settled := true
			for _, publication := range view.SlackAgentSessionPublications {
				settled = settled && (publication.State == "skipped" || publication.State == "settled" && publication.AgentStatus == "active")
			}
			if !settled {
				continue
			}
			assertClosureLiveResult(t, view, backend.queries.Load())
			cancel()
			select {
			case err := <-done:
				stopped = true
				if err != nil {
					t.Fatalf("live closure shutdown failed: %v", err)
				}
			case <-time.After(25 * time.Second):
				t.Fatal("live closure runtime did not finish shutdown")
			}
			t.Log("LIVE_CLOSURE_PASS closed thread stayed local, new root completed, native indicators settled, queries=2 deliveries=3")
			return
		}
	}
}

// This test filter ACKs unrelated events without admitting them to storage or
// the controlled backend. Normal routing and authorization still run afterwards.
type closureLiveSource struct {
	gatewayruntime.Source
	marker  string
	onReady func()
}

func (s closureLiveSource) Run(ctx context.Context, sink gateway.EventSink, ready func()) error {
	return s.Source.Run(ctx, closureLiveSink{next: sink, marker: s.marker}, func() {
		ready()
		s.onReady()
	})
}

type closureLiveSink struct {
	next   gateway.EventSink
	marker string
}

func (s closureLiveSink) Accept(ctx context.Context, message gateway.Message) (gateway.AcceptResult, error) {
	text := strings.TrimSpace(message.Text)
	if text != "GW024-CLOSE "+s.marker && text != "GW024-FOLLOWUP "+s.marker && text != "GW024-NEWROOT "+s.marker {
		return gateway.AcceptResult{Ignored: true}, nil
	}
	return s.next.Accept(ctx, message)
}

type closureLiveBackend struct {
	marker  string
	queries atomic.Int64
}

func (b *closureLiveBackend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if principal != smokePrincipal {
		return nil, fmt.Errorf("unexpected test execution identity")
	}
	return b, nil
}

func (*closureLiveBackend) CreateConversation(ctx context.Context, agent string) (string, error) {
	return (&smokeBackend{}).CreateConversation(ctx, agent)
}

func (b *closureLiveBackend) Query(ctx context.Context, query gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries.Add(1)
	switch strings.TrimSpace(query.Text) {
	case "GW024-CLOSE " + b.marker:
		// Allow the real native processing indicator to publish before closure.
		select {
		case <-ctx.Done():
			return gateway.Turn{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
		return gateway.Turn{ID: "deliberately-wrong-live-turn", State: "completed", Content: "This mismatched result must never be published."}, nil
	case "GW024-NEWROOT " + b.marker:
		return gateway.Turn{ID: query.MessageID, State: "completed", Content: "GW024-NEWROOT-OK " + b.marker}, nil
	default:
		return gateway.Turn{}, fmt.Errorf("closed-thread follow-up unexpectedly reached backend")
	}
}

func (*closureLiveBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, fmt.Errorf("live closure fixture must not attach or replay")
}

func assertClosureLiveResult(t *testing.T, view *storage.Status, queries int64) {
	t.Helper()
	if len(view.ConversationBindings) != 2 || len(view.SlackAgentSessionPublications) != 2 {
		t.Fatal("live hybrid verification did not retain exactly two independent conversation bindings and native publications")
	}
	var closed, followup, fresh *storage.GatewayJobMetadata
	for i := range view.GatewayJobs {
		job := &view.GatewayJobs[i]
		switch {
		case job.Reason == "conversation_closed:turn_identity_mismatch":
			closed = job
		case job.Reason == "conversation_closed":
			followup = job
		case job.State == "completed" && job.Outcome == "completed":
			fresh = job
		}
	}
	if queries != 2 || closed == nil || followup == nil || fresh == nil {
		t.Fatal("the live run did not produce one closure, one local follow-up and one new completed execution")
	}
	if closed.State != "rejected" || closed.BackendSubmissionStartedAt == 0 || followup.BackendSubmissionStartedAt != 0 || followup.ThreadTS != closed.ThreadTS || fresh.ThreadTS == closed.ThreadTS {
		t.Fatal("live execution/closed-thread routing facts are inconsistent")
	}
	if fresh.ConversationBindingID == 0 || fresh.ConversationBindingID == closed.ConversationBindingID {
		t.Fatal("new live root reused the closed backend conversation")
	}
	var foundClosed bool
	for _, binding := range view.ConversationBindings {
		if binding.ID == closed.ConversationBindingID {
			foundClosed = binding.State == "closed" && binding.ClosedAt != 0
		}
	}
	if !foundClosed {
		t.Fatal("old live backend binding was not durably closed")
	}
	for _, delivery := range view.SlackOutgoingMessages {
		if delivery.State != "delivered" || delivery.SlackTS == "" || delivery.Attempts != 1 {
			t.Fatal("live response was not delivered exactly once")
		}
	}
	t.Logf("LIVE_CLOSURE_RESULT closed_job=%d closed_root=%s local_followup=%d fresh_job=%d fresh_root=%s", closed.ID, closed.ThreadTS, followup.ID, fresh.ID, fresh.ThreadTS)
}
