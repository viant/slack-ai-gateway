//go:build slack_live

package integration_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/config"
	"github.com/viant/agently-slack-gateway/internal/gateway"
	gatewayruntime "github.com/viant/agently-slack-gateway/internal/runtime"
	slackadapter "github.com/viant/agently-slack-gateway/internal/slack"
	"github.com/viant/agently-slack-gateway/internal/storage/sqlite"
)

// TestSlackSmoke is intentionally inaccessible from normal tests or the binary.
// Even with the build tag it cannot contact Slack without an explicit opt-in.
func TestSlackSmoke(t *testing.T) {
	if os.Getenv("AGENTLY_SLACK_LIVE") != "1" {
		t.Skip("set AGENTLY_SLACK_LIVE=1 to explicitly enable real Slack writes")
	}
	path := os.Getenv("AGENTLY_SLACK_CONFIG")
	if path == "" {
		t.Fatal("AGENTLY_SLACK_CONFIG is required")
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := smokePolicy(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	duration := 10 * time.Minute
	if value := os.Getenv("AGENTLY_SLACK_TEST_DURATION"); value != "" {
		duration, err = time.ParseDuration(value)
		if err != nil || duration <= 0 || duration > time.Hour {
			t.Fatal("AGENTLY_SLACK_TEST_DURATION must be a positive duration <=1h")
		}
	}
	tokens, err := c.LoadSlackTokens(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, duration)
	defer cancel()
	s, err := sqlite.Open(ctx, c.Storage.Path, false)
	if err != nil {
		t.Fatal("cannot open smoke database; run migrate first and stop any other instance")
	}
	defer s.Close()
	if err = s.CheckSchema(ctx); err != nil {
		t.Fatal("smoke database schema is not current; run migrate first")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	route := policy.Route
	source, sender, err := slackadapter.Connect(ctx, tokens, route, log)
	if err != nil {
		t.Fatal(err)
	}
	policy.Directory = gateway.NewCachedDirectory(slackadapter.NewDirectory(tokens.Bot, route.WorkspaceID))
	backend := &smokeBackend{}
	r := gatewayruntime.Runtime{StatusMode: c.StatusMode(), StatusRefreshInterval: c.StatusRefreshInterval(), Store: s, Source: source, Route: route,
		Policy:  policy,
		Clients: backend, Sender: sender, AgentID: "slack-smoke", Log: log}
	t.Log("LIVE SLACK TEST: no Agently calls. Configured access policy and Slack metadata checks apply. Wait for slack_connection state=ready, then mention the bot on an allowed test channel or send a DM if explicitly enabled. Ctrl+C stops the session gracefully.")
	if err = r.Run(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("Smoke session: %d test queries, %d persisted deliveries. Verify thread mapping and visible replies manually.", backend.queries.Load(), r.Delivered())
	if backend.queries.Load() == 0 || r.Delivered() == 0 {
		t.Fatal("smoke test did not observe a query and a delivered reply; live integration is NOT verified")
	}
}

// Stateless test backend: identifiers are random and recognized after restart.
// This checks durable Gateway bindings, NOT real Agently memory/reconciliation.
type smokeBackend struct{ queries atomic.Int64 }

func (b *smokeBackend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if principal != smokePrincipal {
		return nil, fmt.Errorf("unexpected smoke principal")
	}
	return b, nil
}
func (b *smokeBackend) CreateConversation(context.Context, string) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return "slack-smoke-" + hex.EncodeToString(id[:]), nil
}
func (b *smokeBackend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	if !strings.HasPrefix(q.ConversationID, "slack-smoke-") {
		return gateway.Turn{}, fmt.Errorf("not a smoke conversation")
	}
	b.queries.Add(1)
	text := "[Slack integration test — no Agently call]\nConversation: " + q.ConversationID + "\nRequest: " + q.MessageID + "\nReceived: " + q.Text
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: text}, nil
}
func (*smokeBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{State: "unknown"}, nil
}
