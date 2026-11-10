//go:build agently_live

package agently

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// TestAGUIBackendLive creates one real backend conversation with two short
// prompts. It uses no Slack connection, Slack writes or gateway database.
func TestAGUIBackendLive(t *testing.T) {
	if os.Getenv("AGENTLY_LIVE") != "1" {
		t.Skip("set AGENTLY_LIVE=1 to enable real Agently conversation creation and execution")
	}
	path := os.Getenv("SLACK_AI_GATEWAY_CONFIG")
	if path == "" {
		t.Fatal("SLACK_AI_GATEWAY_CONFIG is required")
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = cfg.ValidateAgently(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	factory, err := LoginOOB(ctx, cfg.Agently.BaseURL, cfg.Agently.PrincipalID, cfg.Agently.AgentID, cfg.Agently.Auth.Secret)
	if err != nil {
		t.Fatal(err)
	}
	client, err := factory.ForPrincipal(ctx, cfg.Agently.PrincipalID)
	if err != nil {
		t.Fatal(err)
	}
	id, err := client.CreateConversation(ctx, cfg.Agently.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created real test conversation: %s", id)
	code := "GATEWAY_AGUI_" + rand.Text()
	for _, prompt := range []string{
		"Do not use tools. Remember this test code for our conversation: " + code + ". Reply with only the test code.",
		"Do not use tools. What test code did I ask you to remember? Reply with only the code from our previous exchange.",
	} {
		requestID := "gateway-probe-" + rand.Text()
		turn, queryErr := client.Query(ctx, gateway.Query{ConversationID: id, MessageID: requestID, AgentID: cfg.Agently.AgentID, Text: prompt}, nil)
		for {
			if queryErr != nil && (errors.Is(queryErr, gateway.ErrBackendAuth) || errors.Is(queryErr, gateway.ErrBackendProtocol)) {
				t.Fatal(queryErr)
			}
			if queryErr == nil {
				if turn.ID != requestID {
					t.Fatal("wrong logical request identity")
				}
				if turn.State == "completed" {
					if !strings.Contains(turn.Content, code) {
						t.Fatal("completed response did not preserve the test code; response text omitted")
					}
					break
				}
				if turn.State != "queued" && turn.State != "running" && turn.State != "unknown" {
					t.Fatalf("test turn ended with unsupported state: %s", turn.State)
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("test conversation observation deadline exceeded")
			case <-time.After(time.Second):
			}
			turn, queryErr = client.ReadTurn(ctx, id, requestID, nil)
		}
	}
	t.Log("verified AG-UI admission, exact canonical final answers and server-owned followup history")
}
