package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestCheckAgentlyPassesOptionalOOBLoginMode(t *testing.T) {
	for _, mode := range []agently.OOBLoginMode{"", agently.OOBLoginParallel, agently.OOBLoginSerialPerUser, "unknown"} {
		t.Run(string(mode), func(t *testing.T) {
			body := agentlyTestConfig
			if mode != "" {
				body = "oob_login_mode: " + string(mode) + "\n" + body
			}
			path := writeConfig(t, body)
			called := 0
			deps := dependencies{login: func(_ context.Context, _, _, _ string, _ *scy.Resource, actual agently.OOBLoginMode) (gateway.AgentlyClientFactory, error) {
				called++
				if actual != mode {
					t.Fatalf("OOB mode = %q, want %q", actual, mode)
				}
				return nil, nil
			}}
			var out bytes.Buffer
			err := execute(context.Background(), []string{"check-agently", "--config", path}, &out, deps)
			if mode == "unknown" {
				if err == nil || !strings.Contains(err.Error(), "oob_login_mode") || called != 0 {
					t.Fatal("invalid policy reached backend authentication")
				}
				return
			}
			if err != nil || called != 1 {
				t.Fatalf("valid policy did not reach login once: %v, calls=%d", err, called)
			}
		})
	}
}
