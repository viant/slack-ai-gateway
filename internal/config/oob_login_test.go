package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/agently"
	"gopkg.in/yaml.v3"
)

func TestOOBLoginModeResolvesForTheWholeProcess(t *testing.T) {
	for _, mode := range []agently.OOBLoginMode{"", agently.OOBLoginParallel, agently.OOBLoginSerialPerUser} {
		t.Run(string(mode), func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join("..", "..", "examples", "gateway.multi.example.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]any
			if err = yaml.Unmarshal(body, &document); err != nil {
				t.Fatal(err)
			}
			delete(document, "oob_login_mode")
			if mode != "" {
				document["oob_login_mode"] = string(mode)
			}
			body, err = yaml.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "gateway.yaml")
			if err = os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			resolved, err := c.Topology()
			if err != nil {
				t.Fatal(err)
			}
			for _, integration := range resolved {
				if integration.Config.OOBLoginMode != mode {
					t.Fatal("integration dropped the process OOB policy")
				}
			}
		})
	}
	c := topologyExample(t)
	c.OOBLoginMode = "unknown"
	if err := c.ValidateIntegrationNames(); err == nil {
		t.Fatal("unknown OOB policy accepted before backend/resource selection")
	}
}
