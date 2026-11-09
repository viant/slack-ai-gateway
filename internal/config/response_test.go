package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	presentation "github.com/viant/agently-core/sdk/slack"
)

func TestResponseConfiguration(t *testing.T) {
	for _, mode := range []string{"", "raw", "cli_compatible", "summary", "normalized_summary", "report_text", "unknown", "SUMMARY"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			body := "storage:\n  driver: sqlite\n  path: test.sqlite\ntransport: socket\naccess_mode: shared_channel\nslack:\n  app_id: A\n  workspace_id: T\n  channel_id: C\n"
			if mode != "" {
				body += "  response:\n    mode: " + mode + "\n"
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			c, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			err = c.ValidateSlack()
			invalid := mode == "unknown" || mode == "SUMMARY"
			if (err != nil) != invalid {
				t.Fatalf("mode %q: %v", mode, err)
			}
			if invalid && !strings.Contains(err.Error(), "slack.response.mode") {
				t.Fatal(err)
			}
			if mode == "" && c.ResponseMode() != presentation.ResponseRaw {
				t.Fatal("default changed")
			}
		})
	}
}
