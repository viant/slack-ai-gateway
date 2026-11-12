package config

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/viant/scy"
	"github.com/viant/scy/kms"
)

func TestSlackTokensUseNativeAWSBinaryPublication(t *testing.T) {
	ctx := context.Background()
	const keyRef = "blowfish://default"
	key, err := kms.NewKey(keyRef)
	if err != nil {
		t.Fatal("cannot prepare synthetic key")
	}
	cipher, err := kms.Lookup(key.Scheme)
	if err != nil {
		t.Fatal("cannot prepare synthetic cipher")
	}
	const appToken, botToken = "xapp-synthetic-only", "xoxb-synthetic-only"
	standardBinary, err := cipher.Encrypt(ctx, key, []byte(appToken))
	if err != nil {
		t.Fatal("cannot prepare synthetic ciphertext")
	}

	// AWS's SDK decodes the wire-level base64 before returning SecretBinary.
	// The pinned AFS provider expects another base64 layer, matching its own
	// native Upload. Standard raw AWS binary publication does not supply it.
	var mu sync.Mutex
	payloads := map[string][]byte{"synthetic/app-token.enc": standardBinary}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		var input struct {
			Name         string
			SecretID     string `json:"SecretId"`
			VersionStage string
			SecretBinary []byte
			SecretString *string
		}
		if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&input) != nil {
			t.Error("invalid native AWS fixture request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.Header.Get("X-Amz-Target") {
		case "secretsmanager.GetSecretValue":
			if input.VersionStage != "AWSCURRENT" {
				t.Error("native provider did not select AWSCURRENT")
			}
			payload, ok := payloads[input.SecretID]
			if !ok {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]string{"__type": "ResourceNotFoundException", "Message": "synthetic resource missing"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"Name": input.SecretID, "SecretBinary": payload})
		case "secretsmanager.CreateSecret":
			if input.SecretString != nil || len(input.SecretBinary) == 0 {
				t.Error("native ciphertext upload did not use SecretBinary")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			decoded := make([]byte, base64.StdEncoding.DecodedLen(len(input.SecretBinary)))
			if _, err := base64.StdEncoding.Decode(decoded, input.SecretBinary); err != nil {
				t.Error("native Upload did not preserve the provider's binary envelope")
			}
			payloads[input.Name] = input.SecretBinary
			_ = json.NewEncoder(w).Encode(map[string]string{"Name": input.Name})
		default:
			t.Error("unexpected AWS fixture operation")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	for name, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":                   "synthetic-access-key",
		"AWS_SECRET_ACCESS_KEY":               "synthetic-secret-key",
		"AWS_SESSION_TOKEN":                   "",
		"AWS_PROFILE":                         "",
		"AWS_CONFIG_FILE":                     filepath.Join(t.TempDir(), "no-config"),
		"AWS_SHARED_CREDENTIALS_FILE":         filepath.Join(t.TempDir(), "no-credentials"),
		"AWS_EC2_METADATA_DISABLED":           "true",
		"AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
		"AWS_ENDPOINT_URL_SECRETS_MANAGER":    server.URL,
	} {
		t.Setenv(name, value)
	}

	c := Config{Storage: Storage{Driver: "sqlite", Path: "slack-smoke.sqlite"}, Transport: "socket"}
	c.Slack.AppID, c.Slack.WorkspaceID = "A", "T"
	c.Slack.AppTokenSecret = &scy.Resource{URL: "aws://secretmanager/us-east-1/secret/synthetic/app-token.enc", Key: keyRef, MaxRetry: 1}
	c.Slack.BotTokenSecret = &scy.Resource{URL: "aws://secretmanager/us-east-1/secret/synthetic/bot-token.enc", Key: keyRef, MaxRetry: 1}
	if _, err := c.LoadSlackTokens(ctx); err == nil || strings.Contains(err.Error(), "synthetic/") || strings.Contains(err.Error(), appToken) {
		t.Fatal("standard raw AWS binary unexpectedly loaded or its failure disclosed secret data")
	}

	mu.Lock()
	delete(payloads, "synthetic/app-token.enc")
	mu.Unlock()
	for _, fixture := range []struct {
		resource *scy.Resource
		token    string
	}{{c.Slack.AppTokenSecret, appToken}, {c.Slack.BotTokenSecret, botToken}} {
		if err := scy.New().Store(ctx, scy.NewSecret(fixture.token, fixture.resource)); err != nil {
			t.Fatal("native AWS token publication failed")
		}
	}
	tokens, err := c.LoadSlackTokens(ctx)
	if err != nil || tokens.App != appToken || tokens.Bot != botToken {
		t.Fatal("native AWS token publication and loading did not round-trip")
	}
}
