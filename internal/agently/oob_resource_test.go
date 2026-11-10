package agently

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/scy"
	"github.com/viant/scy/cred"
	"github.com/viant/scy/kms"
)

func TestOOBResourceRejectsUnsupportedSourcesAndRedactsFailures(t *testing.T) {
	for _, resource := range []*scy.Resource{
		nil,
		{},
		{URL: "relative-secret-marker"},
		{URL: "file://secret-marker/credentials.json"},
		{URL: "https://secret-marker.invalid/credentials"},
		{URL: "inlined://base64/secret-marker"},
		{URL: "aws://ssm/us-east-1/secret/secret-marker"},
		{URL: "aws://secretmanager/us-east-1/secret/secret-marker?unsupported=true"},
		{URL: filepath.Join(t.TempDir(), "missing-secret-marker.json")},
		{URL: filepath.Join(t.TempDir(), "missing.json"), Fallback: &scy.Resource{URL: "https://secret-marker.invalid/credentials"}},
	} {
		_, err := loadBasicSecret(context.Background(), resource)
		if err == nil || strings.Contains(fmt.Sprintf("%+v %#v", err, err), "secret-marker") {
			t.Fatal("unsupported or missing resource accepted, or its location leaked")
		}
	}
}

func TestOOBResourceUsesNativeAWSSecretManagerAndReloadsCredentials(t *testing.T) {
	ctx := context.Background()
	const keyRef = "blowfish://default"
	key, err := kms.NewKey(keyRef)
	storetest.Must(t, err)
	payload := atomic.Value{}
	setPassword := func(password string) {
		basic := &cred.Basic{Username: "synthetic-user", Password: password}
		storetest.Must(t, basic.Cipher(ctx, key))
		data, marshalErr := json.Marshal(basic)
		storetest.Must(t, marshalErr)
		payload.Store(string(data))
	}
	setPassword("synthetic-first-password")
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.Header.Get("X-Amz-Target") != "secretsmanager.GetSecretValue" {
			t.Error("unexpected AWS fixture operation")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var input struct {
			SecretID     string `json:"SecretId"`
			VersionStage string
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil || input.SecretID != "synthetic/oob" || input.VersionStage != "AWSCURRENT" {
			t.Error("native provider did not preserve secret identity and version stage")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		if err := json.NewEncoder(w).Encode(map[string]string{"Name": "synthetic/oob", "SecretString": payload.Load().(string)}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	// Isolate the SDK's native identity/endpoint chain from real credentials,
	// profiles and metadata. Every provider request stays on this loopback server.
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
	resource := &scy.Resource{URL: "aws://secretmanager/us-east-1/secret/synthetic/oob", Key: keyRef, MaxRetry: 1}
	first, err := loadBasicSecret(ctx, resource)
	storetest.Must(t, err)
	if first.Username != "synthetic-user" || first.Password != "synthetic-first-password" {
		t.Fatal("native AWS credentials were not decrypted")
	}
	setPassword("synthetic-rotated-password")
	second, err := loadBasicSecret(ctx, resource)
	storetest.Must(t, err)
	if second.Password != "synthetic-rotated-password" || calls.Load() != 2 {
		t.Fatal("a new login did not reload the configured credential resource")
	}
	if resource.TimeoutMs != 0 || resource.Key != keyRef {
		t.Fatal("credential loading mutated configured resource")
	}
}

func TestOOBResourcePreservesNativeYAMLCredentials(t *testing.T) {
	for _, extension := range []string{".yaml", ".yml"} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encrypted=%v", extension, encrypted), func(t *testing.T) {
				ctx := context.Background()
				file := filepath.Join(t.TempDir(), "credentials"+extension)
				resource := &scy.Resource{URL: file}
				if encrypted {
					resource.Key = "blowfish://default"
					credentials := &cred.Basic{Username: "synthetic-yaml-user", Password: "synthetic-yaml-password"}
					storetest.Must(t, scy.New().Store(ctx, scy.NewSecret(credentials, resource)))
				} else {
					storetest.Must(t, os.WriteFile(file, []byte("username: synthetic-yaml-user\npassword: synthetic-yaml-password\n"), 0600))
				}
				basic, err := loadBasicSecret(ctx, resource)
				storetest.Must(t, err)
				if basic.Username != "synthetic-yaml-user" || basic.Password != "synthetic-yaml-password" || basic.EncryptedPassword != "" {
					t.Fatal("native YAML credentials were not preserved")
				}
				// Fallbacks use Scy's generic target rather than the root target.
				fallback := &scy.Resource{URL: filepath.Join(t.TempDir(), "missing.json"), Fallback: resource}
				basic, err = loadBasicSecret(ctx, fallback)
				storetest.Must(t, err)
				if basic.Username != "synthetic-yaml-user" || basic.Password != "synthetic-yaml-password" || basic.EncryptedPassword != "" {
					t.Fatal("native YAML fallback credentials were not preserved")
				}
			})
		}
	}
}
