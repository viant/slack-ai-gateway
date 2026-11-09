package agently

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storage/sqlite"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/scy/auth/flow"
	"github.com/viant/scy/cred"
	"github.com/viant/scy/kms"
	"golang.org/x/oauth2"
)

// Everything in this fixture is synthetic, including credentials and cookies.
// Only httptest loopback servers and t.TempDir files are used.
type loginFixture struct {
	t                    *testing.T
	server               *httptest.Server
	secretRef, configRef string
	mode                 atomic.Value
	sessionStatus        atomic.Int32
	mu                   sync.Mutex
	counts               map[string]int
	challenge, requestID string
	discovery            oauthDiscovery
	expectedScopes       string
}

func newLoginFixture(t *testing.T) *loginFixture {
	t.Helper()
	f := &loginFixture{
		t: t, counts: map[string]int{},
		discovery:      oauthDiscovery{Scopes: []string{"openid", "profile"}, CLIScopes: []string{"openid", "offline_access"}},
		expectedScopes: "openid profile offline_access",
	}
	f.mode.Store("")
	dir := t.TempDir()
	f.secretRef = filepath.Join(dir, "basic.json")
	f.configRef = filepath.Join(dir, "oauth.json")
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	config := oauth2.Config{ClientID: "offline-client", ClientSecret: "secret-marker-client", Scopes: []string{"file-scope-must-not-be-used"}, Endpoint: oauth2.Endpoint{AuthURL: f.server.URL + "/authorize", TokenURL: f.server.URL + "/token", AuthStyle: oauth2.AuthStyleInParams}}
	data, err := json.Marshal(config)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(f.configRef, data, 0600))
	storetest.Must(t, os.WriteFile(f.secretRef, []byte(`{"Username":"agently_scheduler","Password":"secret-marker-password"}`), 0600))
	if _, err := loadOAuthConfig(context.Background(), f.configRef); err != nil {
		t.Fatalf("synthetic config load: %v", err)
	}
	return f
}

func (f *loginFixture) count(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[path]
}
func (f *loginFixture) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.counts[r.URL.Path]++
	discovery, expectedScopes := f.discovery, f.expectedScopes
	f.mu.Unlock()
	mode := f.mode.Load().(string)
	w.Header().Set("Content-Type", "application/json")
	if r.Header.Get("Authorization") != "" {
		f.t.Error("unexpected bearer or impersonation credentials")
	}
	write := func(v any) {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			f.t.Error(err)
		}
	}
	switch r.URL.Path {
	case "/v1/api/auth/oauth/config":
		discovery.ConfigURL = f.configRef
		write(map[string]any{"data": discovery})
	case "/authorize":
		if r.Header.Get("Origin") != "" {
			f.t.Error("Agently origin leaked to IdP")
		}
		if mode == "timeout" {
			select {
			case <-r.Context().Done():
			case <-time.After(200 * time.Millisecond):
			}
			return
		}
		if err := r.ParseForm(); err != nil {
			f.t.Error(err)
		}
		if r.Method != "POST" || r.PostForm.Get("username") != "agently_scheduler" || r.PostForm.Get("password") != "secret-marker-password" || r.URL.Query().Get("scope") != expectedScopes || r.URL.Query().Get("code_challenge_method") != "S256" {
			f.t.Error("incorrect OOB form or scopes")
		}
		if r.Header.Get("Cookie") != "" {
			f.t.Error("Agently cookie leaked to IdP")
		}
		if mode == "bad_password" {
			http.Error(w, "secret-marker-password", 401)
			return
		}
		f.mu.Lock()
		f.challenge = r.URL.Query().Get("code_challenge")
		f.mu.Unlock()
		state := r.URL.Query().Get("state")
		if mode == "wrong_state" {
			state = "wrong"
		}
		location := "https://localhost/callback.html?code=secret-marker-code&state=" + url.QueryEscape(state)
		if mode == "redirect" {
			location = f.server.URL + "/must-not-follow?code=secret-marker-code"
		}
		if mode == "oauth_error" {
			location = "https://localhost/callback.html?error=secret-marker-error&error_description=secret-marker-description"
		}
		if mode == "missing_code" {
			location = "https://localhost/callback.html?state=" + url.QueryEscape(state)
		}
		if mode == "malformed_redirect" {
			location = "https://localhost/callback.html?code=%secret-marker"
		}
		w.Header().Set("Location", location)
		w.WriteHeader(302)
	case "/token":
		if r.Header.Get("Origin") != "" {
			f.t.Error("Agently origin leaked to IdP")
		}
		if err := r.ParseForm(); err != nil {
			f.t.Error(err)
		}
		f.mu.Lock()
		challenge := f.challenge
		f.mu.Unlock()
		if flow.GenerateCodeChallenge(r.Form.Get("code_verifier")) != challenge || r.Form.Get("redirect_uri") != "https://localhost/callback.html" || r.Form.Get("grant_type") != "authorization_code" {
			f.t.Error("PKCE exchange mismatch or unexpected refresh")
		}
		if mode == "token_error" {
			http.Error(w, "secret-marker-token", 400)
			return
		}
		write(map[string]any{"access_token": "secret-marker-access", "id_token": "secret-marker-id", "refresh_token": "secret-marker-refresh", "token_type": "Bearer", "expires_in": 3600})
	case "/v1/api/auth/session":
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			f.t.Error(err)
		}
		if len(payload) != 4 || payload["accessToken"] != "secret-marker-access" || payload["idToken"] != "secret-marker-id" || payload["refreshToken"] != "secret-marker-refresh" || payload["expiresAt"] == "" {
			f.t.Error("incorrect session token bundle")
		}
		if mode == "session_error" {
			http.Error(w, "secret-marker-session", 403)
			return
		}
		if status := int(f.sessionStatus.Load()); status != 0 {
			w.Header().Set("Location", f.server.URL+"/must-not-follow?secret-marker-location")
			w.Header().Set("X-Diagnostic", "secret-marker-header")
			http.Error(w, "secret-marker-session", status)
			return
		}
		if mode != "no_cookie" {
			http.SetCookie(w, &http.Cookie{Name: "agently_session", Value: "secret-marker-cookie", Path: "/", HttpOnly: true})
		}
		fmt.Fprint(w, `{}`)
	default:
		cookie, err := r.Cookie("agently_session")
		if err != nil || !strings.HasPrefix(cookie.Value, "secret-marker-cookie") {
			f.t.Error("missing session cookie")
			w.WriteHeader(401)
			return
		}
		// Match Core's cookie mutation origin guard (ae030080, 2026-10-23).
		// Preflight GETs succeed, but create/query POSTs fail before dispatch
		// unless the client explicitly supplies the Agently server origin.
		if r.Method == http.MethodPost && r.Header.Get("Origin") != f.server.URL {
			http.Error(w, "cross-origin cookie request denied", http.StatusForbidden)
			return
		}
		if mode == "revoked" || mode == "forbidden" {
			code := 401
			if mode == "forbidden" {
				code = 403
			}
			http.Error(w, "secret-marker-revoked", code)
			return
		}
		switch r.URL.Path {
		case "/v1/api/auth/me":
			subject, provider := "agently_scheduler", "session"
			if mode == "wrong_subject" {
				subject = "another-user"
			}
			if mode == "wrong_provider" {
				provider = "local"
			}
			if mode == "refresh" {
				http.SetCookie(w, &http.Cookie{Name: "agently_session", Value: "secret-marker-cookie-renewed", Path: "/", HttpOnly: true})
			}
			write(map[string]string{"subject": subject, "provider": provider})
		case "/v1/workspace/metadata":
			agent := "steward"
			if mode == "missing_agent" {
				agent = "other"
			}
			if mode == "refresh" && cookie.Value != "secret-marker-cookie-renewed" {
				f.t.Error("renewed session cookie ignored")
			}
			write(map[string]any{"agentInfos": []map[string]string{{"id": agent}}})
		case "/v1/conversations":
			var input map[string]string
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				f.t.Error(err)
			}
			if len(input) != 1 || input["agentId"] != "steward" {
				f.t.Error("create impersonation")
			}
			fmt.Fprint(w, `{"id":"c"}`)
		case "/v1/conversations/c":
			if r.URL.RawQuery != "includeTranscript=false" {
				f.t.Error("unexpected conversation projection")
			}
			owner, agent := "agently_scheduler", "steward"
			if mode == "wrong_owner" {
				owner = "foreign-owner"
			}
			if mode == "wrong_agent" {
				agent = "foreign-agent"
			}
			write(map[string]string{"id": "c", "createdByUserId": owner, "agentId": agent, "aguiThreadId": "wire-thread"})
		case "/v1/ag-ui/run":
			if mode == "run_forbidden" {
				http.Error(w, "secret-marker denied", http.StatusForbidden)
				return
			}
			var input struct {
				ThreadID, RunID string
				Messages        []struct{ ID, Role, Content string }
				ForwardedProps  struct {
					Agently struct {
						Version, Operation, RequestID string
						Target                        map[string]string
						Payload                       map[string]any
					}
				}
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				f.t.Error(err)
			}
			ext := input.ForwardedProps.Agently
			if r.Method != http.MethodPost || r.Header.Get("Accept") != "text/event-stream" || input.ThreadID != "wire-thread" || input.RunID == "" || ext.RequestID != input.RunID || ext.Version != "1" {
				f.t.Error("AG-UI contract mismatch")
			}
			f.mu.Lock()
			f.counts["agui."+ext.Operation]++
			priorID := f.requestID
			if ext.Operation == "chat" {
				f.requestID = input.RunID
			}
			f.mu.Unlock()
			switch ext.Operation {
			case "chat":
				if len(input.Messages) != 1 || input.Messages[0].ID != input.RunID || input.Messages[0].Role != "user" || ext.Payload["agentId"] != "steward" || ext.Payload["useServerState"] != true || len(ext.Target) != 0 {
					f.t.Error("chat payload mismatch")
				}
				if mode == "lost_post" {
					w.WriteHeader(503) // admitted, but the observer lost its response
					return
				}
				if mode == "lost_stream" {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprint(w, "data: {\"type\":\"RUN_STARTED\"")
					return // admitted, disconnected before receiving native identity
				}
			case "run.attach":
				if len(input.Messages) != 0 || len(ext.Payload) != 0 || ext.Target["threadId"] != "wire-thread" || priorID != input.RunID {
					f.t.Error("attachment replayed or changed admitted input")
				}
			default:
				f.t.Error("unexpected operation")
			}
			writeRunStarted(w, "wire-thread", input.RunID, "native-"+input.RunID)
		case "/v1/conversations/c/transcript":
			if mode == "lost_transcript" {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			f.mu.Lock()
			id := "native-" + f.requestID
			f.mu.Unlock()
			fmt.Fprintf(w, `{"conversation":{"conversationId":"c","turns":[{"turnId":%q,"startedByMessageId":"user-message-1","user":{"messageId":"user-message-1"},"status":"completed","assistant":{"final":{"content":"verified answer"}}}]}}`, id)
		default:
			f.t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}
}

func (f *loginFixture) login() (*Factory, error) {
	return LoginOOB(context.Background(), f.server.URL, "agently_scheduler", "steward", f.secretRef)
}

func TestOOBSessionPreflightAndServerCookieRenewal(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("refresh")
	factory, err := f.login()
	storetest.Must(t, err)
	if f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 {
		t.Fatal("preflight created/query conversation")
	}
	client, err := factory.ForPrincipal(context.Background(), "agently_scheduler")
	storetest.Must(t, err)
	id, err := client.CreateConversation(context.Background(), "steward")
	storetest.Must(t, err)
	turn, err := client.Query(context.Background(), gateway.Query{ConversationID: id, MessageID: "request", AgentID: "steward", Text: "question"}, nil)
	storetest.Must(t, err)
	if turn.Content != "verified answer" || f.count("/token") != 1 || f.count("/v1/api/auth/session") != 1 {
		t.Fatal("wrong response or gateway refreshed/relogged in")
	}
}

func TestOOBLoginFailuresAreRedactedAndFailClosed(t *testing.T) {
	diagnostics := map[string]string{
		"bad_password":       "Agently OOB authorization failed (authorization endpoint, HTTP 401)",
		"wrong_state":        "Agently OOB authorization failed (authorization state mismatch, HTTP 302)",
		"redirect":           "Agently OOB authorization failed (authorization redirect, HTTP 302)",
		"oauth_error":        "Agently OOB authorization failed (authorization rejected, HTTP 302)",
		"missing_code":       "Agently OOB authorization failed (authorization code missing, HTTP 302)",
		"malformed_redirect": "Agently OOB authorization failed (authorization code missing, HTTP 302)",
		"token_error":        "Agently OOB authorization failed (token exchange, HTTP 400)",
	}
	for _, mode := range []string{"bad_password", "wrong_state", "redirect", "oauth_error", "missing_code", "malformed_redirect", "token_error", "session_error", "no_cookie", "wrong_subject", "wrong_provider", "missing_agent"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			f.mode.Store(mode)
			factory, err := f.login()
			if factory != nil || err == nil || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("unsafe login result: %v", err)
			}
			if want := diagnostics[mode]; want != "" && err.Error() != want {
				t.Fatalf("diagnostic = %q, want %q", err.Error(), want)
			}
			stage := "/authorize"
			switch mode {
			case "token_error":
				stage = "/token"
			case "session_error", "no_cookie":
				stage = "/v1/api/auth/session"
			case "wrong_subject", "wrong_provider":
				stage = "/v1/api/auth/me"
			case "missing_agent":
				stage = "/v1/workspace/metadata"
			}
			if f.count(stage) != 1 {
				t.Fatalf("failure did not reach expected stage %s", stage)
			}
			if strings.HasPrefix(stage, "/authorize") && (f.count("/token") != 0 || f.count("/v1/api/auth/session") != 0) {
				t.Fatal("authorization failure reached token exchange or session creation")
			}
			if f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 || f.count("/must-not-follow") != 0 {
				t.Fatal("unsafe side effect")
			}
		})
	}
}

type sessionRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn sessionRoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func TestOOBTransportFailureDiagnosticsAreRedacted(t *testing.T) {
	for _, stage := range []string{"authorization request", "token exchange"} {
		t.Run(stage, func(t *testing.T) {
			f := newLoginFixture(t)
			path := "/authorize"
			if stage == "token exchange" {
				path = "/token"
			}
			transport := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(transport.CloseIdleConnections)
			var attempts atomic.Int32
			client := &http.Client{Transport: sessionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == path {
					attempts.Add(1)
					return nil, errors.New("secret-marker-transport")
				}
				return transport.RoundTrip(r)
			})}
			factory, err := loginOOB(context.Background(), f.server.URL, "agently_scheduler", "steward", f.secretRef, loadOAuthConfig, loadBasicSecret, client)
			want := "Agently OOB authorization failed (" + stage + ", HTTP status unavailable)"
			if factory != nil || err == nil || err.Error() != want || strings.Contains(fmt.Sprintf("%+v %#v", err, err), "secret-marker") {
				t.Fatalf("unsafe transport diagnostic: %v", err)
			}
			if attempts.Load() != 1 || f.count("/v1/api/auth/session") != 0 || f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 {
				t.Fatal("failed login retried or reached execution")
			}
		})
	}
}

func TestOOBSessionFailureReportsOnlyHTTPStatus(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusServiceUnavailable, http.StatusFound, 0} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			f := newLoginFixture(t)
			f.sessionStatus.Store(int32(status))
			transport := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(transport.CloseIdleConnections)
			var attempts atomic.Int32
			client := &http.Client{Transport: sessionRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/v1/api/auth/session" {
					attempts.Add(1)
					if status == 0 {
						return nil, errors.New("secret-marker-transport")
					}
				}
				return transport.RoundTrip(r)
			})}
			factory, err := loginOOB(context.Background(), f.server.URL, "agently_scheduler", "steward", f.secretRef, loadOAuthConfig, loadBasicSecret, client)
			if factory != nil || err == nil {
				t.Fatal("failed session accepted")
			}
			want := fmt.Sprintf("Agently session creation failed (POST /v1/api/auth/session, HTTP %d)", status)
			if status == 0 {
				want = "Agently session creation failed (POST /v1/api/auth/session, HTTP status unavailable)"
			}
			if err.Error() != want || strings.Contains(fmt.Sprintf("%+v %#v", err, err), "secret-marker") {
				t.Fatalf("unexpected session diagnostic: %v", err)
			}
			if attempts.Load() != 1 || f.count("/token") != 1 {
				t.Fatal("session creation or token exchange retried")
			}
			for _, path := range []string{"/v1/api/auth/me", "/v1/workspace/metadata", "/v1/conversations", "/v1/ag-ui/run", "/must-not-follow"} {
				if f.count(path) != 0 {
					t.Fatalf("unexpected call after session failure: %s", path)
				}
			}
		})
	}
}

func TestEncryptedLocalOOBAndOAuthFiles(t *testing.T) {
	f := newLoginFixture(t)
	ctx := context.Background()
	const keyRef = "blowfish://inline/offline-test-key"
	key, err := kms.NewKey(keyRef)
	storetest.Must(t, err)
	basic := cred.Basic{Username: "agently_scheduler", Password: "secret-marker-password"}
	storetest.Must(t, basic.Cipher(ctx, key))
	data, err := json.Marshal(basic)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(f.secretRef, data, 0600))
	if _, err := loadBasicSecret(ctx, f.secretRef); err == nil {
		t.Fatal("encrypted file accepted without key")
	}
	f.secretRef += "|" + keyRef
	config, err := loadOAuthConfig(ctx, f.configRef)
	storetest.Must(t, err)
	encrypted := cred.Oauth2Config{Config: *config}
	storetest.Must(t, encrypted.Cipher(ctx, key))
	data, err = json.Marshal(encrypted)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(f.configRef, data, 0600))
	if _, err := loadOAuthConfig(ctx, f.configRef); err == nil {
		t.Fatal("encrypted OAuth config accepted without key")
	}
	f.configRef += "|" + keyRef
	_, err = f.login()
	storetest.Must(t, err)
}

func TestInlineOAuthConfigAndMalformedSecrets(t *testing.T) {
	f := newLoginFixture(t)
	data, err := os.ReadFile(f.configRef)
	storetest.Must(t, err)
	f.configRef = "inlined://base64/" + base64.StdEncoding.EncodeToString(data)
	_, err = f.login()
	storetest.Must(t, err)
	if _, err = loadOAuthConfig(context.Background(), "inlined://base64/invalid!"); err == nil {
		t.Fatal("invalid inline config accepted")
	}
	// Deliberately too short for the legacy cipher: no panic or secret in errors.
	storetest.Must(t, os.WriteFile(f.secretRef, []byte(`{"Username":"agently_scheduler","EncryptedPassword":"AA=="}`), 0600))
	_, err = loadBasicSecret(context.Background(), f.secretRef+"|blowfish://inline/offline-test-key")
	if err == nil || strings.Contains(err.Error(), "AA==") {
		t.Fatal("malformed ciphertext was not safely rejected")
	}
}

func TestSessionRejectionPoisonsAllClients(t *testing.T) {
	for _, mode := range []string{"revoked", "forbidden", "wrong_subject", "wrong_provider", "wrong_owner", "wrong_agent", "expired", "wrong_principal", "smoke"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			factory, err := f.login()
			storetest.Must(t, err)
			client, err := factory.ForPrincipal(context.Background(), "agently_scheduler")
			storetest.Must(t, err)
			f.mode.Store(mode)
			id := "c"
			if mode == "expired" {
				u, _ := url.Parse(f.server.URL)
				factory.http.Jar.SetCookies(u, []*http.Cookie{{Name: "agently_session", Path: "/", MaxAge: -1}})
			}
			if mode == "wrong_principal" {
				_, err = factory.ForPrincipal(context.Background(), "other")
				if !errors.Is(err, gateway.ErrBackendAuth) {
					t.Fatal("principal accepted")
				}
			}
			if mode == "smoke" {
				id = "slack-smoke-fake"
			}
			_, err = client.Query(context.Background(), gateway.Query{ConversationID: id, MessageID: "r", AgentID: "steward", Text: "must not send"}, nil)
			if !errors.Is(err, gateway.ErrBackendAuth) {
				t.Fatalf("not fatal: %v", err)
			}
			f.mode.Store("") // A server recovery cannot silently revive a poisoned factory.
			_, err = client.CreateConversation(context.Background(), "steward")
			if !errors.Is(err, gateway.ErrBackendAuth) || f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 || f.count("/v1/conversations/c/transcript") != 0 {
				t.Fatal("rejection did not stop further operations")
			}
		})
	}
}

func TestOOBTimeoutCancellationAndLocalReference(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("timeout")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := loginOOB(ctx, f.server.URL, "agently_scheduler", "steward", f.secretRef, loadOAuthConfig, loadBasicSecret, &http.Client{Timeout: 25 * time.Millisecond})
	if err == nil || time.Since(start) > time.Second || f.count("/v1/api/auth/session") != 0 {
		t.Fatal("OOB request was not bounded")
	}
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, err = LoginOOB(ctx, f.server.URL, "agently_scheduler", "steward", f.secretRef)
	if err == nil {
		t.Fatal("canceled login accepted")
	}
	for _, ref := range []string{"inlined://base64/secret-marker", "https://example.invalid/secret-marker", "relative-secret-marker"} {
		if _, err := loadBasicSecret(context.Background(), ref); err == nil {
			t.Fatal("nonlocal reference accepted")
		}
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", sessionPayload{AccessToken: "secret-marker"}, sessionPayload{IDToken: "secret-marker"}), "secret-marker") {
		t.Fatal("token bundle formatting leaks")
	}
}

func TestSessionWorkerReconcilesAfterRestartWithoutChatReplay(t *testing.T) {
	for _, mode := range []string{"lost_post", "lost_stream", "lost_transcript"} {
		t.Run(mode, func(t *testing.T) {
			testSessionWorkerReconciliation(t, mode)
		})
	}
}

func TestAGUISessionRejectionStopsChatAndAttachment(t *testing.T) {
	for _, attach := range []bool{false, true} {
		t.Run(fmt.Sprintf("attach=%v", attach), func(t *testing.T) {
			f := newLoginFixture(t)
			factory, err := f.login()
			storetest.Must(t, err)
			client, err := factory.ForPrincipal(context.Background(), "agently_scheduler")
			storetest.Must(t, err)
			if attach {
				_, err = client.Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "r", AgentID: "steward", Text: "question"}, nil)
				storetest.Must(t, err)
			}
			before := f.count("/v1/ag-ui/run")
			f.mode.Store("run_forbidden") // metadata and /me still succeed
			if attach {
				_, err = client.ReadTurn(context.Background(), "c", "r", nil)
			} else {
				_, err = client.Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "r", AgentID: "steward", Text: "question"}, nil)
			}
			if !errors.Is(err, gateway.ErrBackendAuth) || f.count("/v1/ag-ui/run") != before+1 {
				t.Fatalf("run rejection not fatal: %v", err)
			}
			f.mode.Store("")
			_, err = client.ReadTurn(context.Background(), "c", "r", nil)
			if !errors.Is(err, gateway.ErrBackendAuth) || f.count("/v1/ag-ui/run") != before+1 {
				t.Fatal("poisoned session silently recovered")
			}
		})
	}
}

func TestSessionSharedThreadFollowupUsesExistingConversation(t *testing.T) {
	f := newLoginFixture(t)
	factory, err := f.login()
	storetest.Must(t, err)
	ctx := context.Background()
	s, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "shared.sqlite"), true)
	storetest.Must(t, err)
	defer s.Close()
	storetest.Must(t, s.Migrate(ctx))
	w := gateway.Worker{Store: s, Clients: factory, AgentID: "steward", Policy: gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "agently_scheduler"}}
	for i, author := range []string{"alice", "bob"} {
		_, err = s.Enqueue(ctx, storetest.Message(fmt.Sprintf("event-%d", i), "123.456", author))
		storetest.Must(t, err)
		worked, err := w.Step(ctx)
		storetest.Must(t, err)
		if !worked {
			t.Fatal("followup not processed")
		}
	}
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(status.Bindings) != 1 || len(status.Jobs) != 2 || len(status.Deliveries) != 2 || f.count("/v1/conversations") != 1 || f.count("agui.chat") != 2 {
		t.Fatal("shared-thread followup lost its conversation or response")
	}
	for _, job := range status.Jobs {
		if job.State != "completed" || job.TurnID != job.RequestID {
			t.Fatal("server-native turn identity leaked into durable logical ID")
		}
	}
}

func testSessionWorkerReconciliation(t *testing.T, mode string) {
	f := newLoginFixture(t)
	ctx := context.Background()
	factory, err := f.login()
	storetest.Must(t, err)
	dbPath := filepath.Join(t.TempDir(), "real.sqlite")
	s, err := sqlite.Open(ctx, dbPath, true)
	storetest.Must(t, err)
	storetest.Must(t, s.Migrate(ctx))
	_, err = s.Enqueue(ctx, storetest.Message("event", "123.456", "alice"))
	storetest.Must(t, err)
	w := gateway.Worker{Store: s, Clients: factory, AgentID: "steward", Policy: gateway.SharedChannelPolicy{AppID: "A", WorkspaceID: "T", ChannelID: "C", PrincipalID: "agently_scheduler"}, Timing: gateway.QueryTiming{PollInitial: time.Millisecond}}
	f.mode.Store(mode)
	_, err = w.Step(ctx)
	storetest.Must(t, err)
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if status.Jobs[0].State != "querying" || len(status.Deliveries) != 0 || status.Bindings[0].OwnerID != "agently_scheduler" {
		t.Fatal("uncertain query state lost")
	}
	storetest.Must(t, s.Close())
	f.mode.Store("")
	factory, err = f.login()
	storetest.Must(t, err) // deliberate process restart: new login
	s, err = sqlite.Open(ctx, dbPath, false)
	storetest.Must(t, err)
	defer s.Close()
	storetest.Must(t, s.Recover(ctx))
	w.Store = s
	w.Clients = factory
	time.Sleep(5 * time.Millisecond)
	_, err = w.Step(ctx)
	storetest.Must(t, err)
	status, err = s.Status(ctx, 10)
	storetest.Must(t, err)
	if status.Jobs[0].State != "completed" || len(status.Deliveries) != 1 || f.count("agui.chat") != 1 || f.count("agui.run.attach") != 1 || f.count("/v1/ag-ui/run") != 2 || f.count("/v1/conversations") != 1 {
		t.Fatal("query replayed or persisted answer lost")
	}
	delivery, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if delivery == nil || delivery.Text != "verified answer" || delivery.RootTS != "123.456" {
		t.Fatal("wrong persisted delivery")
	}
}
