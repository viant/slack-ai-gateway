package agently

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/scy"
	"github.com/viant/scy/auth/flow"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"golang.org/x/oauth2"
)

// These fixtures exercise the public login path, including native file-based
// secret loading, PKCE exchange, cookie creation and identity verification.
// Their token barriers observe actual HTTP overlap without authenticating to a
// real IdP or sharing authenticated factories between integrations.
type coordinatedIDP struct {
	t             *testing.T
	server        *httptest.Server
	tokenEntered  chan struct{}
	tokenRelease  chan struct{}
	authorizeCall atomic.Int32
	tokenCall     atomic.Int32
	rejectToken   atomic.Bool
	mu            sync.Mutex
	grants        map[string]string
	states        map[string]bool
	challenges    map[string]bool
}

func newCoordinatedIDP(t *testing.T) *coordinatedIDP {
	t.Helper()
	f := &coordinatedIDP{
		t: t, tokenEntered: make(chan struct{}, 8), tokenRelease: make(chan struct{}, 8),
		grants: map[string]string{}, states: map[string]bool{}, challenges: map[string]bool{},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *coordinatedIDP) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost || r.Header.Get("Origin") != "" || r.Header.Get("Cookie") != "" {
		f.t.Error("IdP received an unexpected method or backend session headers")
	}
	if err := r.ParseForm(); err != nil {
		f.t.Error("invalid synthetic authorization form")
	}
	switch r.URL.Path {
	case "/authorize":
		if r.PostForm.Get("username") == "" || r.PostForm.Get("password") != "synthetic-password" || r.URL.Query().Get("code_challenge_method") != "S256" || r.URL.Query().Get("scope") != "openid profile offline_access" {
			f.t.Error("native OOB credentials, scopes or PKCE policy changed")
		}
		state, challenge := r.URL.Query().Get("state"), r.URL.Query().Get("code_challenge")
		f.mu.Lock()
		if state == "" || challenge == "" || f.states[state] || f.challenges[challenge] {
			f.t.Error("OOB logins reused state or PKCE material")
		}
		f.states[state], f.challenges[challenge] = true, true
		code := fmt.Sprintf("synthetic-code-%d", len(f.grants)+1)
		f.grants[code] = challenge
		f.mu.Unlock()
		f.authorizeCall.Add(1)
		w.Header().Set("Location", "https://localhost/callback.html?code="+code+"&state="+url.QueryEscape(state))
		w.WriteHeader(http.StatusFound)
	case "/token":
		f.tokenCall.Add(1)
		f.mu.Lock()
		challenge, exists := f.grants[r.PostForm.Get("code")]
		delete(f.grants, r.PostForm.Get("code"))
		f.mu.Unlock()
		if !exists || flow.GenerateCodeChallenge(r.PostForm.Get("code_verifier")) != challenge || r.PostForm.Get("redirect_uri") != "https://localhost/callback.html" || r.PostForm.Get("grant_type") != "authorization_code" || r.PostForm.Get("client_id") != "synthetic-client" || r.PostForm.Get("client_secret") != "synthetic-client-secret" {
			f.t.Error("native token exchange did not preserve its own grant or client authentication")
		}
		select {
		case f.tokenEntered <- struct{}{}:
		case <-r.Context().Done():
			return
		}
		select {
		case <-f.tokenRelease:
		case <-r.Context().Done():
			return
		}
		if f.rejectToken.Swap(false) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "synthetic-access", "id_token": "synthetic-id",
			"refresh_token": "synthetic-refresh", "token_type": "Bearer", "expires_in": 3600,
		})
	default:
		f.t.Error("unexpected IdP endpoint")
		w.WriteHeader(http.StatusNotFound)
	}
}

type coordinatedBackend struct {
	server       *httptest.Server
	secret       *scy.Resource
	principal    string
	agent        string
	configRef    string
	requests     atomic.Int32
	sessionCalls atomic.Int32
}

func newCoordinatedBackend(t *testing.T, idp *coordinatedIDP, user, principal, agent string) *coordinatedBackend {
	t.Helper()
	dir := t.TempDir()
	f := &coordinatedBackend{
		secret:    &scy.Resource{URL: filepath.Join(dir, "basic.json")},
		principal: principal, agent: agent, configRef: filepath.Join(dir, "oauth.json"),
	}
	config := oauth2.Config{ClientID: "synthetic-client", ClientSecret: "synthetic-client-secret", Endpoint: oauth2.Endpoint{
		AuthURL: idp.server.URL + "/authorize", TokenURL: idp.server.URL + "/token", AuthStyle: oauth2.AuthStyleInParams,
	}}
	data, err := json.Marshal(config)
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(f.configRef, data, 0600))
	data, err = json.Marshal(map[string]string{"Username": user, "Password": "synthetic-password"})
	storetest.Must(t, err)
	storetest.Must(t, os.WriteFile(f.secret.URL, data, 0600))
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "" {
			t.Error("cookie login supplied unexpected bearer credentials")
		}
		switch r.URL.Path {
		case "/v1/api/auth/oauth/config":
			_ = json.NewEncoder(w).Encode(oauthDiscovery{ConfigURL: f.configRef, Scopes: []string{"openid", "profile"}, CLIScopes: []string{"openid", "offline_access"}})
		case "/v1/api/auth/session":
			f.sessionCalls.Add(1)
			var payload sessionPayload
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload.AccessToken != "synthetic-access" || payload.IDToken != "synthetic-id" || payload.RefreshToken != "synthetic-refresh" || payload.ExpiresAt == "" {
				t.Error("session creation did not use the native token bundle")
			}
			if r.Method != http.MethodPost || r.Header.Get("Cookie") != "" {
				t.Error("session bootstrap used an unexpected method or an existing session cookie")
			}
			http.SetCookie(w, &http.Cookie{Name: "agently_session", Value: "synthetic-cookie-" + agent, Path: "/", HttpOnly: true})
			_, _ = w.Write([]byte(`{}`))
		case "/v1/api/auth/me", "/v1/workspace/metadata":
			cookie, err := r.Cookie("agently_session")
			if err != nil || cookie.Value != "synthetic-cookie-"+agent {
				t.Error("backend received another integration's session cookie")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if r.URL.Path == "/v1/api/auth/me" {
				_ = json.NewEncoder(w).Encode(map[string]string{"subject": principal, "provider": "session"})
			} else {
				_ = json.NewEncoder(w).Encode(map[string]any{"agentInfos": []map[string]string{{"id": agent}}})
			}
		default:
			t.Error("authentication unexpectedly called a conversation or other backend endpoint")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}

type coordinatedResult struct {
	factory *Factory
	err     error
}

func (b *coordinatedBackend) start(ctx context.Context, modes ...OOBLoginMode) <-chan coordinatedResult {
	result := make(chan coordinatedResult, 1)
	go func() {
		factory, err := LoginOOB(ctx, b.server.URL, b.principal, b.agent, b.secret, modes...)
		result <- coordinatedResult{factory, err}
	}()
	return result
}

func awaitCoordinatedToken(t *testing.T, ctx context.Context, f *coordinatedIDP) {
	t.Helper()
	select {
	case <-f.tokenEntered:
	case <-ctx.Done():
		t.Fatal("expected native token request did not arrive")
	}
}

func awaitCoordinatedResult(t *testing.T, ctx context.Context, result <-chan coordinatedResult) coordinatedResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-ctx.Done():
		t.Fatal("native OOB login did not finish")
		return coordinatedResult{}
	}
}

func awaitCoordinatedWaiter(t *testing.T, ctx context.Context, idp *coordinatedIDP, username string) {
	t.Helper()
	key := loginKey(idp.server.URL+"/token", username)
	for {
		processOOBLogins.mu.Lock()
		slot := processOOBLogins.slots[key]
		waiting := slot != nil && slot.users == 2
		processOOBLogins.mu.Unlock()
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("second OOB login did not reach the coordination boundary")
		case <-time.After(time.Millisecond):
		}
	}
}

func assertCoordinatedKeyReleased(t *testing.T, idp *coordinatedIDP, username string) {
	t.Helper()
	processOOBLogins.mu.Lock()
	_, exists := processOOBLogins.slots[loginKey(idp.server.URL+"/token", username)]
	processOOBLogins.mu.Unlock()
	if exists {
		t.Fatal("completed or canceled logins retained their account coordination entry")
	}
}

func assertCoordinatedSessions(t *testing.T, ctx context.Context, first, second coordinatedResult) {
	t.Helper()
	storetest.Must(t, first.err)
	storetest.Must(t, second.err)
	if first.factory == second.factory || first.factory.http.Jar == second.factory.http.Jar {
		t.Fatal("OOB coordination shared an authenticated factory or cookie jar")
	}
	storetest.Must(t, first.factory.Verify(ctx))
	storetest.Must(t, second.factory.Verify(ctx))
}

func TestOOBLoginParallelModePreservesIndependentNativeFlows(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "explicit"}[explicit], func(t *testing.T) {
			idp := newCoordinatedIDP(t)
			first := newCoordinatedBackend(t, idp, "same-account", "principal-a", "agent-a")
			second := newCoordinatedBackend(t, idp, "same-account", "principal-b", "agent-b")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var modes []OOBLoginMode
			if explicit {
				modes = []OOBLoginMode{OOBLoginParallel}
			}
			a, b := first.start(ctx, modes...), second.start(ctx, modes...)
			awaitCoordinatedToken(t, ctx, idp)
			awaitCoordinatedToken(t, ctx, idp) // Both are inside HTTP before either completes.
			idp.tokenRelease <- struct{}{}
			idp.tokenRelease <- struct{}{}
			assertCoordinatedSessions(t, ctx, awaitCoordinatedResult(t, ctx, a), awaitCoordinatedResult(t, ctx, b))
			if idp.authorizeCall.Load() != 2 || idp.tokenCall.Load() != 2 {
				t.Fatal("parallel policy retried or combined native logins")
			}
			assertCoordinatedKeyReleased(t, idp, "same-account")
		})
	}
}

func TestOOBLoginSerialPerUserCoordinatesActualAccountAcrossIntegrations(t *testing.T) {
	idp := newCoordinatedIDP(t)
	first := newCoordinatedBackend(t, idp, "same-account", "principal-a", "agent-a")
	second := newCoordinatedBackend(t, idp, "same-account", "principal-b", "agent-b")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a := first.start(ctx, OOBLoginSerialPerUser)
	awaitCoordinatedToken(t, ctx, idp)
	b := second.start(ctx, OOBLoginSerialPerUser)
	awaitCoordinatedWaiter(t, ctx, idp, "same-account")
	if idp.authorizeCall.Load() != 1 || idp.tokenCall.Load() != 1 || second.sessionCalls.Load() != 0 {
		t.Fatal("same actual account overlapped despite different backend identities and secret resources")
	}
	idp.tokenRelease <- struct{}{}
	awaitCoordinatedToken(t, ctx, idp)
	idp.tokenRelease <- struct{}{}
	assertCoordinatedSessions(t, ctx, awaitCoordinatedResult(t, ctx, a), awaitCoordinatedResult(t, ctx, b))
	if idp.authorizeCall.Load() != 2 || idp.tokenCall.Load() != 2 {
		t.Fatal("serialized logins did not perform two independent native grants")
	}
	assertCoordinatedKeyReleased(t, idp, "same-account")
}

func TestOOBLoginSerialPerUserLeavesOtherAccountsAndEndpointsIndependent(t *testing.T) {
	for _, distinctEndpoint := range []bool{false, true} {
		t.Run(map[bool]string{false: "different_account", true: "different_endpoint"}[distinctEndpoint], func(t *testing.T) {
			idpA := newCoordinatedIDP(t)
			idpB, secondUser := idpA, "second-account"
			if distinctEndpoint {
				idpB, secondUser = newCoordinatedIDP(t), "first-account"
			}
			first := newCoordinatedBackend(t, idpA, "first-account", "principal-a", "agent-a")
			second := newCoordinatedBackend(t, idpB, secondUser, "principal-b", "agent-b")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			a := first.start(ctx, OOBLoginSerialPerUser)
			awaitCoordinatedToken(t, ctx, idpA)
			b := second.start(ctx, OOBLoginSerialPerUser)
			awaitCoordinatedToken(t, ctx, idpB) // The first account is still blocked in its token HTTP.
			idpA.tokenRelease <- struct{}{}
			idpB.tokenRelease <- struct{}{}
			assertCoordinatedSessions(t, ctx, awaitCoordinatedResult(t, ctx, a), awaitCoordinatedResult(t, ctx, b))
			assertCoordinatedKeyReleased(t, idpA, "first-account")
			assertCoordinatedKeyReleased(t, idpB, secondUser)
		})
	}
}

func TestOOBLoginCanceledWaiterSendsNoAuthorizationAndDoesNotRetainAccount(t *testing.T) {
	idp := newCoordinatedIDP(t)
	first := newCoordinatedBackend(t, idp, "same-account", "principal-a", "agent-a")
	second := newCoordinatedBackend(t, idp, "same-account", "principal-b", "agent-b")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a := first.start(ctx, OOBLoginSerialPerUser)
	awaitCoordinatedToken(t, ctx, idp)
	waitCtx, cancelWait := context.WithCancel(ctx)
	defer cancelWait()
	b := second.start(waitCtx, OOBLoginSerialPerUser)
	awaitCoordinatedWaiter(t, ctx, idp, "same-account")
	cancelWait()
	got := awaitCoordinatedResult(t, ctx, b)
	var failure *authFailure
	if !errors.As(got.err, &failure) || failure.permanent || failure.stage != "oob_login_wait" || got.factory != nil {
		t.Fatal("canceled waiter was not returned as a controlled, recoverable wait interruption")
	}
	if idp.authorizeCall.Load() != 1 || idp.tokenCall.Load() != 1 || second.sessionCalls.Load() != 0 {
		t.Fatal("canceled waiter sent authorization credentials or created a backend session")
	}
	idp.tokenRelease <- struct{}{}
	storetest.Must(t, awaitCoordinatedResult(t, ctx, a).err)
	assertCoordinatedKeyReleased(t, idp, "same-account")
	c := second.start(ctx, OOBLoginSerialPerUser)
	awaitCoordinatedToken(t, ctx, idp)
	idp.tokenRelease <- struct{}{}
	storetest.Must(t, awaitCoordinatedResult(t, ctx, c).err)
	assertCoordinatedKeyReleased(t, idp, "same-account")
}

func TestOOBLoginFailedOwnerReleasesNextNativeLogin(t *testing.T) {
	for _, cancelOwner := range []bool{false, true} {
		t.Run(map[bool]string{false: "token_rejected", true: "owner_canceled"}[cancelOwner], func(t *testing.T) {
			idp := newCoordinatedIDP(t)
			first := newCoordinatedBackend(t, idp, "same-account", "principal-a", "agent-a")
			second := newCoordinatedBackend(t, idp, "same-account", "principal-b", "agent-b")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ownerCtx, cancelFirst := context.WithCancel(ctx)
			defer cancelFirst()
			a := first.start(ownerCtx, OOBLoginSerialPerUser)
			awaitCoordinatedToken(t, ctx, idp)
			b := second.start(ctx, OOBLoginSerialPerUser)
			awaitCoordinatedWaiter(t, ctx, idp, "same-account")
			if cancelOwner {
				cancelFirst()
			} else {
				idp.rejectToken.Store(true)
				idp.tokenRelease <- struct{}{}
			}
			if got := awaitCoordinatedResult(t, ctx, a); got.err == nil || got.factory != nil || first.sessionCalls.Load() != 0 {
				t.Fatal("failed owner unexpectedly created an authenticated session")
			}
			awaitCoordinatedToken(t, ctx, idp)
			idp.tokenRelease <- struct{}{}
			storetest.Must(t, awaitCoordinatedResult(t, ctx, b).err)
			assertCoordinatedKeyReleased(t, idp, "same-account")
		})
	}
}

func TestOOBLoginInvalidCoordinationModeFailsBeforeHTTP(t *testing.T) {
	for _, modes := range [][]OOBLoginMode{{"unsupported"}, {OOBLoginParallel, OOBLoginSerialPerUser}} {
		t.Run(fmt.Sprint(modes), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
			defer server.Close()
			factory, err := LoginOOB(context.Background(), server.URL, "principal", "agent", nil, modes...)
			var failure *authFailure
			if !errors.As(err, &failure) || !failure.permanent || failure.stage != "local_configuration" || factory != nil || requests.Load() != 0 {
				t.Fatal("invalid coordination mode reached HTTP or did not fail as local configuration")
			}
		})
	}
}
