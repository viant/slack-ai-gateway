package agently

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/scy/auth/flow"
	"github.com/viant/scy/cred"
	"golang.org/x/oauth2"
)

func TestOOBTokenExchangeDiagnostics(t *testing.T) {
	for _, test := range []struct {
		code, wantCode string
		status         int
		permanent      bool
	}{
		{"invalid_client", "invalid_client", http.StatusUnauthorized, true},
		{"invalid_grant", "invalid_grant", http.StatusUnauthorized, true},
		{"unauthorized_client", "unauthorized_client", http.StatusUnauthorized, true},
		{"invalid_request", "invalid_request", http.StatusBadRequest, true},
		{"access_denied", "access_denied", http.StatusForbidden, true},
		{"temporarily_unavailable", "temporarily_unavailable", http.StatusTooManyRequests, false},
		{"server_error", "server_error", http.StatusServiceUnavailable, false},
		{"unsupported_grant_type", "unsupported_grant_type", http.StatusBadRequest, true},
		{"secret-marker-code\ninvalid_grant", "other", http.StatusUnauthorized, true},
		{"", "", http.StatusUnauthorized, true},
	} {
		name := test.code
		if name == "" {
			name = "missing_code"
		}
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var challenge string
			var tokenCalls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/authorize" {
					mu.Lock()
					challenge = r.URL.Query().Get("code_challenge")
					mu.Unlock()
					http.Redirect(w, r, "https://localhost/callback.html?code=secret-marker-code&state="+url.QueryEscape(r.URL.Query().Get("state")), http.StatusFound)
					return
				}
				if r.URL.Path != "/token" {
					t.Error("unexpected endpoint")
					w.WriteHeader(http.StatusNotFound)
					return
				}
				mu.Lock()
				tokenCalls++
				wantChallenge := challenge
				mu.Unlock()
				if err := r.ParseForm(); err != nil {
					t.Error("invalid synthetic form")
				}
				if flow.GenerateCodeChallenge(r.Form.Get("code_verifier")) != wantChallenge || r.Form.Get("code") != "secret-marker-code" {
					t.Error("native PKCE flow changed")
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "2")
				w.Header().Set("WWW-Authenticate", "secret-marker-header")
				w.WriteHeader(test.status)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": test.code, "error_description": "secret-marker-description", "error_uri": "https://secret-marker.invalid/error"})
			}))
			defer server.Close()
			config := diagnosticOAuthConfig(server.URL, oauth2.AuthStyleInParams)
			payload, err := authorizeOOB(context.Background(), config, &cred.Basic{Username: "secret-marker-user", Password: "secret-marker-password"}, diagnosticIDPClient())
			mu.Lock()
			calls := tokenCalls
			mu.Unlock()
			var failure *authFailure
			if payload != nil || !errors.As(err, &failure) || failure.permanent != test.permanent || failure.retryAfter != 2*time.Second || calls != 1 {
				t.Fatalf("diagnostic changed classification or native attempt count: %v", err)
			}
			want := fmt.Sprintf("Agently OOB authorization failed (token exchange, HTTP %d) [", test.status)
			if test.wantCode != "" {
				want += "oauth_error=" + test.wantCode + " "
			}
			want += "configured_auth_style=in_params]"
			if err.Error() != want || strings.Contains(fmt.Sprintf("%v %+v %#v", err, err, err), "secret-marker") {
				t.Fatalf("unsafe or incomplete diagnostic: %v", err)
			}
		})
	}
}

func TestOOBTokenExchangeConfiguredStyleAndNativeFallback(t *testing.T) {
	for _, test := range []struct {
		name, wantCode, wantStyle string
		style                     oauth2.AuthStyle
		acceptParams, secondGrant bool
		wantCalls                 int
		wantSuccess               bool
	}{
		{"auto_last_error", "invalid_grant", "auto_detect", oauth2.AuthStyleAutoDetect, false, true, 2, false},
		{"auto_params_success", "", "auto_detect", oauth2.AuthStyleAutoDetect, true, false, 2, true},
		{"params_success", "", "in_params", oauth2.AuthStyleInParams, true, false, 1, true},
		{"header_failure", "invalid_client", "in_header", oauth2.AuthStyleInHeader, false, true, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var mu sync.Mutex
			var challenge, firstVerifier, firstCode string
			var calls int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/authorize" {
					mu.Lock()
					challenge = r.URL.Query().Get("code_challenge")
					mu.Unlock()
					http.Redirect(w, r, "https://localhost/callback.html?code=secret-marker-code&state="+url.QueryEscape(r.URL.Query().Get("state")), http.StatusFound)
					return
				}
				if r.URL.Path != "/token" {
					t.Error("unexpected endpoint")
					w.WriteHeader(http.StatusNotFound)
					return
				}
				if err := r.ParseForm(); err != nil {
					t.Error("invalid synthetic form")
				}
				code, verifier := r.Form.Get("code"), r.Form.Get("code_verifier")
				mu.Lock()
				calls++
				call := calls
				if calls == 1 {
					firstCode, firstVerifier = code, verifier
				}
				if code != firstCode || verifier != firstVerifier || flow.GenerateCodeChallenge(verifier) != challenge {
					t.Error("native fallback changed its local grant or PKCE")
				}
				mu.Unlock()
				_, _, header := r.BasicAuth()
				w.Header().Set("Content-Type", "application/json")
				if !header && test.acceptParams {
					_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "secret-marker-token", "token_type": "Bearer"})
					return
				}
				code = "invalid_client"
				if call > 1 && test.secondGrant {
					code = "invalid_grant"
				}
				w.WriteHeader(http.StatusUnauthorized)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "error_description": "secret-marker-description"})
			}))
			defer server.Close()
			payload, err := authorizeOOB(context.Background(), diagnosticOAuthConfig(server.URL, test.style), &cred.Basic{Username: "secret-marker-user", Password: "secret-marker-password"}, diagnosticIDPClient())
			mu.Lock()
			attempts := calls
			mu.Unlock()
			if attempts != test.wantCalls || (payload != nil && err == nil) != test.wantSuccess {
				t.Fatalf("native style selection changed: attempts=%d err=%v", attempts, err)
			}
			if !test.wantSuccess && (err == nil || !strings.Contains(err.Error(), "oauth_error="+test.wantCode) || !strings.Contains(err.Error(), "configured_auth_style="+test.wantStyle) || strings.Contains(fmt.Sprintf("%v %#v", err, err), "secret-marker")) {
				t.Fatalf("configured style or last native error missing: %v", err)
			}
		})
	}
}

func TestOOBParallelAttemptsKeepPKCEAndStateIndependent(t *testing.T) {
	for _, style := range []oauth2.AuthStyle{oauth2.AuthStyleInParams, oauth2.AuthStyleInHeader, oauth2.AuthStyleAutoDetect} {
		for _, sameUser := range []bool{true, false} {
			t.Run(fmt.Sprintf("style_%d/same_user_%v", style, sameUser), func(t *testing.T) {
				type grant struct{ state, challenge string }
				var mu sync.Mutex
				grants := map[string]grant{}
				states, challenges := map[string]bool{}, map[string]bool{}
				users := map[string]int{}
				authorized := make(chan struct{}, 2)
				release := make(chan struct{})
				var calls int
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/authorize" {
						if err := r.ParseForm(); err != nil || r.PostForm.Get("password") != "secret-marker-password" {
							t.Error("parallel authorization credentials changed")
						}
						state, challenge := r.URL.Query().Get("state"), r.URL.Query().Get("code_challenge")
						mu.Lock()
						users[r.PostForm.Get("username")]++
						code := fmt.Sprintf("secret-marker-code-%d", len(grants)+1)
						if state == "" || challenge == "" || states[state] || challenges[challenge] {
							t.Error("parallel attempts shared their native state or PKCE")
						}
						states[state], challenges[challenge] = true, true
						grants[code] = grant{state, challenge}
						mu.Unlock()
						authorized <- struct{}{}
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
						http.Redirect(w, r, "https://localhost/callback.html?code="+code+"&state="+url.QueryEscape(state), http.StatusFound)
						return
					}
					if r.URL.Path != "/token" {
						t.Error("unexpected endpoint")
						w.WriteHeader(http.StatusNotFound)
						return
					}
					if err := r.ParseForm(); err != nil {
						t.Error("invalid synthetic form")
					}
					mu.Lock()
					g, found := grants[r.Form.Get("code")]
					calls++
					mu.Unlock()
					if !found || flow.GenerateCodeChallenge(r.Form.Get("code_verifier")) != g.challenge || r.Form.Get("redirect_uri") != "https://localhost/callback.html" {
						t.Error("parallel attempts exchanged another grant")
					}
					clientID, clientSecret, header := r.BasicAuth()
					if !header {
						clientID, clientSecret = r.Form.Get("client_id"), r.Form.Get("client_secret")
					}
					if clientID != "secret-marker-client" || clientSecret != "secret-marker-secret" {
						t.Error("parallel token client authentication changed")
					}
					w.Header().Set("Content-Type", "application/json")
					if style == oauth2.AuthStyleAutoDetect && header {
						w.WriteHeader(http.StatusUnauthorized)
						_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client", "error_description": "secret-marker-description"})
						return
					}
					if header != (style == oauth2.AuthStyleInHeader) {
						t.Error("parallel exchange used unexpected native client auth style")
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "secret-marker-token", "token_type": "Bearer"})
				}))
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				results := make(chan error, 2)
				config := diagnosticOAuthConfig(server.URL, style)
				for n := range 2 {
					go func() {
						user := "secret-marker-user"
						if !sameUser {
							user += fmt.Sprint(n)
						}
						copy := *config // Same clone boundary as the existing session login.
						_, err := authorizeOOB(ctx, &copy, &cred.Basic{Username: user, Password: "secret-marker-password"}, diagnosticIDPClient())
						results <- err
					}()
				}
				for range 2 {
					select {
					case <-authorized:
					case <-ctx.Done():
						close(release)
						t.Fatal("both native authorization attempts did not arrive")
					}
				}
				close(release)
				for range 2 {
					if err := <-results; err != nil {
						t.Fatalf("parallel native exchange failed: %v", err)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				wantCalls, wantUsers := 2, 2
				if style == oauth2.AuthStyleAutoDetect {
					wantCalls = 4 // Each invocation owns its config and native fallback.
				}
				if sameUser {
					wantUsers = 1
				}
				if len(states) != 2 || len(challenges) != 2 || len(users) != wantUsers || calls != wantCalls {
					t.Fatal("parallel native exchanges lost independence")
				}
			})
		}
	}
}

func TestOOBTokenExchangeCancellationIsRedacted(t *testing.T) {
	tokenStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/authorize" {
			http.Redirect(w, r, "https://localhost/callback.html?code=secret-marker-code&state="+url.QueryEscape(r.URL.Query().Get("state")), http.StatusFound)
			return
		}
		if r.URL.Path == "/token" {
			if err := r.ParseForm(); err != nil {
				t.Error("invalid synthetic form")
			}
			close(tokenStarted)
			<-r.Context().Done()
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := authorizeOOB(ctx, diagnosticOAuthConfig(server.URL, oauth2.AuthStyleInParams), &cred.Basic{Username: "secret-marker-user", Password: "secret-marker-password"}, diagnosticIDPClient())
		result <- err
	}()
	select {
	case <-tokenStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("native token exchange did not start")
	}
	cancel()
	select {
	case err := <-result:
		var failure *authFailure
		if !errors.As(err, &failure) || failure.permanent || !strings.Contains(err.Error(), "HTTP status unavailable") || !strings.Contains(err.Error(), "configured_auth_style=in_params") || strings.Contains(fmt.Sprintf("%v %#v", err, err), "secret-marker") {
			t.Fatalf("cancellation changed auth policy or leaked diagnostics: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native token exchange ignored cancellation")
	}
}

func TestOOBUnknownConfiguredStyleIsBounded(t *testing.T) {
	err := oobTokenExchangeFailure(errors.New("secret-marker-transport"), oauth2.AuthStyle(99))
	if !strings.Contains(err.Error(), "configured_auth_style=other") || strings.Contains(fmt.Sprintf("%v %#v", err, err), "secret-marker") {
		t.Fatalf("unknown configured style or transport failure leaked: %v", err)
	}
}

func diagnosticOAuthConfig(base string, style oauth2.AuthStyle) *oauth2.Config {
	return &oauth2.Config{ClientID: "secret-marker-client", ClientSecret: "secret-marker-secret", Endpoint: oauth2.Endpoint{AuthURL: base + "/authorize", TokenURL: base + "/token", AuthStyle: style}}
}

func diagnosticIDPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
