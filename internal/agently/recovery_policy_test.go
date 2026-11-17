package agently

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"golang.org/x/oauth2"
)

type readinessFactory struct {
	verify func(context.Context) error
	client gateway.AgentlyClient
}

func (f *readinessFactory) Verify(ctx context.Context) error {
	if f.verify != nil {
		return f.verify(ctx)
	}
	return nil
}

func (f *readinessFactory) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return f.client, nil
}

// The fake covers only Manager scheduling. Native OAuth/session calls remain in
// the HTTP fixtures below and in the existing session/diagnostic contract tests.
func startReadinessManager(t *testing.T, m *Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	synctest.Wait()
	return func() {
		cancel()
		storetest.Must(t, <-done)
	}
}

func TestManagerRemoteRejectionsRecoverAfterBoundedDelay(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				var attemptsMu sync.Mutex
				var attempts []time.Time
				failure := oobResponseFailure("authorization endpoint", &http.Response{StatusCode: status, Header: http.Header{}})
				m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
					call := calls.Add(1)
					attemptsMu.Lock()
					attempts = append(attempts, time.Now())
					attemptsMu.Unlock()
					if call == 1 {
						return nil, failure
					}
					return &readinessFactory{}, nil
				}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				stop := startReadinessManager(t, m)
				defer stop()
				if calls.Load() != 1 || m.State() != gateway.BackendStarting {
					t.Fatalf("remote rejection blocked startup or retried immediately: calls=%d state=%s", calls.Load(), m.State())
				}
				time.Sleep(59 * time.Second)
				synctest.Wait()
				if calls.Load() != 1 {
					t.Fatal("authentication retried before the rejection cooldown")
				}
				time.Sleep(17 * time.Second)
				synctest.Wait()
				if calls.Load() != 2 || m.State() != gateway.BackendReady {
					t.Fatalf("remote repair did not restore readiness: calls=%d state=%s", calls.Load(), m.State())
				}
				attemptsMu.Lock()
				delay := attempts[1].Sub(attempts[0])
				attemptsMu.Unlock()
				if delay < time.Minute || delay > 75*time.Second {
					t.Fatalf("rejection retry delay = %s, want 60–75 seconds", delay)
				}
			})
		})
	}
}

func TestManagerRejectionRetryAfterAndCancellation(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(map[bool]string{false: "retry_after", true: "cancel"}[cancelEarly], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				failure := oobResponseFailure("token exchange", &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{"Retry-After": []string{"120"}}})
				m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
					if calls.Add(1) == 1 {
						return nil, failure
					}
					return &readinessFactory{}, nil
				}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				stop := startReadinessManager(t, m)
				time.Sleep(119 * time.Second)
				synctest.Wait()
				if calls.Load() != 1 {
					t.Fatal("Retry-After was shortened")
				}
				if cancelEarly {
					stop()
					time.Sleep(time.Hour)
					synctest.Wait()
					if calls.Load() != 1 {
						t.Fatal("cancelled authentication retried")
					}
					return
				}
				defer stop()
				time.Sleep(time.Second)
				synctest.Wait()
				if calls.Load() != 2 || m.State() != gateway.BackendReady {
					t.Fatal("authentication did not recover after Retry-After")
				}
			})
		})
	}
}

func TestManagerPeriodicRejectionLogsCauseAndReusesReadinessFactory(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		var denied atomic.Bool
		var logins, probes atomic.Int32
		factory := &readinessFactory{verify: func(context.Context) error {
			probes.Add(1)
			if denied.Load() {
				return loginFailure("workspace_metadata", "Agently metadata request failed", &APIError{Status: http.StatusForbidden, Permanent: true}, nil)
			}
			return nil
		}}
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			logins.Add(1)
			return factory, nil
		}, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
		stop := startReadinessManager(t, m)
		defer stop()
		denied.Store(true)
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if m.State() != gateway.BackendRecovering || logins.Load() != 1 || probes.Load() != 1 {
			t.Fatal("periodic permission failure was lost or triggered a new login")
		}
		denied.Store(false)
		time.Sleep(59 * time.Second)
		synctest.Wait()
		if m.State() != gateway.BackendRecovering || probes.Load() != 1 {
			t.Fatal("periodic rejection retried before the cooldown")
		}
		time.Sleep(17 * time.Second)
		synctest.Wait()
		if m.State() != gateway.BackendReady || logins.Load() != 1 || probes.Load() != 2 {
			t.Fatal("readiness recovery did not reuse the existing factory")
		}
		decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
		found := false
		for decoder.More() {
			var record map[string]any
			storetest.Must(t, decoder.Decode(&record))
			if record["msg"] != "agently_backend" || record["state"] != string(gateway.BackendRecovering) {
				continue
			}
			found = true
			if record["stage"] != "workspace_metadata" || record["category"] != "remote_authentication_rejected" || record["http_status"] != float64(http.StatusForbidden) || record["reason"] == "" || record["retry_at"] == nil {
				t.Fatalf("periodic rejection lacks safe cause: %v", record)
			}
			if delay, ok := record["retry_after_ms"].(float64); !ok || delay < 60000 || delay > 75000 {
				t.Fatalf("periodic rejection log has wrong retry delay: %v", record)
			}
		}
		if !found {
			t.Fatal("periodic readiness rejection was not logged")
		}
	})
}

func TestReadinessClassificationPreservesCookieAndRemoteRepair(t *testing.T) {
	for _, mode := range []string{"forbidden", "metadata_forbidden", "missing_agent", "missing_subject", "missing_provider"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			factory, err := f.login()
			storetest.Must(t, err)
			f.mode.Store(mode)
			err = factory.Verify(context.Background())
			var failure *authFailure
			if !errors.As(err, &failure) || failure.permanent || !failure.rejectionCooldown || failure.retryAfter != 0 || factory.session.invalid.Load() || errors.Is(err, gateway.ErrBackendAuth) {
				t.Fatalf("remote readiness rejection became a hard identity failure: %v", err)
			}
			manager := NewManager(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			manager.current = factory
			_, err = manager.verifyOrLogin(context.Background())
			if !errors.As(err, &failure) || failure.candidate != factory {
				t.Fatal("readiness failure lost the existing cookie candidate")
			}
			f.mode.Store("")
			recovered, err := manager.verifyOrLogin(context.Background())
			storetest.Must(t, err)
			if recovered != factory || f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 {
				t.Fatal("readiness repair replaced the cookie or submitted a question")
			}
		})
	}
}

func TestOOBRemoteConfigurationRepairDoesNotRequireRestart(t *testing.T) {
	for _, mode := range []string{"missing_resource", "malformed_resource"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			resource := f.configRef
			original, err := os.ReadFile(resource)
			storetest.Must(t, err)
			if mode == "missing_resource" {
				f.mu.Lock()
				f.configRef = ""
				f.mu.Unlock()
			} else {
				storetest.Must(t, os.WriteFile(resource, []byte("secret-marker-malformed-oauth"), 0600))
			}
			_, err = f.login()
			var failure *authFailure
			if !errors.As(err, &failure) || failure.permanent || !failure.rejectionCooldown || failure.retryAfter != 0 || strings.Contains(err.Error(), "secret-marker") {
				t.Fatalf("remote OAuth configuration failure was permanent or unsafe: %v", err)
			}
			if f.count("/authorize") != 0 || f.count("/v1/api/auth/session") != 0 {
				t.Fatal("invalid remote configuration reached authorization")
			}
			f.mu.Lock()
			f.configRef = resource
			f.mu.Unlock()
			storetest.Must(t, os.WriteFile(resource, original, 0600))
			_, err = f.login()
			storetest.Must(t, err)
			if f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || f.count("/v1/ag-ui/run") != 0 {
				t.Fatal("remote configuration repair did not allow one native login")
			}
		})
	}
}

func TestFreshSessionMissingCookieReceivesAuthenticationCooldown(t *testing.T) {
	for _, mode := range []string{"no_cookie", "revoked", "auth_me_unauthorized_retry"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			f.mode.Store(mode)
			_, err := f.login()
			var failure *authFailure
			if !errors.As(err, &failure) || failure.permanent {
				t.Fatalf("fresh session rejection would immediately repeat OOB credentials: %v", err)
			}
			if mode == "auth_me_unauthorized_retry" {
				if failure.retryAfter != 3*time.Minute || failure.candidate == nil || failure.candidate.session.invalid.Load() {
					t.Fatal("fresh session refusal lost Retry-After or poisoned its candidate")
				}
			} else if !failure.rejectionCooldown || failure.retryAfter != 0 {
				t.Fatalf("fresh session rejection lost its local cooldown classification: %+v", failure)
			}
			if f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || f.count("/v1/ag-ui/run") != 0 {
				t.Fatal("fresh session rejection retried inside the login call")
			}
		})
	}
}

func TestOOBRedirectRefusalPreservesRetryAfterAndStateGuard(t *testing.T) {
	for _, mode := range []string{"oauth_error", "missing_code", "error_wrong_state", "error_missing_state"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			f.authorizationRetryAfter.Store(180)
			f.mode.Store(mode)
			_, err := f.login()
			var failure *authFailure
			if !errors.As(err, &failure) {
				t.Fatalf("redirect failure was not classified: %v", err)
			}
			securityFailure := mode == "error_wrong_state" || mode == "error_missing_state"
			if failure.permanent != securityFailure || (!securityFailure && failure.retryAfter != 3*time.Minute) {
				t.Fatalf("redirect refusal lost Retry-After or state validation: permanent=%v retry=%s", failure.permanent, failure.retryAfter)
			}
			if f.count("/authorize") != 1 || f.count("/token") != 0 || f.count("/v1/api/auth/session") != 0 {
				t.Fatal("redirect refusal reached a token/session mutation")
			}
		})
	}
}

func TestManagerMissingSessionPreservesNativeRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logins, probes atomic.Int32
		factory := &readinessFactory{verify: func(context.Context) error {
			if probes.Add(1) == 1 {
				return &missingSession{status: http.StatusUnauthorized, retryAfter: 3 * time.Minute}
			}
			return nil
		}}
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			logins.Add(1)
			return factory, nil
		}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		stop := startReadinessManager(t, m)
		defer stop()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		m.mu.RLock()
		cooldown := time.Until(m.notBefore)
		m.mu.RUnlock()
		if m.State() != gateway.BackendRecovering || cooldown != 3*time.Minute {
			t.Fatal("periodic missing-session transition lost native Retry-After")
		}
		time.Sleep(179 * time.Second)
		synctest.Wait()
		if probes.Load() != 1 || logins.Load() != 1 {
			t.Fatal("missing session retried verification or credentials during Retry-After")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if m.State() != gateway.BackendReady || probes.Load() != 2 || logins.Load() != 1 {
			t.Fatal("readiness did not recover after missing-session Retry-After")
		}
	})
}

func TestManagerHardLocalFailureLogsAndDoesNotRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		var calls atomic.Int32
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			calls.Add(1)
			return nil, &authFailure{message: "cannot load Agently OOB credentials", stage: "local_configuration", category: "credentials_invalid", permanent: true}
		}, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
		stop := startReadinessManager(t, m)
		defer stop()
		time.Sleep(time.Hour)
		synctest.Wait()
		if calls.Load() != 1 || m.State() != gateway.BackendBlocked {
			t.Fatal("local credential failure entered a retry loop")
		}
		decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
		blocked := false
		for decoder.More() {
			var record map[string]any
			storetest.Must(t, decoder.Decode(&record))
			if record["msg"] == "agently_backend_blocked" {
				blocked = record["stage"] == "local_configuration" && record["category"] == "credentials_invalid" && record["reason"] != ""
			}
		}
		if !blocked {
			t.Fatal("hard block lacks a safe local failure reason")
		}
	})
}

func TestRemoteTokenFailureLogsOnlyBoundedNativeDiagnostics(t *testing.T) {
	var logs bytes.Buffer
	err := oobTokenExchangeFailure(&oauth2.RetrieveError{
		Response:         &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{"Retry-After": []string{"120"}}},
		ErrorCode:        "invalid_grant",
		ErrorDescription: "secret-marker-description",
		ErrorURI:         "https://secret-marker.invalid/error",
		Body:             []byte("secret-marker-token"),
	}, oauth2.AuthStyleAutoDetect)
	var failure *authFailure
	if !errors.As(err, &failure) || failure.permanent || failure.retryAfter != 2*time.Minute {
		t.Fatalf("native rejection lost retry policy: %v", err)
	}
	m := NewManager(nil, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
	m.ready(&readinessFactory{})
	_, generation, _ := m.snapshot()
	m.failed(generation, err)
	var record map[string]any
	decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	storetest.Must(t, decoder.Decode(&record)) // ready
	storetest.Must(t, decoder.Decode(&record)) // recovering
	if record["stage"] != "token exchange" || record["category"] != "remote_authentication_rejected" || record["oauth_error"] != "invalid_grant" || record["configured_auth_style"] != "auto_detect" || record["http_status"] != float64(http.StatusUnauthorized) {
		t.Fatalf("native token failure lost bounded diagnostic fields: %v", record)
	}
	if strings.Contains(logs.String(), "secret-marker") {
		t.Fatal("a native OAuth diagnostic leaked secret-bearing data")
	}
}

func TestManagersSameBackendIdentityRecoverIndependentlyNativeHTTP(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("bad_password")
	initial := make(chan error, 1)
	first := NewManager(func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
		factory, err := LoginOOB(ctx, f.server.URL, "agently_scheduler", "steward", f.secretRef)
		select {
		case initial <- err:
		default:
		}
		return factory, err
	}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- first.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		storetest.Must(t, <-done)
	})
	select {
	case err := <-initial:
		var failure *authFailure
		if !errors.As(err, &failure) || failure.permanent {
			t.Fatalf("initial native rejection was not recoverable: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("initial native login did not finish")
	}
	if f.count("/authorize") != 1 || first.State() != gateway.BackendStarting {
		t.Fatal("first remote refusal was not left recoverable")
	}
	f.mode.Store("")
	second := NewManager(func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
		return LoginOOB(ctx, f.server.URL, "agently_scheduler", "steward", f.secretRef)
	}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	second.probeInterval = time.Hour
	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Run(secondCtx) }()
	t.Cleanup(func() { secondCancel(); storetest.Must(t, <-secondDone) })
	waitBackend(t, second, gateway.BackendReady)
	_, secondGeneration, _ := second.snapshot()
	if first.State() != gateway.BackendStarting || f.count("/authorize") != 2 {
		t.Fatal("a second integration changed or retried the first integration")
	}
	deadline := time.Now().Add(77 * time.Second)
	for first.State() != gateway.BackendReady && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	_, generation, state := second.snapshot()
	if first.State() != gateway.BackendReady || state != gateway.BackendReady || generation != secondGeneration || f.count("/authorize") != 3 || f.count("/v1/api/auth/session") != 2 {
		t.Fatal("native authentication recovery did not leave both integrations independently ready")
	}
	firstFactory, _, _ := first.snapshot()
	secondFactory, _, _ := second.snapshot()
	storetest.Must(t, secondFactory.(*Factory).Verify(context.Background()))
	if firstFactory.(*Factory).http.Jar == secondFactory.(*Factory).http.Jar || f.count("/v1/conversations") != 0 || f.count("/v1/ag-ui/run") != 0 {
		t.Fatal("integrations shared a cookie jar or recovery submitted a question")
	}
}

func TestManagerFailedBackendTurnKeepsAuthenticatedConnectionReady(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	f.mode.Store("run_failed")
	turn, err := managerClient(t, m).Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "failed-request", AgentID: "steward", Text: "synthetic request"}, nil)
	storetest.Must(t, err)
	if turn.State != "failed" || m.State() != gateway.BackendReady || f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || f.count("agui.chat") != 1 {
		t.Fatalf("failed native turn changed session readiness or replayed input: state=%s turn=%s", m.State(), turn.State)
	}
}
