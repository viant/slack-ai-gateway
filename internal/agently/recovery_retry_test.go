package agently

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestManagerToken401FastFirstRetryAndOutageReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		var attempts []time.Time
		failure := oobResponseFailure("token exchange", &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{}})
		m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
			attempts = append(attempts, time.Now())
			if calls.Add(1) <= 2 {
				return nil, failure
			}
			return &readinessFactory{}, nil
		}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		m.probeInterval = time.Hour
		stop := startReadinessManager(t, m)
		defer stop()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatal("token exchange was retried before three seconds")
		}
		time.Sleep(6 * time.Second)
		synctest.Wait()
		if calls.Load() != 2 || m.State() != gateway.BackendStarting {
			t.Fatal("first token exchange retry did not start within seven seconds")
		}
		firstDelay := attempts[1].Sub(attempts[0])
		if firstDelay < 3*time.Second || firstDelay > 7*time.Second {
			t.Fatalf("first retry = %s, want 3–7 seconds", firstDelay)
		}
		time.Sleep(time.Until(attempts[1].Add(59 * time.Second)))
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatal("subsequent rejection skipped the normal cooldown")
		}
		time.Sleep(17 * time.Second)
		synctest.Wait()
		if calls.Load() != 3 || m.State() != gateway.BackendReady {
			t.Fatal("subsequent token exchange retry did not restore readiness")
		}
		secondDelay := attempts[2].Sub(attempts[1])
		if secondDelay < time.Minute || secondDelay > 75*time.Second {
			t.Fatalf("subsequent retry = %s, want 60–75 seconds", secondDelay)
		}
		_, generation, _ := m.snapshot()
		m.failed(generation, failure)
		synctest.Wait()
		m.mu.RLock()
		plan := m.retryPlan
		m.mu.RUnlock()
		if plan.attempt != 1 || plan.localDelay < 3*time.Second || plan.localDelay > 7*time.Second {
			t.Fatalf("verified readiness did not reset fast-first policy: %+v", plan)
		}
	})
}

func TestManagerToken401RetryCountersArePerIntegration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := oobResponseFailure("token exchange", &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{}})
		newManager := func() *Manager {
			return NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
				return nil, failure
			}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		}
		first := newManager()
		stopFirst := startReadinessManager(t, first)
		defer stopFirst()
		time.Sleep(8 * time.Second)
		synctest.Wait()
		second := newManager()
		stopSecond := startReadinessManager(t, second)
		defer stopSecond()
		first.mu.RLock()
		firstPlan := first.retryPlan
		first.mu.RUnlock()
		second.mu.RLock()
		secondPlan := second.retryPlan
		second.mu.RUnlock()
		if firstPlan.attempt != 2 || firstPlan.localDelay < time.Minute || firstPlan.localDelay > 75*time.Second {
			t.Fatalf("first integration lost its normal retry: %+v", firstPlan)
		}
		if secondPlan.attempt != 1 || secondPlan.localDelay < 3*time.Second || secondPlan.localDelay > 7*time.Second {
			t.Fatalf("second integration inherited another integration's retry count: %+v", secondPlan)
		}
	})
}

func TestManagerRetryDiagnosticsSeparatePolicyAndServerFloor(t *testing.T) {
	for _, tc := range []struct {
		name, header string
		present      bool
	}{{name: "absent"}, {name: "empty", present: true},
		{name: "short", header: "2", present: true}, {name: "long", header: "120", present: true},
		{name: "invalid", header: "invalid", present: true}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				headers := http.Header{}
				if tc.present {
					headers["Retry-After"] = []string{tc.header}
				}
				failure := oobResponseFailure("token exchange", &http.Response{StatusCode: http.StatusUnauthorized, Header: headers})
				m := NewManager(func(context.Context) (gateway.AgentlyClientFactory, error) {
					return nil, failure
				}, nil, slog.New(slog.NewJSONHandler(&logs, nil)))
				stop := startReadinessManager(t, m)
				defer stop()
				decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
				var retry map[string]any
				for decoder.More() {
					var record map[string]any
					storetest.Must(t, decoder.Decode(&record))
					if record["msg"] == "agently_backend_retry" {
						retry = record
					}
				}
				local, ok := retry["local_delay_ms"].(float64)
				if !ok || local < 3000 || local > 7000 || retry["attempt"] != float64(1) || retry["next_attempt"] != float64(2) {
					t.Fatalf("retry log lacks the actual local policy/count: %+v", retry)
				}
				server := float64(0)
				if tc.header == "2" {
					server = 2000
				} else if tc.header == "120" {
					server = 120000
				}
				if retry["server_retry_after_present"] != tc.present || retry["server_retry_after_ms"] != server || retry["retry_after_ms"] != max(local, server) || retry["retry_at"] == nil {
					t.Fatalf("retry log merged or shortened the server floor: %+v", retry)
				}
				wantSource := "local_policy"
				if server > local {
					wantSource = "server_retry_after"
				}
				if retry["delay_source"] != wantSource {
					t.Fatalf("delay source = %v, want %s", retry["delay_source"], wantSource)
				}
			})
		})
	}
}

func TestManagerProtectedCooldownIsNotReportedAsServerRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := NewManager(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		m.ready(&readinessFactory{})
		_, generation, _ := m.snapshot()
		failure := loginFailure("workspace_metadata", "protected metadata unavailable", &APIError{
			Status: http.StatusUnauthorized, localRetryFloor: 30 * time.Second,
		}, nil)
		m.failed(generation, failure)
		m.mu.RLock()
		plan := m.retryPlan
		m.mu.RUnlock()
		if plan.localDelay != 30*time.Second || plan.serverDelay != 0 || plan.wait != 30*time.Second || plan.source != "local_policy" {
			t.Fatalf("native local refresh floor was misreported as a server header: %+v", plan)
		}
	})
}
