package agently

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func testManager(t *testing.T, f *loginFixture, unavailable func(context.Context, gateway.BackendState) error) *Manager {
	t.Helper()
	m := NewManager(func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
		return LoginOOB(ctx, f.server.URL, "agently_scheduler", "steward", f.secretRef)
	}, unavailable, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.retryBase, m.retryMax, m.probeInterval = 5*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			storetest.Must(t, err)
		case <-time.After(2 * time.Second):
			t.Error("Manager did not stop")
		}
	})
	return m
}

func waitBackend(t *testing.T, m *Manager, want gateway.BackendState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if m.State() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("backend state = %s, want %s", m.State(), want)
}

func managerClient(t *testing.T, m *Manager) gateway.AgentlyClient {
	t.Helper()
	client, err := m.ForPrincipal(context.Background(), "agently_scheduler")
	storetest.Must(t, err)
	return client
}

func TestManagerStartupTransientAndRetryAfter(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("discovery_unavailable")
	var transitions atomic.Int32
	m := testManager(t, f, func(context.Context, gateway.BackendState) error { transitions.Add(1); return nil })
	deadline := time.Now().Add(time.Second)
	for f.count("/v1/api/auth/oauth/config") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.State() != gateway.BackendStarting {
		t.Fatal("startup outage was fatal")
	}
	if _, err := m.ForPrincipal(context.Background(), "agently_scheduler"); !errors.Is(err, gateway.ErrBackendUnavailable) {
		t.Fatal("startup accepted execution")
	}
	f.mode.Store("")
	time.Sleep(40 * time.Millisecond)
	if f.count("/v1/api/auth/oauth/config") != 1 {
		t.Fatal("Retry-After was shortened")
	}
	waitBackend(t, m, gateway.BackendReady)
	if f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || transitions.Load() != 1 {
		t.Fatal("authentication was duplicated or outage persistence skipped")
	}
}

func TestManagerReusesSessionAndRejectsStaleWorkers(t *testing.T) {
	f := newLoginFixture(t)
	var recovering atomic.Int32
	m := testManager(t, f, func(_ context.Context, state gateway.BackendState) error {
		if state == gateway.BackendRecovering {
			recovering.Add(1)
		}
		return nil
	})
	waitBackend(t, m, gateway.BackendReady)
	old := managerClient(t, m)
	oldGeneration := old.(*managedClient).generation
	f.mode.Store("metadata_unavailable")
	waitBackend(t, m, gateway.BackendRecovering)
	_, err := old.CreateConversation(context.Background(), "steward")
	if !errors.Is(err, gateway.ErrBackendUnavailable) || f.count("/v1/conversations") != 0 {
		t.Fatal("old worker sent a mutation during outage")
	}
	f.mode.Store("")
	waitBackend(t, m, gateway.BackendReady)
	if f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 || recovering.Load() != 1 {
		t.Fatal("valid cookie was replaced instead of verified")
	}
	_, err = old.CreateConversation(context.Background(), "steward")
	if !errors.Is(err, gateway.ErrBackendUnavailable) {
		t.Fatal("old generation revived after recovery")
	}
	m.failed(oldGeneration, &APIError{Status: 503})
	if m.State() != gateway.BackendReady {
		t.Fatal("a late failure poisoned the replacement generation")
	}
	_, err = managerClient(t, m).CreateConversation(context.Background(), "steward")
	storetest.Must(t, err)
	if f.count("/v1/conversations") != 1 {
		t.Fatal("conversation was replayed")
	}
}

func TestManagerConfirmedMissingSessionTriggersOneLogin(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	old := managerClient(t, m)
	current, _, _ := m.snapshot()
	native := current.(*Factory)
	u, _ := url.Parse(f.server.URL)
	native.http.Jar.SetCookies(u, []*http.Cookie{{Name: "agently_session", Path: "/", MaxAge: -1}})
	_, err := old.CreateConversation(context.Background(), "steward")
	var missing *missingSession
	if !errors.As(err, &missing) || native.session.invalid.Load() {
		t.Fatal("missing session was treated as an identity mismatch")
	}
	waitBackend(t, m, gateway.BackendReady)
	if f.count("/authorize") != 2 || f.count("/v1/api/auth/session") != 2 || f.count("/v1/conversations") != 0 {
		t.Fatal("relogin duplicated a prompt or authentication")
	}
}

func TestManagerLateIdentityRejectionCannotPoisonRecoveredGeneration(t *testing.T) {
	f := newLoginFixture(t)
	factory, err := f.login()
	storetest.Must(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var block atomic.Bool
	factory.http.Transport = sessionRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		response, err := http.DefaultTransport.RoundTrip(request)
		if request.URL.Path == "/v1/api/auth/me" && block.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		return response, err
	})
	m := NewManager(nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.ready(factory)
	old := managerClient(t, m).(*managedClient)
	f.mode.Store("wrong_subject")
	block.Store(true)
	done := make(chan error, 1)
	go func() { _, err := old.CreateConversation(context.Background(), "steward"); done <- err }()
	<-entered
	m.failed(old.generation, &APIError{Status: 503})
	f.mode.Store("")
	storetest.Must(t, factory.Verify(context.Background()))
	m.ready(factory)
	close(release)
	if !errors.Is(<-done, gateway.ErrBackendAuth) || m.State() != gateway.BackendReady {
		t.Fatal("late identity rejection changed recovered backend state")
	}
	_, err = managerClient(t, m).CreateConversation(context.Background(), "steward")
	storetest.Must(t, err)
	if f.count("/v1/conversations") != 1 {
		t.Fatal("late worker mutated backend or poisoned replacement identity")
	}
}

func TestManagerProtected401UsesNativeRefreshCooldown(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("metadata_unauthorized")
	factory, err := f.login()
	var failure *authFailure
	if factory != nil || !errors.As(err, &failure) || failure.permanent || failure.candidate == nil || failure.retryAfter < 30*time.Second {
		t.Fatalf("protected 401 lost candidate/cooldown: %v", err)
	}
	if failure.candidate.session.invalid.Load() {
		t.Fatal("valid /auth/me cookie poisoned by protected 401")
	}
	// Use the actual LoginOOB path again to prove the manager retains the
	// newly created session rather than retrying credentials during cooldown.
	m := testManager(t, f, nil)
	deadline := time.Now().Add(time.Second)
	for f.count("/v1/api/auth/session") < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	f.mode.Store("")
	time.Sleep(50 * time.Millisecond)
	if m.State() != gateway.BackendStarting || f.count("/authorize") != 2 || f.count("/v1/workspace/metadata") != 2 {
		t.Fatal("native refresh cooldown caused an OOB loop or premature probe")
	}
}

func TestManagerStartupMetadataFailureRetainsNewCookie(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("metadata_unavailable")
	m := testManager(t, f, nil)
	deadline := time.Now().Add(time.Second)
	for f.count("/v1/workspace/metadata") == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	f.mode.Store("")
	waitBackend(t, m, gateway.BackendReady)
	if f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 {
		t.Fatal("post-login transient metadata failure repeated OOB credentials")
	}
}

func TestManagerReadyProtected401PreservesSessionAndCooldown(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	f.mode.Store("metadata_unauthorized")
	waitBackend(t, m, gateway.BackendRecovering)
	m.mu.RLock()
	cooldown := time.Until(m.notBefore)
	m.mu.RUnlock()
	if cooldown < 29*time.Second {
		t.Fatal("periodic protected 401 lost native refresh cooldown")
	}
	probes := f.count("/v1/workspace/metadata")
	f.mode.Store("")
	time.Sleep(50 * time.Millisecond)
	if m.State() != gateway.BackendRecovering || f.count("/authorize") != 1 || f.count("/v1/workspace/metadata") != probes {
		t.Fatal("periodic protected 401 blocked the backend or retried before cooldown")
	}
	deadline := time.Now().Add(32 * time.Second)
	for m.State() != gateway.BackendReady && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if m.State() != gateway.BackendReady || f.count("/authorize") != 1 || f.count("/v1/api/auth/session") != 1 {
		t.Fatal("periodic protected 401 did not recover using the existing cookie")
	}
}

func TestManagerQuery403IsLocalAndMetadata403Blocks(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	f.mode.Store("run_forbidden")
	_, err := managerClient(t, m).Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "r", AgentID: "steward", Text: "question"}, nil)
	if !errors.Is(err, gateway.ErrBackendProtocol) || errors.Is(err, gateway.ErrBackendAuth) || m.State() != gateway.BackendReady {
		t.Fatal("request-specific 403 invalidated shared identity")
	}
	f.mode.Store("metadata_forbidden")
	waitBackend(t, m, gateway.BackendBlocked)
	if f.count("/authorize") != 1 {
		t.Fatal("permission failure triggered relogin")
	}
}

func TestManagerUncertainQueryIsNeverReplayed(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	f.mode.Store("lost_post")
	_, err := managerClient(t, m).Query(context.Background(), gateway.Query{ConversationID: "c", MessageID: "r", AgentID: "steward", Text: "question"}, nil)
	if !errors.Is(err, gateway.ErrBackendUnavailable) {
		t.Fatalf("transient query error did not enter recovery: %v", err)
	}
	f.mode.Store("")
	waitBackend(t, m, gateway.BackendReady)
	turn, err := managerClient(t, m).ReadTurn(context.Background(), "c", "r", nil)
	storetest.Must(t, err)
	if turn.Content != "verified answer" || f.count("agui.chat") != 1 || f.count("agui.run.attach") != 1 || f.count("/authorize") != 1 {
		t.Fatal("query was replayed instead of attached")
	}
}

func TestManagerBadCredentialsBlockWithoutLoop(t *testing.T) {
	f := newLoginFixture(t)
	f.mode.Store("bad_password")
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendBlocked)
	f.mode.Store("")
	time.Sleep(50 * time.Millisecond)
	if m.State() != gateway.BackendBlocked || f.count("/authorize") != 1 {
		t.Fatal("bad password was retried indefinitely")
	}
}

func TestManagerIdentityMismatchBlocksWithoutLogin(t *testing.T) {
	f := newLoginFixture(t)
	m := testManager(t, f, nil)
	waitBackend(t, m, gateway.BackendReady)
	f.mode.Store("wrong_subject")
	_, err := managerClient(t, m).CreateConversation(context.Background(), "steward")
	if !errors.Is(err, gateway.ErrBackendAuth) {
		t.Fatal("wrong subject was accepted")
	}
	waitBackend(t, m, gateway.BackendBlocked)
	if f.count("/authorize") != 1 || f.count("/v1/conversations") != 0 {
		t.Fatal("identity failure retried credentials or mutated backend")
	}
}

func TestManagerPersistsUnavailableBeforeReady(t *testing.T) {
	f := newLoginFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	m := testManager(t, f, func(_ context.Context, state gateway.BackendState) error {
		if state == gateway.BackendRecovering {
			close(entered)
			<-release
		}
		return nil
	})
	waitBackend(t, m, gateway.BackendReady)
	old := managerClient(t, m).(*managedClient)
	f.mode.Store("metadata_unavailable")
	finished := make(chan struct{})
	go func() { m.failed(old.generation, &APIError{Status: 503}); close(finished) }()
	<-entered
	var admitted atomic.Bool
	checked := make(chan struct{})
	go func() { _ = m.WithReady(func() error { admitted.Store(true); return nil }); close(checked) }()
	select {
	case <-checked:
		t.Fatal("admission passed an unfinished durable outage transition")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-finished
	<-checked
	if admitted.Load() {
		t.Fatal("a new request was admitted before recovery")
	}
	f.mode.Store("")
	waitBackend(t, m, gateway.BackendReady)
}

func TestManagerUnavailablePersistenceFailureIsFatal(t *testing.T) {
	want := errors.New("durability failed")
	m := NewManager(nil, func(context.Context, gateway.BackendState) error { return want }, slog.Default())
	if err := m.Run(context.Background()); !errors.Is(err, want) {
		t.Fatal("outage persistence failure ignored")
	}
}

func TestOOBClassifiesFailuresAndValidatesLocalSecrets(t *testing.T) {
	for _, mode := range []string{"discovery_unavailable", "idp_unavailable", "bad_password", "wrong_state", "token_error", "metadata_unavailable", "metadata_unauthorized"} {
		t.Run(mode, func(t *testing.T) {
			f := newLoginFixture(t)
			storetest.Must(t, ValidateOOBReference(context.Background(), f.secretRef))
			f.mode.Store(mode)
			_, err := f.login()
			var failure *authFailure
			if !errors.As(err, &failure) || strings.Contains(fmt.Sprintf("%+v %#v", err, err), "secret-marker") {
				t.Fatal("unclassified or unredacted authentication failure")
			}
			permanent := mode == "bad_password" || mode == "wrong_state" || mode == "token_error"
			if failure.permanent != permanent {
				t.Fatalf("permanent = %v, want %v", failure.permanent, permanent)
			}
			if (mode == "discovery_unavailable" || mode == "idp_unavailable") && failure.retryAfter != time.Second {
				t.Fatal("Retry-After lost")
			}
		})
	}
	if err := ValidateOOBReference(context.Background(), &scy.Resource{URL: "https://secret-marker.invalid/a"}); err == nil || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("unsafe static secret reference validation")
	}
	if retryAfter("2", time.Now()) != 2*time.Second || retryAfter("garbage", time.Now()) != 0 || retryAfter("-1", time.Now()) != 0 {
		t.Fatal("invalid Retry-After parsing")
	}
	now := time.Now().UTC().Truncate(time.Second)
	if retryAfter(now.Add(3*time.Second).Format(http.TimeFormat), now) != 3*time.Second {
		t.Fatal("HTTP date Retry-After ignored")
	}
}

// Generic factories are supported for the injectable runtime/CLI boundary;
// no concrete native client assertion is required for recovery or admission.
type genericFactory struct{ client gateway.AgentlyClient }

func (f genericFactory) ForPrincipal(context.Context, string) (gateway.AgentlyClient, error) {
	return f.client, nil
}

type failingClient struct{}

func (failingClient) CreateConversation(context.Context, string) (string, error) {
	return "", &APIError{Status: 503}
}
func (failingClient) Query(context.Context, gateway.Query, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, &APIError{Status: 503}
}
func (failingClient) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, &APIError{Status: 503}
}

func TestManagerConcurrentFailuresHaveSingleSharedRecovery(t *testing.T) {
	var calls, active, peak atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := NewManager(func(ctx context.Context) (gateway.AgentlyClientFactory, error) {
		calls.Add(1)
		n := active.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		defer active.Add(-1)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
		return genericFactory{client: failingClient{}}, nil
	}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.probeInterval = time.Hour
	done := make(chan error, 1)
	go func() { done <- m.Run(ctx) }()
	waitBackend(t, m, gateway.BackendReady)
	clients := make([]gateway.AgentlyClient, 20)
	for i := range clients {
		clients[i] = managerClient(t, m)
	}
	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Go(func() { _, _ = client.CreateConversation(ctx, "steward") })
	}
	wg.Wait()
	waitBackend(t, m, gateway.BackendReady)
	if peak.Load() != 1 || calls.Load() != 2 {
		t.Fatalf("shared authentication calls=%d peak=%d", calls.Load(), peak.Load())
	}
	cancel()
	storetest.Must(t, <-done)
}
