package agently

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/scy"
)

// missingSession is distinct from an identity mismatch: a missing/expired
// cookie can be replaced, whereas a verified wrong identity must be blocked.
type missingSession struct{}

func (*missingSession) Error() string { return "Viant Steward session is unavailable" }
func (*missingSession) Unwrap() error { return gateway.ErrBackendAuth }

// Only redacted diagnostics leave the authentication boundary. candidate is a
// successfully created cookie session whose protected metadata probe failed.
type authFailure struct {
	message    string
	permanent  bool
	retryAfter time.Duration
	candidate  *Factory
}

func (e *authFailure) Error() string    { return e.message }
func (e *authFailure) String() string   { return e.message }
func (e *authFailure) GoString() string { return e.message }

func transientAPI(e *APIError) bool {
	return e.Status == 0 && !e.Permanent || e.Status >= 500 || e.Status == 408 || e.Status == 429
}

func loginFailure(message string, cause error, candidate *Factory) error {
	failure := &authFailure{message: message, permanent: true}
	var auth *authFailure
	var api *APIError
	if errors.As(cause, &auth) {
		failure.permanent, failure.retryAfter = auth.permanent, auth.retryAfter
	} else if errors.As(cause, &api) {
		failure.permanent = !(transientAPI(api) || api.Status == http.StatusUnauthorized && !api.Permanent)
		failure.retryAfter = api.RetryAfter
	}
	if !failure.permanent {
		failure.candidate = candidate
	}
	return failure
}

func retryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(value); err == nil {
		return max(0, deadline.Sub(now))
	}
	return 0
}

// ValidateOOBReference checks the configured credential resource before Slack
// starts. Agently and IdP availability are handled by Manager afterward.
func ValidateOOBReference(ctx context.Context, ref *scy.Resource) error {
	secret, err := loadBasicSecret(ctx, ref)
	if err != nil || secret == nil || secret.Username == "" || secret.Password == "" {
		return &authFailure{message: "cannot load Viant Steward OOB credentials", permanent: true}
	}
	return nil
}

// Manager shares one cookie session and serializes recovery for every worker.
// It never retries a conversation/query mutation. Only read-only verification
// and a new authentication flow can be retried here.
type Manager struct {
	mu                                 sync.RWMutex
	state                              gateway.BackendState
	generation                         uint64
	current                            gateway.AgentlyClientFactory
	notBefore                          time.Time
	login                              func(context.Context) (gateway.AgentlyClientFactory, error)
	onUnavailable                      func(context.Context, gateway.BackendState) error
	log                                *slog.Logger
	wake                               chan struct{}
	fatal                              chan error
	running                            atomic.Bool
	retryBase, retryMax, probeInterval time.Duration
}

var _ gateway.BackendLifecycle = (*Manager)(nil)

func NewManager(login func(context.Context) (gateway.AgentlyClientFactory, error), onUnavailable func(context.Context, gateway.BackendState) error, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{state: gateway.BackendStarting, login: login, onUnavailable: onUnavailable, log: log,
		wake: make(chan struct{}, 1), fatal: make(chan error, 1), retryBase: time.Second, retryMax: 30 * time.Second, probeInterval: 30 * time.Second}
}

func (m *Manager) State() gateway.BackendState { m.mu.RLock(); defer m.mu.RUnlock(); return m.state }

// WithReady serializes only local durable admission with an outage transition.
// The callback must not perform HTTP or call back into Manager.
func (m *Manager) WithReady(fn func() error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != gateway.BackendReady {
		return gateway.ErrBackendUnavailable
	}
	return fn()
}

func (m *Manager) valid(generation uint64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != gateway.BackendReady || m.generation != generation {
		return gateway.ErrBackendUnavailable
	}
	return nil
}

func (m *Manager) ForPrincipal(ctx context.Context, principal string) (gateway.AgentlyClient, error) {
	m.mu.RLock()
	factory, generation, state := m.current, m.generation, m.state
	m.mu.RUnlock()
	if state != gateway.BackendReady || factory == nil {
		return nil, gateway.ErrBackendUnavailable
	}
	client, err := factory.ForPrincipal(ctx, principal)
	if err != nil {
		m.failed(generation, err)
		if !errors.Is(err, gateway.ErrBackendAuth) && m.valid(generation) != nil {
			return nil, gateway.ErrBackendUnavailable
		}
		return nil, err
	}
	return &managedClient{manager: m, generation: generation, inner: client}, nil
}

// The exclusive lock also prevents new local admissions until their terminal
// unavailable state has been persisted. No network request is made here.
func (m *Manager) unavailableLocked(ctx context.Context, state gateway.BackendState) error {
	m.state, m.generation = state, m.generation+1
	m.log.Info("agently_backend", "state", state)
	if m.onUnavailable != nil {
		return m.onUnavailable(ctx, state)
	}
	return nil
}

func (m *Manager) failed(generation uint64, err error) {
	if err == nil || errors.Is(err, gateway.ErrBackendUnavailable) {
		return
	}
	state := gateway.BackendRecovering
	var missing *missingSession
	var api *APIError
	var auth *authFailure
	switch {
	case errors.As(err, &missing):
	case errors.As(err, &auth):
		if auth.permanent {
			state = gateway.BackendBlocked
		}
	case errors.As(err, &api):
		if api.Status != http.StatusUnauthorized && !transientAPI(api) {
			return
		}
	case errors.Is(err, gateway.ErrBackendAuth):
		state = gateway.BackendBlocked
	default:
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if generation != m.generation || m.state != gateway.BackendReady {
		return
	}
	var cooldown time.Duration
	if api != nil {
		cooldown = api.RetryAfter
	}
	if auth != nil {
		cooldown = max(cooldown, auth.retryAfter)
	}
	m.notBefore = time.Now().Add(cooldown)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.unavailableLocked(ctx, state); err != nil {
		m.state = gateway.BackendBlocked
		select {
		case m.fatal <- err:
		default:
		}
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) ready(factory gateway.AgentlyClientFactory) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.generation++
	generation := m.generation
	if native, ok := factory.(*Factory); ok {
		// Never alter a factory already referenced by an in-flight worker.
		copy := *native
		httpCopy := *native.http
		httpCopy.Timeout = 120 * time.Second
		copy.http = &httpCopy
		if native.session != nil {
			// Verification succeeded for this publication. A late rejection from
			// an older worker must not poison the replacement generation.
			copy.session = &sessionIdentity{principal: native.session.principal, agent: native.session.agent}
		}
		copy.guard = func() error { return m.valid(generation) }
		factory = &copy
	}
	m.current, m.state, m.notBefore = factory, gateway.BackendReady, time.Time{}
	m.log.Info("agently_backend", "state", gateway.BackendReady)
}

func (m *Manager) snapshot() (gateway.AgentlyClientFactory, uint64, gateway.BackendState) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current, m.generation, m.state
}

func (m *Manager) verifyOrLogin(ctx context.Context) (gateway.AgentlyClientFactory, error) {
	current, _, _ := m.snapshot()
	if verifier, ok := current.(interface{ Verify(context.Context) error }); ok {
		err := verifier.Verify(ctx)
		if err == nil {
			return current, nil
		}
		var missing *missingSession
		if !errors.As(err, &missing) {
			var candidate *Factory
			if native, ok := current.(*Factory); ok {
				candidate = native
			}
			return nil, loginFailure("Viant Steward session verification failed", err, candidate)
		}
	}
	if m.login == nil {
		return nil, &authFailure{message: "Viant Steward authentication is not configured", permanent: true}
	}
	factory, err := m.login(ctx)
	if err == nil && factory == nil {
		err = &authFailure{message: "Viant Steward authentication returned no session", permanent: true}
	}
	return factory, err
}

// Run is owned by Runtime and must have exactly one caller. Ready probes reuse
// Core's session/token refresh implementation; a new OOB login is performed only
// when /auth/me confirms that the old cookie session no longer exists.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return errors.New("Viant Steward recovery is already running")
	}
	m.mu.Lock()
	err := m.unavailableLocked(ctx, gateway.BackendStarting)
	m.mu.Unlock()
	if err != nil {
		return err
	}
	delay := m.retryBase
	for ctx.Err() == nil {
		current, generation, state := m.snapshot()
		if state == gateway.BackendReady || state == gateway.BackendBlocked {
			var tick <-chan time.Time
			var timer *time.Timer
			if state == gateway.BackendReady {
				timer = time.NewTimer(m.probeInterval)
				tick = timer.C
			}
			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return nil
			case err := <-m.fatal:
				if timer != nil {
					timer.Stop()
				}
				return err
			case <-m.wake:
				if timer != nil {
					timer.Stop()
				}
			case <-tick:
				if verifier, ok := current.(interface{ Verify(context.Context) error }); ok {
					probe, cancel := context.WithTimeout(ctx, 90*time.Second)
					err := verifier.Verify(probe)
					cancel()
					if ctx.Err() == nil && err != nil {
						var missing *missingSession
						if !errors.As(err, &missing) {
							err = loginFailure("Viant Steward session verification failed", err, nil)
						}
						m.failed(generation, err)
					}
				}
			}
			continue
		}
		m.mu.RLock()
		cooldown := time.Until(m.notBefore)
		m.mu.RUnlock()
		if cooldown > 0 {
			timer := time.NewTimer(cooldown)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case err := <-m.fatal:
				timer.Stop()
				return err
			case <-timer.C:
			}
		}
		attempt, cancel := context.WithTimeout(ctx, 90*time.Second)
		factory, err := m.verifyOrLogin(attempt)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			m.ready(factory)
			delay = m.retryBase
			continue
		}
		var failure *authFailure
		if !errors.As(err, &failure) {
			err = loginFailure("Viant Steward authentication failed", err, nil)
			_ = errors.As(err, &failure)
		}
		m.mu.Lock()
		if failure.candidate != nil {
			m.current = failure.candidate
		}
		if failure.permanent {
			err = m.unavailableLocked(ctx, gateway.BackendBlocked)
		} else {
			err = nil
		}
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if failure.permanent {
			m.log.Error("agently_backend_blocked", "reason", failure.Error())
			continue
		}
		// Jitter only the exponential interval; never shorten Retry-After.
		wait := max(time.Duration(float64(delay)*(0.8+rand.Float64()*0.4)), failure.retryAfter)
		m.log.Info("agently_backend_retry", "state", m.State(), "retry_after_ms", wait.Milliseconds(), "reason", failure.Error())
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case err := <-m.fatal:
			timer.Stop()
			return err
		case <-timer.C:
		}
		delay = min(delay*2, m.retryMax)
	}
	return nil
}

type managedClient struct {
	manager    *Manager
	generation uint64
	inner      gateway.AgentlyClient
}

func (c *managedClient) result(ctx context.Context, err error) error {
	if err == nil || ctx.Err() != nil {
		return err
	}
	c.manager.failed(c.generation, err)
	// A typed 401 may also satisfy the protocol sentinel. Recovery owns that
	// response now; workers must reconcile a started run rather than reject it.
	if !errors.Is(err, gateway.ErrBackendAuth) && c.manager.valid(c.generation) != nil {
		return gateway.ErrBackendUnavailable
	}
	return err
}

func (c *managedClient) CreateConversation(ctx context.Context, agent string) (string, error) {
	if err := c.manager.valid(c.generation); err != nil {
		return "", err
	}
	id, err := c.inner.CreateConversation(ctx, agent)
	return id, c.result(ctx, err)
}

func (c *managedClient) Query(ctx context.Context, query gateway.Query, observe func(gateway.Turn) error) (gateway.Turn, error) {
	if err := c.manager.valid(c.generation); err != nil {
		return gateway.Turn{}, err
	}
	turn, err := c.inner.Query(ctx, query, observe)
	return turn, c.result(ctx, err)
}

func (c *managedClient) ReadTurn(ctx context.Context, conversation, turnID string, observe func(gateway.Turn) error) (gateway.Turn, error) {
	if err := c.manager.valid(c.generation); err != nil {
		return gateway.Turn{}, err
	}
	turn, err := c.inner.ReadTurn(ctx, conversation, turnID, observe)
	return turn, c.result(ctx, err)
}
