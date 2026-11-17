package agently

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// missingSession is distinct from an identity mismatch: a missing/expired
// cookie can be replaced, whereas a verified wrong identity must be blocked.
type missingSession struct {
	status            int
	retryAfter        time.Duration
	retryAfterPresent bool
}

func (*missingSession) Error() string { return "Viant Steward session is unavailable" }
func (*missingSession) Unwrap() error { return gateway.ErrBackendAuth }

// Only redacted diagnostics leave the authentication boundary. candidate is a
// successfully created cookie session whose protected metadata probe failed.
type authFailure struct {
	message   string
	permanent bool
	// retryAfter is only the parsed server Retry-After floor. Local retry
	// policy is selected by Manager, where the attempt history is available.
	retryAfter        time.Duration
	retryAfterPresent bool
	localRetryFloor   time.Duration
	rejectionCooldown bool
	candidate         *Factory
	stage             string
	category          string
	status            int
	oauthError        string
	authStyle         string
	identity          bool
}

func (e *authFailure) Error() string    { return e.message }
func (e *authFailure) String() string   { return e.message }
func (e *authFailure) GoString() string { return e.message }

// Only a verified identity rejection satisfies the authentication sentinel.
// Remote refusals are handled by Manager without poisoning the session.
func (e *authFailure) Unwrap() error {
	if e.identity {
		return gateway.ErrBackendAuth
	}
	return nil
}

const authRetryMin = time.Minute
const authRetryJitter = 15 * time.Second
const authFirstRetryMin = 3 * time.Second
const authFirstRetryJitter = 4 * time.Second

func authRetryDelay() time.Duration {
	return authRetryMin + time.Duration(rand.Int64N(int64(authRetryJitter)+1))
}

// authRetryPlan contains only bounded timing and count metadata. It never
// retains a credential, token, request URL or raw response header.
type authRetryPlan struct {
	attempt, nextAttempt          int
	localDelay, serverDelay, wait time.Duration
	source                        string
	serverPresent                 bool
}

func (p authRetryPlan) logAttrs(at time.Time) []any {
	return []any{"attempt", p.attempt, "next_attempt", p.nextAttempt,
		"local_delay_ms", p.localDelay.Milliseconds(), "server_retry_after_present", p.serverPresent,
		"server_retry_after_ms", p.serverDelay.Milliseconds(),
		"retry_after_ms", p.wait.Milliseconds(), "retry_at", at, "delay_source", p.source}
}

// planRetryLocked is the one place that combines the local retry policy with
// the server floor. A healthy, verified generation resets the outage counters.
func (m *Manager) planRetryLocked(failure *authFailure, base time.Duration) authRetryPlan {
	m.retryAttempts++
	local := max(base, failure.localRetryFloor)
	if failure.rejectionCooldown {
		policy := authRetryDelay()
		if failure.stage == "token exchange" && failure.status == http.StatusUnauthorized {
			if m.token401Retries == 0 {
				policy = authFirstRetryMin + time.Duration(rand.Int64N(int64(authFirstRetryJitter)+1))
			}
			m.token401Retries++
		}
		local = max(policy, failure.localRetryFloor)
	}
	server := max(time.Duration(0), failure.retryAfter)
	source := "local_policy"
	if server > local {
		source = "server_retry_after"
	} else if server > 0 && server == local {
		source = "local_policy_and_server_retry_after"
	}
	return authRetryPlan{attempt: m.retryAttempts, nextAttempt: m.retryAttempts + 1,
		localDelay: local, serverDelay: server, wait: max(local, server), source: source,
		serverPresent: failure.retryAfterPresent || server > 0}
}

func (e *authFailure) logAttrs() []any {
	stage, category := e.stage, e.category
	if stage == "" {
		stage = "authentication"
	}
	if category == "" {
		category = "authentication_failed"
	}
	attrs := []any{"reason", e.message, "stage", stage, "category", category}
	if e.status != 0 {
		attrs = append(attrs, "http_status", e.status)
	}
	if e.oauthError != "" {
		attrs = append(attrs, "oauth_error", e.oauthError)
	}
	if e.authStyle != "" {
		attrs = append(attrs, "configured_auth_style", e.authStyle)
	}
	return attrs
}

func transientAPI(e *APIError) bool {
	return e.Status == 0 && !e.Permanent || e.Status >= 500 || e.Status == 408 || e.Status == 429
}

// This boundary owns authentication/readiness failures only. Query-specific
// API errors keep their existing classification in managedClient.result.
func loginFailure(stage, message string, cause error, candidate *Factory) *authFailure {
	failure := &authFailure{message: message, stage: stage}
	var auth *authFailure
	var api *APIError
	var missing *missingSession
	var network net.Error
	switch {
	case errors.As(cause, &auth):
		*failure = *auth
		failure.message = message
		if failure.stage == "" {
			failure.stage = stage
		}
	case errors.As(cause, &missing):
		failure.stage, failure.category, failure.status = "session_identity", "session_missing", missing.status
		failure.retryAfter, failure.retryAfterPresent = missing.retryAfter, missing.retryAfterPresent
	case errors.As(cause, &api):
		failure.status = api.Status
		failure.retryAfter, failure.retryAfterPresent = api.RetryAfter, api.retryAfterPresent
		failure.localRetryFloor = api.localRetryFloor
		switch {
		case api.Status == 0 && api.Permanent:
			failure.category = "remote_response_invalid"
			failure.rejectionCooldown = true
		case transientAPI(api):
			failure.category = "remote_unavailable"
		case stage == "workspace_metadata" && api.Status == http.StatusUnauthorized && !api.Permanent:
			failure.category = "token_refresh_pending"
		default:
			failure.category = "remote_authentication_rejected"
			failure.rejectionCooldown = true
		}
	case errors.Is(cause, gateway.ErrBackendAuth):
		failure.permanent, failure.identity, failure.category = true, true, "identity_mismatch"
	case errors.As(cause, &network), errors.Is(cause, context.DeadlineExceeded), errors.Is(cause, context.Canceled):
		failure.category = "remote_unavailable"
	default:
		failure.category, failure.rejectionCooldown = "remote_response_invalid", true
	}
	if failure.permanent {
		failure.candidate = nil
	} else if candidate != nil {
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
		return &authFailure{message: "cannot load Viant Steward OOB credentials", permanent: true, stage: "local_configuration", category: "credentials_invalid"}
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
	outageEpoch                        uint64
	cleanup                            *backendCleanup
	runContext                         context.Context
	current                            gateway.AgentlyClientFactory
	notBefore                          time.Time
	retryPlan                          authRetryPlan
	retryAttempts, token401Retries     int
	login                              func(context.Context) (gateway.AgentlyClientFactory, error)
	onUnavailable                      func(context.Context, gateway.BackendState) error
	log                                *slog.Logger
	wake                               chan struct{}
	stateChanges                       chan struct{}
	fatal                              chan error
	running                            atomic.Bool
	retryBase, retryMax, probeInterval time.Duration
}

// backendCleanup belongs to one unavailable generation. Its callback runs on
// the already tracked caller, outside mu; recovery waits for its completion.
type backendCleanup struct {
	generation uint64
	state      gateway.BackendState
	done       chan struct{}
	err        error
}

var _ gateway.BackendLifecycle = (*Manager)(nil)
var _ gateway.ReadyEpochLifecycle = (*Manager)(nil)

func NewManager(login func(context.Context) (gateway.AgentlyClientFactory, error), onUnavailable func(context.Context, gateway.BackendState) error, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{state: gateway.BackendStarting, login: login, onUnavailable: onUnavailable, log: log,
		wake: make(chan struct{}, 1), stateChanges: make(chan struct{}, 1), fatal: make(chan error, 1), retryBase: time.Second, retryMax: 30 * time.Second, probeInterval: 30 * time.Second}
}

func (m *Manager) State() gateway.BackendState { m.mu.RLock(); defer m.mu.RUnlock(); return m.state }

// StateChanges wakes the owning runtime when readiness changes. It is distinct
// from the manager's recovery wake channel, which has a single internal reader.
// Consumers inspect State after receiving a hint; notifications never block the
// readiness lock or perform work while that lock is held.
func (m *Manager) StateChanges() <-chan struct{} { return m.stateChanges }

func (m *Manager) notifyStateChange() {
	select {
	case m.stateChanges <- struct{}{}:
	default:
	}
}

// WithReady serializes only local durable admission with an outage transition.
// The callback must not perform HTTP or call back into Manager.
func (m *Manager) WithReady(fn func() error) error {
	return m.WithReadyEpoch(func(uint64) error { return fn() })
}

func (m *Manager) WithReadyEpoch(fn func(uint64) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.state != gateway.BackendReady {
		return gateway.ErrBackendUnavailable
	}
	return fn(m.outageEpoch)
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

// Close admission before releasing mu. Waiting for thread owners must happen
// afterward: their earlier commands may still need the readiness read lock.
func (m *Manager) unavailableLocked(_ context.Context, state gateway.BackendState, failure *authFailure) error {
	if m.state == gateway.BackendReady {
		m.outageEpoch++
	}
	m.state, m.generation = state, m.generation+1
	m.cleanup = &backendCleanup{generation: m.generation, state: state, done: make(chan struct{})}
	attrs := []any{"state", state}
	if failure != nil {
		attrs = append(attrs, failure.logAttrs()...)
		if !failure.permanent {
			attrs = append(attrs, m.retryPlan.logAttrs(m.notBefore)...)
		}
	} else {
		attrs = append(attrs, "reason", "Backend authentication is starting", "stage", "readiness", "category", "connection_starting")
	}
	m.log.Info("agently_backend", attrs...)
	if state == gateway.BackendBlocked && failure != nil {
		m.log.Error("agently_backend_blocked", failure.logAttrs()...)
	}
	return nil
}

func (m *Manager) finishCleanup(ctx context.Context, cleanup *backendCleanup) error {
	var err error
	if m.onUnavailable != nil {
		err = m.onUnavailable(ctx, cleanup.state)
	}
	m.mu.Lock()
	cleanup.err = err
	close(cleanup.done)
	current := m.cleanup == cleanup && m.generation == cleanup.generation
	if current {
		if err != nil {
			m.state = gateway.BackendBlocked
			select {
			case m.fatal <- err:
			default:
			}
		}
		m.notifyStateChange()
	}
	m.mu.Unlock()
	if !current {
		return nil
	}
	return err
}

func (m *Manager) waitCleanup(ctx context.Context) error {
	m.mu.RLock()
	cleanup := m.cleanup
	m.mu.RUnlock()
	if cleanup == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-cleanup.done:
		m.mu.RLock()
		current := m.cleanup == cleanup && m.generation == cleanup.generation
		m.mu.RUnlock()
		if current {
			return cleanup.err
		}
		return nil
	}
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
	if generation != m.generation || m.state != gateway.BackendReady {
		m.mu.Unlock()
		return
	}
	if auth == nil {
		switch {
		case missing != nil:
			auth = loginFailure("session_identity", missing.Error(), missing, nil)
		case api != nil:
			// This API failure came from a conversation operation, not an auth
			// endpoint. Keep its existing recovery delay and request semantics.
			auth = &authFailure{message: "Viant Steward API request failed", stage: "backend_request", category: "remote_unavailable", status: api.Status,
				retryAfter: api.RetryAfter, retryAfterPresent: api.retryAfterPresent, localRetryFloor: api.localRetryFloor}
		default:
			auth = loginFailure("identity", gateway.ErrBackendAuth.Error(), gateway.ErrBackendAuth, nil)
		}
	}
	if !auth.permanent {
		m.retryPlan = m.planRetryLocked(auth, 0)
		m.notBefore = time.Now().Add(m.retryPlan.wait)
	}
	ctx := m.runContext
	if ctx == nil {
		ctx = context.Background()
	}
	_ = m.unavailableLocked(ctx, state, auth)
	cleanup := m.cleanup
	m.mu.Unlock()
	_ = m.finishCleanup(ctx, cleanup)
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) ready(factory gateway.AgentlyClientFactory) bool {
	_, generation, _ := m.snapshot()
	return m.readyGeneration(factory, generation)
}

func (m *Manager) readyGeneration(factory gateway.AgentlyClientFactory, expected uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.generation != expected {
		return false
	}
	if m.cleanup != nil {
		select {
		case <-m.cleanup.done:
			if m.cleanup.err != nil {
				return false
			}
		default:
			return false
		}
	}
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
	m.retryAttempts, m.token401Retries, m.retryPlan = 0, 0, authRetryPlan{}
	m.log.Info("agently_backend", "state", gateway.BackendReady, "reason", "Backend session and configured agent verified", "stage", "readiness", "category", "connection_verified")
	m.notifyStateChange()
	return true
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
			return nil, loginFailure("readiness", "Viant Steward session verification failed", err, candidate)
		}
	}
	if m.login == nil {
		return nil, &authFailure{message: "Viant Steward authentication is not configured", permanent: true, stage: "local_configuration", category: "configuration_invalid"}
	}
	factory, err := m.login(ctx)
	if err == nil && factory == nil {
		err = &authFailure{message: "Viant Steward authentication returned no session", permanent: true, stage: "local_configuration", category: "configuration_invalid"}
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
	m.runContext = ctx
	_ = m.unavailableLocked(ctx, gateway.BackendStarting, nil)
	cleanup := m.cleanup
	m.mu.Unlock()
	if err := m.finishCleanup(ctx, cleanup); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	delay := m.retryBase
	for ctx.Err() == nil {
		if err := m.waitCleanup(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
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
							err = loginFailure("readiness", "Viant Steward session verification failed", err, nil)
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
		_, generation, state = m.snapshot()
		if state != gateway.BackendStarting && state != gateway.BackendRecovering {
			continue
		}
		attempt, cancel := context.WithTimeout(ctx, 90*time.Second)
		factory, err := m.verifyOrLogin(attempt)
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			m.readyGeneration(factory, generation)
			delay = m.retryBase
			continue
		}
		var failure *authFailure
		if !errors.As(err, &failure) {
			err = loginFailure("authentication", "Viant Steward authentication failed", err, nil)
			_ = errors.As(err, &failure)
		}
		m.mu.Lock()
		if m.generation != generation {
			m.mu.Unlock()
			continue
		}
		if failure.candidate != nil {
			m.current = failure.candidate
		}
		var plan authRetryPlan
		var retryAt time.Time
		if failure.permanent {
			err = m.unavailableLocked(ctx, gateway.BackendBlocked, failure)
			cleanup = m.cleanup
		} else {
			err = nil
			base := time.Duration(float64(delay) * (0.8 + rand.Float64()*0.4))
			plan = m.planRetryLocked(failure, base)
			retryAt = time.Now().Add(plan.wait)
			m.retryPlan, m.notBefore = plan, retryAt
		}
		m.mu.Unlock()
		if failure.permanent {
			err = m.finishCleanup(ctx, cleanup)
		}
		if err != nil {
			return err
		}
		if failure.permanent {
			continue
		}
		attrs := append([]any{"state", m.State()}, plan.logAttrs(retryAt)...)
		m.log.Info("agently_backend_retry", append(attrs, failure.logAttrs()...)...)
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
