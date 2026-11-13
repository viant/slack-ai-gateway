// Package runtime wires transport-neutral gateway components. It knows neither
// Slack credentials nor fake backend implementations.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	slackpresentation "github.com/viant/agently-core/sdk/slack"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type Source interface {
	Run(context.Context, gateway.EventSink, func()) error
}

type Runtime struct {
	Store                 gateway.Store
	Source                Source
	Route                 gateway.Route
	Policy                gateway.AccessPolicy
	Clients               gateway.AgentlyClientFactory
	Backend               gateway.BackendLifecycle
	Sender                gateway.SlackSender
	AgentID               string
	Log                   *slog.Logger
	StatusMode            gateway.StatusMode
	StatusRefreshInterval time.Duration
	ResponseMode          slackpresentation.ResponseMode
	AgentlyBaseURL        string
	delivered             atomic.Int64
}

func (r *Runtime) Delivered() int64 { return r.delivered.Load() }

// Run requires exclusive ownership of a migrated Store for its entire lifetime.
// Workers start only after the transport's identity-verified ready signal.
func (r *Runtime) Run(ctx context.Context) (runErr error) {
	if err := r.validate(); err != nil {
		return err
	}
	if err := r.Store.Recover(ctx); err != nil {
		return fmt.Errorf("runtime recovery failed")
	}
	scheduler := newWorkScheduler(r.Store.(gateway.WorkPlanner))
	defer func() {
		if err := scheduler.close(); err != nil {
			var local *integrationFailure
			if runErr == nil || errors.As(runErr, &local) {
				runErr = err
			}
		}
	}()
	return r.runPrepared(ctx, scheduler)
}

func (r *Runtime) validate() error {
	if r.Backend != nil {
		r.Clients = r.Backend
	}
	if err := r.ResponseMode.Validate(); err != nil {
		return err
	}
	if r.Store == nil || r.Source == nil || r.Policy == nil || r.Clients == nil || r.Sender == nil {
		return fmt.Errorf("runtime components are incomplete")
	}
	if _, ok := r.Policy.(gateway.DeliveryPolicy); !ok {
		return fmt.Errorf("runtime policy must authorize both queries and deliveries")
	}
	if _, ok := r.Sender.(gateway.StatusSender); !ok {
		return fmt.Errorf("runtime sender must support request statuses")
	}
	if planner, ok := r.Store.(gateway.WorkPlanner); !ok || planner.IntegrationID() <= 0 {
		return fmt.Errorf("runtime storage must support scoped work planning")
	}
	return nil
}

// runPrepared reuses the worker lifecycle after the process owner has recovered
// storage once. Query work stays scoped to this assignment; its Slack bot may
// also publish terminal cleanup for historical assignments.
func (r *Runtime) runPrepared(ctx context.Context, scheduler *workScheduler) error {
	deliveryPolicy := r.Policy.(gateway.DeliveryPolicy)
	statusSender := r.Sender.(gateway.StatusSender)
	log := r.Log
	if log == nil {
		log = slog.Default()
	}
	if r.Backend != nil {
		// Restart must not replay unsubmitted questions from a previous process.
		if err := r.Store.FailUnstarted(ctx, "backend_unavailable"); err != nil {
			return fmt.Errorf("runtime unavailable-work recovery failed")
		}
	}
	fatal := make(chan error, 1)
	var sharedFailure atomic.Pointer[error]
	recordFatal := func(err error) {
		var local *integrationFailure
		if !errors.As(err, &local) {
			sharedFailure.CompareAndSwap(nil, &err)
		}
		select {
		case fatal <- err:
		default:
		}
	}
	sourceFailure := func(err error, message string) error {
		if errors.Is(err, gateway.ErrIntakePersistence) {
			failure := errors.New("runtime intake persistence failed")
			recordFatal(failure)
			return failure
		}
		if ctx.Err() != nil && err == nil {
			return nil
		}
		return runtimeFailure(message, true)
	}
	stopBackend := func() {}
	if r.Backend != nil {
		backendCtx, cancelBackend := context.WithCancel(ctx)
		backendDone := make(chan struct{})
		go func() {
			defer close(backendDone)
			if err := r.Backend.Run(backendCtx); err != nil && (backendCtx.Err() == nil || !errors.Is(err, backendCtx.Err())) {
				recordFatal(errors.New("backend connection recovery or storage failed"))
			}
		}()
		stopBackend = func() { cancelBackend(); <-backendDone }
		defer stopBackend()
	}
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	sourceCtx, cancelSource := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSource()
	ready := make(chan struct{})
	var once sync.Once
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- r.Source.Run(sourceCtx, gateway.Intake{Store: r.Store, Route: r.Route, StatusMode: r.StatusMode, Backend: r.Backend}, func() { once.Do(func() { close(ready) }) })
	}()
	var backendAuth atomic.Bool
	var slackUnavailable atomic.Bool
	stopClaims := make(chan struct{})
	var workers sync.WaitGroup
	report := func(taskCtx context.Context, err error) bool {
		if err == nil {
			return false
		}
		if taskCtx.Err() != nil && errors.Is(err, taskCtx.Err()) {
			// A local transport stop cancels in-flight claims. Their native
			// cancellation is not evidence that the shared database failed.
			return false
		}
		if r.Backend != nil && (errors.Is(err, gateway.ErrBackendAuth) || errors.Is(err, gateway.ErrBackendUnavailable)) {
			log.Warn("agently_backend_unavailable", "state", r.Backend.State())
			return false
		}

		var issue *gateway.DeliveryIssue
		if errors.As(err, &issue) {
			log.Warn("delivery_requires_attention", "delivery_id", issue.ID, "state", issue.State, "reason", issue.Reason)
		} else {
			// Errors from dependencies may carry secrets. Report category only.
			message := "runtime worker or storage failed"
			if errors.Is(err, gateway.ErrBackendAuth) {
				backendAuth.Store(true)
				message = gateway.ErrBackendAuth.Error()
			}
			if errors.Is(err, gateway.ErrSlackAuth) {
				slackUnavailable.Store(true)
				message = gateway.ErrSlackAuth.Error()
			}
			if errors.Is(err, gateway.ErrMetadataUnavailable) {
				slackUnavailable.Store(true)
				message = gateway.ErrMetadataUnavailable.Error()
			}
			var send *gateway.SendError
			if errors.As(err, &send) && send.Kind == gateway.SendCredentials {
				slackUnavailable.Store(true)
				message = "Slack credentials rejected; runtime stopped"
			}
			local := errors.Is(err, gateway.ErrBackendAuth) || errors.Is(err, gateway.ErrSlackAuth) || errors.Is(err, gateway.ErrMetadataUnavailable) || send != nil && send.Kind == gateway.SendCredentials
			recordFatal(runtimeFailure(message, local))
			return true
		}
		return false
	}
	// Dispatched steps and observers share one lifetime. A step registers its
	// observer before returning, so shutdown joins every stream before closing Store.
	execute := func(task func() error) {
		workers.Add(1)
		go func() { defer workers.Done(); report(workCtx, task()) }()
	}
	canStart := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-stopClaims:
			return false
		case <-workCtx.Done():
			return false
		default:
			return true
		}
	}
	var runErr error
	var outbound *gateway.OutboundWorker
	registered := false
	integrationID := r.Store.(gateway.WorkPlanner).IntegrationID()
	sourceJoined := false
	startup := time.NewTimer(30 * time.Second)
	select {
	case <-ready:
		w := gateway.Worker{Store: r.Store, Policy: r.Policy, Clients: r.Clients, Backend: r.Backend, AgentID: r.AgentID, Execute: execute,
			ResponseMode: r.ResponseMode, AgentlyBaseURL: r.AgentlyBaseURL, Log: log,
			OnReview: func(jobID int64, requestID, reason string) {
				log.Warn("query_needs_review", "job_id", jobID, "gateway_request_id", requestID, "reason", reason)
			}}
		delivery := gateway.DeliveryWorker{Store: r.Store, Sender: r.Sender, Policy: deliveryPolicy, OnDelivered: func(d gateway.SlackOutgoingMessage) {
			r.delivered.Add(1)
			log.Info("reply_delivered", "delivery_id", d.ID, "job_id", d.JobID, "kind", d.Kind, "workspace_id", d.WorkspaceID, "channel_id", d.ChannelID, "slack_thread_root_message_id", d.RootTS, "slack_message_id", d.SlackTS)
		}}
		statuses := &gateway.StatusWorker{Store: r.Store, Sender: statusSender, Policy: deliveryPolicy, Backend: r.Backend, RefreshInterval: r.StatusRefreshInterval,
			OnStatus: func(s gateway.GatewayJobProgress, result gateway.StatusResult, now time.Time) {
				attrs := []any{"job_id", s.JobID, "job_integration_id", s.IntegrationID, "workspace_id", s.WorkspaceID, "channel_id", s.ChannelID, "mode", s.Mode, "target", s.Target, "slack_thread_root_message_id", s.RootTS, "phase", s.Phase, "acknowledged", result.Acknowledged || result.SlackTS != "", "agent_status", result.AgentStatus, "session_status", result.SessionStatus, "warning", result.Warning, "revision", s.Revision, "state", result.State, "reason", result.Reason}
				if !result.RetryAt.IsZero() {
					attrs = append(attrs, "retry_at", result.RetryAt)
				}
				if s.SlackTS == "" && result.SlackTS != "" {
					attrs = append(attrs, "ack_latency_ms", max(int64(0), now.Sub(s.CreatedAt).Milliseconds()))
				}
				if result.Reason != "" {
					log.Warn("job_progress", attrs...)
				} else {
					log.Info("job_progress", attrs...)
				}
			}}
		outbound = &gateway.OutboundWorker{SlackOutgoingMessage: &delivery, Status: statuses, Interval: time.Second}
		scheduler.register(&scheduledRuntime{id: integrationID, ctx: workCtx, canStart: canStart,
			query: w.Step, outbound: outbound.Step, nextSend: outbound.NextReadyAt,
			report: report, fatal: recordFatal, workers: &workers})
		registered = true
		if backend, ok := r.Backend.(interface{ StateChanges() <-chan struct{} }); ok {
			// The manager coalesces changes for its lifetime. A backend may close
			// its notification channel when that lifetime ends.
			changes := backend.StateChanges()
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-stopClaims:
						return
					case _, open := <-changes:
						if !open {
							return
						}
						scheduler.wake(integrationID)
					}
				}
			}()
		}
		select {
		case <-ctx.Done():
		case runErr = <-fatal:
		case err := <-sourceDone:
			sourceJoined = true
			runErr = sourceFailure(err, "Slack source stopped")
		}
	case <-ctx.Done():
	case runErr = <-fatal:
	case <-startup.C:
		runErr = runtimeFailure("Slack did not become ready within 30 seconds", true)
	case err := <-sourceDone:
		sourceJoined = true
		runErr = sourceFailure(err, "Slack source failed before readiness")
	}
	startup.Stop()
	// Stop ingestion and new claims first. Already started operations can finish
	// and persist their results, but shutdown never drains the entire queue.
	close(stopClaims)
	if registered {
		scheduler.unregister(integrationID)
	}
	cancelSource()
	shutdownDeadline := time.Now().Add(15 * time.Second)
	if runErr != nil {
		cancelWork()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	grace := time.NewTimer(15 * time.Second)
	select {
	case <-joined:
	case <-grace.C:
		cancelWork()
		<-joined // All adapters must honor cancellation; never close Store under them.
	}
	grace.Stop()
	// Normal workers have joined, so this retains single ownership of outbound
	// writes. Only terminal statuses are attempted within the remaining grace.
	if backendAuth.Load() && !slackUnavailable.Load() && outbound != nil && time.Now().Before(shutdownDeadline) {
		drainCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), shutdownDeadline)
		for drainCtx.Err() == nil {
			worked, err := outbound.TerminalStep(drainCtx)
			if err != nil {
				report(drainCtx, err)
				break
			}
			if !worked {
				break
			}
		}
		cancel()
	}
	if !sourceJoined {
		if err := <-sourceDone; errors.Is(err, gateway.ErrIntakePersistence) {
			runErr = sourceFailure(err, "Slack source stopped")
		}
	}
	// Join the backend owner before inspecting shared failures: its outage
	// callback can discover a database error while other workers are stopping.
	stopBackend()
	select {
	case err := <-fatal:
		if runErr == nil {
			runErr = err
		}
	default:
	}
	// A storage failure discovered while joining an already-stopped integration
	// must still stop the process, even when a Slack failure arrived first.
	if err := sharedFailure.Load(); err != nil {
		runErr = *err
	}
	return runErr
}
