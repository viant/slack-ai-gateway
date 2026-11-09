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
	"github.com/viant/agently-slack-gateway/internal/gateway"
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
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.ResponseMode.Validate(); err != nil {
		return err
	}
	if r.Store == nil || r.Source == nil || r.Policy == nil || r.Clients == nil || r.Sender == nil || r.AgentID == "" {
		return fmt.Errorf("runtime components are incomplete")
	}
	deliveryPolicy, ok := r.Policy.(gateway.DeliveryPolicy)
	if !ok {
		return fmt.Errorf("runtime policy must authorize both queries and deliveries")
	}
	statusSender, ok := r.Sender.(gateway.StatusSender)
	if !ok {
		return fmt.Errorf("runtime sender must support request statuses")
	}
	log := r.Log
	if log == nil {
		log = slog.Default()
	}
	if err := r.Store.Recover(ctx); err != nil {
		return fmt.Errorf("runtime recovery failed")
	}
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()
	sourceCtx, cancelSource := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelSource()
	ready := make(chan struct{})
	var once sync.Once
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- r.Source.Run(sourceCtx, gateway.Intake{Store: r.Store, Route: r.Route, StatusMode: r.StatusMode}, func() { once.Do(func() { close(ready) }) })
	}()
	fatal := make(chan error, 1)
	var backendAuth atomic.Bool
	var slackUnavailable atomic.Bool
	stopClaims := make(chan struct{})
	var workers sync.WaitGroup
	report := func(err error) bool {
		if err == nil {
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
			select {
			case fatal <- errors.New(message):
			default:
			}
			return true
		}
		return false
	}
	// Admission loops and observers share one lifetime. A loop registers its
	// task before returning, so shutdown joins every stream before closing Store.
	execute := func(task func() error) {
		workers.Add(1)
		go func() { defer workers.Done(); report(task()) }()
	}
	start := func(step func(context.Context) (bool, error)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-stopClaims:
					return
				case <-workCtx.Done():
					return
				default:
				}
				worked, err := step(workCtx)
				if report(err) {
					return
				}
				if !worked {
					timer := time.NewTimer(100 * time.Millisecond)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return
					case <-stopClaims:
						timer.Stop()
						return
					case <-workCtx.Done():
						timer.Stop()
						return
					}
				}
			}
		}()
	}
	var runErr error
	var outbound *gateway.OutboundWorker
	sourceJoined := false
	startup := time.NewTimer(30 * time.Second)
	select {
	case <-ready:
		for range 4 {
			w := gateway.Worker{Store: r.Store, Policy: r.Policy, Clients: r.Clients, AgentID: r.AgentID, Execute: execute,
				ResponseMode: r.ResponseMode, AgentlyBaseURL: r.AgentlyBaseURL, Log: log,
				OnReview: func(jobID int64, requestID, reason string) {
					log.Warn("query_needs_review", "job_id", jobID, "request_id", requestID, "reason", reason)
				}}
			start(w.Step)
		}
		delivery := gateway.DeliveryWorker{Store: r.Store, Sender: r.Sender, Policy: deliveryPolicy, OnDelivered: func(d gateway.Delivery) {
			r.delivered.Add(1)
			log.Info("reply_delivered", "delivery_id", d.ID, "workspace_id", d.WorkspaceID, "channel_id", d.ChannelID, "thread_ts", d.RootTS, "slack_ts", d.SlackTS)
		}}
		statuses := &gateway.StatusWorker{Store: r.Store, Sender: statusSender, Policy: deliveryPolicy, RefreshInterval: r.StatusRefreshInterval,
			OnStatus: func(s gateway.RequestStatus, result gateway.StatusResult, now time.Time) {
				attrs := []any{"job_id", s.JobID, "workspace_id", s.WorkspaceID, "channel_id", s.ChannelID, "mode", s.Mode, "target", s.Target, "thread_ts", s.RootTS, "phase", s.Phase, "acknowledged", result.Acknowledged || result.SlackTS != "", "agent_status", result.AgentStatus, "session_status", result.SessionStatus, "warning", result.Warning, "revision", s.Revision, "state", result.State, "reason", result.Reason}
				if !result.RetryAt.IsZero() {
					attrs = append(attrs, "retry_at", result.RetryAt)
				}
				if s.SlackTS == "" && result.SlackTS != "" {
					attrs = append(attrs, "ack_latency_ms", max(int64(0), now.Sub(s.CreatedAt).Milliseconds()))
				}
				if result.Reason != "" {
					log.Warn("request_status", attrs...)
				} else {
					log.Info("request_status", attrs...)
				}
			}}
		outbound = &gateway.OutboundWorker{Delivery: &delivery, Status: statuses, Interval: time.Second}
		start(outbound.Step)
		select {
		case <-ctx.Done():
		case runErr = <-fatal:
		case <-sourceDone:
			sourceJoined = true
			runErr = fmt.Errorf("Slack source stopped")
		}
	case <-ctx.Done():
	case <-startup.C:
		runErr = fmt.Errorf("Slack did not become ready within 30 seconds")
	case <-sourceDone:
		sourceJoined = true
		runErr = fmt.Errorf("Slack source failed before readiness")
	}
	startup.Stop()
	// Stop ingestion and new claims first. Already started operations can finish
	// and persist their results, but shutdown never drains the entire queue.
	close(stopClaims)
	cancelSource()
	shutdownDeadline := time.Now().Add(30 * time.Second)
	if runErr != nil {
		cancelWork()
	}
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	grace := time.NewTimer(30 * time.Second)
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
			if err != nil || !worked {
				break
			}
		}
		cancel()
	}
	if !sourceJoined {
		<-sourceDone
	}
	select {
	case err := <-fatal:
		if runErr == nil {
			runErr = err
		}
	default:
	}
	return runErr
}
