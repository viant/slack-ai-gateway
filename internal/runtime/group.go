package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type Integration struct {
	ID      string
	Runtime *Runtime
}

// Group runs the integrations whose assignments have already been reconciled
// and recovered by the process owner. Each runtime retains its readiness,
// worker and shutdown lifecycle.
type Group struct {
	Store        gateway.Store
	Integrations []Integration
	Log          *slog.Logger
}

type integrationFailure struct{ cause error }

func (e *integrationFailure) Error() string { return e.cause.Error() }
func (e *integrationFailure) Unwrap() error { return e.cause }

func runtimeFailure(message string, local bool) error {
	err := errors.New(message)
	if local {
		return &integrationFailure{cause: err}
	}
	return err
}

func (g *Group) Run(ctx context.Context) (runErr error) {
	if g.Store == nil || len(g.Integrations) == 0 {
		return fmt.Errorf("runtime group requires storage and integrations")
	}
	seen := map[string]bool{}
	scopes := map[int64]bool{}
	planner, ok := g.Store.(gateway.WorkPlanner)
	if !ok {
		return fmt.Errorf("runtime group storage must support work planning")
	}
	for _, integration := range g.Integrations {
		if integration.ID == "" || seen[integration.ID] || integration.Runtime == nil {
			return fmt.Errorf("runtime group requires unique integration identities and complete runtimes")
		}
		seen[integration.ID] = true
		if err := integration.Runtime.validate(); err != nil {
			return fmt.Errorf("integration %s: %w", integration.ID, err)
		}
		id := integration.Runtime.Store.(gateway.WorkPlanner).IntegrationID()
		if scopes[id] {
			return fmt.Errorf("runtime group requires unique integration scopes")
		}
		scopes[id] = true
	}
	log := g.Log
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var fatal error
	scheduler := newWorkScheduler(planner)
	defer func() {
		if err := scheduler.close(); err != nil && fatal == nil {
			runErr = err
		}
	}()
	type result struct {
		id  string
		err error
	}
	finished := make(chan result, len(g.Integrations))
	for _, integration := range g.Integrations {
		go func() { finished <- result{id: integration.ID, err: integration.Runtime.runPrepared(ctx, scheduler)} }()
	}
	stopped := 0
	for range g.Integrations {
		result := <-finished
		if result.err == nil {
			continue
		}
		var local *integrationFailure
		if errors.As(result.err, &local) {
			stopped++
			log.Error("integration_stopped", "integration_name", result.id, "reason", local.Error())
			continue
		}
		if fatal == nil {
			fatal = fmt.Errorf("integration %s: %w", result.id, result.err)
			cancel()
		}
	}
	if fatal != nil {
		return fatal
	}
	if stopped == len(g.Integrations) {
		return fmt.Errorf("all Slack integrations stopped; verify credentials and connection configuration")
	}
	return nil
}
