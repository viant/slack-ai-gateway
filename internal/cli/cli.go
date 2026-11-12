package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/agently"
	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	runner "github.com/viant/slack-ai-gateway/internal/runtime"
	"github.com/viant/slack-ai-gateway/internal/slack"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
)

type dependencies struct {
	login          func(context.Context, string, string, string, *scy.Resource) (gateway.AgentlyClientFactory, error)
	validateSecret func(context.Context, *scy.Resource) error
	connect        func(context.Context, config.SlackTokens, gateway.Route, *slog.Logger) (runner.Source, gateway.SlackSender, error)
	directory      func(config.SlackTokens, gateway.Route) gateway.SlackDirectory
	openMySQL      func(context.Context, string, *scy.Resource) (storage.Store, error)
}

func Execute(ctx context.Context, args []string, out io.Writer) error {
	return execute(ctx, args, out, dependencies{
		validateSecret: agently.ValidateOOBReference,
		login: func(ctx context.Context, base, principal, agent string, ref *scy.Resource) (gateway.AgentlyClientFactory, error) {
			return agently.LoginOOB(ctx, base, principal, agent, ref)
		},
		connect: func(ctx context.Context, tokens config.SlackTokens, route gateway.Route, log *slog.Logger) (runner.Source, gateway.SlackSender, error) {
			return slack.Connect(ctx, tokens, route, log)
		},
		directory: func(tokens config.SlackTokens, route gateway.Route) gateway.SlackDirectory {
			return gateway.NewCachedDirectory(slack.NewDirectory(tokens.Bot, route.WorkspaceID))
		},
	})
}

func execute(ctx context.Context, args []string, out io.Writer, deps dependencies) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: slack-ai-gateway <validate-config|migrate|check-agently|run|status|resolve-delivery> --config FILE")
	}
	command := args[0]
	if command != "validate-config" && command != "migrate" && command != "check-agently" && command != "run" && command != "status" && command != "resolve-delivery" {
		return fmt.Errorf("expected validate-config, migrate, check-agently, run, status or resolve-delivery")
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("config", "", "configuration file")
	driver := flags.String("storage-driver", "", "explicit storage driver (or set storage.driver in config)")
	var limit int
	var deliveryID int64
	var action, slackTS string
	var acceptRisk bool
	var channelID string
	var integrationID string
	if command == "validate-config" || command == "check-agently" || command == "status" || command == "resolve-delivery" {
		flags.StringVar(&integrationID, "integration", "", "select an integration by its logical name")
	}
	if command == "validate-config" {
		flags.StringVar(&channelID, "channel-id", "", "report the static effective policy for one channel")
	}
	if command == "status" {
		flags.IntVar(&limit, "limit", 100, "newest rows per table (1..1000); counts cover all rows")
	}
	if command == "resolve-delivery" {
		flags.Int64Var(&deliveryID, "delivery-id", 0, "uncertain delivery ID")
		flags.StringVar(&action, "action", "", "delivered or retry")
		flags.StringVar(&slackTS, "slack-ts", "", "verified Slack message timestamp for delivered")
		flags.BoolVar(&acceptRisk, "confirm-duplicate-risk", false, "explicitly accept duplicate risk for retry")
	}
	if err := flags.Parse(args[1:]); err != nil {
		return fmt.Errorf("invalid command flags; use --config FILE and --storage-driver sqlite|mysql")
	}
	if flags.NArg() != 0 || *path == "" {
		return fmt.Errorf("--config FILE is required; positional arguments are not accepted")
	}
	c, err := config.Load(*path)
	if err != nil {
		return err
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "storage-driver" {
			c.Storage.Driver = *driver
		}
	})
	if command == "validate-config" {
		return validateTopology(c, integrationID, channelID, out)
	}
	login := func(loginCtx context.Context) (gateway.AgentlyClientFactory, error) {
		if err := c.ValidateAgently(); err != nil {
			return nil, err
		}
		return deps.login(loginCtx, c.Agently.BaseURL, c.Agently.PrincipalID, c.Agently.AgentID, c.Agently.Auth.Secret)
	}
	if command == "check-agently" {
		if c.Integrations != nil || integrationID != "" {
			return checkTopologyAgently(ctx, c, integrationID, out, deps)
		}
		if _, err = login(ctx); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, "Viant Steward session identity and configured agent verified. No conversation, query or Slack call was made. Tool permissions are not verified.")
		return err
	}
	if err = c.ValidateStorage(); err != nil {
		return err
	}
	if command == "run" {
		if err = c.ValidateRun(); err != nil {
			return err
		}
	}
	s, err := openStorage(ctx, c, command, out, deps.openMySQL)
	if err != nil {
		return err
	}
	defer s.Close()
	if command == "migrate" {
		_, err = fmt.Fprintln(out, "Gateway SQLite schema is current.")
		return err
	}
	if integrationID != "" {
		s = s.ScopeName(integrationID)
	}
	if command == "status" {
		status, err := s.Status(ctx, limit)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(out)
		encoder.SetIndent("", "  ")
		return encoder.Encode(status)
	}
	if command == "resolve-delivery" {
		if err = s.ResolveDelivery(ctx, deliveryID, action, slackTS, acceptRisk); err != nil {
			return fmt.Errorf("cannot resolve delivery: requires delivery_unknown and either --action delivered --slack-ts TIMESTAMP or --action retry --confirm-duplicate-risk")
		}
		_, err = fmt.Fprintln(out, "Delivery resolution recorded. No Slack message or Agently query was sent.")
		return err
	}
	return runTopology(ctx, c, s, out, deps)
}

// openStorage keeps backend-specific initialization outside the runtime.
// MySQL provisioning is explicit; only SQLite can initialize on first run.
func openStorage(ctx context.Context, c config.Config, command string, out io.Writer, openMySQL func(context.Context, string, *scy.Resource) (storage.Store, error)) (storage.Store, error) {
	if c.Storage.Driver == "mysql" {
		if command == "migrate" {
			return nil, fmt.Errorf("migrate supports SQLite only; provision MySQL explicitly with Endly or the versioned MySQL DDL")
		}
		if openMySQL == nil {
			openMySQL = func(ctx context.Context, dsn string, secret *scy.Resource) (storage.Store, error) {
				return mysql.Open(ctx, dsn, secret)
			}
		}
		s, err := openMySQL(ctx, c.Storage.DSN, c.Storage.Secret)
		if err != nil {
			// Driver errors can include DSN credentials and server details.
			return nil, fmt.Errorf("cannot open gateway MySQL storage; check the configured DSN and provisioned database")
		}
		return s, nil
	}
	s, err := sqlite.Open(ctx, c.Storage.Path, command == "migrate" || command == "run")
	if err != nil {
		return nil, fmt.Errorf("open gateway storage: %w", err)
	}
	failed := true
	defer func() {
		if failed {
			_ = s.Close()
		}
	}()
	if command == "migrate" || command == "run" && s.Created() {
		if err = s.Migrate(ctx); err != nil {
			return nil, err
		}
		if command == "run" {
			if _, err = fmt.Fprintf(out, "Gateway SQLite database initialized at %s.\n", c.Storage.Path); err != nil {
				return nil, err
			}
		}
	}
	// Explicit migration already validates its result, as before.
	if command != "migrate" {
		if err = s.CheckSchema(ctx); err != nil {
			return nil, err
		}
	}
	failed = false
	return s, nil
}
