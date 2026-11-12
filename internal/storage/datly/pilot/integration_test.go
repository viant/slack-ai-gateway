package pilot

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	eventread "github.com/viant/slack-ai-gateway/internal/datly/read/event"
	integrationwrite "github.com/viant/slack-ai-gateway/internal/datly/topology/integrations/write"
	jobwrite "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
)

// Exercise new integration identity and explicit false persistence through the
// same native typed writer and transaction boundary used by reconciliation.
func TestNativeIntegrationIdentityGraphPilot(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			var dsn string
			maxConnections := 8
			if driver == "mysql" {
				dsn = storetest.MySQLDSN(t)
			} else {
				location := &url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "integration.sqlite")}
				location.RawQuery = url.Values{"_pragma": {"foreign_keys(ON)", "busy_timeout(5000)", "journal_mode(WAL)"}, "_txlock": {"immediate"}}.Encode()
				dsn = location.String()
				maxConnections = 1
			}
			db, err := sql.Open(driver, dsn)
			must(t, err)
			t.Cleanup(func() { must(t, db.Close()) })
			if driver == "sqlite" {
				schema, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
				must(t, err)
				_, err = db.ExecContext(ctx, string(schema))
				must(t, err)
			}
			server, err := standalone.New(ctx, standalone.Options{
				Config: &config.Config{BaseDir: t.TempDir(), Connector: "slack", Connectors: []connector.Config{{Name: "slack", Driver: driver, DSN: dsn, MaxOpenConns: maxConnections, MaxIdleConns: maxConnections}},
					GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/slack-ai-gateway/internal/datly/...", "github.com/viant/slack-ai-gateway/internal/storage/datly/pilot"}}},
				RequireLinked: true, Holders: []any{&integrationwrite.WriterComponent{}, &eventread.ReaderComponent{}, &jobwrite.WriterComponent{}, &PilotComponent{}},
			})
			must(t, err)
			t.Cleanup(func() { must(t, server.Shutdown(context.Background())) })
			must(t, server.Reload(ctx, 1))
			invoke := func(row *integrationwrite.Integration, fail bool) (*PilotOutput, error) {
				result, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[PilotComponent]().PkgPath(), Name: "Pilot"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/gateway/pilot"}}, Input: &PilotInput{Action: "integration", Integration: row, Fail: fail}})
				if err != nil {
					return nil, err
				}
				return result.(*PilotOutput), nil
			}
			row := &integrationwrite.Integration{Name: "native-assignment", Active: 1, SlackAppId: "A", SlackWorkspaceId: "T", BackendType: "agui", BackendEndpoint: "https://example.test/v1/ag-ui/run", BackendPrincipalId: "shared", CreatedAt: storetest.Time(1), UpdatedAt: storetest.Time(1)}
			result, err := invoke(row, false)
			must(t, err)
			if row.Id <= 0 || result.ID != row.Id {
				t.Fatalf("native integration identity not propagated: result=%+v row=%+v", result, row)
			}
			patch := &integrationwrite.Integration{}
			patch.SetId(row.Id)
			patch.SetActive(0)
			patch.SetUpdatedAt(storetest.Time(2))
			_, err = invoke(patch, false)
			must(t, err)
			var active int
			var name string
			must(t, db.QueryRowContext(ctx, "SELECT name,active FROM integration WHERE id=?", row.Id).Scan(&name, &active))
			if active != 0 || name != row.Name {
				t.Fatalf("sparse explicit false changed wrong fields: name=%q active=%d", name, active)
			}
			// A different historical assignment may have the same logical name.
			next := &integrationwrite.Integration{Name: row.Name, Active: 1, SlackAppId: row.SlackAppId, SlackWorkspaceId: row.SlackWorkspaceId, BackendType: row.BackendType, BackendEndpoint: row.BackendEndpoint, BackendPrincipalId: row.BackendPrincipalId, CreatedAt: storetest.Time(3), UpdatedAt: storetest.Time(3)}
			_, err = invoke(next, false)
			must(t, err)
			if next.Id <= row.Id {
				t.Fatalf("new assignment did not receive a distinct native identity: %d/%d", row.Id, next.Id)
			}
			rolled := &integrationwrite.Integration{Name: "rolled-back-assignment", Active: 1, SlackAppId: "A2", SlackWorkspaceId: "T", BackendType: "agui", BackendEndpoint: row.BackendEndpoint, BackendPrincipalId: row.BackendPrincipalId, CreatedAt: storetest.Time(4), UpdatedAt: storetest.Time(4)}
			_, err = invoke(rolled, true)
			if err == nil {
				t.Fatal("late integration transaction failure did not roll back")
			}
			var count int
			must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM integration WHERE name=?", rolled.Name).Scan(&count))
			if count != 0 {
				t.Fatal("failed root persisted its integration assignment")
			}
			query := &eventread.Input{}
			query.SetEventID("nonexistent-event")
			found, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[eventread.ReaderComponent]().PkgPath(), Name: "reader"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/gateway/events/exists"}}, Input: query})
			must(t, err)
			data := found.(*eventread.Output).Data
			if len(data) != 1 || data[0].Found != 0 {
				t.Fatalf("native global existence projection=%+v", data)
			}
			job := &jobwrite.GatewayJob{IntegrationId: row.Id, SlackChannelId: "C", SlackThreadRootMessageId: "1.000001", SlackEventId: "native-existing-event", SlackPromptAuthorId: "U", SlackPromptMessageId: "1.000001", PromptText: storetest.Pointer("opaque native probe"), GatewayRequestId: "native-existence-request", GatewayExecutionState: "pending", CreatedAt: storetest.Time(1), GatewayExecutionUpdatedAt: storetest.Time(1), GatewayStatusUpdatedAt: storetest.Time(1), SlackProgressMode: "message", SlackProgressState: "ready", SlackConversationType: "channel"}
			_, err = server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[PilotComponent]().PkgPath(), Name: "Pilot"}, Route: spec.RouteRef{Method: "POST", Path: "/internal/gateway/pilot"}}, Input: &PilotInput{Action: "write", GatewayJob: job}})
			must(t, err)
			query.SetEventID(job.SlackEventId)
			found, err = server.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[eventread.ReaderComponent]().PkgPath(), Name: "reader"}, Route: spec.RouteRef{Method: "GET", Path: "/internal/gateway/events/exists"}}, Input: query})
			must(t, err)
			data = found.(*eventread.Output).Data
			if len(data) != 1 || data[0].Found != 1 {
				t.Fatalf("native existing-event projection=%+v", data)
			}
		})
	}
}
