package operations_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/operations/check"
	"github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/operations/notbefore"
	"github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/operations/status"
	"github.com/viant/agently-slack-gateway/internal/storetest"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
)

// Provision an owned fixture so limits are exercised against multiple rows.
// The administrative DSN's original database is never changed.
func TestOperationGraphsNative(t *testing.T) {
	dsn := storetest.MySQLDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, `INSERT INTO inbox_jobs(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,request_id,state,created_at,updated_at)
		VALUES ('A','T','C','1.001','event1','alice','1.001','private prompt',0,'request1','pending',1,1),
		       ('A','T','C','1.001','event2','bob','1.002','private prompt',0,'request2','pending',1,1)`)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO outbox_messages(job_id,workspace_id,channel_id,root_thread_ts,part,content,state,created_at,updated_at)
		VALUES (1,'T','C','1.001',0,'private reply one','pending',1,1),
		       (1,'T','C','1.001',1,'private reply two','pending',1,1)`)
	storetest.Must(t, err)
	server, err := standalone.New(ctx, standalone.Options{
		RequireLinked: true,
		Config: &config.Config{
			BaseDir: t.TempDir(), Connector: "slack",
			Connectors:  []connector.Config{{Name: "slack", Driver: "mysql", DSN: dsn}},
			GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/agently-slack-gateway/internal/storage/mysql/graph/operations/..."}},
		}, Holders: []any{&status.ReaderComponent{}, &check.ReaderComponent{}, &notbefore.ReaderComponent{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err = server.Reload(ctx, 1); err != nil {
		t.Fatal(err)
	}
	invoke := func(holder any, path string, input any, providers ...locator.Provider) any {
		t.Helper()
		value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{
			Target: dexec.ComponentTarget{
				Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeOf(holder).Elem().PkgPath(), Name: "reader"},
				Route:     spec.RouteRef{Method: "GET", Path: path},
			}, Input: input, Providers: providers,
		})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	selectors := state.Selectors{}
	for _, name := range []string{"jobs", "bindings", "deliveries", "statuses", "sessions"} {
		selectors = append(selectors, &state.NamedSelector{Name: name, Selector: state.Selector{Limit: 1}})
	}
	response := invoke(&status.ReaderComponent{}, "/internal/gateway/operations/status", &status.Input{}, provider.Static(handler.SelectorsKey, selectors)).(*status.Output)
	if len(response.Data) != 1 {
		t.Fatalf("snapshot root count=%d", len(response.Data))
	}
	snapshot := response.Data[0]
	if len(snapshot.Jobs) != 1 || snapshot.Jobs[0].Id != 2 || len(snapshot.Deliveries) != 1 || snapshot.Deliveries[0].Id != 2 {
		t.Fatalf("native newest-first limit did not apply: jobs=%+v deliveries=%+v", snapshot.Jobs, snapshot.Deliveries)
	}
	for name, size := range map[string]int{"jobs": len(snapshot.Jobs), "bindings": len(snapshot.Bindings), "deliveries": len(snapshot.Deliveries), "statuses": len(snapshot.Statuses), "sessions": len(snapshot.Sessions)} {
		if size > 1 {
			t.Errorf("%s metadata exceeds requested limit: %d", name, size)
		}
	}
	counts := make(map[string]int)
	for _, count := range snapshot.Counts {
		counts[count.TableName] += count.RowCount
	}
	for _, table := range []string{"inbox_jobs", "conversation_bindings", "outbox_messages", "request_status", "agent_session_publications"} {
		var actual int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if counts[table] != actual {
			t.Errorf("%s full count=%d, want %d", table, counts[table], actual)
		}
	}
	// The generated operator shapes have no upstream or response payload slots.
	for _, typ := range []reflect.Type{reflect.TypeFor[status.Job](), reflect.TypeFor[status.Binding](), reflect.TypeFor[status.Delivery](), reflect.TypeFor[status.RequestStatus](), reflect.TypeFor[status.AgentSession]()} {
		for field := range typ.Fields() {
			for _, forbidden := range []string{"QueryText", "ResultText", "Content"} {
				if strings.EqualFold(field.Name, forbidden) {
					t.Errorf("operator type %s exposes %s", typ.Name(), field.Name)
				}
			}
		}
	}
	workspace := fmt.Sprintf("operator-read-only-%d", time.Now().UnixNano())
	identityInput := &check.Input{}
	identityInput.SetAppID(workspace)
	identityInput.SetWorkspaceID(workspace)
	identityInput.SetChannelID(workspace)
	identityInput.SetFixedChannel(true)
	identityInput.SetPrincipal(workspace)
	identityResult := invoke(&check.ReaderComponent{}, "/internal/gateway/operations/check", identityInput).(*check.Output)
	if len(identityResult.Data) != 1 || identityResult.Data[0].IncompatibleJobs == nil || identityResult.Data[0].IncompatibleBindings == nil {
		t.Fatal("identity result is incomplete")
	}
	cooldownInput := &notbefore.Input{}
	cooldownInput.SetWorkspaceID(workspace)
	cooldownResult := invoke(&notbefore.ReaderComponent{}, "/internal/gateway/operations/notbefore", cooldownInput).(*notbefore.Output)
	if len(cooldownResult.Data) != 1 || cooldownResult.Data[0].NotBefore != 0 {
		t.Fatalf("unexpected cooldown for unrelated workspace: %+v", cooldownResult)
	}
}
