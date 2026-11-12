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
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/slack-ai-gateway/internal/datly/operations/check"
	"github.com/viant/slack-ai-gateway/internal/datly/operations/notbefore"
	"github.com/viant/slack-ai-gateway/internal/datly/operations/status"
	"github.com/viant/slack-ai-gateway/internal/storetest"
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
	at := time.UnixMilli(1).UTC()
	result, err := db.ExecContext(ctx, `INSERT INTO integration(name,active,slack_app_id,slack_workspace_id,backend_type,backend_endpoint,backend_agent_id,backend_principal_id,created_at,updated_at)
	VALUES ('default',1,'A','T','agui','http://127.0.0.1/v1/ag-ui/run','','shared',?,?)`, at, at)
	storetest.Must(t, err)
	integrationID, err := result.LastInsertId()
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
	VALUES (?,'C','1.001','event1','alice','1.001','private prompt',0,'request1','pending',?,?,?),
	       (?,'C','1.001','event2','bob','1.002','private prompt',0,'request2','pending',?,?,?)`, integrationID, at, at, at, integrationID, at, at, at)
	storetest.Must(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO slack_outgoing_message(job_id,slack_message_index,slack_message_text,slack_delivery_state,created_at,updated_at)
	VALUES (1,0,'private reply one','pending',?,?),
	       (1,1,'private reply two','pending',?,?)`, at, at, at, at)
	storetest.Must(t, err)
	server, err := standalone.New(ctx, standalone.Options{
		RequireLinked: true,
		Config: &config.Config{
			BaseDir: t.TempDir(), Connector: "slack",
			Connectors:  []connector.Config{{Name: "slack", Driver: "mysql", DSN: dsn}},
			GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{"github.com/viant/slack-ai-gateway/internal/datly/operations/..."}},
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
	for _, name := range []string{"gateway_jobs", "conversation_bindings", "slack_outgoing_messages", "slack_agent_session_publications"} {
		selectors = append(selectors, &state.NamedSelector{Name: name, Selector: state.Selector{Limit: 1}})
	}
	response := invoke(&status.ReaderComponent{}, "/internal/gateway/operations/status", &status.Input{}, provider.Static(handler.SelectorsKey, selectors)).(*status.Output)
	if len(response.Data) != 1 {
		t.Fatalf("snapshot root count=%d", len(response.Data))
	}
	snapshot := response.Data[0]
	if len(snapshot.GatewayJobs) != 1 || snapshot.GatewayJobs[0].Id != 2 || len(snapshot.SlackOutgoingMessages) != 1 || snapshot.SlackOutgoingMessages[0].Id != 2 {
		t.Fatalf("native newest-first limit did not apply: jobs=%+v deliveries=%+v", snapshot.GatewayJobs, snapshot.SlackOutgoingMessages)
	}
	request := snapshot.GatewayJobs[0]
	if !request.CreatedAt.Equal(at) || !request.GatewayExecutionUpdatedAt.Equal(at) || !request.GatewayStatusUpdatedAt.Equal(at) || request.GatewayExecutionFinishedAt != nil || request.ContentPurgedAt != nil || !snapshot.SlackOutgoingMessages[0].CreatedAt.Equal(at) || !snapshot.SlackOutgoingMessages[0].UpdatedAt.Equal(at) {
		t.Fatalf("native diagnostic dates lost millisecond precision or nullability: request=%+v outgoing=%+v", request, snapshot.SlackOutgoingMessages[0])
	}
	for name, size := range map[string]int{"gateway_jobs": len(snapshot.GatewayJobs), "conversation_bindings": len(snapshot.ConversationBindings), "slack_outgoing_messages": len(snapshot.SlackOutgoingMessages), "slack_agent_session_publications": len(snapshot.SlackAgentSessionPublications)} {
		if size > 1 {
			t.Errorf("%s metadata exceeds requested limit: %d", name, size)
		}
	}
	counts := make(map[string]int)
	for _, count := range snapshot.Counts {
		counts[count.TableName] += count.RowCount
	}
	for dimension, table := range map[string]string{
		"gateway_job.gateway_execution_state":                     "gateway_job",
		"gateway_job.slack_progress_state":                        "gateway_job",
		"conversation_binding.gateway_binding_state":              "conversation_binding",
		"slack_outgoing_message.slack_delivery_state":             "slack_outgoing_message",
		"slack_agent_session_publication.slack_publication_state": "slack_agent_session_publication",
	} {
		var actual int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&actual); err != nil {
			t.Fatal(err)
		}
		if counts[dimension] != actual {
			t.Errorf("%s full count=%d, want %d", dimension, counts[dimension], actual)
		}
	}
	// The generated operator shapes have no upstream or response payload slots.
	for _, typ := range []reflect.Type{reflect.TypeFor[status.GatewayJobMetadata](), reflect.TypeFor[status.ConversationBindingMetadata](), reflect.TypeFor[status.SlackOutgoingMessageMetadata](), reflect.TypeFor[status.SlackAgentSessionPublicationMetadata]()} {
		for field := range typ.Fields() {
			for _, forbidden := range []string{"PromptText", "GatewayExecutionResultText", "SlackMessageText", "BackendProtocolMessages", "BackendProtocolState"} {
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
	if len(cooldownResult.Data) != 1 || cooldownResult.Data[0].NotBefore != nil {
		t.Fatalf("unexpected cooldown for unrelated workspace: %+v", cooldownResult)
	}
}
