package pilot

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap/connector"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	read "github.com/viant/slack-ai-gateway/internal/datly/read"
	write "github.com/viant/slack-ai-gateway/internal/datly/write"
	"github.com/viant/slack-ai-gateway/internal/storetest"
	"github.com/viant/slack-ai-gateway/migrations"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	_ "modernc.org/sqlite"
)

// Use the MySQL-authored components unchanged against a durable SQLite file.
// The single runtime connection and immediate transactions match gateway SQLite.
func TestNativeSQLiteGraphPilot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	location := &url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "gateway.sqlite")}
	location.RawQuery = url.Values{
		"_pragma": {"foreign_keys(ON)", "busy_timeout(5000)", "journal_mode(WAL)"},
		"_txlock": {"immediate"},
	}.Encode()
	dsn := location.String()
	db, err := sql.Open("sqlite", dsn)
	must(t, err)
	t.Cleanup(func() { must(t, db.Close()) })
	schema, err := migrations.SQLite.ReadFile("sqlite/001_initial.sql")
	must(t, err)
	_, err = db.ExecContext(ctx, string(schema))
	must(t, err)
	seedPilotTopology(t, db)

	server, err := standalone.New(ctx, standalone.Options{
		Config: &config.Config{
			BaseDir: t.TempDir(), Connector: "slack",
			Connectors: []connector.Config{{Name: "slack", Driver: "sqlite", DSN: dsn, MaxOpenConns: 1, MaxIdleConns: 1}},
			GoBootstrap: &config.Packages{LinkedOnly: true, Packages: []string{
				"github.com/viant/slack-ai-gateway/internal/datly/...",
				"github.com/viant/slack-ai-gateway/internal/storage/datly/pilot",
			}},
		},
		RequireLinked: true,
		Holders:       []any{&read.ReaderComponent{}, &write.WriterComponent{}, &PilotComponent{}},
	})
	must(t, err)
	t.Cleanup(func() { must(t, server.Shutdown(context.Background())) })
	must(t, server.Reload(ctx, 1))
	target := dexec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[PilotComponent]().PkgPath(), Name: "Pilot"},
		Route:     spec.RouteRef{Method: "POST", Path: "/internal/gateway/pilot"},
	}
	invoke := func(ctx context.Context, input *PilotInput) (*PilotOutput, error) {
		value, err := server.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Input: input})
		if err != nil {
			return nil, err
		}
		return value.(*PilotOutput), nil
	}
	newJob := func(event string) *write.GatewayJob {
		now := time.Now().UTC().Truncate(time.Millisecond)
		return &write.GatewayJob{
			IntegrationId: 1, SlackChannelId: "C", SlackThreadRootMessageId: "1.000001", SlackEventId: event,
			SlackPromptAuthorId: "alice", SlackPromptMessageId: "1.000001", PromptText: storetest.Pointer("native SQLite graph probe"), GatewayRequestId: event,
			GatewayExecutionState: "pending", CreatedAt: now, GatewayExecutionUpdatedAt: now, SlackConversationType: "channel",
			SlackProgressState: "ready", SlackProgressMode: "message", GatewayStatusUpdatedAt: now,
			SlackOutgoingMessages: []*write.SlackOutgoingMessage{
				{SlackMessageIndex: 0, SlackMessageText: storetest.Pointer("first"), SlackMessageKind: "response", SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now, SlackMessageFormat: "plain"},
				{SlackMessageIndex: 1, SlackMessageText: storetest.Pointer("second"), SlackMessageKind: "response", SlackDeliveryState: "pending", CreatedAt: now, UpdatedAt: now, SlackMessageFormat: "plain"},
			},
		}
	}
	assertAbsent := func(request string) {
		t.Helper()
		var count int
		must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM gateway_job WHERE gateway_request_id=?", request).Scan(&count))
		if count != 0 {
			t.Fatalf("unexpected persisted request %s", request)
		}
	}

	row := newJob("committed")
	created, err := invoke(ctx, &PilotInput{Action: "write", GatewayJob: row})
	must(t, err)
	if created.ID <= 0 || row.Id != created.ID || row.SlackOutgoingMessages[0].JobId != row.Id || row.SlackOutgoingMessages[1].JobId != row.Id || row.SlackOutgoingMessages[0].Id <= 0 || row.SlackOutgoingMessages[1].Id <= 0 {
		t.Fatalf("native SQLite identity/FK propagation failed: %+v", row)
	}
	var count int
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM slack_outgoing_message WHERE job_id=?", row.Id).Scan(&count))
	if count != 2 {
		t.Fatalf("outbox count=%d", count)
	}
	must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM gateway_job WHERE id=?", row.Id).Scan(&count))
	if count != 1 {
		t.Fatalf("request status count=%d", count)
	}

	patch := &write.GatewayJob{}
	patch.SetId(row.Id)
	patch.SetGatewayExecutionReason("updated through common graph")
	_, err = invoke(ctx, &PilotInput{Action: "write", GatewayJob: patch, ExpectedState: "pending"})
	must(t, err)
	var reason string
	must(t, db.QueryRowContext(ctx, "SELECT gateway_execution_reason FROM gateway_job WHERE id=?", row.Id).Scan(&reason))
	if reason != "updated through common graph" {
		t.Fatalf("updated reason=%q", reason)
	}
	patch.SetGatewayExecutionState("rejected")
	_, err = invoke(ctx, &PilotInput{Action: "write", GatewayJob: patch, ExpectedState: "needs_review"})
	if err == nil {
		t.Fatal("wrong expected state accepted")
	}

	rolled := newJob("rolled-back")
	_, err = invoke(ctx, &PilotInput{Action: "write", GatewayJob: rolled, Fail: true})
	if err == nil {
		t.Fatal("late root failure unexpectedly succeeded")
	}
	assertAbsent(rolled.GatewayRequestId)
	for _, table := range []string{"slack_outgoing_message"} {
		must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE job_id=?", rolled.Id).Scan(&count))
		if count != 0 {
			t.Fatalf("%s persisted after rollback", table)
		}
	}

	// Fail during the second actual child INSERT, after earlier writes have run.
	_, err = db.ExecContext(ctx, `CREATE TRIGGER reject_second_probe_part BEFORE INSERT ON slack_outgoing_message
WHEN NEW.slack_message_text='fail second child' BEGIN SELECT RAISE(ABORT,'injected late child failure'); END`)
	must(t, err)
	late := newJob("late-sql-failure")
	late.SlackOutgoingMessages[1].SlackMessageText = storetest.Pointer("fail second child")
	_, err = invoke(ctx, &PilotInput{Action: "write", GatewayJob: late})
	if err == nil || !strings.Contains(err.Error(), "injected late child failure") {
		t.Fatalf("late child failure error=%v", err)
	}
	assertAbsent(late.GatewayRequestId)
	for _, table := range []string{"slack_outgoing_message"} {
		must(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE job_id=?", late.Id).Scan(&count))
		if count != 0 {
			t.Fatalf("%s persisted after late child failure", table)
		}
	}
	_, err = db.ExecContext(ctx, "DROP TRIGGER reject_second_probe_part")
	must(t, err)

	canceled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	_, err = invoke(canceled, &PilotInput{Action: "write", GatewayJob: newJob("canceled")})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled invocation error=%v", err)
	}
	assertAbsent("canceled")

	var actors sync.WaitGroup
	claims := make(chan *PilotOutput, 2)
	errors := make(chan error, 2)
	for range 2 {
		actors.Add(1)
		go func() {
			defer actors.Done()
			claim, err := invoke(ctx, &PilotInput{Action: "claim", ID: row.Id})
			claims <- claim
			errors <- err
		}()
	}
	actors.Wait()
	close(claims)
	close(errors)
	for err := range errors {
		must(t, err)
	}
	claimed := 0
	for result := range claims {
		if result.ID != 0 {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("exclusive SQLite claims=%d", claimed)
	}
	var attempts int
	must(t, db.QueryRowContext(ctx, "SELECT gateway_execution_claim_count FROM gateway_job WHERE id=?", row.Id).Scan(&attempts))
	if attempts != 1 {
		t.Fatalf("claim attempts=%d", attempts)
	}
}
