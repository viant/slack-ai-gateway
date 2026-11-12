package slack

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	sdk "github.com/slack-go/slack"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type dmCleanupBackend struct {
	principal               string
	creates, queries, reads int
}

func (b *dmCleanupBackend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if principal != b.principal {
		return nil, fmt.Errorf("unexpected execution principal")
	}
	return b, nil
}

func (b *dmCleanupBackend) CreateConversation(_ context.Context, agent string) (string, error) {
	if agent != "steward" {
		return "", fmt.Errorf("unexpected backend agent")
	}
	b.creates++
	return "unfinished-dm-conversation", nil
}

func (b *dmCleanupBackend) Query(_ context.Context, query gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries++
	return gateway.Turn{ID: query.MessageID, State: "running"}, nil
}

func (b *dmCleanupBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	b.reads++
	return gateway.Turn{}, fmt.Errorf("closed DM execution must not be resumed")
}

// Inspect the durable context, then exercise the production policy unchanged.
// A substituted author/principal must not be hidden by a permissive test sender.
type dmCleanupPolicy struct {
	t         *testing.T
	policy    gateway.ConfiguredPolicy
	original  gateway.Message
	principal string
	calls     int
}

func (p *dmCleanupPolicy) AuthorizeDelivery(ctx context.Context, d gateway.DeliveryContext, fresh bool) error {
	p.t.Helper()
	if d.Message.ThreadKey != p.original.ThreadKey || d.Message.AuthorID != p.original.AuthorID || d.Message.Surface != gateway.SurfaceDirectMessage ||
		d.IntegrationActive || d.IntegrationPrincipalID != p.principal || d.TerminationReason != "integration_changed" || d.Kind != gateway.DeliveryIntegrationNotice ||
		d.ConversationBinding == nil || d.ConversationBinding.State != "closed" || d.ConversationBinding.PrincipalID != p.principal || d.ConversationBinding.OwnerID != p.principal {
		p.t.Fatal("DM cleanup lost its original author, surface or historical execution identity")
	}
	p.calls++
	return p.policy.AuthorizeDelivery(ctx, d, fresh)
}

// Unlike the channel lifecycle test, this runs a DM-only route through native
// SQLite reconciliation, reopening, current-policy authorization and the SDK.
func TestDMOnlyConfigurationCleanupUsesSDKAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dm-cleanup.sqlite")
	root, err := sqlite.Open(ctx, path, true)
	storetest.Must(t, err)
	t.Cleanup(func() { _ = root.Close() })
	storetest.Must(t, root.Migrate(ctx))
	original := storetest.DefaultIntegration()
	original.Name, original.PrincipalID = "steward", "old-principal"
	rows, err := root.ReconcileIntegrations(ctx, []storage.Integration{original})
	storetest.Must(t, err)
	oldID := rows[0].ID
	oldScope := root.Scope(oldID)
	route := dmRoute(t)
	oldPolicy := gateway.ConfiguredPolicy{Route: route, PrincipalID: original.PrincipalID, Directory: dmAccessDirectory{}}
	message, reason := normalize(dmPayload(nil), route, "UBOT")
	if reason != "" || route.AllowsChannel(message.WorkspaceID, message.ChannelID) {
		t.Fatal("test requires a normalized DM with channel routing disabled")
	}
	accepted, err := (gateway.Intake{Store: oldScope, Route: route, StatusMode: gateway.StatusHybrid}).Accept(ctx, message)
	storetest.Must(t, err)
	if !accepted.Accepted || accepted.Duplicate {
		t.Fatal("original DM was not admitted")
	}

	type sdkCall struct{ path, text, state string }
	var mu sync.Mutex
	var calls []sdkCall
	sequence := 0
	server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Error(err)
			out.WriteHeader(http.StatusBadRequest)
			return
		}
		channel := request.Form.Get("channel")
		if request.URL.Path == "/agents.sessions.setStatus" {
			channel = request.Form.Get("channel_id")
		}
		if channel != message.ChannelID || (request.URL.Path != "/chat.update" && request.Form.Get("thread_ts") != message.RootTS) {
			t.Error("SDK cleanup lost the original DM destination")
		}
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, sdkCall{request.URL.Path, request.Form.Get("text"), request.Form.Get("status")})
		out.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/agents.sessions.setStatus" {
			fmt.Fprintf(out, `{"ok":true,"agent_status":%q,"status":%q}`, request.Form.Get("status"), request.Form.Get("status"))
			return
		}
		if request.URL.Path != "/chat.postMessage" && request.URL.Path != "/chat.update" {
			t.Errorf("unexpected SDK path %s", request.URL.Path)
		}
		sequence++
		ts := fmt.Sprintf("2.%06d", sequence)
		if request.URL.Path == "/chat.update" {
			ts = request.Form.Get("ts")
		}
		fmt.Fprintf(out, `{"ok":true,"channel":%q,"ts":%q}`, channel, ts)
	}))
	defer server.Close()
	sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client())), route: route}
	oldStatus := &gateway.StatusWorker{Store: oldScope, Sender: sender, Policy: oldPolicy}
	for range 3 {
		worked, err := oldStatus.Step(ctx, "urgent")
		storetest.Must(t, err)
		if !worked {
			break
		}
	}
	oldBackend := &dmCleanupBackend{principal: original.PrincipalID}
	worked, err := (gateway.Worker{Store: oldScope, Policy: oldPolicy, Clients: oldBackend, AgentID: "steward"}).Step(ctx)
	storetest.Must(t, err)
	if !worked || oldBackend.creates != 1 || oldBackend.queries != 1 {
		t.Fatal("original backend execution did not begin")
	}

	storetest.Must(t, root.Close())
	root, err = sqlite.Open(ctx, path, false)
	storetest.Must(t, err)
	storetest.Must(t, root.CheckSchema(ctx))
	changed := original
	changed.PrincipalID = "new-principal"
	rows, err = root.ReconcileIntegrations(ctx, []storage.Integration{changed})
	storetest.Must(t, err)
	if len(rows) != 1 || rows[0].ID == oldID {
		t.Fatal("changed principal reused the old assignment")
	}
	current := root.Scope(rows[0].ID)
	currentPolicy := gateway.ConfiguredPolicy{Route: route, PrincipalID: changed.PrincipalID, Directory: dmAccessDirectory{}}
	intake := gateway.Intake{Store: current, Route: route, StatusMode: gateway.StatusHybrid}
	retry, err := intake.Accept(ctx, message)
	storetest.Must(t, err)
	if !retry.Duplicate {
		t.Fatal("Slack retry duplicated a prompt after reassignment")
	}
	newBackend := &dmCleanupBackend{principal: changed.PrincipalID}
	worked, err = (gateway.Worker{Store: current, Policy: currentPolicy, Clients: newBackend, AgentID: "steward"}).Step(ctx)
	storetest.Must(t, err)
	if worked || newBackend.creates != 0 || newBackend.queries != 0 || oldBackend.reads != 0 {
		t.Fatal("reassignment replayed or resumed the old prompt")
	}
	checkedPolicy := &dmCleanupPolicy{t: t, policy: currentPolicy, original: message, principal: original.PrincipalID}
	checkingSender := &surfaceCheckingSender{t: t, sender: sender}
	outbound := &gateway.OutboundWorker{
		SlackOutgoingMessage: &gateway.DeliveryWorker{Store: current, Sender: checkingSender, Policy: checkedPolicy},
		Status:               &gateway.StatusWorker{Store: current, Sender: sender, Policy: checkedPolicy},
	}
	for i := 0; ; i++ {
		worked, err := outbound.Step(ctx)
		storetest.Must(t, err)
		if !worked {
			break
		}
		if i == 9 {
			t.Fatal("DM cleanup did not settle")
		}
	}
	if checkingSender.parts != 1 || checkedPolicy.calls < 3 {
		t.Fatalf("missing authorized cleanup: notices=%d authorization_checks=%d", checkingSender.parts, checkedPolicy.calls)
	}
	state, err := root.Status(ctx, 10)
	storetest.Must(t, err)
	if len(state.GatewayJobs) != 1 || state.GatewayJobs[0].IntegrationID != oldID || state.GatewayJobs[0].AuthorID != message.AuthorID || state.GatewayJobs[0].State != "rejected" || state.GatewayJobs[0].Reason != "integration_changed" ||
		len(state.ConversationBindings) != 1 || state.ConversationBindings[0].State != "closed" || state.ConversationBindings[0].PrincipalID != original.PrincipalID ||
		len(state.SlackOutgoingMessages) != 1 || state.SlackOutgoingMessages[0].Kind != gateway.DeliveryIntegrationNotice || state.SlackOutgoingMessages[0].State != "delivered" {
		t.Fatal("native reconciliation did not close and deliver historical DM work")
	}
	mu.Lock()
	defer mu.Unlock()
	var notices, messageClosed, processing, active int
	for _, call := range calls {
		if call.path == "/chat.postMessage" && call.text == gateway.IntegrationNotice("integration_changed") {
			notices++
		}
		if call.path == "/chat.update" && strings.Contains(call.text, "configuration changed") {
			messageClosed++
		}
		if call.path == "/agents.sessions.setStatus" && call.state == "processing" {
			processing++
		}
		if call.path == "/agents.sessions.setStatus" && call.state == "active" {
			active++
		}
	}
	if notices != 1 || messageClosed != 1 || processing != 1 || active != 1 {
		t.Fatalf("SDK cleanup lifecycle incomplete: notices=%d closed_progress=%d processing=%d active=%d", notices, messageClosed, processing, active)
	}
}
