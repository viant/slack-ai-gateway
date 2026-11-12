package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sdk "github.com/slack-go/slack"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type dmAccessDirectory struct{}

func (dmAccessDirectory) Channel(_ context.Context, workspace, id string, _ bool) (gateway.ChannelInfo, error) {
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: id, IM: true, PeerID: "U1"}, nil
}

func (dmAccessDirectory) User(_ context.Context, workspace, id string) (gateway.UserInfo, error) {
	return gateway.UserInfo{WorkspaceID: workspace, ID: id}, nil
}

type dmAccessBackend struct{ creates, queries int }

func (b *dmAccessBackend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	if principal != "shared" {
		return nil, fmt.Errorf("unexpected execution identity")
	}
	return b, nil
}

func (b *dmAccessBackend) CreateConversation(context.Context, string) (string, error) {
	b.creates++
	return "dm-access-conversation", nil
}

func (b *dmAccessBackend) Query(_ context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	b.queries++
	return gateway.Turn{ID: q.MessageID, State: "completed", Content: strings.Repeat("A bakery story. ", 2200)}, nil
}

func (*dmAccessBackend) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{}, fmt.Errorf("completed DM query must not be replayed")
}

type surfaceCheckingSender struct {
	t      *testing.T
	sender *Sender
	parts  int
}

func (s *surfaceCheckingSender) Send(ctx context.Context, message gateway.SlackOutgoingMessage) (string, error) {
	if message.Surface != gateway.SurfaceDirectMessage {
		s.t.Fatalf("persisted DM was sent as %q", message.Surface)
	}
	s.parts++
	return s.sender.Send(ctx, message)
}

// Exercise native SQLite recovery, the real policy, and the pinned Slack SDK.
// Channel routing is disabled: a channel-only transport guard must fail this test.
func TestDMOnlyDeliveryAndAllProgressModesAfterRestart(t *testing.T) {
	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "dm.sqlite")
			root, err := sqlite.Open(ctx, path, true)
			storetest.Must(t, err)
			t.Cleanup(func() { _ = root.Close() })
			storetest.Must(t, root.Migrate(ctx))
			integrationID := storetest.RegisterDefault(t, root)
			var s storage.Store = root.Scope(integrationID)
			route := dmRoute(t)
			policy := gateway.ConfiguredPolicy{Route: route, PrincipalID: "shared", Directory: dmAccessDirectory{}}
			m, reason := normalize(dmPayload(nil), route, "UBOT")
			if reason != "" {
				t.Fatal(reason)
			}
			accepted, err := (gateway.Intake{Store: s, Route: route, StatusMode: mode}).Accept(ctx, m)
			storetest.Must(t, err)
			if !accepted.Accepted {
				t.Fatal("DM-only intake was disabled by channel routing")
			}

			type call struct{ path, state, blocks string }
			var mu sync.Mutex
			var calls []call
			sequence := 0
			server := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Error(err)
					out.WriteHeader(http.StatusBadRequest)
					return
				}
				channel := r.Form.Get("channel")
				if r.URL.Path == "/agents.sessions.setStatus" {
					channel = r.Form.Get("channel_id")
				}
				if channel != m.ChannelID || (r.URL.Path != "/chat.update" && r.Form.Get("thread_ts") != m.RootTS) {
					t.Error("SDK lost the original DM destination")
				}
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, call{r.URL.Path, r.Form.Get("status"), r.Form.Get("blocks")})
				out.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/agents.sessions.setStatus" {
					fmt.Fprintf(out, `{"ok":true,"agent_status":%q,"status":%q}`, r.Form.Get("status"), r.Form.Get("status"))
					return
				}
				if r.URL.Path != "/chat.postMessage" && r.URL.Path != "/chat.update" {
					t.Errorf("unexpected SDK path %s", r.URL.Path)
				}
				sequence++
				ts := fmt.Sprintf("2.%06d", sequence)
				if r.URL.Path == "/chat.update" {
					ts = r.Form.Get("ts")
				}
				fmt.Fprintf(out, `{"ok":true,"channel":%q,"ts":%q}`, channel, ts)
			}))
			defer server.Close()
			sender := &Sender{api: sdk.New("xoxb-test", sdk.OptionAPIURL(server.URL+"/"), sdk.OptionHTTPClient(server.Client())), route: route}
			now := time.Now().Add(time.Second)
			status := gateway.StatusWorker{Store: s, Sender: sender, Policy: policy, Now: func() time.Time { return now }}
			drainStatus := func() {
				t.Helper()
				for range 8 {
					worked, err := status.Step(ctx, "urgent")
					storetest.Must(t, err)
					if !worked {
						return
					}
				}
				t.Fatal("urgent statuses did not settle")
			}
			drainStatus()
			backend := &dmAccessBackend{}
			worker := gateway.Worker{Store: s, Clients: backend, Policy: policy, AgentID: "steward"}
			worked, err := worker.Step(ctx)
			storetest.Must(t, err)
			if !worked {
				t.Fatal("missing accepted DM query")
			}
			// Retain the same assignment and persisted surface across reopening.
			storetest.Must(t, root.Close())
			root, err = sqlite.Open(ctx, path, false)
			storetest.Must(t, err)
			storetest.Must(t, root.CheckSchema(ctx))
			storetest.Must(t, root.Recover(ctx))
			s = root.Scope(integrationID)
			worker.Store, status.Store = s, s
			worked, err = worker.Step(ctx)
			storetest.Must(t, err)
			if worked || backend.creates != 1 || backend.queries != 1 {
				t.Fatal("restart replayed a completed DM query")
			}
			checking := &surfaceCheckingSender{t: t, sender: sender}
			delivery := gateway.DeliveryWorker{Store: s, Sender: checking, Policy: policy}
			for range 20 {
				worked, err := delivery.Step(ctx)
				storetest.Must(t, err)
				if !worked {
					break
				}
			}
			if checking.parts < 2 {
				t.Fatal("test did not deliver a multipart DM response")
			}
			drainStatus()
			mu.Lock()
			defer mu.Unlock()
			var posts, updates, processing, active, answerParts int
			for _, c := range calls {
				switch c.path {
				case "/chat.postMessage":
					if c.blocks == "" {
						posts++
					} else {
						var blocks []struct{ Type, Text string }
						storetest.Must(t, json.Unmarshal([]byte(c.blocks), &blocks))
						if len(blocks) != 1 || blocks[0].Type != "markdown" || !strings.Contains(blocks[0].Text, "A bakery story.") {
							t.Fatal("final DM content was not delivered through the SDK")
						}
						answerParts++
					}
				case "/chat.update":
					updates++
				case "/agents.sessions.setStatus":
					if c.state == "processing" {
						processing++
					}
					if c.state == "active" {
						if answerParts != checking.parts {
							t.Fatal("native session became active before all parts were delivered")
						}
						active++
					}
				}
			}
			if mode != gateway.StatusAgentSession && (posts != 1 || updates == 0) {
				t.Fatalf("missing first/update DM progress: posts=%d updates=%d", posts, updates)
			}
			if mode == gateway.StatusAgentSession && (posts != 0 || updates != 0) {
				t.Fatal("native-only mode created a progress message")
			}
			if mode != gateway.StatusMessage && (processing == 0 || active == 0) {
				t.Fatal("DM native lifecycle was not completed")
			}
		})
	}
}

func TestDisabledDMSenderRetainsPreparedUserPolicyWithoutPublishing(t *testing.T) {
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{}, gateway.AccessRules{
		DirectMessages: &gateway.DirectMessageRule{Enabled: false, Users: &gateway.UserRule{Scope: "workspace_members_only"}},
	})
	storetest.Must(t, err)
	api := &failingPost{err: fmt.Errorf("must not be called")}
	sender := &Sender{api: api, route: gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: r}}
	key := gateway.ThreadKey{WorkspaceID: "T", ChannelID: "D", RootTS: "1.000001"}
	if _, err := sender.Send(context.Background(), gateway.SlackOutgoingMessage{ThreadKey: key, Surface: gateway.SurfaceDirectMessage, Text: "reply", Format: gateway.ReplyPlain}); err == nil || api.calls != 0 {
		t.Fatal("disabled DM published an answer")
	}
	for _, ts := range []string{"", "2.000001"} {
		if _, err := sender.SendStatus(context.Background(), gateway.GatewayJobProgress{ThreadKey: key, Surface: gateway.SurfaceDirectMessage, SlackTS: ts}, "status"); err == nil || api.calls != 0 {
			t.Fatal("disabled DM published message progress")
		}
	}
}
