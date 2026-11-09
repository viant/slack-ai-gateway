package slack

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/slack-go/slack/socketmode"
	"github.com/viant/agently-slack-gateway/internal/gateway"
)

func dmRoute(t *testing.T) gateway.Route {
	t.Helper()
	mode := "channel_members"
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Mode: "selected", Allowlist: []string{"C"}}, gateway.AccessRules{DirectMessages: &gateway.UserRule{Mode: &mode}})
	if err != nil {
		t.Fatal(err)
	}
	return gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: r}
}

func dmPayload(change func(map[string]any, map[string]any)) []byte {
	return payload(func(c, e map[string]any) {
		delete(c, "is_ext_shared_channel")
		e["type"], e["channel_type"], e["channel"], e["text"] = "message", "im", "D", "hello &lt;world&gt;"
		if change != nil {
			change(c, e)
		}
	})
}

func TestDirectMessageNormalizationAndFilters(t *testing.T) {
	route := dmRoute(t)
	m, reason := normalize(dmPayload(nil), route, "UBOT")
	if reason != "" || m.Surface != gateway.SurfaceDirectMessage || m.Mentioned || m.Text != "hello <world>" || m.RootTS != m.MessageTS {
		t.Fatalf("unexpected DM: %+v %s", m, reason)
	}
	m, reason = normalize(dmPayload(func(c, e map[string]any) { e["thread_ts"] = "0.1" }), route, "UBOT")
	if reason != "" || m.RootTS != "0.1" {
		t.Fatal("thread root lost")
	}
	for _, change := range []func(map[string]any, map[string]any){
		func(c, e map[string]any) { e["channel_type"] = "mpim" },
		func(c, e map[string]any) { e["channel_type"] = "channel" },
		func(c, e map[string]any) { e["type"] = "app_home_opened" },
		func(c, e map[string]any) { e["type"] = "app_mention" },
		func(c, e map[string]any) { e["user"] = "UBOT" },
		func(c, e map[string]any) { e["bot_id"] = "BOT" },
		func(c, e map[string]any) { e["edited"] = map[string]any{"ts": "2"} },
		func(c, e map[string]any) { e["subtype"] = "message_changed" },
		func(c, e map[string]any) { e["subtype"] = "message_deleted" },
		func(c, e map[string]any) { c["team_id"] = "OTHER" },
		func(c, e map[string]any) { c["api_app_id"] = "OTHER" },
		func(c, e map[string]any) { c["is_ext_shared_channel"] = true },
		func(c, e map[string]any) { delete(e, "user") },
	} {
		if _, reason := normalize(dmPayload(change), route, "UBOT"); reason == "" {
			t.Fatal("unsupported DM accepted")
		}
	}
	if _, reason := normalize(dmPayload(nil), testRoute, "UBOT"); reason == "" {
		t.Fatal("DM enabled implicitly")
	}
}

func TestDirectMessageCommitBeforeACKAndAppOpenDoesNotQuery(t *testing.T) {
	s, route := testStore(t), dmRoute(t)
	ctx := context.Background()
	acks := 0
	source := Source{session: &fakeSession{ack: func(string) error {
		m, _ := normalize(dmPayload(nil), route, "UBOT")
		inserted, err := s.Enqueue(ctx, m)
		if err != nil || inserted {
			t.Fatal("ACK before DM commit")
		}
		acks++
		return nil
	}}, route: route, botUserID: "UBOT", log: quietLog()}
	for _, id := range []string{"first", "duplicate"} {
		if err := source.handle(ctx, socketmode.Request{Type: "events_api", EnvelopeID: id, Payload: dmPayload(nil)}, gateway.Intake{Store: s, Route: route}); err != nil {
			t.Fatal(err)
		}
	}
	if acks != 2 {
		t.Fatal("duplicate ACK missing")
	}
	j, err := s.ClaimJob(ctx)
	if err != nil || j == nil || j.Surface != gateway.SurfaceDirectMessage || j.Mentioned {
		t.Fatal("DM projection lost")
	}
	err = source.handle(ctx, socketmode.Request{Type: "events_api", EnvelopeID: "open", Payload: dmPayload(func(c, e map[string]any) { e["type"] = "app_home_opened" })}, sinkFunc(func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
		t.Fatal("open reached query intake")
		return gateway.AcceptResult{}, nil
	}))
	if err != nil || acks != 3 {
		t.Fatal("app open not ACKed")
	}
}

func TestDirectMessageDirectoryUsesNativeMinimalFields(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
		want   error
	}{
		{"minimal", nil, nil},
		{"missing_peer", func(c map[string]any) { delete(c, "user") }, gateway.ErrMetadataUnavailable},
		{"wrong_id", func(c map[string]any) { c["id"] = "OTHER" }, gateway.ErrDenied},
		{"wrong_workspace", func(c map[string]any) { c["context_team_id"] = "OTHER" }, gateway.ErrDenied},
		{"missing_im", func(c map[string]any) { delete(c, "is_im") }, gateway.ErrMetadataUnavailable},
		{"external", func(c map[string]any) { c["is_ext_shared"] = true }, gateway.ErrDenied},
		{"mpim", func(c map[string]any) { c["is_mpim"] = true }, gateway.ErrDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := map[string]any{"id": "D", "is_im": true, "user": "U1"}
			if tc.change != nil {
				tc.change(c)
			}
			d := testDirectory(t, func(w http.ResponseWriter, r *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": c})
			})
			got, err := d.Channel(context.Background(), "T", "D", true)
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
			if err == nil && (!got.IM || got.PeerID != "U1") {
				t.Fatal("DM peer lost")
			}
		})
	}
}
