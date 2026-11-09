package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/agently-slack-gateway/internal/config"
	"github.com/viant/agently-slack-gateway/internal/gateway"
)

const smokePrincipal = "slack-smoke-shared"

// Test-only wiring shared by offline validation and the explicitly gated live
// harness. No Agently configuration, token loading, database or API calls here.
func smokePolicy(c config.Config, directory gateway.SlackDirectory) (gateway.ConfiguredPolicy, error) {
	if err := c.ValidateSlack(); err != nil {
		return gateway.ConfiguredPolicy{}, err
	}
	if filepath.Base(c.Storage.Path) != "slack-smoke.sqlite" {
		return gateway.ConfiguredPolicy{}, fmt.Errorf("live test requires a separate database named slack-smoke.sqlite; never use the real Agently integration database")
	}
	route, err := c.Route()
	if err != nil {
		return gateway.ConfiguredPolicy{}, err
	}
	return gateway.ConfiguredPolicy{Route: route, PrincipalID: smokePrincipal, Directory: directory}, nil
}

func TestSmokeConfigOffline(t *testing.T) {
	path := os.Getenv("AGENTLY_SLACK_CONFIG")
	if path == "" {
		path = "testdata/channels.yaml"
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := smokePolicy(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Smoke configuration valid: routing=%s, %d explicit channel IDs. No tokens, database, Slack or Agently calls.", p.Route.Resolver.Scope().Mode, len(p.Route.Resolver.ChannelIDs()))
}

type smokeDirectory struct {
	channels, users int
	guest           bool
	fresh           bool
	im              bool
	peer            string
}

func (d *smokeDirectory) Channel(_ context.Context, workspace, channel string, fresh bool) (gateway.ChannelInfo, error) {
	d.channels++
	d.fresh = fresh
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: channel, Member: true, IM: d.im, PeerID: d.peer}, nil
}

func (d *smokeDirectory) User(_ context.Context, workspace, user string) (gateway.UserInfo, error) {
	d.users++
	return gateway.UserInfo{WorkspaceID: workspace, ID: user, Guest: d.guest}, nil
}

func smokeConfig(t *testing.T, body string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smoke.yaml")
	yaml := "storage: {driver: sqlite, path: slack-smoke.sqlite}\ntransport: socket\naccess_mode: shared_channel\n" + body
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSmokeDirectMessageUsesNormalPolicyWithoutMention(t *testing.T) {
	c := smokeConfig(t, "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: []}}\naccess: {direct_messages: {users: {mode: channel_members}}}\n")
	d := &smokeDirectory{im: true, peer: "U"}
	p, err := smokePolicy(c, d)
	if err != nil {
		t.Fatal(err)
	}
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "D", RootTS: "1.0"}, AppID: "A", AuthorID: "U", Surface: gateway.SurfaceDirectMessage}
	decision, err := p.Authorize(context.Background(), m, nil)
	if err != nil || decision.OwnerID != smokePrincipal {
		t.Fatalf("DM authorization: %+v %v", decision, err)
	}
	if err = p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m, Binding: &gateway.Binding{ThreadKey: m.ThreadKey, Decision: decision}}, true); err != nil {
		t.Fatal(err)
	}
	if d.channels != 2 || d.users != 0 || !d.fresh {
		t.Fatalf("unexpected DM directory calls: %+v", d)
	}
	c.Access.DirectMessages = nil
	p, err = smokePolicy(c, d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrDenied) {
		t.Fatal("smoke enabled DM implicitly")
	}
}

func TestSmokePolicyUsesCompiledRulesForQueriesAndReplies(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			body := "slack: {app_id: A, workspace_id: T, channels: {mode: selected, allowlist: [C]}}\naccess: {channel_overrides: {C: {users: {mode: channel_members}}}}\n"
			if legacy {
				body = "slack: {app_id: A, workspace_id: T, channel_id: C}\n"
			}
			d := &smokeDirectory{guest: true}
			p, err := smokePolicy(smokeConfig(t, body), d)
			if err != nil {
				t.Fatal(err)
			}
			m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.0"}, AppID: "A", AuthorID: "U", Mentioned: true}
			decision, err := p.Authorize(context.Background(), m, nil)
			if err != nil || decision.PrincipalID != smokePrincipal || decision.OwnerID != smokePrincipal {
				t.Fatalf("authorization=%+v, error=%v", decision, err)
			}
			b := &gateway.Binding{ThreadKey: m.ThreadKey, Decision: decision}
			if err := p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m, Binding: b}, true); err != nil {
				t.Fatal(err)
			}
			if d.channels != 2 || d.users != 0 || !d.fresh {
				t.Fatalf("directory calls: %+v", d)
			}
			m.ChannelID = "OTHER"
			if _, err := p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrDenied) || d.channels != 2 {
				t.Fatal("unconfigured channel must be denied before directory lookup")
			}
		})
	}
}

func TestSmokePolicyDoesNotBypassRolesOrMissingMetadata(t *testing.T) {
	c := smokeConfig(t, "slack: {app_id: A, workspace_id: T, channels: {mode: workspace}}\naccess: {defaults: {users: {mode: members_only}}, global: {users: {denylist: [BLOCKED]}}}\n")
	d := &smokeDirectory{guest: true}
	p, err := smokePolicy(c, d)
	if err != nil {
		t.Fatal(err)
	}
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.0"}, AppID: "A", AuthorID: "U", Mentioned: true}
	if _, err := p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrDenied) || d.users != 1 {
		t.Fatal("members_only must reject a guest using directory facts")
	}
	d.guest = false
	if _, err := p.Authorize(context.Background(), m, nil); err != nil {
		t.Fatal(err)
	}
	m.AuthorID = "BLOCKED"
	before := d.channels
	if err := p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m}, true); !errors.Is(err, gateway.ErrDenied) || d.channels != before {
		t.Fatal("outbound must enforce global denial of the original author")
	}
	m.AuthorID = "U"
	p.Directory = nil
	if _, err := p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrMetadataUnavailable) {
		t.Fatal("smoke must not bypass missing directory metadata")
	}
	c.Storage.Path = filepath.Join(t.TempDir(), "gateway-agently.sqlite")
	if _, err := smokePolicy(c, d); err == nil {
		t.Fatal("smoke must reject the real integration database")
	}
}
