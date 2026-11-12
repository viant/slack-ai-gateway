package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/scy"
	"github.com/viant/slack-ai-gateway/internal/config"
	"github.com/viant/slack-ai-gateway/internal/gateway"
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
	path := os.Getenv("SLACK_AI_GATEWAY_CONFIG")
	if path == "" {
		path = "testdata/channels.yaml"
	}
	c, err := loadSmokeConfig(path, os.Getenv("SLACK_AI_GATEWAY_INTEGRATION"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := smokePolicy(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Smoke configuration valid: routing=%s, %d explicit channel IDs. No tokens, database, Slack or Agently calls.", p.Route.Resolver.Scope().Scope, len(p.Route.Resolver.ChannelIDs()))
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

// Select one public integration without authenticating or initializing its
// configured backend. Only the live harness substitutes the test backend.
func loadSmokeConfig(path, selected string) (config.Config, error) {
	root, err := config.Load(path)
	if err != nil {
		return config.Config{}, err
	}
	if err := root.ValidateIntegrationNames(); err != nil {
		return config.Config{}, err
	}
	var chosen *config.Integration
	for index := range root.Integrations {
		integration := &root.Integrations[index]
		if selected != "" && integration.Name != selected {
			continue
		}
		if chosen != nil {
			return config.Config{}, fmt.Errorf("select exactly one smoke integration with SLACK_AI_GATEWAY_INTEGRATION")
		}
		chosen = integration
	}
	if chosen == nil {
		return config.Config{}, fmt.Errorf("no matching smoke integration")
	}
	return config.Config{Storage: root.Storage, Transport: chosen.Transport, Access: chosen.Access, Slack: chosen.Slack}, nil
}

func smokeConfig(t *testing.T, body string) config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "smoke.yaml")
	body = "storage: {driver: sqlite, path: slack-smoke.sqlite}\nintegrations:\n  - name: smoke\n    transport: socket\n    " + strings.ReplaceAll(strings.TrimSuffix(body, "\n"), "\n", "\n    ") + "\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := loadSmokeConfig(path, "smoke")
	if err != nil {
		t.Fatal(err)
	}
	c.Slack.AppTokenSecret = &scy.Resource{URL: "file:///unused/app-token.enc", Key: "blowfish://default"}
	c.Slack.BotTokenSecret = &scy.Resource{URL: "file:///unused/bot-token.enc", Key: "blowfish://default"}
	return c
}

func TestSmokeDirectMessageUsesNormalPolicyWithoutMention(t *testing.T) {
	c := smokeConfig(t, "slack: {app_id: A, workspace_id: T}\naccess: {channels: {enabled: false}, direct_messages: {enabled: true, users: {scope: workspace_members_and_guests}}}\n")
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
	if err = p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m, ConversationBinding: &gateway.ConversationBinding{ThreadKey: m.ThreadKey, Decision: decision}}, true); err != nil {
		t.Fatal(err)
	}
	if d.channels != 2 || d.users != 2 || !d.fresh {
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
	body := "slack: {app_id: A, workspace_id: T}\naccess: {channels: {enabled: true, scope: selected, allowlist: [], users: {scope: workspace_members_only}, channel_overrides: {C: {enabled: true, users: {scope: workspace_members_and_guests}}}}}\n"
	d := &smokeDirectory{guest: true}
	p, err := smokePolicy(smokeConfig(t, body), d)
	if err != nil {
		t.Fatal(err)
	}
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.0"}, AppID: "A", AuthorID: "U", Mentioned: true}
	decision, err := p.Authorize(context.Background(), m, nil)
	if err != nil || decision.PrincipalID != smokePrincipal || decision.OwnerID != smokePrincipal {
		t.Fatalf("authorization=%+v error=%v", decision, err)
	}
	b := &gateway.ConversationBinding{ThreadKey: m.ThreadKey, Decision: decision}
	if err = p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m, ConversationBinding: b}, true); err != nil {
		t.Fatal(err)
	}
	if d.channels != 2 || d.users != 2 || !d.fresh {
		t.Fatalf("directory calls: %+v", d)
	}
	m.ChannelID = "OTHER"
	if _, err = p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrDenied) || d.channels != 2 {
		t.Fatal("unconfigured channel must be denied before directory lookup")
	}
}

func TestSmokePolicyDoesNotBypassRolesOrMissingMetadata(t *testing.T) {
	c := smokeConfig(t, "slack: {app_id: A, workspace_id: T}\naccess: {channels: {enabled: true, scope: workspace_channels_with_bot, users: {scope: workspace_members_only, denylist: [BLOCKED]}}}\n")
	d := &smokeDirectory{guest: true}
	p, err := smokePolicy(c, d)
	if err != nil {
		t.Fatal(err)
	}
	m := gateway.Message{ThreadKey: gateway.ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1.0"}, AppID: "A", AuthorID: "U", Mentioned: true}
	if _, err := p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrDenied) || d.users != 1 {
		t.Fatal("workspace_members_only must reject a guest using directory facts")
	}
	d.guest = false
	if _, err := p.Authorize(context.Background(), m, nil); err != nil {
		t.Fatal(err)
	}
	m.AuthorID = "BLOCKED"
	before := d.channels
	if err := p.AuthorizeDelivery(context.Background(), gateway.DeliveryContext{Message: m}, true); !errors.Is(err, gateway.ErrDenied) || d.channels != before {
		t.Fatal("outbound must enforce selected user rule denial of the original author")
	}
	m.AuthorID = "U"
	p.Directory = nil
	if _, err := p.Authorize(context.Background(), m, nil); !errors.Is(err, gateway.ErrMetadataUnavailable) {
		t.Fatal("smoke must not bypass missing directory metadata")
	}
	c.Storage.Path = filepath.Join(t.TempDir(), "gateway.sqlite")
	if _, err := smokePolicy(c, d); err == nil {
		t.Fatal("smoke must reject the real integration database")
	}
}

func TestSmokeFixturesUsePublicIntegrationFormat(t *testing.T) {
	for _, name := range []string{"channels", "direct-messages"} {
		t.Run(name, func(t *testing.T) {
			c, err := loadSmokeConfig(filepath.Join("testdata", name+".yaml"), "smoke")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = smokePolicy(c, nil); err != nil {
				t.Fatal(err)
			}
			route, err := c.Route()
			if err != nil {
				t.Fatal(err)
			}
			if route.Resolver.ResolveDirectMessages().RouteAllowed != (name == "direct-messages") {
				t.Fatal("fixture selected the wrong surface")
			}
		})
	}
}

func TestSmokeConfigRequiresUnambiguousIntegrationSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "smoke.yaml")
	body := "storage: {driver: sqlite, path: slack-smoke.sqlite}\nintegrations:\n  - {name: first, transport: socket, slack: {app_id: A, workspace_id: T}}\n  - {name: second, transport: socket, slack: {app_id: B, workspace_id: T}}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSmokeConfig(path, ""); err == nil {
		t.Fatal("ambiguous smoke integration accepted")
	}
	if _, err := loadSmokeConfig(path, "unknown"); err == nil {
		t.Fatal("unknown smoke integration accepted")
	}
	c, err := loadSmokeConfig(path, "second")
	if err != nil || c.Slack.AppID != "B" {
		t.Fatalf("incorrect selected integration: %v", err)
	}
}
