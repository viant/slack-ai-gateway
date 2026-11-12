package mysql

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestDeliveryOriginRejectsForgedDestinationAndInconsistentBinding(t *testing.T) {
	ctx := context.Background()
	s, db := fixtureDB(t)
	m := storetest.Message("a", "one", "alice")
	_, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	b, err := s.BindJob(ctx, j.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	storetest.Must(t, s.Complete(ctx, j.ID, "", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
	d, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	origin, err := s.GetDeliveryContext(ctx, *d)
	storetest.Must(t, err)
	if origin.Message.AuthorID != "alice" || origin.Message.Text != "" || origin.ConversationBinding.ID != b.ID {
		t.Fatal("origin was synthesized or text loaded")
	}
	mode := "channel_members"
	resolver, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Mode: "selected", Allowlist: []string{"C"}}, gateway.AccessRules{Defaults: &gateway.UserRule{Mode: &mode}})
	storetest.Must(t, err)
	route := gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: resolver}
	directory := &deliveryIdentityDirectory{}
	policy := gateway.ConfiguredPolicy{Route: route, PrincipalID: "shared", Directory: directory}
	storetest.Must(t, s.CheckRunBindings(ctx, route, "shared"))
	storetest.Must(t, policy.AuthorizeDelivery(ctx, *origin, true))
	if directory.channelCalls != 1 {
		t.Fatal("valid original context did not reach native channel authorization")
	}
	for _, mutate := range []func(*gateway.SlackOutgoingMessage){
		func(d *gateway.SlackOutgoingMessage) { d.ChannelID = "OTHER" }, func(d *gateway.SlackOutgoingMessage) { d.WorkspaceID = "OTHER" },
		func(d *gateway.SlackOutgoingMessage) { d.RootTS = "OTHER" }, func(d *gateway.SlackOutgoingMessage) { d.JobID++ }, func(d *gateway.SlackOutgoingMessage) { d.ID++ },
	} {
		forged := *d
		mutate(&forged)
		if _, err := s.GetDeliveryContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
			t.Fatalf("forged origin: %v", err)
		}
	}
	_, err = db.Exec("UPDATE conversation_binding SET gateway_binding_owner_id='OTHER' WHERE id=?", b.ID)
	storetest.Must(t, err)
	// The binding is the sole persisted authority after normalization. Native
	// context reading preserves its changed value; the existing policy and
	// startup identity guard must refuse to authorize it for publication.
	changed, err := s.GetDeliveryContext(ctx, *d)
	storetest.Must(t, err)
	if changed.ConversationBinding == nil || changed.ConversationBinding.OwnerID != "OTHER" {
		t.Fatal("context did not reflect the authoritative binding")
	}
	if err := s.CheckRunBindings(ctx, route, "shared"); err == nil {
		t.Fatal("startup guard accepted a changed execution identity")
	}
	if err := policy.AuthorizeDelivery(ctx, *changed, true); !errors.Is(err, gateway.ErrDenied) {
		t.Fatalf("changed binding was authorized for outbound delivery: %v", err)
	}
	if directory.channelCalls != 1 {
		t.Fatal("changed identity reached downstream channel authorization")
	}
}

func TestRemovedHistoricalChannelDoesNotBlockStartupButIdentityStillDoes(t *testing.T) {
	ctx := context.Background()
	s := fixture(t).Store.(*Store)
	m := storetest.Message("a", "one", "alice")
	_, err := s.Enqueue(ctx, m)
	storetest.Must(t, err)
	j, err := s.ClaimJob(ctx)
	storetest.Must(t, err)
	_, err = s.BindJob(ctx, j.ID, gateway.Decision{Mode: "shared_channel", PrincipalID: "shared", OwnerID: "shared"})
	storetest.Must(t, err)
	mode := "channel_members"
	r, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Mode: "selected", Allowlist: []string{"NEW"}}, gateway.AccessRules{Defaults: &gateway.UserRule{Mode: &mode}})
	storetest.Must(t, err)
	route := gateway.Route{AppID: "A", WorkspaceID: "T", Resolver: r}
	before, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	storetest.Must(t, s.CheckRunBindings(ctx, route, "shared"))
	after, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("policy startup mutated history")
	}
	for _, change := range []func(*gateway.Route){func(r *gateway.Route) { r.AppID = "OTHER" }, func(r *gateway.Route) { r.WorkspaceID = "OTHER" }} {
		wrong := route
		change(&wrong)
		if s.CheckRunBindings(ctx, wrong, "shared") == nil {
			t.Fatal("foreign database accepted")
		}
	}
	if s.CheckRunBindings(ctx, route, "OTHER") == nil {
		t.Fatal("foreign principal accepted")
	}
}

// Synthetic channel metadata exercises the production policy without Slack.
type deliveryIdentityDirectory struct{ channelCalls int }

func (d *deliveryIdentityDirectory) Channel(_ context.Context, workspace, channel string, _ bool) (gateway.ChannelInfo, error) {
	d.channelCalls++
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: channel, Member: true}, nil
}

func (*deliveryIdentityDirectory) User(context.Context, string, string) (gateway.UserInfo, error) {
	return gateway.UserInfo{}, errors.New("unexpected users.info for channel_members policy")
}
