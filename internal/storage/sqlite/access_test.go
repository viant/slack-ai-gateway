package sqlite

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
	s := fixture(t).Store.(*Store)
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
	if origin.Message.AuthorID != "alice" || origin.Message.Text != "" || origin.Binding.ID != b.ID {
		t.Fatal("origin was synthesized or text loaded")
	}
	for _, mutate := range []func(*gateway.Delivery){
		func(d *gateway.Delivery) { d.ChannelID = "OTHER" }, func(d *gateway.Delivery) { d.WorkspaceID = "OTHER" },
		func(d *gateway.Delivery) { d.RootTS = "OTHER" }, func(d *gateway.Delivery) { d.JobID++ }, func(d *gateway.Delivery) { d.ID++ },
	} {
		forged := *d
		mutate(&forged)
		if _, err := s.GetDeliveryContext(ctx, forged); !errors.Is(err, gateway.ErrConflict) {
			t.Fatalf("forged origin: %v", err)
		}
	}
	_, err = fixtureDB(t, s).Exec("UPDATE conversation_bindings SET owner_id='OTHER' WHERE id=?", b.ID)
	storetest.Must(t, err)
	if _, err := s.GetDeliveryContext(ctx, *d); !errors.Is(err, gateway.ErrConflict) {
		t.Fatal("inconsistent binding trusted")
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
