package datly_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// A local reply to a new prompt in a closed thread must not take over the
// author/principal used to authorize cleanup of its historical native session.
func TestNativeCleanupKeepsOriginalSourceBeforeStaleFollowup(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, _ := sharedFixture(t, driver)
			original := testIdentity("steward", "A")
			registerNativeIntegrations(t, root, &original)
			old := root.Scope(original.ID)
			now := time.Now()
			alice := storetest.Message("native-source-alice", "100.000001", "alice")
			alice.StatusMode = gateway.StatusAgentSession
			_, err := old.Enqueue(ctx, alice)
			storetest.Must(t, err)
			initial := claimSharedSession(t, old, original.ID, "processing", now)
			acknowledgeSharedSession(t, old, initial, now)
			aliceJob := startSharedSessionJob(t, old)

			changed := original
			changed.BackendAgentID, changed.PrincipalID = "new-agent", "new-principal"
			registerNativeIntegrations(t, root, &changed)
			current := root.Scope(changed.ID)
			bob := storetest.Message("native-source-bob", alice.RootTS, "bob")
			bob.MessageTS, bob.StatusMode = "100.000002", gateway.StatusAgentSession
			_, err = current.Enqueue(ctx, bob)
			storetest.Must(t, err)
			if job, err := current.ClaimJob(ctx); err != nil || job != nil {
				t.Fatalf("stale follow-up resumed backend work: %+v %v", job, err)
			}

			cleanup, err := current.ClaimStatus(ctx, now.Add(time.Second), "integration_terminal")
			storetest.Must(t, err)
			if cleanup == nil || cleanup.Target != gateway.StatusTargetAgentSession || cleanup.SessionState != "active" || !cleanup.Terminal {
				t.Fatalf("missing native cleanup: %+v", cleanup)
			}
			if cleanup.JobID != aliceJob.ID || cleanup.InitiatorUserID != alice.AuthorID {
				t.Errorf("stale follow-up replaced historical cleanup source: job=%d initiator=%q, want original job=%d author=%q", cleanup.JobID, cleanup.InitiatorUserID, aliceJob.ID, alice.AuthorID)
			}
			metadata, err := current.GetStatusContext(ctx, *cleanup)
			storetest.Must(t, err)
			if metadata.Message.AuthorID != alice.AuthorID || metadata.IntegrationActive || metadata.Kind != gateway.DeliveryIntegrationNotice || metadata.IntegrationPrincipalID != original.PrincipalID || metadata.ConversationBinding == nil || metadata.ConversationBinding.PrincipalID != original.PrincipalID {
				t.Errorf("native cleanup lost its original execution context: %+v", metadata)
			}

			scope := "workspace_members_and_guests"
			denied := []string{alice.AuthorID}
			resolver, err := gateway.NewChannelPolicyResolver(gateway.ChannelScope{Enabled: true, Scope: "selected", Allowlist: []string{alice.ChannelID}}, gateway.AccessRules{Users: &gateway.UserRule{Scope: scope, Denylist: denied}})
			storetest.Must(t, err)
			policy := gateway.ConfiguredPolicy{Route: gateway.Route{AppID: changed.AppID, WorkspaceID: changed.WorkspaceID, Resolver: resolver}, PrincipalID: changed.PrincipalID, Directory: sessionSourceDirectory{}}
			if _, err := policy.Authorize(ctx, bob, nil); err != nil {
				t.Fatalf("control follow-up author should be allowed: %v", err)
			}
			if err := policy.AuthorizeDelivery(ctx, *metadata, true); !errors.Is(err, gateway.ErrDenied) {
				t.Errorf("cleanup borrowed the allowed follow-up author after original author was removed: %v", err)
			}
		})
	}
}

type sessionSourceDirectory struct{}

func (sessionSourceDirectory) Channel(_ context.Context, workspace, id string, _ bool) (gateway.ChannelInfo, error) {
	return gateway.ChannelInfo{WorkspaceID: workspace, ID: id, Member: true}, nil
}

func (sessionSourceDirectory) User(_ context.Context, workspace, id string) (gateway.UserInfo, error) {
	return gateway.UserInfo{WorkspaceID: workspace, ID: id}, nil
}
