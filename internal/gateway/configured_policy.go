package gateway

import "context"

// ConfiguredPolicy keeps shared execution identity independent of the Slack
// author, role and channel-specific permission to start or continue a turn.
type ConfiguredPolicy struct {
	Route       Route
	PrincipalID string
	Directory   SlackDirectory
}

func (p ConfiguredPolicy) Authorize(ctx context.Context, m Message, b *Binding) (Decision, error) {
	return p.authorize(ctx, m, b, false)
}

func (p ConfiguredPolicy) AuthorizeDelivery(ctx context.Context, d DeliveryContext, fresh bool) error {
	_, err := p.authorize(ctx, d.Message, d.Binding, fresh)
	return err
}

func (p ConfiguredPolicy) authorize(ctx context.Context, m Message, b *Binding, fresh bool) (Decision, error) {
	if p.Route.Resolver == nil || p.PrincipalID == "" || !p.Route.Allows(m) || m.ExternalShared || m.AuthorID == "" || m.Bot || m.Edited || m.Deleted {
		return Decision{}, ErrDenied
	}
	policy := p.Route.Resolver.Resolve(m.ChannelID)
	if m.MessageSurface() == SurfaceDirectMessage {
		policy = p.Route.Resolver.ResolveDirectMessages()
	}
	if !policy.AllowsUser(m.AuthorID) {
		return Decision{}, ErrDenied
	}
	d := Decision{Mode: "shared_channel", PrincipalID: p.PrincipalID, OwnerID: p.PrincipalID}
	if b != nil && (b.ThreadKey != m.ThreadKey || b.Decision != d) {
		return Decision{}, ErrDenied
	}
	if p.Directory == nil {
		return Decision{}, ErrMetadataUnavailable
	}
	channel, err := p.Directory.Channel(ctx, m.WorkspaceID, m.ChannelID, fresh)
	if err != nil {
		return Decision{}, err
	}
	if channel.WorkspaceID != m.WorkspaceID || channel.ID != m.ChannelID || channel.MPIM || channel.ExternalShared || channel.PendingExternalShared {
		return Decision{}, ErrDenied
	}
	if m.MessageSurface() == SurfaceDirectMessage {
		if !channel.IM || channel.PeerID != m.AuthorID {
			return Decision{}, ErrDenied
		}
	} else if channel.IM || !channel.Member || channel.Archived || (channel.Private && !policy.AllowPrivate) {
		return Decision{}, ErrDenied
	}
	if policy.UserMode == "members_only" {
		user, err := p.Directory.User(ctx, m.WorkspaceID, m.AuthorID)
		if err != nil {
			return Decision{}, err
		}
		if user.ID != m.AuthorID || user.WorkspaceID != m.WorkspaceID || user.Deleted || user.Bot || user.Guest || user.SingleChannelGuest {
			return Decision{}, ErrDenied
		}
	}
	return d, nil
}
