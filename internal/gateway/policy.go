package gateway

import (
	"context"
	"fmt"
	"strings"
)

type SharedChannelPolicy struct {
	AppID       string
	WorkspaceID string
	ChannelID   string
	PrincipalID string
}

// Legacy policy remains useful for isolated/offline tests. Production runtime
// uses ConfiguredPolicy, including Slack metadata verification.
func (p SharedChannelPolicy) AuthorizeDelivery(ctx context.Context, d DeliveryContext, _ bool) error {
	_, err := p.Authorize(ctx, d.Message, d.Binding)
	return err
}

func (p SharedChannelPolicy) Authorize(_ context.Context, m Message, b *Binding) (Decision, error) {
	if strings.TrimSpace(p.AppID) == "" || strings.TrimSpace(p.WorkspaceID) == "" ||
		strings.TrimSpace(p.ChannelID) == "" || strings.TrimSpace(p.PrincipalID) == "" {
		return Decision{}, fmt.Errorf("%w: incomplete shared channel configuration", ErrDenied)
	}
	if m.AppID != p.AppID || m.WorkspaceID != p.WorkspaceID || m.ChannelID != p.ChannelID ||
		m.ExternalShared || m.AuthorID == "" {
		return Decision{}, ErrDenied
	}
	d := Decision{Mode: "shared_channel", PrincipalID: p.PrincipalID, OwnerID: p.PrincipalID}
	if b != nil && (b.ThreadKey != m.ThreadKey || b.Decision != d) {
		return Decision{}, fmt.Errorf("%w: existing conversation identity cannot change", ErrDenied)
	}
	return d, nil
}
