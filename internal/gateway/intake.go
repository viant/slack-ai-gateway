package gateway

import (
	"context"
	"fmt"
	"strings"
)

type Intake struct {
	Store      Store
	StatusMode StatusMode
	Route      Route
}

// Route is a local allowlist, independent of conversation authorization. A
// future per-user policy/IdP must never be called on the transport ACK path.
type Route struct {
	AppID, WorkspaceID, ChannelID string
	Resolver                      *ChannelPolicyResolver
}

func (r Route) AllowsChannel(workspace, channel string) bool {
	if r.AppID == "" || r.WorkspaceID == "" || workspace != r.WorkspaceID || channel == "" {
		return false
	}
	if r.Resolver != nil {
		p := r.Resolver.Resolve(channel)
		return p.RouteAllowed && p.Present
	}
	return r.ChannelID != "" && channel == r.ChannelID
}

func (r Route) Allows(m Message) bool {
	if m.AppID == "" || m.AppID != r.AppID || m.WorkspaceID == "" || m.WorkspaceID != r.WorkspaceID || m.ChannelID == "" {
		return false
	}
	switch m.MessageSurface() {
	case SurfaceChannel:
		return r.AllowsChannel(m.WorkspaceID, m.ChannelID)
	case SurfaceDirectMessage:
		return r.Resolver != nil && r.Resolver.ResolveDirectMessages().Present
	default:
		return false
	}
}

func (i Intake) Accept(ctx context.Context, m Message) (AcceptResult, error) {
	if !i.Route.Allows(m) || (m.MessageSurface() == SurfaceChannel && !m.Mentioned) || m.Bot || m.Edited || m.Deleted || m.ExternalShared {
		return AcceptResult{Ignored: true}, nil
	}
	if m.AppID == "" || m.EventID == "" || m.WorkspaceID == "" || m.ChannelID == "" ||
		m.RootTS == "" || m.MessageTS == "" || m.AuthorID == "" {
		return AcceptResult{}, fmt.Errorf("incomplete normalized event")
	}
	m.Text = strings.TrimSpace(m.Text)
	m.StatusMode = i.StatusMode
	inserted, err := i.Store.Enqueue(ctx, m)
	if err != nil {
		return AcceptResult{}, err
	}
	return AcceptResult{Accepted: true, Duplicate: !inserted}, nil
}
