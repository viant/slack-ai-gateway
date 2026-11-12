package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type Intake struct {
	Store      Store
	StatusMode StatusMode
	Route      Route
	Backend    BackendLifecycle
}

// Route is a local allowlist, independent of conversation authorization. A
// future per-user policy/IdP must never be called on the transport ACK path.
type Route struct {
	AppID, WorkspaceID string
	Resolver           *ChannelPolicyResolver
}

func (r Route) AllowsChannel(workspace, channel string) bool {
	if r.AppID == "" || r.WorkspaceID == "" || workspace != r.WorkspaceID || channel == "" {
		return false
	}
	if r.Resolver != nil {
		p := r.Resolver.Resolve(channel)
		return p.RouteAllowed && p.Present
	}
	return false
}

func (r Route) Allows(m Message) bool {
	if m.AppID == "" || m.AppID != r.AppID || m.WorkspaceID == "" || m.WorkspaceID != r.WorkspaceID || m.ChannelID == "" {
		return false
	}
	switch m.MessageSurface() {
	case SurfaceChannel:
		return r.AllowsChannel(m.WorkspaceID, m.ChannelID)
	case SurfaceDirectMessage:
		if r.Resolver != nil {
			p := r.Resolver.ResolveDirectMessages()
			return p.Enabled && p.RouteAllowed && p.Present
		}
		return false
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
	var inserted bool
	enqueue := func() error {
		var err error
		inserted, err = i.Store.Enqueue(ctx, m)
		return err
	}
	var err error
	if i.Backend == nil {
		err = enqueue()
	} else {
		// Admission and the outage sweep share the manager's readiness lock.
		// A ready snapshot alone could otherwise enqueue work after that sweep.
		err = i.Backend.WithReady(enqueue)
		if errors.Is(err, ErrBackendUnavailable) {
			m.BackendReason = BackendReason(i.Backend.State())
			if m.BackendReason == "" {
				// This question arrived before recovery completed. Preserve its
				// local disposition even if the connection is now ready.
				m.BackendReason = "backend_unavailable"
			}
			err = enqueue()
		}
	}
	if err != nil {
		return AcceptResult{}, err
	}
	return AcceptResult{Accepted: true, Duplicate: !inserted}, nil
}
