package workflow

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/handler/provider"
	identity "github.com/viant/slack-ai-gateway/internal/datly/operations/check"
	"github.com/viant/slack-ai-gateway/internal/datly/operations/notbefore"
	snapshot "github.com/viant/slack-ai-gateway/internal/datly/operations/status"
	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
)

func (x *execution) operations(ctx context.Context, in *Input, out *Output) error {
	switch in.Action {
	case "status":
		result, err := x.operatorStatus(ctx, in.Limit)
		out.Snapshot = result
		return err
	case "check_bindings":
		return x.checkBindings(ctx, in.Route, in.Principal)
	case "not_before":
		result, err := x.notBefore(ctx, in.Key.WorkspaceID)
		out.NotBefore = result
		return err
	default:
		return gateway.ErrConflict
	}
}

func (x *execution) checkBindings(ctx context.Context, route gateway.Route, principal string) error {
	in := &identity.Input{}
	in.SetAppID(route.AppID)
	in.SetWorkspaceID(route.WorkspaceID)
	in.SetChannelID(route.ChannelID)
	in.SetFixedChannel(route.Resolver == nil)
	in.SetPrincipal(principal)
	value, err := x.invoke.Invoke(ctx, handler.ComponentRequest{
		Target: handler.ComponentTarget{
			Scope: reflect.TypeFor[identity.ReaderComponent]().PkgPath(), Name: "reader",
			Method: "GET", Path: "/internal/gateway/operations/check",
		}, Input: in,
	})
	if err != nil {
		return fmt.Errorf("Gateway database contains incompatible event routing or cannot be verified")
	}
	result, ok := value.(*identity.Output)
	if !ok || result == nil || len(result.Data) != 1 || result.Data[0] == nil ||
		result.Data[0].IncompatibleJobs == nil || result.Data[0].IncompatibleBindings == nil {
		return fmt.Errorf("cannot verify Gateway database identity")
	}
	if *result.Data[0].IncompatibleJobs != 0 {
		return fmt.Errorf("Gateway database contains incompatible event routing or cannot be verified")
	}
	if *result.Data[0].IncompatibleBindings != 0 {
		return fmt.Errorf("Gateway database contains smoke or incompatible conversation bindings; use a separate real-integration database")
	}
	return nil
}

func (x *execution) notBefore(ctx context.Context, workspace string) (time.Time, error) {
	in := &notbefore.Input{}
	in.SetWorkspaceID(workspace)
	value, err := x.invoke.Invoke(ctx, handler.ComponentRequest{
		Target: handler.ComponentTarget{
			Scope: reflect.TypeFor[notbefore.ReaderComponent]().PkgPath(), Name: "reader",
			Method: "GET", Path: "/internal/gateway/operations/notbefore",
		}, Input: in,
	})
	if err != nil {
		return time.Time{}, err
	}
	result, ok := value.(*notbefore.Output)
	if !ok || result == nil || len(result.Data) != 1 || result.Data[0] == nil {
		return time.Time{}, fmt.Errorf("unexpected storage cooldown result %T", value)
	}
	return time.UnixMilli(int64(result.Data[0].NotBefore)), nil
}

func (x *execution) operatorStatus(ctx context.Context, limit int) (*storage.Status, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("status limit must be between 1 and 1000")
	}
	// Trusted bound Input intentionally skips transport-side selector binding.
	// Supply the native invocation capability explicitly; the reader owns its
	// policy and applies each limit before loading the corresponding collection.
	selectors := state.Selectors{}
	for _, view := range []string{"jobs", "bindings", "deliveries", "statuses", "sessions"} {
		selectors = append(selectors, &state.NamedSelector{Name: view, Selector: state.Selector{Limit: limit}})
	}
	value, err := x.invoke.Invoke(ctx, handler.ComponentRequest{
		Target: handler.ComponentTarget{
			Scope: reflect.TypeFor[snapshot.ReaderComponent]().PkgPath(), Name: "reader",
			Method: "GET", Path: "/internal/gateway/operations/status",
		}, Input: &snapshot.Input{}, Providers: []locator.Provider{provider.Static(handler.SelectorsKey, selectors)},
	})
	if err != nil {
		return nil, err
	}
	result, ok := value.(*snapshot.Output)
	if !ok || result == nil || len(result.Data) != 1 || result.Data[0] == nil {
		return nil, fmt.Errorf("unexpected storage status result %T", value)
	}
	return projectStatus(limit, result.Data[0]), nil
}

// projectStatus converts the metadata graph to the existing operator contract.
// All scoping, ordering, limits and relations have already run in Datly.
func projectStatus(limit int, s *snapshot.Snapshot) *storage.Status {
	out := &storage.Status{
		Limit: limit, Counts: make(map[string]map[string]int),
		Jobs: []storage.JobStatus{}, Bindings: []storage.BindingStatus{},
		Deliveries: []storage.DeliveryStatus{}, RequestStatuses: []storage.RequestStatusMetadata{},
		AgentSessions: []storage.AgentSessionMetadata{},
	}
	for _, table := range []string{"inbox_jobs", "conversation_bindings", "outbox_messages", "request_status", "agent_session_publications"} {
		out.Counts[table] = make(map[string]int)
	}
	for _, c := range s.Counts {
		out.Counts[c.TableName][c.State] = c.RowCount
	}
	for _, j := range s.Jobs {
		out.Jobs = append(out.Jobs, storage.JobStatus{
			ID: int64(j.Id), WorkspaceID: j.WorkspaceId, ChannelID: j.ChannelId,
			ThreadTS: j.RootThreadTs, AuthorID: j.AuthorId, BindingID: int64(j.BindingId),
			RequestID: j.RequestId, TurnID: j.TurnId, State: j.State, Reason: j.Reason,
			PollAttempts: j.PollAttempts, QueryStartedAt: int64(j.QueryStartedAt), NextAttemptAt: int64(j.NextAttemptAt),
		})
	}
	for _, b := range s.Bindings {
		out.Bindings = append(out.Bindings, storage.BindingStatus{
			ID: int64(b.Id), WorkspaceID: b.WorkspaceId, ChannelID: b.ChannelId,
			ThreadTS: b.RootThreadTs, ConversationID: b.ConversationId,
			Mode: b.AccessMode, PrincipalID: b.PrincipalId, OwnerID: b.OwnerId, State: b.State,
		})
	}
	for _, d := range s.Deliveries {
		out.Deliveries = append(out.Deliveries, storage.DeliveryStatus{
			ID: int64(d.Id), JobID: int64(d.JobId), WorkspaceID: d.WorkspaceId,
			ChannelID: d.ChannelId, ThreadTS: d.RootThreadTs, Part: d.Part,
			State: d.State, SlackTS: d.SlackTs, Attempts: d.Attempts, NextAttemptAt: int64(d.NextAttemptAt),
		})
	}
	for _, r := range s.Statuses {
		m := storage.RequestStatusMetadata{
			Mode: r.Mode, JobID: int64(r.JobId), SlackTS: r.SlackTs, Phase: r.Phase, State: r.State,
			ObservedState: r.ObservedState, ObservedAt: int64(r.ObservedAt), ObservationUncertain: r.ObservationUncertain != 0,
			Outcome: r.Outcome, Revision: int64(r.Revision), SentRevision: int64(r.SentRevision), Attempts: r.Attempts,
			NextAttemptAt: int64(r.NextAttemptAt), RefreshAt: int64(r.RefreshAt), RateLimitedUntil: int64(r.RateLimitedUntil),
			FirstSentAt: int64(r.FirstSentAt), LastSentAt: int64(r.LastSentAt), Reason: r.Reason,
		}
		if r.FirstSentAt != 0 {
			latency := max(int64(0), int64(r.FirstSentAt)-int64(r.CreatedAt))
			m.AckLatencyMS = &latency
		}
		if r.Mode == string(gateway.StatusMessage) || r.Mode == string(gateway.StatusHybrid) {
			m.Target = string(gateway.StatusTargetMessage)
		}
		out.RequestStatuses = append(out.RequestStatuses, m)
	}
	for _, a := range s.Sessions {
		out.AgentSessions = append(out.AgentSessions, storage.AgentSessionMetadata{
			Mode: a.Mode, Target: string(gateway.StatusTargetAgentSession), WorkspaceID: a.WorkspaceId,
			ChannelID: a.ChannelId, ThreadTS: a.RootThreadTs, SourceJobID: int64(a.SourceJobId),
			InitiatorUserID: a.InitiatorUserId, DesiredStatus: a.DesiredStatus, AgentStatus: a.AgentStatus,
			SessionStatus: a.SessionStatus, State: a.State, Revision: int64(a.Revision), SentRevision: int64(a.SentRevision),
			Attempts: a.Attempts, NextAttemptAt: int64(a.NextAttemptAt), RefreshAt: int64(a.RefreshAt),
			RateLimitedUntil: int64(a.RateLimitedUntil), FirstSentAt: int64(a.FirstSentAt), LastSentAt: int64(a.LastSentAt),
			Reason: a.Reason, Warning: a.Warning,
		})
	}
	return out
}
