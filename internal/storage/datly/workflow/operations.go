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
	in.SetIntegrationID(int(x.scope))
	in.SetAppID(route.AppID)
	in.SetWorkspaceID(route.WorkspaceID)
	// Channel access can change without changing the recorded execution identity.
	// Historical channels therefore do not restrict this native identity check.
	in.SetChannelID("")
	in.SetFixedChannel(false)
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
	if x.identity != nil {
		in.SetIntegrationID(int(x.scope))
		in.SetAppID(x.identity.AppID)
	}
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
	if result.Data[0].NotBefore == nil {
		// Preserve the existing domain contract for an absent cooldown.
		return time.UnixMilli(0), nil
	}
	return *result.Data[0].NotBefore, nil
}

func (x *execution) operatorStatus(ctx context.Context, limit int) (*storage.Status, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("status limit must be between 1 and 1000")
	}
	// Trusted bound Input intentionally skips transport-side selector binding.
	// Supply the native invocation capability explicitly; the reader owns its
	// policy and applies each limit before loading the corresponding collection.
	selectors := state.Selectors{}
	for _, view := range []string{"gateway_jobs", "conversation_bindings", "slack_outgoing_messages", "slack_agent_session_publications"} {
		selectors = append(selectors, &state.NamedSelector{Name: view, Selector: state.Selector{Limit: limit}})
	}
	input := &snapshot.Input{}
	input.SetIntegrationID(int(x.scope))
	if x.name != "" {
		input.SetIntegrationName(x.name)
	}
	value, err := x.invoke.Invoke(ctx, handler.ComponentRequest{
		Target: handler.ComponentTarget{
			Scope: reflect.TypeFor[snapshot.ReaderComponent]().PkgPath(), Name: "reader",
			Method: "GET", Path: "/internal/gateway/operations/status",
		}, Input: input, Providers: []locator.Provider{provider.Static(handler.SelectorsKey, selectors)},
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

// projectStatus exposes the metadata graph through the operator contract.
// All scoping, ordering, limits and relations have already run in Datly.
func projectStatus(limit int, s *snapshot.Snapshot) *storage.Status {
	out := &storage.Status{
		Limit: limit, Counts: make(map[string]map[string]int),
		GatewayJobs: []storage.GatewayJobMetadata{}, ConversationBindings: []storage.ConversationBindingMetadata{},
		SlackOutgoingMessages: []storage.SlackOutgoingMessageMetadata{}, SlackAgentSessionPublications: []storage.SlackAgentSessionPublicationMetadata{},
		Integrations: []storage.IntegrationMetadata{},
	}
	for _, dimension := range []string{"gateway_job.gateway_execution_state", "gateway_job.slack_progress_state", "conversation_binding.gateway_binding_state", "slack_outgoing_message.slack_delivery_state", "slack_agent_session_publication.slack_publication_state"} {
		out.Counts[dimension] = make(map[string]int)
	}
	for _, c := range s.Counts {
		if out.Counts[c.TableName] == nil {
			out.Counts[c.TableName] = map[string]int{}
		}
		out.Counts[c.TableName][c.State] = c.RowCount
	}
	for _, i := range s.Integrations {
		out.Integrations = append(out.Integrations, storage.IntegrationMetadata{ID: int64(i.Id), Name: i.Name, Active: i.Active != 0, AppID: i.SlackAppId, WorkspaceID: i.SlackWorkspaceId, BackendType: i.BackendType, BackendAgentID: i.BackendAgentId, CreatedAt: diagnosticMillis(&i.CreatedAt), UpdatedAt: diagnosticMillis(&i.UpdatedAt)})
	}
	for _, r := range s.GatewayJobs {
		m := storage.GatewayJobMetadata{
			ID: int64(r.Id), IntegrationID: int64(r.IntegrationId), IntegrationName: r.IntegrationName, IntegrationActive: r.IntegrationActive != 0,
			WorkspaceID: r.SlackWorkspaceId, ChannelID: r.SlackChannelId, ThreadTS: r.SlackThreadRootMessageId,
			AuthorID: r.SlackPromptAuthorId, ConversationBindingID: int64(r.ConversationBindingId), GatewayRequestID: r.GatewayRequestId,
			State: r.GatewayExecutionState, Reason: r.GatewayExecutionReason, Outcome: r.GatewayExecutionOutcome,
			PollAttempts: r.BackendPollAttempts, BackendSubmissionStartedAt: diagnosticMillis(r.BackendSubmissionStartedAt), NextAttemptAt: diagnosticMillis(r.BackendNextPollAt),
			ObservedState: r.BackendObservedState, ObservedAt: diagnosticMillis(r.BackendObservedAt), ObservationUncertain: r.BackendObservationUncertain != 0,
			Mode: r.SlackProgressMode, SlackTS: r.SlackProgressMessageId, Phase: r.SlackProgressPhase, ProgressState: r.SlackProgressState, ProgressReason: r.SlackProgressReason,
			Revision: int64(r.SlackProgressRevision), SentRevision: int64(r.SlackProgressSentRevision), ProgressAttempts: r.SlackProgressAttempts,
			ProgressNextAttemptAt: diagnosticMillis(r.SlackProgressNextAttemptAt), RefreshAt: diagnosticMillis(r.SlackProgressRefreshAt), RateLimitedUntil: diagnosticMillis(r.SlackProgressRateLimitedUntil),
			FirstSentAt: diagnosticMillis(r.SlackProgressFirstSentAt), LastSentAt: diagnosticMillis(r.SlackProgressLastSentAt),
			CreatedAt: diagnosticMillis(&r.CreatedAt), ExecutionUpdatedAt: diagnosticMillis(&r.GatewayExecutionUpdatedAt), StatusUpdatedAt: diagnosticMillis(&r.GatewayStatusUpdatedAt),
			ExecutionFinishedAt: diagnosticMillis(r.GatewayExecutionFinishedAt), ContentPurgedAt: diagnosticMillis(r.ContentPurgedAt),
		}
		if r.SlackProgressFirstSentAt != nil {
			latency := max(int64(0), r.SlackProgressFirstSentAt.Sub(r.CreatedAt).Milliseconds())
			m.AckLatencyMS = &latency
		}
		if r.SlackProgressMode == string(gateway.StatusMessage) || r.SlackProgressMode == string(gateway.StatusHybrid) || r.SlackProgressMessageId != "" {
			m.Target = string(gateway.StatusTargetMessage)
		}
		out.GatewayJobs = append(out.GatewayJobs, m)
	}
	for _, b := range s.ConversationBindings {
		out.ConversationBindings = append(out.ConversationBindings, storage.ConversationBindingMetadata{
			ID: int64(b.Id), IntegrationID: int64(b.IntegrationId), IntegrationName: b.IntegrationName, IntegrationActive: b.IntegrationActive != 0, WorkspaceID: b.SlackWorkspaceId, ChannelID: b.SlackChannelId,
			ThreadTS: b.SlackThreadRootMessageId, ConversationID: b.BackendConversationId,
			Mode: b.GatewayAccessMode, PrincipalID: b.BackendPrincipalId, OwnerID: b.GatewayBindingOwnerId, State: b.GatewayBindingState,
			CreatedAt: diagnosticMillis(&b.CreatedAt), UpdatedAt: diagnosticMillis(&b.UpdatedAt), LastActivityAt: diagnosticMillis(&b.LastActivityAt), ClosedAt: diagnosticMillis(b.ClosedAt),
		})
	}
	for _, d := range s.SlackOutgoingMessages {
		out.SlackOutgoingMessages = append(out.SlackOutgoingMessages, storage.SlackOutgoingMessageMetadata{
			ID: int64(d.Id), IntegrationID: int64(d.IntegrationId), IntegrationName: d.IntegrationName, IntegrationActive: d.IntegrationActive != 0, JobID: int64(d.JobId), WorkspaceID: d.SlackWorkspaceId,
			ChannelID: d.SlackChannelId, ThreadTS: d.SlackThreadRootMessageId, Part: d.SlackMessageIndex,
			State: d.SlackDeliveryState, SlackTS: d.SlackMessageId, Kind: d.SlackMessageKind, Attempts: d.SlackDeliveryAttempts, NextAttemptAt: diagnosticMillis(d.SlackDeliveryNextAttemptAt), CreatedAt: diagnosticMillis(&d.CreatedAt), UpdatedAt: diagnosticMillis(&d.UpdatedAt),
		})
	}
	for _, a := range s.SlackAgentSessionPublications {
		out.SlackAgentSessionPublications = append(out.SlackAgentSessionPublications, storage.SlackAgentSessionPublicationMetadata{
			Mode: a.SlackProgressMode, IntegrationID: int64(a.InitialIntegrationId), IntegrationName: a.IntegrationName, IntegrationActive: a.IntegrationActive != 0, Target: string(gateway.StatusTargetAgentSession), WorkspaceID: a.SlackWorkspaceId,
			ChannelID: a.SlackChannelId, ThreadTS: a.SlackThreadRootMessageId, StatusSourceJobID: int64(a.StatusSourceJobId),
			InitiatorUserID: a.SlackInitiatorUserId, DesiredStatus: a.SlackAgentDesiredStatus, AgentStatus: a.SlackAgentObservedStatus,
			SessionStatus: a.SlackSessionObservedStatus, State: a.SlackPublicationState, Revision: int64(a.SlackPublicationRevision), SentRevision: int64(a.SlackPublicationSentRevision),
			Attempts: a.SlackPublicationAttempts, NextAttemptAt: diagnosticMillis(a.SlackPublicationNextAttemptAt), RefreshAt: diagnosticMillis(a.SlackPublicationRefreshAt),
			RateLimitedUntil: diagnosticMillis(a.SlackPublicationRateLimitedUntil), FirstSentAt: diagnosticMillis(a.SlackPublicationFirstSentAt), LastSentAt: diagnosticMillis(a.SlackPublicationLastSentAt),
			Reason: a.SlackPublicationReason, Warning: a.SlackPublicationWarning, CreatedAt: diagnosticMillis(&a.CreatedAt), UpdatedAt: diagnosticMillis(&a.UpdatedAt),
		})
	}
	return out
}

// Diagnostic JSON keeps its epoch-millisecond format at this presentation edge.
// Persisted optional moments remain native SQL NULL values until they occur.
func diagnosticMillis(at *time.Time) int64 {
	if at == nil || at.IsZero() {
		return 0
	}
	return at.UnixMilli()
}
