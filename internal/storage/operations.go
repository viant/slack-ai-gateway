package storage

import (
	"context"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// Store includes the runtime and offline operator capabilities of a backend.
type Store interface {
	gateway.Store
	ReconcileIntegrations(context.Context, []Integration) ([]Integration, error)
	Scope(int64) Store
	ScopeName(string) Store
	FailCleanup(context.Context, string) error
	Close() error
	CheckRunBindings(context.Context, gateway.Route, string) error
	Status(context.Context, int) (*Status, error)
	ResolveDelivery(context.Context, int64, string, string, bool) error
}

// Integration is one recorded assignment of a named configuration entry.
// Reconciliation allocates the numeric ID; historical assignments stay closed.
type Integration struct {
	ID                          int64  `json:"integration_id"`
	Name                        string `json:"integration_name"`
	Active                      bool   `json:"active"`
	AppID, WorkspaceID          string
	BackendType, BackendURL     string
	BackendAgentID, PrincipalID string
}

// Status deliberately has no prompt, result, reply text or credential fields.
// It is a consistent snapshot; rows are newest first, counts include all rows.
type Status struct {
	Limit                         int                                    `json:"limit_per_table"`
	Integrations                  []IntegrationMetadata                  `json:"integrations"`
	Counts                        map[string]map[string]int              `json:"counts"`
	GatewayJobs                   []GatewayJobMetadata                   `json:"gateway_jobs"`
	ConversationBindings          []ConversationBindingMetadata          `json:"conversation_bindings"`
	SlackOutgoingMessages         []SlackOutgoingMessageMetadata         `json:"slack_outgoing_messages"`
	SlackAgentSessionPublications []SlackAgentSessionPublicationMetadata `json:"slack_agent_session_publications"`
}

type IntegrationMetadata struct {
	ID             int64  `json:"integration_id"`
	Name           string `json:"integration_name"`
	Active         bool   `json:"active"`
	AppID          string `json:"slack_app_id"`
	WorkspaceID    string `json:"slack_workspace_id"`
	BackendType    string `json:"backend_type"`
	BackendAgentID string `json:"backend_agent_id"`
	CreatedAt      int64  `json:"created_at_ms"`
	UpdatedAt      int64  `json:"updated_at_ms"`
}

// SlackAgentSessionPublicationMetadata describes publication only, without upstream payloads.
type SlackAgentSessionPublicationMetadata struct {
	IntegrationID     int64  `json:"initial_integration_id"`
	IntegrationName   string `json:"initial_integration_name"`
	IntegrationActive bool   `json:"initial_integration_active"`
	Mode              string `json:"slack_progress_mode"`
	Target            string `json:"slack_publication_target"`
	WorkspaceID       string `json:"slack_workspace_id"`
	ChannelID         string `json:"slack_channel_id"`
	ThreadTS          string `json:"slack_thread_root_message_id"`
	StatusSourceJobID int64  `json:"status_source_job_id"`
	InitiatorUserID   string `json:"slack_initiator_user_id"`
	DesiredStatus     string `json:"slack_agent_desired_status"`
	AgentStatus       string `json:"slack_agent_observed_status"`
	SessionStatus     string `json:"slack_session_observed_status"`
	State             string `json:"slack_publication_state"`
	Revision          int64  `json:"slack_publication_revision"`
	SentRevision      int64  `json:"slack_publication_sent_revision"`
	Attempts          int    `json:"slack_publication_attempts"`
	NextAttemptAt     int64  `json:"slack_publication_next_attempt_at_ms"`
	RefreshAt         int64  `json:"slack_publication_refresh_at_ms"`
	RateLimitedUntil  int64  `json:"slack_publication_rate_limited_until_ms"`
	FirstSentAt       int64  `json:"slack_publication_first_sent_at_ms"`
	LastSentAt        int64  `json:"slack_publication_last_sent_at_ms"`
	Reason            string `json:"slack_publication_reason"`
	Warning           string `json:"slack_publication_warning"`
	CreatedAt         int64  `json:"created_at_ms"`
	UpdatedAt         int64  `json:"updated_at_ms"`
}

// GatewayJobMetadata combines execution and progress facts, without prompt,
// result, rendered status or upstream response bodies.
type GatewayJobMetadata struct {
	ID                         int64  `json:"id"`
	IntegrationID              int64  `json:"integration_id"`
	IntegrationName            string `json:"integration_name"`
	IntegrationActive          bool   `json:"integration_active"`
	ConversationBindingID      int64  `json:"conversation_binding_id"`
	GatewayRequestID           string `json:"gateway_request_id"`
	WorkspaceID                string `json:"slack_workspace_id"`
	ChannelID                  string `json:"slack_channel_id"`
	ThreadTS                   string `json:"slack_thread_root_message_id"`
	AuthorID                   string `json:"slack_prompt_author_id"`
	State                      string `json:"gateway_execution_state"`
	Reason                     string `json:"gateway_execution_reason"`
	Outcome                    string `json:"gateway_execution_outcome"`
	PollAttempts               int    `json:"backend_poll_attempts"`
	BackendSubmissionStartedAt int64  `json:"backend_submission_started_at_ms"`
	NextAttemptAt              int64  `json:"backend_next_poll_at_ms"`
	ObservedState              string `json:"backend_observed_state"`
	ObservedAt                 int64  `json:"backend_observed_at_ms"`
	ObservationUncertain       bool   `json:"backend_observation_uncertain"`
	Mode                       string `json:"slack_progress_mode"`
	Target                     string `json:"slack_progress_target,omitempty"`
	SlackTS                    string `json:"slack_progress_message_id"`
	Phase                      string `json:"slack_progress_phase"`
	ProgressState              string `json:"slack_progress_state"`
	ProgressReason             string `json:"slack_progress_reason"`
	Revision                   int64  `json:"slack_progress_revision"`
	SentRevision               int64  `json:"slack_progress_sent_revision"`
	ProgressAttempts           int    `json:"slack_progress_attempts"`
	ProgressNextAttemptAt      int64  `json:"slack_progress_next_attempt_at_ms"`
	RefreshAt                  int64  `json:"slack_progress_refresh_at_ms"`
	RateLimitedUntil           int64  `json:"slack_progress_rate_limited_until_ms"`
	FirstSentAt                int64  `json:"slack_progress_first_sent_at_ms"`
	LastSentAt                 int64  `json:"slack_progress_last_sent_at_ms"`
	AckLatencyMS               *int64 `json:"slack_progress_ack_latency_ms"`
	CreatedAt                  int64  `json:"created_at_ms"`
	ExecutionUpdatedAt         int64  `json:"gateway_execution_updated_at_ms"`
	StatusUpdatedAt            int64  `json:"gateway_status_updated_at_ms"`
	ExecutionFinishedAt        int64  `json:"gateway_execution_finished_at_ms"`
	ContentPurgedAt            int64  `json:"content_purged_at_ms"`
}

type ConversationBindingMetadata struct {
	IntegrationID     int64  `json:"integration_id"`
	IntegrationName   string `json:"integration_name"`
	IntegrationActive bool   `json:"integration_active"`
	ID                int64  `json:"id"`
	WorkspaceID       string `json:"slack_workspace_id"`
	ChannelID         string `json:"slack_channel_id"`
	ThreadTS          string `json:"slack_thread_root_message_id"`
	ConversationID    string `json:"backend_conversation_id"`
	Mode              string `json:"gateway_access_mode"`
	PrincipalID       string `json:"backend_principal_id"`
	OwnerID           string `json:"gateway_binding_owner_id"`
	State             string `json:"gateway_binding_state"`
	CreatedAt         int64  `json:"created_at_ms"`
	UpdatedAt         int64  `json:"updated_at_ms"`
	LastActivityAt    int64  `json:"last_activity_at_ms"`
	ClosedAt          int64  `json:"closed_at_ms"`
}

type SlackOutgoingMessageMetadata struct {
	IntegrationID     int64  `json:"integration_id"`
	IntegrationName   string `json:"integration_name"`
	IntegrationActive bool   `json:"integration_active"`
	ID                int64  `json:"id"`
	JobID             int64  `json:"job_id"`
	WorkspaceID       string `json:"slack_workspace_id"`
	ChannelID         string `json:"slack_channel_id"`
	ThreadTS          string `json:"slack_thread_root_message_id"`
	Part              int    `json:"slack_message_index"`
	State             string `json:"slack_delivery_state"`
	SlackTS           string `json:"slack_message_id"`
	Kind              string `json:"slack_message_kind"`
	Attempts          int    `json:"slack_delivery_attempts"`
	NextAttemptAt     int64  `json:"slack_delivery_next_attempt_at_ms"`
	CreatedAt         int64  `json:"created_at_ms"`
	UpdatedAt         int64  `json:"updated_at_ms"`
}
