package storage

import (
	"context"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

// Store includes the runtime and offline operator capabilities of a backend.
type Store interface {
	gateway.Store
	Close() error
	CheckRunBindings(context.Context, gateway.Route, string) error
	Status(context.Context, int) (*Status, error)
	ResolveDelivery(context.Context, int64, string, string, bool) error
}

// Status deliberately has no prompt, result, reply text or credential fields.
// It is a consistent snapshot; rows are newest first, counts include all rows.
type Status struct {
	Limit           int                       `json:"limit_per_table"`
	Counts          map[string]map[string]int `json:"counts"`
	Jobs            []JobStatus               `json:"jobs"`
	Bindings        []BindingStatus           `json:"bindings"`
	Deliveries      []DeliveryStatus          `json:"deliveries"`
	AgentSessions   []AgentSessionMetadata    `json:"agent_sessions"`
	RequestStatuses []RequestStatusMetadata   `json:"request_statuses"`
}

// AgentSessionMetadata describes publication only, without upstream payloads.
type AgentSessionMetadata struct {
	Mode             string `json:"mode"`
	Target           string `json:"target"`
	WorkspaceID      string `json:"workspace_id"`
	ChannelID        string `json:"channel_id"`
	ThreadTS         string `json:"thread_ts"`
	SourceJobID      int64  `json:"source_job_id"`
	InitiatorUserID  string `json:"initiator_user_id"`
	DesiredStatus    string `json:"desired_status"`
	AgentStatus      string `json:"agent_status"`
	SessionStatus    string `json:"session_status"`
	State            string `json:"state"`
	Revision         int64  `json:"revision"`
	SentRevision     int64  `json:"sent_revision"`
	Attempts         int    `json:"attempts"`
	NextAttemptAt    int64  `json:"next_attempt_at_ms"`
	RefreshAt        int64  `json:"refresh_at_ms"`
	RateLimitedUntil int64  `json:"rate_limited_until_ms"`
	FirstSentAt      int64  `json:"first_sent_at_ms"`
	LastSentAt       int64  `json:"last_sent_at_ms"`
	Reason           string `json:"reason"`
	Warning          string `json:"warning"`
}

// Metadata only: no rendered status or upstream response bodies.
type RequestStatusMetadata struct {
	Mode                 string `json:"mode"`
	Target               string `json:"target,omitempty"`
	JobID                int64  `json:"job_id"`
	SlackTS              string `json:"slack_ts"`
	Phase                string `json:"phase"`
	State                string `json:"state"`
	ObservedState        string `json:"observed_state"`
	ObservedAt           int64  `json:"observed_at_ms"`
	ObservationUncertain bool   `json:"observation_uncertain"`
	Outcome              string `json:"backend_outcome"`
	Revision             int64  `json:"revision"`
	SentRevision         int64  `json:"sent_revision"`
	Attempts             int    `json:"attempts"`
	NextAttemptAt        int64  `json:"next_attempt_at_ms"`
	RefreshAt            int64  `json:"refresh_at_ms"`
	RateLimitedUntil     int64  `json:"rate_limited_until_ms"`
	FirstSentAt          int64  `json:"first_sent_at_ms"`
	LastSentAt           int64  `json:"last_sent_at_ms"`
	AckLatencyMS         *int64 `json:"ack_latency_ms"`
	Reason               string `json:"reason"`
}
type JobStatus struct {
	ID             int64  `json:"id"`
	WorkspaceID    string `json:"workspace_id"`
	ChannelID      string `json:"channel_id"`
	ThreadTS       string `json:"thread_ts"`
	AuthorID       string `json:"author_id"`
	BindingID      int64  `json:"binding_id"`
	RequestID      string `json:"request_id"`
	TurnID         string `json:"turn_id"`
	State          string `json:"state"`
	Reason         string `json:"reason"`
	PollAttempts   int    `json:"poll_attempts"`
	QueryStartedAt int64  `json:"query_started_at_ms"`
	NextAttemptAt  int64  `json:"next_attempt_at_ms"`
}
type BindingStatus struct {
	ID             int64  `json:"id"`
	WorkspaceID    string `json:"workspace_id"`
	ChannelID      string `json:"channel_id"`
	ThreadTS       string `json:"thread_ts"`
	ConversationID string `json:"conversation_id"`
	Mode           string `json:"access_mode"`
	PrincipalID    string `json:"principal_id"`
	OwnerID        string `json:"owner_id"`
	State          string `json:"state"`
}
type DeliveryStatus struct {
	ID            int64  `json:"id"`
	JobID         int64  `json:"job_id"`
	WorkspaceID   string `json:"workspace_id"`
	ChannelID     string `json:"channel_id"`
	ThreadTS      string `json:"thread_ts"`
	Part          int    `json:"part"`
	State         string `json:"state"`
	SlackTS       string `json:"slack_ts"`
	Attempts      int    `json:"attempts"`
	NextAttemptAt int64  `json:"next_attempt_at_ms"`
}
