package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/viant/agently-core/sdk"
)

var (
	ErrDenied   = errors.New("access denied")
	ErrConflict = errors.New("state or identity conflict")
	// ErrBackendProtocol marks an unsafe identity/response or permanent API failure.
	// Transient errors are reconciled by read-only polling, never query replay.
	ErrBackendProtocol = errors.New("backend response requires review")
	// ErrBackendAuth is handled by the shared backend connection lifecycle.
	ErrBackendAuth         = errors.New("Viant Steward session or execution identity rejected")
	ErrBackendUnavailable  = errors.New("Viant Steward is temporarily unavailable")
	ErrSlackAuth           = errors.New("Slack credentials rejected; runtime stopped")
	ErrMetadataUnavailable = errors.New("Slack policy metadata unavailable")
)

type ThreadKey struct {
	WorkspaceID string
	ChannelID   string
	RootTS      string
}

type MessageSurface string

const (
	SurfaceChannel       MessageSurface = "channel"
	SurfaceDirectMessage MessageSurface = "direct_message"
)

// Empty preserves the channel semantics of existing in-process callers.
func (m Message) MessageSurface() MessageSurface {
	if m.Surface == "" {
		return SurfaceChannel
	}
	return m.Surface
}

// Message is transport-neutral. Text has the bot mention removed by the adapter.
// Only verified transport events may populate these fields; this is not a public API.
type Message struct {
	ThreadKey
	StatusMode     StatusMode
	Surface        MessageSurface
	AppID          string
	EventID        string
	AuthorID       string
	MessageTS      string
	Text           string
	Mentioned      bool
	Bot            bool
	Edited         bool
	Deleted        bool
	ExternalShared bool
	HasAttachments bool
	// BackendReason is a local intake disposition, never supplied by Slack or
	// an AI model. Storage persists the terminal notice and event atomically.
	BackendReason string
}

type Decision struct {
	Mode        string
	PrincipalID string
	OwnerID     string
}

type Binding struct {
	ID int64
	ThreadKey
	Decision
	ConversationID string
	State          string
}

type Job struct {
	ID int64
	Message
	BindingID int64
	Decision
	RequestID      string
	TurnID         string
	State          string
	Result         string
	Reason         string
	QueryStartedAt time.Time
	PollAttempts   int
}

type ReplyFormat string

const (
	ReplyPlain    ReplyFormat = "plain"
	ReplyMarkdown ReplyFormat = "markdown"
)

type ReplyPart struct {
	Text   string
	Format ReplyFormat
}

type Delivery struct {
	ID    int64
	JobID int64
	ThreadKey
	Part     int
	Text     string
	Format   ReplyFormat
	State    string
	SlackTS  string
	Attempts int
}

// DeliveryContext is reconstructed from the original durable job and binding,
// not from destination fields supplied by a caller or an AI model.
type DeliveryContext struct {
	Message Message
	Binding *Binding
}

type DeliveryPolicy interface {
	AuthorizeDelivery(context.Context, DeliveryContext, bool) error
}

type AcceptResult struct {
	Accepted  bool
	Duplicate bool
	Ignored   bool
}

type EventSink interface {
	Accept(context.Context, Message) (AcceptResult, error)
}

type AccessPolicy interface {
	Authorize(context.Context, Message, *Binding) (Decision, error)
}

type Query struct {
	ConversationID string
	MessageID      string
	AgentID        string
	Text           string
}

type Turn struct {
	// ID is the stable gateway request ID. Adapters resolve backend-native turn
	// identities without changing the worker's durable correlation key.
	ID      string
	State   string // queued, running, completed, failed, canceled, waiting_for_user, unknown
	Content string
	// StandaloneMessages contains only native public messages of this completed
	// turn, in canonical order; execution narration and the final are excluded.
	StandaloneMessages []*sdk.TurnMessageState
}

type AgentlyClient interface {
	CreateConversation(context.Context, string) (string, error)
	Query(context.Context, Query, func(Turn) error) (Turn, error)
	ReadTurn(context.Context, string, string, func(Turn) error) (Turn, error)
}

type AgentlyClientFactory interface {
	ForPrincipal(context.Context, string) (AgentlyClient, error)
}

type BackendState string

const (
	BackendStarting   BackendState = "starting"
	BackendReady      BackendState = "ready"
	BackendRecovering BackendState = "recovering"
	BackendBlocked    BackendState = "blocked"
)

type BackendAvailability interface {
	State() BackendState
}

// BackendLifecycle owns one shared authentication/recovery flow. WithReady
// serializes durable start markers against a transition out of readiness;
// callers never perform network operations inside its callback.
type BackendLifecycle interface {
	BackendAvailability
	AgentlyClientFactory
	Run(context.Context) error
	WithReady(func() error) error
}

func BackendReason(state BackendState) string {
	if state == BackendReady {
		return ""
	}
	if state == BackendBlocked {
		return "backend_blocked"
	}
	return "backend_unavailable"
}

func BackendNotice(reason string) string {
	if reason == "backend_blocked" {
		return "Viant Steward is currently unavailable. An administrator needs to resolve the connection issue. Your question was not submitted. Please try again after the connection is restored."
	}
	return "Viant Steward is temporarily unavailable. Your question was not submitted. Please try again later."
}

// SlackSender receives a persisted reply, never a model instruction to publish.
type SlackSender interface {
	Send(context.Context, Delivery) (string, error)
}

// Store operations are workflow-specific. Claims and result/outbox writes must
// be atomic. Implementations must serialize unresolved queries within a thread.
type Store interface {
	StatusStore
	Enqueue(context.Context, Message) (bool, error)
	// FailUnstarted atomically ends unsubmitted work with a durable Slack notice.
	// Started/uncertain external operations are left for read-only reconciliation.
	FailUnstarted(context.Context, string) error
	GetBinding(context.Context, ThreadKey) (*Binding, error)
	ClaimJob(context.Context) (*Job, error)
	BindJob(context.Context, int64, Decision) (*Binding, error)
	BeginCreate(context.Context, int64) error
	SetConversation(context.Context, int64, string) error
	BeginQuery(context.Context, int64) error
	SchedulePoll(context.Context, int64, time.Time) error
	Complete(context.Context, int64, string, string, []ReplyPart) error
	Review(context.Context, int64, string) error
	Reject(context.Context, int64, string) error
	ClaimDelivery(context.Context) (*Delivery, error)
	GetDeliveryContext(context.Context, Delivery) (*DeliveryContext, error)
	Delivered(context.Context, int64, string) error
	DeliveryUnknown(context.Context, int64) error
	RetryDelivery(context.Context, int64, time.Time) error
	FailDelivery(context.Context, int64) error
	DeliveryNotBefore(context.Context, string) (time.Time, error)
	// Recover runs once, with exclusive ownership, before starting any workers.
	Recover(context.Context) error
}
