// Package workflow composes the gateway's persistence operations through native
// Datly readers and writers. Its component is private to the embedded runtime.
package workflow

import (
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	xdatly "github.com/viant/xdatly"
)

type Component struct {
	Contract xdatly.Component[Input, Output] `component:"Workflow,path=/internal/slack/storage,method=POST,handler=NewWorkflow,connector=slack,internal=true" componentCallPolicy:"imperative"`
}

// Input is a trusted in-process command, never an HTTP or model-controlled body.
type Input struct {
	Action               string
	Scope                int64
	IntegrationName      string
	Integrations         []storage.Integration
	Protocol             []gateway.ProtocolSnapshot
	ID                   int64
	Message              gateway.Message
	Key                  gateway.ThreadKey
	Decision             gateway.Decision
	SlackOutgoingMessage gateway.SlackOutgoingMessage
	Status               gateway.GatewayJobProgress
	StatusResult         gateway.StatusResult
	At                   time.Time
	Text                 string
	GatewayRequestID     string
	Parts                []gateway.ReplyPart
	Kind                 string
	Limit                int
	Route                gateway.Route
	Principal            string
	AcceptDuplicateRisk  bool
	PreferMessage        bool
}

type Output struct {
	Inserted             bool
	Duplicate            bool
	Integrations         []storage.Integration
	GatewayJob           *gateway.GatewayJob
	ConversationBinding  *gateway.ConversationBinding
	SlackOutgoingMessage *gateway.SlackOutgoingMessage
	Context              *gateway.DeliveryContext
	Status               *gateway.GatewayJobProgress
	Snapshot             *storage.Status
	NotBefore            time.Time
}
