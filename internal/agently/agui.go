package agently

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/viant/agently-core/protocol/agui"
	"github.com/viant/agently-core/sdk"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func chatInput(threadID string, q gateway.Query) *agui.RunAgentInput {
	useServerState := true
	props, _ := json.Marshal(agui.ForwardedProps{Agently: &agui.Extension{
		Version: agui.ExtensionVersion, Operation: "chat", RequestID: q.MessageID,
		Payload: &agui.ExecutionPayload{AgentID: q.AgentID, UseServerState: &useServerState},
	}})
	content, _ := json.Marshal(q.Text)
	// Preserve the pre-submission durable identity. PrepareAGUIChat creates a new
	// random RunID, which would break attachment after an uncertain submission.
	return &agui.RunAgentInput{ThreadID: threadID, RunID: q.MessageID,
		Messages: []agui.Message{{ID: q.MessageID, Role: "user", Content: content}}, ForwardedProps: props}
}

func (c *Client) observeRun(ctx context.Context, conversationID, threadID, runID string, query *gateway.Query, observe func(gateway.Turn) error) (gateway.Turn, error) {
	client, err := sdk.NewHTTP(c.factory.base, sdk.WithHTTPClient(c.httpClient(true)), sdk.WithTokenProvider(c.token))
	if err != nil {
		return gateway.Turn{}, gateway.ErrBackendProtocol
	}
	var stream *sdk.AGUIRunStream
	if query == nil {
		stream, err = client.AttachAGUI(ctx, threadID, runID, "")
	} else {
		stream, err = client.RunAGUI(ctx, chatInput(threadID, *query), nil)
	}
	if err != nil {
		var api *APIError
		if query == nil && errors.As(err, &api) && api.Status == http.StatusNotFound {
			return gateway.Turn{}, &APIError{Status: http.StatusNotFound}
		}
		return gateway.Turn{}, observationError(err)
	}
	defer stream.Close() // Detaches the observer; never cancels durable execution.
	nativeID := ""
	observed := ""
	checkedHints := map[string]bool{}
	notify := func(turn gateway.Turn) error {
		// RUN_STARTED can precede the native turn's first persisted snapshot.
		// Absence is not a failed observation while the initial confirmation
		// budget is still running in the worker.
		if turn.State == "unknown" || turn.State == observed {
			return nil
		}
		if observe != nil {
			if err := observe(turn); err != nil {
				return err
			}
		}
		observed = turn.State
		return nil
	}
	canonical := func() (gateway.Turn, error) {
		turn, err := c.readNativeTurn(ctx, conversationID, runID, nativeID)
		if err == nil {
			err = notify(turn)
		}
		return turn, err
	}
	for {
		event, recvErr := stream.Recv()
		if recvErr != nil {
			if nativeID != "" && ctx.Err() == nil {
				// A disconnected observer may already have a committed canonical outcome.
				// Otherwise the existing scheduler reattaches, never resubmits the prompt.
				return canonical()
			}
			return gateway.Turn{}, observationError(recvErr)
		}
		var header struct {
			ThreadID, RunID, ProtocolVersion string
			SubagentRunID                    *string `json:"subagentRunId"`
			ActivityType                     string
			Metadata                         struct {
				Agently struct{ IdentityVersion, NativeTurnID string }
			}
		}
		if event.Decode(&header) != nil {
			return gateway.Turn{}, gateway.ErrBackendProtocol
		}
		if header.SubagentRunID != nil {
			continue
		}
		if event.Type == "RUN_STARTED" {
			if header.ThreadID != threadID || header.RunID != runID || header.ProtocolVersion != agui.ProtocolVersion || header.Metadata.Agently.IdentityVersion != agui.ExtensionVersion || !validID(header.Metadata.Agently.NativeTurnID) || (nativeID != "" && nativeID != header.Metadata.Agently.NativeTurnID) {
				return gateway.Turn{}, gateway.ErrBackendProtocol
			}
			nativeID = header.Metadata.Agently.NativeTurnID
			if query != nil && observe == nil {
				continue
			}
			turn, err := canonical()
			if err != nil {
				return gateway.Turn{}, err
			}
			// A replay can begin after the result was committed already.
			if terminalTurn(turn) || turn.State == "waiting_for_user" {
				return turn, nil
			}
		}
		if event.Type == "ACTIVITY_SNAPSHOT" && header.ActivityType == "agently.turn" {
			if nativeID == "" || (header.ThreadID != "" && header.ThreadID != threadID) || (header.RunID != "" && header.RunID != runID) {
				continue
			}
			var descriptor struct {
				Content struct{ Version, NativeTurnID, Status string }
			}
			if event.Decode(&descriptor) != nil {
				continue
			}
			content := descriptor.Content
			if content.NativeTurnID != nativeID {
				continue
			}
			if content.Version != agui.ExtensionVersion {
				continue
			}
			switch content.Status {
			case "queued", "running", "completed", "failed", "canceled", "waiting_for_user":
			default:
				continue
			}
			if content.Status == observed || content.Status == "queued" && observed == "running" {
				continue
			}
			if query != nil && (content.Status == "queued" || content.Status == "running") {
				if err = notify(gateway.Turn{ID: runID, State: content.Status}); err != nil {
					return gateway.Turn{}, err
				}
				continue
			}
			// An attachment can replay old state. Confirm each different hint
			// canonically, without turning repeated snapshots into HTTP polling.
			if checkedHints[content.Status] {
				continue
			}
			checkedHints[content.Status] = true
			turn, err := canonical()
			if err != nil {
				return gateway.Turn{}, err
			}
			if terminalTurn(turn) || turn.State == "waiting_for_user" {
				return turn, nil
			}
			if turn.State != "unknown" && turn.State != content.Status {
				// The hint raced the current projection. Let the existing worker
				// schedule attachment again instead of inventing another retry loop.
				return turn, nil
			}
		}
		if event.Type == "RUN_FINISHED" || event.Type == "RUN_ERROR" {
			if (header.ThreadID != "" && header.ThreadID != threadID) || (header.RunID != "" && header.RunID != runID) {
				continue
			}
			if nativeID == "" {
				return gateway.Turn{}, gateway.ErrBackendProtocol
			}
			return canonical()
		}
	}
}

func terminalTurn(t gateway.Turn) bool {
	return t.State == "completed" || t.State == "failed" || t.State == "canceled"
}
func observationError(err error) error {
	if errors.Is(err, gateway.ErrBackendUnavailable) {
		return gateway.ErrBackendUnavailable
	}
	var missing *missingSession
	if errors.As(err, &missing) {
		return missing
	}
	if errors.Is(err, gateway.ErrBackendAuth) {
		return gateway.ErrBackendAuth
	}
	var api *APIError
	if errors.As(err, &api) {
		return api
	}
	if errors.Is(err, gateway.ErrBackendProtocol) {
		return gateway.ErrBackendProtocol
	}
	return &APIError{} // Native errors can contain backend text; never expose it.
}
