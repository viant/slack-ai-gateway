package gateway

import "strings"

const conversationClosedReason = "conversation_closed"

// ConversationClosed identifies a durable local closure, independently of
// whether a backend conversation could be created or its execution confirmed.
func ConversationClosed(reason string) bool {
	return reason == conversationClosedReason || strings.HasPrefix(reason, conversationClosedReason+":")
}

// ConversationClosureReason retains the fixed source cause in the existing
// reason column. The reason is supplied by gateway code, never by Slack/model input.
func ConversationClosureReason(cause string) string {
	if cause == "" {
		return conversationClosedReason
	}
	if ConversationClosed(cause) {
		return cause
	}
	return conversationClosedReason + ":" + cause
}

// ConversationClosureNotice preserves a canonical outcome when already known.
// Local closure neither cancels the backend nor reverses completed side effects.
func ConversationClosureNotice(reason string, submitted bool, outcome string) string {
	ending := "This conversation is closed. Start a new thread to continue."
	if submitted {
		known := map[string]string{
			"completed": "The agent completed your request, but its response could not be recovered.",
			"failed":    "The agent reported that your request failed.",
			"canceled":  "The agent reported that your request was canceled.",
		}[outcome]
		if known != "" {
			return known + " " + ending + " Any queued questions were not submitted."
		}
	}
	cause := strings.TrimPrefix(reason, conversationClosedReason+":")
	if cause == "interaction_requires_review" {
		return "The agent requested an interaction that this Slack integration cannot handle. No response or approval was submitted. " + ending + " Any queued questions were not submitted."
	}
	if submitted {
		return "We could not confirm the outcome of your request. It may have been executed and was not submitted again. " + ending + " Any queued questions were not submitted."
	}
	if reason == conversationClosedReason || reason == "" {
		return "Your question was not submitted. " + ending
	}
	return "Your question was not submitted. " + ending + " Any queued questions were not submitted."
}
