package gateway

import (
	"strings"
	"testing"
	"time"
)

func TestConversationClosureReasons(t *testing.T) {
	for _, reason := range []string{"conversation_closed", "conversation_closed:reconciliation_deadline", "conversation_closed:creation_unknown"} {
		if !ConversationClosed(reason) || ConversationClosureReason(reason) != reason {
			t.Fatalf("closure cause was lost or nested: %q", reason)
		}
	}
	for _, reason := range []string{"", "conversation_closed_later", "reconciliation_deadline", "integration_changed"} {
		if ConversationClosed(reason) {
			t.Fatalf("unrelated reason was treated as a closure: %q", reason)
		}
	}
	if got := ConversationClosureReason("reconciliation_deadline"); got != "conversation_closed:reconciliation_deadline" {
		t.Fatal(got)
	}
}

func TestConversationClosureNoticeDoesNotInventExecutionOutcome(t *testing.T) {
	unknown := ConversationClosureNotice(ConversationClosureReason("reconciliation_deadline"), true, "")
	if !strings.Contains(unknown, "may have been executed") || !strings.Contains(unknown, "was not submitted again") || !strings.Contains(unknown, "queued questions were not submitted") {
		t.Fatalf("uncertain execution was misrepresented: %q", unknown)
	}
	unsubmitted := ConversationClosureNotice(ConversationClosureReason("creation_unknown"), false, "")
	if !strings.Contains(unsubmitted, "Your question was not submitted.") || strings.Contains(unsubmitted, "may have been executed") {
		t.Fatalf("unsubmitted question was misrepresented: %q", unsubmitted)
	}
	interaction := ConversationClosureNotice(ConversationClosureReason("interaction_requires_review"), true, "")
	if !strings.Contains(interaction, "No response or approval was submitted.") {
		t.Fatalf("interaction closure implied approval: %q", interaction)
	}
	for _, notice := range []string{unknown, unsubmitted, interaction, ConversationClosureNotice("conversation_closed", false, "")} {
		if !strings.Contains(notice, "Start a new thread") || strings.Contains(notice, "canceled") || strings.Contains(notice, "Elapsed:") || strings.Contains(notice, "operator") {
			t.Fatalf("invalid terminal notice: %q", notice)
		}
	}
}

func TestConversationClosureNoticePreservesConfirmedOutcome(t *testing.T) {
	for _, tc := range []struct {
		outcome, want string
	}{
		{"completed", "The agent completed your request, but its response could not be recovered."},
		{"failed", "The agent reported that your request failed."},
		{"canceled", "The agent reported that your request was canceled."},
	} {
		t.Run(tc.outcome, func(t *testing.T) {
			notice := ConversationClosureNotice(ConversationClosureReason("reconciliation_deadline"), true, tc.outcome)
			if !strings.HasPrefix(notice, tc.want) || !strings.Contains(notice, "Start a new thread") || !strings.Contains(notice, "queued questions were not submitted") {
				t.Fatalf("confirmed backend outcome was lost: %q", notice)
			}
			if strings.Contains(notice, "could not confirm the outcome") || strings.Contains(notice, "may have been executed") || strings.Contains(notice, "Elapsed:") {
				t.Fatalf("known outcome was presented as unknown or still running: %q", notice)
			}
		})
	}
}

func TestConversationClosedStatusHasNoRunningClock(t *testing.T) {
	for _, phase := range []string{"conversation_closed", ConversationClosureReason("reconciliation_deadline")} {
		s := GatewayJobProgress{Phase: phase, CreatedAt: time.Now().Add(-72 * time.Hour)}
		first := StatusText(s, time.Now())
		if second := StatusText(s, time.Now().Add(time.Hour)); first != second || strings.Contains(first, "Elapsed:") {
			t.Fatalf("closed conversation retained a running clock: %q / %q", first, second)
		}
	}
}
