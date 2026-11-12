package gateway

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/viant/agently-core/sdk"
	presentation "github.com/viant/agently-core/sdk/slack"
)

type presentationStore struct {
	reviewStore
	result string
	parts  []ReplyPart
}

func (s *presentationStore) Complete(_ context.Context, _ int64, _, result string, parts []ReplyPart, _ ...ProtocolSnapshot) error {
	s.result, s.parts = result, parts
	return nil
}

func TestCompletedPresentationURLAttachmentsAndEmptyResult(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		mode                 presentation.ResponseMode
		state, content, want string
		format               ReplyFormat
	}{
		{"default_raw", "", "completed", "Hello.", "[View full response in Agently](https://example.org/prefix/conversation/example%20%2F%3F%23)\n\nHello.", ReplyMarkdown},
		{"text", presentation.ResponseSummary, "completed", "Hello.", "[View full response in Agently](https://example.org/prefix/conversation/example%20%2F%3F%23)\n\nHello.", ReplyMarkdown},
		{"empty_raw", presentation.ResponseRaw, "completed", "", "[View full response in Agently](https://example.org/prefix/conversation/example%20%2F%3F%23)\n\nThe query completed without a text response.", ReplyMarkdown},
		{"empty_text", presentation.ResponseSummary, "completed", "", "[View full response in Agently](https://example.org/prefix/conversation/example%20%2F%3F%23)\n\nThe query completed without a text response.", ReplyMarkdown},
		{"failure", presentation.ResponseSummary, "failed", "Ignored upstream detail", "The query did not complete successfully.", ReplyPlain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &presentationStore{}
			w := Worker{Store: s, ResponseMode: tc.mode, AgentlyBaseURL: "https://example.org/prefix///"}
			j := &GatewayJob{ID: 1, GatewayRequestID: "request"}
			j.HasAttachments = true
			if err := w.settle(context.Background(), j, "example /?#", Turn{ID: "request", State: tc.state, Content: tc.content}, nil); err != nil {
				t.Fatal(err)
			}
			if len(s.parts) != 1 || s.parts[0].Text != tc.want+"\n\nAttachments were not read; paste relevant text if needed." || s.parts[0].Format != tc.format {
				t.Fatalf("unexpected presentation: %+v", s.parts)
			}
			wantResult := tc.content
			if tc.content == "" {
				wantResult = "The query completed without a text response."
			}
			if tc.state == "failed" {
				wantResult = "The query did not complete successfully."
			}
			if s.result != wantResult {
				t.Fatal("canonical result overwritten or attachment note persisted as result")
			}
		})
	}
}

func TestPresentationLogsContainMetadataOnly(t *testing.T) {
	var log bytes.Buffer
	s := &presentationStore{}
	w := Worker{Store: s, ResponseMode: presentation.ResponseSummary, AgentlyBaseURL: "https://example.org",
		Log: slog.New(slog.NewJSONHandler(&log, nil))}
	content := "Authored prose example.\n```forge-ui\nRAW_PAYLOAD\n```"
	if err := w.settle(context.Background(), &GatewayJob{ID: 1, GatewayRequestID: "r"}, "example", Turn{ID: "r", State: "completed", Content: content, StandaloneMessages: []*sdk.TurnMessageState{{Content: "PRIVATE_NOTE"}}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"RAW_PAYLOAD", "Authored prose", "https://", "forge-ui", "PRIVATE_NOTE"} {
		if strings.Contains(log.String(), forbidden) {
			t.Fatal("presentation log disclosed answer or URL")
		}
	}
	if !strings.Contains(log.String(), `"mode":"summary"`) || !strings.Contains(log.String(), "interactive_content_not_shown") {
		t.Fatal("missing presentation metadata")
	}
}

func TestPublicMessagesDoNotProduceEmptyFinalNotice(t *testing.T) {
	for _, mode := range []presentation.ResponseMode{presentation.ResponseRaw, presentation.ResponseCLICompatible, presentation.ResponseSummary, presentation.ResponseNormalizedSummary, presentation.ResponseReportText} {
		s := &presentationStore{}
		w := Worker{Store: s, ResponseMode: mode, AgentlyBaseURL: "https://example.org"}
		err := w.settle(context.Background(), &GatewayJob{ID: 1, GatewayRequestID: "request"}, "example", Turn{ID: "request", State: "completed", StandaloneMessages: []*sdk.TurnMessageState{{MessageID: "note", Content: "Public findings."}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(s.parts) != 1 || s.parts[0].Text != "[View full response in Agently](https://example.org/conversation/example)\n\nPublic findings." || s.parts[0].Format != ReplyMarkdown {
			t.Fatal(s.parts)
		}
		if s.result != "The query completed without a text response." {
			t.Fatal("final-only stored result contract changed")
		}
	}
}
