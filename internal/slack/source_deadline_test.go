package slack

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/slack-go/slack/socketmode"
	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestSourceAcceptDeadlineDisposition(t *testing.T) {
	for _, tc := range []struct {
		name       string
		parent     func() (context.Context, context.CancelFunc)
		accept     func(context.Context, gateway.Message) (gateway.AcceptResult, error)
		wantFatal  bool
		wantACK    bool
		wantBudget bool
	}{
		{
			name: "own budget",
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				<-ctx.Done()
				return gateway.AcceptResult{}, ctx.Err()
			},
			wantBudget: true,
		},
		{
			name: "own budget with transaction and SQL errors",
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				<-ctx.Done()
				return gateway.AcceptResult{}, errors.Join(ctx.Err(), sql.ErrTxDone, errors.New("private-SQL-marker"))
			},
			wantBudget: true,
		},
		{
			name: "unrelated deadline while own budget is live",
			accept: func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
				return gateway.AcceptResult{}, context.DeadlineExceeded
			},
			wantFatal: true,
		},
		{
			name: "inner cancellation while parent is live",
			accept: func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
				return gateway.AcceptResult{}, context.Canceled
			},
			wantFatal: true,
		},
		{
			name: "parent canceled",
			parent: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				return gateway.AcceptResult{}, ctx.Err()
			},
			wantFatal: true,
		},
		{
			name: "parent deadline",
			parent: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				return gateway.AcceptResult{}, ctx.Err()
			},
			wantFatal: true,
		},
		{
			name: "other error despite expired budget",
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				<-ctx.Done()
				return gateway.AcceptResult{}, errors.New("private-SQL-marker")
			},
			wantFatal: true,
		},
		{
			name: "successful admission at deadline",
			accept: func(ctx context.Context, _ gateway.Message) (gateway.AcceptResult, error) {
				<-ctx.Done()
				return gateway.AcceptResult{Accepted: true}, nil
			},
			wantACK: true,
		},
		{
			name: "missing disposition",
			accept: func(context.Context, gateway.Message) (gateway.AcceptResult, error) {
				return gateway.AcceptResult{}, nil
			},
			wantFatal: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if tc.parent != nil {
				cancel()
				ctx, cancel = tc.parent()
			}
			defer cancel()
			var logs bytes.Buffer
			acks := 0
			source := Source{session: &fakeSession{ack: func(string) error { acks++; return nil }},
				route: testRoute, botUserID: "UBOT", log: slog.New(slog.NewJSONHandler(&logs, nil))}
			req := socketmode.Request{Type: socketmode.RequestTypeEventsAPI, EnvelopeID: "current-envelope", RetryAttempt: 2,
				Payload: payload(func(_, e map[string]any) { e["text"] = "<@UBOT> private-prompt-marker" })}
			err := source.handle(ctx, req, sinkFunc(tc.accept))
			if errors.Is(err, gateway.ErrIntakePersistence) != tc.wantFatal || (err != nil) != tc.wantFatal {
				t.Fatalf("fatal disposition changed: %v", err)
			}
			wantACKs := 0
			if tc.wantACK {
				wantACKs = 1
			}
			if acks != wantACKs {
				t.Fatalf("ACK calls=%d want=%d", acks, wantACKs)
			}
			text := logs.String()
			if strings.Contains(text, "private-") || err != nil && strings.Contains(err.Error(), "private-") {
				t.Fatal("private error or prompt leaked")
			}
			budgetRecords := 0
			decoder := json.NewDecoder(strings.NewReader(text))
			for {
				var record map[string]any
				if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				if record["msg"] != "slack_intake_timeout" {
					continue
				}
				budgetRecords++
				if record["level"] != "WARN" || record["event_id"] != "E1" || record["envelope_id"] != "current-envelope" ||
					record["reason"] != "accept_budget_exceeded" || record["ack_state"] != "not_sent" || record["retry_attempt"] != float64(2) {
					t.Fatalf("missing safe intake diagnostics: %+v", record)
				}
				if _, ok := record["duration_ms"].(float64); !ok {
					t.Fatal("missing measured duration")
				}
			}
			if (budgetRecords == 1) != tc.wantBudget || budgetRecords > 1 {
				t.Fatalf("budget records=%d want budget=%v", budgetRecords, tc.wantBudget)
			}
		})
	}
}

func TestSourceRunContinuesAfterOwnAcceptDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream := make(chan socketmode.Event, 3)
	stream <- socketmode.Event{Type: socketmode.EventTypeHello, Request: &socketmode.Request{ConnectionInfo: socketmode.ConnectionInfo{AppID: "A"}}}
	for _, id := range []string{"E1", "E2"} {
		stream <- socketmode.Event{Type: socketmode.EventTypeEventsAPI, Request: &socketmode.Request{
			Type: socketmode.RequestTypeEventsAPI, EnvelopeID: "envelope-" + id,
			Payload: payload(func(c, _ map[string]any) { c["event_id"] = id }),
		}}
	}
	acked := make(chan string, 2)
	source := Source{session: &fakeSession{stream: stream, ack: func(id string) error { acked <- id; return nil }},
		route: testRoute, botUserID: "UBOT", log: quietLog()}
	done := make(chan error, 1)
	go func() {
		done <- source.Run(ctx, sinkFunc(func(ctx context.Context, m gateway.Message) (gateway.AcceptResult, error) {
			if m.EventID == "E1" {
				<-ctx.Done()
				return gateway.AcceptResult{}, errors.Join(ctx.Err(), sql.ErrTxDone)
			}
			return gateway.AcceptResult{Accepted: true}, nil
		}), func() {})
	}()
	select {
	case id := <-acked:
		if id != "envelope-E2" {
			t.Errorf("failed E1 was acknowledged: %s", id)
		}
	case err := <-done:
		t.Fatalf("source stopped at one failed attempt: %v", err)
	case <-ctx.Done():
		t.Fatal("source did not receive E2 after timeout")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-acked:
		t.Fatalf("unexpected extra ACK: %s", id)
	default:
	}
}
