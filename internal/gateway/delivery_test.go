package gateway_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/viant/agently-slack-gateway/internal/gateway"
	"github.com/viant/agently-slack-gateway/internal/storetest"
)

func TestDeliveryClassification(t *testing.T) {
	for _, kind := range []gateway.SendErrorKind{gateway.SendRateLimited, gateway.SendRejected, gateway.SendCredentials, gateway.SendUnknown} {
		t.Run(string(kind), func(t *testing.T) {
			s, reopen := database(t)
			accept(t, s, storetest.Message("a", "one", "alice"))
			work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
			sender := &fakeSlack{err: &gateway.SendError{Kind: kind, RetryAfter: time.Hour}}
			w := gateway.DeliveryWorker{Store: s, Sender: sender}
			ok, err := w.Step(context.Background())
			if !ok {
				t.Fatal("no send")
			}
			switch kind {
			case gateway.SendRateLimited:
				storetest.Must(t, err)
			case gateway.SendCredentials:
				var e *gateway.SendError
				if !errors.As(err, &e) || e.Kind != kind {
					t.Fatal("credential rejection not fatal")
				}
			default:
				var e *gateway.DeliveryIssue
				if !errors.As(err, &e) {
					t.Fatal("missing persisted nonfatal delivery issue")
				}
			}
			s = reopen()
			d, err := s.ClaimDelivery(context.Background())
			storetest.Must(t, err)
			if (d != nil) != (kind == gateway.SendCredentials) {
				t.Fatal("incorrect restart retry policy")
			}
		})
	}
}

func TestDeliveryPacingAndPersistedRendering(t *testing.T) {
	s, _ := database(t)
	m := storetest.Message("a", "one", "alice")
	m.Text = strings.Repeat("<🙂>", 5000)
	accept(t, s, m)
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: &fakeAgently{}, AgentID: "steward"})
	sender := &fakeSlack{}
	w := gateway.DeliveryWorker{Store: s, Sender: sender, Interval: time.Hour}
	ok, err := w.Step(context.Background())
	storetest.Must(t, err)
	if !ok {
		t.Fatal("first send missing")
	}
	ok, err = w.Step(context.Background())
	storetest.Must(t, err)
	if ok || len(sender.snapshot()) != 1 {
		t.Fatal("pacing bypassed")
	}
	if sender.snapshot()[0].Format != gateway.ReplyMarkdown || !strings.Contains(sender.snapshot()[0].Text, "<🙂>") {
		t.Fatal("native Markdown format or content lost")
	}
}
