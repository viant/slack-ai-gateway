package gateway_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type closingCreateBackend struct {
	fakeAgently
	store gateway.Store
	jobID int64
}

func (b *closingCreateBackend) ForPrincipal(_ context.Context, principal string) (gateway.AgentlyClient, error) {
	return &closingCreateClient{fakeClient: &fakeClient{backend: &b.fakeAgently, principal: principal}, backend: b}, nil
}

type closingCreateClient struct {
	*fakeClient
	backend *closingCreateBackend
}

func (c *closingCreateClient) CreateConversation(ctx context.Context, agent string) (string, error) {
	id, err := c.fakeClient.CreateConversation(ctx, agent)
	if err != nil {
		return "", err
	}
	closed, err := c.backend.store.CloseThread(ctx, c.backend.jobID, "creation_unknown")
	if err != nil || !closed {
		return "", fmt.Errorf("fixture could not close in-flight creation: closed=%t: %w", closed, err)
	}
	return id, nil
}

func TestClosureDuringCreateKeepsKnownIDWithoutSubmittingQuestion(t *testing.T) {
	ctx := context.Background()
	s, _ := database(t)
	m := storetest.Message("close-during-create", "create-root", "alice")
	accept(t, s, m)
	before, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	backend := &closingCreateBackend{store: s, jobID: before.GatewayJobs[0].ID}
	work(t, gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"})
	created, queries := backend.snapshot()
	if len(created) != 1 || len(queries) != 0 {
		t.Fatalf("closed conversation submitted a query: created=%v queries=%+v", created, queries)
	}
	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(status.ConversationBindings) != 1 || status.ConversationBindings[0].State != "closed" || status.ConversationBindings[0].ConversationID == "" || status.GatewayJobs[0].BackendSubmissionStartedAt != 0 {
		t.Fatalf("late create lost history or reopened execution: %+v", status)
	}
}

func TestUnresolvedThreadClosesQueueAndNewRootRemainsIndependent(t *testing.T) {
	ctx := context.Background()
	s, reopen := database(t)
	first := storetest.Message("closure-first", "closed-root", "alice")
	accept(t, s, first)
	accept(t, s, storetest.Message("closure-queued", first.RootTS, "bob"))
	backend := &fakeAgently{wrongTurn: true}
	w := gateway.Worker{Store: s, Policy: policy(), Clients: backend, AgentID: "steward"}
	work(t, w)

	status, err := s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(status.GatewayJobs) != 2 || len(status.SlackOutgoingMessages) != 1 || len(status.ConversationBindings) != 1 || status.ConversationBindings[0].State != "closed" {
		t.Fatalf("thread was not closed atomically: %+v", status)
	}
	for _, j := range status.GatewayJobs {
		if j.State != "rejected" || !gateway.ConversationClosed(j.Reason) || j.Outcome != "" || j.NextAttemptAt != 0 {
			t.Fatalf("unfinished or falsely resolved question: %+v", j)
		}
	}
	notice, err := s.ClaimDelivery(ctx)
	storetest.Must(t, err)
	if notice == nil || !strings.Contains(notice.Text, "may have been executed") {
		t.Fatal("closure claimed a known failure or cancellation")
	}
	storetest.Must(t, s.Delivered(ctx, notice.ID, "1.000001"))

	s = reopen()
	w.Store = s
	if ok, err := w.Step(ctx); err != nil || ok {
		t.Fatalf("restart resumed closed work: %t %v", ok, err)
	}
	if !accept(t, s, first).Duplicate {
		t.Fatal("closure lost the original event identity")
	}
	accept(t, s, storetest.Message("closure-followup", first.RootTS, "carol"))
	if ok, err := w.Step(ctx); err != nil || ok {
		t.Fatalf("closed-thread followup reached the backend: %t %v", ok, err)
	}
	status, err = s.Status(ctx, 10)
	storetest.Must(t, err)
	if len(status.SlackOutgoingMessages) != 2 {
		t.Fatal("closed-thread followup lacked its local reply or duplicated the closure")
	}

	backend.wrongTurn = false
	accept(t, s, storetest.Message("fresh-question", "fresh-root", "alice"))
	work(t, w)
	created, queried := backend.snapshot()
	if len(created) != 2 || len(queried) != 2 || queried[0].Query.ConversationID == queried[1].Query.ConversationID {
		t.Fatalf("closed work replayed or fresh root was blocked: creates=%v queries=%+v", created, queried)
	}
}
