package gateway_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type lifecycleSlack struct {
	statuses   []statusCall
	deliveries []gateway.SlackOutgoingMessage
}

func (s *lifecycleSlack) Send(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
	s.deliveries = append(s.deliveries, d)
	return fmt.Sprintf("300.%06d", len(s.deliveries)), nil
}

func (s *lifecycleSlack) SendStatus(_ context.Context, status gateway.GatewayJobProgress, text string) (gateway.StatusReceipt, error) {
	s.statuses = append(s.statuses, statusCall{Status: status, Text: text})
	if status.Target == gateway.StatusTargetAgentSession {
		return gateway.StatusReceipt{Acknowledged: true, AgentStatus: status.SessionState, SessionStatus: status.SessionState}, nil
	}
	if status.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: status.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: fmt.Sprintf("200.%06d", len(s.statuses))}, nil
}

func TestConfigurationChangeClosesThreadWithoutQueryReplay(t *testing.T) {
	for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			root, err := sqlite.Open(ctx, filepath.Join(t.TempDir(), "configuration.sqlite"), true)
			storetest.Must(t, err)
			defer root.Close()
			storetest.Must(t, root.Migrate(ctx))
			original := storetest.DefaultIntegration()
			original.Name, original.PrincipalID = "steward", "old-principal"
			rows, err := root.ReconcileIntegrations(ctx, []storage.Integration{original})
			storetest.Must(t, err)
			oldID := rows[0].ID
			oldScope := root.Scope(oldID)
			oldPolicy := multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{})
			oldPolicy.PrincipalID = original.PrincipalID
			m := storetest.Message("configuration-original", "100.000001", "alice")
			result, err := (gateway.Intake{Store: oldScope, Route: oldPolicy.Route, StatusMode: mode}).Accept(ctx, m)
			storetest.Must(t, err)
			if !result.Accepted || result.Duplicate {
				t.Fatal("original prompt was not admitted")
			}
			slack := &lifecycleSlack{}
			oldStatus := &gateway.StatusWorker{Store: oldScope, Sender: slack, Policy: oldPolicy}
			for range 3 {
				worked, err := oldStatus.Step(ctx, "urgent")
				storetest.Must(t, err)
				if !worked {
					break
				}
			}
			oldBackend := &fakeAgently{state: "running"}
			worked, err := (gateway.Worker{Store: oldScope, Policy: oldPolicy, Clients: oldBackend, AgentID: "analyst"}).Step(ctx)
			storetest.Must(t, err)
			if !worked {
				t.Fatal("old execution did not begin")
			}

			changed := original
			changed.PrincipalID, changed.BackendAgentID = "new-principal", "support"
			rows, err = root.ReconcileIntegrations(ctx, []storage.Integration{changed})
			storetest.Must(t, err)
			if len(rows) != 1 || rows[0].ID == oldID {
				t.Fatal("changed assignment reused historical identity")
			}
			current := root.Scope(rows[0].ID)
			currentPolicy := multiPolicy(t, []string{"C"}, nil, nil, &multiDirectory{})
			currentPolicy.PrincipalID = changed.PrincipalID
			state, err := root.Status(ctx, 100)
			storetest.Must(t, err)
			if len(state.GatewayJobs) != 1 || state.GatewayJobs[0].State != "rejected" || state.GatewayJobs[0].Reason != "integration_changed" || state.GatewayJobs[0].BackendSubmissionStartedAt == 0 || len(state.ConversationBindings) != 1 || state.ConversationBindings[0].State != "closed" {
				t.Fatalf("old conversation was not closed: jobs=%+v bindings=%+v", state.GatewayJobs, state.ConversationBindings)
			}
			intake := gateway.Intake{Store: current, Route: currentPolicy.Route, StatusMode: mode}
			retry, err := intake.Accept(ctx, m)
			storetest.Must(t, err)
			if !retry.Duplicate {
				t.Fatal("ACK retry created a second job after reassignment")
			}
			outbound := &gateway.OutboundWorker{
				SlackOutgoingMessage: &gateway.DeliveryWorker{Store: current, Sender: slack, Policy: currentPolicy},
				Status:               &gateway.StatusWorker{Store: current, Sender: slack, Policy: currentPolicy},
			}
			drain := func() {
				t.Helper()
				for range 30 {
					worked, err := outbound.Step(ctx)
					storetest.Must(t, err)
					if !worked {
						return
					}
				}
				t.Fatal("outbound kept republishing terminated work")
			}
			drain()
			if len(slack.deliveries) != 1 || slack.deliveries[0].Kind != gateway.DeliveryIntegrationNotice || !strings.Contains(slack.deliveries[0].Text, "configuration changed") {
				t.Fatalf("missing closure notice through current Slack policy: %+v", slack.deliveries)
			}
			var messageClosed, nativeActive bool
			for _, call := range slack.statuses {
				messageClosed = messageClosed || call.Status.Target != gateway.StatusTargetAgentSession && call.Status.Phase == "integration_changed" && strings.Contains(call.Text, "configuration changed")
				nativeActive = nativeActive || call.Status.Target == gateway.StatusTargetAgentSession && call.Status.SessionState == "active"
			}
			if mode != gateway.StatusAgentSession && !messageClosed || mode != gateway.StatusMessage && !nativeActive {
				t.Fatalf("progress did not terminate for %s: %+v", mode, slack.statuses)
			}

			followup := storetest.Message("configuration-stale-followup", m.RootTS, "alice")
			followup.MessageTS = "100.000002"
			_, err = intake.Accept(ctx, followup)
			storetest.Must(t, err)
			newBackend := &fakeAgently{}
			worker := gateway.Worker{Store: current, Policy: currentPolicy, Clients: newBackend, AgentID: "support"}
			for range 2 {
				_, err := worker.Step(ctx)
				storetest.Must(t, err)
			}
			_, queries := newBackend.snapshot()
			if len(queries) != 0 {
				t.Fatal("stale thread submitted a prompt to the new agent")
			}
			drain()
			if len(slack.deliveries) != 2 || !strings.Contains(slack.deliveries[1].Text, "new thread") {
				t.Fatal("stale followup did not receive a local closed-conversation response")
			}

			fresh := storetest.Message("configuration-new-thread", "400.000001", "alice")
			_, err = intake.Accept(ctx, fresh)
			storetest.Must(t, err)
			worked, err = worker.Step(ctx)
			storetest.Must(t, err)
			if !worked {
				t.Fatal("new thread did not start")
			}
			_, queries = newBackend.snapshot()
			_, oldQueries := oldBackend.snapshot()
			if len(queries) != 1 || queries[0].Query.AgentID != "support" || queries[0].Principal != "new-principal" || len(oldQueries) != 1 {
				t.Fatal("new thread used incorrect agent or replayed old execution")
			}
			state, err = root.Status(ctx, 100)
			storetest.Must(t, err)
			for _, status := range state.GatewayJobs {
				if status.IntegrationID == oldID && (status.Phase == "running" || status.Phase == "contacting") {
					t.Fatal("historical progress still advertises execution")
				}
			}
		})
	}
}
