package gateway_test

import (
	"context"
	"fmt"
	"sync"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type queryCall struct {
	Principal string
	Query     gateway.Query
}

// These fakes only compile into tests, never the gateway binary.
type fakeAgently struct {
	mu                                  sync.Mutex
	created                             []string
	queries                             []queryCall
	state                               string
	credentialsErr, createErr, queryErr error
	wrongTurn                           bool
	started                             chan gateway.Query
	release                             <-chan struct{}
}

type fakeClient struct {
	backend   *fakeAgently
	principal string
}

func (f *fakeAgently) ForPrincipal(_ context.Context, p string) (gateway.AgentlyClient, error) {
	if f.credentialsErr != nil {
		return nil, f.credentialsErr
	}
	return &fakeClient{backend: f, principal: p}, nil
}

func (c *fakeClient) CreateConversation(_ context.Context, _ string) (string, error) {
	f := c.backend
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, c.principal)
	if f.createErr != nil {
		return "", f.createErr
	}
	return fmt.Sprintf("conversation-%d", len(f.created)), nil
}

func (c *fakeClient) Query(ctx context.Context, q gateway.Query, _ func(gateway.Turn) error) (gateway.Turn, error) {
	f := c.backend
	f.mu.Lock()
	f.queries = append(f.queries, queryCall{Principal: c.principal, Query: q})
	f.mu.Unlock()
	if f.started != nil {
		f.started <- q
	}
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return gateway.Turn{}, ctx.Err()
		}
	}
	if f.queryErr != nil {
		return gateway.Turn{}, f.queryErr
	}
	state := f.state
	if state == "" {
		state = "completed"
	}
	id := q.MessageID
	if f.wrongTurn {
		id = "unrelated-turn"
	}
	return gateway.Turn{ID: id, State: state, Content: "TEST RESPONSE: " + q.Text}, nil
}

func (c *fakeClient) ReadTurn(context.Context, string, string, func(gateway.Turn) error) (gateway.Turn, error) {
	return gateway.Turn{State: "unknown"}, nil
}

func (f *fakeAgently) snapshot() ([]string, []queryCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.created...), append([]queryCall(nil), f.queries...)
}

type fakeSlack struct {
	mu       sync.Mutex
	messages []gateway.SlackOutgoingMessage
	err      error
}

func (s *fakeSlack) Send(_ context.Context, d gateway.SlackOutgoingMessage) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, d)
	if s.err != nil {
		return "", s.err
	}
	return fmt.Sprintf("100.%06d", len(s.messages)), nil
}

func (s *fakeSlack) snapshot() []gateway.SlackOutgoingMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]gateway.SlackOutgoingMessage(nil), s.messages...)
}
