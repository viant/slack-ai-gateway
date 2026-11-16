package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

type outboundPhaseStore struct {
	Store
	t       *testing.T
	kind    string
	inside  bool
	claimed bool
	done    string
	status  StatusResult
}

func (s *outboundPhaseStore) InThread(ctx context.Context, fn func(Store) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.inside {
		s.t.Fatal("nested owner command")
	}
	s.inside = true
	alive := true
	defer func() { alive = false; s.inside = false }()
	return fn(&outboundPhaseLocal{Store: s.Store, parent: s, alive: &alive})
}

type outboundPhaseLocal struct {
	Store
	parent *outboundPhaseStore
	alive  *bool
}

func (s *outboundPhaseLocal) check() {
	if !*s.alive || !s.parent.inside {
		s.parent.t.Fatal("prepared operation retained an expired native facade")
	}
}
func (s *outboundPhaseLocal) ClaimDelivery(context.Context) (*SlackOutgoingMessage, error) {
	s.check()
	if s.parent.kind != "delivery" || s.parent.claimed {
		return nil, nil
	}
	s.parent.claimed = true
	return &SlackOutgoingMessage{ID: 4, JobID: 2, Text: "answer", ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"}}, nil
}
func (s *outboundPhaseLocal) GetDeliveryContext(context.Context, SlackOutgoingMessage) (*DeliveryContext, error) {
	s.check()
	return &DeliveryContext{Message: Message{AuthorID: "original-author"}}, nil
}
func (s *outboundPhaseLocal) Delivered(context.Context, int64, string) error {
	s.check()
	s.parent.done = "delivered"
	return nil
}
func (s *outboundPhaseLocal) RetryDelivery(context.Context, int64, time.Time) error {
	s.check()
	s.parent.done = "pending"
	return nil
}
func (s *outboundPhaseLocal) DeliveryUnknown(context.Context, int64) error {
	s.check()
	s.parent.done = "delivery_unknown"
	return nil
}
func (s *outboundPhaseLocal) FailDelivery(context.Context, int64) error {
	s.check()
	s.parent.done = "failed"
	return nil
}
func (s *outboundPhaseLocal) ClaimStatus(_ context.Context, now time.Time, kind string) (*GatewayJobProgress, error) {
	s.check()
	if s.parent.kind == "delivery" || kind != "urgent" || s.parent.claimed {
		return nil, nil
	}
	s.parent.claimed = true
	target := StatusTargetMessage
	if s.parent.kind == "agent_session" {
		target = StatusTargetAgentSession
	}
	return &GatewayJobProgress{JobID: 2, Target: target, Phase: "running", CreatedAt: now, SessionState: "processing"}, nil
}
func (s *outboundPhaseLocal) GetStatusContext(context.Context, GatewayJobProgress) (*DeliveryContext, error) {
	s.check()
	return &DeliveryContext{Message: Message{AuthorID: "original-author"}}, nil
}
func (s *outboundPhaseLocal) FinishStatus(_ context.Context, _ GatewayJobProgress, result StatusResult, _ time.Time) error {
	s.check()
	s.parent.status = result
	s.parent.done = result.State
	return nil
}

type outboundPhasePolicy struct {
	store  *outboundPhaseStore
	cancel context.CancelFunc
	calls  int
}

func (p *outboundPhasePolicy) AuthorizeDelivery(_ context.Context, metadata DeliveryContext, _ bool) error {
	if p.store.inside || metadata.Message.AuthorID != "original-author" {
		p.store.t.Fatal("policy HTTP held owner or substituted author")
	}
	p.calls++
	if p.cancel != nil {
		p.cancel()
	}
	return nil
}

type outboundPhaseSender struct {
	store *outboundPhaseStore
	calls int
	err   error
}

func (s *outboundPhaseSender) Send(context.Context, SlackOutgoingMessage) (string, error) {
	if s.store.inside {
		s.store.t.Fatal("Slack HTTP held thread owner")
	}
	s.calls++
	return "3.000001", s.err
}
func (s *outboundPhaseSender) SendStatus(_ context.Context, status GatewayJobProgress, _ string) (StatusReceipt, error) {
	if s.store.inside {
		s.store.t.Fatal("Slack status HTTP held thread owner")
	}
	s.calls++
	if status.Target == StatusTargetAgentSession {
		return StatusReceipt{Acknowledged: true, AgentStatus: "processing"}, s.err
	}
	return StatusReceipt{SlackTS: "3.000001"}, s.err
}

func phaseOutbound(t *testing.T, kind string) (*OutboundWorker, *outboundPhaseStore, *outboundPhaseSender, *outboundPhasePolicy) {
	t.Helper()
	store := &outboundPhaseStore{t: t, kind: kind}
	sender := &outboundPhaseSender{store: store}
	policy := &outboundPhasePolicy{store: store}
	worker := &OutboundWorker{Interval: time.Second,
		SlackOutgoingMessage: &DeliveryWorker{Store: store, Sender: sender, Policy: policy},
		Status:               &StatusWorker{Store: store, Sender: sender, Policy: policy},
	}
	return worker, store, sender, policy
}

func TestOutboundStagesReleaseOwnerBeforePolicyAndNetwork(t *testing.T) {
	for _, kind := range []string{"delivery", "message", "agent_session"} {
		t.Run(kind, func(t *testing.T) {
			worker, store, sender, policy := phaseOutbound(t, kind)
			var prepared *PreparedOutbound
			err := InThread(context.Background(), store, func(local Store) error {
				var err error
				prepared, err = worker.Prepare(context.Background(), local)
				return err
			})
			if err != nil || prepared == nil || sender.calls != 0 || policy.calls != 0 {
				t.Fatalf("prepare performed network work: policy=%d sends=%d err=%v", policy.calls, sender.calls, err)
			}
			result := prepared.Send(context.Background())
			if result.Err != nil || policy.calls != 1 || sender.calls != 1 || store.done != "" {
				t.Fatalf("send wrote storage or lost policy: result=%v done=%s", result.Err, store.done)
			}
			if err := commitOutbound(context.Background(), store, result); err != nil || store.done == "" {
				t.Fatalf("receipt was not committed with a new local facade: done=%s err=%v", store.done, err)
			}
		})
	}
}

func TestOutboundCancellationBeforeHTTPIsKnownNotSent(t *testing.T) {
	for _, kind := range []string{"delivery", "message", "agent_session"} {
		for _, cancelAt := range []string{"before_send", "policy"} {
			t.Run(kind+"/"+cancelAt, func(t *testing.T) {
				worker, store, sender, policy := phaseOutbound(t, kind)
				var prepared *PreparedOutbound
				if err := InThread(context.Background(), store, func(local Store) error {
					var err error
					prepared, err = worker.Prepare(context.Background(), local)
					return err
				}); err != nil || prepared == nil {
					t.Fatalf("prepare: %v", err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if cancelAt == "policy" {
					policy.cancel = cancel
				} else {
					cancel()
				}
				result := prepared.Send(ctx)
				err := commitOutbound(ctx, store, result)
				want := "ready"
				if kind == "delivery" {
					want = "pending"
				}
				if !errors.Is(err, context.Canceled) || sender.calls != 0 || store.done != want {
					t.Fatalf("known-no-send became unknown: done=%s sends=%d err=%v", store.done, sender.calls, err)
				}
			})
		}
	}
}

func TestOutboundRateLimitSurvivesReceiptAndGatesOtherThreads(t *testing.T) {
	worker, store, sender, _ := phaseOutbound(t, "delivery")
	sender.err = &SendError{Kind: SendRateLimited, RetryAfter: time.Minute}
	var prepared *PreparedOutbound
	if err := InThread(context.Background(), store, func(local Store) error {
		var err error
		prepared, err = worker.Prepare(context.Background(), local)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := time.Now()
	result := prepared.Send(context.Background())
	if err := commitOutbound(context.Background(), store, result); err != nil || store.done != "pending" || result.NotBefore.Before(before.Add(time.Minute)) {
		t.Fatalf("rate limit receipt: pending=%s notBefore=%s err=%v", store.done, result.NotBefore, err)
	}
}
