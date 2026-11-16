package gateway

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type epochWorkerStore struct {
	Store
	job          *GatewayJob
	binding      *ConversationBinding
	started      int
	rejected     string
	reviewed     string
	retained     int
	completed    int
	insideOwner  bool
	ownerEntries int
}

func newEpochWorkerStore() *epochWorkerStore {
	return &epochWorkerStore{
		job: &GatewayJob{ID: 1, GatewayRequestID: "run", State: "pending", Message: Message{
			ThreadKey: ThreadKey{WorkspaceID: "T", ChannelID: "C", RootTS: "1"}, AppID: "A", AuthorID: "alice", Text: "question",
		}},
		binding: &ConversationBinding{ID: 2, Decision: Decision{PrincipalID: "shared"}, State: "reserved"},
	}
}

func (s *epochWorkerStore) ClaimJob(context.Context) (*GatewayJob, error) { return s.job, nil }
func (s *epochWorkerStore) GetBinding(context.Context, ThreadKey) (*ConversationBinding, error) {
	return s.binding, nil
}
func (s *epochWorkerStore) BindJob(context.Context, int64, Decision) (*ConversationBinding, error) {
	if s.rejected != "" {
		return nil, ErrBackendUnavailable
	}
	return s.binding, nil
}
func (s *epochWorkerStore) BeginCreate(context.Context, int64) error {
	s.binding.State = "creating"
	return nil
}
func (s *epochWorkerStore) SetConversation(ctx context.Context, _ int64, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.binding.ConversationID, s.binding.State = id, "ready"
	return nil
}
func (s *epochWorkerStore) BeginQuery(ctx context.Context, _ int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.started++
	return nil
}
func (s *epochWorkerStore) Reject(_ context.Context, _ int64, reason string) error {
	if s.rejected != "" {
		return ErrBackendUnavailable
	}
	s.rejected = reason
	return nil
}
func (s *epochWorkerStore) Review(_ context.Context, _ int64, reason string) error {
	s.reviewed = reason
	return nil
}
func (s *epochWorkerStore) RecordObservation(context.Context, int64, string, time.Time) error {
	return nil
}
func (s *epochWorkerStore) Complete(context.Context, int64, string, string, []ReplyPart, ...ProtocolSnapshot) error {
	s.completed++
	return nil
}
func (s *epochWorkerStore) RetainThread() func() {
	s.retained++
	return func() { s.retained-- }
}
func (s *epochWorkerStore) InThread(ctx context.Context, fn func(Store) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.insideOwner {
		return errors.New("reentered thread owner")
	}
	s.insideOwner = true
	s.ownerEntries++
	defer func() { s.insideOwner = false }()
	// Return only the local native methods, not the queued owner facade.
	return fn(s)
}

type epochWorkerBackend struct {
	BackendLifecycle
	mu      sync.Mutex
	epoch   uint64
	state   BackendState
	store   *epochWorkerStore
	client  AgentlyClient
	acquire func(int) (AgentlyClient, error)
	calls   int
}

func (b *epochWorkerBackend) State() BackendState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
func (b *epochWorkerBackend) WithReady(fn func() error) error {
	return b.WithReadyEpoch(func(uint64) error { return fn() })
}
func (b *epochWorkerBackend) WithReadyEpoch(fn func(uint64) error) error {
	if !b.store.insideOwner {
		return errors.New("readiness acquired before thread owner")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != BackendReady {
		return ErrBackendUnavailable
	}
	return fn(b.epoch)
}
func (b *epochWorkerBackend) ForPrincipal(ctx context.Context, _ string) (AgentlyClient, error) {
	if b.store.insideOwner {
		return nil, errors.New("client acquisition inside thread owner")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.calls++
	if b.acquire != nil {
		return b.acquire(b.calls)
	}
	return b.client, nil
}
func (b *epochWorkerBackend) outageAndRecover() {
	b.mu.Lock()
	b.epoch++
	b.state = BackendReady
	b.mu.Unlock()
}

type epochWorkerClient struct {
	AgentlyClient
	create  func(context.Context) (string, error)
	queries int
}

func (c *epochWorkerClient) CreateConversation(ctx context.Context, _ string) (string, error) {
	return c.create(ctx)
}
func (c *epochWorkerClient) Query(_ context.Context, q Query, _ func(Turn) error) (Turn, error) {
	c.queries++
	return Turn{ID: q.MessageID, State: "completed", Content: "answer"}, nil
}

type epochWorkerPolicy struct{ authorize func() }

func (p epochWorkerPolicy) Authorize(context.Context, Message, *ConversationBinding) (Decision, error) {
	if p.authorize != nil {
		p.authorize()
	}
	return Decision{PrincipalID: "shared"}, nil
}

func TestWorkerOutageDuringCreateCannotSubmitAfterRecovery(t *testing.T) {
	for _, outageAt := range []string{"create", "fresh_client"} {
		t.Run(outageAt, func(t *testing.T) {
			s := newEpochWorkerStore()
			backend := &epochWorkerBackend{state: BackendReady, store: s}
			client := &epochWorkerClient{create: func(context.Context) (string, error) {
				if s.insideOwner || s.retained == 0 {
					t.Fatal("Create held owner or lost its continuation reference")
				}
				if outageAt == "create" {
					backend.outageAndRecover()
				}
				return "confirmed-conversation", nil
			}}
			backend.client = client
			backend.acquire = func(call int) (AgentlyClient, error) {
				if call == 2 && outageAt == "fresh_client" {
					backend.outageAndRecover()
				}
				return client, nil
			}
			worked, err := (Worker{Store: s, Policy: epochWorkerPolicy{}, Backend: backend, Clients: backend}).Step(context.Background())
			if err != nil || !worked || s.binding.ConversationID != "confirmed-conversation" || s.rejected != "backend_unavailable" || s.started != 0 || client.queries != 0 || s.retained != 0 {
				t.Fatalf("outage revived unsubmitted work: worked=%t id=%s rejection=%s starts=%d queries=%d refs=%d err=%v", worked, s.binding.ConversationID, s.rejected, s.started, client.queries, s.retained, err)
			}
		})
	}
}

func TestWorkerUsesFreshClientAndRetainsAsyncContinuation(t *testing.T) {
	s := newEpochWorkerStore()
	original := &epochWorkerClient{create: func(context.Context) (string, error) { return "confirmed", nil }}
	fresh := &epochWorkerClient{}
	backend := &epochWorkerBackend{state: BackendReady, store: s, acquire: func(call int) (AgentlyClient, error) {
		if call == 1 {
			return original, nil
		}
		return fresh, nil
	}}
	var continuation func() error
	worked, err := (Worker{Store: s, Policy: epochWorkerPolicy{}, Backend: backend, Clients: backend, Execute: func(fn func() error) { continuation = fn }}).Step(context.Background())
	if err != nil || !worked || continuation == nil || s.retained != 1 || s.started != 1 || original.queries != 0 {
		t.Fatalf("continuation ownership: worked=%t refs=%d starts=%d err=%v", worked, s.retained, s.started, err)
	}
	if err = continuation(); err != nil || s.retained != 0 || original.queries != 0 || fresh.queries != 1 || s.completed != 1 {
		t.Fatalf("fresh continuation: refs=%d old=%d fresh=%d completed=%d err=%v", s.retained, original.queries, fresh.queries, s.completed, err)
	}
}

func TestWorkerCancellationPreservesConfirmedCreateID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := newEpochWorkerStore()
	client := &epochWorkerClient{create: func(context.Context) (string, error) { cancel(); return "confirmed-before-cancel", nil }}
	backend := &epochWorkerBackend{state: BackendReady, store: s, client: client}
	worked, err := (Worker{Store: s, Policy: epochWorkerPolicy{}, Backend: backend, Clients: backend}).Step(ctx)
	if !worked || !errors.Is(err, context.Canceled) || s.binding.ConversationID != "confirmed-before-cancel" || s.started != 0 || client.queries != 0 || s.reviewed != "" || s.retained != 0 {
		t.Fatalf("known create cancellation: worked=%t id=%s starts=%d review=%s refs=%d err=%v", worked, s.binding.ConversationID, s.started, s.reviewed, s.retained, err)
	}
}

func TestWorkerLatePolicyCannotReviveLocallyClosedQuestion(t *testing.T) {
	s := newEpochWorkerStore()
	client := &epochWorkerClient{create: func(context.Context) (string, error) {
		t.Fatal("closed question created a conversation")
		return "", nil
	}}
	backend := &epochWorkerBackend{state: BackendReady, store: s, client: client}
	policy := epochWorkerPolicy{authorize: func() {
		if s.insideOwner {
			t.Fatal("policy HTTP inside thread owner")
		}
		backend.outageAndRecover()
		s.rejected = "backend_unavailable"
	}}
	worked, err := (Worker{Store: s, Policy: policy, Backend: backend, Clients: backend}).Step(context.Background())
	if err != nil || !worked || s.started != 0 || client.queries != 0 || backend.calls != 0 || s.retained != 0 {
		t.Fatalf("late policy revived work: worked=%t calls=%d starts=%d refs=%d err=%v", worked, backend.calls, s.started, s.retained, err)
	}
}
