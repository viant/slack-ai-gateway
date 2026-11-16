package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/sqlite"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type actorSend struct {
	app, root string
	at        time.Time
}

type actorOutboundSender struct {
	mu    sync.Mutex
	calls map[string]int
	sent  chan actorSend
	hook  func(context.Context, string, gateway.SlackOutgoingMessage, int) (string, error)
}

func (s *actorOutboundSender) Send(ctx context.Context, app string, d gateway.SlackOutgoingMessage) (string, error) {
	s.mu.Lock()
	s.calls[app]++
	call := s.calls[app]
	s.mu.Unlock()
	s.sent <- actorSend{app, d.RootTS, time.Now()}
	if s.hook != nil {
		return s.hook(ctx, app, d, call)
	}
	return fmt.Sprintf("%d.000001", call), nil
}

type actorBotSender struct {
	app    string
	shared *actorOutboundSender
}

func (s actorBotSender) Send(ctx context.Context, d gateway.SlackOutgoingMessage) (string, error) {
	return s.shared.Send(ctx, s.app, d)
}
func (actorBotSender) SendStatus(_ context.Context, status gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	if status.Target == gateway.StatusTargetAgentSession {
		return gateway.StatusReceipt{Acknowledged: true, AgentStatus: status.SessionState}, nil
	}
	if status.SlackTS != "" {
		return gateway.StatusReceipt{SlackTS: status.SlackTS}, nil
	}
	return gateway.StatusReceipt{SlackTS: "900.000001"}, nil
}

type deferredOwnerCommand struct {
	ctx      context.Context
	run      func(gateway.Store) error
	complete func(error)
}

// This facade only controls admission timing; every executed command still uses
// the actual SQLite/Datly thread registry and the production gateway workers.
type controlledActorStore struct {
	gateway.Store
	mu          sync.Mutex
	calls       int
	hold        int
	entered     chan struct{}
	deferred    *deferredOwnerCommand
	commitError error
}

func (s *controlledActorStore) RetainThread() func() { return gateway.RetainThread(s.Store) }
func (s *controlledActorStore) TryInThread(ctx context.Context, run func(gateway.Store) error, complete func(error)) bool {
	s.mu.Lock()
	s.calls++
	call := s.calls
	failure := s.commitError
	if call == 2 && failure != nil {
		run = func(gateway.Store) error { return failure }
	}
	if call == s.hold {
		s.deferred = &deferredOwnerCommand{ctx, run, complete}
		close(s.entered)
		s.mu.Unlock()
		return true
	}
	s.mu.Unlock()
	return s.Store.(gateway.AsyncThreadStore).TryInThread(ctx, run, complete)
}
func (s *controlledActorStore) release() {
	s.mu.Lock()
	command := s.deferred
	s.deferred = nil
	s.mu.Unlock()
	if command == nil {
		return
	}
	if err := command.ctx.Err(); err != nil {
		command.complete(err)
		return
	}
	if !s.Store.(gateway.AsyncThreadStore).TryInThread(command.ctx, command.run, command.complete) {
		command.complete(errors.New("fixture owner admission unexpectedly full"))
	}
}

type actorOutboundFixture struct {
	root          *sqlite.Store
	scheduler     *workScheduler
	ctx           context.Context
	cancel        context.CancelFunc
	sender        *actorOutboundSender
	identities    []storage.Integration
	scopes        map[int64]storage.Store
	controls      map[gateway.PhysicalThreadKey]*controlledActorStore
	registrations []*scheduledRuntime
	cleanupHooks  []func()
	mu            sync.Mutex
}

func newActorOutboundFixture(t *testing.T, bots, threads int) *actorOutboundFixture {
	t.Helper()
	root, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "outbound.sqlite"), true)
	storetest.Must(t, err)
	storetest.Must(t, root.Migrate(context.Background()))
	identities := make([]storage.Integration, bots)
	for i := range identities {
		identities[i] = storetest.DefaultIntegration()
		identities[i].Name = fmt.Sprintf("bot-%d", i)
		identities[i].AppID = fmt.Sprintf("A%d", i)
	}
	identities, err = root.ReconcileIntegrations(context.Background(), identities)
	storetest.Must(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	f := &actorOutboundFixture{root: root, ctx: ctx, cancel: cancel, identities: identities,
		sender: &actorOutboundSender{calls: map[string]int{}, sent: make(chan actorSend, 128)},
		scopes: map[int64]storage.Store{}, controls: map[gateway.PhysicalThreadKey]*controlledActorStore{}}
	for _, identity := range identities {
		scope := root.Scope(identity.ID)
		f.scopes[identity.ID] = scope
		for thread := range threads {
			message := storetest.Message(fmt.Sprintf("%s-event-%d", identity.AppID, thread), fmt.Sprintf("thread-%d", thread), "alice")
			message.AppID = identity.AppID
			bound := gateway.ForThread(scope, message.PhysicalThread())
			_, err := bound.Enqueue(ctx, message)
			storetest.Must(t, err)
			job, err := bound.ClaimJob(ctx)
			storetest.Must(t, err)
			if job == nil {
				t.Fatal("native fixture did not claim its thread")
			}
			storetest.Must(t, bound.Complete(ctx, job.ID, job.GatewayRequestID, "reply", gateway.RenderText("reply")))
		}
	}
	root.StartThreadOwnership()
	f.scheduler = newWorkScheduler(root)
	t.Cleanup(func() {
		for _, release := range f.cleanupHooks {
			release()
		}
		f.cancel()
		f.mu.Lock()
		for _, control := range f.controls {
			control.mu.Lock()
			control.hold = 0
			control.mu.Unlock()
			control.release()
		}
		f.mu.Unlock()
		for _, registration := range f.registrations {
			f.scheduler.unregister(registration.id)
		}
		for _, registration := range f.registrations {
			registration.workers.Wait()
		}
		storetest.Must(t, f.scheduler.close())
		storetest.Must(t, f.root.Close())
	})
	return f
}

func (f *actorOutboundFixture) control(identity storage.Integration, root string) *controlledActorStore {
	key := gateway.PhysicalThreadKey{AppID: identity.AppID, WorkspaceID: "T", ChannelID: "C", RootTS: root}
	f.mu.Lock()
	defer f.mu.Unlock()
	control := f.controls[key]
	if control == nil {
		control = &controlledActorStore{Store: gateway.ForThread(f.scopes[identity.ID], key), entered: make(chan struct{})}
		f.controls[key] = control
	}
	return control
}

func (f *actorOutboundFixture) register(identity storage.Integration, policy gateway.DeliveryPolicy) *scheduledRuntime {
	r := f.registration(identity, policy)
	f.scheduler.register(r)
	return r
}

func (f *actorOutboundFixture) registration(identity storage.Integration, policy gateway.DeliveryPolicy) *scheduledRuntime {
	if policy == nil {
		policy = storetest.Policy(identity.AppID, "T", "shared", "C")
	}
	sender := actorBotSender{identity.AppID, f.sender}
	outbound := &gateway.OutboundWorker{Interval: time.Second,
		SlackOutgoingMessage: &gateway.DeliveryWorker{Sender: sender, Policy: policy},
		Status:               &gateway.StatusWorker{Sender: sender, Policy: policy},
	}
	r := schedulerRegistration(identity.ID, emptyScheduledStep, emptyScheduledStep)
	r.ctx = f.ctx
	r.canStart = func() bool { return f.ctx.Err() == nil }
	r.threadStore = func(key gateway.PhysicalThreadKey) gateway.Store { return f.control(identity, key.RootTS) }
	r.prepareOutbound = func(ctx context.Context, _ gateway.PhysicalThreadKey, local gateway.Store) (*gateway.PreparedOutbound, error) {
		return outbound.Prepare(ctx, local)
	}
	f.registrations = append(f.registrations, r)
	return r
}

func actorAwait[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(10 * time.Second):
		t.Fatal("actor outbound did not reach its controlled boundary")
		var zero T
		return zero
	}
}

type actorPolicyGate struct {
	gateway.DeliveryPolicy
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *actorPolicyGate) AuthorizeDelivery(ctx context.Context, metadata gateway.DeliveryContext, fresh bool) error {
	p.once.Do(func() { close(p.entered) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.release:
		return p.DeliveryPolicy.AuthorizeDelivery(ctx, metadata, fresh)
	}
}

func TestActorOutboundPrepareWaitDoesNotOccupyHTTPExecutors(t *testing.T) {
	f := newActorOutboundFixture(t, 5, 1)
	gate := &actorPolicyGate{DeliveryPolicy: storetest.Policy("A4", "T", "shared", "C"), entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	f.cleanupHooks = append(f.cleanupHooks, release)
	f.register(f.identities[4], gate)
	actorAwait(t, gate.entered)
	for _, identity := range f.identities[:4] {
		f.control(identity, "thread-0").hold = 1
		f.register(identity, nil)
	}
	for _, identity := range f.identities[:4] {
		actorAwait(t, f.control(identity, "thread-0").entered)
	}
	release()
	if sent := actorAwait(t, f.sender.sent); sent.app != "A4" {
		t.Fatalf("blocked owner preparation consumed network executors: %+v", sent)
	}
	for _, identity := range f.identities[:4] {
		f.control(identity, "thread-0").release()
	}
	for range 4 {
		actorAwait(t, f.sender.sent)
	}
}

func TestActorOutboundReceiptReleasesHTTPButRetainsBotPermitAndCooldown(t *testing.T) {
	f := newActorOutboundFixture(t, 5, 2)
	for _, root := range []string{"thread-0", "thread-1"} {
		f.control(f.identities[0], root).hold = 2
	}
	releaseHTTP := make(chan struct{})
	var once sync.Once
	f.cleanupHooks = append(f.cleanupHooks, func() { once.Do(func() { close(releaseHTTP) }) })
	f.sender.hook = func(ctx context.Context, app string, _ gateway.SlackOutgoingMessage, call int) (string, error) {
		if app == "A0" && call == 1 {
			return "", &gateway.SendError{Kind: gateway.SendRateLimited, RetryAfter: 2 * time.Second}
		}
		if app != "A0" {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-releaseHTTP:
			}
		}
		return fmt.Sprintf("%d.000001", call), nil
	}
	f.register(f.identities[0], nil)
	first := actorAwait(t, f.sender.sent)
	control := f.control(f.identities[0], first.root)
	actorAwait(t, control.entered)
	for _, identity := range f.identities[1:] {
		f.register(identity, nil)
	}
	for range 4 {
		if sent := actorAwait(t, f.sender.sent); sent.app == "A0" {
			t.Fatal("same bot sent another thread before its receipt was persisted")
		}
	}
	// All four network executors reached other bots while the first bot's
	// receipt remained waiting for owner admission.
	once.Do(func() { close(releaseHTTP) })
	control.release()
	var distinct bool
	for !distinct {
		sent := actorAwait(t, f.sender.sent)
		if sent.app == "A0" {
			if sent.at.Before(first.at.Add(2 * time.Second)) {
				t.Fatal("persisted bot Retry-After was shortened")
			}
			distinct = sent.root != first.root
		}
	}
}

func TestActorOutboundCommitFailureIsReportedBeforeJoin(t *testing.T) {
	f := newActorOutboundFixture(t, 1, 1)
	want := errors.New("synthetic receipt persistence failure")
	control := f.control(f.identities[0], "thread-0")
	control.hold, control.commitError = 2, want
	r := f.registration(f.identities[0], nil)
	reported, release := make(chan error, 1), make(chan struct{})
	var once sync.Once
	f.cleanupHooks = append(f.cleanupHooks, func() { once.Do(func() { close(release) }) })
	r.report = func(_ context.Context, err error) bool {
		if err == nil {
			return false
		}
		reported <- err
		<-release
		return true
	}
	f.scheduler.register(r)
	actorAwait(t, f.sender.sent)
	actorAwait(t, control.entered)
	joined := make(chan struct{})
	go func() { r.workers.Wait(); close(joined) }()
	control.release()
	if err := actorAwait(t, reported); !errors.Is(err, want) {
		t.Fatalf("receipt failure = %v", err)
	}
	select {
	case <-joined:
		t.Fatal("join returned before the storage failure was reported")
	default:
	}
	once.Do(func() { close(release) })
	actorAwait(t, joined)
}

func TestActorOutboundUnregisterStillCommitsKnownSuccessAfterCancellation(t *testing.T) {
	f := newActorOutboundFixture(t, 1, 1)
	release := make(chan struct{})
	var once sync.Once
	f.cleanupHooks = append(f.cleanupHooks, func() { once.Do(func() { close(release) }) })
	f.sender.hook = func(context.Context, string, gateway.SlackOutgoingMessage, int) (string, error) {
		<-release
		return "4.000001", nil
	}
	r := f.register(f.identities[0], nil)
	actorAwait(t, f.sender.sent)
	f.scheduler.unregister(r.id)
	f.cancel()
	joined := make(chan struct{})
	go func() { r.workers.Wait(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("unregister lost a started network operation")
	default:
	}
	once.Do(func() { close(release) })
	actorAwait(t, joined)
	state, err := f.root.Status(context.Background(), 10)
	storetest.Must(t, err)
	if len(state.SlackOutgoingMessages) != 1 || state.SlackOutgoingMessages[0].State != "delivered" || state.SlackOutgoingMessages[0].SlackTS != "4.000001" {
		t.Fatal("known Slack success was lost during unregister/cancel")
	}
}

type actorTerminalSender struct{ sent chan time.Time }

func (s actorTerminalSender) SendStatus(_ context.Context, status gateway.GatewayJobProgress, _ string) (gateway.StatusReceipt, error) {
	s.sent <- time.Now()
	return gateway.StatusReceipt{SlackTS: status.SlackTS}, nil
}

func TestActorTerminalDrainRetainsPhysicalBotPermitAfterUnregister(t *testing.T) {
	f := newActorOutboundFixture(t, 1, 1)
	identity := f.identities[0]
	r := f.register(identity, nil)
	first := actorAwait(t, f.sender.sent)
	f.scheduler.unregister(r.id)
	r.workers.Wait()

	// A known progress message can still require its final authentication
	// notice after normal registration and intake have already stopped.
	message := storetest.Message("terminal-event", "terminal", "alice")
	message.AppID = identity.AppID
	bound := gateway.ForThread(f.scopes[identity.ID], message.PhysicalThread())
	_, err := bound.Enqueue(f.ctx, message)
	storetest.Must(t, err)
	job, err := bound.ClaimJob(f.ctx)
	storetest.Must(t, err)
	if job == nil {
		t.Fatal("missing native terminal fixture")
	}
	progress, err := bound.ClaimStatus(f.ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if progress == nil {
		t.Fatal("missing native progress fixture")
	}
	storetest.Must(t, bound.FinishStatus(f.ctx, *progress, gateway.StatusResult{State: "ready", SlackTS: "500.000001"}, time.Now()))
	storetest.Must(t, bound.Review(f.ctx, job.ID, "backend_auth_or_identity_rejected"))

	sent := make(chan time.Time, 1)
	status := &gateway.StatusWorker{Sender: actorTerminalSender{sent}, Policy: storetest.Policy(identity.AppID, "T", "shared", "C")}
	drainCtx, cancelDrain := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancelDrain()
	terminal := schedulerRegistration(identity.ID, emptyScheduledStep, emptyScheduledStep)
	terminal.ctx = drainCtx
	terminal.threadStore = func(key gateway.PhysicalThreadKey) gateway.Store {
		return gateway.ForThread(f.scopes[identity.ID], key)
	}
	terminal.prepareOutbound = func(ctx context.Context, _ gateway.PhysicalThreadKey, local gateway.Store) (*gateway.PreparedOutbound, error) {
		return status.Prepare(ctx, local, "terminal")
	}
	worked, err := f.scheduler.terminal(drainCtx, terminal, message.PhysicalThread())
	storetest.Must(t, err)
	if !worked {
		t.Fatal("terminal notice was not published")
	}
	if at := actorAwait(t, sent); at.Before(first.at.Add(time.Second)) {
		t.Fatal("terminal drain bypassed the physical bot's existing pacing permit")
	}
	state, err := f.root.Status(f.ctx, 10)
	storetest.Must(t, err)
	var found bool
	for _, row := range state.GatewayJobs {
		if row.ID != job.ID {
			continue
		}
		found = true
		if row.ProgressState != "settled" || row.SlackTS != "500.000001" {
			t.Fatal("terminal completion returned before its native progress receipt")
		}
	}
	if !found {
		t.Fatal("terminal progress fixture disappeared")
	}
}
