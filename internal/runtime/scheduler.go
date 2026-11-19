package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

const schedulingSlots = 4
const reconciliationInterval = 5 * time.Second

type workVersion struct{ root, integration, backend, reconciliation uint64 }
type scheduledLane struct {
	hint          gateway.WorkHint
	parked        bool
	parkedVersion workVersion
	consumedTimer time.Time
	wakePending   bool
	active        int
}
type scheduledRuntime struct {
	id                      int64
	ctx                     context.Context
	canStart                func() bool
	query, outbound         func(context.Context) (bool, error)
	threadStore             func(gateway.PhysicalThreadKey) gateway.Store
	queryThread             func(context.Context, gateway.PhysicalThreadKey) (bool, error)
	prepareOutbound         func(context.Context, gateway.PhysicalThreadKey, gateway.Store) (*gateway.PreparedOutbound, error)
	nextSend                func() time.Time
	report                  func(context.Context, error) bool
	fatal                   func(error)
	workers                 *sync.WaitGroup
	enabled                 atomic.Bool
	backend                 uint64
	notifiedVersion         workVersion
	queryLane, outboundLane scheduledLane
	parent                  *scheduledRuntime
	key                     gateway.PhysicalThreadKey
	seen                    uint64
}
type schedulingCommand struct {
	registration *scheduledRuntime
	id           int64
	remove, wake bool
	ack          chan struct{}
	terminal     *terminalRequest
}

type terminalResult struct {
	worked bool
	err    error
}
type terminalRequest struct {
	runtime *scheduledRuntime
	reply   chan terminalResult
}
type scheduledTask struct {
	registration *scheduledRuntime
	query        bool
	version      workVersion
	flight       *outboundFlight
}
type scheduledResult struct {
	task     scheduledTask
	worked   bool
	outbound gateway.OutboundResult
}
type discoveryResult struct {
	candidates []gateway.WorkCandidate
	versions   map[int64]workVersion
	page       gateway.WorkPage
	err        error
}
type scheduleKey struct {
	integration int64
	thread      gateway.PhysicalThreadKey
}
type botKey struct {
	app, workspace string
	legacy         int64
}
type botSchedule struct {
	busy      bool
	notBefore time.Time
}
type outboundFlight struct {
	registration  *scheduledRuntime
	version       workVersion
	store         gateway.Store
	prepared      *gateway.PreparedOutbound
	result        gateway.OutboundResult
	release       func()
	saveCtx       context.Context
	cancel        context.CancelFunc
	submitted     bool
	terminalReply chan terminalResult
}
type outboundPrepared struct {
	flight *outboundFlight
	err    error
}
type outboundCommitted struct {
	flight *outboundFlight
	err    error
}

// workScheduler is the existing process dispatcher. Physical-thread entries
// replace broad integration claims; no historical thread owns a polling loop.
type workScheduler struct {
	planner                 gateway.WorkPlanner
	ctx                     context.Context
	cancel                  context.CancelFunc
	commands                chan schedulingCommand
	queryJobs, outboundJobs chan scheduledTask
	results                 chan scheduledResult
	discoveries             chan discoveryResult
	prepared                chan outboundPrepared
	committed               chan outboundCommitted
	done                    chan struct{}
	workers, discovery      sync.WaitGroup
	failure                 atomic.Pointer[error]
}

func newWorkScheduler(planner gateway.WorkPlanner) *workScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &workScheduler{planner: planner, ctx: ctx, cancel: cancel, commands: make(chan schedulingCommand),
		queryJobs: make(chan scheduledTask, schedulingSlots), outboundJobs: make(chan scheduledTask, schedulingSlots),
		results: make(chan scheduledResult, 2*schedulingSlots), discoveries: make(chan discoveryResult, 1),
		prepared: make(chan outboundPrepared, 2*schedulingSlots), committed: make(chan outboundCommitted, 2*schedulingSlots), done: make(chan struct{})}
	for range schedulingSlots {
		s.workers.Add(2)
		go s.worker(s.queryJobs)
		go s.worker(s.outboundJobs)
	}
	go s.run()
	return s
}
func (s *workScheduler) register(r *scheduledRuntime) {
	r.enabled.Store(true)
	ack := make(chan struct{})
	s.commands <- schedulingCommand{registration: r, ack: ack}
	<-ack
}
func (s *workScheduler) unregister(id int64) {
	ack := make(chan struct{})
	s.commands <- schedulingCommand{id: id, remove: true, ack: ack}
	<-ack
}
func (s *workScheduler) wake(id int64) {
	select {
	case s.commands <- schedulingCommand{id: id, wake: true}:
	case <-s.ctx.Done():
	}
}

// terminal publishes shutdown-only progress through the same physical-bot
// permit and HTTP executors as healthy integrations. An admitted request joins
// its receipt even when its caller deadline expires.
func (s *workScheduler) terminal(ctx context.Context, r *scheduledRuntime, key gateway.PhysicalThreadKey) (bool, error) {
	r.key = key
	r.enabled.Store(true)
	if r.workers == nil {
		r.workers = &sync.WaitGroup{}
	}
	request := &terminalRequest{runtime: r, reply: make(chan terminalResult, 1)}
	select {
	case s.commands <- schedulingCommand{terminal: request}:
	case <-ctx.Done():
		return false, ctx.Err()
	case <-s.ctx.Done():
		return false, s.ctx.Err()
	}
	result := <-request.reply
	r.workers.Wait()
	return result.worked, result.err
}
func (s *workScheduler) close() error {
	s.cancel()
	<-s.done
	s.workers.Wait()
	s.discovery.Wait()
	if failure := s.failure.Load(); failure != nil {
		return *failure
	}
	return nil
}
func (s *workScheduler) worker(jobs <-chan scheduledTask) {
	defer s.workers.Done()
	for task := range jobs {
		r := task.registration
		if task.flight != nil {
			result := task.flight.prepared.Send(r.ctx)
			s.results <- scheduledResult{task: task, worked: true, outbound: result}
			continue
		}
		worked := false
		if r.enabled.Load() && (r.parent == nil || r.parent.enabled.Load()) && r.canStart() {
			var err error
			if task.query && r.queryThread != nil {
				worked, err = r.queryThread(r.ctx, r.key)
			} else {
				step := r.outbound
				if task.query {
					step = r.query
				}
				worked, err = step(r.ctx)
			}
			if r.report(r.ctx, err) {
				r.enabled.Store(false)
				if r.parent != nil {
					r.parent.enabled.Store(false)
				}
			}
		}
		r.workers.Done()
		s.results <- scheduledResult{task: task, worked: worked}
	}
}
func (s *workScheduler) run() {
	defer close(s.done)
	defer close(s.queryJobs)
	defer close(s.outboundJobs)
	registrations := map[int64]*scheduledRuntime{}
	entries := map[scheduleKey]*scheduledRuntime{}
	unregistered := map[scheduleKey]gateway.WorkCandidate{}
	var order []*scheduledRuntime
	bots := map[botKey]*botSchedule{}
	var queryCursor, outboundCursor, queryActive, outboundActive, preparing int
	var readyHTTP, receipts []*outboundFlight
	var terminals []*terminalRequest
	var reconciliation, round uint64
	dirty, discovering, failed := false, false, false
	var cursor *gateway.WorkCursor
	nextReconciliation := time.Now().Add(reconciliationInterval)
	threaded, hasThreads := s.planner.(gateway.ThreadWorkPlanner)
	var available <-chan struct{}
	if owner, ok := s.planner.(interface{ OwnershipChanges() <-chan struct{} }); ok {
		available = owner.OwnershipChanges()
	}
	version := func(r *scheduledRuntime) workVersion {
		revision := s.planner.WorkRevision(r.id)
		if hasThreads && r.key.Valid() {
			revision = threaded.ThreadRevision(r.key)
		}
		backend := r.backend
		if r.parent != nil {
			backend = r.parent.backend
		}
		return workVersion{s.planner.WorkRevision(0), revision, backend, reconciliation}
	}
	wake := func(r *scheduledRuntime) {
		v := version(r)
		for _, lane := range []*scheduledLane{&r.queryLane, &r.outboundLane} {
			if !lane.parked || lane.parkedVersion != v {
				lane.wakePending = true
			}
		}
	}
	botFor := func(r *scheduledRuntime) *botSchedule {
		key := botKey{app: r.key.AppID, workspace: r.key.WorkspaceID}
		if !r.key.Valid() {
			key.legacy = r.id
		}
		bot := bots[key]
		if bot == nil {
			bot = &botSchedule{}
			bots[key] = bot
		}
		return bot
	}
	forgetUnusedThread := func(key gateway.PhysicalThreadKey) {
		if !key.Valid() {
			return
		}
		// Check one direct entry per configured runtime, rather than scanning
		// every active thread again for each key pruned from a large round.
		for id, registration := range registrations {
			if registration != nil && entries[scheduleKey{id, key}] != nil {
				return
			}
		}
		if owner, ok := s.planner.(interface {
			ForgetWorkThread(gateway.PhysicalThreadKey)
		}); ok {
			owner.ForgetWorkThread(key)
		}
	}
	ensure := func(c gateway.WorkCandidate) *scheduledRuntime {
		key := scheduleKey{c.IntegrationID, c.Thread}
		parent, known := registrations[c.IntegrationID]
		if parent == nil {
			// Absence permits startup buffering. A nil entry is a runtime that
			// explicitly stopped; its durable work waits for a deliberate register.
			if !known {
				unregistered[key] = c
			} else {
				forgetUnusedThread(c.Thread)
			}
			return nil
		}
		if r := entries[key]; r != nil {
			return r
		}
		r := parent
		if c.Thread.Valid() {
			r = &scheduledRuntime{id: parent.id, ctx: parent.ctx, canStart: parent.canStart, query: parent.query, outbound: parent.outbound,
				threadStore: parent.threadStore, queryThread: parent.queryThread, prepareOutbound: parent.prepareOutbound, nextSend: parent.nextSend,
				report: parent.report, fatal: parent.fatal, workers: parent.workers, parent: parent, key: c.Thread}
			r.enabled.Store(true)
		}
		entries[key] = r
		order = append(order, r)
		return r
	}
	merge := func(c gateway.WorkCandidate, direct bool) {
		if c.Reconcile {
			dirty = true
		}
		r := ensure(c)
		if r == nil {
			return
		}
		r.seen = round
		for _, pair := range []struct {
			lane *scheduledLane
			hint gateway.WorkHint
		}{{&r.queryLane, c.Query}, {&r.outboundLane, c.Outbound}} {
			if direct {
				if pair.hint.ProbeNow {
					pair.lane.wakePending = true
				}
				if !pair.hint.NextAt.IsZero() && (pair.lane.hint.NextAt.IsZero() || pair.lane.hint.NextAt.Equal(pair.lane.consumedTimer) || pair.lane.hint.NextAt.Before(time.Now()) || pair.hint.NextAt.Before(pair.lane.hint.NextAt)) {
					pair.lane.hint.NextAt = pair.hint.NextAt
				}
				continue
			}
			previous := pair.lane.hint.NextAt
			if !previous.IsZero() && !previous.After(time.Now()) && !previous.Equal(pair.lane.consumedTimer) {
				pair.lane.wakePending = true
			}
			pair.lane.hint = pair.hint
		}
	}
	// An empty claim can finish after the authoritative discovery that found its
	// thread absent. Retire it then as well, before an old ProbeNow hint is made
	// eligible again by the next reconciliation version.
	pruneIdle := func() {
		if !hasThreads || discovering || cursor != nil {
			return
		}
		filtered := order[:0]
		for _, r := range order {
			if r.queryLane.active == 0 && r.outboundLane.active == 0 && r.seen != round && !r.queryLane.wakePending && !r.outboundLane.wakePending {
				delete(entries, scheduleKey{r.id, r.key})
				forgetUnusedThread(r.key)
				continue
			}
			filtered = append(filtered, r)
		}
		order = filtered
		queryCursor, outboundCursor = 0, 0
	}
	startDiscovery := func() {
		versions := make(map[int64]workVersion, len(registrations))
		for id, r := range registrations {
			if r != nil {
				versions[id] = version(r)
			}
		}
		if cursor == nil {
			round++
		}
		pageCursor := cursor
		dirty, discovering = false, true
		s.discovery.Add(1)
		go func() {
			defer s.discovery.Done()
			result := discoveryResult{versions: versions}
			if hasThreads {
				result.page, result.err = threaded.WorkPage(s.ctx, time.Now(), pageCursor)
				result.candidates = result.page.Candidates
			} else {
				result.candidates, result.err = s.planner.WorkCandidates(s.ctx, time.Now())
			}
			if result.err != nil && (s.ctx.Err() == nil || !errors.Is(result.err, s.ctx.Err())) {
				failure := errors.New("runtime work discovery failed")
				s.failure.CompareAndSwap(nil, &failure)
			}
			select {
			case s.discoveries <- result:
			case <-s.ctx.Done():
			}
		}()
	}
	finishFlight := func(f *outboundFlight, err error, worked bool) {
		r := f.registration
		r.outboundLane.active--
		bot := botFor(r)
		bot.busy = false
		if worked {
			bot.notBefore = time.Now().Add(time.Second)
			if f.result.NotBefore.After(bot.notBefore) {
				bot.notBefore = f.result.NotBefore
			}
		}
		if !worked {
			r.outboundLane.parked = true
			r.outboundLane.parkedVersion = f.version
			if r.seen != round {
				pruneIdle()
			}
		}
		if f.cancel != nil {
			f.cancel()
		}
		f.release()
		if err != nil && r.report(r.ctx, err) {
			r.enabled.Store(false)
			if r.parent != nil {
				r.parent.enabled.Store(false)
			}
		}
		r.workers.Done()
		if f.terminalReply != nil {
			f.terminalReply <- terminalResult{worked: worked, err: err}
		}
	}
	prepareFlight := func(r *scheduledRuntime, v workVersion, reply chan terminalResult) bool {
		store := r.threadStore(r.key)
		async, ok := store.(gateway.AsyncThreadStore)
		if !ok {
			err := errors.New("thread storage does not support outbound ownership")
			r.enabled.Store(false)
			if reply != nil {
				reply <- terminalResult{err: err}
			} else {
				r.fatal(err)
				r.enabled.Store(false)
			}
			return false
		}
		f := &outboundFlight{registration: r, version: v, store: store, release: gateway.RetainThread(store), terminalReply: reply}
		prepareCtx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		r.workers.Add(1)
		accepted := async.TryInThread(prepareCtx, func(local gateway.Store) error {
			var err error
			f.prepared, err = r.prepareOutbound(prepareCtx, r.key, local)
			return err
		}, func(err error) { cancel(); s.prepared <- outboundPrepared{f, err} })
		if !accepted {
			cancel()
			f.release()
			r.workers.Done()
			return false
		}
		preparing++
		botFor(r).busy = true
		r.outboundLane.active++
		return true
	}
	timer := time.NewTimer(reconciliationInterval)
	defer timer.Stop()
	hasRegistrations := func() bool {
		for _, r := range registrations {
			if r != nil {
				return true
			}
		}
		return false
	}
	for {
		if dirty && !discovering && !failed && hasRegistrations() {
			startDiscovery()
		}
		now := time.Now()
		for i := 0; i < len(terminals); {
			request := terminals[i]
			if err := request.runtime.ctx.Err(); err != nil {
				request.reply <- terminalResult{err: err}
				terminals = append(terminals[:i], terminals[i+1:]...)
				continue
			}
			bot := botFor(request.runtime)
			if preparing < schedulingSlots && len(readyHTTP) < schedulingSlots && !bot.busy && !bot.notBefore.After(now) && prepareFlight(request.runtime, version(request.runtime), request.reply) {
				terminals = append(terminals[:i], terminals[i+1:]...)
				continue
			}
			if !request.runtime.enabled.Load() {
				terminals = append(terminals[:i], terminals[i+1:]...)
				continue
			}
			i++
		}
		// Receipt persistence never occupies an HTTP executor. Its bot permit stays
		// reserved until the owner confirms the write (including Retry-After).
		for i := 0; i < len(receipts); {
			f := receipts[i]
			if f.submitted {
				i++
				continue
			}
			if f.saveCtx.Err() != nil {
				finishFlight(f, errors.New("outbound receipt persistence deadline exceeded"), true)
				receipts = append(receipts[:i], receipts[i+1:]...)
				continue
			}
			async := f.store.(gateway.AsyncThreadStore)
			f.submitted = async.TryInThread(f.saveCtx, func(local gateway.Store) error { return f.result.Commit(f.saveCtx, local) }, func(err error) { s.committed <- outboundCommitted{f, err} })
			i++
		}
		for outboundActive < schedulingSlots && len(readyHTTP) > 0 {
			f := readyHTTP[0]
			readyHTTP = readyHTTP[1:]
			outboundActive++
			s.outboundJobs <- scheduledTask{registration: f.registration, flight: f}
		}
		dispatch := func(query bool) {
			position, active, jobs := &outboundCursor, &outboundActive, s.outboundJobs
			if query {
				position, active, jobs = &queryCursor, &queryActive, s.queryJobs
			}
			for len(order) != 0 {
				if query && *active >= schedulingSlots || !query && preparing >= schedulingSlots || !query && len(readyHTTP) >= schedulingSlots {
					return
				}
				chosen := false
				for range len(order) {
					r := order[*position%len(order)]
					*position = (*position + 1) % len(order)
					lane, limit := &r.outboundLane, 1
					if query {
						lane, limit = &r.queryLane, schedulingSlots
						if r.key.Valid() {
							limit = 1
						}
					}
					v := version(r)
					due := lane.wakePending || !lane.hint.NextAt.IsZero() && !lane.hint.NextAt.After(now) && !lane.hint.NextAt.Equal(lane.consumedTimer)
					immediate := lane.hint.ProbeNow && (!lane.parked || lane.parkedVersion != v)
					if !r.enabled.Load() || r.parent != nil && !r.parent.enabled.Load() || !r.canStart() || lane.active >= limit || !due && !immediate {
						continue
					}
					bot := botFor(r)
					if !query && (bot.busy || bot.notBefore.After(now) || (r.prepareOutbound == nil || !r.key.Valid()) && r.nextSend().After(now)) {
						continue
					}
					if !query && (r.prepareOutbound == nil || !r.key.Valid()) && *active >= schedulingSlots {
						return
					}
					phased := !query && r.prepareOutbound != nil && r.key.Valid()
					if phased {
						if !prepareFlight(r, v, nil) {
							continue
						}
					} else {
						*active++
						r.workers.Add(1)
						jobs <- scheduledTask{registration: r, query: query, version: v}
					}
					lane.wakePending = false
					if due && !lane.hint.NextAt.IsZero() && !lane.hint.NextAt.After(now) {
						lane.consumedTimer = lane.hint.NextAt
					}
					if !phased {
						lane.active++
					}
					chosen = true
					break
				}
				if !chosen {
					return
				}
			}
		}
		if !failed {
			dispatch(true)
			dispatch(false)
		}
		deadline := nextReconciliation
		for _, request := range terminals {
			at, ok := request.runtime.ctx.Deadline()
			if ok && at.Before(deadline) {
				deadline = at
			}
			bot := botFor(request.runtime)
			if !bot.busy && bot.notBefore.After(now) && bot.notBefore.Before(deadline) {
				deadline = bot.notBefore
			}
		}

		for _, f := range receipts {
			if !f.submitted {
				if at, ok := f.saveCtx.Deadline(); ok && at.Before(deadline) {
					deadline = at
				}
			}
		}
		for _, r := range order {
			if !r.enabled.Load() || r.parent != nil && !r.parent.enabled.Load() || !r.canStart() {
				continue
			}
			for _, query := range []bool{true, false} {
				lane, limit, active := &r.outboundLane, 1, outboundActive
				if query {
					lane, limit, active = &r.queryLane, schedulingSlots, queryActive
					if r.key.Valid() {
						limit = 1
					}
				}
				if lane.active >= limit || query && active >= schedulingSlots || !query && botFor(r).busy {
					continue
				}
				at := lane.hint.NextAt
				if at.Equal(lane.consumedTimer) {
					at = time.Time{}
				}
				if lane.wakePending || lane.hint.ProbeNow && (!lane.parked || lane.parkedVersion != version(r)) {
					at = now
				}
				if !at.IsZero() && !query {
					gate := botFor(r).notBefore
					if (r.prepareOutbound == nil || !r.key.Valid()) && r.nextSend().After(gate) {
						gate = r.nextSend()
					}
					if gate.After(at) {
						at = gate
					}
				}
				if !at.IsZero() && at.After(now) && at.Before(deadline) {
					deadline = at
				}
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Duration(0), time.Until(deadline)))
		select {
		case <-s.ctx.Done():
			return
		case command := <-s.commands:
			switch {
			case command.registration != nil:
				r := command.registration
				r.notifiedVersion = version(r)
				registrations[r.id] = r
				for key, hint := range unregistered {
					if key.integration == r.id {
						merge(hint, true)
						delete(unregistered, key)
					}
				}
				if !hasThreads {
					ensure(gateway.WorkCandidate{IntegrationID: r.id})
					wake(r)
				} else {
					for _, hint := range threaded.TakeWorkHints() {
						merge(hint, true)
					}
				}
				if failed {
					r.enabled.Store(false)
					r.fatal(errors.New("runtime work discovery failed"))
				}
				dirty = true
			case command.terminal != nil:
				terminals = append(terminals, command.terminal)
			case command.remove:
				if r := registrations[command.id]; r != nil {
					r.enabled.Store(false)
				}
				registrations[command.id] = nil
				for key := range unregistered {
					if key.integration == command.id {
						delete(unregistered, key)
						forgetUnusedThread(key.thread)
					}
				}
				for key, r := range entries {
					if key.integration == command.id {
						r.enabled.Store(false)
						delete(entries, key)
						forgetUnusedThread(key.thread)
					}
				}
				filtered := order[:0]
				for _, r := range order {
					if r.id != command.id {
						filtered = append(filtered, r)
					}
				}
				order = filtered
				queryCursor, outboundCursor = 0, 0
			case command.wake:
				if r := registrations[command.id]; r != nil {
					r.backend++
					for _, entry := range order {
						if entry.id == r.id {
							wake(entry)
						}
					}
					dirty = true
				}
			}
			if command.ack != nil {
				close(command.ack)
			}
		case <-available:
			// Capacity notification only: never discover SQL or invent work here.
		case <-s.planner.WorkChanges():
			if hasThreads {
				for _, hint := range threaded.TakeWorkHints() {
					merge(hint, true)
				}
				for _, r := range registrations {
					if r == nil {
						continue
					}
					v := version(r)
					if v.root != r.notifiedVersion.root {
						r.notifiedVersion = v
						dirty = true
						for _, entry := range order {
							wake(entry)
						}
					}
				}
			} else {
				for _, r := range order {
					v := version(r)
					if v.root != r.notifiedVersion.root || v.integration != r.notifiedVersion.integration {
						r.notifiedVersion = v
						wake(r)
					}
				}
				dirty = true
			}
		case result := <-s.discoveries:
			discovering = false
			if result.err != nil {
				if s.ctx.Err() != nil && errors.Is(result.err, s.ctx.Err()) {
					continue
				}
				failed = true
				for _, r := range registrations {
					if r != nil {
						r.enabled.Store(false)
						r.fatal(errors.New("runtime work discovery failed"))
					}
				}
				continue
			}
			if !hasThreads {
				for id, r := range registrations {
					if r != nil {
						merge(gateway.WorkCandidate{IntegrationID: id}, false)
					}
				}
			}
			for _, candidate := range result.candidates {
				merge(candidate, false)
			}
			if result.page.Deadline != nil {
				merge(*result.page.Deadline, true)
			}
			if hasThreads {
				cursor = result.page.Next
				dirty = cursor != nil
				if cursor == nil {
					// Settled history does not retain scheduling entries. In-flight external
					// continuations retain ownership separately and can reactivate by hint.
					pruneIdle()
				}
			} else {
				for id, r := range registrations {
					if r == nil {
						continue
					}
					if v, ok := result.versions[id]; !ok || v != version(r) {
						dirty = true
					}
				}
			}
		case result := <-s.results:
			r := result.task.registration
			if result.task.flight != nil {
				outboundActive--
				f := result.task.flight
				f.result = result.outbound
				f.saveCtx, f.cancel = context.WithTimeout(context.WithoutCancel(r.ctx), 3*time.Second)
				receipts = append(receipts, f)
				continue
			}
			lane, active := &r.outboundLane, &outboundActive
			if result.task.query {
				lane, active = &r.queryLane, &queryActive
			}
			lane.active--
			*active--
			if !result.worked && (!lane.parked || lane.parkedVersion != version(r) || lane.parkedVersion == result.task.version) {
				lane.parked = true
				lane.parkedVersion = result.task.version
			}
			if !result.worked && r.seen != round {
				pruneIdle()
			}
			if !hasThreads {
				dirty = true
			}
		case prepared := <-s.prepared:
			preparing--
			f := prepared.flight
			if prepared.err != nil || f.prepared == nil {
				finishFlight(f, prepared.err, false)
			} else {
				readyHTTP = append(readyHTTP, f)
			}
		case committed := <-s.committed:
			f := committed.flight
			for i, candidate := range receipts {
				if candidate == f {
					receipts = append(receipts[:i], receipts[i+1:]...)
					break
				}
			}
			err := committed.err
			if err == nil {
				err = f.result.Err
			}
			finishFlight(f, err, true)
		case <-timer.C:
			if hasThreads {
				dirty = true
			}
			if !time.Now().Before(nextReconciliation) {
				reconciliation++
				nextReconciliation = time.Now().Add(reconciliationInterval)
				dirty = true
			}
		}
	}
}
