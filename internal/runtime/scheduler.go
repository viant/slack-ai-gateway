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

type workVersion struct {
	root, integration, backend, reconciliation uint64
}

type scheduledLane struct {
	hint          gateway.WorkHint
	parked        bool
	parkedVersion workVersion
	consumedTimer time.Time
	wakePending   bool
	active        int
}

type scheduledRuntime struct {
	id              int64
	ctx             context.Context
	canStart        func() bool
	query           func(context.Context) (bool, error)
	outbound        func(context.Context) (bool, error)
	nextSend        func() time.Time
	report          func(context.Context, error) bool
	fatal           func(error)
	workers         *sync.WaitGroup
	enabled         atomic.Bool
	backend         uint64
	notifiedVersion workVersion
	queryLane       scheduledLane
	outboundLane    scheduledLane
}

type schedulingCommand struct {
	registration *scheduledRuntime
	id           int64
	remove       bool
	wake         bool
	ack          chan struct{}
}

type scheduledTask struct {
	registration *scheduledRuntime
	query        bool
	version      workVersion
}

type scheduledResult struct {
	task   scheduledTask
	worked bool
}

type discoveryResult struct {
	candidates []gateway.WorkCandidate
	versions   map[int64]workVersion
	err        error
}

// workScheduler owns dispatch only. The scoped workers retain every durable
// claim, policy decision, state transition, and external operation.
type workScheduler struct {
	planner      gateway.WorkPlanner
	ctx          context.Context
	cancel       context.CancelFunc
	commands     chan schedulingCommand
	queryJobs    chan scheduledTask
	outboundJobs chan scheduledTask
	results      chan scheduledResult
	discoveries  chan discoveryResult
	done         chan struct{}
	workers      sync.WaitGroup
	discovery    sync.WaitGroup
	failure      atomic.Pointer[error]
}

func newWorkScheduler(planner gateway.WorkPlanner) *workScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &workScheduler{planner: planner, ctx: ctx, cancel: cancel,
		commands: make(chan schedulingCommand), queryJobs: make(chan scheduledTask, schedulingSlots),
		outboundJobs: make(chan scheduledTask, schedulingSlots), results: make(chan scheduledResult, 2*schedulingSlots),
		discoveries: make(chan discoveryResult, 1), done: make(chan struct{})}
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

// unregister is the admission barrier before the runtime joins its WaitGroup.
// Every dispatched task is counted before acknowledgement, including tasks
// waiting for a pool goroutine; no task can be added after acknowledgement.
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
		worked := false
		if r.enabled.Load() && r.canStart() {
			step := r.outbound
			if task.query {
				step = r.query
			}
			var err error
			worked, err = step(r.ctx)
			if r.report(r.ctx, err) {
				r.enabled.Store(false)
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
	var order []*scheduledRuntime
	var queryCursor, outboundCursor, queryActive, outboundActive int
	var reconciliation uint64
	dirty, discovering, failed := false, false, false
	nextReconciliation := time.Now().Add(reconciliationInterval)
	version := func(r *scheduledRuntime) workVersion {
		return workVersion{root: s.planner.WorkRevision(0), integration: s.planner.WorkRevision(r.id), backend: r.backend, reconciliation: reconciliation}
	}
	wake := func(r *scheduledRuntime) {
		v := version(r)
		for _, lane := range []*scheduledLane{&r.queryLane, &r.outboundLane} {
			if !lane.parked || lane.parkedVersion != v {
				lane.wakePending = true
			}
		}
	}
	startDiscovery := func() {
		versions := make(map[int64]workVersion, len(registrations))
		for id, r := range registrations {
			versions[id] = version(r)
		}
		dirty, discovering = false, true
		s.discovery.Add(1)
		go func() {
			defer s.discovery.Done()
			candidates, err := s.planner.WorkCandidates(s.ctx, time.Now())
			// The last runtime can unregister before this read finishes. Retain
			// real shared failures independently of the remaining registrations
			// and of whether the coordinator still consumes its result.
			if err != nil && (s.ctx.Err() == nil || !errors.Is(err, s.ctx.Err())) {
				failure := errors.New("runtime work discovery failed")
				s.failure.CompareAndSwap(nil, &failure)
			}
			select {
			case s.discoveries <- discoveryResult{candidates: candidates, versions: versions, err: err}:
			case <-s.ctx.Done():
			}
		}()
	}
	timer := time.NewTimer(reconciliationInterval)
	defer timer.Stop()
	for {
		if dirty && !discovering && !failed && len(registrations) != 0 {
			startDiscovery()
		}
		now := time.Now()
		dispatch := func(query bool) {
			cursor, active, jobs := &outboundCursor, &outboundActive, s.outboundJobs
			if query {
				cursor, active, jobs = &queryCursor, &queryActive, s.queryJobs
			}
			for *active < schedulingSlots && len(order) != 0 {
				chosen := false
				for range len(order) {
					r := order[*cursor%len(order)]
					*cursor = (*cursor + 1) % len(order)
					lane, limit := &r.outboundLane, 1
					if query {
						lane, limit = &r.queryLane, schedulingSlots
					}
					v := version(r)
					due := lane.wakePending || !lane.hint.NextAt.IsZero() && !lane.hint.NextAt.After(now) && !lane.hint.NextAt.Equal(lane.consumedTimer)
					immediate := lane.hint.ProbeNow && (!lane.parked || lane.parkedVersion != v)
					if !r.enabled.Load() || !r.canStart() || lane.active >= limit || !due && !immediate || !query && r.nextSend().After(now) {
						continue
					}
					lane.wakePending = false
					if due && !lane.hint.NextAt.IsZero() && !lane.hint.NextAt.After(now) {
						lane.consumedTimer = lane.hint.NextAt
					}
					lane.active++
					*active++
					r.workers.Add(1)
					jobs <- scheduledTask{registration: r, query: query, version: v}
					chosen = true
					break
				}
				if !chosen {
					break
				}
			}
		}
		if !failed {
			dispatch(true)
			dispatch(false)
		}
		deadline := nextReconciliation
		for _, r := range order {
			if !r.enabled.Load() || !r.canStart() {
				continue
			}
			for _, query := range []bool{true, false} {
				lane, limit, active := &r.outboundLane, 1, outboundActive
				if query {
					lane, limit, active = &r.queryLane, schedulingSlots, queryActive
				}
				if lane.active >= limit || active >= schedulingSlots {
					continue
				}
				at := lane.hint.NextAt
				if at.Equal(lane.consumedTimer) {
					at = time.Time{}
				}
				if lane.wakePending || lane.hint.ProbeNow && (!lane.parked || lane.parkedVersion != version(r)) {
					at = now
				}
				if !at.IsZero() && !query && r.nextSend().After(at) {
					at = r.nextSend()
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
				order = append(order, r)
				// Ready runtimes retain their immediate startup claims even when
				// the shared deadline discovery is still reading stored history.
				wake(r)
				if failed {
					r.enabled.Store(false)
					r.fatal(errors.New("runtime work discovery failed"))
				}
				dirty = true
			case command.remove:
				if r := registrations[command.id]; r != nil {
					r.enabled.Store(false)
					delete(registrations, command.id)
					for i, candidate := range order {
						if candidate == r {
							order = append(order[:i], order[i+1:]...)
							break
						}
					}
					queryCursor, outboundCursor = 0, 0
				}
			case command.wake:
				if r := registrations[command.id]; r != nil {
					r.backend++
					wake(r)
					dirty = true
				}
			}
			if command.ack != nil {
				close(command.ack)
			}
		case <-s.planner.WorkChanges():
			// Committed work must not wait behind a slow global timing read.
			// Only changed scoped/root generations grant a direct claim token;
			// unrelated commits preserve another runtime's parked lanes.
			for _, r := range order {
				v := version(r)
				if v.root != r.notifiedVersion.root || v.integration != r.notifiedVersion.integration {
					r.notifiedVersion = v
					wake(r)
				}
			}
			dirty = true
		case result := <-s.discoveries:
			discovering = false
			if result.err != nil {
				if s.ctx.Err() != nil && errors.Is(result.err, s.ctx.Err()) {
					continue
				}
				failed = true
				for _, r := range order {
					r.enabled.Store(false)
					r.fatal(errors.New("runtime work discovery failed"))
				}
				continue
			}
			candidates := make(map[int64]gateway.WorkCandidate, len(result.candidates))
			for _, candidate := range result.candidates {
				candidates[candidate.IntegrationID] = candidate
			}
			for id, r := range registrations {
				v, included := result.versions[id]
				if !included {
					dirty = true
					continue
				}
				candidate := candidates[id]
				for _, pair := range []struct {
					lane *scheduledLane
					hint gateway.WorkHint
				}{{&r.queryLane, candidate.Query}, {&r.outboundLane, candidate.Outbound}} {
					previous := pair.lane.hint.NextAt
					if !previous.IsZero() && !previous.After(time.Now()) && !previous.Equal(pair.lane.consumedTimer) {
						pair.lane.wakePending = true
					}
					pair.lane.hint = pair.hint
				}
				if v != version(r) {
					dirty = true
				}
			}
		case result := <-s.results:
			r := result.task.registration
			lane, active := &r.outboundLane, &outboundActive
			if result.task.query {
				lane, active = &r.queryLane, &queryActive
			}
			lane.active--
			*active--
			if !result.worked && (!lane.parked || lane.parkedVersion != version(r) || lane.parkedVersion == result.task.version) {
				lane.parked, lane.parkedVersion = true, result.task.version
			}
			dirty = true
		case <-timer.C:
			if !time.Now().Before(nextReconciliation) {
				reconciliation++
				nextReconciliation = time.Now().Add(reconciliationInterval)
				dirty = true
			}
		}
	}
}
