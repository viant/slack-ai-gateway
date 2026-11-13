package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type schedulerPlanner struct {
	mu         sync.Mutex
	candidates []gateway.WorkCandidate
	revisions  map[int64]uint64
	changes    chan struct{}
	afterRead  func()
	err        error
}

func newSchedulerPlanner() *schedulerPlanner {
	return &schedulerPlanner{revisions: map[int64]uint64{}, changes: make(chan struct{}, 1)}
}
func (*schedulerPlanner) IntegrationID() int64           { return 0 }
func (p *schedulerPlanner) WorkChanges() <-chan struct{} { return p.changes }
func (p *schedulerPlanner) WorkRevision(id int64) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.revisions[id]
}
func (p *schedulerPlanner) WorkCandidates(context.Context, time.Time) ([]gateway.WorkCandidate, error) {
	p.mu.Lock()
	result := append([]gateway.WorkCandidate(nil), p.candidates...)
	hook := p.afterRead
	err := p.err
	p.afterRead = nil
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	return result, err
}
func (p *schedulerPlanner) change(id int64, candidates []gateway.WorkCandidate) {
	p.mu.Lock()
	if candidates != nil {
		p.candidates = candidates
	}
	p.revisions[id]++
	p.mu.Unlock()
	select {
	case p.changes <- struct{}{}:
	default:
	}
}

func schedulerRegistration(id int64, query, outbound func(context.Context) (bool, error)) *scheduledRuntime {
	return &scheduledRuntime{id: id, ctx: context.Background(), canStart: func() bool { return true }, query: query, outbound: outbound,
		nextSend: func() time.Time { return time.Time{} }, report: func(context.Context, error) bool { return false }, fatal: func(error) {}, workers: &sync.WaitGroup{}}
}
func emptyScheduledStep(context.Context) (bool, error) { return false, nil }
func finishScheduler(s *workScheduler, registrations ...*scheduledRuntime) {
	for _, r := range registrations {
		s.unregister(r.id)
	}
	for _, r := range registrations {
		r.workers.Wait()
	}
	s.close()
}

func TestSchedulerParksEmptyClaimsAcrossUnrelatedChanges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		p.candidates = []gateway.WorkCandidate{{IntegrationID: 1, Query: gateway.WorkHint{ProbeNow: true}}, {IntegrationID: 2}}
		s := newWorkScheduler(p)
		var calls atomic.Int32
		first := schedulerRegistration(1, func(context.Context) (bool, error) { calls.Add(1); return false, nil }, emptyScheduledStep)
		second := schedulerRegistration(2, emptyScheduledStep, emptyScheduledStep)
		s.register(first)
		s.register(second)
		defer finishScheduler(s, first, second)
		synctest.Wait()
		initial := calls.Load()
		if initial < 1 || initial > 4 {
			t.Fatalf("initial probes=%d", initial)
		}
		for range 20 {
			p.change(2, nil)
			synctest.Wait()
		}
		if calls.Load() != initial {
			t.Fatal("unrelated committed changes unparked empty claims")
		}
		p.change(1, nil)
		synctest.Wait()
		if calls.Load() <= initial {
			t.Fatal("relevant commit did not unpark claims")
		}
		before := calls.Load()
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if calls.Load() <= before {
			t.Fatal("shared reconciliation did not probe a conservative candidate")
		}
	})
}

func TestSchedulerRetainsFutureDeadlineAndOutboundPacing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		start := time.Now()
		p.candidates = []gateway.WorkCandidate{{IntegrationID: 1, Outbound: gateway.WorkHint{ProbeNow: true, NextAt: start.Add(3 * time.Second)}}}
		s := newWorkScheduler(p)
		var calls []time.Time
		var callsMu sync.Mutex
		snapshot := func() []time.Time {
			callsMu.Lock()
			defer callsMu.Unlock()
			return append([]time.Time(nil), calls...)
		}
		var nextSend time.Time
		r := schedulerRegistration(1, emptyScheduledStep, func(context.Context) (bool, error) {
			callsMu.Lock()
			defer callsMu.Unlock()
			calls = append(calls, time.Now())
			if len(calls) == 2 {
				nextSend = time.Now().Add(time.Second)
				return true, nil
			}
			return false, nil
		})
		r.nextSend = func() time.Time { return nextSend }
		s.register(r)
		defer finishScheduler(s, r)
		synctest.Wait()
		if calls := snapshot(); len(calls) != 1 {
			t.Fatalf("initial outbound probes=%d", len(calls))
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if calls := snapshot(); len(calls) != 2 || !calls[1].Equal(start.Add(3*time.Second)) {
			t.Fatalf("future deadline lost behind empty immediate probe: %v", calls)
		}
		p.change(1, nil)
		synctest.Wait()
		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		if calls := snapshot(); len(calls) != 2 {
			t.Fatal("committed change bypassed outbound pacing")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if calls := snapshot(); len(calls) != 3 || !calls[2].Equal(start.Add(4*time.Second)) {
			t.Fatalf("pacing deadline was not honored: %v", calls)
		}
	})
}

func TestSchedulerDoesNotLoseCommitDuringDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		entered, release := make(chan struct{}), make(chan struct{})
		p.afterRead = func() { close(entered); <-release }
		s := newWorkScheduler(p)
		var calls atomic.Int32
		r := schedulerRegistration(1, func(context.Context) (bool, error) { calls.Add(1); return false, nil }, emptyScheduledStep)
		s.register(r)
		defer finishScheduler(s, r)
		<-entered
		p.change(1, []gateway.WorkCandidate{{IntegrationID: 1, Query: gateway.WorkHint{ProbeNow: true}}})
		close(release)
		synctest.Wait()
		if calls.Load() == 0 {
			t.Fatal("a commit arriving during discovery was lost")
		}
	})
}

func TestSchedulerFairSeparateBoundedPools(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		s := newWorkScheduler(p)
		release := make(chan struct{})
		queries := make(chan int64, 24)
		outbound := make(chan int64, 24)
		var registrations []*scheduledRuntime
		var candidates []gateway.WorkCandidate
		var ready atomic.Bool
		for id := int64(1); id <= 6; id++ {
			r := schedulerRegistration(id, func(context.Context) (bool, error) {
				if ready.Load() {
					queries <- id
					<-release
				}
				return false, nil
			}, func(context.Context) (bool, error) {
				if ready.Load() {
					outbound <- id
				}
				return false, nil
			})
			registrations = append(registrations, r)
			s.register(r)
			candidates = append(candidates, gateway.WorkCandidate{IntegrationID: id, Query: gateway.WorkHint{ProbeNow: true}, Outbound: gateway.WorkHint{ProbeNow: true}})
		}
		defer finishScheduler(s, registrations...)
		synctest.Wait() // Consume each ready registration's initial claim tokens.
		ready.Store(true)
		p.change(0, candidates)
		synctest.Wait()
		if len(queries) != 4 {
			t.Fatalf("admission slots=%d", len(queries))
		}
		seen := map[int64]bool{}
		for range 4 {
			seen[<-queries] = true
		}
		if len(seen) != 4 {
			t.Fatalf("one integration monopolized the admission pool: %v", seen)
		}
		if len(outbound) != 6 {
			t.Fatalf("blocked admission prevented outbound acknowledgments: %d", len(outbound))
		}
		close(release)
		synctest.Wait()
		for len(queries) > 0 {
			seen[<-queries] = true
		}
		if len(seen) != 6 {
			t.Fatalf("ready integrations starved after slots became available: %v", seen)
		}
	})
}

func TestSchedulerReadyStartupDoesNotWaitForDiscovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		p.change(1, nil)
		s := newWorkScheduler(p)
		synctest.Wait() // The pre-readiness commit notification has been consumed.
		entered, release := make(chan struct{}), make(chan struct{})
		p.mu.Lock()
		p.afterRead = func() { close(entered); <-release }
		p.mu.Unlock()
		var queries, outbound atomic.Int32
		r := schedulerRegistration(1, func(context.Context) (bool, error) { queries.Add(1); return false, nil }, func(context.Context) (bool, error) { outbound.Add(1); return false, nil })
		s.register(r)
		defer finishScheduler(s, r)
		<-entered
		synctest.Wait()
		if queries.Load() != 1 || outbound.Load() != 1 {
			t.Fatalf("ready claims waited for global history discovery: queries=%d outbound=%d", queries.Load(), outbound.Load())
		}
		close(release)
		synctest.Wait()
		if queries.Load() != 1 || outbound.Load() != 1 {
			t.Fatal("empty discovery repeated consumed startup tokens")
		}
	})
}

func TestSchedulerCommitWakesEmptyHintWhileDiscoveryBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		s := newWorkScheduler(p)
		var firstQueries, firstOutbound, secondQueries, secondOutbound atomic.Int32
		first := schedulerRegistration(1, func(context.Context) (bool, error) { firstQueries.Add(1); return false, nil }, func(context.Context) (bool, error) { firstOutbound.Add(1); return false, nil })
		second := schedulerRegistration(2, func(context.Context) (bool, error) { secondQueries.Add(1); return false, nil }, func(context.Context) (bool, error) { secondOutbound.Add(1); return false, nil })
		s.register(first)
		s.register(second)
		defer finishScheduler(s, first, second)
		synctest.Wait()
		entered, release := make(chan struct{}), make(chan struct{})
		p.mu.Lock()
		p.afterRead = func() { close(entered); <-release }
		p.mu.Unlock()
		p.change(99, nil)
		<-entered
		p.change(1, nil)
		synctest.Wait()
		if firstQueries.Load() != 2 || firstOutbound.Load() != 2 {
			t.Fatalf("committed work waited for in-flight empty discovery: queries=%d outbound=%d", firstQueries.Load(), firstOutbound.Load())
		}
		if secondQueries.Load() != 1 || secondOutbound.Load() != 1 {
			t.Fatal("unrelated scoped commit unparked another integration")
		}
		for range 10 {
			select {
			case p.changes <- struct{}{}:
			default:
			}
			synctest.Wait()
		}
		if firstQueries.Load() != 2 || firstOutbound.Load() != 2 {
			t.Fatal("same-generation notifications repeated an empty claim")
		}
		p.change(0, nil)
		synctest.Wait()
		if firstQueries.Load() != 3 || firstOutbound.Load() != 3 || secondQueries.Load() != 2 || secondOutbound.Load() != 2 {
			t.Fatal("root invalidation did not wake registered native claim paths")
		}
		s.wake(2)
		synctest.Wait()
		if secondQueries.Load() != 3 || secondOutbound.Load() != 3 {
			t.Fatal("backend state change waited for global discovery")
		}
		close(release)
		synctest.Wait()
	})
}

func TestSchedulerSingleIntegrationRetainsFourAdmissionSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		p.candidates = []gateway.WorkCandidate{{IntegrationID: 1, Query: gateway.WorkHint{ProbeNow: true}}}
		s := newWorkScheduler(p)
		release := make(chan struct{})
		var calls atomic.Int32
		r := schedulerRegistration(1, func(context.Context) (bool, error) { calls.Add(1); <-release; return false, nil }, emptyScheduledStep)
		s.register(r)
		synctest.Wait()
		if calls.Load() != 4 {
			t.Fatalf("single-integration admission slots=%d", calls.Load())
		}
		s.unregister(r.id)
		close(release)
		r.workers.Wait()
		synctest.Wait()
		if calls.Load() != 4 {
			t.Fatal("unregistered integration received another task")
		}
		s.close()
	})
}

func TestSchedulerDiscoveryFailureStopsRegisteredAndLateIntegrations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		p.err = errors.New("synthetic private database detail")
		s := newWorkScheduler(p)
		failures := make(chan error, 2)
		first := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep)
		second := schedulerRegistration(2, emptyScheduledStep, emptyScheduledStep)
		first.fatal = func(err error) { failures <- err }
		second.fatal = first.fatal
		s.register(first)
		synctest.Wait()
		s.register(second)
		defer finishScheduler(s, first, second)
		synctest.Wait()
		if len(failures) != 2 {
			t.Fatalf("storage failure did not reach every registration: %d", len(failures))
		}
		for range 2 {
			if err := <-failures; err.Error() != "runtime work discovery failed" {
				t.Fatalf("unsafe storage failure: %v", err)
			}
		}
	})
}

func TestSchedulerRetainsDiscoveryFailureAfterUnregister(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newSchedulerPlanner()
		entered, release := make(chan struct{}), make(chan struct{})
		p.afterRead = func() { close(entered); <-release }
		p.err = errors.New("private late storage failure")
		s := newWorkScheduler(p)
		r := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep)
		s.register(r)
		<-entered
		s.unregister(r.id)
		close(release)
		synctest.Wait()
		if err := s.close(); err == nil || err.Error() != "runtime work discovery failed" {
			t.Fatalf("late storage failure was lost or exposed private detail: %v", err)
		}
	})
}

func TestSchedulerCloseJoinsDiscoveryAndIgnoresNativeCancellation(t *testing.T) {
	for _, realFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancellation", true: "storage_failure"}[realFailure], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newSchedulerPlanner()
				entered, release := make(chan struct{}), make(chan struct{})
				p.afterRead = func() { close(entered); <-release }
				p.err = context.Canceled
				if realFailure {
					p.err = errors.New("private storage error racing close")
				}
				s := newWorkScheduler(p)
				r := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep)
				s.register(r)
				<-entered
				s.unregister(r.id)
				closed := make(chan error, 1)
				go func() { closed <- s.close() }()
				<-s.ctx.Done()
				synctest.Wait()
				if len(closed) != 0 {
					t.Fatal("scheduler closed before its discovery joined")
				}
				close(release)
				err := <-closed
				if realFailure {
					if err == nil || err.Error() != "runtime work discovery failed" {
						t.Fatalf("real discovery failure vanished during close: %v", err)
					}
				} else if err != nil {
					t.Fatalf("native cancellation became a shared failure: %v", err)
				}
			})
		})
	}
}
