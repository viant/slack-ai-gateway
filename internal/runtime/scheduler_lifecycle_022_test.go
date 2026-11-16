package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

type lifecycle022Planner struct {
	*schedulerPlanner
	hints   []gateway.WorkCandidate
	threads map[gateway.PhysicalThreadKey]uint64
	forgot  map[gateway.PhysicalThreadKey]int
	reads   atomic.Int32
}

func newLifecycle022Planner() *lifecycle022Planner {
	return &lifecycle022Planner{schedulerPlanner: newSchedulerPlanner(), threads: map[gateway.PhysicalThreadKey]uint64{}, forgot: map[gateway.PhysicalThreadKey]int{}}
}
func (p *lifecycle022Planner) WorkPage(context.Context, time.Time, *gateway.WorkCursor) (gateway.WorkPage, error) {
	p.reads.Add(1)
	p.mu.Lock()
	rows := append([]gateway.WorkCandidate(nil), p.candidates...)
	hook, err := p.afterRead, p.err
	p.afterRead = nil
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	return gateway.WorkPage{Candidates: rows}, err
}
func (p *lifecycle022Planner) TakeWorkHints() []gateway.WorkCandidate {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.hints
	p.hints = nil
	return result
}
func (p *lifecycle022Planner) ThreadRevision(key gateway.PhysicalThreadKey) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.threads[key]
}
func (p *lifecycle022Planner) ForgetWorkThread(key gateway.PhysicalThreadKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pending := range p.hints {
		if pending.Thread == key {
			return
		}
	}
	p.forgot[key]++
	delete(p.threads, key)
}
func (p *lifecycle022Planner) hint(candidates ...gateway.WorkCandidate) {
	p.mu.Lock()
	for _, candidate := range candidates {
		p.threads[candidate.Thread]++
		p.hints = append(p.hints, candidate)
	}
	p.mu.Unlock()
	select {
	case p.changes <- struct{}{}:
	default:
	}
}
func lifecycle022Candidate(id int64, root string) gateway.WorkCandidate {
	return gateway.WorkCandidate{IntegrationID: id, Thread: gateway.PhysicalThreadKey{AppID: "A", WorkspaceID: "T", ChannelID: "C", RootTS: root}, Query: gateway.WorkHint{ProbeNow: true}}
}

func TestScheduler022BuffersStartupButDropsRemovedRuntimeWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newLifecycle022Planner()
		s := newWorkScheduler(p)
		var mu sync.Mutex
		calls := map[string]int{}
		registration := func(id int64) *scheduledRuntime {
			r := schedulerRegistration(id, emptyScheduledStep, emptyScheduledStep)
			r.queryThread = func(_ context.Context, key gateway.PhysicalThreadKey) (bool, error) {
				mu.Lock()
				calls[key.RootTS]++
				mu.Unlock()
				return false, nil
			}
			return r
		}
		count := func(root string) int { mu.Lock(); defer mu.Unlock(); return calls[root] }
		p.hint(lifecycle022Candidate(1, "early"))
		synctest.Wait()
		first := registration(1)
		s.register(first)
		synctest.Wait()
		if count("early") != 1 {
			t.Fatal("pre-registration committed hint was lost")
		}
		p.hint(lifecycle022Candidate(2, "buffered-before-remove"))
		synctest.Wait()
		s.unregister(2) // Explicit removal also retires buffered startup work.
		p.change(0, []gateway.WorkCandidate{lifecycle022Candidate(3, "old-registered")})
		old := registration(3)
		s.register(old)
		synctest.Wait()
		if count("old-registered") == 0 {
			t.Fatal("fixture did not create a real registered thread")
		}
		s.unregister(3)
		old.workers.Wait()
		for round := range 3 {
			p.change(0, []gateway.WorkCandidate{lifecycle022Candidate(2, fmt.Sprintf("late-page-two-%d", round)), lifecycle022Candidate(3, fmt.Sprintf("late-page-three-%d", round))})
			p.hint(lifecycle022Candidate(2, fmt.Sprintf("late-hint-two-%d", round)), lifecycle022Candidate(3, fmt.Sprintf("late-hint-three-%d", round)))
			synctest.Wait()
			time.Sleep(reconciliationInterval)
			synctest.Wait()
		}
		p.mu.Lock()
		p.candidates = nil
		p.mu.Unlock()
		second, restarted := registration(2), registration(3)
		s.register(second)
		s.register(restarted)
		defer finishScheduler(s, first, second, restarted)
		synctest.Wait()
		mu.Lock()
		for root, n := range calls {
			if root != "early" && root != "old-registered" && n != 0 {
				mu.Unlock()
				t.Fatalf("removed runtime retained old buffered work: %s=%d", root, n)
			}
		}
		mu.Unlock()
		p.hint(lifecycle022Candidate(2, "fresh-two"), lifecycle022Candidate(3, "fresh-three"))
		synctest.Wait()
		if count("fresh-two") != 1 || count("fresh-three") != 1 {
			t.Fatal("deliberate fresh registration did not accept new work")
		}
		p.mu.Lock()
		forgot := p.forgot[lifecycle022Candidate(3, "old-registered").Thread]
		p.mu.Unlock()
		if forgot == 0 {
			t.Fatal("removed registered key retained revision metadata")
		}
	})
}

func TestScheduler022RetainsSharedPhysicalRevisionUntilLastRuntimeLeaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newLifecycle022Planner()
		one, two := lifecycle022Candidate(1, "shared"), lifecycle022Candidate(2, "shared")
		p.candidates = []gateway.WorkCandidate{one, two}
		p.threads[one.Thread] = 7
		s := newWorkScheduler(p)
		first, second := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep), schedulerRegistration(2, emptyScheduledStep, emptyScheduledStep)
		s.register(first)
		s.register(second)
		defer s.close()
		synctest.Wait()
		s.unregister(1)
		first.workers.Wait()
		p.hint(one)
		synctest.Wait()
		if p.ThreadRevision(one.Thread) != 8 {
			t.Fatal("removing one integration reset another live physical-thread revision")
		}
		s.unregister(2)
		second.workers.Wait()
		synctest.Wait()
		if p.ThreadRevision(one.Thread) != 0 {
			t.Fatal("last removed entry retained unused revision metadata")
		}
		before := p.reads.Load()
		time.Sleep(2 * reconciliationInterval)
		synctest.Wait()
		if p.reads.Load() != before {
			t.Fatal("tombstones alone kept discovery running")
		}
	})
}

func TestScheduler022LateDiscoveryFailureSurvivesRuntimeRemoval(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(fmt.Sprint(healthy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p := newLifecycle022Planner()
				entered, release := make(chan struct{}), make(chan struct{})
				p.err = errors.New("fixture discovery failure")
				p.afterRead = func() { close(entered); <-release }
				s := newWorkScheduler(p)
				first := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep)
				s.register(first)
				<-entered
				s.unregister(1)
				first.workers.Wait()
				var fatal atomic.Int32
				var second *scheduledRuntime
				if healthy {
					second = schedulerRegistration(2, emptyScheduledStep, emptyScheduledStep)
					second.fatal = func(error) { fatal.Add(1) }
					s.register(second)
				}
				close(release)
				synctest.Wait()
				if healthy {
					if fatal.Load() != 1 {
						t.Fatal("late shared discovery failure did not stop the remaining runtime")
					}
					s.unregister(2)
					second.workers.Wait()
				}
				if err := s.close(); err == nil || err.Error() != "runtime work discovery failed" {
					t.Fatalf("late discovery failure was lost: %v", err)
				}
			})
		})
	}
}
