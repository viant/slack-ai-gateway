package runtime

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
)

func TestScheduler026PrunesAbsentThreadAfterInflightEmptyClaim(t *testing.T) {
	for _, lane := range []string{"query", "outbound"} {
		t.Run(lane, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				planner := newLifecycle022Planner()
				candidate := lifecycle022Candidate(1, "settled")
				if lane == "outbound" {
					candidate.Query = gateway.WorkHint{}
					candidate.Outbound = gateway.WorkHint{ProbeNow: true}
				}
				entered, release := make(chan struct{}), make(chan struct{})
				absent := make(chan struct{})
				planner.candidates = []gateway.WorkCandidate{candidate}
				scheduler := newWorkScheduler(planner)
				var calls atomic.Int32
				empty := func(context.Context) (bool, error) {
					if calls.Add(1) == 1 {
						close(entered)
						<-release
					}
					return false, nil
				}
				registration := schedulerRegistration(1, emptyScheduledStep, emptyScheduledStep)
				if lane == "query" {
					registration.query = empty
				} else {
					registration.outbound = empty
				}
				scheduler.register(registration)
				defer finishScheduler(scheduler, registration)
				<-entered // The authoritative first page supplied ProbeNow.
				synctest.Wait()
				planner.mu.Lock()
				planner.candidates = nil
				planner.afterRead = func() { close(absent) }
				planner.mu.Unlock()
				time.Sleep(reconciliationInterval)
				<-absent
				synctest.Wait() // Absence has been processed, but the call is active.
				if calls.Load() != 1 {
					t.Fatalf("initial empty claims = %d, want 1", calls.Load())
				}
				close(release)
				synctest.Wait() // The empty result must retire the unseen entry.
				for round := range 3 {
					time.Sleep(reconciliationInterval)
					synctest.Wait()
					if calls.Load() != 1 {
						t.Fatalf("authoritatively absent thread retried empty %s claim after round %d: calls=%d, want 1", lane, round+1, calls.Load())
					}
				}
				planner.mu.Lock()
				forgotten := planner.forgot[candidate.Thread]
				planner.mu.Unlock()
				if forgotten == 0 {
					t.Fatal("settled thread retained its revision metadata")
				}
			})
		})
	}
}
