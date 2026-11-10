package mysql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

// Exercise the real cross-workflow lock order: intake updates the native session
// after inserting a job, status reads session/job graphs, and query workers claim
// and settle jobs. No Slack or Agently calls participate in this test.
func TestConcurrentHybridIntakeStatusAndQueryClaims(t *testing.T) {
	s := fixture(t).Store.(*Store)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const producers, eventsEach, initial = 4, 6, 2
	const expected = producers*eventsEach + initial
	for thread := range initial {
		m := storetest.Message(fmt.Sprintf("seed-%d", thread), fmt.Sprintf("thread-%d", thread), "alice")
		m.StatusMode = gateway.StatusHybrid
		inserted, err := s.Enqueue(ctx, m)
		storetest.Must(t, err)
		if !inserted {
			t.Fatal("seed event was not inserted")
		}
	}
	// Begin with an actual publication in flight. Intake must preserve its
	// claimed revision while new questions update the same two session rows.
	firstStatus, err := s.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if firstStatus == nil || firstStatus.Target != gateway.StatusTargetAgentSession {
		t.Fatal("expected an initial native session publication")
	}
	var processed, published atomic.Int64
	var actors, producing sync.WaitGroup
	start := make(chan struct{})
	producersDone := make(chan struct{})
	failures := make(chan error, producers+2)
	report := func(operation string, err error) {
		if err == nil || ctx.Err() != nil && errors.Is(err, context.Canceled) {
			return
		}
		failures <- fmt.Errorf("%s: %w", operation, err)
		cancel()
	}
	allProduced := func() bool {
		select {
		case <-producersDone:
			return true
		default:
			return false
		}
	}
	idle := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Millisecond):
			return true
		}
	}
	for producer := range producers {
		actors.Add(1)
		producing.Add(1)
		go func() {
			defer actors.Done()
			defer producing.Done()
			<-start
			for event := range eventsEach {
				if ctx.Err() != nil {
					return
				}
				id := fmt.Sprintf("producer-%d-event-%d", producer, event)
				m := storetest.Message(id, fmt.Sprintf("thread-%d", event%2), fmt.Sprintf("author-%d", producer))
				m.StatusMode = gateway.StatusHybrid
				inserted, err := s.Enqueue(ctx, m)
				if err != nil {
					report("enqueue "+id, err)
					return
				}
				if !inserted {
					report("enqueue "+id, fmt.Errorf("distinct event was treated as a duplicate"))
					return
				}
			}
		}()
	}
	go func() {
		producing.Wait()
		close(producersDone)
	}()
	actors.Add(1)
	go func() {
		defer actors.Done()
		<-start
		for ctx.Err() == nil {
			j, err := s.ClaimJob(ctx)
			if err != nil {
				report("claim query", err)
				return
			}
			if j == nil {
				if allProduced() && processed.Load() == expected {
					return
				}
				if !idle() {
					return
				}
				continue
			}
			if err := s.Reject(ctx, j.ID, "concurrent_storage_test"); err != nil {
				report(fmt.Sprintf("settle query %d", j.ID), err)
				return
			}
			processed.Add(1)
		}
	}()
	actors.Add(1)
	go func() {
		defer actors.Done()
		<-start
		a := firstStatus
		for ctx.Err() == nil {
			if a == nil {
				var err error
				a, err = s.ClaimStatus(ctx, time.Now(), "urgent")
				if err != nil {
					report("claim status", err)
					return
				}
				if a == nil {
					if allProduced() && processed.Load() == expected {
						return
					}
					if !idle() {
						return
					}
					continue
				}
			}
			if _, err := s.GetStatusContext(ctx, *a); err != nil {
				report("status destination", err)
				return
			}
			now := time.Now()
			result := gateway.StatusResult{State: "ready", NextRefreshAt: now.Add(gateway.DefaultStatusRefresh)}
			if a.Target == gateway.StatusTargetAgentSession {
				result.State = "settled"
				result.Acknowledged = true
				result.AgentStatus = a.SessionState
				result.SessionStatus = a.SessionState
				result.NextRefreshAt = now.Add(gateway.AgentSessionRefresh)
			} else {
				result.SlackTS = a.SlackTS
				if result.SlackTS == "" {
					result.SlackTS = fmt.Sprintf("1800000000.%06d", a.JobID)
				}
				if a.Terminal {
					result.State = "settled"
				}
			}
			if err := s.FinishStatus(ctx, *a, result, now); err != nil {
				report("acknowledge status", err)
				return
			}
			published.Add(1)
			a = nil
		}
	}()
	close(start)
	actors.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if ctx.Err() != nil {
		t.Fatalf("concurrent workflows did not complete: %v; processed=%d/%d, publications=%d", ctx.Err(), processed.Load(), expected, published.Load())
	}
	if processed.Load() != expected || published.Load() == 0 {
		t.Fatalf("incomplete concurrent work: processed=%d/%d, publications=%d", processed.Load(), expected, published.Load())
	}
	snapshot, err := s.Status(ctx, 1000)
	storetest.Must(t, err)
	if len(snapshot.Jobs) != expected || snapshot.Counts["inbox_jobs"]["rejected"] != expected {
		t.Fatalf("lost or unfinished events: %+v", snapshot.Counts["inbox_jobs"])
	}
	for _, r := range snapshot.RequestStatuses {
		if r.State == "sending" {
			t.Fatalf("message publication %d was left in flight", r.JobID)
		}
	}
	for _, r := range snapshot.AgentSessions {
		if r.State == "sending" {
			t.Fatalf("native session %s was left in flight", r.ThreadTS)
		}
	}
}
