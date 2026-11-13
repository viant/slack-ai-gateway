package runtime_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"testing/synctest"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storage/mysql"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

type admissionTimingStore struct {
	storage.Store
	claimed chan time.Time
}

func (s admissionTimingStore) ClaimJob(ctx context.Context) (*gateway.GatewayJob, error) {
	job, err := s.Store.ClaimJob(ctx)
	if err == nil && job != nil {
		s.claimed <- time.Now()
	}
	return job, err
}

func TestRuntimeCommittedIntakeWakesIdleAdmission(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			var store storage.Store
			if driver == "sqlite" {
				_, store, _ = setup(t)
			} else {
				root, err := mysql.Open(context.Background(), storetest.MySQLDSN(t), nil)
				storetest.Must(t, err)
				t.Cleanup(func() { storetest.Must(t, root.Close()) })
				id := storetest.RegisterDefault(t, root)
				store = root.Scope(id)
			}
			runtimeCommittedIntakeWakeCase(t, store)
		})
	}
}

func runtimeCommittedIntakeWakeCase(t *testing.T, store storage.Store) {
	t.Helper()
	r, b := runtimeForStore(store)
	claimed := make(chan time.Time, 16)
	r.Store = admissionTimingStore{Store: store, claimed: claimed}
	queries := make(chan time.Time, 16)
	b.query = func(_ context.Context, q gateway.Query) (gateway.Turn, error) {
		queries <- time.Now()
		return gateway.Turn{ID: q.MessageID, State: "completed", Content: "reply"}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sinks := make(chan gateway.EventSink, 1)
	r.Source = sourceFunc(func(ctx context.Context, sink gateway.EventSink, ready func()) error {
		ready()
		sinks <- sink
		<-ctx.Done()
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); storetest.Must(t, finished(t, done)) }()
	var sink gateway.EventSink
	select {
	case sink = <-sinks:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not become ready")
	}
	var latencies []time.Duration
	var intakeLatencies []time.Duration
	var queryLatencies []time.Duration
	for sample := 0; sample < 11; sample++ {
		// Allow the previous admission to finish; the next committed intake must
		// wake an idle lane without waiting for the reconciliation scan.
		if sample > 0 {
			time.Sleep(50 * time.Millisecond)
		}
		at := time.Now()
		message := storetest.Message(fmt.Sprintf("wake-%d", sample), fmt.Sprintf("thread-%d", sample), "alice")
		result, err := sink.Accept(ctx, message)
		storetest.Must(t, err)
		committed := time.Now()
		if !result.Accepted {
			t.Fatal("intake was not persisted")
		}
		select {
		case observed := <-claimed:
			if sample > 0 {
				latencies = append(latencies, max(time.Duration(0), observed.Sub(committed)))
				intakeLatencies = append(intakeLatencies, observed.Sub(at))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("committed intake waited for the five-second reconciliation scan")
		}
		select {
		case observed := <-queries:
			if sample > 0 {
				queryLatencies = append(queryLatencies, observed.Sub(at))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("admitted query did not reach the local backend")
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	sort.Slice(intakeLatencies, func(i, j int) bool { return intakeLatencies[i] < intakeLatencies[j] })
	sort.Slice(queryLatencies, func(i, j int) bool { return queryLatencies[i] < queryLatencies[j] })
	p95 := latencies[len(latencies)-1]
	t.Logf("warm committed-intake→durable-claim p95=%s; intake-start→durable-claim p95=%s; intake-start→backend-Query p95=%s (%d samples)", p95, intakeLatencies[len(intakeLatencies)-1], queryLatencies[len(queryLatencies)-1], len(latencies))
	if p95 >= 500*time.Millisecond {
		t.Fatalf("admission p95=%s, want <500ms with available slots", p95)
	}
}

func TestRuntimeRestartRetainsPersistedPollDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, s, b := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := s.Enqueue(ctx, storetest.Message("submitted", "persisted-poll", "alice"))
		storetest.Must(t, err)
		b.query = func(_ context.Context, q gateway.Query) (gateway.Turn, error) {
			return gateway.Turn{ID: q.MessageID, State: "running"}, nil
		}
		worked, err := (gateway.Worker{Store: s, Policy: r.Policy, Clients: b, AgentID: r.AgentID, Timing: gateway.QueryTiming{PollInitial: time.Millisecond, PollMax: time.Millisecond}}).Step(ctx)
		storetest.Must(t, err)
		if !worked {
			t.Fatal("original query did not start")
		}
		metadata, err := s.Status(ctx, 10)
		storetest.Must(t, err)
		job := metadata.GatewayJobs[0]
		// Take ownership of the scheduled reconciliation once more, then persist
		// its next deadline exactly as a previous process would leave it.
		time.Sleep(time.Millisecond)
		claimed, err := s.ClaimJob(ctx)
		storetest.Must(t, err)
		if claimed == nil || claimed.ID != job.ID {
			t.Fatal("scheduled reconciliation was not claimable")
		}
		deadline := time.Now().Add(3 * time.Second).Truncate(time.Millisecond)
		storetest.Must(t, s.SchedulePoll(ctx, job.ID, deadline))
		attached := &attachOnlyBackend{}
		r.Clients = attached
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()
		defer func() { cancel(); storetest.Must(t, finished(t, done)) }()
		synctest.Wait()
		time.Sleep(2999 * time.Millisecond)
		synctest.Wait()
		if attached.reads.Load() != 0 {
			t.Fatal("restart ignored the persisted poll deadline")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if attached.reads.Load() != 1 || attached.creates.Load() != 0 || attached.queries.Load() != 0 {
			t.Fatalf("deadline did not trigger one read-only attachment: reads=%d creates=%d queries=%d", attached.reads.Load(), attached.creates.Load(), attached.queries.Load())
		}
		metadata, err = s.Status(ctx, 10)
		storetest.Must(t, err)
		if metadata.GatewayJobs[0].BackendSubmissionStartedAt != job.BackendSubmissionStartedAt {
			t.Fatal("reconciliation reset the original submission deadline")
		}
	})
}
