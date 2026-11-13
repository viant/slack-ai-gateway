package datly_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storage"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func discoveryFor(t *testing.T, root storage.Store, id int64, now time.Time) gateway.WorkCandidate {
	t.Helper()
	rows, err := root.WorkCandidates(context.Background(), now)
	storetest.Must(t, err)
	for _, row := range rows {
		if row.IntegrationID == id {
			return row
		}
	}
	t.Fatalf("integration %d absent from native work discovery", id)
	return gateway.WorkCandidate{}
}

func clearWorkChanges(s gateway.WorkPlanner) {
	for {
		select {
		case <-s.WorkChanges():
		default:
			return
		}
	}
}

func assertNoWorkChange(t *testing.T, s gateway.WorkPlanner, id int64, global, scoped uint64) {
	t.Helper()
	if s.WorkRevision(0) != global || s.WorkRevision(id) != scoped {
		t.Fatal("read, no-op or rollback changed work generations")
	}
	select {
	case <-s.WorkChanges():
		t.Fatal("read, no-op or rollback notified work")
	default:
	}
}

func TestWorkDiscoveryIsNativeReadOnlyAndPreservesQueryEligibility(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			s := root.Scope(id)
			now := time.Now().UTC().Truncate(time.Millisecond)
			clearWorkChanges(root)
			global, scoped := root.WorkRevision(0), root.WorkRevision(id)
			// A held writer proves the reader does not start SQLite IMMEDIATE
			// transactions or issue native row-lock reads on either database.
			tx, err := db.BeginTx(ctx, nil)
			storetest.Must(t, err)
			_, err = tx.ExecContext(ctx, "UPDATE integration SET updated_at=updated_at WHERE id=?", id)
			storetest.Must(t, err)
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			rows, err := root.WorkCandidates(readCtx, now)
			cancel()
			storetest.Must(t, tx.Rollback())
			storetest.Must(t, err)
			if len(rows) != 1 || rows[0].Query.ProbeNow || rows[0].Outbound.ProbeNow || !rows[0].Query.NextAt.IsZero() || !rows[0].Outbound.NextAt.IsZero() {
				t.Fatalf("empty store has scheduling work: %+v", rows)
			}
			assertNoWorkChange(t, root, id, global, scoped)
			enqueue := func(event, thread string) int64 {
				_, err := s.Enqueue(ctx, storetest.Message(event, thread, "alice"))
				storetest.Must(t, err)
				var job int64
				storetest.Must(t, db.QueryRow("SELECT id FROM gateway_job WHERE slack_event_id=?", event).Scan(&job))
				return job
			}
			first := enqueue("work-earlier", "ordered")
			second := enqueue("work-later", "ordered")
			review := enqueue("work-review", "blocked")
			enqueue("work-behind-review", "blocked")
			future := now.Add(time.Minute)
			_, err = db.Exec("UPDATE gateway_job SET backend_next_poll_at=? WHERE id=?", future, first)
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='needs_review' WHERE id=?", review)
			storetest.Must(t, err)
			candidate := discoveryFor(t, root, id, now)
			if candidate.Query.ProbeNow || !candidate.Query.NextAt.Equal(future) {
				t.Fatalf("later or blocked question bypassed existing eligibility: %+v", candidate.Query)
			}
			independent := enqueue("work-independent", "independent")
			candidate = discoveryFor(t, root, id, now)
			if !candidate.Query.ProbeNow || !candidate.Query.NextAt.Equal(future) {
				t.Fatalf("immediate work erased independent future deadline: %+v", candidate.Query)
			}
			claimed, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			if claimed == nil || claimed.ID != independent {
				t.Fatal("native scoped claim disagreed with discovery")
			}
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='completed' WHERE id IN (?,?)", first, independent)
			storetest.Must(t, err)
			claimed, err = s.ClaimJob(ctx)
			storetest.Must(t, err)
			if claimed == nil || claimed.ID != second {
				t.Fatal("completed earlier question still blocked native claim")
			}
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='querying',backend_poll_ready=1,backend_next_poll_at=? WHERE id=?", future, second)
			storetest.Must(t, err)
			candidate = discoveryFor(t, root, id, future)
			if !candidate.Query.ProbeNow || !candidate.Query.NextAt.IsZero() {
				t.Fatalf("equal native poll deadline is not due: %+v", candidate.Query)
			}
		})
	}
}

func TestWorkDiscoveryKeepsStatusTimersFallbackAndBotCooldown(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			a, b, c := testIdentity("work-a", "A"), testIdentity("work-b", "A"), testIdentity("work-c", "B")
			registerNativeIntegrations(t, root, &a, &b, &c)
			s := root.Scope(a.ID)
			now := time.Now().UTC().Truncate(time.Millisecond)
			message := storetest.Message("work-status", "work-status", "alice")
			message.StatusMode = gateway.StatusHybrid
			_, err := s.Enqueue(ctx, message)
			storetest.Must(t, err)
			refresh, observed := now.Add(time.Minute), now.Add(-59*time.Second)
			_, err = db.Exec("UPDATE gateway_job SET gateway_execution_state='processing',slack_progress_refresh_at=?,backend_observed_at=?", refresh, observed)
			storetest.Must(t, err)
			candidate := discoveryFor(t, root, a.ID, now)
			stale := observed.Add(gateway.ObservationMaxAge + time.Millisecond)
			if !candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.Equal(stale) {
				t.Fatalf("status staleness timer was lost behind immediate probe: %+v", candidate.Outbound)
			}
			// During backend recovery/blocked state, a submitted native-only
			// question needs a message fallback even without a Slack timestamp.
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_mode='agent_session',gateway_execution_state='querying',backend_submission_started_at=?", now)
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE slack_agent_session_publication SET slack_publication_state='failed'")
			storetest.Must(t, err)
			candidate = discoveryFor(t, root, a.ID, now)
			if !candidate.Outbound.ProbeNow {
				t.Fatal("native-only submitted question lost backend fallback")
			}
			status, err := s.ClaimStatus(ctx, now, "backend_blocked")
			storetest.Must(t, err)
			if status == nil || status.Target != gateway.StatusTargetMessage {
				t.Fatal("native backend fallback path did not claim a message")
			}
			storetest.Must(t, s.FinishStatus(ctx, *status, gateway.StatusResult{State: "ready", SlackTS: "200.000001", NextRefreshAt: refresh}, now))
			cooldown := now.Add(2 * time.Minute)
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_rate_limited_until=?", cooldown)
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE slack_agent_session_publication SET slack_publication_state='ready',slack_publication_refresh_at=?", refresh)
			storetest.Must(t, err)
			for _, identity := range []storage.Integration{a, b} {
				candidate = discoveryFor(t, root, identity.ID, now)
				if candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.Equal(cooldown) {
					t.Fatalf("physical bot cooldown not respected: %+v", candidate)
				}
			}
			candidate = discoveryFor(t, root, c.ID, now)
			if candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.IsZero() {
				t.Fatal("another Slack bot inherited work/cooldown")
			}
			// An unchanged processing session retains its ten-minute timer even
			// when its conservative immediate candidate has already been probed.
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_state='skipped',slack_progress_rate_limited_until=NULL")
			storetest.Must(t, err)
			sessionRefresh := now.Add(gateway.AgentSessionRefresh)
			_, err = db.Exec("UPDATE slack_agent_session_publication SET slack_publication_state='settled',slack_publication_sent_revision=slack_publication_revision,slack_publication_refresh_at=?", sessionRefresh)
			storetest.Must(t, err)
			candidate = discoveryFor(t, root, a.ID, now)
			if !candidate.Outbound.NextAt.Equal(sessionRefresh) {
				t.Fatalf("session refresh timer lost: %+v", candidate.Outbound)
			}
		})
	}
}

func TestWorkChangesObserveNativeCommitAndFanOutPhysicalBot(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) { testWorkChangesObserveNativeCommitAndFanOutPhysicalBot(t, driver) })
	}
}

func testWorkChangesObserveNativeCommitAndFanOutPhysicalBot(t *testing.T, driver string) {
	ctx := context.Background()
	root, db := sharedFixture(t, driver)
	a, b, c := testIdentity("notify-a", "A"), testIdentity("notify-b", "A"), testIdentity("notify-c", "B")
	registerNativeIntegrations(t, root, &a, &b, &c)
	as, bs := root.Scope(a.ID), root.Scope(b.ID)
	if as.IntegrationID() != a.ID || root.IntegrationID() != 0 || as.WorkChanges() != bs.WorkChanges() {
		t.Fatal("scopes do not share the process storage planner")
	}
	discoveryFor(t, root, a.ID, time.Now())
	clearWorkChanges(root)
	global, scoped := root.WorkRevision(0), root.WorkRevision(a.ID)
	_, err := as.ClaimJob(ctx)
	storetest.Must(t, err)
	_, err = as.ClaimDelivery(ctx)
	storetest.Must(t, err)
	_, err = as.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	assertNoWorkChange(t, root, a.ID, global, scoped)
	bBefore, cBefore := root.WorkRevision(b.ID), root.WorkRevision(c.ID)
	m := storetest.Message("notify-event", "notify-thread", "alice")
	inserted, err := as.Enqueue(ctx, m)
	storetest.Must(t, err)
	if !inserted || root.WorkRevision(a.ID) <= scoped || root.WorkRevision(b.ID) <= bBefore || root.WorkRevision(c.ID) != cBefore || root.WorkRevision(0) != global {
		t.Fatal("committed scope mutation did not invalidate only matching physical bots")
	}
	select {
	case <-root.WorkChanges():
	default:
		t.Fatal("committed intake did not notify")
	}
	scoped = root.WorkRevision(a.ID)
	inserted, err = as.Enqueue(ctx, m)
	storetest.Must(t, err)
	if inserted {
		t.Fatal("duplicate inserted")
	}
	assertNoWorkChange(t, root, a.ID, global, scoped)
	job, err := as.ClaimJob(ctx)
	storetest.Must(t, err)
	if job == nil {
		t.Fatal("queued work missing")
	}
	clearWorkChanges(root)
	scoped = root.WorkRevision(a.ID)
	// The job patch is queued before its outbox insert fails. Native rollback
	// must remove both the write and its scheduling notification.
	reject := "CREATE TRIGGER reject_work_outbox BEFORE INSERT ON slack_outgoing_message BEGIN SELECT RAISE(ABORT,'test rollback'); END"
	if driver == "mysql" {
		reject = "CREATE TRIGGER reject_work_outbox BEFORE INSERT ON slack_outgoing_message FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test rollback'"
	}
	_, err = db.Exec(reject)
	storetest.Must(t, err)
	err = as.Complete(ctx, job.ID, "", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}})
	if err == nil {
		t.Fatal("rollback trigger did not reject native write")
	}
	assertNoWorkChange(t, root, a.ID, global, scoped)
	var state string
	storetest.Must(t, db.QueryRow("SELECT gateway_execution_state FROM gateway_job WHERE id=?", job.ID).Scan(&state))
	if state != "processing" {
		t.Fatal("failed native write did not roll back")
	}
	_, err = db.Exec("DROP TRIGGER reject_work_outbox")
	storetest.Must(t, err)
	storetest.Must(t, as.Complete(ctx, job.ID, "", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
	clearWorkChanges(root)
	scoped = root.WorkRevision(a.ID)
	status, err := as.ClaimStatus(ctx, time.Now(), "urgent")
	storetest.Must(t, err)
	if status != nil {
		t.Fatal("fast completed response should skip its first status")
	}
	if root.WorkRevision(a.ID) <= scoped {
		t.Fatal("nil claim hid its committed skipped-status patch")
	}
	clearWorkChanges(root)
	scoped = root.WorkRevision(a.ID)
	storetest.Must(t, root.Recover(ctx))
	assertNoWorkChange(t, root, a.ID, global, scoped)
	// A changed root assignment invalidates the global tuple, including handles
	// which have never yet appeared in a discovery result.
	d := testIdentity("notify-d", "D")
	registerNativeIntegrations(t, root, &a, &b, &c, &d)
	if root.WorkRevision(0) <= global {
		t.Fatal("committed root mutation did not invalidate all work")
	}
}

func TestWorkDiscoveryPreservesDeliveryRetriesAndManualReviewOrdering(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			s := root.Scope(id)
			_, err := s.Enqueue(ctx, storetest.Message("work-delivery", "work-delivery", "alice"))
			storetest.Must(t, err)
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Complete(ctx, job.ID, "", "answer", []gateway.ReplyPart{{Text: "first", Format: gateway.ReplyPlain}, {Text: "second", Format: gateway.ReplyPlain}}))
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_state='skipped'")
			storetest.Must(t, err)
			first, err := s.ClaimDelivery(ctx)
			storetest.Must(t, err)
			now := time.Now().UTC().Truncate(time.Millisecond)
			retry := now.Add(45 * time.Second)
			storetest.Must(t, s.RetryDelivery(ctx, first.ID, retry))
			candidate := discoveryFor(t, root, id, now)
			if candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.Equal(retry) {
				t.Fatalf("delivery retry was bypassed by a later part: %+v", candidate.Outbound)
			}
			candidate = discoveryFor(t, root, id, retry)
			if !candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.IsZero() {
				t.Fatalf("equal delivery deadline not due: %+v", candidate.Outbound)
			}
			_, err = db.Exec("UPDATE slack_outgoing_message SET slack_delivery_state='delivery_unknown',slack_delivery_next_attempt_at=NULL WHERE id=?", first.ID)
			storetest.Must(t, err)
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_state='settled',slack_progress_phase='delivery_uncertain'")
			storetest.Must(t, err)
			candidate = discoveryFor(t, root, id, now)
			if candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.IsZero() {
				t.Fatal("unresolved manual-review delivery advertised later parts or settled status")
			}
			storetest.Must(t, s.ResolveDelivery(ctx, first.ID, "delivered", "200.000001", false))
			candidate = discoveryFor(t, root, id, now)
			if !candidate.Outbound.ProbeNow {
				t.Fatal("operator resolution did not expose remaining work")
			}
			second, err := s.ClaimDelivery(ctx)
			storetest.Must(t, err)
			if second == nil || second.Part != 1 {
				t.Fatal("native delivery claim lost part ordering after operator resolution")
			}
		})
	}
}

func TestWorkDiscoveryWakesImmediateDeliveryAtCooldownBeforeLaterRefresh(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			ctx := context.Background()
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			s := root.Scope(id)
			_, err := s.Enqueue(ctx, storetest.Message("cooldown-before-refresh", "cooldown-before-refresh", "alice"))
			storetest.Must(t, err)
			job, err := s.ClaimJob(ctx)
			storetest.Must(t, err)
			storetest.Must(t, s.Complete(ctx, job.ID, "", "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
			now := time.Now().UTC().Truncate(time.Millisecond)
			cooldown, refresh := now.Add(7*time.Second), now.Add(time.Minute)
			_, err = db.Exec("UPDATE gateway_job SET slack_progress_rate_limited_until=?,slack_progress_refresh_at=? WHERE id=?", cooldown, refresh, job.ID)
			storetest.Must(t, err)
			candidate := discoveryFor(t, root, id, now)
			if candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.Equal(cooldown) {
				t.Fatalf("later refresh delayed the already-due delivery past cooldown: %+v", candidate.Outbound)
			}
			candidate = discoveryFor(t, root, id, cooldown)
			if !candidate.Outbound.ProbeNow || !candidate.Outbound.NextAt.Equal(refresh) {
				t.Fatalf("cooldown expiry lost immediate work or its independent refresh: %+v", candidate.Outbound)
			}
		})
	}
}

func TestWorkChangesConcurrentDiscoveryAndCommitsCoalesce(t *testing.T) {
	ctx := context.Background()
	root, _ := sharedFixture(t, "sqlite")
	id := storetest.RegisterDefault(t, root)
	s := root.Scope(id)
	clearWorkChanges(root)
	before, global := root.WorkRevision(id), root.WorkRevision(0)
	var workers sync.WaitGroup
	errors := make(chan error, 20)
	for reader := range 3 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 4 {
				_, err := root.WorkCandidates(ctx, time.Now())
				if err != nil {
					errors <- fmt.Errorf("reader %d: %w", reader, err)
					return
				}
			}
		}()
	}
	for event := range 6 {
		_, err := s.Enqueue(ctx, storetest.Message(fmt.Sprintf("notify-concurrent-%d", event), fmt.Sprintf("thread-%d", event), "alice"))
		storetest.Must(t, err)
	}
	workers.Wait()
	close(errors)
	for err := range errors {
		storetest.Must(t, err)
	}
	if root.WorkRevision(id) < before+6 || root.WorkRevision(0) != global {
		t.Fatal("concurrent native commits lost or broadened invalidation")
	}
	select {
	case <-root.WorkChanges():
	default:
		t.Fatal("coalesced wake missing")
	}
	select {
	case <-root.WorkChanges():
		t.Fatal("nonblocking wake did not coalesce")
	default:
	}
}
