package datly_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestThreadClosure024PreservesConfirmedOutcomeWithoutInventingResponse(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, outcome := range []string{"completed", "failed", "canceled"} {
				t.Run(outcome, func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("confirmed-before-crash", "confirmed-root", "alice")
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, _ := closure024Start(t, store, true)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, outcome, time.Now()))
					// The canonical terminal observation committed, but the response
					// had not been persisted when the old process stopped.
					_, err = db.Exec("UPDATE gateway_job SET backend_submission_started_at=? WHERE id=?", time.Now().UTC().Add(-16*time.Minute).Truncate(time.Millisecond), job.ID)
					storetest.Must(t, err)
					storetest.Must(t, root.Recover(ctx))
					recovered := closure024Job(t, root, job.ID)
					if recovered.State != "querying" || recovered.Outcome != outcome {
						t.Fatalf("startup closed a recoverable confirmed outcome: %+v", recovered)
					}
					// Exercise explicit closure separately from startup recovery.
					_, err = store.CloseThread(ctx, job.ID, "reconciliation_deadline")
					storetest.Must(t, err)
					closed := closure024Job(t, root, job.ID)
					if closed.Outcome != outcome || closed.State != "rejected" || closed.Reason != "conversation_closed:reconciliation_deadline" {
						t.Fatalf("local closure changed canonical execution facts: %+v", closed)
					}
					notice, err := store.ClaimDelivery(ctx)
					storetest.Must(t, err)
					if notice == nil || !strings.Contains(notice.Text, outcome) || strings.Contains(notice.Text, "could not confirm the outcome") || strings.Contains(notice.Text, "Elapsed:") {
						t.Fatalf("closure misrepresented a known terminal outcome: %+v", notice)
					}
				})
			}
		})
	}
}

func TestThreadClosure024SerializesConcurrentIntakeAndClosure(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, enqueueFirst := range []bool{true, false} {
				name := "closure_first"
				if enqueueFirst {
					name = "intake_first"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					root.(interface{ StartThreadOwnership() }).StartThreadOwnership()
					message := storetest.Message("concurrent-source", "concurrent-root", "alice")
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, _ := closure024Start(t, store, true)
					owner := store.(gateway.AsyncThreadStore)
					entered, release := make(chan struct{}), make(chan struct{})
					barrierDone := make(chan error, 1)
					if !owner.TryInThread(ctx, func(gateway.Store) error { close(entered); <-release; return nil }, func(err error) { barrierDone <- err }) {
						t.Fatal("owner barrier was not admitted")
					}
					select {
					case <-entered:
					case <-time.After(2 * time.Second):
						t.Fatal("owner barrier did not start")
					}
					followup := message
					followup.EventID, followup.MessageTS = "concurrent-followup", "concurrent.000002"
					intakeDone, closureDone := make(chan error, 1), make(chan error, 1)
					intake := func(local gateway.Store) error { _, err := local.Enqueue(ctx, followup); return err }
					closure := func(local gateway.Store) error {
						closed, err := local.CloseThread(ctx, job.ID, "reconciliation_deadline")
						if err == nil && !closed {
							t.Error("first local closure was not performed")
						}
						return err
					}
					queueIntake := func() bool { return owner.TryInThread(ctx, intake, func(err error) { intakeDone <- err }) }
					queueClosure := func() bool { return owner.TryInThread(ctx, closure, func(err error) { closureDone <- err }) }
					first, second := queueClosure, queueIntake
					if enqueueFirst {
						first, second = queueIntake, queueClosure
					}
					admitted := first() && second()
					close(release)
					if !admitted {
						t.Fatal("concurrent commands were not admitted to the occupied owner")
					}
					for _, done := range []chan error{barrierDone, intakeDone, closureDone} {
						select {
						case err := <-done:
							storetest.Must(t, err)
						case <-time.After(5 * time.Second):
							t.Fatal("serialized closure/intake did not finish")
						}
					}
					var live, questions int
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job WHERE gateway_execution_state IN ('pending','processing','querying','needs_review')").Scan(&live))
					storetest.Must(t, db.QueryRow("SELECT COUNT(*) FROM gateway_job").Scan(&questions))
					if live != 0 || questions != 2 {
						t.Fatalf("intake escaped the closure barrier: live=%d questions=%d", live, questions)
					}
					wantNotices := 1
					if !enqueueFirst {
						wantNotices = 2 // Collective closure plus the later local reply.
					}
					if closure024CountNotices(t, db) != wantNotices {
						t.Fatal("concurrent intake duplicated closure or lost its local reply")
					}
					if claimed, err := store.ClaimJob(ctx); err != nil || claimed != nil {
						t.Fatalf("closed concurrent question became backend work: %+v %v", claimed, err)
					}
				})
			}
		})
	}
}
