package datly_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestObservation026PreservesUncertaintyAcrossRecoveryAndTerminalCallbacks(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			for _, mode := range []gateway.StatusMode{gateway.StatusMessage, gateway.StatusAgentSession, gateway.StatusHybrid} {
				t.Run(string(mode), func(t *testing.T) {
					ctx := context.Background()
					root, db := sharedFixture(t, driver)
					id := storetest.RegisterDefault(t, root)
					message := storetest.Message("observation-026", "observation-root", "alice")
					message.StatusMode = mode
					_, err := root.Scope(id).Enqueue(ctx, message)
					storetest.Must(t, err)
					store := gateway.ForThread(root.Scope(id), message.PhysicalThread())
					job, _ := closure024Start(t, store, true)
					now := time.Now().UTC().Truncate(time.Millisecond)
					confirmed := now.Add(-18 * time.Minute)
					_, err = db.Exec("UPDATE gateway_job SET backend_submission_started_at=? WHERE id=?", confirmed, job.ID)
					storetest.Must(t, err)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", confirmed))
					assertSnapshot := func(wantState string, wantObserved time.Time, wantSince *time.Time) {
						t.Helper()
						var state string
						var at, since sql.NullTime
						storetest.Must(t, db.QueryRow("SELECT backend_observed_state,backend_observed_at,backend_observation_uncertain_since FROM gateway_job WHERE id=?", job.ID).Scan(&state, &at, &since))
						if state != wantState || !at.Valid || !at.Time.Equal(wantObserved) || since.Valid != (wantSince != nil) || wantSince != nil && !since.Time.Equal(*wantSince) {
							t.Fatalf("observation changed: state=%q observed=%+v since=%+v", state, at, since)
						}
					}
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now))
					assertSnapshot("running", confirmed, &now)
					clearWorkChanges(root)
					global, scoped := root.WorkRevision(0), root.WorkRevision(id)
					for range 20 {
						storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", now.Add(time.Hour)))
					}
					assertSnapshot("running", confirmed, &now)
					assertNoWorkChange(t, root, id, global, scoped)
					for range 3 {
						storetest.Must(t, root.Recover(ctx))
						assertSnapshot("running", confirmed, &now)
						resumed, err := store.ClaimJob(ctx)
						storetest.Must(t, err)
						if resumed == nil || resumed.ID != job.ID || resumed.GatewayRequestID != job.GatewayRequestID || !resumed.BackendObservationUncertainSince.Equal(now) || resumed.BackendObservedState != "running" || !resumed.BackendObservedAt.Equal(confirmed) {
							t.Fatalf("recovery lost durable observation: %+v", resumed)
						}
					}
					recovered := now.Add(time.Minute)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "running", recovered))
					assertSnapshot("running", recovered, nil)
					secondOutage := recovered.Add(time.Minute)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "unknown", secondOutage))
					assertSnapshot("running", recovered, &secondOutage)
					finished := secondOutage.Add(time.Minute)
					storetest.Must(t, store.RecordObservation(ctx, job.ID, "completed", finished))
					storetest.Must(t, store.Complete(ctx, job.ID, job.GatewayRequestID, "answer", []gateway.ReplyPart{{Text: "answer", Format: gateway.ReplyPlain}}))
					clearWorkChanges(root)
					global, scoped = root.WorkRevision(0), root.WorkRevision(id)
					for _, late := range []string{"unknown", "running", "queued"} {
						storetest.Must(t, store.RecordObservation(ctx, job.ID, late, finished.Add(time.Minute)))
					}
					assertSnapshot("completed", finished, nil)
					assertNoWorkChange(t, root, id, global, scoped)
				})
			}
		})
	}
}
