package datly_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/viant/slack-ai-gateway/internal/gateway"
	"github.com/viant/slack-ai-gateway/internal/storetest"
)

func TestNativeWorkPagesBoundPhysicalKeysAndFreezePopulation(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			ctx := context.Background()
			now := time.Now().UTC().Truncate(time.Millisecond)
			at := now.Add(-time.Minute)
			tx, err := db.BeginTx(ctx, nil)
			storetest.Must(t, err)
			stmt, err := tx.PrepareContext(ctx, `INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,slack_progress_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
VALUES (?,'C',?,?,'alice',?,'metadata fixture',0,?,'pending','skipped',?,?,?)`)
			storetest.Must(t, err)
			for i := 0; i < 600; i++ {
				key := fmt.Sprintf("1000.%06d", i)
				_, err = stmt.ExecContext(ctx, id, key, "page-event-"+key, key, "page-request-"+key, at, at, at)
				storetest.Must(t, err)
			}
			storetest.Must(t, stmt.Close())
			storetest.Must(t, tx.Commit())
			planner := root.(gateway.ThreadWorkPlanner)
			first, err := planner.WorkPage(ctx, now, nil)
			storetest.Must(t, err)
			if len(first.Candidates) != 256 || first.Next == nil {
				t.Fatalf("first page size/continuation=%d/%+v", len(first.Candidates), first.Next)
			}
			cutoff := first.Next.Cutoff
			// Rows created during a round, both before and after its cursor,
			// belong to the next round instead of extending this one forever.
			later := now.Add(2 * time.Second)
			for _, key := range []string{"0000.000001", "9999.000001"} {
				_, err = db.ExecContext(ctx, `INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_has_attachments,gateway_request_id,gateway_execution_state,slack_progress_state,created_at,gateway_execution_updated_at,gateway_status_updated_at)
VALUES (?,'C',?,?,'alice',?,0,?,'pending','skipped',?,?,?)`, id, key, "late-event-"+key, key, "late-request-"+key, later, later, later)
				storetest.Must(t, err)
			}
			seen := map[gateway.PhysicalThreadKey]bool{}
			add := func(page gateway.WorkPage) {
				t.Helper()
				if len(page.Candidates) > 256 {
					t.Fatalf("page returned %d keys", len(page.Candidates))
				}
				for _, candidate := range page.Candidates {
					if !candidate.Thread.Valid() || !candidate.Query.ProbeNow || candidate.Outbound.ProbeNow {
						t.Fatalf("invalid metadata candidate: %+v", candidate)
					}
					if seen[candidate.Thread] {
						t.Fatalf("duplicate key across keyset pages: %+v", candidate.Thread)
					}
					seen[candidate.Thread] = true
				}
			}
			add(first)
			cursor := first.Next
			for cursor != nil {
				if !cursor.Cutoff.Equal(cutoff) {
					t.Fatal("round cutoff moved between pages")
				}
				next, err := planner.WorkPage(ctx, now.Add(3*time.Second), cursor)
				storetest.Must(t, err)
				add(next)
				cursor = next.Next
			}
			if len(seen) != 600 {
				t.Fatalf("round included new rows or skipped old keys: %d", len(seen))
			}
			nextRound, err := planner.WorkPage(ctx, later.Add(time.Second), nil)
			storetest.Must(t, err)
			if len(nextRound.Candidates) == 0 || nextRound.Candidates[0].Thread.RootTS != "0000.000001" {
				t.Fatalf("new key behind prior cursor never appeared: %+v", nextRound)
			}
		})
	}
}

func TestNativeWorkDeadlineAppliesPhysicalBotCooldownBeforeMinimum(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql"} {
		t.Run(driver, func(t *testing.T) {
			root, db := sharedFixture(t, driver)
			id := storetest.RegisterDefault(t, root)
			now := time.Now().UTC().Truncate(time.Millisecond)
			ctx := context.Background()
			// Every progress message has a different refresh/retry; a bot-wide
			// 429 must gate all of them, including a deadline behind a cursor.
			for i, seconds := range []int{10, 20, 40} {
				key := fmt.Sprintf("2000.%06d", i)
				at := now.Add(-time.Minute)
				refresh := now.Add(time.Duration(seconds) * time.Second)
				_, err := db.ExecContext(ctx, `INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_has_attachments,gateway_request_id,gateway_execution_state,slack_progress_state,slack_progress_message_id,slack_progress_phase,slack_progress_revision,slack_progress_sent_revision,slack_progress_refresh_at,created_at,gateway_execution_updated_at,gateway_status_updated_at)
VALUES (?,'C',?,?,'alice',?,0,?,'querying','ready',?,'running',2,2,?,?,?,?)`, id, key, "deadline-event-"+key, key, "deadline-request-"+key, key+"-status", refresh, at, at, at)
				storetest.Must(t, err)
			}
			cooldown := now.Add(30 * time.Second)
			_, err := db.ExecContext(ctx, "UPDATE gateway_job SET slack_progress_rate_limited_until=? WHERE slack_thread_root_message_id='2000.000002'", cooldown)
			storetest.Must(t, err)
			planner := root.(gateway.ThreadWorkPlanner)
			page, err := planner.WorkPage(ctx, now, &gateway.WorkCursor{After: gateway.PhysicalThreadKey{AppID: "Z", WorkspaceID: "Z", ChannelID: "Z", RootTS: "Z"}, Cutoff: now})
			storetest.Must(t, err)
			if len(page.Candidates) != 0 || page.Deadline == nil || !page.Deadline.Outbound.NextAt.Equal(cooldown) || page.Deadline.Thread.RootTS != "2000.000000" {
				t.Fatalf("deadline lost cooldown or behind-cursor key: %+v", page)
			}
		})
	}
}
