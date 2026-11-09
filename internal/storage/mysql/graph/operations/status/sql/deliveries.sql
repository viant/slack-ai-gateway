SELECT * FROM (SELECT 1 AS snapshot_id, id, job_id, workspace_id, channel_id,
       root_thread_ts, part, state, slack_ts, attempts, next_attempt_at
FROM outbox_messages ORDER BY id DESC
) deliveries