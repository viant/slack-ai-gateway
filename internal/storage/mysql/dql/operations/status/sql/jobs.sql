SELECT 1 AS snapshot_id, id, workspace_id, channel_id, root_thread_ts,
       author_id, COALESCE(binding_id,0) AS binding_id, request_id, turn_id,
       state, reason, poll_attempts, query_started_at, next_attempt_at
FROM inbox_jobs ORDER BY id DESC
