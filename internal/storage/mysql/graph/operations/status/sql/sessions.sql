SELECT * FROM (SELECT 1 AS snapshot_id, s.mode,a.workspace_id,a.channel_id,a.root_thread_ts,
       a.source_job_id,a.initiator_user_id,a.desired_status,a.agent_status,
       a.session_status,a.state,a.revision,a.sent_revision,a.attempts,
       a.next_attempt_at,a.refresh_at,a.rate_limited_until,a.first_sent_at,
       a.last_sent_at,a.reason,a.warning
FROM agent_session_publications a JOIN request_status s ON s.job_id=a.source_job_id
ORDER BY a.latest_job_id DESC
) sessions