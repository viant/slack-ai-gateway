SELECT * FROM (SELECT 1 AS snapshot_id, s.mode,s.job_id,s.slack_ts,s.phase,s.state,
       s.observed_state,s.observed_at,s.observation_uncertain,s.outcome,
       s.revision,s.sent_revision,s.attempts,s.next_attempt_at,s.refresh_at,
       s.rate_limited_until,s.first_sent_at,s.last_sent_at,s.reason,j.created_at
FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id
ORDER BY s.job_id DESC
) statuses