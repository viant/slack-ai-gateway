SELECT MAX(candidates.not_before) AS not_before
FROM (
    SELECT COALESCE(MAX(next_attempt_at),0) AS not_before
    FROM outbox_messages
    WHERE workspace_id=$WorkspaceID AND state='pending'
    UNION ALL
    SELECT COALESCE(MAX(s.rate_limited_until),0) AS not_before
    FROM request_status s JOIN inbox_jobs j ON j.id=s.job_id
    WHERE j.workspace_id=$WorkspaceID
    UNION ALL
    SELECT COALESCE(MAX(rate_limited_until),0) AS not_before
    FROM agent_session_publications
    WHERE workspace_id=$WorkspaceID
) candidates
