SELECT GREATEST(
    COALESCE((SELECT MAX(next_attempt_at) FROM outbox_messages
              WHERE workspace_id=$WorkspaceID AND state='pending'),0),
    COALESCE((SELECT MAX(s.rate_limited_until) FROM request_status s
              JOIN inbox_jobs j ON j.id=s.job_id
              WHERE j.workspace_id=$WorkspaceID),0),
    COALESCE((SELECT MAX(rate_limited_until) FROM agent_session_publications
              WHERE workspace_id=$WorkspaceID),0)
) AS not_before
