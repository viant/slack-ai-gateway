WHERE EXISTS (
    SELECT 1 FROM request_status s WHERE s.job_id=j.id
    AND (s.mode IN ('message','hybrid')
        OR (s.mode='agent_session' AND s.slack_ts <> '')
        #if($Mode == "backend_status")
        OR (s.mode='agent_session' AND j.turn_id <> '' AND j.state IN ('processing','querying'))
        #end
    )
    AND (s.state='ready' OR (s.state='settled' AND s.phase='delivery_uncertain'
        AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('failed','delivery_unknown'))))
    AND s.next_attempt_at <= $Now
)
AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.workspace_id=j.workspace_id AND o.state='pending' AND o.next_attempt_at > $Now)
AND NOT EXISTS (SELECT 1 FROM request_status s JOIN inbox_jobs lj ON lj.id=s.job_id WHERE lj.workspace_id=j.workspace_id AND s.rate_limited_until > $Now)
AND NOT EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=j.workspace_id AND a.rate_limited_until > $Now)
ORDER BY (SELECT s.last_sent_at FROM request_status s WHERE s.job_id=j.id),j.id
