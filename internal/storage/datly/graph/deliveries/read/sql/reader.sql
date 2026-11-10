SELECT * FROM  (SELECT m.* FROM outbox_messages m
#if($Mode == "claim")
WHERE m.state='pending' AND m.next_attempt_at <= $Now
AND NOT EXISTS (SELECT 1 FROM outbox_messages cd WHERE cd.workspace_id=m.workspace_id AND cd.state='pending' AND cd.next_attempt_at>$Now)
AND NOT EXISTS (SELECT 1 FROM request_status rs JOIN inbox_jobs j ON j.id=rs.job_id WHERE j.workspace_id=m.workspace_id AND rs.rate_limited_until>$Now)
AND NOT EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=m.workspace_id AND a.rate_limited_until>$Now)
AND NOT EXISTS (SELECT 1 FROM outbox_messages earlier WHERE earlier.workspace_id=m.workspace_id AND earlier.channel_id=m.channel_id AND earlier.root_thread_ts=m.root_thread_ts AND earlier.id<m.id AND earlier.state!='delivered')
ORDER BY m.id LIMIT 1
#elseif($Mode == "recover")
WHERE m.state='sending'
#else
WHERE m.id=$ID
#end
)  deliveries