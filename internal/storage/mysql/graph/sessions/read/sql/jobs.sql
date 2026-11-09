SELECT * FROM (SELECT j.id,j.workspace_id,j.channel_id,j.root_thread_ts,j.message_ts,j.message_surface,j.created_at,j.state,j.reason,
 COALESCE(s.mode,'message') AS mode,COALESCE(s.observed_state,'') AS observed_state,COALESCE(b.state,'') AS binding_state,
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('pending','sending')) AS pending,
 (SELECT COUNT(*) FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('failed','delivery_unknown')) AS bad
FROM inbox_jobs j LEFT JOIN request_status s ON s.job_id=j.id LEFT JOIN conversation_bindings b ON b.id=j.binding_id
ORDER BY j.id
) jobs