SELECT * FROM  (SELECT j.*,
 EXISTS(SELECT 1 FROM inbox_jobs earlier WHERE earlier.workspace_id=j.workspace_id AND earlier.channel_id=j.channel_id
 AND earlier.root_thread_ts=j.root_thread_ts AND earlier.id<>j.id
 AND ((earlier.id<j.id AND earlier.state NOT IN ('completed','rejected'))
 OR earlier.state IN ('processing','querying','needs_review'))) AS earlier,
 EXISTS(SELECT 1 FROM inbox_jobs blocked WHERE blocked.workspace_id=j.workspace_id AND blocked.channel_id=j.channel_id
 AND blocked.root_thread_ts=j.root_thread_ts AND blocked.id<>j.id AND blocked.state='needs_review') AS blocked
FROM inbox_jobs j
#if($Mode == "claim")
WHERE j.next_attempt_at <= $Now AND (
 (j.state='querying' AND j.poll_ready=1)
 OR (
  j.state='pending'
  AND NOT EXISTS (
   SELECT 1 FROM inbox_jobs active_job
   WHERE active_job.workspace_id=j.workspace_id AND active_job.channel_id=j.channel_id
   AND active_job.root_thread_ts=j.root_thread_ts AND active_job.id<>j.id
   AND active_job.state IN ('processing','querying','needs_review')
  )
  AND NOT EXISTS (
   SELECT 1 FROM inbox_jobs earlier
   WHERE earlier.workspace_id=j.workspace_id AND earlier.channel_id=j.channel_id
   AND earlier.root_thread_ts=j.root_thread_ts AND earlier.id<j.id
   AND earlier.state NOT IN ('completed','rejected')
  )
 )
)
ORDER BY j.id LIMIT 1
#elseif($Mode == "status")
WHERE EXISTS (
    SELECT 1 FROM request_status s WHERE s.job_id=j.id
    AND s.mode IN ('message','hybrid')
    AND (s.state='ready' OR (s.state='settled' AND s.phase='delivery_uncertain'
        AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.job_id=j.id AND o.state IN ('failed','delivery_unknown'))))
    AND s.next_attempt_at <= $Now
)
AND NOT EXISTS (SELECT 1 FROM outbox_messages o WHERE o.workspace_id=j.workspace_id AND o.state='pending' AND o.next_attempt_at > $Now)
AND NOT EXISTS (SELECT 1 FROM request_status s JOIN inbox_jobs lj ON lj.id=s.job_id WHERE lj.workspace_id=j.workspace_id AND s.rate_limited_until > $Now)
AND NOT EXISTS (SELECT 1 FROM agent_session_publications a WHERE a.workspace_id=j.workspace_id AND a.rate_limited_until > $Now)
ORDER BY (SELECT s.last_sent_at FROM request_status s WHERE s.job_id=j.id),j.id

#elseif($Mode == "recover_status")
WHERE EXISTS(SELECT 1 FROM request_status s WHERE s.job_id=j.id AND s.state='sending')
ORDER BY j.id
#elseif($Mode == "recover")
WHERE j.state IN ('processing','querying')
ORDER BY j.id
#elseif($Mode == "key")
WHERE ($ID <> 0 OR $AppID <> '' OR $WorkspaceID <> '' OR $RequestID <> '')
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
ORDER BY j.id
#else
WHERE 1=0
#end
)  jobs