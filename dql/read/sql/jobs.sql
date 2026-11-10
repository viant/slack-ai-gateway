SELECT j.*,
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
${embed:status.sql}
#elseif($Mode == "backend_status")
${embed:status.sql}
#elseif($Mode == "recover_status")
WHERE EXISTS(SELECT 1 FROM request_status s WHERE s.job_id=j.id AND s.state='sending')
ORDER BY j.id
#elseif($Mode == "recover")
WHERE j.state IN ('processing','querying')
ORDER BY j.id
#elseif($Mode == "unstarted")
WHERE j.state IN ('pending','processing') AND j.turn_id = ''
AND NOT EXISTS (
 SELECT 1 FROM conversation_bindings b
 WHERE b.id = j.binding_id AND b.state IN ('creating','needs_review')
)
ORDER BY j.id
#elseif($Mode == "key")
WHERE ($ID <> 0 OR $AppID <> '' OR $WorkspaceID <> '' OR $RequestID <> '')
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
ORDER BY j.id
#else
WHERE 1=0
#end
