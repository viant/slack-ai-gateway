SELECT * FROM  (SELECT j.*,
 (SELECT integration.slack_app_id FROM integration integration WHERE integration.id=j.integration_id) AS slack_app_id,
 (SELECT integration.slack_workspace_id FROM integration integration WHERE integration.id=j.integration_id) AS slack_workspace_id,
 (SELECT integration.name FROM integration integration WHERE integration.id=j.integration_id) AS integration_name,
 (SELECT integration.active FROM integration integration WHERE integration.id=j.integration_id) AS integration_active,
 (SELECT integration.backend_principal_id FROM integration integration WHERE integration.id=j.integration_id) AS integration_principal_id,
 EXISTS(SELECT 1 FROM gateway_job earlier WHERE earlier.integration_id=j.integration_id AND earlier.slack_channel_id=j.slack_channel_id
 AND earlier.slack_thread_root_message_id=j.slack_thread_root_message_id AND earlier.id<>j.id
 AND ((earlier.id<j.id AND earlier.gateway_execution_state NOT IN ('completed','rejected'))
 OR earlier.gateway_execution_state IN ('processing','querying','needs_review'))) AS earlier,
 EXISTS(SELECT 1 FROM gateway_job blocked WHERE blocked.integration_id=j.integration_id AND blocked.slack_channel_id=j.slack_channel_id
 AND blocked.slack_thread_root_message_id=j.slack_thread_root_message_id AND blocked.id<>j.id AND blocked.gateway_execution_state='needs_review') AS blocked
FROM gateway_job j
#if($Mode == "claim")
WHERE j.id = (
 SELECT eligible.id FROM (
  SELECT candidate.id FROM gateway_job candidate
  WHERE candidate.integration_id=$IntegrationID
   AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=candidate.integration_id AND integration.active=1)
   AND (candidate.backend_next_poll_at IS NULL OR candidate.backend_next_poll_at <= $Now) AND (
   (candidate.gateway_execution_state='querying' AND candidate.backend_poll_ready=1)
   OR (candidate.gateway_execution_state='pending'
    AND NOT EXISTS (SELECT 1 FROM gateway_job active_job WHERE active_job.integration_id=candidate.integration_id
     AND active_job.slack_channel_id=candidate.slack_channel_id AND active_job.slack_thread_root_message_id=candidate.slack_thread_root_message_id
     AND active_job.id<>candidate.id AND active_job.gateway_execution_state IN ('processing','querying','needs_review'))
    AND NOT EXISTS (SELECT 1 FROM gateway_job earlier WHERE earlier.integration_id=candidate.integration_id
     AND earlier.slack_channel_id=candidate.slack_channel_id AND earlier.slack_thread_root_message_id=candidate.slack_thread_root_message_id
     AND earlier.id<candidate.id AND earlier.gateway_execution_state NOT IN ('completed','rejected'))
   ))
  ORDER BY candidate.id LIMIT 1
 ) eligible
)
ORDER BY j.id LIMIT 1
#elseif(($Mode == "status") || ($Mode == "backend_status"))
WHERE 1=1
#if($IntegrationID != 0)
AND (j.integration_id=$IntegrationID OR (
 (j.gateway_execution_reason IN ('integration_changed','integration_removed') OR j.slack_progress_phase IN ('integration_changed','integration_removed'))
 AND EXISTS (SELECT 1 FROM integration historical JOIN integration current_integration
 ON current_integration.slack_app_id=historical.slack_app_id AND current_integration.slack_workspace_id=historical.slack_workspace_id
 WHERE historical.id=j.integration_id AND historical.active=0 AND current_integration.id=$IntegrationID AND current_integration.active=1)))
#end
 AND (j.slack_progress_mode IN ('message','hybrid') OR (j.slack_progress_mode='agent_session' AND j.slack_progress_message_id<>'')
 #if($Mode == "backend_status")
 OR (j.slack_progress_mode='agent_session' AND j.backend_submission_started_at IS NOT NULL AND j.gateway_execution_state IN ('processing','querying'))
 #end
 )
 AND (j.slack_progress_state='ready' OR (j.slack_progress_state='settled' AND j.slack_progress_phase='delivery_uncertain'
 AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_delivery_state IN ('failed','delivery_unknown'))))
 AND (j.slack_progress_next_attempt_at IS NULL OR j.slack_progress_next_attempt_at <= $Now)
AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message o JOIN gateway_job cdj ON cdj.id=o.job_id
 JOIN integration cd_integration ON cd_integration.id=cdj.integration_id
 JOIN integration own_integration ON own_integration.id=j.integration_id
 WHERE cd_integration.slack_app_id=own_integration.slack_app_id AND cd_integration.slack_workspace_id=own_integration.slack_workspace_id
 AND o.slack_delivery_state='pending' AND o.slack_delivery_next_attempt_at>$Now)
AND NOT EXISTS (SELECT 1 FROM gateway_job sj
 JOIN integration si ON si.id=sj.integration_id JOIN integration oi ON oi.id=j.integration_id
 WHERE si.slack_app_id=oi.slack_app_id AND si.slack_workspace_id=oi.slack_workspace_id AND sj.slack_progress_rate_limited_until>$Now)
AND NOT EXISTS (SELECT 1 FROM slack_agent_session_publication a JOIN integration ai ON ai.id=a.initial_integration_id
 JOIN integration oi ON oi.id=j.integration_id WHERE ai.slack_app_id=oi.slack_app_id AND ai.slack_workspace_id=oi.slack_workspace_id
 AND a.slack_publication_rate_limited_until>$Now)
ORDER BY CASE WHEN j.gateway_execution_reason IN ('integration_changed','integration_removed') OR j.slack_progress_phase IN ('integration_changed','integration_removed') THEN 0 ELSE 1 END,
 j.slack_progress_last_sent_at,j.id

#elseif($Mode == "recover_status")
WHERE 1=1
#if($IntegrationID != 0)
AND j.integration_id=$IntegrationID
#end
 AND j.slack_progress_state='sending'
ORDER BY j.id
#elseif($Mode == "recover")
WHERE 1=1
#if($IntegrationID != 0)
AND j.integration_id=$IntegrationID
#end
 AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id AND integration.active=1)
 AND j.gateway_execution_state IN ('processing','querying')
ORDER BY j.id
#elseif($Mode == "unstarted")
WHERE j.integration_id=$IntegrationID AND j.gateway_execution_state IN ('pending','processing') AND j.backend_submission_started_at IS NULL
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id AND integration.active=1)
AND NOT EXISTS (SELECT 1 FROM conversation_binding b WHERE b.id=j.conversation_binding_id AND b.gateway_binding_state IN ('creating','needs_review'))
ORDER BY j.id
#elseif($Mode == "publication")
WHERE j.id=$ID
 AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id
  AND integration.slack_app_id=$AppID AND integration.slack_workspace_id=$WorkspaceID)
ORDER BY j.id
#elseif($Mode == "inactive_bot")
WHERE EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id
 AND integration.slack_app_id=$AppID AND integration.slack_workspace_id=$WorkspaceID AND integration.active=0)
ORDER BY j.id
#elseif(($Mode == "stale_thread") || ($Mode == "physical_thread"))
WHERE j.slack_channel_id=$ChannelID AND j.slack_thread_root_message_id=$SlackThreadRootMessageID
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id
 AND integration.slack_app_id=$AppID AND integration.slack_workspace_id=$WorkspaceID
 #if($Mode == "stale_thread")
 AND (integration.active=0 OR EXISTS (SELECT 1 FROM conversation_binding b WHERE b.id=j.conversation_binding_id AND b.gateway_binding_state='closed'))
 #end
)
ORDER BY j.id
#elseif(($Mode == "all") || ($Mode == "termination"))
WHERE 1=1
#if($IntegrationID != 0)
AND j.integration_id=$IntegrationID
#end
#if($IntegrationName != "")
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id AND integration.name=$IntegrationName)
#end
ORDER BY j.id
#elseif($Mode == "key")
WHERE 1=1
#if($IntegrationID != 0)
AND j.integration_id=$IntegrationID
#end
 AND ($ID <> 0 OR $AppID <> '' OR $WorkspaceID <> '' OR $GatewayRequestID <> '')
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=j.integration_id
 ${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")})
${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
ORDER BY j.id
#else
WHERE 1=0
#end
)  gateway_jobs