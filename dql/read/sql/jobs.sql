SELECT j.*,
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
   AND (candidate.backend_next_poll_at IS NULL OR candidate.backend_next_poll_at <= $Now) ${embed:query_eligibility.sql}
  ORDER BY candidate.id LIMIT 1
 ) eligible
)
ORDER BY j.id LIMIT 1
#elseif(($Mode == "status") || ($Mode == "backend_status"))
${embed:status.sql}
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
