SELECT * FROM (SELECT j.id,j.integration_id,integration.slack_app_id,integration.slack_workspace_id,integration.name AS integration_name,
 integration.active AS integration_active,integration.backend_principal_id AS integration_principal_id,
 j.slack_channel_id,j.slack_thread_root_message_id,j.slack_prompt_message_id,j.slack_conversation_type,j.created_at,j.gateway_execution_state,j.gateway_execution_reason,
 j.slack_progress_mode,j.backend_observed_state,COALESCE(b.gateway_binding_state,'') AS binding_state,
 (SELECT COUNT(*) FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_message_kind='response' AND o.slack_delivery_state IN ('pending','sending')) AS pending,
 (SELECT COUNT(*) FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_message_kind='response' AND o.slack_delivery_state IN ('failed','delivery_unknown')) AS bad
FROM (
 SELECT source.id FROM integration origin JOIN gateway_job source ON source.integration_id=origin.id
 LEFT JOIN conversation_binding binding ON binding.id=source.conversation_binding_id
 WHERE source.gateway_execution_state IN ('pending','processing','querying','needs_review')
  AND origin.active=1 AND COALESCE(binding.gateway_binding_state,'')<>'closed'
 AND source.gateway_execution_reason NOT IN ('conversation_closed','integration_changed','integration_removed')
 AND SUBSTR(source.gateway_execution_reason,1,20)<>'conversation_closed:'
#if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end


 UNION
 SELECT source.id FROM conversation_binding binding JOIN gateway_job source ON source.conversation_binding_id=binding.id
 JOIN integration origin ON origin.id=source.integration_id
 WHERE binding.gateway_binding_state='needs_review' AND source.gateway_execution_state IN ('completed','rejected')
  AND origin.active=1 AND COALESCE(binding.gateway_binding_state,'')<>'closed'
 AND source.gateway_execution_reason NOT IN ('conversation_closed','integration_changed','integration_removed')
 AND SUBSTR(source.gateway_execution_reason,1,20)<>'conversation_closed:'
#if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end


 UNION
 SELECT source.id FROM slack_outgoing_message live_part JOIN gateway_job source ON source.id=live_part.job_id
 JOIN integration origin ON origin.id=source.integration_id
 LEFT JOIN conversation_binding binding ON binding.id=source.conversation_binding_id
 WHERE live_part.slack_message_kind='response' AND live_part.slack_delivery_state IN ('pending','sending','failed','delivery_unknown')
  AND origin.active=1 AND COALESCE(binding.gateway_binding_state,'')<>'closed'
 AND source.gateway_execution_reason NOT IN ('conversation_closed','integration_changed','integration_removed')
 AND SUBSTR(source.gateway_execution_reason,1,20)<>'conversation_closed:'
#if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end


 UNION
 SELECT source.status_source_job_id AS id FROM slack_agent_session_publication source JOIN integration origin ON origin.id=source.initial_integration_id
 WHERE 1=1
 #if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end

 UNION
 SELECT source.latest_status_job_id AS id FROM slack_agent_session_publication source JOIN integration origin ON origin.id=source.initial_integration_id
 WHERE 1=1
 #if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end

) relevant
JOIN gateway_job j ON j.id=relevant.id
JOIN integration integration ON integration.id=j.integration_id
LEFT JOIN conversation_binding b ON b.id=j.conversation_binding_id
ORDER BY j.id
) gateway_jobs