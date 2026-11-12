SELECT j.id,j.integration_id,integration.slack_app_id,integration.slack_workspace_id,integration.name AS integration_name,
 integration.active AS integration_active,integration.backend_principal_id AS integration_principal_id,
 j.slack_channel_id,j.slack_thread_root_message_id,j.slack_prompt_message_id,j.slack_conversation_type,j.created_at,j.gateway_execution_state,j.gateway_execution_reason,
 j.slack_progress_mode,j.backend_observed_state,COALESCE(b.gateway_binding_state,'') AS binding_state,
 (SELECT COUNT(*) FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_message_kind='response' AND o.slack_delivery_state IN ('pending','sending')) AS pending,
 (SELECT COUNT(*) FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_message_kind='response' AND o.slack_delivery_state IN ('failed','delivery_unknown')) AS bad
FROM gateway_job j JOIN integration integration ON integration.id=j.integration_id
LEFT JOIN conversation_binding b ON b.id=j.conversation_binding_id
#if($IntegrationID != 0)
WHERE EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
 AND current_integration.slack_app_id=integration.slack_app_id AND current_integration.slack_workspace_id=integration.slack_workspace_id)
#end
ORDER BY j.id
