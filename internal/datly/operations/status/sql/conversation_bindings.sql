SELECT * FROM (SELECT 1 AS snapshot_id,b.id,b.integration_id,integration.name AS integration_name,integration.active AS integration_active,
 integration.slack_workspace_id,b.slack_channel_id,b.slack_thread_root_message_id,COALESCE(b.backend_conversation_id,'') AS backend_conversation_id,
 b.gateway_access_mode,b.backend_principal_id,b.gateway_binding_owner_id,b.gateway_binding_state,
 b.created_at,b.updated_at,b.last_activity_at,b.closed_at
FROM conversation_binding b JOIN integration integration ON integration.id=b.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
ORDER BY b.id DESC
) conversation_bindings