SELECT identity_check.* FROM  (SELECT
 (SELECT COUNT(*) FROM gateway_job j JOIN integration integration ON integration.id=j.integration_id
 LEFT JOIN conversation_binding binding ON binding.id=j.conversation_binding_id
 WHERE 1=1
#if($IntegrationID != 0)
 AND j.integration_id=$IntegrationID
#end
 AND (integration.slack_app_id<>$AppID OR integration.slack_workspace_id<>$WorkspaceID
 OR ($FixedChannel AND j.slack_channel_id<>$ChannelID)
 OR (j.conversation_binding_id IS NOT NULL AND (binding.id IS NULL OR binding.integration_id<>j.integration_id
 OR binding.slack_channel_id<>j.slack_channel_id OR binding.slack_thread_root_message_id<>j.slack_thread_root_message_id
 OR binding.gateway_access_mode<>'shared_channel' OR binding.backend_principal_id<>$Principal OR binding.gateway_binding_owner_id<>$Principal)))) AS incompatible_jobs,
 (SELECT COUNT(*) FROM conversation_binding JOIN integration integration ON integration.id=conversation_binding.integration_id
 WHERE 1=1
#if($IntegrationID != 0)
 AND conversation_binding.integration_id=$IntegrationID
#end
 AND ((1=0 ${predicate.Builder().CombineOr($predicate.FilterGroup(1,"AND")).Build("OR")})
 OR gateway_access_mode<>'shared_channel' OR integration.slack_workspace_id<>$WorkspaceID OR integration.slack_app_id<>$AppID
 OR ($FixedChannel AND slack_channel_id<>$ChannelID) OR conversation_binding.backend_principal_id<>$Principal OR gateway_binding_owner_id<>$Principal)) AS incompatible_bindings
)  identity_check