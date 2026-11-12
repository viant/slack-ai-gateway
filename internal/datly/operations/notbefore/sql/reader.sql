SELECT cooldown.* FROM  (SELECT MAX(candidates.not_before) AS not_before
FROM (
 SELECT MAX(o.slack_delivery_next_attempt_at) AS not_before
 FROM slack_outgoing_message o JOIN gateway_job j ON j.id=o.job_id JOIN integration integration ON integration.id=j.integration_id
 WHERE integration.slack_workspace_id=$WorkspaceID AND o.slack_delivery_state='pending'
#if($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
 AND current_integration.slack_app_id=integration.slack_app_id AND current_integration.slack_workspace_id=integration.slack_workspace_id)
#end
#if($AppID != "")
 AND integration.slack_app_id=$AppID
#end
 UNION ALL
 SELECT MAX(s.slack_progress_rate_limited_until) AS not_before
 FROM gateway_job s JOIN integration integration ON integration.id=s.integration_id
 WHERE integration.slack_workspace_id=$WorkspaceID
#if($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
 AND current_integration.slack_app_id=integration.slack_app_id AND current_integration.slack_workspace_id=integration.slack_workspace_id)
#end
#if($AppID != "")
 AND integration.slack_app_id=$AppID
#end
 UNION ALL
 SELECT MAX(a.slack_publication_rate_limited_until) AS not_before
 FROM slack_agent_session_publication a JOIN integration integration ON integration.id=a.initial_integration_id
 WHERE integration.slack_workspace_id=$WorkspaceID
#if($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
 AND current_integration.slack_app_id=integration.slack_app_id AND current_integration.slack_workspace_id=integration.slack_workspace_id)
#end
#if($AppID != "")
 AND integration.slack_app_id=$AppID
#end
) candidates
)  cooldown