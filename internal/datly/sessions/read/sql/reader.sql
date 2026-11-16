SELECT * FROM  (SELECT a.*,
 (SELECT integration.slack_app_id FROM integration integration WHERE integration.id=a.initial_integration_id) AS slack_app_id,
 (SELECT integration.slack_workspace_id FROM integration integration WHERE integration.id=a.initial_integration_id) AS slack_workspace_id
FROM slack_agent_session_publication a
WHERE 1=1
#if($PhysicalThread)
AND a.slack_channel_id=$ChannelID AND a.slack_thread_root_message_id=$SlackThreadRootMessageID
AND EXISTS (SELECT 1 FROM integration thread_integration WHERE thread_integration.id=a.initial_integration_id
 AND thread_integration.slack_app_id=$AppID AND thread_integration.slack_workspace_id=$WorkspaceID)
#end
#if($IntegrationID != 0)
AND EXISTS (SELECT 1 FROM integration historical JOIN integration current_integration
 ON current_integration.slack_app_id=historical.slack_app_id AND current_integration.slack_workspace_id=historical.slack_workspace_id
 WHERE historical.id=a.initial_integration_id AND current_integration.id=$IntegrationID)
#end
#if($AppID != "")
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=a.initial_integration_id AND integration.slack_app_id=$AppID)
#end
#if($Mode == "due")
 AND a.slack_publication_state NOT IN ('sending','failed','skipped')
 AND (a.slack_publication_next_attempt_at IS NULL OR a.slack_publication_next_attempt_at <= $Now)
 AND (a.slack_publication_revision>a.slack_publication_sent_revision
  OR (a.slack_agent_desired_status='processing' AND a.slack_publication_refresh_at <= $Now))
#elseif($Mode == "recover")
 AND (a.slack_publication_state='sending' OR (a.slack_agent_desired_status='processing' AND a.slack_publication_state NOT IN ('failed','skipped')))
#elseif($Mode != "all")
 AND (SELECT integration.slack_workspace_id FROM integration integration WHERE integration.id=a.initial_integration_id)=$WorkspaceID
 AND a.slack_channel_id=$ChannelID AND a.slack_thread_root_message_id=$SlackThreadRootMessageID
#end
ORDER BY a.slack_publication_last_sent_at,a.latest_status_job_id
)  slack_agent_session_publications