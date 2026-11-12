SELECT * FROM (SELECT 1 AS snapshot_id,a.initial_integration_id,integration.name AS integration_name,integration.active AS integration_active,
 s.slack_progress_mode,integration.slack_workspace_id,a.slack_channel_id,a.slack_thread_root_message_id,a.status_source_job_id,a.slack_initiator_user_id,
 a.slack_agent_desired_status,a.slack_agent_observed_status,a.slack_session_observed_status,a.slack_publication_state,a.slack_publication_revision,a.slack_publication_sent_revision,a.slack_publication_attempts,a.slack_publication_next_attempt_at,
 a.slack_publication_refresh_at,a.slack_publication_rate_limited_until,a.slack_publication_first_sent_at,a.slack_publication_last_sent_at,a.slack_publication_reason,a.slack_publication_warning,
 a.created_at,a.updated_at
FROM slack_agent_session_publication a JOIN gateway_job s ON s.id=a.status_source_job_id
JOIN integration integration ON integration.id=a.initial_integration_id
WHERE 1=1
#if($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration selected WHERE selected.id=$IntegrationID
 AND selected.slack_app_id=integration.slack_app_id AND selected.slack_workspace_id=integration.slack_workspace_id)
#end
#if($IntegrationName != "")
 AND EXISTS (SELECT 1 FROM integration selected WHERE selected.name=$IntegrationName
 AND selected.slack_app_id=integration.slack_app_id AND selected.slack_workspace_id=integration.slack_workspace_id)
#end
ORDER BY a.latest_status_job_id DESC
) slack_agent_session_publications