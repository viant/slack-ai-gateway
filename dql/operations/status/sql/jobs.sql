SELECT 1 AS snapshot_id,j.id,j.integration_id,integration.name AS integration_name,integration.active AS integration_active,
 integration.slack_workspace_id,j.slack_channel_id,j.slack_thread_root_message_id,j.slack_prompt_author_id,COALESCE(j.conversation_binding_id,0) AS conversation_binding_id,
 j.gateway_request_id,j.gateway_execution_state,j.gateway_execution_reason,j.gateway_execution_outcome,
 j.backend_poll_attempts,j.backend_submission_started_at,j.backend_next_poll_at,
 j.backend_observed_state,j.backend_observed_at,j.backend_observation_uncertain,
 j.slack_progress_mode,j.slack_progress_message_id,j.slack_progress_phase,j.slack_progress_state,
 j.slack_progress_revision,j.slack_progress_sent_revision,j.slack_progress_attempts,j.slack_progress_next_attempt_at,
 j.slack_progress_refresh_at,j.slack_progress_rate_limited_until,j.slack_progress_first_sent_at,j.slack_progress_last_sent_at,
 j.slack_progress_reason,j.created_at,
 j.gateway_execution_updated_at,j.gateway_status_updated_at,j.gateway_execution_finished_at,j.content_purged_at
FROM gateway_job j JOIN integration integration ON integration.id=j.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
ORDER BY j.id DESC
