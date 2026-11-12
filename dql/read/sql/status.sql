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
