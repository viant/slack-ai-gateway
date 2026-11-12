SELECT * FROM  (SELECT m.*, owner_job.integration_id,owner_job.gateway_execution_reason AS job_reason,
 (SELECT integration.slack_app_id FROM integration integration WHERE integration.id=owner_job.integration_id) AS slack_app_id,
 (SELECT integration.slack_workspace_id FROM integration integration WHERE integration.id=owner_job.integration_id) AS slack_workspace_id,
 (SELECT integration.name FROM integration integration WHERE integration.id=owner_job.integration_id) AS integration_name,
 (SELECT integration.active FROM integration integration WHERE integration.id=owner_job.integration_id) AS integration_active,
 (SELECT integration.backend_principal_id FROM integration integration WHERE integration.id=owner_job.integration_id) AS integration_principal_id,
 owner_job.slack_channel_id,owner_job.slack_thread_root_message_id
FROM slack_outgoing_message m JOIN gateway_job owner_job ON owner_job.id=m.job_id
WHERE 1=1
#if($IntegrationID != 0)
AND (owner_job.integration_id=$IntegrationID OR (m.slack_message_kind='integration_notice'
 AND EXISTS (SELECT 1 FROM integration historical JOIN integration current_integration
 ON current_integration.slack_app_id=historical.slack_app_id AND current_integration.slack_workspace_id=historical.slack_workspace_id
 WHERE historical.id=owner_job.integration_id AND historical.active=0 AND current_integration.id=$IntegrationID AND current_integration.active=1)))
#end
#if($IntegrationName != "")
AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=owner_job.integration_id AND integration.name=$IntegrationName)
#end
#if($Mode == "claim")
 AND m.slack_delivery_state='pending' AND (m.slack_delivery_next_attempt_at IS NULL OR m.slack_delivery_next_attempt_at <= $Now)
 AND (m.slack_message_kind='integration_notice' OR EXISTS (SELECT 1 FROM integration integration WHERE integration.id=owner_job.integration_id AND integration.active=1))
AND NOT EXISTS (SELECT 1 FROM (SELECT job_id FROM slack_outgoing_message WHERE slack_delivery_state='pending' AND slack_delivery_next_attempt_at>$Now GROUP BY job_id) cd JOIN gateway_job cdj ON cdj.id=cd.job_id
 JOIN integration ci ON ci.id=cdj.integration_id JOIN integration oi ON oi.id=owner_job.integration_id
 WHERE ci.slack_app_id=oi.slack_app_id AND ci.slack_workspace_id=oi.slack_workspace_id)
AND NOT EXISTS (SELECT 1 FROM gateway_job j
 JOIN integration ri ON ri.id=j.integration_id JOIN integration oi ON oi.id=owner_job.integration_id
 WHERE ri.slack_app_id=oi.slack_app_id AND ri.slack_workspace_id=oi.slack_workspace_id AND j.slack_progress_rate_limited_until>$Now)
AND NOT EXISTS (SELECT 1 FROM slack_agent_session_publication a JOIN integration ai ON ai.id=a.initial_integration_id
 JOIN integration oi ON oi.id=owner_job.integration_id WHERE ai.slack_app_id=oi.slack_app_id AND ai.slack_workspace_id=oi.slack_workspace_id AND a.slack_publication_rate_limited_until>$Now)
AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message earlier JOIN gateway_job earlier_job ON earlier_job.id=earlier.job_id
 WHERE earlier_job.integration_id=owner_job.integration_id AND earlier_job.slack_channel_id=owner_job.slack_channel_id
 AND earlier_job.slack_thread_root_message_id=owner_job.slack_thread_root_message_id AND earlier.id<m.id
 AND (earlier.slack_delivery_state IN ('pending','sending') OR (earlier.slack_delivery_state IN ('failed','delivery_unknown')
 AND m.slack_message_kind='response' AND owner_job.gateway_execution_reason<>'conversation_closed')))
ORDER BY m.id LIMIT 1
#elseif($Mode == "recover")
 AND m.slack_delivery_state='sending'
ORDER BY m.id
#elseif(($Mode == "all") || ($Mode == "termination"))
ORDER BY m.id
#else
 AND m.id=$ID
#end
)  slack_outgoing_messages