SELECT * FROM (SELECT 1 AS snapshot_id,'gateway_job.gateway_execution_state' AS table_name,j.gateway_execution_state AS state,COUNT(*) AS row_count FROM gateway_job j JOIN integration integration ON integration.id=j.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
GROUP BY j.gateway_execution_state
UNION ALL
SELECT 1 AS snapshot_id,'conversation_binding.gateway_binding_state' AS table_name,b.gateway_binding_state AS state,COUNT(*) AS row_count FROM conversation_binding b JOIN integration integration ON integration.id=b.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
GROUP BY b.gateway_binding_state
UNION ALL
SELECT 1 AS snapshot_id,'slack_outgoing_message.slack_delivery_state' AS table_name,m.slack_delivery_state AS state,COUNT(*) AS row_count FROM slack_outgoing_message m JOIN gateway_job j ON j.id=m.job_id JOIN integration integration ON integration.id=j.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
GROUP BY m.slack_delivery_state
UNION ALL
SELECT 1 AS snapshot_id,'gateway_job.slack_progress_state' AS table_name,s.slack_progress_state AS state,COUNT(*) AS row_count FROM gateway_job s JOIN integration integration ON integration.id=s.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
GROUP BY s.slack_progress_state
UNION ALL
SELECT 1 AS snapshot_id,'slack_agent_session_publication.slack_publication_state' AS table_name,a.slack_publication_state AS state,COUNT(*) AS row_count FROM slack_agent_session_publication a JOIN integration integration ON integration.id=a.initial_integration_id
WHERE 1=1
#if($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration selected WHERE selected.id=$IntegrationID
 AND selected.slack_app_id=integration.slack_app_id AND selected.slack_workspace_id=integration.slack_workspace_id)
#end
#if($IntegrationName != "")
 AND EXISTS (SELECT 1 FROM integration selected WHERE selected.name=$IntegrationName
 AND selected.slack_app_id=integration.slack_app_id AND selected.slack_workspace_id=integration.slack_workspace_id)
#end
GROUP BY a.slack_publication_state
) counts