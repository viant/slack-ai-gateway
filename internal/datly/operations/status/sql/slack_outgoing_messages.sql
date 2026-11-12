SELECT * FROM (SELECT 1 AS snapshot_id,m.id,m.job_id,j.integration_id,integration.name AS integration_name,integration.active AS integration_active,
 integration.slack_workspace_id,j.slack_channel_id,j.slack_thread_root_message_id,m.slack_message_index,m.slack_message_kind,m.slack_delivery_state,m.slack_message_id,m.slack_delivery_attempts,m.slack_delivery_next_attempt_at,
 m.created_at,m.updated_at
FROM slack_outgoing_message m JOIN gateway_job j ON j.id=m.job_id JOIN integration integration ON integration.id=j.integration_id
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
ORDER BY m.id DESC
) slack_outgoing_messages