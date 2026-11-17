#if($Mode == "stale_thread")
SELECT evidence.integration_id,evidence.slack_app_id,evidence.slack_workspace_id,evidence.slack_channel_id,evidence.slack_thread_root_message_id,
 'stale' AS kind,NULL AS candidate_at,NULL AS refresh_at,NULL AS observed_at
FROM (
 SELECT inactive_job.* FROM (
  SELECT historical.id AS integration_id,historical.slack_app_id,historical.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id
  FROM integration historical JOIN gateway_job candidate ON candidate.integration_id=historical.id
  WHERE historical.slack_app_id=$AppID AND historical.slack_workspace_id=$WorkspaceID AND historical.active=0
   AND candidate.slack_channel_id=$ChannelID AND candidate.slack_thread_root_message_id=$SlackThreadRootMessageID
  ORDER BY candidate.id LIMIT 1
 ) inactive_job
 UNION ALL
 SELECT closed_binding.* FROM (
  SELECT historical.id AS integration_id,historical.slack_app_id,historical.slack_workspace_id,binding.slack_channel_id,binding.slack_thread_root_message_id
  FROM integration historical JOIN conversation_binding binding ON binding.integration_id=historical.id
  WHERE historical.slack_app_id=$AppID AND historical.slack_workspace_id=$WorkspaceID AND binding.gateway_binding_state='closed'
   AND binding.slack_channel_id=$ChannelID AND binding.slack_thread_root_message_id=$SlackThreadRootMessageID
  ORDER BY binding.id LIMIT 1
 ) closed_binding
 UNION ALL
 SELECT closed_job.* FROM (
  SELECT historical.id AS integration_id,historical.slack_app_id,historical.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id
  FROM conversation_binding binding JOIN gateway_job candidate ON candidate.conversation_binding_id=binding.id
  JOIN integration historical ON historical.id=candidate.integration_id
  WHERE historical.slack_app_id=$AppID AND historical.slack_workspace_id=$WorkspaceID AND binding.gateway_binding_state='closed'
   AND candidate.slack_channel_id=$ChannelID AND candidate.slack_thread_root_message_id=$SlackThreadRootMessageID
  ORDER BY candidate.id LIMIT 1
 ) closed_job
 UNION ALL
 SELECT unbound_closed_job.* FROM (
  SELECT historical.id AS integration_id,historical.slack_app_id,historical.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id
  FROM integration historical JOIN gateway_job candidate ON candidate.integration_id=historical.id
  WHERE historical.slack_app_id=$AppID AND historical.slack_workspace_id=$WorkspaceID
   AND candidate.slack_channel_id=$ChannelID AND candidate.slack_thread_root_message_id=$SlackThreadRootMessageID
   AND candidate.gateway_execution_state IN ('completed','rejected')
   AND (candidate.gateway_execution_reason='conversation_closed' OR SUBSTR(candidate.gateway_execution_reason,1,20)='conversation_closed:')
   AND NOT EXISTS (SELECT 1 FROM conversation_binding binding JOIN integration binding_integration ON binding_integration.id=binding.integration_id
    WHERE binding_integration.slack_app_id=$AppID AND binding_integration.slack_workspace_id=$WorkspaceID
     AND binding.slack_channel_id=$ChannelID AND binding.slack_thread_root_message_id=$SlackThreadRootMessageID)
  ORDER BY candidate.id LIMIT 1
 ) unbound_closed_job
) evidence LIMIT 1
#elseif(($Mode == "cleanup_keys") || ($Mode == "backend_keys"))
SELECT i.id AS integration_id,i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id,
 CASE WHEN $Mode='cleanup_keys' THEN 'cleanup' ELSE 'backend' END AS kind,NULL AS candidate_at,NULL AS refresh_at,NULL AS observed_at
FROM integration i JOIN gateway_job candidate ON candidate.integration_id=i.id
WHERE i.active=1 AND i.id=$IntegrationID
#if($Mode == "cleanup_keys")
 AND candidate.backend_submission_started_at IS NULL AND candidate.gateway_execution_state IN ('pending','processing')
#else
 AND candidate.backend_submission_started_at IS NOT NULL AND candidate.gateway_execution_state IN ('processing','querying')
#end
${embed:source_scope.sql}
GROUP BY i.id,i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id
ORDER BY i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id LIMIT 256
#else
WITH bot_deadlines AS (
 SELECT i.slack_app_id,i.slack_workspace_id,MAX(j.slack_progress_rate_limited_until) AS next_at
 FROM integration i JOIN gateway_job j ON j.integration_id=i.id
 WHERE j.slack_progress_rate_limited_until>$Now
 GROUP BY i.slack_app_id,i.slack_workspace_id
 UNION ALL
 SELECT i.slack_app_id,i.slack_workspace_id,MAX(m.slack_delivery_next_attempt_at)
 FROM integration i JOIN gateway_job j ON j.integration_id=i.id JOIN slack_outgoing_message m ON m.job_id=j.id
 WHERE m.slack_delivery_state='pending' AND m.slack_delivery_next_attempt_at>$Now
 GROUP BY i.slack_app_id,i.slack_workspace_id
 UNION ALL
 SELECT i.slack_app_id,i.slack_workspace_id,MAX(a.slack_publication_rate_limited_until)
 FROM integration i JOIN slack_agent_session_publication a ON a.initial_integration_id=i.id
 WHERE a.slack_publication_rate_limited_until>$Now
 GROUP BY i.slack_app_id,i.slack_workspace_id
), bot_cooldown AS (
 SELECT slack_app_id,slack_workspace_id,MAX(next_at) AS next_at
 FROM bot_deadlines GROUP BY slack_app_id,slack_workspace_id
), source_work AS (
 SELECT i.id AS integration_id,i.slack_app_id,i.slack_workspace_id,
  candidate.slack_channel_id,candidate.slack_thread_root_message_id,'query' AS kind,
  candidate.backend_next_poll_at AS candidate_at,NULL AS refresh_at,NULL AS observed_at
 FROM integration i JOIN gateway_job candidate ON candidate.integration_id=i.id
 WHERE i.active=1
 ${embed:source_scope.sql}
 ${embed:job_claim_sql:sql/query_eligibility.sql}
 UNION ALL
 SELECT i.id,i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id,
  'outbound',m.slack_delivery_next_attempt_at,NULL,NULL
 FROM integration i JOIN integration owner ON owner.id=i.id OR (owner.active=0 AND owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id)
 JOIN gateway_job candidate ON candidate.integration_id=owner.id JOIN slack_outgoing_message m ON m.job_id=candidate.id
 WHERE i.active=1 AND m.slack_delivery_state='pending'
  AND (owner.id=i.id OR m.slack_message_kind='integration_notice')
 ${embed:source_scope.sql}
 AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message earlier JOIN gateway_job earlier_job ON earlier_job.id=earlier.job_id
  WHERE earlier_job.integration_id=candidate.integration_id AND earlier_job.slack_channel_id=candidate.slack_channel_id
  AND earlier_job.slack_thread_root_message_id=candidate.slack_thread_root_message_id AND earlier.id<m.id
  AND (earlier.slack_delivery_state IN ('pending','sending') OR (earlier.slack_delivery_state IN ('failed','delivery_unknown')
  AND m.slack_message_kind='response' AND candidate.gateway_execution_reason<>'conversation_closed'
  AND SUBSTR(candidate.gateway_execution_reason,1,20)<>'conversation_closed:')))
 UNION ALL
 SELECT i.id,i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id,
  'outbound',CASE
   WHEN candidate.slack_progress_message_id='' OR candidate.slack_progress_revision>candidate.slack_progress_sent_revision
    OR candidate.slack_progress_refresh_at IS NULL THEN candidate.slack_progress_next_attempt_at
   WHEN candidate.slack_progress_next_attempt_at>candidate.slack_progress_refresh_at THEN candidate.slack_progress_next_attempt_at
   ELSE candidate.slack_progress_refresh_at END,candidate.slack_progress_refresh_at,
  CASE WHEN candidate.gateway_execution_state IN ('processing','querying') THEN candidate.backend_observed_at ELSE NULL END
 FROM integration i JOIN integration owner ON owner.id=i.id OR (owner.active=0 AND owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id)
 JOIN gateway_job candidate ON candidate.integration_id=owner.id
 WHERE i.active=1 AND candidate.slack_progress_state='ready'
  AND (owner.id=i.id OR candidate.gateway_execution_reason IN ('integration_changed','integration_removed') OR candidate.slack_progress_phase IN ('integration_changed','integration_removed'))
  AND (candidate.slack_progress_mode IN ('message','hybrid') OR candidate.slack_progress_message_id<>'')
 ${embed:source_scope.sql}
 UNION ALL
 SELECT i.id,i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id,
  'outbound',CASE
   WHEN candidate.slack_publication_revision>candidate.slack_publication_sent_revision THEN candidate.slack_publication_next_attempt_at
   WHEN candidate.slack_publication_next_attempt_at>candidate.slack_publication_refresh_at THEN candidate.slack_publication_next_attempt_at
   ELSE candidate.slack_publication_refresh_at END,candidate.slack_publication_refresh_at,NULL
 FROM integration i JOIN integration owner ON owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id
 JOIN slack_agent_session_publication candidate ON candidate.initial_integration_id=owner.id
 WHERE i.active=1 AND candidate.slack_publication_state NOT IN ('sending','failed','skipped')
  AND (candidate.slack_publication_revision>candidate.slack_publication_sent_revision
   OR (candidate.slack_agent_desired_status='processing' AND candidate.slack_publication_refresh_at IS NOT NULL))
 ${embed:source_scope.sql}
), gated_work_base AS (
 SELECT w.integration_id,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.kind,
  CASE WHEN w.kind='outbound' AND c.next_at IS NOT NULL AND (w.candidate_at IS NULL OR c.next_at>w.candidate_at)
   THEN c.next_at ELSE w.candidate_at END AS candidate_at,w.refresh_at,w.observed_at
 FROM source_work w LEFT JOIN bot_cooldown c ON c.slack_app_id=w.slack_app_id AND c.slack_workspace_id=w.slack_workspace_id
), gated_work AS (
 SELECT w.integration_id,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.kind,w.candidate_at,
  CASE WHEN w.refresh_at IS NULL THEN NULL WHEN w.candidate_at>w.refresh_at THEN w.candidate_at ELSE w.refresh_at END AS refresh_at,w.observed_at
 FROM gated_work_base w
), grouped_work AS (
 SELECT integration_id,slack_app_id,slack_workspace_id,slack_channel_id,slack_thread_root_message_id,kind,
  CASE WHEN COUNT(candidate_at)<COUNT(*) THEN NULL ELSE MIN(candidate_at) END AS candidate_at,MIN(refresh_at) AS refresh_at,MIN(observed_at) AS observed_at
 FROM gated_work
 GROUP BY integration_id,slack_app_id,slack_workspace_id,slack_channel_id,slack_thread_root_message_id,kind
)
#if($Mode == "deadline")
SELECT d.integration_id,d.slack_app_id,d.slack_workspace_id,d.slack_channel_id,d.slack_thread_root_message_id,d.kind,
 d.candidate_at,d.refresh_at,d.observed_at
FROM (
 SELECT w.*,ROW_NUMBER() OVER (PARTITION BY w.kind ORDER BY w.candidate_at,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.integration_id) AS position_in_kind
 FROM (
  SELECT w.* FROM grouped_work w WHERE w.candidate_at>$Now
  UNION ALL
  SELECT w.integration_id,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.kind,
   w.refresh_at AS candidate_at,w.refresh_at,w.observed_at
  FROM grouped_work w WHERE w.refresh_at>$Now
 ) w
) d WHERE d.position_in_kind=1
ORDER BY d.kind
#elseif($Mode == "target")
SELECT w.integration_id,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.kind,
 w.candidate_at,w.refresh_at,w.observed_at
FROM grouped_work w
ORDER BY w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.integration_id,w.kind
#else
SELECT w.integration_id,w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.kind,
 w.candidate_at,w.refresh_at,w.observed_at
FROM grouped_work w JOIN (
 SELECT slack_app_id,slack_workspace_id,slack_channel_id,slack_thread_root_message_id
 FROM grouped_work WHERE candidate_at IS NULL OR candidate_at<=$Now
 GROUP BY slack_app_id,slack_workspace_id,slack_channel_id,slack_thread_root_message_id
 ORDER BY slack_app_id,slack_workspace_id,slack_channel_id,slack_thread_root_message_id LIMIT 256
) k ON k.slack_app_id=w.slack_app_id AND k.slack_workspace_id=w.slack_workspace_id
 AND k.slack_channel_id=w.slack_channel_id AND k.slack_thread_root_message_id=w.slack_thread_root_message_id
WHERE w.candidate_at IS NULL OR w.candidate_at<=$Now
ORDER BY w.slack_app_id,w.slack_workspace_id,w.slack_channel_id,w.slack_thread_root_message_id,w.integration_id,w.kind
#end
#end
