SELECT i.id AS integration_id,i.slack_app_id,i.slack_workspace_id,'identity' AS kind,
 NULL AS candidate_at,NULL AS refresh_at,NULL AS observed_at
FROM integration i WHERE i.active=1
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,'query',candidate.backend_next_poll_at,NULL,NULL
FROM integration i JOIN gateway_job candidate ON candidate.integration_id=i.id
WHERE i.active=1
${embed:job_claim_sql:sql/query_eligibility.sql}
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,'outbound',m.slack_delivery_next_attempt_at,NULL,NULL
FROM integration i JOIN integration owner ON owner.id=i.id OR (owner.active=0 AND owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id)
JOIN gateway_job j ON j.integration_id=owner.id JOIN slack_outgoing_message m ON m.job_id=j.id
WHERE i.active=1 AND m.slack_delivery_state='pending'
 AND (owner.id=i.id OR m.slack_message_kind='integration_notice')
 AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message earlier JOIN gateway_job earlier_job ON earlier_job.id=earlier.job_id
 WHERE earlier_job.integration_id=j.integration_id AND earlier_job.slack_channel_id=j.slack_channel_id
 AND earlier_job.slack_thread_root_message_id=j.slack_thread_root_message_id AND earlier.id<m.id
 AND (earlier.slack_delivery_state IN ('pending','sending') OR (earlier.slack_delivery_state IN ('failed','delivery_unknown')
 AND m.slack_message_kind='response' AND j.gateway_execution_reason<>'conversation_closed')))
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,'outbound',j.slack_progress_next_attempt_at,j.slack_progress_refresh_at,
 CASE WHEN j.gateway_execution_state IN ('processing','querying') THEN j.backend_observed_at ELSE NULL END
FROM (
 SELECT j.integration_id,j.gateway_execution_reason,j.slack_progress_phase,j.slack_progress_mode,j.slack_progress_message_id,j.backend_submission_started_at,j.gateway_execution_state,j.slack_progress_next_attempt_at,j.slack_progress_refresh_at,j.backend_observed_at
 FROM gateway_job j WHERE j.slack_progress_state='ready'
 UNION ALL
 SELECT j.integration_id,j.gateway_execution_reason,j.slack_progress_phase,j.slack_progress_mode,j.slack_progress_message_id,j.backend_submission_started_at,j.gateway_execution_state,j.slack_progress_next_attempt_at,j.slack_progress_refresh_at,j.backend_observed_at
 FROM gateway_job j WHERE j.slack_progress_state='settled' AND j.slack_progress_phase='delivery_uncertain'
 AND NOT EXISTS (SELECT 1 FROM slack_outgoing_message o WHERE o.job_id=j.id AND o.slack_delivery_state IN ('failed','delivery_unknown'))
) j
JOIN integration owner ON owner.id=j.integration_id
JOIN integration i ON owner.id=i.id OR (owner.active=0 AND owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id)
WHERE i.active=1
 AND (owner.id=i.id OR j.gateway_execution_reason IN ('integration_changed','integration_removed') OR j.slack_progress_phase IN ('integration_changed','integration_removed'))
 AND (j.slack_progress_mode IN ('message','hybrid') OR j.slack_progress_message_id<>''
 OR (j.slack_progress_mode='agent_session' AND j.backend_submission_started_at IS NOT NULL AND j.gateway_execution_state IN ('processing','querying')))
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,a.kind,a.candidate_at,a.refresh_at,NULL
FROM (
 SELECT a.initial_integration_id,'outbound' AS kind,a.slack_publication_next_attempt_at AS candidate_at,a.slack_publication_refresh_at AS refresh_at
 FROM slack_agent_session_publication a
 WHERE a.slack_publication_state NOT IN ('sending','failed','skipped')
 AND NOT (a.slack_publication_state='settled' AND a.slack_agent_desired_status='active' AND a.slack_publication_sent_revision>=a.slack_publication_revision)
 UNION ALL
 SELECT a.initial_integration_id,'cooldown',a.slack_publication_rate_limited_until,NULL
 FROM slack_agent_session_publication a WHERE a.slack_publication_rate_limited_until>$Now
) a
JOIN integration owner ON owner.id=a.initial_integration_id
JOIN integration i ON owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id
WHERE i.active=1
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,'cooldown',j.slack_progress_rate_limited_until,NULL,NULL
FROM integration i JOIN integration owner ON owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id
JOIN gateway_job j ON j.integration_id=owner.id
WHERE i.active=1 AND j.slack_progress_rate_limited_until>$Now
UNION ALL
SELECT i.id,i.slack_app_id,i.slack_workspace_id,'cooldown',m.slack_delivery_next_attempt_at,NULL,NULL
FROM integration i JOIN integration owner ON owner.slack_app_id=i.slack_app_id AND owner.slack_workspace_id=i.slack_workspace_id
JOIN gateway_job j ON j.integration_id=owner.id JOIN slack_outgoing_message m ON m.job_id=j.id
WHERE i.active=1 AND m.slack_delivery_next_attempt_at>$Now AND m.slack_delivery_state='pending'
