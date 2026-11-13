AND (
   (candidate.gateway_execution_state='querying' AND candidate.backend_poll_ready=1)
   OR (candidate.gateway_execution_state='pending'
    AND NOT EXISTS (SELECT 1 FROM gateway_job active_job WHERE active_job.integration_id=candidate.integration_id
     AND active_job.slack_channel_id=candidate.slack_channel_id AND active_job.slack_thread_root_message_id=candidate.slack_thread_root_message_id
     AND active_job.id<>candidate.id AND active_job.gateway_execution_state IN ('processing','querying','needs_review'))
    AND NOT EXISTS (SELECT 1 FROM gateway_job earlier WHERE earlier.integration_id=candidate.integration_id
     AND earlier.slack_channel_id=candidate.slack_channel_id AND earlier.slack_thread_root_message_id=candidate.slack_thread_root_message_id
     AND earlier.id<candidate.id AND earlier.gateway_execution_state NOT IN ('completed','rejected'))
   ))
