-- Additive scheduling indexes; migration 1 and all business rows stay intact.
CREATE INDEX gateway_job_state ON gateway_job(gateway_execution_state,integration_id,id);
CREATE INDEX slack_agent_session_publication_cooldown
    ON slack_agent_session_publication(slack_publication_rate_limited_until,initial_integration_id,id);
