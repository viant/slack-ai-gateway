SELECT j.*,integration.slack_app_id,integration.slack_workspace_id,integration.name AS integration_name,integration.active AS integration_active,integration.backend_principal_id AS integration_principal_id
FROM gateway_job j JOIN integration integration ON integration.id=j.integration_id
