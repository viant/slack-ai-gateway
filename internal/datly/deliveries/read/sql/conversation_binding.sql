SELECT * FROM (SELECT b.*,integration.slack_workspace_id FROM conversation_binding b JOIN integration integration ON integration.id=b.integration_id
) conversation_binding