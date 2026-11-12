SELECT integrations.* FROM  (
 SELECT i.id,i.name,i.active,i.slack_app_id,i.slack_workspace_id,i.backend_type,i.backend_endpoint,i.backend_agent_id,i.backend_principal_id,i.created_at,i.updated_at
 FROM integration i
 WHERE 1=1 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
 ORDER BY i.id
)  integrations