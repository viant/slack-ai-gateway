SELECT conversation_bindings.* FROM  (
 SELECT b.*,
 (SELECT integration.slack_workspace_id FROM integration integration WHERE integration.id=b.integration_id) AS slack_workspace_id,
 (SELECT integration.slack_app_id FROM integration integration WHERE integration.id=b.integration_id) AS slack_app_id,
 (SELECT integration.name FROM integration integration WHERE integration.id=b.integration_id) AS integration_name,
 (SELECT integration.active FROM integration integration WHERE integration.id=b.integration_id) AS integration_active
 FROM conversation_binding b
 WHERE 1=1
 #if(($IntegrationID != 0) && ($Mode != "physical_thread"))
 AND b.integration_id=$IntegrationID
 #end
 #if($Mode != "all")
 AND ($ID != 0 OR $WorkspaceID != '' OR $State != '')
 #end
 AND EXISTS (SELECT 1 FROM integration integration WHERE integration.id=b.integration_id
  ${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")})
 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
)  conversation_bindings