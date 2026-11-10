SELECT bindings.* FROM  (
 SELECT b.* FROM conversation_bindings b
 WHERE ($ID != 0 OR $WorkspaceID != '' OR $State != '')
 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("AND")}
)  bindings