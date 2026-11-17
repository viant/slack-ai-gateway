 AND origin.active=1 AND COALESCE(binding.gateway_binding_state,'')<>'closed'
 AND source.gateway_execution_reason NOT IN ('conversation_closed','integration_changed','integration_removed')
 AND SUBSTR(source.gateway_execution_reason,1,20)<>'conversation_closed:'
${embed:projection_scope.sql}
