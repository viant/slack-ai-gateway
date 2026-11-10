SELECT 1 AS snapshot_id, 'inbox_jobs' AS table_name, state, COUNT(*) AS row_count FROM inbox_jobs GROUP BY state
UNION ALL
SELECT 1 AS snapshot_id, 'conversation_bindings' AS table_name, state, COUNT(*) AS row_count FROM conversation_bindings GROUP BY state
UNION ALL
SELECT 1 AS snapshot_id, 'outbox_messages' AS table_name, state, COUNT(*) AS row_count FROM outbox_messages GROUP BY state
UNION ALL
SELECT 1 AS snapshot_id, 'request_status' AS table_name, state, COUNT(*) AS row_count FROM request_status GROUP BY state
UNION ALL
SELECT 1 AS snapshot_id, 'agent_session_publications' AS table_name, state, COUNT(*) AS row_count FROM agent_session_publications GROUP BY state
