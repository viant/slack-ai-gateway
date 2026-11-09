SELECT * FROM (SELECT 1 AS snapshot_id, id, workspace_id, channel_id, root_thread_ts,
       COALESCE(conversation_id,'') AS conversation_id, access_mode,
       principal_id, owner_id, state
FROM conversation_bindings ORDER BY id DESC
) bindings