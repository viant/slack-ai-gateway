SELECT identity_check.* FROM  (SELECT
    (SELECT COUNT(*) FROM inbox_jobs
     WHERE app_id <> $AppID OR workspace_id <> $WorkspaceID
       OR ($FixedChannel AND channel_id <> $ChannelID)
       OR (access_mode <> '' AND (access_mode <> 'shared_channel'
           OR principal_id <> $Principal OR owner_id <> $Principal))) AS incompatible_jobs,
    (SELECT COUNT(*) FROM conversation_bindings
     WHERE (1=0 ${predicate.Builder().CombineOr($predicate.FilterGroup(1,"AND")).Build("OR")})
       OR access_mode <> 'shared_channel' OR workspace_id <> $WorkspaceID
       OR ($FixedChannel AND channel_id <> $ChannelID)
       OR principal_id <> $Principal OR owner_id <> $Principal) AS incompatible_bindings
)  identity_check