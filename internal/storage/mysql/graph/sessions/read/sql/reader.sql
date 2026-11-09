SELECT * FROM  (SELECT a.* FROM agent_session_publications a
#if($Mode == "due")
WHERE a.state NOT IN ('sending','failed','skipped')
#elseif($Mode == "recover")
WHERE a.state='sending'
#else
WHERE a.workspace_id=$WorkspaceID AND a.channel_id=$ChannelID AND a.root_thread_ts=$RootThreadTS
#end
ORDER BY a.last_sent_at,a.latest_job_id
)  sessions