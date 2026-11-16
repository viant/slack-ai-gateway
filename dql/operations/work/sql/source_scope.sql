#if(($Mode == "page") || ($Mode == "cleanup_keys") || ($Mode == "backend_keys"))
 AND candidate.created_at <= $Cutoff
 AND (i.slack_app_id,i.slack_workspace_id,candidate.slack_channel_id,candidate.slack_thread_root_message_id)
  > ($AfterApp,$AfterWorkspace,$AfterChannel,$AfterRoot)
#end
#if($FixedThread)
 AND i.slack_app_id=$AppID AND i.slack_workspace_id=$WorkspaceID
 AND candidate.slack_channel_id=$ChannelID AND candidate.slack_thread_root_message_id=$SlackThreadRootMessageID
#end
