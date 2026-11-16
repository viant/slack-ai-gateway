#if($PhysicalThread)
 AND j.slack_channel_id=$ChannelID AND j.slack_thread_root_message_id=$SlackThreadRootMessageID
 AND EXISTS (SELECT 1 FROM integration thread_integration WHERE thread_integration.id=j.integration_id
  AND thread_integration.slack_app_id=$AppID AND thread_integration.slack_workspace_id=$WorkspaceID)
#end
