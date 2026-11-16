#if($PhysicalThread)
 AND origin.slack_app_id=$AppID AND origin.slack_workspace_id=$WorkspaceID
 AND source.slack_channel_id=$ChannelID AND source.slack_thread_root_message_id=$SlackThreadRootMessageID
#elseif($IntegrationID != 0)
 AND EXISTS (SELECT 1 FROM integration current_integration WHERE current_integration.id=$IntegrationID
  AND current_integration.slack_app_id=origin.slack_app_id AND current_integration.slack_workspace_id=origin.slack_workspace_id)
#end
