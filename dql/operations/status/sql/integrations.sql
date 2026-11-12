SELECT 1 AS snapshot_id,integration.* FROM integration integration
WHERE 1=1
#if($IntegrationID != 0)
AND integration.id=$IntegrationID
#end
#if($IntegrationName != "")
AND integration.name=$IntegrationName
#end
ORDER BY integration.id DESC
