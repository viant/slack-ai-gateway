#!/usr/bin/env bash
# Explicit initialization of this project's local MySQL fixture with a fresh schema.
set -euo pipefail

: "${SLACK_AI_GATEWAY_MYSQL_PASSWORD:?Run endly -t=init with the documented MySQL credentials}"
mysql_user=${SLACK_AI_GATEWAY_MYSQL_USER:-root}
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
container=mysql_slack_ai_gateway
volume=slack_ai_gateway_mysql
owner_label=com.viant.slack-ai-gateway.e2e

docker info >/dev/null
if docker volume inspect "$volume" >/dev/null 2>&1; then
  owner=$(docker volume inspect --format '{{index .Labels "com.viant.slack-ai-gateway.e2e"}}' "$volume")
  [ "$owner" = mysql ] || { echo "Refusing to reuse an unowned MySQL volume" >&2; exit 1; }
else
  docker volume create --label "$owner_label=mysql" "$volume" >/dev/null
fi

if docker container inspect "$container" >/dev/null 2>&1; then
  owner=$(docker inspect --format '{{index .Config.Labels "com.viant.slack-ai-gateway.e2e"}}' "$container")
  [ "$owner" = mysql ] || { echo "Refusing to reuse an unowned MySQL container" >&2; exit 1; }
  docker start "$container" >/dev/null
else
  MYSQL_ROOT_PASSWORD="$SLACK_AI_GATEWAY_MYSQL_PASSWORD" docker run --detach \
    --name "$container" --label "$owner_label=mysql" \
    --publish 127.0.0.1:3309:3306 \
    --mount "type=volume,source=$volume,target=/var/lib/mysql" \
    --env MYSQL_ROOT_PASSWORD mysql:8.0 >/dev/null
fi

export MYSQL_PWD="$SLACK_AI_GATEWAY_MYSQL_PASSWORD"
ready=false
for ((attempt=1; attempt<=90; attempt++)); do
  if docker exec --env MYSQL_PWD "$container" mysql --host=127.0.0.1 --protocol=tcp --user="$mysql_user" --execute='SELECT 1' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
[ "$ready" = true ] || { echo "Local gateway MySQL did not become ready" >&2; exit 1; }

# Explicit local init always starts empty; the canonical DDL owns database creation.
docker exec --env MYSQL_PWD "$container" \
  mysql --host=127.0.0.1 --protocol=tcp --user="$mysql_user" \
  --execute='DROP DATABASE IF EXISTS `slack_ai_gateway`;'

docker exec --interactive --env MYSQL_PWD "$container" \
  mysql --host=127.0.0.1 --protocol=tcp --user="$mysql_user" \
  < "$repo_root/script/mysql/schema_versioned.ddl"
echo 'Fresh MySQL schema slack_ai_gateway is ready on 127.0.0.1:3309.'
