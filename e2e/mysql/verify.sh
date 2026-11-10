#!/usr/bin/env bash
# Exercise the DDL in an isolated server; never reset the user's local fixture.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
container="slack_ai_gateway_schema_test_$(openssl rand -hex 6)"
export MYSQL_ROOT_PASSWORD
MYSQL_ROOT_PASSWORD=$(openssl rand -hex 24)
export MYSQL_PWD="$MYSQL_ROOT_PASSWORD"
cleanup() { docker rm --force --volumes "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM

docker run --detach --rm --name "$container" \
  --label com.viant.slack-ai-gateway.e2e=schema-test \
  --tmpfs /var/lib/mysql:rw --env MYSQL_ROOT_PASSWORD mysql:8.0 >/dev/null
ready=false
for ((attempt=1; attempt<=90; attempt++)); do
  if docker exec --env MYSQL_PWD "$container" mysql --host=127.0.0.1 --protocol=tcp --user=root --execute='SELECT 1' >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 2
done
[ "$ready" = true ] || { echo 'Schema acceptance MySQL did not become ready' >&2; exit 1; }

load_schema() {
  docker exec --interactive --env MYSQL_PWD "$container" \
    mysql --host=127.0.0.1 --protocol=tcp --user=root \
    < "$repo_root/script/mysql/schema_versioned.ddl"
}
sql() {
  docker exec --env MYSQL_PWD "$container" \
    mysql --host=127.0.0.1 --protocol=tcp --user=root --default-character-set=utf8mb4 \
    --batch --skip-column-names slack_ai_gateway --execute="$1"
}
reject() {
  if sql "$1" >/dev/null 2>&1; then
    echo "Expected database constraint rejection: $2" >&2
    exit 1
  fi
}

load_schema
[ "$(sql 'SELECT get_schema_version()')" = 1 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE()")" = 7 ]
[ "$(sql 'SELECT (SELECT COUNT(*) FROM conversation_bindings)+(SELECT COUNT(*) FROM inbox_jobs)+(SELECT COUNT(*) FROM outbox_messages)+(SELECT COUNT(*) FROM delivery_resolutions)+(SELECT COUNT(*) FROM request_status)+(SELECT COUNT(*) FROM agent_session_publications)')" = 0 ]

binding="INSERT INTO conversation_bindings(workspace_id,channel_id,root_thread_ts,access_mode,principal_id,owner_id,state,created_at,updated_at) VALUES ('W','C','123.456','shared_channel','principal','principal','reserved',1,1)"
sql "$binding"
load_schema
[ "$(sql 'SELECT COUNT(*) FROM conversation_bindings')" = 1 ]
reject "$binding" 'duplicate thread identity'
sql "INSERT INTO conversation_bindings(workspace_id,channel_id,root_thread_ts,access_mode,principal_id,owner_id,state,created_at,updated_at) VALUES ('w','C','123.456','shared_channel','principal','principal','reserved',1,1),('W','C','123.456 ','shared_channel','principal','principal','reserved',1,1)"
[ "$(sql 'SELECT COUNT(*) FROM conversation_bindings')" = 3 ]
reject "UPDATE conversation_bindings SET state='invalid' WHERE id=1" 'binding state'

sql "INSERT INTO inbox_jobs(app_id,workspace_id,channel_id,root_thread_ts,event_id,author_id,message_ts,query_text,has_attachments,request_id,state,created_at,updated_at) VALUES ('A','W','C','123.456','E','U','123.456',REPEAT('Unicode 😀 ',10000),0,'request','pending',1,1)"
[ "$(sql 'SELECT CHAR_LENGTH(query_text)>65535 FROM inbox_jobs')" = 1 ]
reject "UPDATE inbox_jobs SET binding_id=99999 WHERE id=1" 'binding foreign key'
reject "UPDATE inbox_jobs SET has_attachments=2 WHERE id=1" 'boolean constraint'
reject "UPDATE inbox_jobs SET message_surface='invalid' WHERE id=1" 'message surface'
sql "INSERT INTO request_status(job_id,updated_at) VALUES (1,1)"
reject "UPDATE request_status SET mode='invalid' WHERE job_id=1" 'request status mode'
sql "INSERT INTO outbox_messages(job_id,workspace_id,channel_id,root_thread_ts,part,content,state,created_at,updated_at) VALUES(1,'W','C','123.456',0,'reply','pending',1,1)"
reject "UPDATE outbox_messages SET part=-1 WHERE id=1" 'nonnegative response part'
reject "UPDATE outbox_messages SET format='invalid' WHERE id=1" 'reply format'
reject "INSERT INTO delivery_resolutions(delivery_id,action,duplicate_risk_accepted,created_at) VALUES(99999,'retry',1,1)" 'delivery foreign key'

sql "INSERT INTO agent_session_publications(workspace_id,channel_id,root_thread_ts,latest_job_id,source_job_id,initiator_user_id,updated_at) VALUES ('W','C','123.456',1,1,'U',1)"
reject "UPDATE agent_session_publications SET source_job_id=99999" 'session source foreign key'
reject "UPDATE agent_session_publications SET desired_status='invalid'" 'desired session status'
echo 'MySQL schema acceptance passed; local fixture was not modified.'
