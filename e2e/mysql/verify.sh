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
[ "$(sql "SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema=DATABASE() AND constraint_type='CHECK'")" = 0 ]
[ "$(sql "SELECT GROUP_CONCAT(table_name ORDER BY table_name) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name!='schema_version'")" = conversation_binding,gateway_job,integration,slack_agent_session_publication,slack_delivery_resolution,slack_outgoing_message ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND data_type='varchar' AND character_maximum_length IN (32,64)")" = 0 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND (column_name IN ('gateway_request_id','slack_thread_root_message_id','slack_prompt_message_id','slack_progress_message_id','slack_message_id')) AND character_maximum_length=255")" = 8 ]
[ "$(sql "SELECT character_maximum_length FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='integration' AND column_name='name'")" = 128 ]
[ "$(sql "SELECT data_type FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='gateway_job' AND column_name='gateway_execution_result_text'")" = longtext ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.key_column_usage WHERE table_schema=DATABASE() AND referenced_table_name IS NOT NULL")" = 8 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND (column_name LIKE '%\_at' OR column_name LIKE '%\_until') AND (data_type!='datetime' OR datetime_precision!=3)")" = 0 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND ((table_name='gateway_job' AND column_name IN ('prompt_text','gateway_execution_result_text')) OR (table_name='slack_outgoing_message' AND column_name='slack_message_text')) AND is_nullable='YES'")" = 3 ]
[ "$(sql "SELECT COUNT(*) FROM (SELECT constraint_name,table_name FROM information_schema.key_column_usage WHERE table_schema=DATABASE() AND referenced_table_name IS NOT NULL GROUP BY constraint_name,table_name HAVING COUNT(*)>1) composite_fks")" = 0 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND column_name IN ('turn_id','installation_id','message_ts','root_thread_ts','slack_ts','query_started_at','message_surface','conversation_id','request_id','latest_status_request_id','status_source_request_id')")" = 0 ]
[ "$(sql "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND column_name='updated_at'")" = 4 ]
[ "$(sql "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='gateway_job' AND index_name='gateway_job_thread'")" = integration_id,slack_channel_id,slack_thread_root_message_id,gateway_execution_state,id ]
[ "$(sql "SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='slack_outgoing_message' AND index_name='slack_outgoing_message_cooldown'")" = slack_delivery_state,slack_delivery_next_attempt_at,job_id ]
[ "$(sql 'SELECT (SELECT COUNT(*) FROM integration)+(SELECT COUNT(*) FROM conversation_binding)+(SELECT COUNT(*) FROM gateway_job)+(SELECT COUNT(*) FROM slack_outgoing_message)+(SELECT COUNT(*) FROM slack_delivery_resolution)+(SELECT COUNT(*) FROM slack_agent_session_publication)')" = 0 ]

# Records are explicit fixture facts; the DDL never inserts application data.
sql "INSERT INTO integration(name,active,slack_app_id,slack_workspace_id,backend_type,backend_endpoint,backend_agent_id,backend_principal_id,created_at,updated_at) VALUES ('first',1,'A','W','agently','https://backend.example.test','agent','principal','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),('second',1,'A2','W','agently','https://backend.example.test','agent','principal','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),('first',0,'A','W','agently','https://backend.example.test','old-agent','principal','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),('first',0,'A','W','agently','https://backend.example.test','older-agent','principal','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
[ "$(sql 'SELECT COUNT(DISTINCT id)=COUNT(*) AND MIN(id)>0 FROM integration')" = 1 ]
[ "$(sql "SELECT COUNT(*) FROM integration WHERE name='first' AND active=0")" = 2 ]
binding="INSERT INTO conversation_binding(integration_id,slack_channel_id,slack_thread_root_message_id,gateway_access_mode,backend_principal_id,gateway_binding_owner_id,gateway_binding_state,created_at,updated_at,last_activity_at) VALUES (1,'C','123.456','shared_channel','principal','principal','reserved','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
sql "$binding"
load_schema
[ "$(sql 'SELECT COUNT(*) FROM conversation_binding')" = 1 ]
reject "$binding" 'duplicate integration thread identity'
sql "INSERT INTO conversation_binding(integration_id,slack_channel_id,slack_thread_root_message_id,gateway_access_mode,backend_principal_id,gateway_binding_owner_id,gateway_binding_state,created_at,updated_at,last_activity_at) VALUES (1,'c','123.456','shared_channel','principal','principal','reserved','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),(1,'C','123.456 ','shared_channel','principal','principal','reserved','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),(2,'C','123.456','shared_channel','principal','principal','reserved','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
[ "$(sql 'SELECT COUNT(*) FROM conversation_binding')" = 4 ]
[ "$(sql "SELECT backend_protocol_messages='[]' AND backend_protocol_state='{}' FROM conversation_binding WHERE id=1")" = 1 ]
sql "UPDATE conversation_binding SET gateway_binding_state='closed' WHERE id=1"

sql "INSERT INTO gateway_job(integration_id,slack_channel_id,slack_thread_root_message_id,slack_event_id,slack_prompt_author_id,slack_prompt_message_id,prompt_text,prompt_has_attachments,gateway_request_id,gateway_execution_state,created_at,gateway_execution_updated_at,gateway_status_updated_at) VALUES (1,'C','123.456','E1','U','123.456',REPEAT('Unicode 😀 ',10000),0,'request','pending','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),(2,'C','123.456','E2','U','123.456','second app',0,'request2','pending','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
[ "$(sql 'SELECT CHAR_LENGTH(prompt_text)>65535 FROM gateway_job WHERE id=1')" = 1 ]
reject "UPDATE gateway_job SET slack_event_id='E1' WHERE id=2" 'global event identity across integration assignments'
reject "UPDATE gateway_job SET conversation_binding_id=99999 WHERE id=1" 'binding foreign key'
other_binding=$(sql "SELECT id FROM conversation_binding WHERE integration_id=2")
# Single-column FKs check existence; cross-scope invariants belong to typed workflow.
sql "UPDATE gateway_job SET conversation_binding_id=$other_binding WHERE id=1"
sql "UPDATE gateway_job SET conversation_binding_id=NULL WHERE id=1"
reject "UPDATE gateway_job SET integration_id=99999 WHERE id=1" 'integration foreign key'
sql "UPDATE gateway_job SET gateway_request_id=REPEAT('r',255) WHERE id=1"
[ "$(sql 'SELECT CHAR_LENGTH(gateway_request_id) FROM gateway_job WHERE id=1')" = 255 ]
reject "UPDATE gateway_job SET gateway_request_id=REPEAT('r',256) WHERE id=1" 'request identifier bound'
# No MySQL CHECKs: typed runtime writes own flag semantics.
sql "UPDATE gateway_job SET prompt_has_attachments=2 WHERE id=1"
[ "$(sql 'SELECT prompt_has_attachments FROM gateway_job WHERE id=1')" = 2 ]
sql "UPDATE gateway_job SET prompt_has_attachments=0 WHERE id=1"
sql "INSERT INTO slack_outgoing_message(job_id,slack_message_index,slack_message_text,slack_delivery_state,created_at,updated_at) VALUES(1,0,REPEAT('😀',12000),'pending','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
[ "$(sql 'SELECT OCTET_LENGTH(slack_message_text) FROM slack_outgoing_message WHERE id=1')" = 48000 ]
[ "$(sql 'SELECT slack_message_kind FROM slack_outgoing_message WHERE id=1')" = response ]
sql "INSERT INTO slack_outgoing_message(job_id,slack_message_index,slack_message_text,slack_message_kind,slack_delivery_state,created_at,updated_at) VALUES (1,1,'Configuration changed','integration_notice','pending','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
reject "UPDATE slack_outgoing_message SET job_id=99999 WHERE id=1" 'outbox parent foreign key'
reject "INSERT INTO slack_delivery_resolution(outgoing_message_id,resolution_action,duplicate_delivery_risk_accepted,created_at) VALUES(99999,'retry',1,'1970-01-01 00:00:00.001')" 'delivery foreign key'

sql "INSERT INTO slack_agent_session_publication(initial_integration_id,slack_channel_id,slack_thread_root_message_id,latest_status_job_id,status_source_job_id,slack_initiator_user_id,updated_at,created_at) VALUES (1,'C','123.456',1,1,'U','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001'),(2,'C','123.456',2,2,'U','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')"
[ "$(sql 'SELECT COUNT(*) FROM slack_agent_session_publication')" = 2 ]
reject "UPDATE slack_agent_session_publication SET status_source_job_id=99999 WHERE initial_integration_id=1" 'session source foreign key'
sql "UPDATE slack_agent_session_publication SET status_source_job_id=2 WHERE initial_integration_id=1"
sql "UPDATE slack_agent_session_publication SET status_source_job_id=1 WHERE initial_integration_id=1"
[ "$(sql 'SELECT COUNT(DISTINCT id)=COUNT(*) AND MIN(id)>0 FROM slack_agent_session_publication')" = 1 ]
reject "INSERT INTO slack_agent_session_publication(initial_integration_id,slack_channel_id,slack_thread_root_message_id,latest_status_job_id,status_source_job_id,slack_initiator_user_id,updated_at,created_at) VALUES (1,'C','123.456',1,1,'U','1970-01-01 00:00:00.001','1970-01-01 00:00:00.001')" 'one publication per original assignment thread'

# Old developer baseline must fail explicitly; never auto-migrate or reset it.
sql "CREATE DATABASE slack_ai_gateway_old_baseline; CREATE TABLE slack_ai_gateway_old_baseline.schema_version(version_number INT UNSIGNED PRIMARY KEY); INSERT INTO slack_ai_gateway_old_baseline.schema_version VALUES(1); CREATE TABLE slack_ai_gateway_old_baseline.integrations(id VARCHAR(128) PRIMARY KEY,installation_id VARCHAR(128),agent_id VARCHAR(128))"
if sed 's/`slack_ai_gateway`/`slack_ai_gateway_old_baseline`/g' "$repo_root/script/mysql/schema_versioned.ddl" | docker exec --interactive --env MYSQL_PWD "$container" mysql --host=127.0.0.1 --protocol=tcp --user=root >/dev/null 2>&1; then
  echo 'Expected old developer baseline rejection' >&2
  exit 1
fi
[ "$(sql 'SELECT COUNT(*) FROM slack_ai_gateway_old_baseline.schema_version WHERE version_number=1')" = 1 ]
echo 'MySQL schema acceptance passed; local fixture was not modified.'
