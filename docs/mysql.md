# MySQL storage

The gateway supports SQLite and MySQL 8.0 through the same embedded Datly object
graphs: generated readers/writers and one private typed storage workflow run
inside the gateway. The configured driver and DSN select the database connection;
the generated Go and SQL are shared. No separate Datly HTTP service is required.

Use one dedicated database per gateway process. Multiple configured integrations
share one assignment table with numeric identities and scoped conversation and
queue records. Multi-process recovery and HA are not supported; stop the runtime
before operator commands. See [multiple integrations](integrations.md).
The local E2E database is named `slack_ai_gateway`, while its tables and
gateway behavior remain independent of the configured Agently agent.

## Provision a local database

The explicit Endly workflow requires Docker, Endly and local MySQL credentials.
Prepare the credentials once before initializing the local fixture or using
the default local connection for standalone Datly transcription/validation.

First check whether `~/.secret/mysql-e2e.json` already exists. It may be shared
with Agently Core or other local E2E workflows. Reuse it only when its credentials
match the local gateway MySQL server; do not overwrite an existing shared file
just to configure this project.

If the credential file does not exist, create it through Endly:

```sh
endly -c=mysql-e2e
```

Enter username `root` and password `dev` for the project's standard local
fixture. Endly writes `~/.secret/mysql-e2e.json` and owns its credential encoding
and encrypted-password loading. Do not handwrite a plaintext JSON credential
file. For an existing MySQL volume, the password must match the one used when
that volume was initialized; creating credentials does not change MySQL's password.

If the existing shared credential belongs to another server, create a separate
one with `endly -c=mysql-slack-e2e` and select it with
`mysqlCred=mysql-slack-e2e.json` on the initialization, transcription and
validation commands below. `mysqlCred=/absolute/path/another.json` is also
supported. Keep the shared file intact; see the
[credential setup examples](../e2e/mysql/README.md#create-local-credentials-once).

Then, from `e2e/`:

```sh
endly -t=init
```

Stop the gateway first. This workflow creates or reuses the project's own MySQL
container and persistent volume, bound to `127.0.0.1:3309`. **Every `init` drops
and recreates only `slack_ai_gateway`**, then applies
`script/mysql/schema_versioned.ddl`. All previous local gateway tables and rows
are removed; the current schema starts empty without migrating old data.

The container, volume, authentication and other databases remain unchanged.
Use the original password when restarting an existing volume. This explicit
local reset does not run during gateway startup or reset Agently test databases.
See the [fixture instructions](../e2e/mysql/README.md).

For another environment, have its database administrator apply the versioned
schema explicitly and configure the gateway's DSN for that database. The gateway's
`run`, `status` and `resolve-delivery` commands do not create, migrate or repair a
MySQL schema and do not check its version. `migrate` supports SQLite only.

## Configure the gateway

Replace the entire `storage` section in the existing runtime configuration:

```yaml
storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true&loc=UTC'
  secret:
    URL: file:///absolute/path/mysql.json
    Key: blowfish://default
```

The DSN retains the address, database and options; Scy loads the username/password
from the resource, and Datly performs native substitution when opening storage.
Do not set `storage.path` with MySQL. `storage.secret` is optional when using a
complete DSN for local development. There is no environment-variable fallback.

Create the runtime credential file with `scy secure --target basic`, following
[secret setup](secrets.md). Use the actual credentials of an existing volume.
The runtime resource is independent of the credential file selected by Endly for
provisioning and Datly discovery. Keep real credentials and resource locations
outside version control.

For production, set the database address and choose an AWS Secrets Manager
resource in `storage.secret.URL`. Configure verified MySQL TLS when supported by
the deployment, for example by adding `&tls=true` when its certificate is trusted
by the gateway. The configured database must already be provisioned.

```sh
go run ./cmd/slack-ai-gateway validate-config --config /absolute/path/gateway.local.yaml
go run ./cmd/slack-ai-gateway run --config /absolute/path/gateway.local.yaml
```

Validation checks configuration without loading credentials or connecting.
Commands that open MySQL load its credential resource at that point. Missing or
unavailable storage causes an error; the gateway never switches to SQLite.

The assignment schema requires a fresh database. This development change does
not migrate the earlier four-table registry or other prerelease initial schemas.
Directly reapplying a compatible initial DDL definition preserves data; an
incompatible initial shape is rejected instead of being silently upgraded or
reset. The explicit local `endly -t=init` workflow drops its database before
applying that DDL, so it always starts fresh.

The [six business tables](schema.md) use eight single-column foreign keys to
stable parent IDs. `integration` records Slack app/workspace and backend/agent
assignments, with a logical name and configuration-derived `active` flag. The
gateway reconciles assignments at startup; replacing/removing one closes its old
local work. Numeric IDs are internal and inactive historical IDs are never revived.
See [assignment lifecycle](integrations.md#recorded-assignments-and-restarts).

`gateway_job` combines each prompt's execution, backend observation and Slack
progress metadata. It and `conversation_binding` reference their numeric
assignment. `slack_outgoing_message` obtains its Slack destination through
`job_id`. Deduplication is global `UNIQUE(slack_event_id)` while request primary
keys remain numeric. Native session publications retain an immutable
`initial_integration_id`; their source/latest requests must agree on the physical
Slack app/workspace/channel/thread. The transactional workflow owns these
cross-record agreements; individual foreign keys enforce parent existence.

Slack message IDs remain text: `slack_prompt_message_id`,
`slack_thread_root_message_id`, `slack_progress_message_id` and `slack_message_id`.
`backend_conversation_id` identifies the backend conversation.
`gateway_request_id` remains stable across observation/recovery;
`backend_submission_started_at` records the start of a submission attempt, not
proof of remote acceptance. There is no duplicated `turn_id` column.

Bounded external IDs and Slack message/thread IDs use `VARCHAR(255)`; fixed
state, mode, outcome, kind, format, backend-type and resolution-action values
use `VARCHAR(128)`. Reason codes remain `VARCHAR(255)` and open-ended backend
IDs/endpoints remain `TEXT`. The largest declared key,
`gateway_job_thread`, fits at 2,568 bytes on the verified 16 KiB InnoDB setup.
DDL and Skeema definitions use the server default row format, without an explicit
`ROW_FORMAT` clause. See [capacity contracts](schema.md#naming-and-capacity-contracts).

Persisted moments use `DATETIME(3)` in UTC, with absent/not-yet moments as SQL
`NULL`. IDs, attempts, revisions and duration measurements remain numeric.
Application-supplied moments are normalized to UTC and truncated to milliseconds;
the native MySQL driver reads dates with `parseTime=true&loc=UTC`. Timestamp fields
in operator JSON remain Unix-millisecond numbers, independent of the database
representation. Required row creation/update dates and the separate execution
and status update dates retain their operation-specific meanings.

The schema supports future removal of redundant prompt/result/outgoing text while
preserving IDs, metadata and protocol context needed to continue old threads.
It also supports full graph removal, which forgets the old backend conversation
mapping and deduplication history. No retention configuration, scheduled cleanup
or deletion command is implemented. See [retention preparation](schema.md#future-retention-contract).

Query and delivery authorization continues through the existing policy and
original author/binding context. Terminal notices can use a still-authorized
transport for the same Slack app/workspace without starting the old backend;
missing historical credentials are never reconstructed from the database.

Selecting MySQL does not copy SQLite rows, conversation bindings, pending replies
or thread history. Keep the previous database if it is needed for inspection.
Diagnostics and uncertain-delivery reconciliation use the same commands and JSON
format with either driver; see [operations](operations.md).

## Generate and verify Datly graphs

Use the stock Datly v1 CLI matching the Datly revision in `go.mod`. Authoritative
sources live in [`dql/`](../dql/README.md); generated Go and embedded SQL live
under `internal/datly/`. Edit DQL rather than generated files. Application-owned
sidecars and shared predicates also live under `internal/datly/`; the storage
adapter and transactional orchestration stay in `internal/storage/`.
MySQL remains the reference metadata source for generation. These same components
run on SQLite without regeneration or a MySQL server.

From `e2e/datly/`:

```sh
endly -r=run
```

This workflow starts an isolated MySQL container on a random loopback port,
applies the DDL and transcribes the common graphs once. It validates them against
both MySQL and a temporary SQLite fixture, then runs the SQLite, MySQL and shared
Datly storage suites. Each live MySQL test receives its own database. The disposable
container and storage are removed afterward. This does not reset the persistent
database on port 3309 and requires neither the personal MySQL credential file
nor Slack or Agently credentials.

For generation or validation against the initialized local fixture, use
`endly -r=transcribe` or `endly -r=validate` from `e2e/datly/`. They load the
local E2E credentials through Endly and connect to
`127.0.0.1:3309/slack_ai_gateway`. These standalone commands use the existing
fixture; they do not start MySQL, recreate the database or run initialization.
Their default credential name is `mysql-e2e.json`; use `mysqlCred` to choose
another file. An explicit `dsn` workflow parameter overrides the discovery
connection and bypasses the personal credential file, without changing local
initialization.
The gateway runtime uses its configured DSN and optional Scy credential resource.
Ordinary `go test ./...` skips live MySQL tests unless
`SLACK_AI_GATEWAY_MYSQL_TEST_DSN` is set. Details and test-user requirements are in
the [Datly workflow instructions](../e2e/datly/README.md).
