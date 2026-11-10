# MySQL storage

The gateway supports SQLite and MySQL 8.0 through the same embedded Datly object
graphs: generated readers/writers and one private typed storage workflow run
inside the gateway. The configured driver and DSN select the database connection;
the generated Go and SQL are shared. No separate Datly HTTP service is required.

Use a dedicated database for one integration and one gateway process. Separate
integrations can use separate databases on the same MySQL server. Multi-process
recovery and HA are not supported; stop the runtime before operator commands.
The local E2E database is named `agently_steward_slack`, while its tables and
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

This creates the project's own MySQL container and persistent volume, binds
`127.0.0.1:3309`, and applies `script/mysql/schema_versioned.ddl` to
`agently_steward_slack`. Its initial business tables are empty. Repeating `init`
preserves data; use the same password when restarting an existing volume.
The workflow does not reset other MySQL databases or existing Agently test servers.
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
  dsn_env: AGENTLY_SLACK_MYSQL_DSN
```

`dsn_env` names an environment variable; it is not the DSN itself. Do not set
`storage.path` with MySQL. For the standard local fixture:

```sh
export AGENTLY_SLACK_MYSQL_DSN='root:dev@tcp(127.0.0.1:3309)/agently_steward_slack?parseTime=true'
```

Use the actual password for an existing volume. Store the real value in a private
environment/secret file outside version control, then load that environment
before starting the gateway. Keep the Slack, access
policy and Agently sections of the configuration as usual.
The gateway runtime continues to read this DSN environment variable; it does not
load the Endly E2E credential file.

```sh
go run ./cmd/agently-slack-gateway validate-config --config /absolute/path/gateway.local.yaml
go run ./cmd/agently-slack-gateway run --config /absolute/path/gateway.local.yaml
```

Validation checks the configured variable name without loading credentials or
connecting. Commands that open MySQL require the variable's value. Missing or
unavailable storage causes an error; the gateway never switches to SQLite.

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
`127.0.0.1:3309/agently_steward_slack`. These commands do not start MySQL.
Their default credential name is `mysql-e2e.json`; use `mysqlCred` to choose
another file. An explicit `dsn` workflow parameter takes precedence over a
nonempty `AGENTLY_SLACK_MYSQL_DSN`; either overrides the discovery connection
and bypasses the personal credential file, without changing local initialization.
The gateway runtime still requires its configured DSN environment variable.
Ordinary `go test ./...` skips live MySQL tests unless
`AGENTLY_SLACK_MYSQL_TEST_DSN` is set. Details and test-user requirements are in
the [Datly workflow instructions](../e2e/datly/README.md).
