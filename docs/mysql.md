# MySQL storage

The gateway supports SQLite and MySQL 8.0. MySQL uses embedded Datly object graphs:
generated readers/writers and a private typed workflow run inside the gateway.
No separate Datly HTTP service is required.

Use a dedicated database for one integration and one gateway process. Separate
integrations can use separate databases on the same MySQL server. Multi-process
recovery and HA are not supported; stop the runtime before operator commands.
The local E2E database is named `agently_steward_slack`, while its tables and
gateway behavior remain independent of the configured Agently agent.

## Provision a local database

The explicit Endly workflow requires Docker and Endly. From `e2e/`:

```sh
export AGENTLY_SLACK_MYSQL_PASSWORD='<local test password>'
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
`storage.path` with MySQL. A local DSN has this form:

```text
username:password@tcp(127.0.0.1:3309)/agently_steward_slack?parseTime=true
```

Store the real value in a private environment/secret file outside version control,
then load that environment before starting the gateway. Keep the Slack, access
policy and Agently sections of the configuration as usual.

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
sources live in `internal/storage/mysql/dql`; generated Go and embedded SQL live
under `internal/storage/mysql/graph`. Edit DQL rather than generated files.

From `e2e/datly/`:

```sh
endly -r=run
```

This workflow starts an isolated MySQL container on a random loopback port,
applies the DDL, transcribes and validates the graphs, and runs MySQL storage
tests. Each test receives its own database. The disposable container and storage
are removed afterward. This does not reset the persistent database on port 3309
and requires no Slack or Agently credentials.

For generation or validation against an explicitly prepared discovery database,
set `AGENTLY_SLACK_MYSQL_DSN`, then use `endly -r=transcribe` or
`endly -r=validate`. Ordinary `go test ./...` skips live MySQL tests unless
`AGENTLY_SLACK_MYSQL_TEST_DSN` is set. Details and test-user requirements are in
the [Datly workflow instructions](../e2e/datly/README.md).
