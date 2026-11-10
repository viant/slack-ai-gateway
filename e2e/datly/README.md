# Datly storage verification

These workflows use the stock Datly v1 CLI and the typed Go components generated
from the root [`dql/`](../../dql/README.md) tree into `internal/datly/`.
SQLite and MySQL use the same generated Go,
embedded SQL and storage workflow. Use the generator version matching the Datly
revision in `go.mod`. Go, Datly, Endly and the native MySQL client must
be on `PATH`; the full workflow also requires Docker.

Check the selected runtime and generator revision before regenerating:

```sh
go list -m github.com/viant/datly
go version -m "$(command -v datly)"
```

The Datly module version (or its `vcs.revision` commit for a local v1 build) must
match. From the gateway repository root, build the selected revision using the
project's module dependencies:

```sh
go build -o bin/datly github.com/viant/datly/cmd/datly
export PATH="$PWD/bin:$PATH"
```

Ensure the resulting binary is the `datly` found on `PATH`. The pinned Datly module
contains a `replace` directive, so `go install ...@version` rejects it. Building
from this project's module uses its selected dependencies without that directive;
generation and tests need no local absolute `replace`.

From this directory:

```sh
endly -r=run
```

`run` creates a disposable MySQL 8.0 container on an automatically assigned
loopback port and applies the canonical MySQL DDL. It transcribes the common
components once using MySQL metadata, validates the same sources against both
MySQL and a temporary SQLite fixture, and runs the SQLite, MySQL and shared Datly
storage suites. Each live MySQL test additionally gets its own database.
The container, its anonymous storage and the SQLite fixture are removed afterward.
This does not reset or use the persistent E2E database on port 3309. No Slack or
Agently calls are made, and the personal `~/.secret/mysql-e2e.json` credential
file is not required.

Endly runs explicit stages for provisioning, readiness (up to 180 seconds),
schema initialization, transcription, MySQL validation, SQLite fixture
preparation and validation, and Go tests. Transcription
lists each component and its operation directly in YAML; when adding a DQL
component, add its native `datly transcribe` command to `transcribe.yaml`.
The workflow does not parse DQL or select operations itself.

The DQL package passed to `datly transcribe` selects the source, while its
`#package` declaration selects the generated Go destination. Source SQL remains
beside its DQL contract; generated SQL resources are embedded with their Go
components. Keep handwritten component dependencies and hooks in separate
sidecar files. Shared predicates live in `internal/datly/predicate`; the storage
adapter, transactional workflow and native pilot tests stay in
`internal/storage/datly`. Generation and validation commands below are unchanged.

## Standalone transcription and validation

By default, these commands use the existing local MySQL fixture. Prepare its Endly
credentials once before starting. Check whether `~/.secret/mysql-e2e.json`
already exists; it may be shared with Agently Core or other E2E workflows.
Reuse it when it matches the gateway server and do not overwrite it blindly.
If it does not exist, run:

```sh
endly -c=mysql-e2e
```

Enter username `root` and password `dev` for the standard local fixture.
Endly writes `~/.secret/mysql-e2e.json` and loads its stored credentials,
including encrypted passwords, through its native credential support. Do not
handwrite plaintext JSON credentials. For an existing MySQL volume, use its
original password: a credential file does not change the server's password.

The default credential name is `mysql-e2e.json`. If the existing shared file
belongs to another server, create a separate credential with
`endly -c=mysql-slack-e2e`, then pass `mysqlCred=mysql-slack-e2e.json` on the
initialization and standalone generation/validation commands. An absolute
credential path, such as `mysqlCred=/absolute/path/another.json`, is also
supported. The [local MySQL instructions](../mysql/README.md) show the complete
alternate-credential sequence.

Initialize the server from `e2e/`:

```sh
endly -t=init
```

Then regenerate or validate from `e2e/datly/`:

```sh
endly -r=transcribe
endly -r=validate
```

The default connection uses these Endly credentials with
`127.0.0.1:3309/slack_ai_gateway`. Both commands invoke the stock Datly CLI
with `-driver` and `-dsn` directly. They do not create a temporary connector file,
start Docker or run container cleanup. See the
[local MySQL instructions](../mysql/README.md) for fixture lifecycle details.

The discovery connection is selected in this order:

1. An explicit `dsn` workflow parameter.
2. A local MySQL DSN built from the Endly credential selected by `mysqlCred`.

An explicit DSN bypasses the personal credential file. `driver` defaults to
`mysql` and can also be passed as a workflow parameter. The isolated full `run`
workflow passes its own `driver` and `dsn` for MySQL and SQLite discovery;
neither standalone commands nor full E2E create connector JSON files.
Native Endly credential placeholders receive its secret masking in command
logs. Direct DSN parameters use ordinary command logging; they are not implicitly
masked.

Generated Go and embedded SQL resources belong in the repository; edit their
DQL sources rather than generated files. The gateway runtime independently
loads its configured DSN and Scy credential resource; it does not implicitly use
Endly's credential file. The removed runtime DSN environment variable is not a
standalone workflow override.

MySQL is the reference metadata source for generation, not a runtime requirement
for SQLite. Changing `storage.driver` selects a connector; it does not require
regenerating components. Ordinary SQLite use requires neither Datly CLI nor
Docker. The fixture helper `go run ./e2e/datly/schema FILE` reuses the gateway's
SQLite initialization and migration lifecycle instead of maintaining another DDL.
The stock Datly CLI inspects that fixture through its linked `sqlite3` driver;
the gateway runtime keeps its existing `modernc` SQLite driver and connection
settings. Both access the same SQLite file format.

The common graphs retain each engine's existing comparison behavior. In
particular, the native smoke-namespace predicate selects the original MySQL
case/accent-insensitive collation or SQLite `LIKE` semantics from the invocation
dialect; these spellings are not separate generated models.

`transcribe` and `validate` only inspect the selected database; they
do not apply DDL or reset its data. The full `run` workflow supplies its own
isolated MySQL and SQLite discovery connections, independently of the personal
local E2E credentials.

In the full `run` workflow, Endly `defer` tasks remove temporary credentials and
the owned test container after success or an ordinary stage failure. The failing
stage remains a failure even if cleanup also fails. Forced process termination, including `SIGKILL`,
cannot guarantee cleanup; normal completion and reported workflow errors are
the supported cleanup paths.

For direct Go tests, `SLACK_AI_GATEWAY_MYSQL_TEST_DSN` is an explicit administrative
connection to a test MySQL server. Its original database is never changed. Tests
create and drop only their own randomly named databases, so this user must have
permission to create databases and stored routines. Without this variable, the
live MySQL tests skip; `endly -r=run` supplies it automatically.

Datly validation checks source contracts and available metadata. Runtime,
transaction, concurrency and recovery behavior is established by the Go tests,
not by validation alone.
