# Datly storage verification

These workflows use the stock Datly v1 CLI and the typed Go components generated
from `internal/storage/datly/dql`. SQLite and MySQL use the same generated Go,
embedded SQL and storage workflow. Use the generator version matching the Datly
revision in `go.mod`. Go, Datly, Endly and the native MySQL client must
be on `PATH`; the full workflow also requires Docker.

Check the selected runtime and generator revision before regenerating:

```sh
go list -m github.com/viant/datly
go version -m "$(command -v datly)"
```

The Datly module version (or its `vcs.revision` commit for a local v1 build) must
match. To build the published revision selected by this project:

```sh
datly_version="$(go list -m -f '{{.Version}}' github.com/viant/datly)"
go install "github.com/viant/datly/cmd/datly@${datly_version}"
```

Ensure the resulting binary is the `datly` found on `PATH`. Generation and tests
use the project's module dependencies; no local absolute `replace` is required.

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
Agently calls are made.

Endly runs explicit stages for provisioning, readiness (up to 180 seconds),
schema initialization, transcription, MySQL validation, SQLite fixture
preparation and validation, and Go tests. Transcription
lists each component and its operation directly in YAML; when adding a DQL
component, add its native `datly transcribe` command to `transcribe.yaml`.
The workflow does not parse DQL or select operations itself.

To regenerate or validate against an explicitly provisioned discovery database,
set `AGENTLY_SLACK_MYSQL_DSN` in the environment, then run:

```sh
endly -r=transcribe
endly -r=validate
```

The workflow stores discovery credentials in a temporary private connector file,
rather than command arguments. Generated Go and embedded SQL resources belong in
the repository; edit their DQL sources rather than generated files.

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

`transcribe` and `validate` only inspect the explicitly selected database; they
do not apply DDL or reset its data. Both share the native Endly connection
preparation in `connection.yaml`. The full `run` workflow supplies its own
connector file, independently of any discovery DSN in your environment.

Endly `defer` tasks remove temporary credentials and the owned test container
after success or an ordinary stage failure. The failing stage remains a failure
even if cleanup also fails. Forced process termination, including `SIGKILL`,
cannot guarantee cleanup; normal completion and reported workflow errors are
the supported cleanup paths.

For direct Go tests, `AGENTLY_SLACK_MYSQL_TEST_DSN` is an explicit administrative
connection to a test MySQL server. Its original database is never changed. Tests
create and drop only their own randomly named databases, so this user must have
permission to create databases and stored routines. Without this variable, the
live MySQL tests skip; `endly -r=run` supplies it automatically.

Datly validation checks source contracts and available metadata. Runtime,
transaction, concurrency and recovery behavior is established by the Go tests,
not by validation alone.
