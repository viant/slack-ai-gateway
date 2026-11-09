# Datly storage verification

These workflows use the stock Datly v1 CLI and the typed Go components generated
from `internal/storage/mysql/dql`. Use the generator version matching the Datly
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
loopback port, applies the canonical DDL, transcribes and validates the graph,
and runs all MySQL storage tests. Each test additionally gets its own database.
The container and its anonymous storage are removed afterward. This does not
reset or use the persistent E2E database on port 3309. No Slack or Agently calls
are made.

Endly runs explicit stages for provisioning, readiness (up to 180 seconds),
schema initialization, transcription, validation and Go tests. Transcription
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
