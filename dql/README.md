# Datly contracts

This is the gateway's authoritative DQL tree. Each component declares its
generated Go package, private route, input/output shapes, parameters and view
graph. Executable source SQL stays in adjacent `sql/` directories and is
referenced through `${embed:sql/...}` resources. Preserve the existing component
hierarchy when editing contracts.

The source and runtime responsibilities are separate:

- `dql/`: authored reader/writer contracts and their source SQL assets.
- `internal/datly/`: generated Go components and embedded SQL, with the same
  component hierarchy. Handwritten dependency sidecars stay with their owning
  component; shared predicates live in `internal/datly/predicate`.
- `internal/storage/`: the storage adapter, transactional workflow, database
  initialization and storage tests. The common adapter, workflow and native pilot
  tests remain in `internal/storage/datly`.

The source package passed to `datly transcribe` selects the DQL input;
`#package(...)` selects its generated Go destination. Edit DQL to change generated
contracts. Keep application code in separate sidecars or storage workflow files,
and regenerate rather than modifying generated files directly.

Use the stock Datly v1 CLI matching the revision in `go.mod`, through the existing
Endly workflows. From `e2e/datly/`:

```sh
endly -r=transcribe
endly -r=validate
endly -r=run
```

For the default local `transcribe` or `validate`, prepare the E2E credentials
with `endly -c=mysql-e2e` if `~/.secret/mysql-e2e.json` does not already exist.
Use username `root` and password `dev` for the standard fixture, or the original
password of an existing MySQL volume. Do not overwrite a shared existing file;
Endly owns credential creation and encrypted-password loading. Initialize the
server with `endly -t=init` from `e2e/`. See the
[local MySQL instructions](../e2e/mysql/README.md) for the complete setup.
If the shared credential belongs to another server, create a separate one with
`endly -c=mysql-slack-e2e` and pass `mysqlCred=mysql-slack-e2e.json` to local
initialization and standalone transcription/validation.

`transcribe` and `validate` use those credentials and the initialized local MySQL
fixture by default. They invoke the stock Datly CLI directly without provisioning
Docker or creating temporary connector files.
An explicit `dsn` workflow parameter overrides the discovery connection and
bypasses the personal credential file. There is no runtime-DSN environment
override. The full workflow passes its own driver and DSN
directly, without connector JSON files.
`run` provisions an isolated fixture and performs generation, validation and
runtime tests without requiring personal MySQL credentials. MySQL remains the
reference metadata source for generation. The same generated components execute
against SQLite and MySQL without regenerating
when the runtime driver changes. See the [workflow instructions](../e2e/datly/README.md)
for credentials, prerequisites and fixture isolation.
