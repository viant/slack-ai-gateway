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

`transcribe` and `validate` use an explicitly configured discovery database;
`run` provisions an isolated fixture and performs generation, validation and
runtime tests. MySQL remains the reference metadata source for generation. The
same generated components execute against SQLite and MySQL without regenerating
when the runtime driver changes. See the [workflow instructions](../e2e/datly/README.md)
for credentials, prerequisites and fixture isolation.
