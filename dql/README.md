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

Lock-bearing root SQL reads integration metadata through ordinary scalar
subqueries. Native row locking places `FOR UPDATE` on the physical root SELECT;
joining shared assignment rows into that SELECT would also lock those shared
rows and couple independent threads. Child and operator reads remain ordinary
non-locking joins. Optional metadata filters retain native predicate groups
inside `EXISTS`, including explicit empty-value presence.

The existing `integrations` component owns the singular `integration` table,
numeric assignment snapshots and configuration activity. Global event-existence
lookup returns duplicate disposition without exposing another assignment's job. Runtime mutation scopes use numeric IDs;
offline diagnostics can select a logical name across its historical assignments.
Explicit false/zero updates retain native presence markers.

The root job writer persists the combined `gateway_job` record and its outgoing
messages. Backend observation and Slack progress are scalar fields on that root;
there is no separate `request_status` child or backfill path. Sparse writes retain
separate execution/status field ownership, including
`gateway_execution_updated_at` and `gateway_status_updated_at`.

Physical moments use MySQL `DATETIME(3)` metadata and SQLite `DATETIME` through
native `time.Time` / `*time.Time` shapes. Authored SQL uses nullable deadlines and
nullable submission/publication markers instead of numeric zero sentinels.
Application times are UTC and millisecond-truncated; diagnostic serialization
alone converts them to Unix milliseconds. Cooldown aggregation returns nullable
temporal MAX values, including an empty result, through the same reader.
Keep source column nullability and native sparse omission/explicit-NULL behavior
when regenerating. No separate time adapter or alternate database workflow exists.

See [the schema glossary](../docs/schema.md) for the canonical names and the
future content-cleanup/full-deletion contract. Retention columns and indexes do
not add a scheduler, configuration section or deletion component.

Use the stock Datly v1 CLI matching the revision in `go.mod`, through the existing
Endly workflows. From `e2e/datly/`:

```sh
endly -r=transcribe
endly -r=validate
endly -r=run
```

Transcription excludes each generated destination package from source discovery.
The authored DQL and discovery schema therefore define the new component rather
than admitting an older generated `Current` read that may refer to removed
columns. This uses the native CLI `-exclude` option; generated files are still
produced and replaced by Datly.

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

The canonical generated graph uses `GatewayJob`, `ConversationBinding`,
`SlackOutgoingMessage` and `SlackAgentSessionPublication` entities, with plural
collection holders. Diagnostic readers expose the corresponding `...Metadata`
types. The outgoing-message parent is `JobId` / `job_id`; native-session pointers
are `LatestStatusJobId` / `latest_status_job_id` and `StatusSourceJobId` /
`status_source_job_id`. These names do not change public CLI commands, YAML
configuration keys or backend AG-UI correlation fields.
