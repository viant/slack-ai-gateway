# Slack AI Gateway

A Go service connecting Slack channels and opt-in direct messages to Agently or
protocol-only AG-UI backends.
Each Slack thread maps to a conversation under one configured execution identity.
Questions in a channel require a bot mention, including thread follow-ups. Direct
messages use a separate access policy and do not require mentions.

The gateway persists questions, conversation bindings, final replies and progress
state in SQLite or MySQL. It reports progress while Agently works and delivers completed
answers in the selected presentation mode. Interrupted executions are observed
without resubmitting prompts; uncertain remote effects require operator review.
Answer text is not streamed to Slack.

Both database drivers use the same embedded Datly readers, writers and storage
workflow. The configured driver selects the connection; it does not select a
separate implementation or require regenerating code. Schema installation and
SQLite file preparation remain specific to the selected database.

The [storage schema](docs/schema.md) groups execution and progress in one
`gateway_job` table. Six business tables preserve routing, backend conversation
identity, delivery history and native Slack publication metadata. Dates use UTC
with millisecond precision. The schema is prepared for future content cleanup
and whole-conversation removal; no retention schedule or cleanup command runs yet.

## Requirements

- Go 1.26 or newer; macOS or Linux for the SQLite lifetime lock.
- An Agently deployment with OOB/PKCE login, the configured agent and an account
  permitted to use it, or a compatible AG-UI execution endpoint. Browser login and
  arbitrary MFA flows are not implemented.
- A Slack app installed in one workspace, with Socket Mode enabled.
- SQLite storage, or a provisioned MySQL 8.0 database. Socket Mode is the supported transport.

Users of each integration execute through its configured backend identity. Slack
access rules do not provide separate per-user backend identities or hide responses
from channel readers. Configure that identity's data and tool permissions accordingly.

## Setup

1. Create/install the app using the [Slack manifest](examples/slack-app-manifest.yaml).
   Create an app-level token with `connections:write` and obtain the bot token.
   Invite the bot to each channel where it should respond.
2. Prepare a personal configuration outside the repository:

   ```sh
   install -d -m 700 "$HOME/.slack-ai-gateway/config" "$HOME/.slack-ai-gateway/db"
   cp -n examples/gateway.example.yaml "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
   chmod 600 "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
   ```

   `cp -n` preserves an existing file. Edit the placeholder Slack app, workspace
   and channel IDs, access policy, Agently URL, agent and execution principal.
   The single [runtime example](examples/gateway.example.yaml) starts with one
   selected public channel. See [access policy](docs/access-policy.md) for workspace
   routing, private channels and user restrictions, or [DM setup](docs/direct-messages.md).
3. Create or reuse the private Scy resources referenced by that configuration:

   | Configuration field | Resource contents |
   | --- | --- |
   | `integrations[].slack.app_token_secret` | App-level token beginning with `xapp-` |
   | `integrations[].slack.bot_token_secret` | Bot token beginning with `xoxb-` |
   | `integrations[].backend.auth.secret` | Basic credentials for the execution account |

   Each resource has `URL` and explicit `Key: blowfish://default` fields.
   Local resources use absolute file paths or local `file://` URLs; production
   resources can use AWS Secrets Manager. Follow [secret setup](docs/secrets.md)
   for interactive `scy secure` commands and production examples. The configured
   `principal_id` must match the authenticated Agently subject; see
   [authentication](docs/agently-contract.md).

   The runtime does not read Slack tokens, OOB references or MySQL DSNs from
   environment variables. Keep real resource locations and credentials outside
   version control, in a private configuration and secret store.
4. Build, validate, check the backend, then start:

   ```sh
   go build -o bin/slack-ai-gateway ./cmd/slack-ai-gateway
   ./bin/slack-ai-gateway validate-config --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
   ./bin/slack-ai-gateway check-agently --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
   ./bin/slack-ai-gateway run --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
   ```

`validate-config` is entirely offline. `check-agently` is an optional, bounded,
one-shot diagnostic: it logs in and verifies the session identity and protected
agent listing, without gateway storage, Slack or a conversation/query. It does
not verify downstream tool permissions.

`run` connects Slack and establishes the Viant Steward session independently.
Wait for both `slack_connection state=ready` and `agently_backend state=ready`
before testing a new question. Temporary backend or IdP failures keep Slack
connected while authentication recovers. Questions received before readiness or
during an outage get a durable notice that they were not submitted; they never
run automatically after recovery. Started requests are observed using their
original identifiers without submitting another prompt. Invalid configured
secrets, configuration, storage or Slack setup still prevent normal startup. See
[connection recovery](docs/operations.md#viant-steward-connection-recovery).

The bot needs `app_mentions:read`, `chat:write` and `channels:read`. Private channels
also need `groups:read`. Both `workspace_members_only` and
`workspace_members_and_guests` require `users:read` to verify active local accounts.
DM needs `im:read`,
`im:history` and `message.im`. Reinstall the app after changing scopes. The manifest
includes the agent experience used by optional native progress indicators.

## Configuration and storage

Always pass `--config`; there is no automatic configuration discovery. The example's
`../db/gateway.sqlite` resolves relative to the copied YAML, placing data in
`~/.slack-ai-gateway/db/`. The SQLite path does not expand `~` or environment
variables. Use absolute locations for local secret resources.
These directories are a recommended layout, not a dependency on Agently's own storage.

`run` automatically creates a missing SQLite file and its current schema, with
file mode `0600`. The parent directory must already exist. Existing databases are
checked, never automatically migrated or overwritten. `migrate` remains available
for offline initialization, including an existing empty file. This first-release
source contains one complete initial schema; earlier development database histories
are incompatible. Use a new database path if you were testing a prerelease version.
See [operations](docs/operations.md) for backups, schema checks and recovery.

For MySQL, replace the example's entire `storage` section with:

```yaml
storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true&loc=UTC'
  secret:
    URL: file:///absolute/path/mysql.json
    Key: blowfish://default
```

Scy reads the credential resource and Datly fills the username/password in the
DSN when storage opens. Configuration validation reads no secrets and makes no
connection. Provision MySQL explicitly before `run`.
The gateway does not create or migrate its MySQL schema, check its schema version,
or fall back to SQLite. See [MySQL setup and Datly verification](docs/mysql.md).

Runtime connections always use `integrations[]`, including a single bot. Each
entry groups technical Slack settings, its backend connection and independent
`access.channels` / `access.direct_messages` rules. A present surface requires
explicit `enabled`; omission disables it. The general channel rule has `scope`,
channel filters, privacy and `users.scope`. An exact `channel_overrides` entry
replaces that whole rule, with no inherited fields or lists. The channel master
switch still disables all exact rules. See the
[complete worked example](examples/access/full.yaml) and
[nine smaller access recipes](examples/access/README.md).

The former single-integration format, `access_mode`, `slack.channel_id`,
`slack.channels`, global/default user layers and old mode names are rejected.
Rewrite the private YAML before starting this version; permission-only changes
require no database migration. See [upgrading access configuration](docs/access-policy.md#offline-verification-and-upgrading).

For several bots, use [self-contained integration blocks](docs/integrations.md).
Each entry has a stable logical `name`, nested Slack settings and its complete
backend connection. Numeric assignment IDs are generated by the database.
Changing an execution assignment closes its old conversations; new top-level
messages use the new assignment. An unchanged restart retains normal recovery.

Use one gateway process per database; configured integrations share scoped storage. Stop the runtime
before operator commands. SQLite enforces this with a lifetime file lock; MySQL
deployment exclusivity is an operational requirement, not a distributed lock or HA
feature. Keep SQLite on local disk and protect database access and backups:
questions and answers are stored. The SQLite `.lock` file may remain after shutdown;
do not remove it while a process is active. Do not reuse a Slack-only smoke database
for real Agently conversations. Changing the storage configuration does not copy
existing conversations or pending work between databases.

YAML changes require restart. `slack.status.mode` selects one edited progress
message (`message`, default), a native indicator (`agent_session`), or both
(`hybrid`). `slack.status.refresh_interval_sec` controls message refresh in positive
whole seconds, default 30. Existing questions keep their saved status mode.

`slack.response.mode` selects `raw` (default), `cli_compatible`, `summary`,
`normalized_summary`, or `report_text`. All start with a link to the full Agently
response. Start with `summary` for readable rich-report summaries; `report_text`
includes authored report prose. Already-persisted replies retain their content
through retries. See [response presentation](docs/response-presentation.md).

## Documentation

| Reference | Purpose |
| --- | --- |
| [Access policy](docs/access-policy.md) | Channel/DM scopes, complete overrides and worked examples |
| [Direct messages](docs/direct-messages.md) | Slack app setup and independent DM authorization |
| [Request status](docs/request-status.md) | Progress modes, timing and failure behavior |
| [Response presentation](docs/response-presentation.md) | Answer modes and rich-content limitations |
| [Operations](docs/operations.md) | Storage initialization, diagnostics and uncertain-delivery resolution |
| [MySQL](docs/mysql.md) | Docker provisioning, MySQL configuration and Datly graph verification |
| [Multiple integrations](docs/integrations.md) | Self-contained configuration, assignment history and Agently/AG-UI backends |
| [Secrets](docs/secrets.md) | Local Scy resources, production AWS references and configuration migration |
| [Agently contract](docs/agently-contract.md) | Authentication, SDK operations and execution correlation |

## Development

Ordinary tests need no credentials or live services:

```sh
go test ./...
go test -race ./...
go vet ./...
SLACK_AI_GATEWAY_LIVE=0 AGENTLY_LIVE=0 go test -tags 'slack_live agently_live' ./internal/integration ./internal/agently
```

For an explicitly enabled Slack-only test, use the [integration test instructions](internal/integration/README.md).
Its channel and DM configurations are test fixtures with a fake backend, not
alternative runtime examples.

Live MySQL storage tests run only when explicitly configured. From `e2e/datly/`,
`endly -r=run` provisions a disposable MySQL server, regenerates the common Datly
components once, validates them against MySQL and SQLite, and runs both storage
suites. Ordinary SQLite use requires neither the Datly CLI nor Docker.
See the [Datly workflow](e2e/datly/README.md).

Default transcription/validation against the persistent local MySQL fixture
requires a one-time Endly credential setup. Follow the
[local MySQL instructions](e2e/mysql/README.md) before `endly -t=init`,
`endly -r=transcribe` or `endly -r=validate`. The isolated full E2E workflow
uses its own credentials. Gateway runtime configuration uses its own Scy
resources, independently of the Endly credential used for tooling.

Authoritative Datly contracts and their SQL assets live in [`dql/`](dql/README.md).
Transcription generates components and embedded SQL under `internal/datly/`;
application-owned sidecars and shared predicates remain alongside those
components. The storage adapter, transactional workflow and database
initialization stay in `internal/storage/`.

| Directory | Contents |
| --- | --- |
| `cmd/` | Executable entry point |
| `dql/` | Authoritative Datly contracts and adjacent source SQL assets |
| `internal/datly/` | Generated components, embedded SQL, authored sidecars and shared predicates |
| `internal/storage/` | Shared storage adapter and workflow, database initialization and storage tests |
| `internal/` | Configuration, adapters, gateway workers and remaining tests |
| `migrations/sqlite/` | Embedded current initial schema; future versioned changes belong here |
| `script/mysql/` | Explicit versioned MySQL schema |
| `e2e/` | Local MySQL provisioning and shared Datly generation/verification for both drivers |
| `examples/` | Runtime configuration and Slack app manifest |
| `docs/` | User, operator and integration references |

Local configurations, databases, build output, IDE files and `_LOCAL/` are ignored.
`_LOCAL/` is a personal archive and is not required to build or run the project.
