# Agently Slack Gateway

A Go service connecting Slack channels and opt-in direct messages to Agently.
Each Slack thread maps to a conversation under one configured execution identity.
Questions in a channel require a bot mention, including thread follow-ups. Direct
messages use a separate access policy and do not require mentions.

The gateway persists questions, conversation bindings, final replies and progress
state in SQLite. It reports progress while Agently works and delivers completed
answers in the selected presentation mode. Interrupted executions are observed
without resubmitting prompts; uncertain remote effects require operator review.
Answer text is not streamed to Slack.

## Requirements

- Go 1.26 or newer; macOS or Linux for the SQLite lifetime lock.
- The current `go.mod` uses sibling `../agently-core` and `../forge` checkouts.
  Keep those checkouts available when building.
- An Agently deployment with OOB/PKCE login, the configured agent and an account
  permitted to use it. Browser login and arbitrary MFA flows are not implemented.
- A Slack app installed in one workspace, with Socket Mode enabled. SQLite and
  Socket Mode are the only implemented storage and transport options.

All accepted users execute through the same Agently account. Slack access rules
do not provide separate backend identities or hide responses from channel readers.
Configure the account's data and tool permissions accordingly.

## Setup

1. Create/install the app using the [Slack manifest](examples/slack-app-manifest.yaml).
   Create an app-level token with `connections:write` and obtain the bot token.
   Invite the bot to each channel where it should respond.
2. Prepare a personal configuration outside the repository:

   ```sh
   install -d -m 700 "$HOME/.agently-slack-gateway/config" "$HOME/.agently-slack-gateway/db"
   cp -n examples/gateway.example.yaml "$HOME/.agently-slack-gateway/config/gateway.local.yaml"
   chmod 600 "$HOME/.agently-slack-gateway/config/gateway.local.yaml"
   ```

   `cp -n` preserves an existing file. Edit the placeholder Slack app, workspace
   and channel IDs, access policy, Agently URL, agent and execution principal.
   The single [runtime example](examples/gateway.example.yaml) starts with one
   selected public channel. See [access policy](docs/access-policy.md) for workspace
   routing, private channels and user restrictions, or [DM setup](docs/direct-messages.md).
3. Set the environment variables referenced by that configuration:

   | Default variable | Value |
   | --- | --- |
   | `SLACK_APP_TOKEN` | App-level token beginning with `xapp-` |
   | `SLACK_BOT_TOKEN` | Bot token beginning with `xoxb-` |
   | `AGENTLY_OOB_SECRETS` | Absolute local Scy-compatible credential-file reference |

   The credential file contains the service account's `Username` and `Password`.
   Prepare it using your secret-management tooling outside the repository. An
   encrypted reference can look like this:

   ```sh
   export AGENTLY_OOB_SECRETS='/absolute/path/agently_service.enc|blowfish://default'
   ```

   Token values and passwords belong in private environment/secret files, never
   YAML or command-line flags. The configured `principal_id` must match the
   authenticated Agently subject. See [authentication](docs/agently-contract.md).
4. Build, validate, check the backend, then start:

   ```sh
   go build -o bin/agently-slack-gateway ./cmd/agently-slack-gateway
   ./bin/agently-slack-gateway validate-config --config "$HOME/.agently-slack-gateway/config/gateway.local.yaml"
   ./bin/agently-slack-gateway check-agently --config "$HOME/.agently-slack-gateway/config/gateway.local.yaml"
   ./bin/agently-slack-gateway run --config "$HOME/.agently-slack-gateway/config/gateway.local.yaml"
   ```

`validate-config` is entirely offline. `check-agently` logs in and verifies the
session identity and agent listing, without SQLite, Slack or a conversation/query;
it does not verify downstream tool permissions. Wait for `slack_connection
state=ready` before mentioning the bot on an allowed channel.

The bot needs `app_mentions:read`, `chat:write` and `channels:read`. Private channels
also need `groups:read`; `members_only` needs `users:read`. DM needs `im:read`,
`im:history` and `message.im`. Reinstall the app after changing scopes. The manifest
includes the agent experience used by optional native progress indicators.

## Configuration and storage

Always pass `--config`; there is no automatic configuration discovery. The example's
`../db/gateway-agently.sqlite` resolves relative to the copied YAML, placing data in
`~/.agently-slack-gateway/db/`. YAML paths do not expand `~` or environment variables.
These directories are a recommended layout, not a dependency on Agently's own storage.

`run` automatically creates a missing SQLite file and its current schema, with
file mode `0600`. The parent directory must already exist. Existing databases are
checked, never automatically migrated or overwritten. `migrate` remains available
for offline initialization, including an existing empty file. This first-release
source contains one complete initial schema; earlier development database histories
are incompatible. Use a new database path if you were testing a prerelease version.
See [operations](docs/operations.md) for backups, schema checks and recovery.

Only one process may use a gateway database, including operator commands. Keep it
on local disk. Protect its directory and backups: questions and answers are stored.
The `.lock` file may remain after shutdown; do not remove it while a process is active.
Do not reuse a Slack-only smoke database for real Agently conversations.

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
| [Access policy](docs/access-policy.md) | Channel routing, user rules and inheritance examples |
| [Direct messages](docs/direct-messages.md) | Slack app setup and independent DM authorization |
| [Request status](docs/request-status.md) | Progress modes, timing and failure behavior |
| [Response presentation](docs/response-presentation.md) | Answer modes and rich-content limitations |
| [Operations](docs/operations.md) | SQLite initialization, diagnostics and uncertain-delivery resolution |
| [Agently contract](docs/agently-contract.md) | Authentication, SDK operations and execution correlation |

## Development

Ordinary tests need no credentials or live services:

```sh
go test ./...
go test -race ./...
go vet ./...
AGENTLY_SLACK_LIVE=0 AGENTLY_LIVE=0 go test -tags 'slack_live agently_live' ./internal/integration ./internal/agently
```

For an explicitly enabled Slack-only test, use the [integration test instructions](internal/integration/README.md).
Its channel and DM configurations are test fixtures with a fake backend, not
alternative runtime examples.

| Directory | Contents |
| --- | --- |
| `cmd/` | Executable entry point |
| `internal/` | Configuration, adapters, gateway workers, SQLite store and tests |
| `migrations/sqlite/` | Embedded current initial schema; future versioned changes belong here |
| `examples/` | Runtime configuration and Slack app manifest |
| `docs/` | User, operator and integration references |

Local configurations, databases, build output, IDE files and `_LOCAL/` are ignored.
`_LOCAL/` is a personal archive and is not required to build or run the project.
