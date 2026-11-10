# Gateway operator commands

The first release has one complete initial schema for inbox/outbox storage,
execution observation and progress publications. SQLite schema history and checksums
are validated; future SQLite changes can use the existing migration runner.
MySQL uses an explicitly provisioned versioned DDL; startup performs no DDL or
schema-version checks. See [MySQL setup](mysql.md).
See [request status](request-status.md) and [DM setup](direct-messages.md).

SQLite and MySQL use the same Datly storage components and operator behavior.
Selecting a driver changes the connection and database preparation; the generated
components do not need to be rebuilt. SQLite retains its file lock and migration
history, while MySQL retains explicit provisioning and deployment exclusivity.

## Validate configuration without storage or secrets

```sh
go run ./cmd/slack-ai-gateway validate-config --config examples/gateway.example.yaml --channel-id C_TEST
```

This command needs the complete runtime configuration, not just storage. It is
entirely offline: no database, lock, token loading, login or API calls, including
when the runtime is active. It reports static policy, not verified membership or
granted scopes. See [access policy](access-policy.md) for inheritance and legacy
compatibility.

## SQLite commands

Run the SQLite commands below from the gateway repository. They operate only on the
gateway's SQLite database; they do not connect to Slack or Agently. Only the
`storage` configuration is needed. A minimal example is:

```yaml
storage:
  driver: sqlite
  path: ../db/gateway.sqlite
```

With YAML in `~/.slack-ai-gateway/config/`, this path places SQLite in the
application's `db/` directory. Relative database paths are resolved against the
configuration file; a full absolute path is also supported. `~` and environment
variables are not expanded in `storage.path`. See [local setup](../README.md#setup)
for copying a public example and explicitly creating these directories.
The gateway does not create parent directories. For a new installation, create
the configured parent directory explicitly, then use `run`: if the database file
is missing, it creates the file with mode `0600` and initializes the current schema
using the embedded migrations while holding the lifetime lock. It reports the full
path after initialization. This happens before loading credentials or connecting
to Agently or Slack, so a later authentication failure can leave an initialized
database ready for the next run.

Existing files must have a current, compatible gateway schema. `run` does not
initialize an existing empty file or upgrade an older schema. Use `migrate` for
an empty file; earlier development histories require a new database path. `migrate`
can also create a database offline using only storage configuration. `status` and
`resolve-delivery` require an existing current database and never initialize one.

These commands acquire the same lifetime lock as the runtime:
**stop the gateway or live test first**.
Do not remove the lock file to bypass a running process. Protect the database,
its directory and backups: questions and answers are stored even though `status`
does not print them.

## Initialize a database offline

`run` initializes a missing file automatically. To initialize without credentials
or network access, or to initialize an existing empty file:

```sh
go run ./cmd/slack-ai-gateway migrate --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml"
```

The current initial schema replaces the unpublished development migration history.
It does not upgrade those development databases: use a new configured database
path if conversation continuity is unnecessary, retaining any old files as backups.
Incompatible or unrelated databases are rejected, not erased or reset.

Take consistent backups with no process using the database, using SQLite backup
facilities or preserving the complete database/WAL family. Do not copy only the
main file while writes are active. New files have mode `0600`; existing permissions
are not silently changed. Never point these commands at Agently's database.

## Inspect state

`status` and `resolve-delivery` support both storage drivers and do not contact
Slack or Agently. Stop the gateway first. For MySQL, configure `storage.dsn` and
its optional Scy `storage.secret`; these commands load that storage resource when
opening the database and never create a schema.

```sh
go run ./cmd/slack-ai-gateway status --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml" --limit 100
```

Output is a consistent JSON snapshot of jobs, conversation bindings and deliveries,
containing IDs, states, fixed reason codes, poll metadata and Slack timestamps.
Jobs include workspace/channel/thread IDs and the original Slack author ID;
deliveries include workspace/channel/thread IDs for multi-channel diagnosis.
Question text, result text, rendered replies and credentials are excluded. IDs and
principal/owner names can still be sensitive operational metadata.
`request_statuses` additionally reports publication phase/state, backend outcome,
last confirmation, desired/sent revisions, retry/refresh/cooldown metadata,
acknowledgment latency and fixed error reasons. Counts include `request_status`.

`limit` is 1–1000, default 100, **per table**, newest rows first. `counts` covers
all rows, not only those displayed. This is not a full audit export/search tool;
older blocked rows may not be in the displayed window. Millisecond timestamps
are Unix time. `status` neither changes work state nor runs recovery.

| State | Meaning / action |
|---|---|
| Job `pending` | Accepted, awaiting a worker |
| Job `processing` | Claimed local work or an in-flight transcript read |
| Job `querying` | Query in flight or durable read reconciliation scheduled |
| Job `completed` | Result or local not-submitted notice persisted; not proof of Slack delivery or backend execution |
| Job `needs_review` | Automation stopped for this query; later queries in its thread are blocked |
| Job `rejected` | Current policy denied execution before a turn was submitted |
| Delivery `pending` | Stored reply awaiting sending or a known-safe retry |
| Delivery `sending` | Attempt started; crash recovery makes this `delivery_unknown` |
| Delivery `delivery_unknown` | Slack may have received it; manual resolution required |
| Delivery `failed` | Known permanent rejection; no automatic retry |
| Delivery `delivered` | Confirmed send, or operator assertion with a Slack timestamp |

Examples of review reasons are `creation_unknown`, `reconciliation_deadline`,
`backend_protocol_or_access_error`, `turn_identity_mismatch` and
`interaction_requires_review`. A query timeout/deadline does not cancel backend
execution. Do not send the same question again as an attempted recovery.
Other reasons include `policy_denied`/`policy_unavailable` and `slack_auth_rejected`.
A denied queued query is rejected without execution; an already submitted turn
is quarantined without polling or replay when its policy no longer permits it.
Slack auth rejection also stops the runtime. A missing scope/metadata failure
before a query requires review; adding the scope does not automatically release it.
There is no CLI to resolve `needs_review` or permanent `failed` deliveries yet.
Investigate the recorded IDs and backend state before designing a recovery action;
do not delete rows, alter IDs or issue ad-hoc SQL to unblock the thread.

When a query worker successfully persists `needs_review`, the runtime emits a
`WARN` record named `query_needs_review` with `job_id`, `request_id` and the fixed
`reason` code. It does not log question/answer text, credentials or raw backend
errors. A failure to persist the transition is surfaced as a worker error, not
logged as a successful transition. The log callback itself does not send a reply
or automatically resolve the job. Status publications independently project durable review
state into the question's status message, subject to current policy and Slack
availability. Startup recovery can also quarantine jobs;
inspect `status` for the complete persisted state.

## Viant Steward connection recovery

The Slack connection and backend readiness have separate lifecycles. A backend
restart, temporary network failure or IdP outage does not terminate the gateway.
No configuration switch or schema migration is needed for this behavior.

| `agently_backend` state | Meaning / action |
| --- | --- |
| `starting` | Initial authentication/verification is in progress; transient failures retry |
| `ready` | Expected session identity, protected metadata and configured agent verified |
| `recovering` | A previously ready connection failed; the existing session is checked first |
| `blocked` | Permanent authentication/identity problem; correct the cause and restart |

Wait for both `slack_connection state=ready` and `agently_backend state=ready`
before sending a new question. `agently_backend_retry` logs the fixed error
category and `retry_after_ms`; `agently_backend_blocked` records a redacted
permanent failure. Raw server bodies, credentials and question text are excluded.
Retry intervals grow to approximately 30 seconds, with jitter and longer
`Retry-After` delays respected. Native session/token refresh is reused. A missing
session can trigger a new OOB login; a query-specific access rejection does not
invalidate everyone else's connection.

During `starting`, `recovering` or `blocked`, accepted Slack questions finish
locally with reason `backend_unavailable` or `backend_blocked`, plus an ordinary
durable Slack reply explaining that the question was not submitted. This also
closes previously accepted but unstarted questions, including on gateway restart.
These questions **never execute automatically after recovery**; the user may
submit a new question after readiness returns. A locally completed notice is not
a successful Viant Steward execution. Deduplication, access checks, ordering and
uncertain-delivery rules still apply in every status mode, including `agent_session`.

Started requests retain their stored conversation/run/turn identifiers. Recovery
attaches/reads the existing request without repeating a create or query POST;
the original 15-minute reconciliation deadline is retained across outages and
restarts. Status messages explain that the connection is unavailable without
claiming that remote work stopped. Unknown conversation creation remains
`creation_unknown`; expired or otherwise uncertain execution requires review.
Already prepared final replies continue through the outbox while the backend is
unavailable. Existing `needs_review` rows, including older
`backend_auth_or_identity_rejected` records, are not automatically released.

For `blocked`, check the configured identity, account credentials, IdP and agent
permissions, correct the cause, then restart. There is no credential hot reload.
Invalid configured secrets, invalid static configuration, unusable storage and Slack
authentication failures retain their startup/stop behavior. `check-agently` is
a bounded one-shot diagnostic and exits on a failed check; it does not start the
runtime's recovery loop. It is optional before `run`.

## Resolve one uncertain delivery

First inspect its `delivery-id`, `job_id` and binding to identify the exact channel,
thread and reply part. Check the message in Slack manually. The command below
does **not** look up Slack or verify that a timestamp really belongs to that part.
Use the reply's timestamp, not the root thread timestamp.

If the reply is visibly present and verified:

```sh
go run ./cmd/slack-ai-gateway resolve-delivery \
  --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml" \
  --delivery-id 123 --action delivered --slack-ts '1790882981.053349'
```

The ID and timestamp above are illustrative; replace them with verified values.
This marks only that `delivery_unknown` part delivered and records an audit row.
It does not post a message. Subsequent parts become eligible for normal delivery
when the runtime resumes, subject to other ordering/rate-limit constraints.

If a retry is explicitly chosen after investigation, accept the duplicate risk:

```sh
go run ./cmd/slack-ai-gateway resolve-delivery \
  --config "$HOME/.slack-ai-gateway/config/gateway.local.yaml" \
  --delivery-id 123 --action retry --confirm-duplicate-risk
```

Slack might already contain the original message: **this can produce a duplicate**.
The command queues exactly the existing stored part and records the decision in
`delivery_resolutions`. It does not resend immediately, re-render the content,
re-run Agently or affect another part. Actual retry happens in a subsequent runtime
session. Repeated resolution of a part no longer in `delivery_unknown` is rejected.
The state update and audit row commit together. Audit records contain the delivery,
action, timestamp, time and risk confirmation, not an authenticated operator ID;
filesystem access for SQLite, or database credentials and grants for MySQL,
remain the administrative boundary for these operator commands.

The gateway rechecks the original author's current policy before sending or retrying a part.
Policy denial produces `failed` with no POST. Unavailable metadata or rejected
Slack credentials before sending leave the exact part `pending` and stop the
runtime, not `delivery_unknown`. Resolve the configuration/scope issue before
restarting; no uncertain-delivery resolution is needed when no send happened.
Channel facts are refreshed per reply job; roles may be cached for 60 seconds.
Changing policy does not rewrite old bindings, un-fail denied deliveries, or
recover a `needs_review` turn. There is still no CLI for those state transitions.

To change routing, stop normally, preserve/backup the database, edit YAML and
validate offline, then restart. Removed-channel
history is retained and no longer blocks startup; pending work is checked against
the new policy. Wrong app/workspace/principal/owner and smoke data remain fatal.
The one-second sender pacing and durable 429 cooldown apply across all channels.

## Current execution boundary

Unlike the storage operator commands, `check-agently` contacts the IdP and Agently
to create and validate an authentication session. It does not use gateway storage
or Slack, and does not create/query a conversation. `run` independently connects
Slack and recovers backend readiness, then executes admitted real queries and
durable deliveries. Neither command
connects directly to Agently's database. Agently retains control of its own storage.

Verify live failure/recovery scenarios and downstream tool permissions in the
intended environment. A successful restart after a completed reply does not verify
recovery during an in-flight request.
See [the API contract](agently-contract.md), [request status](request-status.md)
and [integration test instructions](../internal/integration/README.md).
