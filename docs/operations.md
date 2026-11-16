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
granted scopes. The report names the chosen general/override/disabled rule and
its effective user scope. See [access policy](access-policy.md) for complete-rule
selection and the one-time configuration rewrite.

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

Output is a consistent JSON metadata snapshot with five collections:
`integrations`, `gateway_jobs`, `conversation_bindings`, `slack_outgoing_messages`
and `slack_agent_session_publications`. Each job combines execution, backend
observation and Slack progress metadata. Execution and progress states/reasons
remain distinct; completing backend work is not proof of Slack delivery.

Jobs include workspace/channel/thread IDs and the original Slack author ID;
outgoing messages include the Slack address for multi-channel diagnosis.
Prompt text, result text, protocol history, rendered replies and credentials are
excluded. IDs and principal/owner names can still be sensitive operational metadata.

Counts cover five independent state dimensions:
`gateway_job.gateway_execution_state`, `gateway_job.slack_progress_state`,
`conversation_binding.gateway_binding_state`,
`slack_outgoing_message.slack_delivery_state` and
`slack_agent_session_publication.slack_publication_state`.

Progress metadata includes desired/sent revisions, first/last confirmation,
retry/refresh/cooldown dates and `slack_progress_ack_latency_ms`. Unknown latency
is `null`; a confirmed publication may have a valid zero-millisecond latency.
Session metadata distinguishes the desired agent status, Slack's observed status
for this agent and the aggregate session status. `initial_integration_id` is the
original assignment anchor, while `status_source_job_id` identifies the
job whose author supplies the publication access context.

`limit` is 1–1000, default 100, **per table**, newest rows first. `counts` covers
all rows, not only those displayed. This is not a full audit export/search tool;
older blocked rows may not be in the displayed window. Diagnostic timestamp
fields retain Unix-millisecond numbers with `_ms` suffixes; absent dates are `0`.
`status` neither changes work state nor runs recovery.

| State | Meaning / action |
|---|---|
| Job `pending` | Accepted, awaiting a worker |
| Job `processing` | Claimed local work or an in-flight transcript read |
| Job `querying` | Query in flight or durable read reconciliation scheduled |
| Job `completed` | Result or local not-submitted notice persisted; not proof of Slack delivery or backend execution |
| Job `needs_review` | Automation stopped for this query; later queries in its thread are blocked |
| Job `rejected` | Policy denied execution, or an assignment change closed local work; inspect `gateway_execution_reason` |
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
`WARN` record named `query_needs_review` with `job_id`, `gateway_request_id` and the fixed
`reason` code. It does not log question/answer text, credentials or raw backend
errors. A failure to persist the transition is surfaced as a worker error, not
logged as a successful transition. The log callback itself does not send a reply
or automatically resolve the job. Status publications independently project durable review
state into the question's status message, subject to current policy and Slack
availability. Startup recovery can also quarantine jobs;
inspect `status` for the complete persisted state.

An assignment replaced or removed at startup closes its bindings and unfinished
work with `integration_changed` or `integration_removed`. This does not cancel a
backend run. The gateway retains delivered parts and uncertain-delivery audit,
stops pending ordinary responses, and records one closure notice per unfinished
physical Slack thread. A remaining authorized transport for that bot handles the
notice and terminal progress updates; otherwise publication fails locally.
`status --integration NAME` includes inactive assignment history. See
[assignment lifecycle](integrations.md#recorded-assignments-and-restarts).

## Slack intake deadlines and acknowledgements

The source gives durable admission a one-second context budget before requesting
an acknowledgement from Slack. A failed admission whose error includes
`context.DeadlineExceeded`, whose admission budget has expired, and whose parent
source is still running produces `slack_intake_timeout`. It skips ACK for that
attempt and continues receiving through the same Socket Mode client. The record
includes `event_id`, `envelope_id`, `duration_ms`, `retry_attempt`,
`reason=accept_budget_exceeded` and `ack_state=not_sent`; it excludes the raw
storage error and message contents.

This is evidence that the admission budget expired, not proof that the database
is healthy or that a transaction rolled back. A joined error can also contain
another database cause. The native transaction outcome remains unchanged. Other
intake failures keep the existing persistence-failure handling, and a separately
reported shared storage failure can still stop the process.

The source does not replay the prompt. If the attempt committed, ordinary durable
claims or the shared reconciliation discover its job. A subsequent Slack retry
uses the existing global `slack_event_id` deduplication and acknowledges its own
envelope. If the attempt did not persist, a later retry can be admitted normally.
A successful durable admission follows the normal ACK path even when its child
context expires immediately afterward. Access checks and ignored-event handling
retain their existing behavior.

Slack retries are not an indefinitely durable queue. A long database outage can
exhaust delivery attempts; leaving an envelope unacknowledged cannot guarantee
eventual delivery. A later message may be accepted before a retry of an earlier
unaccepted message. Ordering applies to durably accepted jobs.

The native SDK queues acknowledgements before its WebSocket writer sends them.
`AckCtx` success and `slack_envelope_handled` do not prove that Slack received
the acknowledgement. The admission timer also excludes residence in the SDK's
event buffer. Investigate bursts and transport write errors separately from the
time spent in the admission handler; increasing its timeout can delay later events.

## Viant Steward connection recovery

The Slack connection and backend readiness have separate lifecycles. A backend
restart, temporary network failure or IdP outage does not terminate the gateway.
No configuration switch or schema migration is needed for this behavior.

| `agently_backend` state | Meaning / action |
| --- | --- |
| `starting` | Initial authentication/verification is in progress; transient failures retry |
| `ready` | Expected session identity, protected metadata and configured agent verified |
| `recovering` | A previously ready connection failed; the existing session is checked first |
| `blocked` | Invalid local configuration or unreadable credential resource, confirmed identity mismatch or security violation; correct the cause and restart |

Wait for both `slack_connection state=ready` and `agently_backend state=ready`
before sending a new question. `agently_backend_retry` records `stage`, `category`,
`http_status` when available, and `retry_after_ms`/`retry_at`.
`agently_backend_blocked` records a hard local, identity or security failure.
State changes also carry a safe reason/category. Raw server bodies, credentials
and question text are excluded.
For a failed OOB token exchange, the redacted reason can additionally include
`oauth_error` and `configured_auth_style`. OAuth error codes are restricted to
known protocol values, with unknown values shown as `other`. The style is the
loaded setting (`auto_detect`, `in_params` or `in_header`), not an inference about
which request AutoDetect tried first. Native AutoDetect can try two authentication
styles with the same authorization code and return the last error; an
`invalid_grant` alone does not establish the first failure's cause.
Use the provider's client configuration and available IdP diagnostics to establish
the cause before changing authentication. The existing OAuth resource supports
[native client authentication style selection](secrets.md#oauth-client-authentication-style).
Remote login/session/readiness refusals, including HTTP `401`/`403`, a rejected
password and a missing configured agent in remote metadata, are retried without
restarting the gateway. The next attempt starts 60–75 seconds after the failed
attempt finishes; a longer `Retry-After` takes precedence. A login attempt can
itself take up to 90 seconds, so this is not a fixed interval between attempt
start times. Correct the remote account, permissions or service configuration,
then wait for verified readiness. A refusal without an OAuth error code is still
a remote refusal; automatic recovery does not establish what caused the refusal.

Network failures, timeouts, `408`, `429` and server errors retain the existing
exponential backoff of approximately 1–30 seconds, with jitter and longer
`Retry-After` respected. Native session/token refresh is reused. A protected
`401` with a valid cookie keeps the existing cooldown of at least 30 seconds;
it does not immediately start another OOB login. A missing session can trigger a
new OOB login. Authentication is serialized per integration, not across the
process; other integrations can keep working while one recovers. A verified
ready transition wakes the existing scheduler. A query-specific access rejection
does not invalidate everyone else's connection.

During `starting`, `recovering` or `blocked`, accepted Slack questions finish
locally with reason `backend_unavailable` or `backend_blocked`, plus an ordinary
durable Slack reply explaining that the question was not submitted. This also
closes previously accepted but unstarted questions, including on gateway restart.
These questions **never execute automatically after recovery**; the user may
submit a new question after readiness returns. A locally completed notice is not
a successful Viant Steward execution. Deduplication, access checks, ordering and
uncertain-delivery rules still apply in every status mode, including `agent_session`.

A readiness transition closes admission before dispatching cleanup to thread
owners. Cleanup runs without holding the readiness lock, and a successful login
cannot publish `ready` until cleanup for that generation finishes. Its work is
bounded per storage command, rather than by a five-second limit for an entire
integration.

Conversation creation retains its readiness epoch. If the backend becomes
unavailable at any point during Create, even if it recovers before the response
arrives, a confirmed conversation ID is saved but that question is closed as
not submitted. The gateway obtains a fresh client and checks the epoch again
before the first query marker. An unknown Create is never repeated automatically.

Started requests retain their stored conversation/run/turn identifiers. Recovery
attaches/reads the existing request without repeating a create or query POST;
the original 15-minute reconciliation deadline is retained across outages and
restarts. Status messages explain that the connection is unavailable without
claiming that remote work stopped. Unknown conversation creation remains
`creation_unknown`; expired or otherwise uncertain execution requires review.
Already prepared final replies continue through the outbox while the backend is
unavailable. Existing `needs_review` rows, including older
`backend_auth_or_identity_rejected` records, are not automatically released.

For `blocked`, correct the local configuration or unreadable credential resource,
confirmed identity mismatch or endpoint/redirect/state/PKCE security problem,
then restart. A remote `403`
alone does not prove an identity mismatch. Every recovery attempt still verifies
the configured principal/provider and protected agent metadata; ownership and
conversation-agent checks remain mandatory. Unusable storage and Slack
authentication failures retain their startup/stop behavior. Each new OOB login
rereads its resource, but this is not general configuration hot reload.
`check-agently` is
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
`slack_delivery_resolution`. It does not resend immediately, re-render the content,
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

The process multiplexes physical-thread owners over four short database
executors. A physical key includes Slack app, workspace, channel and root
message; integration assignment history does not create a second owner for the
same thread. Policy and Create use four separate preparation slots, Slack HTTP
uses four outbound slots, and running Query/ReadTurn observations have independent
tracked lifetimes. Slow HTTP therefore does not hold a database executor or
prevent another thread from persisting intake and receipts.

Each active thread has an ordered mailbox with 64 queued commands. Backpressure
honors the caller's context: intake retains its one-second budget, including
queueing. Canceling a queued command does not release an already running native
transaction early. Owners with no queued/executing work or external continuation
references are removed without per-thread polling loops or timers.

Idle work discovery uses a read-only Datly component shared by the process.
It does not claim a job, begin the mutation workflow, or load prompt/result
bodies. Candidate hints are checked by the existing scoped transactional workers.
An empty check waits for a relevant change, a future deadline, or the shared
five-second reconciliation instead of polling every 100 milliseconds.
Committed changes wake the affected scoped workers directly, including while a
discovery query is in flight. Discovery supplies future deadlines and missed-work
reconciliation; its database cost still depends on retained history and indexes.

Datly owns bounded transaction replay. The workflow opts into one retry after a
proven rollback of its own composed transaction; HTTP is never part of that
replay. Unknown/partial completion, caller-owned transactions and cleanup
failures remain ineligible. A storage failure after the native retry still stops
the process. `storage_operation_failed` records the operation, completion state,
safe driver code and timing without SQL, prompt text or credentials.

The concurrency update requires Datly's custom capability forwarding, composed
transaction ownership proof and preservation of unannotated typed input facts.
The actual-workflow regression
`TestWorkflow022NativeRetryKeepsIntakeAndPublicationAtomic` is the dependency
release gate. Publish and pin the framework fix before a standalone gateway
release; a local workspace is suitable for cross-repository verification and
must not become a permanent module replacement.

`slack.status.refresh_interval_sec` continues to control visible message status
refreshes. It is independent of the reconciliation interval and does not control
the number of workers. Native session refreshes, one-second outbound pacing and
durable Slack rate-limit deadlines retain their existing behavior.

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
