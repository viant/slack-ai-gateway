# Multiple integrations

One gateway process owns one database and can run several independent integrations.
Each configuration entry describes its complete Slack-to-backend connection.
Agently and protocol-only AG-UI backends can run in the same process. SQLite and
MySQL use the same native Datly storage workflow.

See the [complete multi-integration example](../examples/gateway.multi.example.yaml).
The original single-integration syntax remains supported and normalizes to the
logical name `default`. Do not combine its top-level `slack`, `agently`,
`transport`, `access_mode` or `access` settings with `integrations`.
The earlier four-dictionary prerelease format is no longer accepted.

## Configuration

Each item in `integrations` has:

- `name`: a stable logical identifier, unique within the configuration, with at
  most 128 bytes and no whitespace. Numeric database IDs are assigned internally.
- `transport`, `access_mode`, `access` and `slack`: the existing transport,
  channel/DM routing, user policy, credential and presentation settings.
- `backend`: `type`, execution connection, `principal_id`, authentication and
  backend agent selector where needed. `type: agently` uses `base_url`, required
  `agent_id`, and the existing `oob_session` credential resource. `type: agui`
  uses `endpoint`, a local `principal_id`, and explicit `auth.mode: bearer` with a
  Scy secret, or `auth.mode: none` on loopback. Generic AG-UI need not specify an
  Agently agent ID.

Several entries can repeat the same backend connection settings with different
agent selectors. There is no separate backend or agent dictionary to maintain.
List order never selects an agent or changes existing work. Credential values
are loaded through existing resources and never saved in the integration table.

The current Socket Mode runtime requires a distinct `app_id` for each active
entry. Shared-bot routing, menus and automatic selection among multiple agents
behind one Slack app are not implemented. Channel intake requires the bot mention;
DM intake does not. Both use a main message and thread replies for conversation
mapping.

An explicitly present empty list is meaningful:

```yaml
storage:
  driver: sqlite
  path: gateway.sqlite
integrations: []
```

It deactivates the recorded assignments, closes unfinished work locally, and
exits successfully without opening Slack/backend clients. It cannot notify old
Slack threads without a remaining authorized Slack transport. Omitting the list
selects the legacy syntax; `integrations: null` is invalid.

## Recorded assignments and restarts

The `integration` table stores non-secret snapshots with an automatically
assigned numeric `id`, logical `name`, `active`, Slack app/workspace, backend
connection, backend agent selector and execution principal.

At startup the gateway validates the whole configuration, then reconciles it in
one storage transaction before intake or execution workers start:

- An unchanged, currently active assignment keeps its numeric ID.
- A changed agent, backend, execution principal or Slack app/workspace creates a
  new assignment. The previous row becomes inactive.
- Removing a name deactivates its previous assignment.
- Returning to an inactive historical assignment creates a new row rather than
  reviving old conversations.
- Changing credential resources, channel/user policies, response presentation or
  status refresh settings alone does not create another assignment.

`active` records configuration membership, not process or backend availability.
It is not an independent SQL administration switch. Several historical rows may
have the same logical name.

An ordinary restart keeps the existing recovery policy: started Agently runs can
be observed through their original identifiers without sending the prompt again.
Previously unsubmitted work is handled by the existing outage notice behavior;
it is not silently replayed. An ID saved before a backend call records a possible
submission, not proof that the backend received the request.

Replacing or removing an assignment closes its bindings and unfinished jobs.
Pending response parts stop; already delivered IDs and uncertain-delivery audit
facts remain. The gateway does not restart the old backend to finish them or
transfer their conversation to a different agent. Local closure does not cancel
external execution automatically.

Where an authorized current transport can still reach the same Slack app and
workspace, the existing outbound queue publishes a final notice and ends known
progress messages/native activity. One notice is recorded per unfinished physical
thread. Without such access, local closure is still final and diagnostics explain
why Slack publication was unavailable.

A new prompt in a closed old thread receives a local explanation to start a new
thread, without another backend conversation or query. A new top-level message
uses the current active assignment.

## Messages, deduplication and native status

Each accepted prompt creates one numeric `gateway_job.id`. Its `slack_event_id` is
unique across the gateway database, so Slack retry after a configuration change
cannot create a second job under a new assignment ID. Independent events must
have independent event IDs.

Bindings are unique by assignment, channel and `slack_thread_root_message_id`.
They own `backend_conversation_id`, execution identity, and generic AG-UI
messages/state. Replies reference their original job; their destination is read
through the job and recorded assignment. Slack message IDs remain opaque text,
even though the wire API calls them `ts` and `thread_ts`.

The stable `gateway_request_id` correlates the backend run with its prompt.
`backend_submission_started_at` is written before the external call and replaces
the old duplicated `turn_id` marker. `NULL` means no submission attempt began;
a populated timestamp means an attempt may already have reached the backend.
Backend replies still have to match the original request identity.

Native Slack status belongs to the physical app/workspace/channel/thread across
assignment changes. Its original assignment is an anchor; source/latest jobs may
belong to another assignment of that same physical Slack thread. Closed work does
not keep the agent working indefinitely. Message progress, notices and native
status retain the existing access policy, pacing and retry rules.

## Runtime and diagnostics

```sh
go run ./cmd/slack-ai-gateway validate-config --config /absolute/path/gateway.yaml
go run ./cmd/slack-ai-gateway run --config /absolute/path/gateway.yaml
go run ./cmd/slack-ai-gateway status --config /absolute/path/gateway.yaml
go run ./cmd/slack-ai-gateway status --config /absolute/path/gateway.yaml --integration steward
go run ./cmd/slack-ai-gateway check-agently --config /absolute/path/gateway.yaml --integration steward
```

`validate-config` resolves and checks configuration offline, without storage,
credential loading or network calls. It reports each integration's effective
access and presentation settings. `--integration NAME` selects a logical name;
`--channel-id` retains its policy diagnostic meaning. `check-agently` checks the
selected configured Agently entry, or all configured Agently entries when omitted.
It does not submit a prompt.

`status --integration NAME` includes recorded history for that name, including
inactive assignments. Diagnostics distinguish logical name, numeric assignment ID
and activity. `resolve-delivery` also accepts the logical name, remains offline,
and retains its explicit delivery confirmation and duplicate-risk requirements.
An old uncertain delivery can be confirmed for audit; retrying an ordinary response
must not reactivate an inactive conversation. Stop the runtime before operator
commands.

Each active entry has its own backend readiness, access policy and lifecycle.
A Slack connection or credential failure stops that integration while others
continue. A common storage failure stops the process.

## Generic AG-UI

Generic AG-UI uses the native SDK run transport and protocol state projection.
The gateway retains messages/state for subsequent turns, publishes completed
root-run output, and keeps child output/tool data separate from Slack text.
The endpoint owns agent selection. Base AG-UI has no standard `agent_id` wire
field; this adapter does not forward that optional configuration metadata as a
producer-specific selection extension.

The pinned SDK supports the `/v1/ag-ui/run` execution path. Other paths require
native configurable-endpoint support; the gateway does not rewrite URLs.

A disconnect after submission may leave the backend's outcome unknown. Base AG-UI
has no Agently-equivalent durable attachment or canonical turn lookup. Existing
review behavior remains for an unchanged active integration; the prompt is not
repeated. Deactivation closes that local work according to the rules above.
Frontend tool execution, approval UI and cancellation remain separate capabilities.
