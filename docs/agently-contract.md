# Agently API and authentication contract

This document describes an integration with `backend.type: agently`. Its native
conversation, BFF/OOB authentication, transcript and durable attachment APIs are
Agently-specific. A protocol-only `backend.type: agui` has a separate bearer or
loopback-only unauthenticated contract; see [generic AG-UI](integrations.md#generic-ag-ui).

The gateway uses the Go client in `github.com/viant/agently-core/sdk` for native
AG-UI execution and attachment, and `sdk/slack` for completed-answer presentation.
The selected SDK version is declared in [go.mod](../go.mod). It does not invoke
the Agently CLI or change browser/CLI rendering.

## Configuration scope

Current settings belong to an `integrations[]` entry: `backend.base_url`,
`backend.agent_id`, `backend.principal_id` and `backend.auth`. Each integration
has its own independent `access.channels` and `access.direct_messages` policies
and shared backend execution identity. Only complete `integrations[]` entries
are supported; former top-level `agently` settings are rejected. See
[multiple integrations](integrations.md) and [access policy](access-policy.md).

## Operations and identities

| Operation | Request | Expected response |
| --- | --- | --- |
| Discover OAuth configuration | `GET /v1/api/auth/oauth/config` | `configURL`, base `scopes`, `cliScopes`, `webUIScopes`, `mobileUIScopes` |
| Establish BFF session | `POST /v1/api/auth/session` | Session cookie; token bundle in request, no username override |
| Verify effective identity | `GET /v1/api/auth/me` | Configured `subject`, `provider: session` |
| Verify agent listing | `GET /v1/workspace/metadata` | Configured agent in `agentInfos[].id` |
| Create conversation | `POST /v1/conversations`, JSON `agentId` | Nonempty conversation ID |
| Verify ownership and wire mapping | `GET /v1/conversations/{id}?includeTranscript=false` | Matching `id`, `createdByUserId`, `agentId`; optional `aguiThreadId` |
| Submit chat once | `POST /v1/ag-ui/run`, `Accept: text/event-stream` | Root `RUN_STARTED` with matching thread/run, protocol `1.0`, native turn identity |
| Observe an admitted run | Same endpoint, `operation: run.attach`, empty `messages` | Replay of the exact run without a new prompt |
| Read the canonical turn | `GET /v1/conversations/{id}/transcript` | Matching conversation and exactly one matching native turn |

The native conversation ID identifies metadata and transcript resources. The wire
`threadId` comes from authenticated `aguiThreadId`, falling back to the conversation
ID only when that field is absent/null; an explicitly empty mapping is rejected.
The durable `gateway_request_id` is the wire `runId`, the new wire user message
ID (`messages[0].id`) and extension `requestId`. Native persisted message and turn
IDs are separate identities. Core's native turn ID comes from root
`RUN_STARTED.metadata.agently` with `identityVersion: "1"`; it need not equal those
request IDs. The store records `backend_submission_started_at` before submission;
it does not retain a second copy of the gateway request ID as `turn_id`.

Chat sends only the new user message, an `agently` version-1 extension with
operation `chat`, and payload `agentId`, `useServerState: true`. Core owns history.
No user, owner, client-state, tools or parent-run override is supplied.

The SDK owns SSE parsing and observes the root run. Child-run completion does not
finish the observer. After root completion, the adapter immediately reads the exact
canonical turn. A completed canonical turn found during replay can finish earlier.
The latest turn, matching prompt text and `startedByMessageId` are not substitutes
for native turn identity. Stream narration and deltas are never Slack final text.

Only canonical `completed` permits final text: `assistant.final.content`, or, when
that final object is absent, the last matching execution page marked `finalResponse`.
The SDK also selects that turn's public standalone assistant messages by ownership
and ID, excluding execution narration, internal messages and tool results. Each
public message is projected independently through `sdk/slack`, then combined and
chunked for Slack delivery; one backend message does not imply one Slack post. See
[presentation](response-presentation.md).

Agently's canonical transcript projection maps stored success states such as
`succeeded` to `completed`; the gateway requires that canonical status.
Previously produced text or a visible report is not proof of that
terminal state: an execution can produce content and subsequently fail.
Failure/cancellation produces controlled text, while waiting for interaction,
inconsistent IDs or invalid responses require review. No structured approval is
automatically accepted.

## Durable observation

The binding and query-start marker are persisted before submission. After an
uncertain query or restart, the adapter verifies ownership, attaches the original
run and reads its native turn. It never resends the prompt or replaces the run.
Correlation IDs are not a backend idempotency guarantee. An uncertain conversation
creation requires review because no safe lookup/retry contract is available.

The local `gateway_job` row combines execution and progress facts. Its numeric `id`
links outgoing Slack messages, while `gateway_request_id` remains the separate
128-bit random value encoded as 32 hexadecimal characters. It is not a formatted
UUID, and the database additionally enforces uniqueness. Renaming storage columns
does not rename AG-UI's `runId`, `threadId` or the Agently extension fields.

The schema is prepared for future partial content cleanup. Agently retains history
on the server through `useServerState: true`; the protocol-only AG-UI adapter sends
its saved `backend_protocol_messages` and `backend_protocol_state` on the next run.
Those opaque protocol values must remain while preserving continuation, even if
redundant prompt, result and outgoing-message bodies are cleared. Full local
conversation removal also removes its mapping and deduplication facts; a later
prompt in the old Slack thread can then start a new backend conversation. No
retention process currently performs either cleanup. See [the storage schema](schema.md).

Ordinary API operations have a 120-second timeout. Execution observation has a
15-minute deadline from durable query start; attachment retries wait 2, 4, 8, then
15 seconds. These are implementation defaults, not YAML options. SSE lifetime is
owned by that deadline and runtime cancellation. Each integration has four
admission workers; execution observers run outside them, and questions within
one thread remain ordered. The original persisted deadline survives restart.
While the gateway is stopped it cannot publish timeout transitions; after restart,
an already expired job requires review before further execution observation.

A timeout, disconnect or missing attachment is not proof that a prompt was rejected.
Transient uncertainty schedules further observation, never a replacement chat.
Expiry stops observation without canceling the backend. Jobs without sufficient
correlation facts or already in `needs_review` are not automatically replayed.

## Authentication

An Agently integration requires `backend.auth.mode: oob_session`. Login discovers
the trusted server's OAuth configuration, loads the configured Scy basic credential
resource, performs OOB login with PKCE/state, validates the exact callback and
creates an Agently BFF session. The callback `https://localhost/callback.html`
is parsed, never followed;
there is no browser or local listener. The IdP must support this OOB form flow.

Scopes are normalized base `scopes` plus the first nonempty normalized group in
order `cliScopes`, `webUIScopes`, `mobileUIScopes`, deduplicated in order; an empty
result defaults to `openid`. Discovered scopes replace those in the referenced
OAuth configuration. This requests scopes, not role assignment or an IdP bypass.

`integrations[].backend.auth.secret` supplies a native Scy resource with `URL`
and `Key` fields. Basic credentials use an absolute local file, a local `file://`
URL or an AWS Secrets Manager resource. Arbitrary HTTP/inline OOB credential locations are
rejected. The binary registers the AWS storage provider and Blowfish support;
examples explicitly select `blowfish://default`. See [secret setup](secrets.md)
for local creation, production access and rotation. The backend's OAuth
`configURL` remains trusted discovery configuration and uses its existing
Scy-compatible OAuth formats; it is not overridden by the gateway YAML.

A private in-memory cookie jar holds the BFF session. The `/me` subject must match
`principal_id` exactly, with provider `session`. Before create, the session is
verified. Before each query or reattachment, the conversation owner and agent
are also verified; canonical transcript reads then belong to that verified
observation. Ownership lookup is not repeated before every transcript GET.
Cookie-authenticated mutations send the destination's scheme and authority as
`Origin`, excluding its base path. HTTPS is required outside loopback; redirects
are disabled and normal TLS verification remains enabled.

Each Agently integration has a connection manager that shares authentication
across its workers and publishes `starting`, `ready`, `recovering` or `blocked`.
Slack connectivity and outbound delivery continue independently. Readiness requires
the expected `/me` subject and session provider, protected workspace metadata and
the configured agent. Healthy sessions are probed every 30 seconds. A successful `/me` alone
does not establish protected API access or downstream tool permissions.

Recovery first verifies the existing cookie session, allowing Agently's native
token refresh to run. A protected `401` retains the native refresh cooldown of
at least 30 seconds; it does not immediately repeat OOB login. A new OOB flow is
attempted only when `/me` confirms the cookie session is missing. Authentication
is serialized, and stale worker clients cannot invalidate a newer verified
generation. A query-specific `403` remains a local access/review error.

Network failures, timeouts, `408`, `429` and server errors retry verification or
authentication with exponential intervals around 1, 2, 4, 8, 16 and 30 seconds,
with jitter and any longer `Retry-After` respected. Each attempt is bounded by
90 seconds. Invalid credentials, identity mismatches and other permanent
authentication failures enter `blocked`; correct the cause and restart the
gateway. Offline `validate-config` checks the declared authentication mode and
native resource shape (including a nonempty URL); it does not load credentials
or verify the referenced location. Runtime setup/login checks the supported
file/AWS source, decoding and readability. Unreadable backend credentials block
that integration's execution while its authorized Slack transport can still
deliver notices and end historical progress.
Each new OOB login reads its configured resource again.
Slack token and MySQL credential rotation require restart; no account switching
or general configuration hot reload is provided.

Recovery never repeats conversation creation or a chat POST. Unsubmitted questions
finish locally with a durable Slack notice and never run after recovery. Started
requests attach/read using the original correlation and 15-minute deadline;
uncertain creation still requires review. These behaviors are implemented only
in the gateway; Core session, refresh and SDK contracts are unchanged.

`check-agently` remains a bounded, one-shot identity and protected agent-listing
diagnostic. `--integration NAME` selects an Agently integration; when omitted,
all configured Agently integrations are checked. The diagnostic can use named
backend entries without Slack or storage settings; it still requires the
`integrations[]` syntax. It can create server
authentication state but no conversation; it does not start the recovery loop or
contact Slack.

Login is bounded by 90 seconds overall and 30 seconds per HTTP request. JSON
support/transcript responses are limited to 8 MiB; oversized conversations require
review rather than silent truncation. Errors omit raw URLs, bodies and credentials.
There is no transcript pagination, backend cancellation or Slack interaction UI.
See [operations](operations.md) for persisted review states and delivery recovery.
