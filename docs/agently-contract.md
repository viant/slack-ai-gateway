# Agently API and authentication contract

The gateway uses the Go client in `github.com/viant/agently-core/sdk` for native
AG-UI execution and attachment, and `sdk/slack` for completed-answer presentation.
The exact dependency and local replacements are declared in [go.mod](../go.mod).
It does not invoke the Agently CLI or change browser/CLI rendering.

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
The durable gateway `request_id` is the wire `runId`, user message ID and extension
`requestId`. Core's native turn ID comes from root
`RUN_STARTED.metadata.agently` with `identityVersion: "1"`; it need not equal those
request IDs. The store's `turn_id` retains the logical request ID.

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
and ID, excluding execution narration, internal messages and tool results. Messages
are presented separately through `sdk/slack`; see [presentation](response-presentation.md).
Failure/cancellation produces controlled text, while waiting for interaction,
inconsistent IDs or invalid responses require review. No structured approval is
automatically accepted.

## Durable observation

The binding and query-start marker are persisted before submission. After an
uncertain query or restart, the adapter verifies ownership, attaches the original
run and reads its native turn. It never resends the prompt or replaces the run.
Correlation IDs are not a backend idempotency guarantee. An uncertain conversation
creation requires review because no safe lookup/retry contract is available.

Ordinary API operations have a 120-second timeout. Execution observation has a
15-minute deadline from durable query start; attachment retries wait 2, 4, 8, then
15 seconds. These are implementation defaults, not YAML options. SSE lifetime is
owned by that deadline and runtime cancellation. Observers run outside the four
admission workers; questions within one thread remain ordered.

A timeout, disconnect or missing attachment is not proof that a prompt was rejected.
Transient uncertainty schedules further observation, never a replacement chat.
Expiry stops observation without canceling the backend. Jobs without sufficient
correlation facts or already in `needs_review` are not automatically replayed.

## Authentication

`run` requires `auth.mode: oob_session`. Login discovers the trusted server's OAuth
configuration, reads a local Scy-compatible basic credential file, performs OOB
login with PKCE/state, validates the exact callback and creates an Agently BFF
session. The callback `https://localhost/callback.html` is parsed, never followed;
there is no browser or local listener. The IdP must support this OOB form flow.

Scopes are normalized base `scopes` plus the first nonempty normalized group in
order `cliScopes`, `webUIScopes`, `mobileUIScopes`, deduplicated in order; an empty
result defaults to `openid`. Discovered scopes replace those in the referenced
OAuth configuration. This requests scopes, not role assignment or an IdP bypass.

Basic credentials must be in an absolute local file or local `file://` URL,
optionally followed by a Scy key reference. Remote/inline basic credentials are
rejected. The binary supports legacy Blowfish references; it does not register
cloud KMS/storage providers. The backend's OAuth `configURL` is trusted
configuration and can reference Scy-compatible OAuth configuration formats.

A private in-memory cookie jar holds the BFF session. The `/me` subject must match
`principal_id` exactly, with provider `session`. Before create, the session is
verified; before query/transcript reads, the owner and agent are also verified.
Cookie-authenticated mutations send the destination's scheme and authority as
`Origin`, excluding its base path. HTTPS is required outside loopback; redirects
are disabled and normal TLS verification remains enabled.

Authentication or identity rejection invalidates the clients and stops the runtime.
There is no automatic relogin, account switching or query replay. Agently owns
backend token refresh. A successful `/me` check does not prove downstream token
refresh or tool permissions. `check-agently` verifies only identity and agent
listing, and can create server authentication state but no conversation.

Login is bounded by 90 seconds overall and 30 seconds per HTTP request. JSON
support/transcript responses are limited to 8 MiB; oversized conversations require
review rather than silent truncation. Errors omit raw URLs, bodies and credentials.
There is no transcript pagination, backend cancellation or Slack interaction UI.
See [operations](operations.md) for persisted review states and delivery recovery.
