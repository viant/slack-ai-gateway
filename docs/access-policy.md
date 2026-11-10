# Channel routing and access policy

Socket Mode is the only implemented transport. One installation serves one app,
one Slack workspace, one Agently backend/agent and one shared principal/owner.
Routing changes neither conversation ownership nor downstream tool permissions.

## Configuration contract

Keep the storage, transport, token-reference and Agently sections from
[gateway.example.yaml](../examples/gateway.example.yaml). Replace only `slack.channels`
and `access` with the patterns below. Use Slack IDs, not names.
Set `agently.agent_id` to the ID of the agent this gateway instance should use.

`slack.channels.mode: selected` requires an explicit channel `allowlist`. An empty
list routes no channels. `workspace` accepts any eligible channel in the configured
workspace and forbids an allowlist, even `[]`. Either mode supports a `denylist`;
denied channels cannot be reopened by an override. A selected-mode override outside
the allowlist is a configuration error. Private channels require `allow_private:
true`; the default is false. The bot must already belong to a channel. The gateway
does not discover/join/create channels or change invitations.

`access.global` contains optional hard user limits, **not an access grant**.
`access.defaults` is an optional complete baseline; it requires `users.mode`.
`access.channel_overrides` replaces explicitly provided local fields for that
channel. With no defaults, each override must specify `users.mode`. A channel
without defaults or a complete override is denied, even when routed.

| Field | Omitted | Explicit value |
|---|---|---|
| Local `users.mode` in an override | Inherit defaults | Replace default mode |
| Local user allowlist | Inherit defaults; otherwise unrestricted IDs | Replace default list; `[]` allows nobody |
| Local user denylist | Inherit defaults; otherwise no local exclusions | Replace default list; `[]` clears only local exclusions |
| Global user allowlist | No hard ID cap | Intersect with local allowlist; `[]` allows nobody |
| Global user denylist | No global exclusions | Union with effective local denylist; denial wins |

Modes:

- `channel_members`: verified human `app_mention` events may be submitted,
  including local-workspace guests. Slack has already allowed the author to post
  the event. No `users.info` or fresh author-membership lookup is made. Channel
  metadata verifies the **bot's** membership, not the author's current membership.
- `members_only`: additionally use `users.info` to verify a local-workspace or
  Enterprise workspace member who is active, non-bot and neither guest type.
  This is a Slack account classification, **not proof of employment**.

Allowlisting a guest does not override `members_only`; choose `channel_members`
for channels where guests should be able to ask. Local guest eligibility does
not enable external-company/Slack Connect users. MPIM, archived, externally
shared and pending-externally-shared channels are denied in both modes. DM is a
separate opt-in policy; channel defaults/overrides never enable it. See
[direct messages](direct-messages.md). A public
channel is not automatically eligible: routing, policy and bot membership all apply.

Security sections reject `null`; use omission for inheritance and `[]` for empty
lists. Unknown fields/modes, duplicates, blank/whitespace IDs and empty channel
policies are errors. YAML aliases and merges are rejected throughout the document
so they cannot bypass strict fields/presence checks. Rules are compiled into
immutable copies; applying a changed YAML requires a restart, not hot reload.

## 1. One channel, explicit policy only

No global or defaults are needed. Every channel can have an independent policy.

```yaml
slack:
  app_id: A_COMPANY
  workspace_id: T_COMPANY
  channels:
    mode: selected
    allowlist: [C_SUPPORT]
access:
  channel_overrides:
    C_SUPPORT:
      users:
        mode: channel_members
```

Only C_SUPPORT is routed. It accepts verified human mentions, including local
guests. To add another channel, allowlist it and define its own complete policy;
allowlisting it alone does not open access.

## 2. Selected channels with defaults and exceptions

```yaml
slack:
  app_id: A_COMPANY
  workspace_id: T_COMPANY
  channels:
    mode: selected
    allowlist: [C_SUPPORT, C_INTERNAL, C_RESTRICTED]
    allow_private: false
access:
  global:
    users:
      denylist: [U_BLOCKED]
  defaults:
    users:
      mode: channel_members
  channel_overrides:
    C_INTERNAL:
      users:
        mode: members_only
    C_RESTRICTED:
      users:
        allowlist: [U_ALICE, U_BOB]
```

C_SUPPORT permits local guests; C_INTERNAL requires non-guest Slack membership.
C_RESTRICTED inherits `channel_members` but only Alice/Bob can ask. U_BLOCKED is
denied everywhere, even if later included in a channel allowlist.

## 3. Eligible workspace channels with hard global limits

```yaml
slack:
  app_id: A_COMPANY
  workspace_id: T_COMPANY
  channels:
    mode: workspace
    denylist: [C_SENSITIVE]
    allow_private: false
access:
  global:
    users:
      allowlist: [U_ALICE, U_BOB, U_CAROL]
      denylist: [U_BOB]
  defaults:
    users:
      mode: members_only
```

Inviting the bot to a new public channel makes that channel eligible without
editing its ID into YAML. Only active, non-guest Alice/Carol can submit; Bob is
denied despite being allowlisted. C_SENSITIVE is denied entirely. Opening this
scope needs a deliberate review of the shared principal's data/tool permissions.

## 4. Override replacement, empty lists and a non-overridable global cap

```yaml
slack:
  app_id: A_COMPANY
  workspace_id: T_COMPANY
  channels:
    mode: selected
    allowlist: [C_SUPPORT, C_OVERRIDE, C_DISABLED]
access:
  global:
    users:
      allowlist: [U_ALICE, U_BOB]
      denylist: [U_BLOCKED]
  defaults:
    users:
      mode: channel_members
      allowlist: [U_ALICE]
      denylist: [U_BOB]
  channel_overrides:
    C_OVERRIDE:
      users:
        allowlist: [U_BOB, U_CAROL, U_BLOCKED]
        denylist: []
    C_DISABLED:
      users:
        allowlist: []
```

C_SUPPORT allows Alice. C_OVERRIDE allows Bob: its allowlist replaces the local
default, and `denylist: []` clears the local Bob exclusion. Carol remains outside
the global cap; U_BLOCKED remains globally denied. C_DISABLED allows nobody.
To disable a route itself, use the channel denylist or remove its selected ID.

## Legacy configuration syntax

Existing `slack.channel_id: C_SUPPORT` remains accepted. It cannot be combined
with `slack.channels`. When both `access` and `slack.channels` are absent, the
resolver synthesizes a single-channel `channel_members` policy with private
channels permitted, preserving legacy scope. If `access: {}` is explicitly
present, no implicit grant is synthesized: that channel is denied.

Channel metadata is checked for queries and outbound replies. The public example
permits public channels only; private channels require an explicit opt-in.

Threads are keyed by `(workspace_id, channel_id, root_ts)`. Startup rejects
incompatible app/workspace/principal/owner bindings and test-only smoke data.
History for removed channels is retained and does not prevent startup; queued
work is checked against the current policy. No conversations are reassigned.

## Slack scopes and metadata

Normal runtime needs `app_mentions:read`, `chat:write`, `channels:read`, plus the
app-level `connections:write` token for Socket Mode. `allow_private: true` also
requires `groups:read`. Any effective `members_only` policy requires `users:read`.
The baseline manifest includes `channels:read`; add optional scopes in app settings
only when the configuration uses them, then reinstall the app. Channel mentions need no email/history scopes or public callback URL.
Direct messages have separate requirements in [DM setup](direct-messages.md).

Metadata is read through bounded `conversations.info`/`users.info` requests. The
raw JSON projection preserves absent/null security flags instead of treating
SDK boolean zero values as known false. Unknown required flags fail closed.
Workspace/user identity is verified, including Enterprise workspace membership;
`is_stranger: false` alone does not prove membership. `is_app_user` is not a bot flag.

Caches last at most 60 seconds, hold at most 1024 entries each for channels/users,
are workspace-keyed and concurrency-safe, and coalesce concurrent misses. A failed
refresh removes the prior positive entry; there is no stale-data fallback. Delivery
forces fresh channel metadata at the first part of every job and after 60 seconds
for a long reply. Role facts can remain cached for 60 seconds. There is an inherent
race between metadata verification and posting, not an instantaneous revocation
or atomic Slack authorization guarantee.

References: [conversation object](https://docs.slack.dev/reference/objects/conversation-object/),
[conversations.info](https://docs.slack.dev/reference/methods/conversations.info/),
[user object](https://docs.slack.dev/reference/objects/user-object/) and
[users.info](https://docs.slack.dev/reference/methods/users.info/).

## Runtime decisions and operations

Socket intake checks local routing and policy presence, then commits/deduplicates
before ACK. Directory, Agently and IdP calls never delay that ACK path. User lists,
channel facts and roles are checked by the worker before create/query or resumed
polling. Empty-mention help also follows policy, without creating a conversation.

| Condition | Query job | Outbound part before any POST |
|---|---|---|
| Policy denied | `rejected` before a turn; `needs_review` for an existing turn | `failed`, no send |
| Metadata unavailable, missing scope, invalid response, timeout or 429 | `needs_review`, no create/query/read | Exact part remains `pending`; stop runtime |
| Slack credential rejection | `needs_review`, stop runtime | Exact part remains `pending`; stop runtime |

An actual send with unknown outcome still becomes `delivery_unknown`; no safe
pre-send metadata failure is mislabeled as a successful/uncertain Slack POST.
Delivery checks use the **original persisted job author and binding**, not the
shared principal as a substitute author. Denied/failed replies are not unblocked
automatically by subsequently changing YAML. `needs_review` still needs operator
review; there is no CLI command to release a `needs_review` query. See [operations](operations.md).

One sender keeps the existing conservative one-second pacing across all channels;
a persisted 429 cooldown gates the workspace, including after restart. Logs/status
expose correlation, workspace/channel/thread/author IDs but not question/answer or
credentials. `status` still needs exclusive database ownership; stop runtime first.

For offline configuration validation, without tokens, credentials or a database:

```sh
go run ./cmd/slack-ai-gateway validate-config --config examples/gateway.example.yaml
go run ./cmd/slack-ai-gateway validate-config --config examples/gateway.example.yaml --channel-id C_TEST
```

The report shows static routing, policy presence, effective lists/mode and private
eligibility. `allowlist: null` in this **JSON report** means unrestricted IDs; `[]`
means none. It does not mean YAML `null` is allowed. Workspace-wide validation
does not enumerate channels: use `--channel-id` to evaluate a candidate. A valid
file is not proof that the bot is joined, scopes exist, a user has the required
role, or backend/tool access is appropriate. No automatic Slack reconfiguration
or database migration occurs.
