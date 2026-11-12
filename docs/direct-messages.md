# Direct messages

Each integration enables direct messages independently under
`integrations[].access.direct_messages`. An omitted block or `enabled: false`
disables DM. Channel scope, channel user rules, exact channel overrides and
private-channel access do not enable or restrict DM. Group DMs (MPIM), external
users and Slack Connect remain unsupported.

Use the [runtime example](../examples/gateway.example.yaml),
[multiple integrations](integrations.md) or the complete
[DM-only pilot example](../examples/access/01-dm-pilot.yaml). Only `integrations[]`
is supported for runtime connections; former top-level connection/access fields
are rejected.

## Slack app setup

1. Enable Socket Mode, install the [app manifest](../examples/slack-app-manifest.yaml)
   and create an app-level token with `connections:write`.
2. Enable the Messages tab in App Home and allow users to send messages.
3. Grant bot scopes `im:read`, `im:history`, `chat:write` and **`users:read`**.
   Both user scopes verify active local-workspace accounts, including the scope
   that permits guests.
4. Subscribe to the bot event `message.im` and save the app settings.
5. Reinstall the app after adding scopes. Verify the actual installation;
   receiving an event alone does not grant gateway access.
6. Configure the integration's DM block and restart the gateway.

The manifest includes these DM scopes, the event and the agent experience used by
optional native progress modes. Agent experience is separate from DM access.
Opening App Home or the Messages tab never submits a backend query.

The adapter reads DM metadata through
[`conversations.info`](https://docs.slack.dev/reference/methods/conversations.info/),
receives [`message.im`](https://docs.slack.dev/reference/events/message.im), and
verifies account identity/classification through
[`users.info`](https://docs.slack.dev/reference/methods/users.info/).

## Independent policy

Inside an existing `integrations[]` entry, use this complete `access` block for a
DM-only pilot:

```yaml
access:
  channels:
    enabled: false
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB]
```

Keep the integration's name, transport, Slack credentials/presentation and backend
connection. If other access settings should remain, add `direct_messages` inside
the existing `access` mapping rather than duplicating its key. Each present
surface requires an explicit `enabled`; enabled DM requires a complete `users`.

| `users.scope` | Eligible authors | Required bot scope |
| --- | --- | --- |
| `workspace_members_only` | Active local-workspace members, excluding bots and both guest types | `users:read` |
| `workspace_members_and_guests` | Active local-workspace members including guests, excluding bots and external users | `users:read` |

Both scopes accept verified Enterprise workspace membership. A guest listed under
`workspace_members_only` remains denied. Slack membership does not prove employment
or access to backend data.

An omitted DM user allowlist admits all IDs satisfying the user scope;
`allowlist: []` admits nobody. A denylist wins over the allowlist in this policy.
Use Slack user IDs, not names, email addresses, wildcards or group IDs; group
membership is not expanded. There is no global user cap or inherited channel rule.
To apply the same pilot to channels and DM, put the intended list in both complete
policies. See the [shared pilot](../examples/access/06-shared-pilot.yaml) and
[access policy](access-policy.md).

## Common setups

| Goal | Channels | Direct messages |
| --- | --- | --- |
| Channels only | Configure the channel block | Omit the DM block or use `enabled: false` |
| DM only | Omit the channel block or use `enabled: false` | Enable a complete DM user policy |
| DM pilot | Independent channel policy | `workspace_members_only` with a DM-local user allowlist |
| DM for all non-guest members | Independent channel policy | `workspace_members_only`, omit the DM user allowlist |
| DM including local guests | Independent channel policy | `workspace_members_and_guests`, optionally restrict user IDs |

Both surfaces may be disabled without deactivating the integration or deleting
history. `allow_private` and channel allow/deny lists never control DM. There is
no need to configure an empty selected-channel list just to turn channels off.

## Conversation behavior

Users can send a normal DM with or without mentioning the bot. Each new top-level
DM starts a separate backend conversation; replies in its known thread continue
that conversation. Different top-level messages do not implicitly share history.
A reply in an untracked thread can establish a new binding from that prompt,
without importing earlier Slack messages. The gateway does not import preexisting
Slack history or expose an unrelated backend conversation.

The adapter verifies native DM metadata and that the peer matches the original
message author. It rejects group messages, unsupported metadata, bots, edits and
deletions. Execution, resumed observation, progress and final delivery use the
same effective policy and persisted original author/surface. Delivery rechecks the
DM peer and account facts. A channel ID prefix is not authorization, and DM reply
routing does not depend on channel access being enabled.

Turning DM off keeps history and applies the new policy to pending work after
restart. It does not automatically release review/blocked jobs or retry failed
replies, nor does it cancel an already submitted backend run. Deduplication,
execution ordering, observation and delivery share the normal durable storage path.
See [request status](request-status.md) and [operations](operations.md).

Access lists govern invocation, not app visibility in Slack's catalog/sidebar or
who can read a channel answer. Every admitted author uses the integration's
configured backend identity. For Agently it is `backend.principal_id`; there is
no per-Slack-user backend login. Generic AG-UI uses its configured identity too.
DM policy cannot select a different model/agent or grant backend tool permissions.
See [the Agently contract](agently-contract.md).

## Verify without changing access

Run the existing offline diagnostic for the selected integration:

```sh
go run ./cmd/slack-ai-gateway validate-config \
  --config /absolute/path/gateway.yaml --integration INTEGRATION_NAME
```

Replace `INTEGRATION_NAME` with the configured name, such as `steward`. Inspect
`direct_messages.enabled`, `direct_messages.user_scope` and its `allowlist` and
`denylist`. JSON `null` for an allowlist means no ID cap; `[]` permits nobody.
This representation does not permit YAML `null`.

Validation loads no credentials, opens no storage and contacts neither Slack nor
the backend. It does not prove actual installed scopes, live account roles or
backend tool access. A live Slack test is a separate opt-in; none is implied by
offline validation. A Slack-only test with a fake backend is described in the
[integration test instructions](../internal/integration/README.md).
