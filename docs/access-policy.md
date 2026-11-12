# Channel and direct-message access policy

Each `integrations[]` entry contains its complete Slack-to-backend connection and
its independent `access` policy. `slack` contains app/workspace, credential and
presentation settings; channel selection and user authorization belong under
`access.channels` and `access.direct_messages`. See
[multiple integrations](integrations.md).

Socket Mode is the only implemented transport. Every channel must belong to the
configured workspace and have the bot as a member. The gateway does not join,
create or invite itself to channels. Mentioning an app without inviting it does
not deliver an `app_mention` event. Inviting it from that mention can deliver the
original message after the invite. See the official
[app mention contract](https://docs.slack.dev/reference/events/app_mention/).

## Configuration contract

Only the `storage` plus `integrations[]` runtime format is supported. Every
present channel/DM block and every exact channel override requires an explicit
`enabled`. An omitted surface is disabled. A disabled block can contain just
`enabled: false`, or retain valid prepared settings without all required enabled
fields. Supplied user policies still need a valid `scope`. `allow_private`
defaults to false independently in every rule.

An enabled general channel rule requires `scope` and a complete `users` policy.
An enabled DM or exact channel rule requires a complete `users` policy. Each
integration uses one shared backend identity; author access does not change
backend ownership, model/agent selection or downstream data/tool permissions.

| Field | Value | Meaning |
| --- | --- | --- |
| `channels.scope` | `workspace_channels_with_bot` | Eligible workspace channels containing the bot; an optional allowlist narrows this pool |
| `channels.scope` | `selected` | An explicit channel allowlist is required, even when empty |
| `users.scope` | `workspace_members_only` | Active local-workspace members, excluding bots and both guest types |
| `users.scope` | `workspace_members_and_guests` | Active local-workspace members including guests, excluding bots and external users |

Both user scopes verify account identity, active status and workspace membership
through the existing Slack directory lookup, including verified Enterprise
workspace membership. Both require **`users:read`**. A guest on an allowlist is
still rejected by `workspace_members_only`. Slack membership is not proof of
employment or permission to access backend tools.

Use Slack IDs, not names, emails, wildcards or group IDs. Group membership is not
expanded. Security settings reject `null`, unknown fields/scopes, duplicate or
blank/whitespace IDs, YAML aliases and merges. Omission and `[]` have different
meanings. Configuration is compiled into immutable rules; changes require restart.

| List | Omitted | Explicit `[]` |
| --- | --- | --- |
| Channel allowlist with `workspace_channels_with_bot` | Entire eligible pool of channels with the bot | No general channel routes |
| Channel allowlist with `selected` | Configuration error | No general channel routes |
| User allowlist | All users satisfying the selected user scope | Nobody |
| Channel or user denylist | No additional exclusions | No additional exclusions |

A denylist wins over its allowlist **within the selected rule**. Channel lists
contain channel IDs; lists inside `users` contain user IDs. Neither a user ID
allowlist nor an exact rule bypasses account classification.

## Complete channel overrides

The channel policy is selected once, without merging fields or lists:

```text
channels.enabled = false
└─ all channels disabled, including exact overrides

channels.enabled = true
├─ channel_overrides contains the channel ID
│  └─ use only that exact rule
└─ no exact rule
   └─ use the general scope, channel filters, privacy and users
```

`channel_overrides.<ID>` contains its own `enabled`, optional `allow_private` and
complete `users` when enabled. It has no channel `scope`, channel allowlist or
channel denylist: its key already identifies one channel. An enabled exact rule
can admit a channel outside the general allowlist, on the general denylist, or
private despite general `allow_private: false`. A disabled exact rule rejects
without falling back to the general rule.

An exact user policy replaces the entire general user policy. A missing allowlist
there means every user satisfying its scope; a missing denylist means no user
exclusions. Neither list is inherited. Consequently, an exact rule can admit a
user excluded by the general user policy. There is no hidden global user cap.

Exact rules never bypass the configured app/workspace, actual bot membership,
valid human author, archive/external-sharing checks or the app's real Slack
permissions. Archived, MPIM, externally shared and pending-external channels remain
unsupported. `allow_private: true` needs actual private-channel access and
`groups:read`; it does not invite the bot.

## Complete worked example

The [standalone configuration](../examples/access/full.yaml) uses MySQL, one
Agently integration, general filters, private/guest exceptions and independent DM.
IDs, backend address and secret references are placeholders. Alice, Bob, Carol and
Dave are active non-guest members; `U_GUEST` is a local guest. The bot belongs to
all channels considered. `C_PARTNERS` and `C_PRIVATE_DEFAULT` are private.

```yaml
# Complete access-policy example; IDs and secret references are placeholders.
storage:
  driver: mysql
  dsn: '${Username}:${Password}@tcp(127.0.0.1:3309)/slack_ai_gateway?parseTime=true&loc=UTC'
  secret:
    URL: file:///absolute/path/mysql.json
    Key: blowfish://default

integrations:
  - name: steward
    transport: socket
    slack:
      app_id: A_STEWARD
      workspace_id: T_COMPANY
      app_token_secret:
        URL: file:///absolute/path/steward-app-token.enc
        Key: blowfish://default
      bot_token_secret:
        URL: file:///absolute/path/steward-bot-token.enc
        Key: blowfish://default
      status:
        mode: hybrid
        refresh_interval_sec: 30
    backend:
      type: agently
      base_url: https://steward.example.com
      agent_id: steward
      principal_id: shared-steward
      auth:
        mode: oob_session
        secret:
          URL: file:///absolute/path/steward-backend.json
          Key: blowfish://default
    access:
      channels:
        enabled: true
        scope: workspace_channels_with_bot
        allowlist: [C_MAIN, C_TEAM, C_DENIED, C_OFF, C_PRIVATE_DEFAULT]
        denylist: [C_DENIED]
        allow_private: false
        users:
          scope: workspace_members_only
          allowlist: [U_ALICE, U_BOB, U_CAROL]
          denylist: [U_BOB]
        channel_overrides:
          # An exact rule can enable a private channel outside the general list.
          C_PARTNERS:
            enabled: true
            allow_private: true
            users:
              scope: workspace_members_and_guests
              allowlist: [U_ALICE, U_GUEST]
          # An exact rule replaces the general channel and user denials.
          C_DENIED:
            enabled: true
            allow_private: false
            users:
              scope: workspace_members_only
              allowlist: [U_BOB]
          # Missing user lists mean all users qualifying for this scope.
          C_OPEN:
            enabled: true
            allow_private: false
            users:
              scope: workspace_members_only
          # A disabled exact rule never falls back to the general rule.
          C_OFF:
            enabled: false
      direct_messages:
        enabled: true
        users:
          scope: workspace_members_only
          allowlist: [U_BOB]
```

| Request | Result | Reason |
| --- | --- | --- |
| Alice or Carol in C_MAIN | Allow | General user policy |
| Bob in C_MAIN | Deny | General user denylist |
| Dave in C_MAIN | Deny | Missing from general user allowlist |
| Guest in C_MAIN | Deny | General user scope excludes guests |
| Alice or guest in C_PARTNERS | Allow | Complete private/guest rule outside the general channel list |
| Bob in C_PARTNERS | Deny | Missing from that rule's user allowlist |
| Bob in C_DENIED | Allow | Exact rule replaces general channel and user denials |
| Alice in C_DENIED | Deny | Exact rule allows Bob only |
| Dave or Bob in C_OPEN | Allow | Exact rule has no inherited user lists |
| Any user in C_OFF | Deny | Exact rule is disabled |
| Alice in C_PRIVATE_DEFAULT | Deny | General rule rejects private channels |
| Alice in a new C_NEW with the bot | Deny | Outside the general allowlist, with no exact rule |
| Bob in DM | Allow | Independent DM allowlist |
| Alice in DM | Deny | DM allows Bob only |

## Smaller complete examples

Each linked file is a standalone configuration. The blocks below replace the
**whole `access` mapping inside an integration**, retaining its name, transport,
Slack and backend settings. Do not merge them into a previous example's overrides.
An omitted DM/channel block is disabled.

### 1. DM only for three users

[Standalone configuration](../examples/access/01-dm-pilot.yaml).

```yaml
access:
  channels:
    enabled: false
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB, U_CAROL]
```

Channels are explicitly disabled. Only the three listed non-guest members can ask in DM.

### 2. Public channels with the bot for a three-user pilot

[Standalone configuration](../examples/access/02-workspace-pilot.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    allow_private: false
    denylist: [C_SENSITIVE]
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB, U_CAROL]
```

No channel allowlist is present. Adding the bot to another eligible public channel includes it, except C_SENSITIVE.

### 3. Selected channels for all non-guest members

[Standalone configuration](../examples/access/03-selected-members.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: selected
    allowlist: [C_SUPPORT, C_PLATFORM]
    allow_private: false
    users:
      scope: workspace_members_only
```

Both channels still need the bot. Another channel requires an explicit configuration edit.

### 4. Both channel filters on the workspace-with-bot pool

[Standalone configuration](../examples/access/04-filtered-workspace.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    allowlist: [C_SUPPORT, C_PLATFORM, C_TEST]
    denylist: [C_TEST]
    allow_private: false
    users:
      scope: workspace_members_only
```

C_SUPPORT and C_PLATFORM are eligible; C_TEST is denied despite appearing in the allowlist.

### 5. Only individually configured channels

[Standalone configuration](../examples/access/05-overrides-only.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: selected
    allowlist: []
    allow_private: false
    users:
      scope: workspace_members_only
    channel_overrides:
      C_SUPPORT:
        enabled: true
        allow_private: false
        users:
          scope: workspace_members_only
          allowlist: [U_ALICE, U_BOB]
      C_PARTNERS:
        enabled: true
        allow_private: true
        users:
          scope: workspace_members_and_guests
          allowlist: [U_ALICE, U_GUEST]
```

The empty general allowlist does not block the two exact rules. C_PARTNERS can be private and admits the listed local guest.

### 6. The same pilot in channels and DM

[Standalone configuration](../examples/access/06-shared-pilot.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    allow_private: false
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB, U_CAROL]
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB, U_CAROL]
```

The repeated list is intentional. There is no global policy or inheritance connecting the surfaces.

### 7. Wide channel access with a smaller DM pilot

[Standalone configuration](../examples/access/07-dm-pilot-wide-channels.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    allow_private: false
    users:
      scope: workspace_members_only
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_only
      allowlist: [U_ALICE, U_BOB]
```

The DM pilot does not restrict channel authors.

### 8. Channel master switch with prepared overrides

[Standalone configuration](../examples/access/08-channels-disabled.yaml).

```yaml
access:
  channels:
    enabled: false
    channel_overrides:
      C_SUPPORT:
        enabled: true
        allow_private: false
        users:
          scope: workspace_members_only
          allowlist: [U_ALICE]
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_only
```

C_SUPPORT remains disabled: its enabled exact rule cannot bypass the channel master switch. DM remains independent.

### 9. Verified local members including guests in channels and DM

[Standalone configuration](../examples/access/09-workspace-members-and-guests.yaml).

```yaml
access:
  channels:
    enabled: true
    scope: workspace_channels_with_bot
    allow_private: false
    users:
      scope: workspace_members_and_guests
  direct_messages:
    enabled: true
    users:
      scope: workspace_members_and_guests
```

Both surfaces verify active local membership and permit guests. This is not a bypass of account verification.

## Slack setup and runtime checks

The [manifest](../examples/slack-app-manifest.yaml) includes `users:read` for both
user scopes. Channel use needs `app_mentions:read`, `channels:read`, `chat:write`
and the Socket Mode app token with `connections:write`. Private channels need
`groups:read`; DM needs `im:read`, `im:history`, the `message.im` event and enabled
App Home messaging. Reinstall the app after adding bot scopes. See
[DM setup](direct-messages.md).

Queries, resumed observation, answer parts, text/native progress and closure
notices use the same effective policy and the original persisted author/surface.
DM does not borrow channel routing. Account and channel facts use existing bounded
Slack directory requests and caches; revocation is not an atomic transaction with
posting. Membership/account metadata remains subject to the existing refresh
window. Unavailable required metadata does not grant access. See
[operations](operations.md) for review, delivery and retry behavior.

Disabling a surface preserves integration history and does not itself deactivate
its assignment. Both surfaces may be disabled. Permission-only changes retain
integration identity and bindings; they neither cancel a running backend nor replay
a prompt. Existing blocked/review/failed work is not automatically released by a
later configuration change. Access lists govern invocation, not Slack catalog
visibility or who can read a channel reply. See [the backend contract](agently-contract.md).

## Offline verification and upgrading

```sh
go run ./cmd/slack-ai-gateway validate-config --config examples/access/full.yaml --integration steward
go run ./cmd/slack-ai-gateway validate-config --config examples/access/full.yaml --integration steward --channel-id C_PARTNERS
```

Validation loads no credentials, opens no database and contacts neither Slack nor
a backend. The report identifies `general`, `channel_override` or `disabled`
through `rule_source`, with the effective `user_scope`, lists and private-channel
setting. A JSON allowlist of `null` means no ID cap; `[]` means nobody. YAML `null`
remains invalid. Workspace validation does not enumerate live channels; use
`--channel-id` for a candidate. A valid file does not prove bot membership,
installed OAuth scopes, account roles or backend tool access.

The former top-level single-integration format, `access_mode`, `slack.channel_id`,
`slack.channels`, `access.global`, `access.defaults`, partial overrides and old
user/channel mode names are rejected. Rewrite once into complete integrations;
there is no automatic conversion at startup. Materialize previous effective user
limits separately in each surface and exact rule before removing old global
limits. Review exact rules because they now replace general channel denials and
privacy as well as users. No database migration is needed: the persisted shared
execution identity and conversation bindings remain unchanged.

| Former setting | New location/value |
| --- | --- |
| Channel `mode: workspace` | `access.channels.scope: workspace_channels_with_bot` |
| Channel `mode: selected` | `access.channels.scope: selected` |
| User `channel_members` | `users.scope: workspace_members_and_guests`; account verification and `users:read` are now required |
| User `members_only` | `users.scope: workspace_members_only` |
| Global/default/partial user rules | Complete effective user policies in each surface and exact channel rule |

Automated examples and adapter tests do not establish that a real workspace is
configured correctly. A live Slack test is a separate opt-in after scope/setup
verification; no live result is implied by offline validation.
