# Direct messages

Direct messages are disabled unless `access.direct_messages` defines a complete
user policy. Channel defaults and overrides never enable DM. Group messages
(MPIM), external users and Slack Connect remain unsupported.

## Slack app setup

1. Enable Socket Mode and install the [app manifest](../examples/slack-app-manifest.yaml).
2. Enable the Messages tab in App Home and allow users to send messages.
3. Grant bot scopes `im:read`, `im:history` and `chat:write`.
4. Subscribe to the bot event `message.im`.
5. Reinstall the app after updating scopes or event subscriptions.
6. Enable DM access in the gateway YAML and restart the gateway.

The manifest includes the scopes and event subscription; verify App Home settings
in the actual installation. Receiving a DM event does not itself grant access.

## Independent access policy

Add this to a complete [runtime configuration](../examples/gateway.example.yaml):

```yaml
access:
  direct_messages:
    users:
      mode: members_only
      allowlist: [U_ALICE, U_BOB]
```

`members_only` requires an active local-workspace non-guest account and needs
`users:read`. Use `channel_members` to permit local human users including guests;
that mode does not make DM a channel or enable external users. Optional DM user
allow/deny lists apply independently of channel policies. An omitted allowlist
means unrestricted eligible IDs; `[]` permits nobody. Global user limits still
apply and cannot be overridden by the DM policy. See [access policy](access-policy.md).

For DM-only operation, set `slack.channels.mode: selected` and its `allowlist: []`.
Keep the other Slack, transport, storage and Agently settings from the runtime
example. Both DM and channel operation use the same configured backend account.

## Conversation behavior

Send a normal message to the app without a mention. Each new top-level DM message
starts a conversation; replies inside its thread continue that conversation.
Multiple top-level messages do not implicitly share history. The gateway does not
import preexisting Slack history or expose an Agently conversation created elsewhere.

The adapter verifies native DM metadata and the peer identity. It rejects group
messages, unsupported metadata, bots, edits and deletions. Durable event
deduplication, execution ordering, observation, progress and final reply delivery
use the same path as channels. A channel ID's prefix is not an authorization rule.

The persisted message surface distinguishes channel and DM work across restarts.
Turning off DM leaves history intact and subjects pending work to the new policy.
It does not automatically release blocked queries or retry failed replies.
See [request status](request-status.md) and [operations](operations.md).

A private conversation with the bot is still executed as the shared Agently
principal. It is not per-user backend authentication or a separate backend account.
For a Slack-only integration check with a fake backend, see the
[test instructions](../internal/integration/README.md).
