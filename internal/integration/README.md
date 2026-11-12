# Integration tests

Ordinary `go test ./...` runs offline tests with local/fake services. Live tests
require both a build tag and an explicit environment opt-in.

The two [test fixtures](testdata/) test different Slack surfaces:

| File | Purpose |
| --- | --- |
| `channels.yaml` | Selected channel mentions with an explicit channel policy |
| `direct-messages.yaml` | Opt-in DM, with channel access disabled |

Both use the same public `storage` plus `integrations[]` configuration format,
with the logical integration name `smoke` and a separate database named
`slack-smoke.sqlite`. They contain a valid placeholder backend block so the public
shape is complete. The harness alone injects a fake backend and never authenticates
the placeholder backend or reads its secret. The normal `run` command rejects this
dedicated smoke database. For real service use,
start from [the runtime example](../../examples/gateway.example.yaml) and
[setup](../../README.md#setup).

## Optional Slack-only live test

This test connects to Slack and posts visible replies. Stop other gateway/test
processes using the same Slack app before running it. Use a dedicated test channel
or DM and keep its database separate from real Agently conversations.

Copy one fixture outside the repository, choosing a separate directory for each
independent test installation:

```sh
install -d -m 700 "$HOME/.slack-ai-gateway/smoke-channels"
cp -n internal/integration/testdata/channels.yaml "$HOME/.slack-ai-gateway/smoke-channels/gateway.local.yaml"
```

For DM, copy `direct-messages.yaml` instead, to a separate directory. Edit Slack
app/workspace/channel placeholders and policy keys. Configure app scopes/events
as in the [main setup](../../README.md#setup) or [DM guide](../../docs/direct-messages.md).
Both user scopes require `users:read`; reinstall the app after adding scopes.
Set `integrations[].slack.app_token_secret` and
`integrations[].slack.bot_token_secret` to the absolute locations of the private Scy resources prepared with [secret setup](../../docs/secrets.md).
The test loads these resources only after explicit live-test opt-in. The harness
selects the sole integration automatically. If the file contains multiple entries,
set `SLACK_AI_GATEWAY_INTEGRATION` to the exact name for both offline and live tests.

Validate the Slack-only configuration offline and initialize its database:

```sh
SLACK_AI_GATEWAY_CONFIG="$HOME/.slack-ai-gateway/smoke-channels/gateway.local.yaml" \
SLACK_AI_GATEWAY_INTEGRATION=smoke \
  go test ./internal/integration -run '^TestSmokeConfigOffline$' -count=1 -v
go run ./cmd/slack-ai-gateway migrate \
  --config "$HOME/.slack-ai-gateway/smoke-channels/gateway.local.yaml"
```

Start the explicitly gated test:

```sh
SLACK_AI_GATEWAY_LIVE=1 \
SLACK_AI_GATEWAY_CONFIG="$HOME/.slack-ai-gateway/smoke-channels/gateway.local.yaml" \
SLACK_AI_GATEWAY_INTEGRATION=smoke \
SLACK_AI_GATEWAY_TEST_DURATION=3m \
go test -tags slack_live ./internal/integration \
  -run '^TestSlackSmoke$' -count=1 -v -timeout 5m
```

Wait for `slack_connection state=ready`, then mention the bot on the allowed channel
or send a DM when using the DM fixture. The response identifies itself as a Slack
integration test with no Agently call. Verify root/thread continuation and visible
replies manually. Ctrl+C stops gracefully. A run with no query or delivered final
reply fails; this test does not establish Agently memory, authentication or recovery.
