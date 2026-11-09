# Integration tests

Ordinary `go test ./...` runs offline tests with local/fake services. Live tests
require both a build tag and an explicit environment opt-in.

The two [test fixtures](testdata/) test different Slack surfaces:

| File | Purpose |
| --- | --- |
| `channels.yaml` | Selected channel mentions with an explicit channel policy |
| `direct-messages.yaml` | Opt-in DM, with channel access disabled |

Both use a fake backend, no Agently credentials, and a separate database named
`slack-smoke.sqlite`. They are not runtime configurations. For the real service,
use [the runtime example](../../examples/gateway.example.yaml) and [setup](../../README.md#setup).

## Optional Slack-only live test

This test connects to Slack and posts visible replies. Stop other gateway/test
processes using the same Slack app before running it. Use a dedicated test channel
or DM and keep its database separate from real Agently conversations.

Copy one fixture outside the repository, choosing a separate directory for each
independent test installation:

```sh
install -d -m 700 "$HOME/.agently-slack-gateway/smoke-channels"
cp -n internal/integration/testdata/channels.yaml "$HOME/.agently-slack-gateway/smoke-channels/gateway.local.yaml"
```

For DM, copy `direct-messages.yaml` instead, to a separate directory. Edit Slack
app/workspace/channel placeholders and policy keys. Configure app scopes/events
as in the [main setup](../../README.md#setup) or [DM guide](../../docs/direct-messages.md).
Load the referenced `SLACK_APP_TOKEN` and `SLACK_BOT_TOKEN` privately.

Validate the Slack-only configuration offline and initialize its database:

```sh
AGENTLY_SLACK_CONFIG="$HOME/.agently-slack-gateway/smoke-channels/gateway.local.yaml" \
  go test ./internal/integration -run '^TestSmokeConfigOffline$' -count=1 -v
go run ./cmd/agently-slack-gateway migrate \
  --config "$HOME/.agently-slack-gateway/smoke-channels/gateway.local.yaml"
```

Start the explicitly gated test:

```sh
AGENTLY_SLACK_LIVE=1 \
AGENTLY_SLACK_CONFIG="$HOME/.agently-slack-gateway/smoke-channels/gateway.local.yaml" \
AGENTLY_SLACK_TEST_DURATION=3m \
go test -tags slack_live ./internal/integration \
  -run '^TestSlackSmoke$' -count=1 -v -timeout 5m
```

Wait for `slack_connection state=ready`, then mention the bot on the allowed channel
or send a DM when using the DM fixture. The response identifies itself as a Slack
integration test with no Agently call. Verify root/thread continuation and visible
replies manually. Ctrl+C stops gracefully. A run with no query or delivered final
reply fails; this test does not establish Agently memory, authentication or recovery.
