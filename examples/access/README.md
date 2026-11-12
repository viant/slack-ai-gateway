# Access configuration examples

All files are standalone runtime configurations with placeholder IDs and Scy
resources. Copy one outside the repository and replace its technical connection
settings before running the gateway. Offline validation does not load the secret
files or contact Slack/backend services.

See the [access guide](../../docs/access-policy.md) for the complete contract and
request/result matrix. `full.yaml` is the extensive MySQL/Agently example; the
smaller files use SQLite and an Agently connection only to provide a complete
configuration around each access recipe. No example is a migration or a live-test result.

| File | Purpose |
| --- | --- |
| [full.yaml](full.yaml) | General rules, complete overrides, private channels, guests and independent DM |
| [01-dm-pilot.yaml](01-dm-pilot.yaml) | DM only for three users |
| [02-workspace-pilot.yaml](02-workspace-pilot.yaml) | Public channels with the bot for a three-user pilot |
| [03-selected-members.yaml](03-selected-members.yaml) | Selected channels for all non-guest members |
| [04-filtered-workspace.yaml](04-filtered-workspace.yaml) | Both channel filters on the workspace-with-bot pool |
| [05-overrides-only.yaml](05-overrides-only.yaml) | Only individually configured channels |
| [06-shared-pilot.yaml](06-shared-pilot.yaml) | The same pilot in channels and DM |
| [07-dm-pilot-wide-channels.yaml](07-dm-pilot-wide-channels.yaml) | Wide channel access with a smaller DM pilot |
| [08-channels-disabled.yaml](08-channels-disabled.yaml) | Channel master switch with prepared overrides |
| [09-workspace-members-and-guests.yaml](09-workspace-members-and-guests.yaml) | Verified local members including guests in channels and DM |

Validate a candidate channel without credentials or storage:

```sh
go run ./cmd/slack-ai-gateway validate-config --config examples/access/full.yaml --integration steward --channel-id C_PARTNERS
```

Every user scope requires `users:read`. Private-channel access also needs
`groups:read` and an invited bot. Reinstall the app after adding OAuth scopes.
Each integration needs a distinct Slack app; see the
[multi-integration example](../gateway.multi.example.yaml).
