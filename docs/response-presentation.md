# Response presentation

Select how completed Agently answers are shown on Slack:

```yaml
slack:
  response:
    mode: summary
```

Omitting this section selects `raw`. The setting is
global for channels and DM. Restart the gateway after changing it. It applies to
answers finalized by the new process, including queries recovered after restart.
Already-persisted outbox messages retain their original content through retries.
`validate-config` reports
`response_mode` and rejects unknown values before startup/network calls.

| Mode | Behavior |
| --- | --- |
| `raw` | Link first, then unchanged public message bodies and full final, including Forge fences |
| `cli_compatible` | Link first, prose and frozen CLI summary rules, including `[Interactive content]` and `[report]` |
| `summary` | Link first, same extraction with readable report headings, no technical markers |
| `normalized_summary` | Link first, prose and summary of the final native assembled report after accepted changes |
| `report_text` | Link first, prose and authored textual parts of final `report-document-v1` reports |

For every mode, completed answers include the exact canonical turn's standalone
public assistant messages before its final answer/report. This preserves
separately authored findings, including their original headings. The native SDK
selects messages by ownership and ID, excluding execution narration, internal
messages, tool results and the final itself. No heading detection or semantic
deduplication is used. Messages are processed separately, so Forge transactions
from independent messages cannot be combined. Nothing is published before the
turn completes; recovery follows the same selection path.

Start with `summary`. Use `cli_compatible` to compare with the CLI extraction
baseline. These modes extract the first nonempty Markdown block from matching
`start`/`commit` pairs, in commit order; they deliberately ignore
append/patch/replace. They do not reproduce infrastructure lines or earlier
stream narration from CLI stdout, and do not execute the CLI.

`normalized_summary` and `report_text` reuse the SDK's native report assembler,
including implicit commit. They may therefore differ from the frozen baseline.
`summary` and `normalized_summary` can be identical when there are no changes
to the selected summary. Accepted updates can change `normalized_summary` while
`summary` continues to use its frozen start/commit extraction.
The three summary modes cap each summary at 8192 Unicode characters with an
explicit notice. `report_text` reads document title, subtitle and description,
and authored text/labels from Markdown, info panels, callouts, sections and
groups. It includes all sections once, without following references again.
It does not evaluate expressions or convert KPI, tables, chart configuration or
datasets. Other grammars produce prose, a notice and a link. Existing native
report bounds and the gateway's Markdown chunker still apply.

All five modes begin with `[View full response in Agently](URL)`, including
ordinary greetings and empty answers. The link occurs once, before chunking,
so it is in the first Slack message even for long reports. The URL
uses `agently.base_url` and the bound conversation ID, preserving a deployment
path prefix. Recipients need browser access to that deployment to open it; a
localhost URL refers to the recipient's own computer. No new public URL setting
is introduced here.

Non-raw modes retain available public prose and summaries and never revert to raw
Forge. Intentional omissions add `Additional interactive content is available in
Agently.` Incomplete, unsupported or malformed rich content adds `Some content
could not be presented here. View the full response in Agently.` Only one notice
is added across messages, with incomplete presentation taking precedence. It is a
presentation limitation, not a failed backend query. The gateway stores the
original final result in `inbox_jobs.result_text`; complete presentation is stored separately in
the existing outbox. The source history, browser output, prompts, client
capabilities and progress/status behavior remain on their existing paths.
Logs include mode, duration, character count and fixed diagnostic codes without
answer payloads.

## Local verification

No credentials or live Slack/Agently instance are needed:

```sh
go test ./...
go test ./internal/slack -run '^TestResponsePresentationThroughHTTPAndDurableOutbox$' -v
go test github.com/viant/agently-core/sdk/slack
go run ./cmd/agently-slack-gateway validate-config --config examples/gateway.example.yaml
```

The integration regression uses local HTTP servers and temporary SQLite. It
checks all five modes, normal/recovered execution, exact turn selection, public
findings once without execution/tool/internal messages, original
result persistence, multipart native Markdown delivery, prefix-preserving links
in the first part and unchanged outbox retries after a mode change. Existing tests cover progress,
authorization and Slack delivery failures. Check configuration locally, then use
your usual `run --config ...` command for manual acceptance.
