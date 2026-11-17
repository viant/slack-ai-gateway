# Request status

The gateway reports progress while waiting for Viant Steward, then sends completed
answers through the durable outbox. It does not stream answer text to Slack.
Set `slack.status` inside the chosen `integrations[]` entry; this is a fragment:

```yaml
slack:
  status:
    # message (default), agent_session, or hybrid; restart to apply.
    mode: message
    # Positive whole seconds for message/hybrid refresh; default 30.
    refresh_interval_sec: 30
```

Settings apply to channels and DM of that integration; other integrations have
independent settings. Each question saves its status mode
at intake; changing the mode affects new questions, including new ones in existing
threads. Existing message statuses adopt a changed refresh interval after their next
successful update. First acknowledgments, phase changes and completion do not wait
for the heartbeat, but share outbound pacing and Slack cooldowns. A fast final reply
can skip the first status entirely. Socket envelope ACKs separately follow durable
inbox commit.

The interval does not change update retries (30 seconds), observation freshness
(60 seconds), ordinary HTTP timeouts (120 seconds), or execution deadlines
(15 minutes). There is no status-disable or answer-streaming switch.

## Message mode

`message` creates at most one bot-owned progress message per question and edits it
with phase and elapsed time, without a technical request header. Elapsed time starts
at intake and includes waiting for delivery. Queues, metadata calls and rate limits
can delay updates. This is wall-clock request age, not backend execution time.
Final conversation-closure messages omit elapsed time and are not refreshed.

| Phase | Meaning |
| --- | --- |
| `received` | Durable intake; awaiting a worker |
| `queued` | An earlier unresolved request owns the thread |
| `blocked` | Legacy progress awaiting automatic thread closure during recovery |
| `contacting` | Local work or HTTP request started; no confirmed running state |
| `backend_queued`, `running` | The correlated Agently turn confirmed this state |
| `checking` | Observation failed or the last confirmation is over 60 seconds old |
| `backend_recovering` | Connection unavailable; existing submitted request will be checked without resubmission |
| `backend_blocked` | Connection requires administrator attention; existing request will not be resubmitted |
| `not_submitted` | Question finished locally during an outage; it will never execute automatically |
| `delivering` | Final parts are persisted but not all confirmed delivered |
| `completed` | Agently succeeded and every final part is delivered |
| `failed`, `canceled` | Agently confirmed failure or cancellation |
| `delivered` | Local help was delivered without an Agently execution |
| `needs_review` | Legacy state finalized by startup recovery; no automatic query replay |
| `delivery_uncertain` | A final part is uncertain or permanently failed |
| `conversation_closed` | Local handling ended; queued questions were not submitted; start a new thread |

Checking includes the last confirmed backend state and UTC timestamp, when
available. Heartbeats are not proof of backend activity. No percentages, ETAs,
tool details or model-generated progress text are emitted. A gateway deadline
does not cancel remote execution.

## Automatic conversation closure

Execution observation lasts at most 15 minutes from the persisted submission
marker. The original deadline survives restart. At expiry the gateway closes
local handling immediately, without a final extra read or a replacement prompt.
An unsafe or unsupported execution state, uncertain conversation creation or an
interaction requiring approval also closes local handling instead of waiting
for an operator. A normal access-policy rejection before submission does not
close an otherwise valid conversation.

Closure is atomic with the durable notice and completion of unsubmitted queued
questions. One collective reply explains whether the original question may have
executed or was definitely not submitted; it never claims cancellation of remote
work. Existing progress messages receive a terminal update without a growing
timer. Late backend results cannot reopen the thread or publish another answer.

Further permitted questions in the same Slack thread receive a local
not-submitted reply directing the user to a new thread; they do not start backend
work or another processing indicator. A new top-level DM message or a new channel
thread is independent. The gateway does not move old queued prompts to that new
conversation. Startup applies the same rule to historical `needs_review` and
expired execution records without requiring backend authentication.

## Native agent sessions

`agent_session` uses the Slack SDK's `SetAgentSessionStatusContext` for the channel
and root thread timestamp. Normal progress uses the native indicator without an
ordinary message, custom phase or elapsed text. `hybrid` publishes both the native
indicator and the message.

A backend connection outage is an exception: an already submitted request also
gets one ordinary progress message explaining the connection problem. Further
updates, including completion, edit that same message. Questions that were not
submitted get an ordinary durable reply in every status mode. This makes the
required user action visible even when a native indicator cannot explain it.

| Current thread facts | Native state |
| --- | --- |
| Accepted/queued/running work, observation retry or pending final parts | `processing` |
| No remaining work and all required final parts delivered | `active` |
| Uncertain/failed final delivery while local handling remains open | `suspended` |
| Automatically closed local conversation | Terminal `active` cleanup, or skip if never published |

A blocker takes precedence over queued work. The indicator aggregates the thread,
including different authors; completing one question cannot clear another's
processing state. `active` clears this app's working indicator; it does not imply
that a locally closed conversation accepts more backend questions. The ordinary
closure reply explains that a new Slack thread is required. Indicator failure
does not block execution or final answers.

Unchanged processing is renewed every 10 minutes, independently of
`refresh_interval_sec`; Slack expires it after one hour without renewal. An open
native cycle includes new message-mode work until the final active state is
confirmed. Later message-only questions leave the indicator alone; a new
agent-session/hybrid question opens another cycle. If all work finishes before the
first publication claim, an idle session can be skipped.

The app must declare `features.agent_view` and its installed bot token needs
`chat:write`. The [manifest](../examples/slack-app-manifest.yaml) includes the
agent declaration and `assistant:write`. A permanent `feature_disabled` or
`missing_scope` error is recorded without automatic fallback. After repairing the
installation, a new accepted question allows another attempt without query replay.
Stop/cancellation is not implemented; no active Stop control is promised.

## Persistence and scheduling

Message-progress intent is stored in `gateway_job` with execution state in the
same inbox transaction; duplicate events cannot create another intention.
Native thread publications are persisted in `slack_agent_session_publication`,
with revisions, acknowledgments and retry facts. Both are included in the
initial schema. Progress observes the existing
execution; it never creates another conversation, binding or backend query.

One physical Slack thread owns its short state transitions, including progress
claims and receipts. Preparation reads the durable destination and original
author inside that owner; policy checks and Slack HTTP run outside it. A bot's
send permit remains reserved until its receipt is persisted, so valid
`429 Retry-After` also gates other threads and historical assignments of that
bot. First acknowledgments and changed states retain priority over periodic
refreshes, with one-second bot pacing. Hybrid targets retain independent
publication and retry states; one failing target does not disable the other.

Cancellation before Slack HTTP restores the claimed work to `pending`/`ready`;
it does not invent an uncertain send. Once HTTP starts, the existing uncertain
delivery rules apply. No native store borrowed during preparation is retained
by a network operation or receipt callback.

Every publication rechecks the original persisted author's policy and binding,
with fresh channel metadata. Denial stops publication. Unavailable metadata or
rejected Slack credentials preserve unsent work and stop the runtime. Status
never bypasses authorization. See [access policy](access-policy.md).

## Failure and restart

| Operation/result | Action |
| --- | --- |
| First message POST ambiguous or interrupted before result persistence | `delivery_unknown`; no automatic retry |
| Update of known timestamp ambiguous | Retry the latest snapshot after 30 seconds |
| Native setter ambiguous/interrupted | Retry the current thread projection after 30 seconds |
| Native setter permanently rejected | `failed`; final delivery continues |
| Deleted/uneditable message or permanent rejection | `failed`; no replacement message |
| Valid `429` | Persist retry/cooldown across restart |
| Slack authentication/metadata unavailable | Preserve work and stop safely |
| Temporary Viant Steward/IdP failure | Keep Slack connected, recover authentication and observe started requests without replay |
| Remote authentication/readiness refusal | Keep Slack connected; retry after 60–75 seconds or longer `Retry-After`, without replay |
| Invalid local backend configuration/credential resource, confirmed identity mismatch or security failure | Keep Slack connected in `blocked`; administrator fixes the cause and restarts |
| New/unstarted question while backend unavailable | Persist a not-submitted reply; never run it automatically later |
| Original execution deadline expired or execution cannot be safely continued | Atomically close local handling and queued work; direct the user to a new thread |

Status failure or ambiguity does not block the final outbox. Restart reprojects
current facts instead of replaying missed progress messages. Backend recovery
retains each submitted request's original reconciliation deadline and identifiers;
it does not cancel remote execution or send the prompt again. Outage notices use
the same policy checks, sender pacing and durable delivery rules as other replies.
For final delivery ambiguity and automatic closure metadata, see [operations](operations.md).
Final answer formatting is described separately in [response presentation](response-presentation.md).
