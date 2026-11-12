# Request status

The gateway reports progress while waiting for Viant Steward, then sends completed
answers through the durable outbox. It does not stream answer text to Slack.

```yaml
slack:
  status:
    # message (default), agent_session, or hybrid; restart to apply.
    mode: message
    # Positive whole seconds for message/hybrid refresh; default 30.
    refresh_interval_sec: 30
```

Settings apply gateway-wide to channels and DM. Each question saves its status mode
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
can delay updates.

| Phase | Meaning |
| --- | --- |
| `received` | Durable intake; awaiting a worker |
| `queued` | An earlier unresolved request owns the thread |
| `blocked` | An earlier request needs operator attention |
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
| `needs_review` | Automation stopped; no automatic query replay |
| `delivery_uncertain` | A final part is uncertain or permanently failed |

Checking includes the last confirmed backend state and UTC timestamp, when
available. Heartbeats are not proof of backend activity. No percentages, ETAs,
tool details or model-generated progress text are emitted. A gateway deadline
does not cancel remote execution.

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
| Waiting for interaction, review/blocker or uncertain/failed final delivery | `suspended` |

A blocker takes precedence over queued work. The indicator aggregates the thread,
including different authors; completing one question cannot clear another's
processing state. `active` means ready for another question, not a closed session.
Indicator failure does not block execution or final answers.

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

One serialized outbound worker handles final parts, status messages and native
setters. First acknowledgments and changed states have priority over periodic
refreshes. All writes share one-second pacing and a durable workspace cooldown
for valid `429 Retry-After`. Hybrid targets retain independent publication and
retry states; one failing target does not disable the other.

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
| Permanent Viant Steward authentication/identity failure | Keep Slack connected in `blocked`; administrator fixes the cause and restarts |
| New/unstarted question while backend unavailable | Persist a not-submitted reply; never run it automatically later |

Status failure or ambiguity does not block the final outbox. Restart reprojects
current facts instead of replaying missed progress messages. Backend recovery
retains each submitted request's original reconciliation deadline and identifiers;
it does not cancel remote execution or send the prompt again. Outage notices use
the same policy checks, sender pacing and durable delivery rules as other replies.
For final delivery ambiguity and job review states, see [operations](operations.md).
Final answer formatting is described separately in [response presentation](response-presentation.md).
