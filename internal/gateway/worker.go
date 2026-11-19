package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	slackpresentation "github.com/viant/agently-core/sdk/slack"
)

// Worker handles one durably claimed job at a time. Multiple workers can serve
// different threads; the Store prevents concurrent queries in the same thread.
type Worker struct {
	Store          Store
	Policy         AccessPolicy
	Clients        AgentlyClientFactory
	Backend        BackendLifecycle
	AgentID        string
	Timing         QueryTiming
	ResponseMode   slackpresentation.ResponseMode
	AgentlyBaseURL string
	Log            *slog.Logger
	// Execute transfers a durably marked query/attachment to runtime ownership.
	// When unset, Step executes synchronously (e.g. command and contract tests).
	Execute func(func() error)
	// OnThreadClosed runs only after the local thread closure is persisted.
	// It receives correlation metadata and a fixed reason code, never query text.
	OnThreadClosed func(jobID int64, requestID, reason string)
	finalSince     time.Time
	finalDeadline  time.Time
}

// Zero values select the stage C defaults. Shorter values are useful in tests.
type QueryTiming struct {
	Timeout     time.Duration
	PollInitial time.Duration
	PollMax     time.Duration
	MaxWait     time.Duration
}

// ReconciliationMaxWait bounds one continuous loss of observation, not the
// duration of an execution whose current state has been confirmed.
const ReconciliationMaxWait = 15 * time.Minute

func (t QueryTiming) defaults() QueryTiming {
	if t.Timeout <= 0 {
		t.Timeout = 120 * time.Second
	}
	if t.PollInitial <= 0 {
		t.PollInitial = 2 * time.Second
	}
	if t.PollMax <= 0 {
		t.PollMax = 15 * time.Second
	}
	if t.MaxWait <= 0 {
		t.MaxWait = ReconciliationMaxWait
	}
	return t
}

func (w Worker) Step(ctx context.Context) (bool, error) {
	release := RetainThread(w.Store)
	defer release()
	j, err := w.Store.ClaimJob(ctx)
	if err != nil || j == nil {
		return false, err
	}
	if w.uncertaintyExpired(j, time.Now()) {
		return true, w.execute(func() error {
			final := w
			final.Execute = nil
			final.finalSince = j.BackendObservationUncertainSince
			final.finalDeadline = time.Now().Add(w.Timing.defaults().Timeout)
			if err := final.waitForBackend(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if errors.Is(err, ErrBackendAuth) {
					return final.authCloseThread(ctx, j, err)
				}
				return final.closeThread(ctx, j, "reconciliation_deadline")
			}
			return final.step(ctx, j)
		})
	}
	return true, w.step(ctx, j)
}

func (w Worker) step(ctx context.Context, j *GatewayJob) (result error) {
	runtimeContext := ctx
	preparing := true
	if !w.finalDeadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, w.finalDeadline)
		defer cancel()
		defer func() {
			if preparing && ctx.Err() != nil && runtimeContext.Err() == nil {
				result = w.closeThread(runtimeContext, j, "reconciliation_deadline")
			}
		}()
	}
	if paused, err := w.pauseUnavailable(ctx, j); paused {
		return err
	}
	b, err := w.Store.GetBinding(ctx, j.ThreadKey)
	if err != nil {
		return err
	}
	d, err := w.Policy.Authorize(ctx, j.Message, b)
	if err != nil {
		if errors.Is(err, ErrSlackAuth) {
			return errors.Join(err, w.finishBeforeSubmission(ctx, j, func(local Store) error { return w.closeThreadStore(ctx, local, j, "slack_auth_rejected") }))
		}
		if errors.Is(err, ErrDenied) && j.BackendSubmissionStartedAt.IsZero() {
			return w.finishBeforeSubmission(ctx, j, func(local Store) error { return local.Reject(ctx, j.ID, "policy_denied") })
		}
		if errors.Is(err, ErrDenied) {
			return w.closeThread(ctx, j, "policy_denied")
		}
		return w.finishBeforeSubmission(ctx, j, func(local Store) error { return w.closeThreadStore(ctx, local, j, "policy_unavailable") })
	}
	if strings.TrimSpace(j.Text) == "" {
		text := "Mention the bot and include a question."
		if j.HasAttachments {
			text += " Attachments are not read; paste the relevant text."
		}
		return w.finishBeforeSubmission(ctx, j, func(local Store) error { return local.Complete(ctx, j.ID, "", text, RenderText(text)) })
	}
	err = w.withReady(ctx, func(local Store) error {
		var bindErr error
		b, bindErr = local.BindJob(ctx, j.ID, d)
		return bindErr
	})
	if err != nil {
		if errors.Is(err, ErrBackendUnavailable) {
			return w.deferUnavailable(ctx, j)
		}
		if errors.Is(err, ErrConflict) {
			return w.closeThread(ctx, j, "binding_conflict")
		}
		return err
	}
	client, err := w.Clients.ForPrincipal(ctx, b.PrincipalID)
	if err != nil {
		if w.Backend != nil && (errors.Is(err, ErrBackendAuth) || errors.Is(err, ErrBackendUnavailable)) {
			return w.deferUnavailable(ctx, j)
		}
		if errors.Is(err, ErrBackendAuth) {
			return w.authCloseThread(ctx, j, err)
		}
		return w.closeThread(ctx, j, "credentials_unavailable")
	}
	timing := w.Timing.defaults()
	if !j.BackendSubmissionStartedAt.IsZero() {
		if b.ConversationID == "" || j.GatewayRequestID == "" {
			return w.closeThread(ctx, j, "invalid_reconciliation_state")
		}
		preparing = false
		return w.execute(func() error {
			return w.observeAttempt(runtimeContext, j, b.ConversationID, func(attempt context.Context, observe func(Turn) error) (Turn, error) {
				return client.ReadTurn(attempt, b.ConversationID, j.GatewayRequestID, observe)
			})
		})
	}
	var createEpoch uint64
	var created, hasCreateEpoch bool
	if b.ConversationID == "" {
		if err = w.withReadyEpoch(ctx, func(local Store, epoch uint64, known bool) error {
			createEpoch, hasCreateEpoch = epoch, known
			return local.BeginCreate(ctx, b.ID)
		}); err != nil {
			if errors.Is(err, ErrBackendUnavailable) {
				err = w.deferUnavailable(ctx, j)
			}
			return err
		}
		createCtx, cancel := context.WithTimeout(ctx, timing.Timeout)
		id, createErr := client.CreateConversation(createCtx, w.AgentID)
		cancel()
		persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer persistCancel()
		if createErr != nil || id == "" {
			if errors.Is(createErr, ErrBackendAuth) && w.Backend == nil {
				return w.authCloseThread(ctx, j, createErr)
			}
			return w.closeThread(persistCtx, j, "creation_unknown")
		}
		if err = w.Store.SetConversation(persistCtx, b.ID, id); err != nil {
			return err
		}
		b.ConversationID = id
		created = true
		// Creation may have outlived the authenticated client generation. Obtain
		// its replacement outside the thread owner before durable submission.
		client, err = w.Clients.ForPrincipal(ctx, b.PrincipalID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if w.Backend != nil && (errors.Is(err, ErrBackendAuth) || errors.Is(err, ErrBackendUnavailable)) {
				return w.deferUnavailable(persistCtx, j)
			}
			if errors.Is(err, ErrBackendAuth) {
				return w.authCloseThread(persistCtx, j, err)
			}
			return w.closeThread(persistCtx, j, "credentials_unavailable")
		}
	}
	// The durable marker precedes the external side effect. Never blindly replay
	// an interrupted query; stable MessageID is correlation, not idempotency.
	if err = w.withReadyEpoch(ctx, func(local Store, epoch uint64, known bool) error {
		if created && hasCreateEpoch && known && epoch != createEpoch {
			return ErrBackendUnavailable
		}
		return local.BeginQuery(ctx, j.ID)
	}); err != nil {
		if errors.Is(err, ErrBackendUnavailable) {
			err = w.deferUnavailable(ctx, j)
		}
		return err
	}
	j.BackendSubmissionStartedAt = time.Now()
	preparing = false
	return w.execute(func() error {
		return w.observeAttempt(runtimeContext, j, b.ConversationID, func(attempt context.Context, observe func(Turn) error) (Turn, error) {
			return client.Query(attempt, Query{ConversationID: b.ConversationID, MessageID: j.GatewayRequestID, AgentID: w.AgentID, Text: j.Text, ProtocolMessages: b.ProtocolMessages, ProtocolState: b.ProtocolState}, observe)
		})
	})
}

func (w Worker) withReady(ctx context.Context, fn func(Store) error) error {
	return w.withReadyEpoch(ctx, func(local Store, _ uint64, _ bool) error { return fn(local) })
}

func (w Worker) withReadyEpoch(ctx context.Context, fn func(Store, uint64, bool) error) error {
	return InThread(ctx, w.Store, func(local Store) error {
		if epochs, ok := w.Backend.(ReadyEpochLifecycle); ok {
			return epochs.WithReadyEpoch(func(epoch uint64) error { return fn(local, epoch, true) })
		}
		if w.Backend != nil {
			return w.Backend.WithReady(func() error { return fn(local, 0, false) })
		}
		return fn(local, 0, false)
	})
}

func (w Worker) pauseUnavailable(ctx context.Context, j *GatewayJob) (bool, error) {
	if w.Backend == nil || w.Backend.State() == BackendReady {
		return false, nil
	}
	return true, w.deferUnavailable(ctx, j)
}

func (w Worker) deferUnavailable(ctx context.Context, j *GatewayJob) error {
	if !j.BackendSubmissionStartedAt.IsZero() {
		if err := w.observer(ctx, j)(Turn{ID: j.GatewayRequestID, State: "unknown"}); err != nil {
			return err
		}
		return w.poll(ctx, j)
	}
	// An in-flight create can succeed after the transition's sweep skipped its
	// uncertain binding. Close only this still-unsubmitted question; a repeated
	// global sweep could close new questions admitted after recovery.
	reason := "backend_unavailable"
	if w.Backend != nil && w.Backend.State() == BackendBlocked {
		reason = "backend_blocked"
	}
	err := w.Store.Reject(ctx, j.ID, reason)
	if errors.Is(err, ErrBackendUnavailable) {
		return nil // The transition already closed this exact question.
	}
	return err
}

func (w Worker) finishBeforeSubmission(ctx context.Context, j *GatewayJob, fn func(Store) error) error {
	if w.Backend == nil || !j.BackendSubmissionStartedAt.IsZero() {
		return InThread(ctx, w.Store, fn)
	}
	err := w.withReady(ctx, fn)
	if errors.Is(err, ErrBackendUnavailable) {
		return w.deferUnavailable(ctx, j)
	}
	return err
}

func (w Worker) execute(task func() error) error {
	release := RetainThread(w.Store)
	if w.Execute != nil {
		w.Execute(func() error { defer release(); return task() })
		return nil
	}
	defer release()
	return task()
}
func (w Worker) observer(ctx context.Context, j *GatewayJob) func(Turn) error {
	return func(turn Turn) error { return w.recordObservation(ctx, j, turn) }
}

func (w Worker) settle(ctx context.Context, j *GatewayJob, conversationID string, turn Turn, err error) error {
	// A shutdown preserves the durable deadline for the next process. A canceled
	// final attachment must not close a thread solely because shutdown interrupted it.
	if ctx.Err() != nil {
		w.finalSince = time.Time{}
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err != nil {
		if w.Backend != nil && (errors.Is(err, ErrBackendAuth) || errors.Is(err, ErrBackendUnavailable)) {
			if saveErr := w.recordObservation(ctx, j, Turn{ID: j.GatewayRequestID, State: "unknown"}); saveErr != nil {
				return saveErr
			}
			return w.poll(ctx, j)
		}
		if errors.Is(err, ErrBackendAuth) {
			return w.authCloseThread(ctx, j, err)
		}
		if errors.Is(err, ErrBackendProtocol) {
			return w.closeThread(ctx, j, "backend_protocol_or_access_error")
		}
		if saveErr := w.recordObservation(ctx, j, Turn{ID: j.GatewayRequestID, State: "unknown"}); saveErr != nil {
			return saveErr
		}
		return w.poll(ctx, j)
	}
	if turn.ID != j.GatewayRequestID {
		return w.closeThread(ctx, j, "turn_identity_mismatch")
	}
	switch turn.State {
	case "queued", "running", "completed", "failed", "canceled", "waiting_for_user", "unknown":
		if err := w.recordObservation(ctx, j, turn); err != nil {
			return err
		}
	}
	var text string
	switch turn.State {
	case "completed":
		text = turn.Content
		if strings.TrimSpace(text) == "" {
			text = "The query completed without a text response."
		}
	case "failed", "canceled":
		text = "The query did not complete successfully."
	case "queued", "running", "unknown":
		return w.poll(ctx, j)
	case "waiting_for_user":
		return w.closeThread(ctx, j, "interaction_requires_review")
	default:
		return w.closeThread(ctx, j, "unsupported_turn_state")
	}
	result := text
	if turn.State == "completed" {
		started := time.Now()
		link := ""
		if w.AgentlyBaseURL != "" && conversationID != "" {
			link = strings.TrimRight(w.AgentlyBaseURL, "/") + "/conversation/" + url.PathEscape(conversationID)
		}
		// Keep result_text's final-only contract, but do not add a misleading
		// empty-final notice when standalone public text is available.
		for _, message := range turn.StandaloneMessages {
			if message != nil && strings.TrimSpace(message.Content) != "" {
				text = turn.Content
				break
			}
		}
		presented, presentErr := slackpresentation.PresentResponse(slackpresentation.ResponseInput{
			Content: text, StandaloneMessages: turn.StandaloneMessages, ConversationURL: link, Mode: w.ResponseMode,
		})
		if presentErr != nil {
			return presentErr
		}
		text = presented.Text
		if w.Log != nil {
			mode := w.ResponseMode
			if mode == "" {
				mode = slackpresentation.ResponseRaw
			}
			w.Log.Info("response_presented", "job_id", j.ID, "mode", mode, "duration_ms", time.Since(started).Milliseconds(),
				"characters", utf8.RuneCountInString(text), "diagnostics", presented.Diagnostics)
		}
	}
	if j.HasAttachments {
		text += "\n\nAttachments were not read; paste relevant text if needed."
	}
	parts := RenderText(text)
	if turn.State == "completed" {
		parts = RenderMarkdown(text)
	}
	if turn.ProtocolMessages != nil || turn.ProtocolState != nil {
		return w.Store.Complete(ctx, j.ID, turn.ID, result, parts, ProtocolSnapshot{Messages: turn.ProtocolMessages, State: turn.ProtocolState})
	}
	return w.Store.Complete(ctx, j.ID, turn.ID, result, parts)
}

func (w Worker) closeThread(ctx context.Context, j *GatewayJob, reason string) error {
	return w.closeThreadStore(ctx, w.Store, j, reason)
}

func (w Worker) closeThreadStore(ctx context.Context, local Store, j *GatewayJob, reason string) error {
	closed, err := local.CloseThread(ctx, j.ID, reason)
	if err != nil {
		return err
	}
	if closed && w.OnThreadClosed != nil {
		w.OnThreadClosed(j.ID, j.GatewayRequestID, reason)
	}
	return nil
}

func (w Worker) authCloseThread(ctx context.Context, j *GatewayJob, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	return errors.Join(cause, w.closeThread(ctx, j, "backend_auth_or_identity_rejected"))
}

func (w Worker) poll(ctx context.Context, j *GatewayJob) error {
	timing := w.Timing.defaults()
	if !w.finalSince.IsZero() && j.BackendObservationUncertainSince.Equal(w.finalSince) {
		return w.closeThread(ctx, j, "reconciliation_deadline")
	}
	delay := min(timing.PollInitial, timing.PollMax)
	for n := 0; n < j.PollAttempts && delay < timing.PollMax; n++ {
		delay = min(delay*2, timing.PollMax)
	}
	at := time.Now().Add(delay)
	if !j.BackendObservationUncertainSince.IsZero() {
		deadline := j.BackendObservationUncertainSince.Add(timing.MaxWait)
		if at.After(deadline) {
			at = deadline
		}
	}
	return w.Store.SchedulePoll(ctx, j.ID, at)
}
