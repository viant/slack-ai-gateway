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
	// OnReview runs only after the needs_review transition is persisted.
	// It receives correlation metadata and a fixed reason code, never query text.
	OnReview func(jobID int64, requestID, reason string)
}

// Zero values select the stage C defaults. Shorter values are useful in tests.
type QueryTiming struct {
	Timeout     time.Duration
	PollInitial time.Duration
	PollMax     time.Duration
	MaxWait     time.Duration
}

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
		t.MaxWait = 15 * time.Minute
	}
	return t
}

func (w Worker) Step(ctx context.Context) (bool, error) {
	j, err := w.Store.ClaimJob(ctx)
	if err != nil || j == nil {
		return false, err
	}
	// Preserve the original deadline even while the shared connection is down.
	if !j.BackendSubmissionStartedAt.IsZero() && time.Since(j.BackendSubmissionStartedAt) >= w.Timing.defaults().MaxWait {
		return true, w.review(ctx, j, "reconciliation_deadline")
	}
	if paused, err := w.pauseUnavailable(ctx, j); paused {
		return true, err
	}
	b, err := w.Store.GetBinding(ctx, j.ThreadKey)
	if err != nil {
		return true, err
	}
	d, err := w.Policy.Authorize(ctx, j.Message, b)
	if err != nil {
		if errors.Is(err, ErrSlackAuth) {
			return true, errors.Join(err, w.finishBeforeSubmission(ctx, j, func() error { return w.review(ctx, j, "slack_auth_rejected") }))
		}
		if errors.Is(err, ErrDenied) && j.BackendSubmissionStartedAt.IsZero() {
			return true, w.finishBeforeSubmission(ctx, j, func() error { return w.Store.Reject(ctx, j.ID, "policy_denied") })
		}
		if errors.Is(err, ErrDenied) {
			return true, w.review(ctx, j, "policy_denied")
		}
		return true, w.finishBeforeSubmission(ctx, j, func() error { return w.review(ctx, j, "policy_unavailable") })
	}
	if strings.TrimSpace(j.Text) == "" {
		text := "Mention the bot and include a question."
		if j.HasAttachments {
			text += " Attachments are not read; paste the relevant text."
		}
		return true, w.finishBeforeSubmission(ctx, j, func() error { return w.Store.Complete(ctx, j.ID, "", text, RenderText(text)) })
	}
	err = w.withReady(func() error {
		var bindErr error
		b, bindErr = w.Store.BindJob(ctx, j.ID, d)
		return bindErr
	})
	if err != nil {
		if errors.Is(err, ErrBackendUnavailable) {
			return true, w.deferUnavailable(ctx, j)
		}
		if errors.Is(err, ErrConflict) {
			return true, w.review(ctx, j, "binding_conflict")
		}
		return true, err
	}
	client, err := w.Clients.ForPrincipal(ctx, b.PrincipalID)
	if err != nil {
		if w.Backend != nil && (errors.Is(err, ErrBackendAuth) || errors.Is(err, ErrBackendUnavailable)) {
			if !j.BackendSubmissionStartedAt.IsZero() {
				return true, w.poll(ctx, j)
			}
			return true, w.deferUnavailable(ctx, j)
		}
		if errors.Is(err, ErrBackendAuth) {
			return true, w.authReview(ctx, j, err)
		}
		return true, w.review(ctx, j, "credentials_unavailable")
	}
	timing := w.Timing.defaults()
	if !j.BackendSubmissionStartedAt.IsZero() {
		if b.ConversationID == "" || j.GatewayRequestID == "" {
			return true, w.review(ctx, j, "invalid_reconciliation_state")
		}
		if time.Since(j.BackendSubmissionStartedAt) >= timing.MaxWait {
			return true, w.review(ctx, j, "reconciliation_deadline")
		}
		return true, w.execute(func() error {
			readCtx, cancel := context.WithDeadline(ctx, j.BackendSubmissionStartedAt.Add(timing.MaxWait))
			defer cancel()
			turn, readErr := client.ReadTurn(readCtx, b.ConversationID, j.GatewayRequestID, w.observer(ctx, j))
			return w.settle(ctx, j, b.ConversationID, turn, readErr)
		})
	}
	if b.ConversationID == "" {
		if err = w.withReady(func() error { return w.Store.BeginCreate(ctx, b.ID) }); err != nil {
			if errors.Is(err, ErrBackendUnavailable) {
				err = w.deferUnavailable(ctx, j)
			}
			return true, err
		}
		createCtx, cancel := context.WithTimeout(ctx, timing.Timeout)
		id, createErr := client.CreateConversation(createCtx, w.AgentID)
		cancel()
		persistCtx, persistCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer persistCancel()
		if createErr != nil || id == "" {
			if errors.Is(createErr, ErrBackendAuth) && w.Backend == nil {
				return true, w.authReview(ctx, j, createErr)
			}
			return true, w.review(persistCtx, j, "creation_unknown")
		}
		if err = w.Store.SetConversation(persistCtx, b.ID, id); err != nil {
			return true, err
		}
		b.ConversationID = id
	}
	// The durable marker precedes the external side effect. Never blindly replay
	// an interrupted query; stable MessageID is correlation, not idempotency.
	if err = w.withReady(func() error { return w.Store.BeginQuery(ctx, j.ID) }); err != nil {
		if errors.Is(err, ErrBackendUnavailable) {
			err = w.deferUnavailable(ctx, j)
		}
		return true, err
	}
	j.BackendSubmissionStartedAt = time.Now()
	return true, w.execute(func() error {
		queryCtx, cancel := context.WithDeadline(ctx, j.BackendSubmissionStartedAt.Add(timing.MaxWait))
		defer cancel()
		turn, err := client.Query(queryCtx, Query{ConversationID: b.ConversationID, MessageID: j.GatewayRequestID, AgentID: w.AgentID, Text: j.Text, ProtocolMessages: b.ProtocolMessages, ProtocolState: b.ProtocolState}, w.observer(ctx, j))
		return w.settle(ctx, j, b.ConversationID, turn, err)
	})
}

func (w Worker) withReady(fn func() error) error {
	if w.Backend != nil {
		return w.Backend.WithReady(fn)
	}
	return fn()
}

func (w Worker) pauseUnavailable(ctx context.Context, j *GatewayJob) (bool, error) {
	if w.Backend == nil || w.Backend.State() == BackendReady {
		return false, nil
	}
	return true, w.deferUnavailable(ctx, j)
}

func (w Worker) deferUnavailable(ctx context.Context, j *GatewayJob) error {
	if !j.BackendSubmissionStartedAt.IsZero() {
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

func (w Worker) finishBeforeSubmission(ctx context.Context, j *GatewayJob, fn func() error) error {
	if w.Backend == nil || !j.BackendSubmissionStartedAt.IsZero() {
		return fn()
	}
	err := w.withReady(fn)
	if errors.Is(err, ErrBackendUnavailable) {
		return w.deferUnavailable(ctx, j)
	}
	return err
}

func (w Worker) execute(task func() error) error {
	if w.Execute != nil {
		w.Execute(task)
		return nil
	}
	return task()
}
func (w Worker) observer(ctx context.Context, j *GatewayJob) func(Turn) error {
	return func(turn Turn) error {
		if turn.ID != j.GatewayRequestID {
			return ErrBackendProtocol
		}
		return w.Store.RecordObservation(ctx, j.ID, turn.State, time.Now())
	}
}

func (w Worker) settle(ctx context.Context, j *GatewayJob, conversationID string, turn Turn, err error) error {
	// A caller timeout/shutdown must not prevent recording the need to reconcile.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err != nil {
		if w.Backend != nil && (errors.Is(err, ErrBackendAuth) || errors.Is(err, ErrBackendUnavailable)) {
			if saveErr := w.Store.RecordObservation(ctx, j.ID, "unknown", time.Now()); saveErr != nil {
				return saveErr
			}
			return w.poll(ctx, j)
		}
		if errors.Is(err, ErrBackendAuth) {
			return w.authReview(ctx, j, err)
		}
		if errors.Is(err, ErrBackendProtocol) {
			return w.review(ctx, j, "backend_protocol_or_access_error")
		}
		if saveErr := w.Store.RecordObservation(ctx, j.ID, "unknown", time.Now()); saveErr != nil {
			return saveErr
		}
		return w.poll(ctx, j)
	}
	if turn.ID != j.GatewayRequestID {
		return w.review(ctx, j, "turn_identity_mismatch")
	}
	switch turn.State {
	case "queued", "running", "completed", "failed", "canceled", "waiting_for_user", "unknown":
		if err := w.Store.RecordObservation(ctx, j.ID, turn.State, time.Now()); err != nil {
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
		return w.review(ctx, j, "interaction_requires_review")
	default:
		return w.review(ctx, j, "unsupported_turn_state")
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

func (w Worker) review(ctx context.Context, j *GatewayJob, reason string) error {
	if err := w.Store.Review(ctx, j.ID, reason); err != nil {
		return err
	}
	if w.OnReview != nil {
		w.OnReview(j.ID, j.GatewayRequestID, reason)
	}
	return nil
}

func (w Worker) authReview(ctx context.Context, j *GatewayJob, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	return errors.Join(cause, w.review(ctx, j, "backend_auth_or_identity_rejected"))
}

func (w Worker) poll(ctx context.Context, j *GatewayJob) error {
	timing := w.Timing.defaults()
	deadline := j.BackendSubmissionStartedAt.Add(timing.MaxWait)
	if !time.Now().Before(deadline) {
		// This does NOT cancel backend execution; no cancellation endpoint is called.
		return w.review(ctx, j, "reconciliation_deadline")
	}
	delay := min(timing.PollInitial, timing.PollMax)
	for n := 0; n < j.PollAttempts && delay < timing.PollMax; n++ {
		delay = min(delay*2, timing.PollMax)
	}
	at := time.Now().Add(delay)
	if at.After(deadline) {
		at = deadline
	}
	return w.Store.SchedulePoll(ctx, j.ID, at)
}
