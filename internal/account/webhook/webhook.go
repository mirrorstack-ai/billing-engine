// Package webhook handles Stripe webhook delivery for the account
// (subscription/invoice) surface of the billing-engine.
//
// The flow is:
//
//	1. Verify the Stripe-Signature header against STRIPE_WEBHOOK_SECRET.
//	2. Open a pgx transaction.
//	3. Insert into billing_webhook_events_processed for idempotency.
//	4. Dispatch to a per-event-type handler that writes into the
//	   account mirror tables on the SAME transaction.
//	5. Commit on handler success, ROLLBACK on handler error so the
//	   dedup row also disappears and Stripe's retry hits a fresh attempt.
//
// Bad signature is the only case that returns 4xx; everything else
// (unknown event type, replay) returns 200 so Stripe stops retrying.
// Handler errors and DB errors return 500 to trigger a Stripe retry —
// because the dedup row was rolled back, the retry won't be a no-op.
package webhook

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/stripe/stripe-go/v85"
	stripewebhook "github.com/stripe/stripe-go/v85/webhook"
)

// EventDB is the narrow interface needed to orchestrate the per-delivery
// transaction. It exists so tests can mock it without standing up Postgres.
type EventDB interface {
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
}

// HandlerFunc handles a single decoded Stripe event. It receives the same
// pgx.Tx that already owns the dedup-row insert, so any writes happen
// atomically with idempotency tracking.
type HandlerFunc func(ctx context.Context, tx pgx.Tx, event stripe.Event) error

// Handler is the webhook entrypoint. It owns the signing secret, the DB
// pool, and the dispatch map. Construct one per Lambda cold-start.
type Handler struct {
	secret   string
	db       EventDB
	dispatch map[string]HandlerFunc
	// verify is injected for tests; production uses stripewebhook.ConstructEventWithOptions.
	verify func(payload []byte, sigHeader, secret string) (stripe.Event, error)
}

// markProcessedSQL inserts the event id and reports a conflict via
// RowsAffected()==0. Schema-qualified because the Lambda role's
// search_path is not guaranteed to include ms_billing_account.
const markProcessedSQL = `
INSERT INTO ms_billing_account.billing_webhook_events_processed (stripe_event_id, processed_at)
VALUES ($1, now())
ON CONFLICT (stripe_event_id) DO NOTHING
`

// NewHandler wires a Handler with the default Stripe verification path
// and the production dispatch table (real handlers from handlers.go).
func NewHandler(secret string, db EventDB) *Handler {
	return &Handler{
		secret:   secret,
		db:       db,
		dispatch: defaultDispatch(),
		verify:   verifyStripeSignature,
	}
}

// Result is the outcome of processing one webhook delivery. The Lambda
// adapter translates this into an HTTP response.
type Result struct {
	StatusCode int
	Body       string
}

// Process runs the full receive pipeline for a single delivery.
func (h *Handler) Process(ctx context.Context, payload []byte, sigHeader string) Result {
	event, err := h.verify(payload, sigHeader, h.secret)
	if err != nil {
		slog.WarnContext(ctx, "stripe signature verification failed", "error", err)
		return Result{StatusCode: 400, Body: "invalid signature"}
	}

	logger := slog.With("event_id", event.ID, "event_type", event.Type)

	tx, err := h.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		logger.ErrorContext(ctx, "failed to begin webhook transaction", "error", err)
		return Result{StatusCode: 500, Body: "tx begin failed"}
	}
	// Rollback is a no-op once Commit has succeeded, so deferring it is
	// safe and guarantees we never leak a transaction on an early return.
	defer func() { _ = tx.Rollback(ctx) }()

	inserted, err := markProcessed(ctx, tx, event.ID)
	if err != nil {
		logger.ErrorContext(ctx, "failed to record webhook event", "error", err)
		return Result{StatusCode: 500, Body: "dedup write failed"}
	}
	if !inserted {
		// Already processed in a previous delivery. Roll back the
		// no-op insert and tell Stripe we're done.
		logger.InfoContext(ctx, "webhook event already processed, skipping dispatch")
		return Result{StatusCode: 200, Body: "duplicate"}
	}

	handler, ok := h.dispatch[string(event.Type)]
	if !ok {
		// Unknown event types should not 4xx; Stripe may add new types
		// and we do not want them to wedge delivery for the fleet.
		// We commit the dedup row so we don't re-process the same
		// uninteresting event on every retry cycle.
		if err := tx.Commit(ctx); err != nil {
			logger.ErrorContext(ctx, "failed to commit dedup row for ignored event", "error", err)
			return Result{StatusCode: 500, Body: "commit failed"}
		}
		logger.InfoContext(ctx, "no handler registered for event type")
		return Result{StatusCode: 200, Body: "ignored"}
	}

	if err := handler(ctx, tx, event); err != nil {
		// Critical: returning 500 here causes the deferred Rollback to
		// fire, which removes the dedup row inserted earlier in this
		// tx. Stripe will retry, and the retry will see no dedup row
		// and re-attempt the handler. This is the whole point of the
		// transaction-based approach — without it, a transient handler
		// error becomes a permanent silent drop.
		logger.ErrorContext(ctx, "event handler returned error", "error", err)
		return Result{StatusCode: 500, Body: "handler failed"}
	}

	if err := tx.Commit(ctx); err != nil {
		logger.ErrorContext(ctx, "failed to commit webhook transaction", "error", err)
		return Result{StatusCode: 500, Body: "commit failed"}
	}
	return Result{StatusCode: 200, Body: "ok"}
}

// markProcessed inserts the event id; returns true iff the row was new.
func markProcessed(ctx context.Context, tx pgx.Tx, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx, markProcessedSQL, eventID)
	if err != nil {
		return false, fmt.Errorf("mark webhook event processed: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// verifyStripeSignature is the production verifier. It tolerates API
// version mismatches because Stripe may deliver events tagged with a
// newer API than the SDK pins; refusing those would block delivery on
// every Stripe API release.
func verifyStripeSignature(payload []byte, sigHeader, secret string) (stripe.Event, error) {
	return stripewebhook.ConstructEventWithOptions(payload, sigHeader, secret, stripewebhook.ConstructEventOptions{
		IgnoreAPIVersionMismatch: true,
	})
}
