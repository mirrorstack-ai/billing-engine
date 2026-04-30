// Package webhook handles Stripe webhook delivery for the account
// (subscription/invoice) surface of the billing-engine.
//
// The flow is:
//
//	1. Verify the Stripe-Signature header against STRIPE_WEBHOOK_SECRET.
//	2. Dedupe by event.id against webhook_events_processed (created in #2).
//	3. Dispatch to a per-event-type handler stub (real handlers land in #8).
//	4. Always return 200 once the event is recorded — Stripe retries
//	   non-2xx, and we do not want a buggy handler to cause infinite redelivery.
//
// Bad signature is the only case that returns 4xx; everything else
// (unknown event type, handler error, replay) returns 200 so Stripe
// stops retrying and we surface the problem in our own logs/metrics.
package webhook

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v85"
	stripewebhook "github.com/stripe/stripe-go/v85/webhook"
)

// EventDB is the subset of pgx the webhook needs. Defined as an
// interface so unit tests can supply a mock without standing up Postgres.
type EventDB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// HandlerFunc handles a single decoded Stripe event. Real handlers
// land in #8; the skeleton uses log-only stubs.
type HandlerFunc func(ctx context.Context, event stripe.Event) error

// Handler is the webhook entrypoint. It owns the signing secret, the
// dedup table, and the dispatch map. Construct one per Lambda cold-start.
type Handler struct {
	secret   string
	db       EventDB
	dispatch map[string]HandlerFunc
	// verify is injected for tests; production uses stripewebhook.ConstructEventWithOptions.
	verify func(payload []byte, sigHeader, secret string) (stripe.Event, error)
}

// markProcessedSQL inserts the event id and reports a conflict via
// RowsAffected()==0. The table is owned by migration #2.
const markProcessedSQL = `
INSERT INTO webhook_events_processed (stripe_event_id, processed_at)
VALUES ($1, now())
ON CONFLICT (stripe_event_id) DO NOTHING
`

// NewHandler wires a Handler with the default Stripe verification path
// and the default stub dispatch table for #7.
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

	inserted, err := h.markProcessed(ctx, event.ID)
	if err != nil {
		// We failed to record the dedup row. Returning 5xx here would
		// trigger a Stripe retry, which is what we want — a transient
		// DB blip should not silently drop the event.
		logger.ErrorContext(ctx, "failed to record webhook event", "error", err)
		return Result{StatusCode: 500, Body: "dedup write failed"}
	}
	if !inserted {
		logger.InfoContext(ctx, "webhook event already processed, skipping dispatch")
		return Result{StatusCode: 200, Body: "duplicate"}
	}

	handler, ok := h.dispatch[string(event.Type)]
	if !ok {
		// Unknown event types should not 4xx; Stripe may add new types
		// and we do not want them to wedge delivery for the fleet.
		logger.InfoContext(ctx, "no handler registered for event type")
		return Result{StatusCode: 200, Body: "ignored"}
	}

	if err := handler(ctx, event); err != nil {
		// Handler errors are logged and swallowed — the dedup row is
		// already committed, so a retry would skip dispatch anyway.
		// Real handlers in #8 are expected to be idempotent and surface
		// their own retry signals via metrics, not via webhook 5xx.
		logger.ErrorContext(ctx, "event handler returned error", "error", err)
	}
	return Result{StatusCode: 200, Body: "ok"}
}

// markProcessed inserts the event id; returns true iff the row was new.
func (h *Handler) markProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := h.db.Exec(ctx, markProcessedSQL, eventID)
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

// defaultDispatch registers stub handlers for the event types we care
// about in #8. Each stub logs and returns nil; #8 replaces them with
// real subscription/invoice sync logic.
func defaultDispatch() map[string]HandlerFunc {
	stub := func(name string) HandlerFunc {
		return func(ctx context.Context, event stripe.Event) error {
			slog.InfoContext(ctx, fmt.Sprintf("received event %s id=%s", name, event.ID))
			return nil
		}
	}
	return map[string]HandlerFunc{
		"customer.subscription.created": stub("customer.subscription.created"),
		"customer.subscription.updated": stub("customer.subscription.updated"),
		"customer.subscription.deleted": stub("customer.subscription.deleted"),
		"invoice.paid":                  stub("invoice.paid"),
		"invoice.payment_failed":        stub("invoice.payment_failed"),
	}
}
