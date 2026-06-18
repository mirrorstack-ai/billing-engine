// Package webhook implements the v1 Stripe webhook router. It handles
// customer + payment_method CRUD plus the invoice lifecycle
// (created/finalized/paid/payment_failed/voided/marked_uncollectible);
// idempotency is enforced via ms_billing.webhook_events_processed. All
// other events ACK with status "unhandled" so Stripe doesn't retry.
//
// Spec: mirrorstack-docs/api/billing/account-webhook.md
package webhook

import (
	"context"
	"log/slog"

	stripego "github.com/stripe/stripe-go/v85"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Status is the body of the {"status": …} envelope returned to Stripe.
// Stripe only inspects HTTP status; the body is for human + log
// inspection.
type Status string

const (
	StatusOK           Status = "ok"
	StatusDuplicate    Status = "duplicate"
	StatusUnhandled    Status = "unhandled"
	StatusBadSignature Status = "bad signature"
	StatusInvalidBody  Status = "invalid body"
	StatusInternal     Status = "internal"
	StatusDriftWarning Status = "drift_warning"
)

// Result is the outcome of processing a webhook. Status maps to the
// JSON body Stripe receives; HTTPStatus is the wire code.
type Result struct {
	HTTPStatus int
	Status     Status
}

// StatusEnvelope is the JSON wire shape returned to Stripe. Defined
// here so every transport (Lambda proxy + local HTTP) marshals the
// same `{"status":"<status>"}` body — kept next to Status so the
// envelope and its allowed values evolve together.
type StatusEnvelope struct {
	Status Status `json:"status"`
}

// Store is the persistence surface the webhook router writes to.
// Narrow on purpose — every method maps to a specific webhook side
// effect — so tests can fake it in-process.
type Store interface {
	// MarkEventProcessed inserts the event_id into webhook_events_processed
	// with an ON CONFLICT DO NOTHING. Returns (firstTime bool, error):
	// firstTime == false means a duplicate was detected and the caller
	// MUST NOT execute the side effect.
	MarkEventProcessed(ctx context.Context, eventID, eventType string) (firstTime bool, err error)

	// TouchAccountByStripeCustomer updates accounts.updated_at for the
	// account matching stripeCustomerID. Used by customer.updated.
	// Returns (found bool, error): missing account is logged as a drift
	// warning but not treated as an error.
	TouchAccountByStripeCustomer(ctx context.Context, stripeCustomerID string) (found bool, err error)

	// SetDefaultPaymentMethod marks defaultStripePMID as is_default=true
	// for the given Stripe customer's account, and false on every other
	// active PM for that account. One-shot for customer.updated when
	// invoice_settings.default_payment_method changes. Empty
	// defaultStripePMID clears the flag everywhere for the account.
	SetDefaultPaymentMethod(ctx context.Context, stripeCustomerID, defaultStripePMID string) error

	// InsertPaymentMethod inserts a row into payment_methods_mirror.
	// Resolves account_id from stripeCustomerID inline; returns
	// (found bool, error) where found=false signals Stripe→DB drift
	// (customer.id has no matching accounts row).
	InsertPaymentMethod(ctx context.Context, stripeCustomerID string, pm InsertPaymentMethodParams) (found bool, err error)

	// SoftDeletePaymentMethod sets deleted_at=now() on the matching
	// stripe_payment_method_id row. Returns (found bool, error) where
	// found=false is a no-op (idempotent on detach).
	SoftDeletePaymentMethod(ctx context.Context, stripePaymentMethodID string) (found bool, err error)

	// SetAddCardRequestStripePM stamps stripe_pm_id onto the still-pending
	// add_card_requests row whose setup_intent_id matches. Called from the
	// setup_intent.succeeded handler. Idempotent on already-resolved rows
	// (UPDATE … WHERE status='pending' filters them out). No row matching
	// the setup_intent_id is a no-op — happens for setup intents created
	// outside the StartAddPaymentMethod flow (e.g. Stripe Dashboard).
	SetAddCardRequestStripePM(ctx context.Context, setupIntentID, stripePaymentMethodID string) error

	// ResolvePendingAddCardRequest flips the matching pending request
	// row from pending → completed (fresh card) or duplicate (the same
	// fingerprint already exists on the account). On duplicate, the
	// just-mirrored row is also soft-deleted so the UI shows one
	// canonical row per real-world card. No matching row OR no mirror
	// row yet → no-op (the partner webhook event resolves it when
	// both have arrived).
	ResolvePendingAddCardRequest(ctx context.Context, stripePaymentMethodID string) error

	// ApplyInvoiceStatus reconciles a Stripe invoice.* event onto the
	// ms_billing.invoices mirror row keyed by stripe_invoice_id. It
	// updates status + amount_paid + amount_due only; period + currency
	// are owned by the charge spine's UpsertInvoice. Returns (found, error)
	// where found=false means the row was not updated — either no mirror
	// row exists yet (drift: the invoice was created out-of-band or the
	// charge spine hasn't mirrored it) OR the monotonic-status guard
	// rejected a stale / out-of-order event (e.g. a late finalized after
	// paid). A not-found is a logged no-op, never an error: webhook delivery
	// is at-least-once + unordered, so the guard MUST tolerate replays and
	// reordering without regressing a terminal status.
	ApplyInvoiceStatus(ctx context.Context, params ApplyInvoiceStatusParams) (found bool, err error)
}

// ApplyInvoiceStatusParams carries the columns ApplyInvoiceStatus reconciles
// onto the invoices mirror row. Money is whole cents (Stripe minor units;
// Stripe invoice amounts are integer cents) — never float.
type ApplyInvoiceStatusParams struct {
	StripeInvoiceID string
	Status          string // Stripe invoice status verbatim: draft/open/paid/uncollectible/void
	AmountPaidCents int64
	AmountDueCents  int64
}

// InsertPaymentMethodParams is the row data extracted from a
// payment_method.attached event's card block. Fingerprint is Stripe's
// canonical "same card" identifier and is the key the duplicate-card
// resolver uses; empty when Stripe omits it on legacy non-card PMs (we
// log + skip those before reaching here, but the type stays tolerant).
type InsertPaymentMethodParams struct {
	StripePaymentMethodID string
	Brand                 string
	Last4                 string
	ExpMonth              int
	ExpYear               int
	Fingerprint           string
}

// Router is the entry point exposed to cmd/account-webhook. It owns
// signature verification + idempotency + per-event dispatch. The
// Lambda binary calls Process; everything else is internal.
type Router struct {
	verifier billingstripe.Verifier
	store    Store
	log      *slog.Logger
}

// NewRouter wires a Router. All three dependencies are required; nil
// values panic at construction. The strict checks catch wiring bugs
// at startup rather than silently falling back to defaults that would
// mask the misconfiguration in production.
func NewRouter(verifier billingstripe.Verifier, store Store, log *slog.Logger) *Router {
	if verifier == nil {
		panic("webhook.NewRouter: verifier must not be nil")
	}
	if store == nil {
		panic("webhook.NewRouter: store must not be nil")
	}
	if log == nil {
		panic("webhook.NewRouter: log must not be nil")
	}
	return &Router{verifier: verifier, store: store, log: log}
}

// Process verifies the signature, performs the idempotency check,
// dispatches to the per-event handler, and returns the Result the
// Lambda binary should write back to Stripe. It never returns an
// error from the function signature — every outcome is mapped to a
// Result so the caller can serialize directly.
func (r *Router) Process(ctx context.Context, payload []byte, signature string) Result {
	event, err := r.verifier.Verify(payload, signature)
	if err != nil {
		r.log.WarnContext(ctx, "webhook signature verify failed", "error", err)
		return Result{HTTPStatus: 400, Status: StatusBadSignature}
	}

	// Idempotency: insert the event_id FIRST. Duplicate → short-circuit
	// before any side effect.
	firstTime, err := r.store.MarkEventProcessed(ctx, event.ID, string(event.Type))
	if err != nil {
		r.log.ErrorContext(ctx, "idempotency record insert failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !firstTime {
		r.log.InfoContext(ctx, "webhook duplicate", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 200, Status: StatusDuplicate}
	}

	return r.dispatch(ctx, event)
}

// dispatch routes to the per-event handler. Unknown events ACK with
// "unhandled" so Stripe doesn't retry events we deliberately ignore.
func (r *Router) dispatch(ctx context.Context, event stripego.Event) Result {
	switch event.Type {
	case stripego.EventTypeCustomerCreated:
		return r.handleCustomerCreated(ctx, event)
	case stripego.EventTypeCustomerUpdated:
		return r.handleCustomerUpdated(ctx, event)
	case stripego.EventTypeCustomerDeleted:
		return r.handleCustomerDeleted(ctx, event)
	case stripego.EventTypePaymentMethodAttached:
		return r.handlePaymentMethodAttached(ctx, event)
	case stripego.EventTypePaymentMethodDetached:
		return r.handlePaymentMethodDetached(ctx, event)
	case stripego.EventTypeSetupIntentSucceeded:
		return r.handleSetupIntentSucceeded(ctx, event)
	case stripego.EventTypeInvoiceCreated,
		stripego.EventTypeInvoiceFinalized,
		stripego.EventTypeInvoicePaid,
		stripego.EventTypeInvoicePaymentFailed,
		stripego.EventTypeInvoiceVoided,
		stripego.EventTypeInvoiceMarkedUncollectible:
		// All six ride the same reconciler: each carries the full Invoice
		// object with its current status, and ApplyInvoiceStatus's monotonic
		// guard decides whether the event advances the mirror. payment_failed
		// leaves the invoice 'open' (Stripe keeps retrying), which is exactly
		// the unpaid state Ensure derives delinquency from — no separate flag.
		// voided (status 'void') and marked_uncollectible (status
		// 'uncollectible') are the terminal collection outcomes the
		// delinquency predicate (AccountHasUnpaidInvoice's IN clause) keys on:
		// void CLEARS the signal (debt forgiven), uncollectible KEEPS it
		// (Stripe gave up but the account still owes) — both must reach the
		// mirror or the row sticks at 'open' and the signal never recovers /
		// never reflects the precise terminal state. The monotonic rank ladder
		// already ranks void/uncollectible as terminal (rank 2), so no handler
		// change is needed — they only need to be dispatched here.
		return r.handleInvoiceLifecycle(ctx, event)
	default:
		r.log.InfoContext(ctx, "webhook unhandled event", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 200, Status: StatusUnhandled}
	}
}
