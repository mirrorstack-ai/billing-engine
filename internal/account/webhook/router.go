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

	// UnmarkEventProcessed deletes the idempotency row for an event. The router
	// calls it ONLY when dispatch returned a 5xx, so Stripe's redelivery of the
	// same event re-runs the handler instead of being deduped as a duplicate —
	// the mark-before-dispatch ordering (which serializes concurrent duplicate
	// deliveries) would otherwise make the "500 → Stripe retries" recovery a
	// permanent no-op. Handler writes are replay-idempotent, so re-running is safe.
	UnmarkEventProcessed(ctx context.Context, eventID string) error

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
	// Resolves account_id from stripeCustomerID inline. found=false signals
	// Stripe→DB drift (customer.id has no matching accounts row);
	// becameDefault=true means the inserted row was the advisory first-card
	// default and must also be written to the Stripe Customer.
	InsertPaymentMethod(ctx context.Context, stripeCustomerID string, pm InsertPaymentMethodParams) (found bool, becameDefault bool, err error)

	// StampAccountActivated freezes the billing-period anchor (migration 025):
	// the first-card-bind instant, keyed by stripe_customer_id. FIRST-BIND-WINS
	// (WHERE activated_at IS NULL), so a detach + re-add never regresses it and a
	// webhook retry is a no-op. Returns (firstBind, error): firstBind=true only on
	// the row that actually set it (0 rows = already activated OR drift). It never
	// fails the attach path — a stamp error is logged, the mirror insert stands.
	StampAccountActivated(ctx context.Context, stripeCustomerID string) (firstBind bool, err error)

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
	// updates status + amount_paid + amount_due, plus the presentment
	// fields (number / hosted_invoice_url / invoice_pdf, migration 026)
	// SET-ONLY — a non-empty value lands, an empty one never clears an
	// enriched column; period + currency are owned by the charge spine's
	// UpsertInvoice. Returns (found, error)
	// where found=false means the row was not updated — either no mirror
	// row exists yet (drift: the invoice was created out-of-band or the
	// charge spine hasn't mirrored it) OR the monotonic-status guard
	// rejected a stale / out-of-order event (e.g. a late finalized after
	// paid). A not-found is a logged no-op, never an error: webhook delivery
	// is at-least-once + unordered, so the guard MUST tolerate replays and
	// reordering without regressing a terminal status.
	ApplyInvoiceStatus(ctx context.Context, params ApplyInvoiceStatusParams) (found bool, err error)

	// RelaxCollectionOnPaidInvoice is the risk-graded RELAX driver (PR #9): on a
	// paid invoice, conservatively re-trust an account that was tightened to
	// 'prepaid' back to 'arrears' — but ONLY when no open/uncollectible invoice
	// remains for the account. Returns (relaxed bool, error): relaxed=false is a
	// no-op (the account was not prepaid, is still delinquent, or has no mirror
	// row), never an error. It NEVER charges — relax and charge are decoupled.
	RelaxCollectionOnPaidInvoice(ctx context.Context, stripeInvoiceID string) (relaxed bool, err error)

	// MarkInvoiceFailed latches the sticky ever_failed flag (migration 039) on an
	// invoice that failed a payment (invoice.payment_failed / marked_uncollectible),
	// so ServiceBlockSignals' read-time streak derivation counts an invoice still
	// 'open' after a failed charge. Set-only + invoice-keyed, so it is idempotent
	// under Stripe's at-least-once + out-of-order delivery and a safe no-op when
	// the mirror row has not landed. The failed-charge STREAK is DERIVED at read
	// time (not a maintained counter), so there is no account write and nothing to
	// reset on invoice.paid.
	MarkInvoiceFailed(ctx context.Context, stripeInvoiceID string) error

	// FlagPaymentMethodFraud latches fraud_blocked (migration 038) on a disputed /
	// early-fraud-warned card so the service-block gate stops counting it as
	// usable. Card-scoped + account-bounded: every ACTIVE mirror row for the
	// card's fingerprint on the charge's account (fallback: the pm id when the
	// charge carries no fingerprint). Set-only + idempotent. Returns (found,
	// error): found=false (0 rows) is a drift no-op — the card was never
	// mirrored, is already detached, or is already flagged.
	FlagPaymentMethodFraud(ctx context.Context, stripeCustomerID, fingerprint, stripePaymentMethodID, reason string) (found bool, err error)
}

// ApplyInvoiceStatusParams carries the columns ApplyInvoiceStatus reconciles
// onto the invoices mirror row. Money is whole cents (Stripe minor units;
// Stripe invoice amounts are integer cents) — never float.
//
// Number / HostedInvoiceURL / InvoicePDF are the PRESENTMENT fields Stripe
// assigns at finalization (migration 026), carried verbatim from the event
// payload — "" when the event predates finalization (invoice.created). The
// store persists them upsert-style: a non-empty value lands, an empty one
// NEVER clears an already-enriched column back to NULL (Stripe delivers
// at-least-once and unordered; a sparse payload must not un-enrich the
// mirror).
type ApplyInvoiceStatusParams struct {
	StripeInvoiceID string
	Status          string // Stripe invoice status verbatim: draft/open/paid/uncollectible/void
	AmountPaidCents int64
	AmountDueCents  int64

	Number           string // Stripe customer-facing invoice number, e.g. 813C8918-0001
	HostedInvoiceURL string // Stripe-hosted invoice page (view / pay online)
	InvoicePDF       string // direct PDF download link
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
// ChargeRetriever is the narrow Stripe surface the fraud handlers need: resolve
// a charge id (all a dispute / early-fraud-warning event carries) to the card's
// pm id + fingerprint + owning customer. Kept as a webhook-local interface (not
// the full billingstripe.Client) so the Router depends only on what it uses;
// *billingstripe.realClient satisfies it structurally, and tests pass a fake.
type ChargeRetriever interface {
	RetrieveCharge(ctx context.Context, chargeID string) (billingstripe.ChargeCardRef, error)
}

// DefaultPMSetter sets a Stripe Customer's invoice-settings default
// payment method. Narrow on purpose (the Router only needs this one
// Stripe write for the first-card-bind default sync); *billingstripe.realClient
// satisfies it structurally, tests pass a fake.
type DefaultPMSetter interface {
	SetDefaultPaymentMethod(ctx context.Context, stripeCustomerID, stripePaymentMethodID string) error
}

// ServingBlockNotifier is the optional standing-transition hook (funding-gates
// C6, satisfied by *standing.Notifier): after every standing-relevant event —
// invoice lifecycle, card attach/detach, fraud flag — the router pushes the
// owner's CURRENT serving-block verdict to api-platform, best-effort. The
// methods never return errors; the notifier logs and swallows every failure
// so a notify can never fail or delay webhook processing (Stripe must get its
// 200 regardless). nil = disabled.
type ServingBlockNotifier interface {
	NotifyStripeCustomer(ctx context.Context, stripeCustomerID string)
	NotifyStripeInvoice(ctx context.Context, stripeInvoiceID string)
	NotifyStripePaymentMethod(ctx context.Context, stripePaymentMethodID string)
}

type Router struct {
	verifier billingstripe.Verifier
	store    Store
	charges  ChargeRetriever
	pmSetter DefaultPMSetter
	notify   ServingBlockNotifier // nil = serving-block pushes disabled
	log      *slog.Logger
}

// NewRouter wires a Router. All dependencies are required; nil
// values panic at construction. The strict checks catch wiring bugs
// at startup rather than silently falling back to defaults that would
// mask the misconfiguration in production.
func NewRouter(verifier billingstripe.Verifier, store Store, charges ChargeRetriever, pmSetter DefaultPMSetter, log *slog.Logger) *Router {
	if verifier == nil {
		panic("webhook.NewRouter: verifier must not be nil")
	}
	if store == nil {
		panic("webhook.NewRouter: store must not be nil")
	}
	if charges == nil {
		panic("webhook.NewRouter: charges must not be nil")
	}
	if pmSetter == nil {
		panic("webhook.NewRouter: pmSetter must not be nil")
	}
	if log == nil {
		panic("webhook.NewRouter: log must not be nil")
	}
	return &Router{verifier: verifier, store: store, charges: charges, pmSetter: pmSetter, log: log}
}

// WithServingBlockNotifier attaches the optional serving-block notifier
// (nil-tolerant: the hooks no-op without one, so the constructor's strict
// non-nil posture doesn't apply). Returns the Router for chaining.
func (r *Router) WithServingBlockNotifier(n ServingBlockNotifier) *Router {
	r.notify = n
	return r
}

// notifyCustomer / notifyInvoice / notifyPaymentMethod are the nil-guarded
// hook call sites the handlers use — one line at each standing-relevant
// event, after its store writes succeeded.
func (r *Router) notifyCustomer(ctx context.Context, stripeCustomerID string) {
	if r.notify != nil {
		r.notify.NotifyStripeCustomer(ctx, stripeCustomerID)
	}
}

func (r *Router) notifyInvoice(ctx context.Context, stripeInvoiceID string) {
	if r.notify != nil {
		r.notify.NotifyStripeInvoice(ctx, stripeInvoiceID)
	}
}

func (r *Router) notifyPaymentMethod(ctx context.Context, stripePaymentMethodID string) {
	if r.notify != nil {
		r.notify.NotifyStripePaymentMethod(ctx, stripePaymentMethodID)
	}
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
	return r.processVerifiedEvent(ctx, event)
}

// ProcessTrusted runs the idempotency check + dispatch for an event whose
// authenticity is already guaranteed by the transport, skipping signature
// verification entirely. It exists for the EventBridge entry point
// (cmd/account-webhook-eventbridge): trust there is structural — only
// Stripe's partner event source can PutEvents onto the bus, and only that
// Rule's ARN can invoke the Lambda — so there is no HMAC signature to
// verify in the first place. Everything after verification (idempotency,
// dispatch, 5xx compensation) is identical to Process.
func (r *Router) ProcessTrusted(ctx context.Context, event stripego.Event) Result {
	return r.processVerifiedEvent(ctx, event)
}

// processVerifiedEvent is the shared post-verification tail of Process and
// ProcessTrusted: idempotency check, dispatch to the per-event handler, and
// 5xx compensation. Callers are responsible for establishing that event is
// authentic before calling this.
func (r *Router) processVerifiedEvent(ctx context.Context, event stripego.Event) Result {
	// Idempotency: insert the event_id FIRST so concurrent duplicate deliveries
	// of the same event serialize (only one wins the INSERT). A 5xx dispatch
	// outcome is compensated below so the mark doesn't permanently dedupe an
	// event whose side effect never completed.
	firstTime, err := r.store.MarkEventProcessed(ctx, event.ID, string(event.Type))
	if err != nil {
		r.log.ErrorContext(ctx, "idempotency record insert failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !firstTime {
		r.log.InfoContext(ctx, "webhook duplicate", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 200, Status: StatusDuplicate}
	}

	res := r.dispatch(ctx, event)
	if res.HTTPStatus >= 500 {
		// The side effect did not complete. Drop the idempotency row so Stripe's
		// redelivery of this same event re-enters dispatch instead of short-
		// circuiting as a duplicate — otherwise the "500 → Stripe retries"
		// recovery every handler relies on (the fraud flag, the invoice latches)
		// is a permanent no-op. All handler writes are replay-idempotent, so
		// at-least-once re-execution is safe. Best-effort: a failed compensation
		// leaves the event deduped (logged loudly); the residual crash-between-
		// dispatch-and-delete window is far narrower than the Stripe-error window
		// this closes.
		if derr := r.store.UnmarkEventProcessed(ctx, event.ID); derr != nil {
			r.log.ErrorContext(ctx, "idempotency compensation delete failed; redelivery will no-op", "event_id", event.ID, "error", derr)
		}
	}
	return res
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
	case stripego.EventTypeChargeDisputeCreated:
		return r.handleChargeDisputeCreated(ctx, event)
	case stripego.EventTypeRadarEarlyFraudWarningCreated:
		return r.handleEarlyFraudWarningCreated(ctx, event)
	default:
		r.log.InfoContext(ctx, "webhook unhandled event", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 200, Status: StatusUnhandled}
	}
}
