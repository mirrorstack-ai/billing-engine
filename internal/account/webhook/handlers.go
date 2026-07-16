package webhook

import (
	"context"
	"encoding/json"
	"errors"

	stripego "github.com/stripe/stripe-go/v85"
)

// handleCustomerCreated is informational — we initiated the customer
// via PrepareAddPaymentMethod, so the accounts row already has the
// stripe_customer_id set. Logging is sufficient; no DB write.
func (r *Router) handleCustomerCreated(ctx context.Context, event stripego.Event) Result {
	r.log.InfoContext(ctx, "webhook customer.created", "event_id", event.ID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handleCustomerUpdated touches accounts.updated_at and syncs the
// default payment method when invoice_settings.default_payment_method
// has changed.
func (r *Router) handleCustomerUpdated(ctx context.Context, event stripego.Event) Result {
	customer, err := decodeCustomer(event)
	if err != nil {
		r.log.WarnContext(ctx, "customer.updated decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}

	found, err := r.store.TouchAccountByStripeCustomer(ctx, customer.ID)
	if err != nil {
		r.log.ErrorContext(ctx, "customer.updated touch failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.WarnContext(ctx, "customer.updated drift: no accounts row for stripe customer", "event_id", event.ID, "stripe_customer_id", customer.ID)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
	}

	defaultPM := ""
	if customer.InvoiceSettings != nil && customer.InvoiceSettings.DefaultPaymentMethod != nil {
		defaultPM = customer.InvoiceSettings.DefaultPaymentMethod.ID
	}
	if err := r.store.SetDefaultPaymentMethod(ctx, customer.ID, defaultPM); err != nil {
		r.log.ErrorContext(ctx, "customer.updated set default PM failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handleCustomerDeleted logs the event. The full erasure cascade
// (detach apps, archive Stripe Customer, delete row) lives in
// PrepareAccountDeletion (future / ms_account erasure design),
// not the webhook. The webhook is a notification, not the
// authoritative source for deletion.
func (r *Router) handleCustomerDeleted(ctx context.Context, event stripego.Event) Result {
	customer, err := decodeCustomer(event)
	if err != nil {
		r.log.WarnContext(ctx, "customer.deleted decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	r.log.InfoContext(ctx, "webhook customer.deleted (notification only)", "event_id", event.ID, "stripe_customer_id", customer.ID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handlePaymentMethodAttached mirrors the new payment method into
// payment_methods_mirror. is_default on this INSERT is ADVISORY: the
// first active card on an account auto-defaults (#14's feature) so a
// new account has a usable default. That advisory first-card default is
// also written to the Stripe Customer's invoice settings here so invoice
// payment and auto-collection can use it; the follow-on customer.updated
// still owns the authoritative mirror sync.
func (r *Router) handlePaymentMethodAttached(ctx context.Context, event stripego.Event) Result {
	pm, err := decodePaymentMethod(event)
	if err != nil {
		r.log.WarnContext(ctx, "payment_method.attached decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if pm.Customer == nil || pm.Customer.ID == "" {
		r.log.WarnContext(ctx, "payment_method.attached missing customer", "event_id", event.ID)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if pm.Card == nil {
		r.log.WarnContext(ctx, "payment_method.attached non-card type — v1 supports cards only", "event_id", event.ID, "type", pm.Type)
		return Result{HTTPStatus: 200, Status: StatusUnhandled}
	}

	params := InsertPaymentMethodParams{
		StripePaymentMethodID: pm.ID,
		Brand:                 string(pm.Card.Brand),
		Last4:                 pm.Card.Last4,
		ExpMonth:              int(pm.Card.ExpMonth),
		ExpYear:               int(pm.Card.ExpYear),
		// Stripe's "same card" identifier across PaymentMethod IDs —
		// the resolver compares this against existing active mirror rows
		// on the same account to set status='duplicate'.
		Fingerprint: pm.Card.Fingerprint,
	}
	found, becameDefault, err := r.store.InsertPaymentMethod(ctx, pm.Customer.ID, params)
	if err != nil {
		r.log.ErrorContext(ctx, "payment_method.attached insert failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.WarnContext(ctx, "payment_method.attached drift: no accounts row for customer", "event_id", event.ID, "stripe_customer_id", pm.Customer.ID)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
	}
	if becameDefault {
		if err := r.pmSetter.SetDefaultPaymentMethod(ctx, pm.Customer.ID, pm.ID); err != nil {
			r.log.ErrorContext(ctx, "payment_method.attached Stripe default sync failed", "event_id", event.ID, "stripe_customer_id", pm.Customer.ID, "stripe_payment_method_id", pm.ID, "error", err)
			return Result{HTTPStatus: 500, Status: StatusInternal}
		}
	}

	// Freeze the billing-period ANCHOR (migration 025) on FIRST card bind: this is
	// the confirmed-bind moment, so activated_at = now() sets the account's
	// period-anchor day. FIRST-BIND-WINS (the query's WHERE activated_at IS NULL),
	// so a detach + re-add never moves it. Best-effort: the card is already
	// mirrored, so a stamp error must not fail the attach — log it and continue
	// (the next card bind, or a backfill, re-stamps the still-NULL anchor).
	if firstBind, err := r.store.StampAccountActivated(ctx, pm.Customer.ID); err != nil {
		r.log.ErrorContext(ctx, "payment_method.attached stamp activation anchor failed (card still mirrored)", "event_id", event.ID, "stripe_customer_id", pm.Customer.ID, "error", err)
	} else if firstBind {
		r.log.InfoContext(ctx, "payment_method.attached froze billing-period anchor (first card bind)", "event_id", event.ID, "stripe_customer_id", pm.Customer.ID)
	}

	// Resolve any pending add-card request keyed on this Stripe PM.
	// Best-effort: setup_intent.succeeded may not have stamped the
	// stripe_pm_id yet (event ordering), in which case this is a no-op
	// and that handler resolves the row instead.
	if err := r.store.ResolvePendingAddCardRequest(ctx, pm.ID); err != nil {
		r.log.ErrorContext(ctx, "payment_method.attached resolve add-card request failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}

	// A new card can flip the standing verdict (no-card → unblocked): push the
	// owner's current serving-block verdict, best-effort (C6).
	r.notifyCustomer(ctx, pm.Customer.ID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handleSetupIntentSucceeded is the primary resolution path for
// add-card requests. The event carries the setup_intent.id (linked
// to ms_billing.add_card_requests.setup_intent_id by Start) and the
// pm.id Stripe ended up attaching. We:
//
//  1. Stamp stripe_pm_id on the matching pending request row so
//     payment_method.attached's eventual resolve can correlate.
//  2. Try to resolve immediately — payment_method.attached may have
//     already arrived (inserting the mirror row); if so we resolve
//     here, if not the attached handler will when it runs.
//
// Both store calls are idempotent against already-resolved rows.
//
// Unresolvable case: if setup_intent.succeeded is never delivered (and
// payment_method.attached never correlates either), the matching
// add_card_requests row stays status='pending' indefinitely. A TTL sweep
// — mark rows >24h old still 'pending' as 'failed' (setup intents expire
// at 24h; see migration 004's operational note) — is a tracked
// follow-up; the request row carries no FK pressure so it is safe to age.
func (r *Router) handleSetupIntentSucceeded(ctx context.Context, event stripego.Event) Result {
	si, err := decodeSetupIntent(event)
	if err != nil {
		r.log.WarnContext(ctx, "setup_intent.succeeded decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if si.PaymentMethod == nil || si.PaymentMethod.ID == "" {
		r.log.WarnContext(ctx, "setup_intent.succeeded missing payment_method", "event_id", event.ID, "setup_intent_id", si.ID)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if err := r.store.SetAddCardRequestStripePM(ctx, si.ID, si.PaymentMethod.ID); err != nil {
		r.log.ErrorContext(ctx, "setup_intent.succeeded stamp stripe_pm_id failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if err := r.store.ResolvePendingAddCardRequest(ctx, si.PaymentMethod.ID); err != nil {
		r.log.ErrorContext(ctx, "setup_intent.succeeded resolve add-card request failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handlePaymentMethodDetached soft-deletes the mirror row. Idempotent:
// a detached event for an already-soft-deleted row (e.g. Stripe retry
// after another agent did the same delete) is a no-op but logged so
// monitoring can distinguish "expected retry" from "never-mirrored PM".
func (r *Router) handlePaymentMethodDetached(ctx context.Context, event stripego.Event) Result {
	pm, err := decodePaymentMethod(event)
	if err != nil {
		r.log.WarnContext(ctx, "payment_method.detached decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if pm.ID == "" {
		r.log.WarnContext(ctx, "payment_method.detached missing pm.ID", "event_id", event.ID)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	found, err := r.store.SoftDeletePaymentMethod(ctx, pm.ID)
	if err != nil {
		r.log.ErrorContext(ctx, "payment_method.detached soft-delete failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.InfoContext(ctx, "payment_method.detached no-op: pm not in mirror", "event_id", event.ID, "stripe_payment_method_id", pm.ID)
		return Result{HTTPStatus: 200, Status: StatusOK}
	}

	// Losing a card can flip the standing verdict (last card gone → blocked):
	// push the owner's current serving-block verdict, best-effort (C6). The
	// detached event's customer field is unreliable post-detach, so the owner
	// resolves through the just-soft-deleted mirror row instead.
	r.notifyPaymentMethod(ctx, pm.ID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// handleInvoiceLifecycle reconciles an invoice.created / .finalized / .paid /
// .payment_failed / .voided / .marked_uncollectible event onto the
// ms_billing.invoices mirror. The six events share one handler because each
// delivers the full Invoice object carrying its current status;
// ApplyInvoiceStatus's monotonic guard (draft<open<terminal) is what makes the
// path safe under Stripe's at-least-once + out-of-order delivery — a replayed
// or late event can never regress a row past a terminal state. payment_failed
// carries status 'open' (Stripe smart-retries the invoice), which is the unpaid
// state Ensure derives the delinquency signal from; voided (status 'void')
// clears that signal and marked_uncollectible (status 'uncollectible') keeps
// it. This handler lands the mirror; the COLLECTION policy is PR #9.
//
// On invoice.paid it ALSO runs the risk-graded RELAX driver
// (RelaxCollectionOnPaidInvoice): a paid invoice that clears the account's last
// delinquency conservatively re-trusts a prepaid account back to arrears. This is
// the inverse of the charge cycle's tighten and the ONLY path that flips an
// account out of prepaid — without it a tightened account would be stuck in
// prepaid forever. It runs AFTER ApplyInvoiceStatus so the just-paid invoice no
// longer counts as unpaid; it never charges (relax and charge are decoupled).
//
// found=false (no mirror row, OR the guard rejected a stale event) is a
// drift_warning, not an error: the charge spine's UpsertInvoice may not have
// mirrored the invoice yet, or the invoice was created out-of-band. We ACK 200
// either way so Stripe stops retrying.
func (r *Router) handleInvoiceLifecycle(ctx context.Context, event stripego.Event) Result {
	inv, err := decodeInvoice(event)
	if err != nil {
		r.log.WarnContext(ctx, "invoice event decode failed", "event_id", event.ID, "type", event.Type, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	if inv.ID == "" {
		r.log.WarnContext(ctx, "invoice event missing invoice id", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}

	// SERVICE-BLOCK failure latch — set BEFORE the found-guard and the status
	// reconcile, on BOTH failure signals (payment_failed leaves the invoice
	// 'open'; marked_uncollectible is a terminal that may arrive first under
	// out-of-order delivery). ever_failed is invoice-keyed and set-only, so this
	// is order-independent and a harmless no-op when the mirror row hasn't landed
	// — the read-time streak derivation (ServiceBlockSignals) does the rest, so
	// there is no counter to advance here and no reset on invoice.paid. A failure
	// here is surfaced (500) so Stripe retries; the latch makes the retry a no-op.
	if event.Type == stripego.EventTypeInvoicePaymentFailed || event.Type == stripego.EventTypeInvoiceMarkedUncollectible {
		if err := r.store.MarkInvoiceFailed(ctx, inv.ID); err != nil {
			r.log.ErrorContext(ctx, "invoice failure latch (ever_failed) failed", "event_id", event.ID, "type", event.Type, "stripe_invoice_id", inv.ID, "error", err)
			return Result{HTTPStatus: 500, Status: StatusInternal}
		}
	}

	found, err := r.store.ApplyInvoiceStatus(ctx, ApplyInvoiceStatusParams{
		StripeInvoiceID: inv.ID,
		// Stripe invoice status mirrored verbatim (draft/open/paid/
		// uncollectible/void); the column is TEXT so a new status never needs
		// a migration.
		Status: string(inv.Status),
		// Stripe amounts are integer cents (minor units).
		AmountPaidCents: inv.AmountPaid,
		AmountDueCents:  inv.AmountDue,
		// Presentment fields (migration 026): Stripe assigns number /
		// hosted_invoice_url / invoice_pdf at FINALIZATION, so they ride every
		// finalized-and-later event and are "" on invoice.created. The store
		// persists them set-only (an empty value never clears), so the mirror
		// enriches on the first event that carries them and stays enriched.
		Number:           inv.Number,
		HostedInvoiceURL: inv.HostedInvoiceURL,
		InvoicePDF:       inv.InvoicePDF,
	})
	if err != nil {
		r.log.ErrorContext(ctx, "invoice event apply status failed", "event_id", event.ID, "type", event.Type, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.WarnContext(ctx, "invoice event drift/stale: no mirror row updated", "event_id", event.ID, "type", event.Type, "stripe_invoice_id", inv.ID, "status", inv.Status)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
	}

	// RELAX driver — invoice.paid only. A failure here is surfaced (500) so Stripe
	// retries: the relax UPDATE is idempotent (a no-op once the account is already
	// arrears), so a retry after the mirror already advanced is safe. The
	// service-block auto-cure needs no write here — paying an invoice moves the
	// ServiceBlockSignals streak cutoff (most-recent paid) forward automatically.
	if event.Type == stripego.EventTypeInvoicePaid {
		relaxed, err := r.store.RelaxCollectionOnPaidInvoice(ctx, inv.ID)
		if err != nil {
			r.log.ErrorContext(ctx, "invoice.paid relax collection failed", "event_id", event.ID, "stripe_invoice_id", inv.ID, "error", err)
			return Result{HTTPStatus: 500, Status: StatusInternal}
		}
		if relaxed {
			r.log.InfoContext(ctx, "invoice.paid relaxed account prepaid → arrears", "event_id", event.ID, "stripe_invoice_id", inv.ID)
		}
	}

	// Any invoice status move can flip the standing verdict (unpaid count,
	// streak, first-charge): push the owner's current serving-block verdict,
	// best-effort (C6). Fired on every lifecycle event rather than diffed —
	// the receiving side is idempotent (see package standing).
	r.notifyInvoice(ctx, inv.ID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// fraud_reason tokens recorded on the mirror row, stable + compact (they feed
// the fraud_reason audit column, migration 038).
const (
	fraudReasonDispute = "dispute"
	fraudReasonEFW     = "early_fraud_warning"
)

// handleChargeDisputeCreated flags the disputed card as fraud (charge.dispute.created).
func (r *Router) handleChargeDisputeCreated(ctx context.Context, event stripego.Event) Result {
	d, err := decodeDispute(event)
	if err != nil {
		r.log.WarnContext(ctx, "charge.dispute.created decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	chargeID := ""
	if d.Charge != nil {
		chargeID = d.Charge.ID
	}
	return r.flagFraudForCharge(ctx, event, chargeID, fraudReasonDispute)
}

// handleEarlyFraudWarningCreated flags the warned card as fraud
// (radar.early_fraud_warning.created). Radar EFWs are issuer-driven and fire in
// LIVE mode only (never test mode), so this path is exercised end-to-end only
// in production; its logic is identical to the dispute path modulo the reason.
func (r *Router) handleEarlyFraudWarningCreated(ctx context.Context, event stripego.Event) Result {
	efw, err := decodeEarlyFraudWarning(event)
	if err != nil {
		r.log.WarnContext(ctx, "radar.early_fraud_warning.created decode failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}
	chargeID := ""
	if efw.Charge != nil {
		chargeID = efw.Charge.ID
	}
	return r.flagFraudForCharge(ctx, event, chargeID, fraudReasonEFW)
}

// flagFraudForCharge is the shared resolve→flag core for the dispute + EFW
// handlers: both events carry only a charge id, so it retrieves the charge to
// get the card's customer + fingerprint + pm id, then latches fraud_blocked on
// the matching mirror rows (card-scoped, account-bounded — see
// FlagPaymentMethodFraud). Failure modes:
//   - missing charge id (malformed event): 400.
//   - Stripe retrieve error: 500 so Stripe redelivers (the flag is idempotent).
//   - charge has no card ref (non-card charge): drift_warning 200, no store call.
//   - no active mirror row matched (never mirrored / detached / already flagged):
//     drift_warning 200, no error.
func (r *Router) flagFraudForCharge(ctx context.Context, event stripego.Event, chargeID, reason string) Result {
	if chargeID == "" {
		r.log.WarnContext(ctx, "fraud event missing charge id", "event_id", event.ID, "type", event.Type)
		return Result{HTTPStatus: 400, Status: StatusInvalidBody}
	}

	ref, err := r.charges.RetrieveCharge(ctx, chargeID)
	if err != nil {
		r.log.ErrorContext(ctx, "fraud charge retrieve failed", "event_id", event.ID, "charge_id", chargeID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if ref.PaymentMethodID == "" && ref.Fingerprint == "" {
		r.log.WarnContext(ctx, "fraud drift: charge carries no card ref", "event_id", event.ID, "charge_id", chargeID)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
	}

	found, err := r.store.FlagPaymentMethodFraud(ctx, ref.StripeCustomerID, ref.Fingerprint, ref.PaymentMethodID, reason)
	if err != nil {
		r.log.ErrorContext(ctx, "fraud flag payment method failed", "event_id", event.ID, "charge_id", chargeID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.WarnContext(ctx, "fraud drift: no active mirror row for charge card (never mirrored / detached / already flagged)", "event_id", event.ID, "charge_id", chargeID)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
	}
	r.log.InfoContext(ctx, "card fraud-blocked", "event_id", event.ID, "type", event.Type, "reason", reason)

	// A fraud-flagged card stops counting as usable, which can flip the
	// standing verdict: push the owner's current serving-block verdict,
	// best-effort (C6).
	r.notifyCustomer(ctx, ref.StripeCustomerID)
	return Result{HTTPStatus: 200, Status: StatusOK}
}

// --- decode helpers -------------------------------------------------------
//
// stripe-go's Event.Data.Raw is the JSON payload of the event's data
// object. We unmarshal directly into the relevant stripe-go struct
// rather than re-decoding the whole event (Event.Data also exposes
// `Object` but as a map[string]any; the typed struct is cleaner).

// errNilEventData guards against a structurally valid but malformed
// event payload where Verify accepts the signature but Data is nil.
// In practice stripe-go's webhook.ConstructEvent populates Data, but
// the nil check costs nothing and prevents a panic in production.
var errNilEventData = errors.New("event.Data is nil")

func decodeCustomer(event stripego.Event) (*stripego.Customer, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var c stripego.Customer
	if err := json.Unmarshal(event.Data.Raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func decodePaymentMethod(event stripego.Event) (*stripego.PaymentMethod, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var pm stripego.PaymentMethod
	if err := json.Unmarshal(event.Data.Raw, &pm); err != nil {
		return nil, err
	}
	return &pm, nil
}

func decodeSetupIntent(event stripego.Event) (*stripego.SetupIntent, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var si stripego.SetupIntent
	if err := json.Unmarshal(event.Data.Raw, &si); err != nil {
		return nil, err
	}
	return &si, nil
}

func decodeInvoice(event stripego.Event) (*stripego.Invoice, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var inv stripego.Invoice
	if err := json.Unmarshal(event.Data.Raw, &inv); err != nil {
		return nil, err
	}
	return &inv, nil
}

func decodeDispute(event stripego.Event) (*stripego.Dispute, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var d stripego.Dispute
	if err := json.Unmarshal(event.Data.Raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

func decodeEarlyFraudWarning(event stripego.Event) (*stripego.RadarEarlyFraudWarning, error) {
	if event.Data == nil {
		return nil, errNilEventData
	}
	var efw stripego.RadarEarlyFraudWarning
	if err := json.Unmarshal(event.Data.Raw, &efw); err != nil {
		return nil, err
	}
	return &efw, nil
}
