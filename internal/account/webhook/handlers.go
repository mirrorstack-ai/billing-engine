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
// payment_methods_mirror. is_default is set to false initially;
// the follow-on customer.updated event syncs it. This keeps webhook
// processing to a single DB write per event (no synchronous Stripe
// API call inside the transaction).
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
	}
	found, err := r.store.InsertPaymentMethod(ctx, pm.Customer.ID, params)
	if err != nil {
		r.log.ErrorContext(ctx, "payment_method.attached insert failed", "event_id", event.ID, "error", err)
		return Result{HTTPStatus: 500, Status: StatusInternal}
	}
	if !found {
		r.log.WarnContext(ctx, "payment_method.attached drift: no accounts row for customer", "event_id", event.ID, "stripe_customer_id", pm.Customer.ID)
		return Result{HTTPStatus: 200, Status: StatusDriftWarning}
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
	}
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
