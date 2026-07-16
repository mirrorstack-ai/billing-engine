package webhook_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook/webhooktest"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// --- event builders -------------------------------------------------------

func customerEvent(id, eventType, customerID, defaultPMID string) stripego.Event {
	payload := map[string]any{"id": customerID}
	if defaultPMID != "" {
		payload["invoice_settings"] = map[string]any{
			"default_payment_method": map[string]any{"id": defaultPMID},
		}
	}
	raw, _ := json.Marshal(payload)
	return stripego.Event{
		ID:   id,
		Type: stripego.EventType(eventType),
		Data: &stripego.EventData{Raw: raw},
	}
}

func cardPMEvent(id, eventType, pmID, customerID, brand, last4 string, expMonth, expYear int) stripego.Event {
	payload := map[string]any{
		"id":       pmID,
		"type":     "card",
		"customer": map[string]any{"id": customerID},
		"card": map[string]any{
			"brand":     brand,
			"last4":     last4,
			"exp_month": expMonth,
			"exp_year":  expYear,
		},
	}
	raw, _ := json.Marshal(payload)
	return stripego.Event{
		ID:   id,
		Type: stripego.EventType(eventType),
		Data: &stripego.EventData{Raw: raw},
	}
}

// invoiceEvent builds a Stripe invoice.* event carrying the typed Invoice
// fields ApplyInvoiceStatus reads: id, status, amount_paid, amount_due (cents).
func invoiceEvent(id, eventType, invoiceID, status string, amountPaid, amountDue int64) stripego.Event {
	payload := map[string]any{
		"id":          invoiceID,
		"status":      status,
		"amount_paid": amountPaid,
		"amount_due":  amountDue,
	}
	raw, _ := json.Marshal(payload)
	return stripego.Event{
		ID:   id,
		Type: stripego.EventType(eventType),
		Data: &stripego.EventData{Raw: raw},
	}
}

// fraudEvent builds a charge.dispute.created / radar.early_fraud_warning.created
// event carrying a bare charge id (Stripe delivers `charge` unexpanded, so it
// arrives as a "ch_…" string that decodes into Charge{ID}).
func fraudEvent(id, eventType, chargeID string) stripego.Event {
	raw, _ := json.Marshal(map[string]any{"id": "obj_" + id, "charge": chargeID})
	return stripego.Event{ID: id, Type: stripego.EventType(eventType), Data: &stripego.EventData{Raw: raw}}
}

func newRouter(v *webhooktest.FakeVerifier, s *webhooktest.FakeStore) *webhook.Router {
	return newRouterWithCharges(v, s, &webhooktest.FakeChargeRetriever{})
}

func newRouterWithCharges(v *webhooktest.FakeVerifier, s *webhooktest.FakeStore, c *webhooktest.FakeChargeRetriever) *webhook.Router {
	return webhook.NewRouter(v, s, c, c, webhooktest.SilentLogger())
}

// --- tests ----------------------------------------------------------------

func TestProcess_BadSignature(t *testing.T) {
	v := &webhooktest.FakeVerifier{Err: errors.New("signature mismatch")}
	r := newRouter(v, webhooktest.NewFakeStore())

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 400, res.HTTPStatus)
	require.Equal(t, webhook.StatusBadSignature, res.Status)
}

func TestProcess_Duplicate(t *testing.T) {
	event := customerEvent("evt_dup_1", "customer.created", "cus_x", "")
	v := &webhooktest.FakeVerifier{Event: event}
	s := webhooktest.NewFakeStore()
	s.Processed["evt_dup_1"] = true
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDuplicate, res.Status)
}

func TestProcess_IdempotencyInsertError(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: customerEvent("evt_e", "customer.created", "cus_x", "")}
	s := webhooktest.NewFakeStore()
	s.ErrMark = errors.New("db down")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

func TestProcess_CustomerCreated_LogOnly(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: customerEvent("evt_c1", "customer.created", "cus_x", "")}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Empty(t, s.Inserts)
	require.Empty(t, s.SoftDeletes)
	require.Empty(t, s.DefaultsSet)
}

func TestProcess_CustomerUpdated_SyncsDefault(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: customerEvent("evt_u1", "customer.updated", "cus_x", "pm_default")}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"cus_x=pm_default"}, s.DefaultsSet)
}

func TestProcess_CustomerUpdated_NoAccountRow_DriftWarning(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: customerEvent("evt_u2", "customer.updated", "cus_orphan", "")}
	s := webhooktest.NewFakeStore()
	s.TouchedFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Empty(t, s.DefaultsSet)
}

func TestProcess_CustomerDeleted_LogOnly(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: customerEvent("evt_d1", "customer.deleted", "cus_x", "")}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
}

func TestProcess_PaymentMethodAttached_InsertsMirror(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma1", "payment_method.attached", "pm_x", "cus_x", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.Inserts, 1)
	require.Equal(t, "pm_x", s.Inserts[0].StripePaymentMethodID)
	require.Equal(t, "visa", s.Inserts[0].Brand)
	require.Equal(t, "4242", s.Inserts[0].Last4)
	require.Equal(t, 12, s.Inserts[0].ExpMonth)
	require.Equal(t, 2029, s.Inserts[0].ExpYear)
	// The attach also freezes the billing-period anchor (migration 025) for the
	// customer's account — the confirmed first-card-bind moment.
	require.Equal(t, []string{"cus_x"}, s.ActivatedCustomers)
}

func TestProcess_PaymentMethodAttached_FirstCardSetsStripeDefault(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma_default", "payment_method.attached", "pm_first", "cus_first", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.InsertBecameDefault = true
	stripe := &webhooktest.FakeChargeRetriever{}
	r := newRouterWithCharges(v, s, stripe)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"cus_first=pm_first"}, stripe.DefaultsSet)
}

func TestProcess_PaymentMethodAttached_NonDefaultCardSkipsStripeDefault(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma_nondefault", "payment_method.attached", "pm_second", "cus_cards", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.InsertBecameDefault = false
	stripe := &webhooktest.FakeChargeRetriever{}
	r := newRouterWithCharges(v, s, stripe)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Empty(t, stripe.DefaultsSet)
}

func TestProcess_PaymentMethodAttached_DefaultSetterErrorIsInternal(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma_default_error", "payment_method.attached", "pm_first", "cus_first", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.InsertBecameDefault = true
	stripe := &webhooktest.FakeChargeRetriever{ErrSetDefault: errors.New("stripe down")}
	r := newRouterWithCharges(v, s, stripe)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
	require.Equal(t, []string{"cus_first=pm_first"}, stripe.DefaultsSet)
}

func TestProcess_PaymentMethodAttached_NoAccountRow_DriftWarning(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma2", "payment_method.attached", "pm_x", "cus_orphan", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.InsertFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	// Drift (no accounts row) must NOT stamp an anchor — the handler returns
	// before the stamp when the mirror insert found no account.
	require.Empty(t, s.ActivatedCustomers)
}

// TestProcess_PaymentMethodAttached_StampErrorIsBestEffort proves an
// activation-stamp failure never fails the attach: the card is still mirrored and
// the webhook ACKs 200/OK (the next bind re-stamps the still-NULL anchor).
func TestProcess_PaymentMethodAttached_StampErrorIsBestEffort(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma3", "payment_method.attached", "pm_x", "cus_x", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.ErrActivate = errors.New("anchor stamp db error")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.Inserts, 1, "card is still mirrored despite the stamp error")
}

func TestProcess_PaymentMethodDetached_SoftDeletes(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pmd1", "payment_method.detached", "pm_x", "cus_x", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"pm_x"}, s.SoftDeletes)
}

func TestProcess_UnknownEvent_Acks(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: stripego.Event{
		ID:   "evt_unknown",
		Type: stripego.EventType("charge.refunded"),
		Data: &stripego.EventData{Raw: []byte(`{}`)},
	}}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.Inserts)
	require.Empty(t, s.SoftDeletes)
	require.Empty(t, s.AppliedInvoices)
}

// TestProcess_UnhandledInvoiceSubtype_Acks confirms an invoice.* sub-state we
// do NOT switch on (e.g. invoice.updated) falls through to the default
// 200/unhandled — it never reaches ApplyInvoiceStatus. (voided +
// marked_uncollectible ARE handled — see the dedicated tests below.)
func TestProcess_UnhandledInvoiceSubtype_Acks(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_upd", "invoice.updated", "in_x", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.AppliedInvoices, "an unhandled invoice sub-type must not reconcile")
}

func TestProcess_PaymentMethodAttached_NonCard_Unhandled(t *testing.T) {
	payload := map[string]any{
		"id":       "pm_sepa_x",
		"type":     "sepa_debit",
		"customer": map[string]any{"id": "cus_x"},
	}
	raw, _ := json.Marshal(payload)
	v := &webhooktest.FakeVerifier{Event: stripego.Event{
		ID:   "evt_pma_sepa",
		Type: stripego.EventType("payment_method.attached"),
		Data: &stripego.EventData{Raw: raw},
	}}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.Inserts)
}

// --- invoice lifecycle reconciliation -------------------------------------

func TestProcess_InvoiceCreated_AppliesStatus(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_c", "invoice.created", "in_1", "draft", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1)
	require.Equal(t, "in_1", s.AppliedInvoices[0].StripeInvoiceID)
	require.Equal(t, "draft", s.AppliedInvoices[0].Status)
	require.Equal(t, int64(0), s.AppliedInvoices[0].AmountPaidCents)
	require.Equal(t, int64(1200), s.AppliedInvoices[0].AmountDueCents)
}

func TestProcess_InvoiceFinalized_AppliesOpen(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_f", "invoice.finalized", "in_1", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1)
	require.Equal(t, "open", s.AppliedInvoices[0].Status)
}

// TestProcess_InvoiceFinalized_CarriesPresentmentFields: the handler forwards
// number / hosted_invoice_url / invoice_pdf (migration 026) from the event
// payload into ApplyInvoiceStatusParams verbatim — Stripe assigns them at
// finalization, so this event is the first one that can enrich the mirror.
// The set-only (never-clear) semantics live in the SQL, exercised by the
// store integration test; here we pin the decode → params plumbing.
func TestProcess_InvoiceFinalized_CarriesPresentmentFields(t *testing.T) {
	payload := map[string]any{
		"id":                 "in_1",
		"status":             "open",
		"amount_paid":        int64(0),
		"amount_due":         int64(1200),
		"number":             "813C8918-0001",
		"hosted_invoice_url": "https://invoice.stripe.com/i/in_1",
		"invoice_pdf":        "https://pay.stripe.com/invoice/in_1/pdf",
	}
	raw, _ := json.Marshal(payload)
	v := &webhooktest.FakeVerifier{Event: stripego.Event{
		ID:   "evt_inv_f_pres",
		Type: stripego.EventType("invoice.finalized"),
		Data: &stripego.EventData{Raw: raw},
	}}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1)
	applied := s.AppliedInvoices[0]
	require.Equal(t, "813C8918-0001", applied.Number)
	require.Equal(t, "https://invoice.stripe.com/i/in_1", applied.HostedInvoiceURL)
	require.Equal(t, "https://pay.stripe.com/invoice/in_1/pdf", applied.InvoicePDF)
}

// TestProcess_InvoiceCreated_EmptyPresentmentFields: invoice.created predates
// finalization, so the payload carries none of the presentment fields and the
// params must pass "" through (which the store treats as "keep stored").
func TestProcess_InvoiceCreated_EmptyPresentmentFields(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_c_pres", "invoice.created", "in_1", "draft", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Len(t, s.AppliedInvoices, 1)
	require.Empty(t, s.AppliedInvoices[0].Number)
	require.Empty(t, s.AppliedInvoices[0].HostedInvoiceURL)
	require.Empty(t, s.AppliedInvoices[0].InvoicePDF)
}

func TestProcess_InvoicePaid_RecordsAmountPaid(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_p", "invoice.paid", "in_1", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1)
	require.Equal(t, "paid", s.AppliedInvoices[0].Status)
	require.Equal(t, int64(1200), s.AppliedInvoices[0].AmountPaidCents)
	// invoice.paid ALSO runs the relax driver (PR #9).
	require.Equal(t, []string{"in_1"}, s.RelaxedInvoices, "invoice.paid must attempt the prepaid → arrears relax")
}

// --- risk-graded RELAX driver (PR #9) -------------------------------------

func TestProcess_InvoicePaid_RelaxesPrepaidAccount(t *testing.T) {
	// A paid invoice that clears the last delinquency flips the account back to
	// arrears: the store reports relaxed=true, the handler ACKs ok.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_relax", "invoice.paid", "in_relax", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	s.Relaxed = true
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"in_relax"}, s.RelaxedInvoices)
}

func TestProcess_NonPaidInvoiceEvent_DoesNotRelax(t *testing.T) {
	// Only invoice.paid drives the relax. A finalized/failed/void event lands the
	// mirror but NEVER attempts to re-trust the account.
	for _, ev := range []struct{ name, typ, status string }{
		{"finalized", "invoice.finalized", "open"},
		{"payment_failed", "invoice.payment_failed", "open"},
		{"voided", "invoice.voided", "void"},
		{"uncollectible", "invoice.marked_uncollectible", "uncollectible"},
	} {
		t.Run(ev.name, func(t *testing.T) {
			v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_"+ev.name, ev.typ, "in_x", ev.status, 0, 1200)}
			s := webhooktest.NewFakeStore()
			r := newRouter(v, s)

			res := r.Process(context.Background(), []byte(`{}`), "sig")

			require.Equal(t, webhook.StatusOK, res.Status)
			require.Empty(t, s.RelaxedInvoices, "only invoice.paid relaxes the account")
		})
	}
}

func TestProcess_InvoicePaid_DriftMirror_DoesNotRelax(t *testing.T) {
	// If the invoice has no mirror row (drift), the relax driver is not reached —
	// the handler returns drift_warning before the relax step.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_relax_drift", "invoice.paid", "in_orphan", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	s.InvoiceFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Empty(t, s.RelaxedInvoices, "drift short-circuits before the relax driver")
}

func TestProcess_InvoicePaid_RelaxError_Internal(t *testing.T) {
	// A relax-driver store error surfaces as 500 so Stripe retries (the relax
	// UPDATE is idempotent, so a retry is safe).
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_relax_err", "invoice.paid", "in_1", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	s.ErrRelax = errors.New("db down")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

func TestProcess_InvoicePaymentFailed_StaysOpen(t *testing.T) {
	// payment_failed leaves the invoice 'open' (Stripe smart-retries); that
	// is the unpaid state Ensure derives delinquency from — no separate flag.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_pf", "invoice.payment_failed", "in_1", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1)
	require.Equal(t, "open", s.AppliedInvoices[0].Status)
	require.Equal(t, int64(0), s.AppliedInvoices[0].AmountPaidCents)
}

// --- service-block failure latch (migration 039; streak derived at read time) ---

func TestProcess_FailureEvents_LatchEverFailed(t *testing.T) {
	// Both failure signals latch ever_failed via MarkInvoiceFailed — and BEFORE
	// the found-guard, so an out-of-order marked_uncollectible (or a not-yet-
	// mirrored invoice) still latches. No account write happens; the streak is
	// derived at read time.
	for _, ev := range []struct{ name, typ, status string }{
		{"payment_failed", "invoice.payment_failed", "open"},
		{"marked_uncollectible", "invoice.marked_uncollectible", "uncollectible"},
	} {
		t.Run(ev.name, func(t *testing.T) {
			v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_lat_"+ev.name, ev.typ, "in_1", ev.status, 0, 1200)}
			s := webhooktest.NewFakeStore()
			r := newRouter(v, s)

			res := r.Process(context.Background(), []byte(`{}`), "sig")

			require.Equal(t, 200, res.HTTPStatus)
			require.Equal(t, webhook.StatusOK, res.Status)
			require.Equal(t, []string{"in_1"}, s.FailedInvoices, "failure events must latch ever_failed")
		})
	}
}

func TestProcess_FailureLatch_RunsEvenOnDriftMirror(t *testing.T) {
	// The latch is set BEFORE the found-guard, so a payment_failed for a not-yet-
	// mirrored invoice still calls MarkInvoiceFailed (a store-side no-op) — this
	// is what makes the read-time streak order-independent. The event still ACKs
	// drift (the status reconcile found no row).
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_lat_drift", "invoice.payment_failed", "in_orphan", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	s.InvoiceFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Equal(t, []string{"in_orphan"}, s.FailedInvoices, "latch runs ahead of the found-guard")
}

func TestProcess_NonFailureInvoiceEvent_DoesNotLatch(t *testing.T) {
	// Only the two failure signals latch ever_failed. created/finalized/paid/void
	// land the mirror but never mark the invoice failed.
	for _, ev := range []struct{ name, typ, status string }{
		{"created", "invoice.created", "draft"},
		{"finalized", "invoice.finalized", "open"},
		{"paid", "invoice.paid", "paid"},
		{"voided", "invoice.voided", "void"},
	} {
		t.Run(ev.name, func(t *testing.T) {
			v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_nl_"+ev.name, ev.typ, "in_x", ev.status, 0, 1200)}
			s := webhooktest.NewFakeStore()
			r := newRouter(v, s)

			res := r.Process(context.Background(), []byte(`{}`), "sig")

			require.Equal(t, webhook.StatusOK, res.Status)
			require.Empty(t, s.FailedInvoices, "only failure events latch ever_failed")
		})
	}
}

func TestProcess_FailureLatchError_Internal(t *testing.T) {
	// A latch store error surfaces as 500 so Stripe retries; the set-only latch
	// makes the retry a harmless no-op.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_lat_err", "invoice.payment_failed", "in_1", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	s.ErrMarkFailed = errors.New("db down")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

// --- fraud webhook (charge.dispute.created / radar.early_fraud_warning.created) ---

func fraudRetriever(chargeID string, ref billingstripe.ChargeCardRef) *webhooktest.FakeChargeRetriever {
	return &webhooktest.FakeChargeRetriever{Refs: map[string]billingstripe.ChargeCardRef{chargeID: ref}}
}

func TestProcess_ChargeDisputeCreated_FlagsCard(t *testing.T) {
	// dispute → retrieve the charge → flag the card with reason "dispute".
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp", "charge.dispute.created", "ch_1")}
	s := webhooktest.NewFakeStore()
	c := fraudRetriever("ch_1", billingstripe.ChargeCardRef{PaymentMethodID: "pm_1", Fingerprint: "fp_1", StripeCustomerID: "cus_1"})
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.FraudFlags, 1)
	require.Equal(t, webhooktest.FraudFlag{StripeCustomerID: "cus_1", Fingerprint: "fp_1", StripePaymentMethodID: "pm_1", Reason: "dispute"}, s.FraudFlags[0])
}

func TestProcess_EarlyFraudWarningCreated_FlagsCard(t *testing.T) {
	// EFW → same resolve+flag with reason "early_fraud_warning".
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_efw", "radar.early_fraud_warning.created", "ch_2")}
	s := webhooktest.NewFakeStore()
	c := fraudRetriever("ch_2", billingstripe.ChargeCardRef{PaymentMethodID: "pm_2", Fingerprint: "fp_2", StripeCustomerID: "cus_2"})
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.FraudFlags, 1)
	require.Equal(t, "early_fraud_warning", s.FraudFlags[0].Reason)
}

func TestProcess_Fraud_NoMirrorMatch_DriftWarning(t *testing.T) {
	// The card isn't in our mirror (or is already flagged): store reports 0 rows
	// → drift_warning 200, no error. The store WAS consulted.
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp_drift", "charge.dispute.created", "ch_3")}
	s := webhooktest.NewFakeStore()
	s.FraudFound = false
	c := fraudRetriever("ch_3", billingstripe.ChargeCardRef{PaymentMethodID: "pm_3", Fingerprint: "fp_3", StripeCustomerID: "cus_3"})
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Len(t, s.FraudFlags, 1)
}

func TestProcess_Fraud_NonCardCharge_DriftNoStoreCall(t *testing.T) {
	// A charge with no card ref (empty pm + fingerprint) → drift_warning, and the
	// store is never consulted.
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp_noncard", "charge.dispute.created", "ch_4")}
	s := webhooktest.NewFakeStore()
	c := fraudRetriever("ch_4", billingstripe.ChargeCardRef{StripeCustomerID: "cus_4"}) // no pm, no fingerprint
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Empty(t, s.FraudFlags, "non-card charge must not reach the store")
}

func TestProcess_Fraud_MissingChargeID_BadRequest(t *testing.T) {
	// A dispute event with no charge → 400, retriever never called.
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp_nocharge", "charge.dispute.created", "")}
	s := webhooktest.NewFakeStore()
	c := &webhooktest.FakeChargeRetriever{} // any charge id resolves to zero ref
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 400, res.HTTPStatus)
	require.Equal(t, webhook.StatusInvalidBody, res.Status)
	require.Empty(t, s.FraudFlags)
}

func TestProcess_Fraud_RetrieveError_Internal(t *testing.T) {
	// A Stripe retrieve failure → 500 so Stripe redelivers; the store is never
	// reached (the flag is idempotent, so the redelivery is safe).
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp_rerr", "charge.dispute.created", "ch_5")}
	s := webhooktest.NewFakeStore()
	c := &webhooktest.FakeChargeRetriever{Err: errors.New("stripe down")}
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
	require.Empty(t, s.FraudFlags)
}

func TestProcess_Fraud_FlagStoreError_Internal(t *testing.T) {
	// A store error while flagging → 500 so Stripe redelivers.
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_disp_serr", "charge.dispute.created", "ch_6")}
	s := webhooktest.NewFakeStore()
	s.ErrFlagFraud = errors.New("db down")
	c := fraudRetriever("ch_6", billingstripe.ChargeCardRef{PaymentMethodID: "pm_6", Fingerprint: "fp_6", StripeCustomerID: "cus_6"})
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

// --- 5xx idempotency compensation (redelivery recovery) -------------------

func TestProcess_5xxDispatch_UnmarksSoRedeliveryReRuns(t *testing.T) {
	// A 5xx dispatch outcome must DROP the idempotency row, so Stripe's
	// redelivery of the same event re-enters the handler instead of being deduped
	// forever (the "500 → Stripe retries" recovery the handlers document).
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_retry", "charge.dispute.created", "ch_1")}
	s := webhooktest.NewFakeStore()
	c := &webhooktest.FakeChargeRetriever{Err: errors.New("stripe 503")}
	r := newRouterWithCharges(v, s, c)

	res := r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, 500, res.HTTPStatus)
	require.False(t, s.Processed["evt_retry"], "a 5xx must unmark the event for redelivery")

	// Redelivery with a now-healthy retriever actually applies the flag.
	c.Err = nil
	c.Refs = map[string]billingstripe.ChargeCardRef{"ch_1": {PaymentMethodID: "pm_1", Fingerprint: "fp_1", StripeCustomerID: "cus_1"}}
	res = r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.FraudFlags, 1, "redelivery re-ran the handler and flagged the card")
}

func TestProcess_2xxDispatch_StaysDedupedOnRedelivery(t *testing.T) {
	// A successful (2xx) event stays recorded, so a redelivery is deduped — the
	// compensation must fire ONLY on 5xx.
	v := &webhooktest.FakeVerifier{Event: fraudEvent("evt_ok", "charge.dispute.created", "ch_1")}
	s := webhooktest.NewFakeStore()
	c := fraudRetriever("ch_1", billingstripe.ChargeCardRef{PaymentMethodID: "pm_1", Fingerprint: "fp_1", StripeCustomerID: "cus_1"})
	r := newRouterWithCharges(v, s, c)

	require.Equal(t, webhook.StatusOK, r.Process(context.Background(), []byte(`{}`), "sig").Status)
	require.True(t, s.Processed["evt_ok"], "a 2xx stays recorded")

	res := r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, webhook.StatusDuplicate, res.Status)
	require.Len(t, s.FraudFlags, 1, "a deduped redelivery must not re-flag")
}

func TestProcess_InvoiceVoided_AppliesVoid(t *testing.T) {
	// An admin/Stripe void (invoice.voided, status 'void') MUST reach the
	// reconciler so the mirror leaves 'open' and the delinquency signal clears.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_void", "invoice.voided", "in_1", "void", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1, "invoice.voided must reconcile onto the mirror")
	require.Equal(t, "void", s.AppliedInvoices[0].Status)
}

func TestProcess_InvoiceMarkedUncollectible_AppliesUncollectible(t *testing.T) {
	// invoice.marked_uncollectible (status 'uncollectible') MUST reach the
	// reconciler so the mirror records the precise terminal state; the
	// delinquency predicate keeps the signal TRUE for uncollectible.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_unc", "invoice.marked_uncollectible", "in_1", "uncollectible", 0, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.AppliedInvoices, 1, "invoice.marked_uncollectible must reconcile onto the mirror")
	require.Equal(t, "uncollectible", s.AppliedInvoices[0].Status)
}

func TestProcess_InvoiceEvent_Duplicate_NoDoubleApply(t *testing.T) {
	// Same event_id replayed → the idempotency gate short-circuits BEFORE
	// dispatch, so ApplyInvoiceStatus is never called the second time.
	event := invoiceEvent("evt_inv_dup", "invoice.paid", "in_1", "paid", 1200, 1200)
	v := &webhooktest.FakeVerifier{Event: event}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res1 := r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, webhook.StatusOK, res1.Status)

	res2 := r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, 200, res2.HTTPStatus)
	require.Equal(t, webhook.StatusDuplicate, res2.Status)

	require.Len(t, s.AppliedInvoices, 1, "duplicate event must not re-apply the invoice status")
}

func TestProcess_InvoiceEvent_Drift_NoMirrorRow(t *testing.T) {
	// A finalized event for an invoice the charge spine never mirrored:
	// ApplyInvoiceStatus returns found=false → drift_warning, ACK 200.
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_drift", "invoice.finalized", "in_orphan", "open", 0, 1200)}
	s := webhooktest.NewFakeStore()
	s.InvoiceFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Len(t, s.AppliedInvoices, 1, "the apply is attempted; the store reports not-found")
}

func TestProcess_InvoiceEvent_MissingInvoiceID_InvalidBody(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_noid", "invoice.paid", "", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 400, res.HTTPStatus)
	require.Equal(t, webhook.StatusInvalidBody, res.Status)
	require.Empty(t, s.AppliedInvoices)
}

func TestProcess_InvoiceEvent_StoreError_Internal(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: invoiceEvent("evt_inv_err", "invoice.paid", "in_1", "paid", 1200, 1200)}
	s := webhooktest.NewFakeStore()
	s.ErrApplyInvoice = errors.New("db down")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

// --- ProcessTrusted (EventBridge entry point) -----------------------------

// TestProcessTrusted_SkipsVerification_DispatchesDespiteFailingVerifier proves
// the Process/ProcessTrusted split: both are wired to the SAME Router (and
// therefore the same always-erroring FakeVerifier). Process calls
// r.verifier.Verify first and fails closed at 400/bad-signature — the
// verifier IS reached and IS the reason it fails. ProcessTrusted, called
// against that identical router, reaches dispatch and ACKs 200/OK — proving
// it never calls r.verifier.Verify at all (an EventBridge event never
// carries a signature to verify in the first place; trust is structural).
func TestProcessTrusted_SkipsVerification_DispatchesDespiteFailingVerifier(t *testing.T) {
	v := &webhooktest.FakeVerifier{Err: errors.New("signature mismatch")}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	// Sanity: Process against this router+verifier fails closed.
	processRes := r.Process(context.Background(), []byte(`{}`), "sig")
	require.Equal(t, 400, processRes.HTTPStatus)
	require.Equal(t, webhook.StatusBadSignature, processRes.Status)

	// ProcessTrusted against the identical router bypasses the verifier
	// entirely and reaches dispatch.
	event := customerEvent("evt_trusted_1", "customer.created", "cus_x", "")
	trustedRes := r.ProcessTrusted(context.Background(), event)
	require.Equal(t, 200, trustedRes.HTTPStatus)
	require.Equal(t, webhook.StatusOK, trustedRes.Status)
	require.True(t, s.Processed["evt_trusted_1"], "ProcessTrusted must still run the idempotency + dispatch tail")
}

// TestProcessTrusted_Duplicate proves ProcessTrusted shares the same
// idempotency gate as Process (both delegate to processVerifiedEvent): a
// second delivery of the same event_id short-circuits to StatusDuplicate
// without re-dispatching.
func TestProcessTrusted_Duplicate(t *testing.T) {
	v := &webhooktest.FakeVerifier{Err: errors.New("signature mismatch")}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)
	event := customerEvent("evt_trusted_dup", "customer.created", "cus_x", "")

	first := r.ProcessTrusted(context.Background(), event)
	require.Equal(t, webhook.StatusOK, first.Status)

	second := r.ProcessTrusted(context.Background(), event)
	require.Equal(t, 200, second.HTTPStatus)
	require.Equal(t, webhook.StatusDuplicate, second.Status)
}

// TestProcessTrusted_5xxDispatch_UnmarksSoRedeliveryReRuns proves the 5xx
// idempotency compensation (shared via processVerifiedEvent) also applies to
// the ProcessTrusted path — EventBridge's own retry redelivers the same
// event, and it must re-enter dispatch rather than dedupe forever.
func TestProcessTrusted_5xxDispatch_UnmarksSoRedeliveryReRuns(t *testing.T) {
	v := &webhooktest.FakeVerifier{Err: errors.New("signature mismatch")}
	s := webhooktest.NewFakeStore()
	c := &webhooktest.FakeChargeRetriever{Err: errors.New("stripe 503")}
	r := newRouterWithCharges(v, s, c)
	event := fraudEvent("evt_trusted_retry", "charge.dispute.created", "ch_1")

	res := r.ProcessTrusted(context.Background(), event)
	require.Equal(t, 500, res.HTTPStatus)
	require.False(t, s.Processed["evt_trusted_retry"], "a 5xx must unmark the event for redelivery")

	c.Err = nil
	c.Refs = map[string]billingstripe.ChargeCardRef{"ch_1": {PaymentMethodID: "pm_1", Fingerprint: "fp_1", StripeCustomerID: "cus_1"}}
	res = r.ProcessTrusted(context.Background(), event)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.FraudFlags, 1, "redelivery re-ran the handler and flagged the card")
}
