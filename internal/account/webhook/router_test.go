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

func newRouter(v *webhooktest.FakeVerifier, s *webhooktest.FakeStore) *webhook.Router {
	return webhook.NewRouter(v, s, webhooktest.SilentLogger())
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
}

func TestProcess_PaymentMethodAttached_NoAccountRow_DriftWarning(t *testing.T) {
	v := &webhooktest.FakeVerifier{Event: cardPMEvent("evt_pma2", "payment_method.attached", "pm_x", "cus_orphan", "visa", "4242", 12, 2029)}
	s := webhooktest.NewFakeStore()
	s.InsertFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
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
