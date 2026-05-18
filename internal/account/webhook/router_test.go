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
		Type: stripego.EventType("invoice.paid"),
		Data: &stripego.EventData{Raw: []byte(`{}`)},
	}}
	s := webhooktest.NewFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.Inserts)
	require.Empty(t, s.SoftDeletes)
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
