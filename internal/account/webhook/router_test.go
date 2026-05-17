package webhook_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
)

// --- fakes ----------------------------------------------------------------

type fakeVerifier struct {
	event stripego.Event
	err   error
}

func (f *fakeVerifier) Verify(_ []byte, _ string) (stripego.Event, error) {
	return f.event, f.err
}

type fakeStore struct {
	processed     map[string]bool
	touchedFound  bool
	defaultsSet   []string
	inserts       []webhook.InsertPaymentMethodParams
	softDeletes   []string
	insertFound   bool
	softDelFound  bool
	errMark       error
	errTouch      error
	errSetDefault error
	errInsert     error
	errSoftDel    error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		processed:    map[string]bool{},
		touchedFound: true,
		insertFound:  true,
		softDelFound: true,
	}
}

func (s *fakeStore) MarkEventProcessed(_ context.Context, eventID, _ string) (bool, error) {
	if s.errMark != nil {
		return false, s.errMark
	}
	if s.processed[eventID] {
		return false, nil
	}
	s.processed[eventID] = true
	return true, nil
}

func (s *fakeStore) TouchAccountByStripeCustomer(_ context.Context, _ string) (bool, error) {
	if s.errTouch != nil {
		return false, s.errTouch
	}
	return s.touchedFound, nil
}

func (s *fakeStore) SetDefaultPaymentMethod(_ context.Context, customerID, defaultPM string) error {
	if s.errSetDefault != nil {
		return s.errSetDefault
	}
	s.defaultsSet = append(s.defaultsSet, customerID+"="+defaultPM)
	return nil
}

func (s *fakeStore) InsertPaymentMethod(_ context.Context, _ string, params webhook.InsertPaymentMethodParams) (bool, error) {
	if s.errInsert != nil {
		return false, s.errInsert
	}
	s.inserts = append(s.inserts, params)
	return s.insertFound, nil
}

func (s *fakeStore) SoftDeletePaymentMethod(_ context.Context, stripePMID string) (bool, error) {
	if s.errSoftDel != nil {
		return false, s.errSoftDel
	}
	s.softDeletes = append(s.softDeletes, stripePMID)
	return s.softDelFound, nil
}

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

var silentLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

func newRouter(v *fakeVerifier, s *fakeStore) *webhook.Router {
	return webhook.NewRouter(v, s, silentLogger)
}

// --- tests ----------------------------------------------------------------

func TestProcess_BadSignature(t *testing.T) {
	v := &fakeVerifier{err: errors.New("signature mismatch")}
	r := newRouter(v, newFakeStore())

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 400, res.HTTPStatus)
	require.Equal(t, webhook.StatusBadSignature, res.Status)
}

func TestProcess_Duplicate(t *testing.T) {
	event := customerEvent("evt_dup_1", "customer.created", "cus_x", "")
	v := &fakeVerifier{event: event}
	s := newFakeStore()
	s.processed["evt_dup_1"] = true
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDuplicate, res.Status)
}

func TestProcess_IdempotencyInsertError(t *testing.T) {
	v := &fakeVerifier{event: customerEvent("evt_e", "customer.created", "cus_x", "")}
	s := newFakeStore()
	s.errMark = errors.New("db down")
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 500, res.HTTPStatus)
	require.Equal(t, webhook.StatusInternal, res.Status)
}

func TestProcess_CustomerCreated_LogOnly(t *testing.T) {
	v := &fakeVerifier{event: customerEvent("evt_c1", "customer.created", "cus_x", "")}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Empty(t, s.inserts)
	require.Empty(t, s.softDeletes)
	require.Empty(t, s.defaultsSet)
}

func TestProcess_CustomerUpdated_SyncsDefault(t *testing.T) {
	v := &fakeVerifier{event: customerEvent("evt_u1", "customer.updated", "cus_x", "pm_default")}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"cus_x=pm_default"}, s.defaultsSet)
}

func TestProcess_CustomerUpdated_NoAccountRow_DriftWarning(t *testing.T) {
	v := &fakeVerifier{event: customerEvent("evt_u2", "customer.updated", "cus_orphan", "")}
	s := newFakeStore()
	s.touchedFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
	require.Empty(t, s.defaultsSet)
}

func TestProcess_CustomerDeleted_LogOnly(t *testing.T) {
	v := &fakeVerifier{event: customerEvent("evt_d1", "customer.deleted", "cus_x", "")}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
}

func TestProcess_PaymentMethodAttached_InsertsMirror(t *testing.T) {
	v := &fakeVerifier{event: cardPMEvent("evt_pma1", "payment_method.attached", "pm_x", "cus_x", "visa", "4242", 12, 2029)}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Len(t, s.inserts, 1)
	require.Equal(t, "pm_x", s.inserts[0].StripePaymentMethodID)
	require.Equal(t, "visa", s.inserts[0].Brand)
	require.Equal(t, "4242", s.inserts[0].Last4)
	require.Equal(t, 12, s.inserts[0].ExpMonth)
	require.Equal(t, 2029, s.inserts[0].ExpYear)
}

func TestProcess_PaymentMethodAttached_NoAccountRow_DriftWarning(t *testing.T) {
	v := &fakeVerifier{event: cardPMEvent("evt_pma2", "payment_method.attached", "pm_x", "cus_orphan", "visa", "4242", 12, 2029)}
	s := newFakeStore()
	s.insertFound = false
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusDriftWarning, res.Status)
}

func TestProcess_PaymentMethodDetached_SoftDeletes(t *testing.T) {
	v := &fakeVerifier{event: cardPMEvent("evt_pmd1", "payment_method.detached", "pm_x", "cus_x", "visa", "4242", 12, 2029)}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusOK, res.Status)
	require.Equal(t, []string{"pm_x"}, s.softDeletes)
}

func TestProcess_UnknownEvent_Acks(t *testing.T) {
	v := &fakeVerifier{event: stripego.Event{
		ID:   "evt_unknown",
		Type: stripego.EventType("invoice.paid"),
		Data: &stripego.EventData{Raw: []byte(`{}`)},
	}}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.inserts)
	require.Empty(t, s.softDeletes)
}

func TestProcess_PaymentMethodAttached_NonCard_Unhandled(t *testing.T) {
	payload := map[string]any{
		"id":       "pm_sepa_x",
		"type":     "sepa_debit",
		"customer": map[string]any{"id": "cus_x"},
	}
	raw, _ := json.Marshal(payload)
	v := &fakeVerifier{event: stripego.Event{
		ID:   "evt_pma_sepa",
		Type: stripego.EventType("payment_method.attached"),
		Data: &stripego.EventData{Raw: raw},
	}}
	s := newFakeStore()
	r := newRouter(v, s)

	res := r.Process(context.Background(), []byte(`{}`), "sig")

	require.Equal(t, 200, res.HTTPStatus)
	require.Equal(t, webhook.StatusUnhandled, res.Status)
	require.Empty(t, s.inserts)
}
