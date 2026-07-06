// Package webhooktest provides fakes for unit-testing code that
// depends on the billingstripe.Verifier and webhook.Store interfaces.
//
// FakeStore is configurable: zero values give a "successful no-op"
// store useful for happy-path tests; tests that need to assert calls
// (router_test.go) read the recorded slices/maps; tests that need to
// simulate failures (drift, DB down) set the Err* fields.
package webhooktest

import (
	"context"
	"io"
	"log/slog"

	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
)

// FakeVerifier implements billingstripe.Verifier. Configure Event for
// the happy path or Err to simulate a verification failure.
type FakeVerifier struct {
	Event stripego.Event
	Err   error
}

// Verify returns the configured Event and Err pair.
func (f *FakeVerifier) Verify(_ []byte, _ string) (stripego.Event, error) {
	return f.Event, f.Err
}

// FakeStore implements webhook.Store. The recording fields (Processed,
// DefaultsSet, Inserts, SoftDeletes) capture every call so tests can
// assert post-conditions; the boolean fields (TouchedFound, InsertFound,
// SoftDelFound) drive the "found" return value of each method; the
// Err* fields force a particular error path on demand.
//
// Use NewFakeStore() for sensible defaults — every "found" boolean
// true, the Processed map initialized, no errors.
type FakeStore struct {
	// Recording
	Processed          map[string]bool                     // event IDs seen by MarkEventProcessed
	DefaultsSet        []string                            // "customerID=defaultPM" pairs from SetDefaultPaymentMethod
	Inserts            []webhook.InsertPaymentMethodParams // params from InsertPaymentMethod
	SoftDeletes        []string                            // stripe_payment_method_id values from SoftDeletePaymentMethod
	StampedPMs         []string                            // "setupIntentID=stripePMID" from SetAddCardRequestStripePM
	ResolvedPMs        []string                            // stripe_payment_method_id values from ResolvePendingAddCardRequest
	ActivatedCustomers []string                            // stripe_customer_id values from StampAccountActivated

	AppliedInvoices []webhook.ApplyInvoiceStatusParams // captured calls to ApplyInvoiceStatus
	RelaxedInvoices []string                           // stripe_invoice_id values from RelaxCollectionOnPaidInvoice
	FailedInvoices  []string                           // stripe_invoice_id values from RecordFailedCharge
	ResetInvoices   []string                           // stripe_invoice_id values from ResetFailedChargeStreak

	// Found-flag knobs
	TouchedFound bool // returned by TouchAccountByStripeCustomer
	InsertFound  bool // returned by InsertPaymentMethod
	SoftDelFound bool // returned by SoftDeletePaymentMethod
	InvoiceFound bool // returned by ApplyInvoiceStatus
	Relaxed      bool // returned by RelaxCollectionOnPaidInvoice
	FailCounted  bool // returned by RecordFailedCharge (counted)
	StreakReset  bool // returned by ResetFailedChargeStreak (reset)
	ActivatedNew bool // returned by StampAccountActivated (firstBind)

	// Error injection
	ErrMark         error // from MarkEventProcessed
	ErrTouch        error // from TouchAccountByStripeCustomer
	ErrSetDefault   error // from SetDefaultPaymentMethod
	ErrInsert       error // from InsertPaymentMethod
	ErrSoftDel      error // from SoftDeletePaymentMethod
	ErrStamp        error // from SetAddCardRequestStripePM
	ErrResolve      error // from ResolvePendingAddCardRequest
	ErrApplyInvoice error // from ApplyInvoiceStatus
	ErrRelax        error // from RelaxCollectionOnPaidInvoice
	ErrRecordFailed error // from RecordFailedCharge
	ErrResetStreak  error // from ResetFailedChargeStreak
	ErrActivate     error // from StampAccountActivated
}

// NewFakeStore returns a FakeStore configured for happy-path tests:
// every "found" boolean true, the Processed map initialized, no
// errors injected.
func NewFakeStore() *FakeStore {
	return &FakeStore{
		Processed:    map[string]bool{},
		TouchedFound: true,
		InsertFound:  true,
		SoftDelFound: true,
		InvoiceFound: true,
		ActivatedNew: true,
	}
}

func (s *FakeStore) MarkEventProcessed(_ context.Context, eventID, _ string) (bool, error) {
	if s.ErrMark != nil {
		return false, s.ErrMark
	}
	if s.Processed == nil {
		s.Processed = map[string]bool{}
	}
	if s.Processed[eventID] {
		return false, nil
	}
	s.Processed[eventID] = true
	return true, nil
}

func (s *FakeStore) TouchAccountByStripeCustomer(_ context.Context, _ string) (bool, error) {
	if s.ErrTouch != nil {
		return false, s.ErrTouch
	}
	return s.TouchedFound, nil
}

func (s *FakeStore) SetDefaultPaymentMethod(_ context.Context, customerID, defaultPM string) error {
	if s.ErrSetDefault != nil {
		return s.ErrSetDefault
	}
	s.DefaultsSet = append(s.DefaultsSet, customerID+"="+defaultPM)
	return nil
}

func (s *FakeStore) InsertPaymentMethod(_ context.Context, _ string, params webhook.InsertPaymentMethodParams) (bool, error) {
	if s.ErrInsert != nil {
		return false, s.ErrInsert
	}
	s.Inserts = append(s.Inserts, params)
	return s.InsertFound, nil
}

func (s *FakeStore) StampAccountActivated(_ context.Context, customerID string) (bool, error) {
	if s.ErrActivate != nil {
		return false, s.ErrActivate
	}
	s.ActivatedCustomers = append(s.ActivatedCustomers, customerID)
	return s.ActivatedNew, nil
}

func (s *FakeStore) SoftDeletePaymentMethod(_ context.Context, stripePMID string) (bool, error) {
	if s.ErrSoftDel != nil {
		return false, s.ErrSoftDel
	}
	s.SoftDeletes = append(s.SoftDeletes, stripePMID)
	return s.SoftDelFound, nil
}

func (s *FakeStore) SetAddCardRequestStripePM(_ context.Context, setupIntentID, stripePMID string) error {
	if s.ErrStamp != nil {
		return s.ErrStamp
	}
	s.StampedPMs = append(s.StampedPMs, setupIntentID+"="+stripePMID)
	return nil
}

func (s *FakeStore) ResolvePendingAddCardRequest(_ context.Context, stripePMID string) error {
	if s.ErrResolve != nil {
		return s.ErrResolve
	}
	s.ResolvedPMs = append(s.ResolvedPMs, stripePMID)
	return nil
}

func (s *FakeStore) ApplyInvoiceStatus(_ context.Context, params webhook.ApplyInvoiceStatusParams) (bool, error) {
	if s.ErrApplyInvoice != nil {
		return false, s.ErrApplyInvoice
	}
	s.AppliedInvoices = append(s.AppliedInvoices, params)
	return s.InvoiceFound, nil
}

func (s *FakeStore) RelaxCollectionOnPaidInvoice(_ context.Context, stripeInvoiceID string) (bool, error) {
	if s.ErrRelax != nil {
		return false, s.ErrRelax
	}
	s.RelaxedInvoices = append(s.RelaxedInvoices, stripeInvoiceID)
	return s.Relaxed, nil
}

func (s *FakeStore) RecordFailedCharge(_ context.Context, stripeInvoiceID string) (bool, error) {
	if s.ErrRecordFailed != nil {
		return false, s.ErrRecordFailed
	}
	s.FailedInvoices = append(s.FailedInvoices, stripeInvoiceID)
	return s.FailCounted, nil
}

func (s *FakeStore) ResetFailedChargeStreak(_ context.Context, stripeInvoiceID string) (bool, error) {
	if s.ErrResetStreak != nil {
		return false, s.ErrResetStreak
	}
	s.ResetInvoices = append(s.ResetInvoices, stripeInvoiceID)
	return s.StreakReset, nil
}

// SilentLogger returns a slog.Logger that discards all output. Useful
// for tests that don't make logging assertions.
func SilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
