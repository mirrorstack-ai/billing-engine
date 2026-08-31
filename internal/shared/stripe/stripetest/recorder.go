// Package stripetest provides a recording test double for the provider
// client interfaces in internal/shared/stripe.
//
// It exists to make one claim assertable: that a code path did not move
// money. "It didn't charge" is not something a fake returning zero
// values can show — the call has to be observed and classified. So every
// method here records a Call carrying the Effect the real method has at
// Stripe, and tests assert over those effects rather than over their own
// reading of the call graph.
//
// The three effects are deliberately finer than read/write:
//
//	EffectRead       observes provider state and changes nothing.
//	EffectMutate     changes provider state but collects no money —
//	                 a draft invoice, a pinned line item, a detach.
//	EffectCollect    can take money from a stored payment method.
//
// The middle class is what a plain read/write split loses. Creating a
// draft invoice is a real provider mutation and still cannot charge
// anyone, while FinalizeInvoice's whole purpose is that it can. A read
// path that creates a draft is a capability leak worth failing on; a
// read path that reaches Finalize is an incident.
package stripetest

import (
	"context"
	"sync"

	stripego "github.com/stripe/stripe-go/v85"

	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Effect classifies what a provider method does at Stripe.
type Effect int

const (
	// EffectRead observes provider state without changing it.
	EffectRead Effect = iota
	// EffectMutate changes provider state without collecting money.
	EffectMutate
	// EffectCollect can charge a stored payment method.
	EffectCollect
)

func (e Effect) String() string {
	switch e {
	case EffectRead:
		return "read"
	case EffectMutate:
		return "mutate"
	case EffectCollect:
		return "collect"
	}
	return "unknown"
}

// Call is one observed provider method invocation.
type Call struct {
	Method string
	Effect Effect
	// Ref is the most identifying argument for the call — a customer,
	// invoice, or payment-method id — so a failure message names the
	// object rather than only the method.
	Ref string
	// AmountCents is set for the line-item calls, whose amount is the
	// thing a disclosure has to match.
	AmountCents int64
	// IdemKey is the deterministic key the caller supplied, or "" when
	// the method takes none. A mutation with no key is worth failing on
	// in its own right.
	IdemKey string
	// Description is the customer-facing line text, for the line-item
	// calls. It is recorded because it is what the customer reads on their
	// statement, and a rail that quietly substituted its own text for the
	// one a document sealed would otherwise be unobservable here.
	Description string
}

// Recorder implements the provider client interfaces and records every
// call. The zero value is ready to use; a nil Recorder must not be used.
//
// Every method returns a zero-valued success unless a Stub is set for
// it, because the point of this double is the record, not the response.
// Tests needing a specific response set Stubs.
type Recorder struct {
	mu    sync.Mutex
	calls []Call

	// Stubs supplies responses by method name. A method with no stub
	// returns its zero value and a nil error.
	Stubs map[string]any
	// Errs supplies an error by method name, so a test can drive the
	// ambiguous-response and partial-failure paths.
	Errs map[string]error
}

// New returns a Recorder ready for use.
func New() *Recorder {
	return &Recorder{Stubs: map[string]any{}, Errs: map[string]error{}}
}

func (r *Recorder) record(method string, effect Effect, ref, idemKey string, amountCents int64) error {
	r.mu.Lock()
	r.calls = append(r.calls, Call{
		Method:      method,
		Effect:      effect,
		Ref:         ref,
		AmountCents: amountCents,
		IdemKey:     idemKey,
	})
	r.mu.Unlock()
	return r.Errs[method]
}

func stub[T any](r *Recorder, method string) T {
	var zero T
	if v, ok := r.Stubs[method]; ok {
		if typed, ok := v.(T); ok {
			return typed
		}
	}
	return zero
}

// Calls returns every recorded call, in order.
// setLastDescription attaches the description to the call just recorded.
func (r *Recorder) setLastDescription(description string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n := len(r.calls); n > 0 {
		r.calls[n-1].Description = description
	}
}

func (r *Recorder) Calls() []Call {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Call, len(r.calls))
	copy(out, r.calls)
	return out
}

// CallsWithEffect returns the recorded calls of one effect class.
func (r *Recorder) CallsWithEffect(e Effect) []Call {
	var out []Call
	for _, c := range r.Calls() {
		if c.Effect == e {
			out = append(out, c)
		}
	}
	return out
}

// Reset drops the recorded calls, keeping stubs and errors. Useful when
// a test sets up state through the provider and then asserts over only
// the phase it is actually testing.
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.calls = nil
	r.mu.Unlock()
}

// --- reads ---

func (r *Recorder) GetCustomer(_ context.Context, id string) (*stripego.Customer, error) {
	if err := r.record("GetCustomer", EffectRead, id, "", 0); err != nil {
		return nil, err
	}
	return stub[*stripego.Customer](r, "GetCustomer"), nil
}

func (r *Recorder) GetInvoice(_ context.Context, id string) (billingstripe.Invoice, error) {
	if err := r.record("GetInvoice", EffectRead, id, "", 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return stub[billingstripe.Invoice](r, "GetInvoice"), nil
}

func (r *Recorder) FindInvoiceByRef(_ context.Context, custID, ref string) (billingstripe.Invoice, bool, error) {
	if err := r.record("FindInvoiceByRef", EffectRead, custID+"/"+ref, "", 0); err != nil {
		return billingstripe.Invoice{}, false, err
	}
	inv, found := r.Stubs["FindInvoiceByRef"].(billingstripe.Invoice)
	return inv, found, nil
}

func (r *Recorder) ListInvoiceItems(_ context.Context, invoiceID string) ([]billingstripe.InvoiceItem, error) {
	if err := r.record("ListInvoiceItems", EffectRead, invoiceID, "", 0); err != nil {
		return nil, err
	}
	return stub[[]billingstripe.InvoiceItem](r, "ListInvoiceItems"), nil
}

func (r *Recorder) ListInvoicePayments(_ context.Context, invoiceID string) ([]billingstripe.InvoicePaymentProof, error) {
	if err := r.record("ListInvoicePayments", EffectRead, invoiceID, "", 0); err != nil {
		return nil, err
	}
	return stub[[]billingstripe.InvoicePaymentProof](r, "ListInvoicePayments"), nil
}

func (r *Recorder) RetrieveCharge(_ context.Context, chargeID string) (billingstripe.ChargeCardRef, error) {
	if err := r.record("RetrieveCharge", EffectRead, chargeID, "", 0); err != nil {
		return billingstripe.ChargeCardRef{}, err
	}
	return stub[billingstripe.ChargeCardRef](r, "RetrieveCharge"), nil
}

// --- mutations that cannot collect ---

func (r *Recorder) CreateCustomer(_ context.Context, billingAccountID, email string) (*stripego.Customer, error) {
	if err := r.record("CreateCustomer", EffectMutate, billingAccountID, "", 0); err != nil {
		return nil, err
	}
	if c := stub[*stripego.Customer](r, "CreateCustomer"); c != nil {
		return c, nil
	}
	return &stripego.Customer{ID: "cus_recorder", Email: email}, nil
}

func (r *Recorder) UpdateCustomerEmail(_ context.Context, custID, _ string) error {
	return r.record("UpdateCustomerEmail", EffectMutate, custID, "", 0)
}

func (r *Recorder) CreateCheckoutSession(_ context.Context, custID, _ string) (*stripego.CheckoutSession, error) {
	if err := r.record("CreateCheckoutSession", EffectMutate, custID, "", 0); err != nil {
		return nil, err
	}
	if s := stub[*stripego.CheckoutSession](r, "CreateCheckoutSession"); s != nil {
		return s, nil
	}
	return &stripego.CheckoutSession{ID: "cs_recorder", ClientSecret: "cs_secret_recorder"}, nil
}

func (r *Recorder) DetachPaymentMethod(_ context.Context, pmID string) error {
	return r.record("DetachPaymentMethod", EffectMutate, pmID, "", 0)
}

func (r *Recorder) SetDefaultPaymentMethod(_ context.Context, custID, pmID string) error {
	return r.record("SetDefaultPaymentMethod", EffectMutate, custID+"/"+pmID, "", 0)
}

func (r *Recorder) CreateDraftInvoice(_ context.Context, custID, ref, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("CreateDraftInvoice", EffectMutate, custID+"/"+ref, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("CreateDraftInvoice", "in_draft_recorder"), nil
}

func (r *Recorder) CreateCreditPurchaseInvoice(_ context.Context, customerID, accountID, ledgerID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("CreateCreditPurchaseInvoice", EffectMutate, customerID+"/"+accountID+"/"+ledgerID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("CreateCreditPurchaseInvoice", "in_creditpurchase_recorder"), nil
}

func (r *Recorder) CreateAutoTopUpInvoice(_ context.Context, customerID, paymentMethodID, accountID, ledgerID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("CreateAutoTopUpInvoice", EffectMutate, customerID+"/"+paymentMethodID+"/"+ledgerID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("CreateAutoTopUpInvoice", "in_autotopup_recorder"), nil
}

func (r *Recorder) CreateInvoiceItem(_ context.Context, custID, invoiceID string, amountCents int64, _, description string, _ billingstripe.LinePeriod, idemKey string) (billingstripe.InvoiceItem, error) {
	if err := r.record("CreateInvoiceItem", EffectMutate, invoiceID, idemKey, amountCents); err != nil {
		return billingstripe.InvoiceItem{}, err
	}
	r.setLastDescription(description)
	_ = custID
	return stub[billingstripe.InvoiceItem](r, "CreateInvoiceItem"), nil
}

func (r *Recorder) CreateCombinedProrationInvoiceItem(_ context.Context, custID, invoiceID string, amountCents int64, _, _ string, _ billingstripe.LinePeriod, idemKey string, _ billingstripe.CombinedProrationItemIdentity) (billingstripe.InvoiceItem, error) {
	if err := r.record("CreateCombinedProrationInvoiceItem", EffectMutate, invoiceID, idemKey, amountCents); err != nil {
		return billingstripe.InvoiceItem{}, err
	}
	_ = custID
	return stub[billingstripe.InvoiceItem](r, "CreateCombinedProrationInvoiceItem"), nil
}

// FinalizeInvoiceWithoutAutoAdvance finalizes without handing the
// invoice to Stripe's automatic collection, so it changes provider
// state without itself charging anyone.
func (r *Recorder) FinalizeInvoiceWithoutAutoAdvance(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("FinalizeInvoiceWithoutAutoAdvance", EffectMutate, invoiceID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("FinalizeInvoiceWithoutAutoAdvance", invoiceID), nil
}

func (r *Recorder) VoidInvoice(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("VoidInvoice", EffectMutate, invoiceID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("VoidInvoice", invoiceID), nil
}

func (r *Recorder) DeleteDraftInvoice(_ context.Context, invoiceID string) (billingstripe.Invoice, error) {
	if err := r.record("DeleteDraftInvoice", EffectMutate, invoiceID, "", 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("DeleteDraftInvoice", invoiceID), nil
}

// --- collection: these can take money ---

func (r *Recorder) FinalizeInvoice(_ context.Context, invoiceID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("FinalizeInvoice", EffectCollect, invoiceID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("FinalizeInvoice", invoiceID), nil
}

func (r *Recorder) PayInvoice(_ context.Context, invoiceID string) (billingstripe.Invoice, error) {
	if err := r.record("PayInvoice", EffectCollect, invoiceID, "", 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("PayInvoice", invoiceID), nil
}

func (r *Recorder) PayInvoiceWithMethod(_ context.Context, invoiceID, paymentMethodID, idemKey string) (billingstripe.Invoice, error) {
	if err := r.record("PayInvoiceWithMethod", EffectCollect, invoiceID+"/"+paymentMethodID, idemKey, 0); err != nil {
		return billingstripe.Invoice{}, err
	}
	return r.invoiceStub("PayInvoiceWithMethod", invoiceID), nil
}

func (r *Recorder) invoiceStub(method, fallbackID string) billingstripe.Invoice {
	if inv, ok := r.Stubs[method].(billingstripe.Invoice); ok {
		return inv
	}
	return billingstripe.Invoice{ID: fallbackID}
}
