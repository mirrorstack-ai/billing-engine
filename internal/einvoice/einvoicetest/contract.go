// Package einvoicetest is the one contract suite every einvoice.Provider
// must pass (mirrorstack-ai/billing-engine#286, item 5), "so a new
// vendor cannot change behaviour the rest of the system relies on."
//
// It is not a _test.go file so that internal/provider/systemleadadapter
// and later internal/provider/ecpayadapter can import and run it from
// their own *_test.go files once they implement einvoice.Provider.
package einvoicetest

import (
	"context"
	"errors"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

// Factory returns a fresh, empty Provider for one subtest. A fresh
// instance per subtest keeps the suite's cases independent of each
// other's state.
type Factory func(t *testing.T) einvoice.Provider

func sampleDoc(order einvoice.ProviderOrderNumber) einvoice.Document {
	return einvoice.Document{
		SellerRef: "seller-1",
		Buyer:     einvoice.Buyer{Name: "Test Buyer"},
		Lines: []einvoice.Line{
			{Description: "usage", NetNTD: 1000, TaxNTD: 50, GrossNTD: 1050},
		},
		Kind:        einvoice.KindB2C,
		OrderNumber: order,
		Mode:        einvoice.ModeSandbox,
	}
}

// Run executes the contract suite against one provider factory.
func Run(t *testing.T, newProvider Factory) {
	t.Run("issue twice with the same order number yields one invoice", func(t *testing.T) {
		p := newProvider(t)
		doc := sampleDoc("CONTRACT_IDEMPOT_1")

		first, err := p.Issue(context.Background(), doc)
		if err != nil {
			t.Fatalf("first Issue: %v", err)
		}
		second, err := p.Issue(context.Background(), doc)
		if err != nil {
			t.Fatalf("second Issue: %v", err)
		}
		if first.InvoiceNumber == "" {
			t.Fatal("first Issue returned no invoice number")
		}
		if first.InvoiceNumber != second.InvoiceNumber {
			t.Fatalf("issuing twice with the same order number produced two invoices: %q and %q",
				first.InvoiceNumber, second.InvoiceNumber)
		}
	})

	t.Run("void then void again fails cleanly", func(t *testing.T) {
		p := newProvider(t)
		doc := sampleDoc("CONTRACT_DOUBLE_VOID")
		issued, err := p.Issue(context.Background(), doc)
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}

		if _, err := p.Void(context.Background(), issued.InvoiceNumber, einvoice.ModeSandbox); err != nil {
			t.Fatalf("first Void: %v", err)
		}
		_, err = p.Void(context.Background(), issued.InvoiceNumber, einvoice.ModeSandbox)
		if !errors.Is(err, einvoice.ErrAlreadyVoided) {
			t.Fatalf("second Void = %v, want ErrAlreadyVoided", err)
		}
	})

	t.Run("an allowance above the invoice gross is refused", func(t *testing.T) {
		p := newProvider(t)
		doc := sampleDoc("CONTRACT_OVER_ALLOW")
		issued, err := p.Issue(context.Background(), doc)
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}

		_, err = p.Allowance(context.Background(), issued.InvoiceNumber, []einvoice.Line{
			{Description: "too much", NetNTD: 10_000, TaxNTD: 500, GrossNTD: 10_500},
		}, einvoice.ModeSandbox)
		if !errors.Is(err, einvoice.ErrAllowanceExceedsGross) {
			t.Fatalf("over-gross Allowance = %v, want ErrAllowanceExceedsGross", err)
		}
	})

	t.Run("an ambiguous answer never yields a second invoice", func(t *testing.T) {
		timeoutInjector, ok := newProvider(t).(interface {
			InjectTimeout(einvoice.ProviderOrderNumber)
		})
		if !ok {
			t.Skip("provider does not support timeout injection; nothing to verify here")
		}
		p := timeoutInjector.(einvoice.Provider)
		order := einvoice.ProviderOrderNumber("CONTRACT_AMBIGUOUS")
		timeoutInjector.InjectTimeout(order)

		_, err := p.Issue(context.Background(), sampleDoc(order))
		var ambiguous einvoice.Ambiguous
		if !errors.As(err, &ambiguous) {
			t.Fatalf("Issue after InjectTimeout = %v, want einvoice.Ambiguous", err)
		}

		// The ambiguous attempt must not have created an invoice: a
		// retry with the SAME order number is the recovery path, and it
		// must issue exactly once.
		retried, err := p.Issue(context.Background(), sampleDoc(order))
		if err != nil {
			t.Fatalf("retry Issue: %v", err)
		}
		if retried.InvoiceNumber == "" {
			t.Fatal("retry after Ambiguous produced no invoice")
		}

		again, err := p.Issue(context.Background(), sampleDoc(order))
		if err != nil {
			t.Fatalf("second retry Issue: %v", err)
		}
		if again.InvoiceNumber != retried.InvoiceNumber {
			t.Fatalf("retrying after Ambiguous produced two invoices: %q and %q",
				retried.InvoiceNumber, again.InvoiceNumber)
		}
	})

	t.Run("registry refuses an unknown name", func(t *testing.T) {
		r := einvoice.NewRegistry()
		_, err := r.Get("not-a-real-provider")
		if !errors.Is(err, einvoice.ErrUnknownProvider) {
			t.Fatalf("Get(unknown) = %v, want ErrUnknownProvider", err)
		}
	})
}
