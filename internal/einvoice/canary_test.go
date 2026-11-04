package einvoice_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
	"github.com/mirrorstack-ai/billing-engine/internal/einvoice/fakeinvoice"
)

// TestNoBuyerDataInErrorsOrResults is the canary mirrorstack-ai/billing-engine#286
// asks for: "no key bytes and no buyer data in logs or errors." This
// package defines no credential type (there is none to leak — #312
// owns where a sealed key lives), so this test's job is the buyer half:
// feed a marked buyer value through every error path reachable without
// a stage account, and confirm the marker never surfaces in an error
// string or in a Result's rendered form.
func TestNoBuyerDataInErrorsOrResults(t *testing.T) {
	const marker = "CANARY_BUYER_SECRET_MARKER"
	f := fakeinvoice.New(nil)
	ctx := context.Background()

	doc := einvoice.Document{
		SellerRef: "seller-canary",
		Buyer: einvoice.Buyer{
			Name:  marker,
			TaxID: marker,
			Email: marker,
		},
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
		OrderNumber: "CANARY_ORDER_1",
	}

	issued, err := f.Issue(ctx, doc)
	assertNoMarker(t, "Issue result", fmt.Sprintf("%+v", issued), marker)
	if err != nil {
		assertNoMarker(t, "Issue error", err.Error(), marker)
	}

	// An allowance that is refused still must not echo the buyer back
	// in its error.
	_, err = f.Allowance(ctx, issued.InvoiceNumber, []einvoice.Line{
		{NetNTD: 1_000_000, TaxNTD: 50_000, GrossNTD: 1_050_000},
	}, einvoice.ModeSandbox)
	if err != nil {
		assertNoMarker(t, "Allowance error", err.Error(), marker)
	}

	// An ambiguous outcome's own error type carries only the order
	// number and a short reason — never the document.
	f.InjectTimeout("CANARY_ORDER_2")
	_, err = f.Issue(ctx, einvoice.Document{
		Buyer:       einvoice.Buyer{Name: marker},
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
		OrderNumber: "CANARY_ORDER_2",
	})
	if err != nil {
		assertNoMarker(t, "Ambiguous error", err.Error(), marker)
	}
}

func assertNoMarker(t *testing.T, label, rendered, marker string) {
	t.Helper()
	if strings.Contains(rendered, marker) {
		t.Errorf("%s contains buyer data: %q", label, rendered)
	}
}
