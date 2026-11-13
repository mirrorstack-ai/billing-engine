package fakeinvoice_test

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
	"github.com/mirrorstack-ai/billing-engine/internal/einvoice/einvoicetest"
	"github.com/mirrorstack-ai/billing-engine/internal/einvoice/fakeinvoice"
)

func TestFakeSatisfiesContractSuite(t *testing.T) {
	einvoicetest.Run(t, func(t *testing.T) einvoice.Provider {
		return fakeinvoice.New(nil)
	})
}

var invoiceNumberPattern = regexp.MustCompile(`^[A-Z]{2}[0-9]{8}$`)

func TestFakeInvoiceNumberShape(t *testing.T) {
	f := fakeinvoice.New(nil)
	result, err := f.Issue(context.Background(), einvoice.Document{
		OrderNumber: "SHAPE_TEST_1",
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if !invoiceNumberPattern.MatchString(result.InvoiceNumber) {
		t.Fatalf("invoice number %q does not match two letters + eight digits", result.InvoiceNumber)
	}
}

func TestFakeAsyncPendingThenIssued(t *testing.T) {
	f := fakeinvoice.New(nil)
	order := einvoice.ProviderOrderNumber("ASYNC_TEST_1")
	f.InjectAsyncPending(order)

	first, err := f.Issue(context.Background(), einvoice.Document{
		OrderNumber: order,
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if first.Status != einvoice.StatusPending {
		t.Fatalf("Status = %q, want pending", first.Status)
	}
	if first.InvoiceNumber != "" {
		t.Fatalf("Pending result carried an invoice number: %q", first.InvoiceNumber)
	}

	resolved, err := f.QueryByOrderNumber(context.Background(), order, einvoice.ModeSandbox)
	if err != nil {
		t.Fatalf("QueryByOrderNumber: %v", err)
	}
	if resolved.Status != einvoice.StatusIssued {
		t.Fatalf("Status after Query = %q, want issued", resolved.Status)
	}
	if resolved.InvoiceNumber == "" {
		t.Fatal("Query after pending resolution returned no invoice number")
	}
}

func TestFakeVoidOutsideWindowFails(t *testing.T) {
	issuedAt := time.Date(2026, time.January, 10, 0, 0, 0, 0, time.UTC)
	tooLate := einvoice.DefaultVoidWindow(issuedAt).Add(time.Second)

	now := issuedAt
	f := fakeinvoice.New(func() time.Time { return now })

	issued, err := f.Issue(context.Background(), einvoice.Document{
		OrderNumber: "VOID_WINDOW_TEST",
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
	})
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	now = tooLate
	_, err = f.Void(context.Background(), issued.InvoiceNumber, einvoice.ModeSandbox)
	if !errors.Is(err, einvoice.ErrVoidWindowClosed) {
		t.Fatalf("Void after window = %v, want ErrVoidWindowClosed", err)
	}
}

func TestFakeRegisterCounterpartyUnsupported(t *testing.T) {
	f := fakeinvoice.New(nil)
	err := f.RegisterCounterparty(context.Background(), einvoice.Counterparty{}, einvoice.ModeSandbox)
	if !errors.Is(err, einvoice.ErrUnsupported) {
		t.Fatalf("RegisterCounterparty = %v, want ErrUnsupported", err)
	}
}

func TestFakeInvalidCarrierRejected(t *testing.T) {
	f := fakeinvoice.New(nil)
	_, err := f.Issue(context.Background(), einvoice.Document{
		OrderNumber: "BAD_CARRIER",
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
		Buyer:       einvoice.Buyer{Carrier: einvoice.CarrierMobile, CarrierCode: "not-valid"},
	})
	if !errors.Is(err, einvoice.ErrInvalidCarrier) {
		t.Fatalf("Issue with bad carrier = %v, want ErrInvalidCarrier", err)
	}
}

func TestFakeInvalidLoveCodeRejected(t *testing.T) {
	f := fakeinvoice.New(nil)
	_, err := f.Issue(context.Background(), einvoice.Document{
		OrderNumber: "BAD_LOVE_CODE",
		Lines:       []einvoice.Line{{NetNTD: 100, TaxNTD: 5, GrossNTD: 105}},
		Buyer:       einvoice.Buyer{LoveCode: "not-digits"},
	})
	if !errors.Is(err, einvoice.ErrInvalidLoveCode) {
		t.Fatalf("Issue with bad love code = %v, want ErrInvalidLoveCode", err)
	}
}
