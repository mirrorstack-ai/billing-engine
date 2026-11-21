package einvoice

import (
	"context"
	"errors"
	"time"
)

// ErrUnsupported is returned by a method a provider's Capabilities say
// it does not implement, per item 1 of
// mirrorstack-ai/billing-engine#286: "other adapters return a typed
// ErrUnsupported" for RegisterCounterparty, which only ECPay needs.
//
// It is a sentinel wrapped with the method name, never a bare string,
// so a caller can errors.Is it regardless of which provider said so.
var ErrUnsupported = errors.New("einvoice: unsupported by this provider")

// ErrUnknownProvider is returned by Registry.Get for a name nothing
// registered.
var ErrUnknownProvider = errors.New("einvoice: unknown provider")

// Counterparty is a B2B buyer registration record, for the vendors that
// require the buyer to exist at the vendor before an invoice can name
// it (ECPay). Adapters that don't need it return ErrUnsupported from
// RegisterCounterparty rather than silently succeeding.
type Counterparty struct {
	SellerRef string
	TaxID     string
	Name      string
}

// TrackStatus is the state of one 字軌 (invoice number track) range.
type TrackStatus struct {
	SellerRef string
	Track     string
	Remaining int64
}

// Provider is the vendor-agnostic invoice port. A caller hands it a
// normalized Document and gets back a normalized Result; vendor quirks
// — field names, signatures, host paths, rounding — stay inside the
// adapter that implements this interface.
//
// Every method is synchronous and returns promptly: D550 forbids a
// callback route, so an asynchronous vendor answer is read back with
// Query, never pushed back to this process.
type Provider interface {
	// Name is the registry key this provider was registered under.
	Name() string
	Capabilities() Capabilities

	// Issue is idempotent on Document.OrderNumber: issuing twice with
	// the same order number must yield one invoice, not two. Only Issue
	// is safe to retry, and only with the same order number (item 9).
	Issue(ctx context.Context, doc Document) (Result, error)

	// Void invalidates a previously issued invoice. Voiding an
	// already-voided invoice fails cleanly rather than succeeding a
	// second time. Void outside VoidWindow fails; only Allowance is
	// possible past it.
	Void(ctx context.Context, invoiceNumber string, mode Mode) (Result, error)

	// Allowance issues a partial credit note against an invoice. An
	// allowance whose total would exceed the invoice's gross is
	// refused.
	Allowance(ctx context.Context, invoiceNumber string, lines []Line, mode Mode) (Result, error)
	VoidAllowance(ctx context.Context, allowanceNumber string, mode Mode) (Result, error)

	// QueryByOrderNumber rereads an invoice by the caller's idempotency
	// key — the read path for a Pending result, and for adopting the
	// existing invoice after a vendor duplicate-order error (item 8).
	QueryByOrderNumber(ctx context.Context, orderNumber ProviderOrderNumber, mode Mode) (Result, error)
	QueryByInvoiceNumber(ctx context.Context, invoiceNumber string, mode Mode) (Result, error)

	ValidateCarrier(carrier CarrierKind, code string) error
	ValidateLoveCode(loveCode string) error
	LookupTrack(ctx context.Context, sellerRef, track string, mode Mode) (TrackStatus, error)

	// VoidWindow reports the last instant a Void (rather than only an
	// Allowance) is possible for an invoice issued at issuedAt.
	VoidWindow(issuedAt time.Time) time.Time

	// RegisterCounterparty is B2B buyer registration. Providers that
	// don't need it return ErrUnsupported.
	RegisterCounterparty(ctx context.Context, cp Counterparty, mode Mode) error
}
