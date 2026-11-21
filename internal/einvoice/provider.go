package einvoice

import (
	"context"
	"errors"
	"time"
)

// ErrUnsupported is returned by a method a provider's Capabilities say
// it does not implement — today RegisterCounterparty, which the Turnkey
// provider does not need (a B2B buyer receives on the MOF platform,
// D640).
//
// It is a sentinel wrapped with the method name, never a bare string,
// so a caller can errors.Is it regardless of which provider said so.
var ErrUnsupported = errors.New("einvoice: unsupported by this provider")

// ErrUnknownProvider is returned by Registry.Get for a name nothing
// registered.
var ErrUnknownProvider = errors.New("einvoice: unknown provider")

// Counterparty is a B2B buyer registration record, for a provider that
// requires the buyer to exist before an invoice can name it. A provider
// that does not need it returns ErrUnsupported from RegisterCounterparty
// rather than silently succeeding.
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

// Provider is the invoice port. A caller hands it a normalized Document
// and gets back a normalized Result; provider detail — the MIG file
// format, folder layout, rounding — stays inside the adapter that
// implements this interface. Under D627 the providers are the Turnkey
// adapter (ProviderMOFTurnkey, mirrorstack-ai/billing-engine#341) and the
// fake (ProviderFake).
//
// Every method is synchronous and returns promptly: D550 forbids a
// callback route, so an asynchronous answer (the MOF upload result) is
// read back with Query, never pushed back to this process.
type Provider interface {
	// Name is the registry key this provider was registered under.
	Name() string
	Capabilities() Capabilities

	// Issue is idempotent on Document.OrderNumber: issuing twice with
	// the same order number must yield one invoice, not two. Only Issue
	// is safe to retry, and only with the same order number.
	//
	// A self-numbering provider (Capabilities.SelfNumbering) reserves the
	// number from the seller's 字軌 ledger inside Issue and returns
	// StatusIssued, a non-empty InvoiceNumber and a Transmission state
	// (normally TransmissionQueued) at once — before MOF has the invoice.
	// It never returns StatusPending. An empty range is refused with
	// ErrTrackExhausted, and no number is consumed by a refused call.
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
	// key — the read path after an Ambiguous answer, for a Pending result,
	// and for the Transmission state of a self-numbered invoice.
	//
	// Query moves Transmission forward only (TransmissionState.Advances):
	// queued, then sent, then accepted or rejected. Rejected is terminal
	// and stays visible on every later Query; it is never dropped and
	// never turned into a second invoice.
	QueryByOrderNumber(ctx context.Context, orderNumber ProviderOrderNumber, mode Mode) (Result, error)
	QueryByInvoiceNumber(ctx context.Context, invoiceNumber string, mode Mode) (Result, error)

	ValidateCarrier(carrier CarrierKind, code string) error
	ValidateLoveCode(loveCode string) error

	// LookupTrack reports the remaining numbers in one 字軌 range. A
	// self-numbering provider answers from our own ledger
	// (mirrorstack-ai/billing-engine#338), never from a remote call.
	LookupTrack(ctx context.Context, sellerRef, track string, mode Mode) (TrackStatus, error)

	// VoidWindow reports the last instant a Void (rather than only an
	// Allowance) is possible for an invoice issued at issuedAt. Every
	// provider returns DefaultVoidWindow: the rule is ours, not the
	// provider's.
	VoidWindow(issuedAt time.Time) time.Time

	// RegisterCounterparty is B2B buyer registration. Providers that
	// don't need it return ErrUnsupported.
	RegisterCounterparty(ctx context.Context, cp Counterparty, mode Mode) error
}
