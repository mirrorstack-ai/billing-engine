package einvoice

import "errors"

// Errors common to every Provider implementation. A concrete adapter
// may wrap one of these with vendor detail, but should return one of
// these (via errors.Is) rather than an adapter-private sentinel, so the
// contract suite in internal/einvoice/einvoicetest can assert on
// behaviour without knowing which adapter it is testing.
var (
	// ErrAlreadyVoided: voiding an already-voided invoice fails cleanly
	// rather than voiding a second time.
	ErrAlreadyVoided = errors.New("einvoice: invoice already voided")
	// ErrVoidWindowClosed: Void was called past VoidWindow; only
	// Allowance remains possible.
	ErrVoidWindowClosed = errors.New("einvoice: void window has closed")
	// ErrAllowanceExceedsGross: the sum of an allowance's lines would
	// exceed the invoice's remaining gross.
	ErrAllowanceExceedsGross = errors.New("einvoice: allowance exceeds invoice gross")
	// ErrInvalidCarrier: ValidateCarrier rejected a carrier code.
	ErrInvalidCarrier = errors.New("einvoice: invalid carrier code")
	// ErrInvalidLoveCode: ValidateLoveCode rejected a love code.
	ErrInvalidLoveCode = errors.New("einvoice: invalid love code")
	// ErrAmountMismatch: the adapter's own recomputation of net, tax or
	// gross disagreed with the document's — item 6's refusal rule.
	ErrAmountMismatch = errors.New("einvoice: computed amount does not match the document")
	// ErrInvoiceNotFound: Query found nothing for the given key.
	ErrInvoiceNotFound = errors.New("einvoice: invoice not found")
)
