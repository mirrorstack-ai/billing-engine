package invoice

import "context"

// SealedPayload is the raw vendor credential bytes a caller hands to
// PutInvoiceProviderKeys, before sealing. It is vendor-specific and opaque to
// this package: e首發票 {apiKey, hash salt, CompanyID}, ECPay seller-keys
// {MerchantID, HashKey, HashIV}, ECPay platform mode {PlatformID, bound
// MerchantID, platform keys} — all JSON blobs the broker seals without this
// package ever inspecting their shape.
//
// GoString and String are overridden so an accidental %v/%+v/%s on a struct
// holding one, in a log line or an error, prints a redaction instead of the
// payload. This is a backstop, not the control — the control is that nothing
// in this package ever logs a SealedPayload or a Broker.Seal/Verify error
// that embeds it.
type SealedPayload []byte

func (SealedPayload) String() string   { return "[REDACTED invoice credential]" }
func (SealedPayload) GoString() string { return "[REDACTED invoice credential]" }

// Broker is the sealing writer's interface as this package needs it. The
// real implementation is billing-engine#292's isolated sealing writer plus
// KMS key (mirrorstack-infra#433); until that lands, callers wire FakeBroker
// for sandbox/dev use. Production code must never construct a Broker whose
// Seal/Verify can be satisfied by reading a key into THIS process — the
// isolated broker opens a sealed credential only for the one call it is
// making, and returns a classification, never the key (D516 pattern).
type Broker interface {
	// Seal stores payload and returns an opaque reference plus a key
	// version. The caller must discard payload after this call returns —
	// Broker, not this package, is where it is retained (sealed).
	Seal(ctx context.Context, provider Provider, payload SealedPayload) (secretRef string, keyVersion int, err error)

	// Revoke marks secretRef permanently unusable. It does not erase the
	// reference itself — invoice_provider_accounts.secret_ref survives a
	// revoke for audit trail — only what it points to.
	Revoke(ctx context.Context, secretRef string) error

	// Verify opens the credential at secretRef internally and makes the
	// adapter's cheapest read-only signed call for provider, returning only
	// a classification. This process never receives the opened key. A
	// provider with no read-only call returns VerifyResultNotVerifiable,
	// not an error.
	Verify(ctx context.Context, provider Provider, secretRef string, keyVersion int) (VerifyResult, error)
}
