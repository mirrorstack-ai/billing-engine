package systemleadadapter

import "errors"

// Field names from the vendor's public documentation
// (mirrorstack-ai/billing-engine#286 item 2's research notes). These
// are the HTTP header and body field NAMES only — verified as names,
// not as a wire format, since the request shape they belong to is not
// recorded here (see package doc).
const (
	// HeaderAPIKey is the HTTP header the vendor reads the per-seller
	// API key from.
	HeaderAPIKey = "apiKey"
	// FieldCompanyID, FieldTimestamp and FieldSignature are body fields
	// every signed request carries alongside its payload.
	FieldCompanyID = "CompanyID"
	FieldTimestamp = "Timestamp"
	FieldSignature = "Signature"
)

// ErrSignatureSchemeUnverified is returned by Sign. The vendor's
// research notes say the signature is "SHA256 over the timestamp plus a
// Hash Salt", but the exact concatenation order, string encoding, and
// hex-vs-base64 rendering are NOT recorded anywhere this worker could
// read (mirrorstack-ai/billing-engine#286 item 2: "do not guess any of
// them"). Implementing Sign against a guessed concatenation would pass
// its own tests while producing a signature no real e首發票 endpoint
// accepts — worse than refusing, because it would look verified.
//
// Whoever reads the vendor's Swagger (or runs the signed stage
// activation) replaces this function's body and this error with a real
// implementation and known-answer tests from the vendor's own examples.
var ErrSignatureSchemeUnverified = errors.New("systemleadadapter: signature concatenation is unverified; read the vendor Swagger before implementing Sign (billing-engine#286)")

// Sign is not implemented. See ErrSignatureSchemeUnverified.
func Sign(companyID, timestamp, hashSalt string) (string, error) {
	return "", ErrSignatureSchemeUnverified
}
