package billing

import "fmt"

// Code is the wire-level error code returned in the RPC envelope's
// {"ok": false, "error": {"code": …}} field. Matches the vocabulary
// documented in mirrorstack-docs/api/billing/account-api.md.
type Code string

const (
	CodeInvalidInput Code = "INVALID_INPUT"
	CodeNotFound     Code = "NOT_FOUND"
	CodeStripeError  Code = "STRIPE_ERROR"
	CodeInternal     Code = "INTERNAL"
)

// Error is the typed error returned by every service method. The RPC
// dispatch layer (cmd/account-api/main.go in PR 4) type-asserts to
// *Error to populate the envelope; unrecognized errors map to
// CodeInternal with the raw message.
//
// Code is the machine-readable enum; Message is the human-readable
// hint. Wrapped retains the underlying cause for log inspection but
// is NOT serialized over the wire (would leak internals).
type Error struct {
	Code    Code
	Message string
	Wrapped error
}

func (e *Error) Error() string {
	if e.Wrapped != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Wrapped)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.Wrapped }

// Constructors. Use these instead of struct literals so the package
// vocabulary stays narrow (grep for "billing.InvalidInput(" to find
// every site that emits a given code). CodeNotFound has no constructor
// in v1 — no RPC returns it — but the Code constant is kept so the
// dispatch layer can map future errors uniformly without a vocabulary
// change.
func InvalidInput(msg string) *Error {
	return &Error{Code: CodeInvalidInput, Message: msg}
}

func StripeError(msg string, wrapped error) *Error {
	return &Error{Code: CodeStripeError, Message: msg, Wrapped: wrapped}
}

func Internal(msg string, wrapped error) *Error {
	return &Error{Code: CodeInternal, Message: msg, Wrapped: wrapped}
}
