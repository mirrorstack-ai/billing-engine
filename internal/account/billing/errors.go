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
	// CodeUnavailable is returned by the credit RPCs while the fail-closed
	// wallet flag or migration-048 capability probe is off.
	CodeUnavailable Code = "CREDIT_WALLET_DISABLED"
	// CodePaymentRequired is the funding-gates rejection (design:
	// docs-temp/billing-funding-gates/design.md, DECIDED 2026-07-11): the
	// requested action needs a funded billing account (activated + usable
	// non-fraud card) the owner doesn't have. RegisterApp returns it for
	// unfunded creates; PayInvoice for a pay attempt with no usable default
	// card. api-platform surfaces it as HTTP 402.
	CodePaymentRequired Code = "PAYMENT_REQUIRED"
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
// every site that emits a given code).
func InvalidInput(msg string) *Error {
	return &Error{Code: CodeInvalidInput, Message: msg}
}

// NotFound is returned when a requested resource (e.g. a payment method
// or an add-card request) doesn't exist or isn't owned by the caller.
// We deliberately don't distinguish "no row" from "owned by someone else"
// — both surface as NOT_FOUND so a malicious caller can't enumerate
// resource ids belonging to other users. The dispatch layer maps it
// to HTTP 404 on the local path.
func NotFound(msg string) *Error {
	return &Error{Code: CodeNotFound, Message: msg}
}

// PaymentRequired is returned when an action is gated on funding the caller
// doesn't have (no activated account, no usable non-fraud card, or an org
// without a resolvable funding designation). The dispatch layer maps it to
// HTTP 402 on the local path; api-platform relays the same 402 outward.
func PaymentRequired(msg string) *Error {
	return &Error{Code: CodePaymentRequired, Message: msg}
}

// Unavailable reports a deliberately disabled capability. The distinct wire
// code lets clients distinguish it from invalid input and transient internals.
func Unavailable(msg string) *Error {
	return &Error{Code: CodeUnavailable, Message: msg}
}

func StripeError(msg string, wrapped error) *Error {
	return &Error{Code: CodeStripeError, Message: msg, Wrapped: wrapped}
}

func Internal(msg string, wrapped error) *Error {
	return &Error{Code: CodeInternal, Message: msg, Wrapped: wrapped}
}
