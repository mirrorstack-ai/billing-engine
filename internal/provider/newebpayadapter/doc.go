// Package newebpayadapter signs outgoing NewebPay (藍新) requests and
// verifies incoming notices, standing in for the provider's own MPG
// signing scheme the way internal/provider/stripeadapter stands in for
// Stripe's.
//
// NewebPay authenticates with a shared HashKey/HashIV rather than a
// request signature header: every field goes out as an AES-256-CBC
// blob (TradeInfo) plus a SHA-256 digest over that blob and the key
// material (TradeSha), and a notice is only as trustworthy as the
// TradeSha check that runs before anything is decrypted. See
// VerifyNotice for that order.
//
// This package is pure: no network call, no clock read (every time
// value is a caller-supplied field, never time.Now), no environment
// read, and every import is the standard library. It does not log or
// print its inputs, and HashKey/HashIV refuse to be printed, marshaled
// or logged in the clear — see HashKey and HashIV.
//
// Scope. This package signs, encrypts, decrypts and verifies. It does
// not decide what to do with a verified Observation: replay protection
// (has this MerchantOrderNo already been credited?) and unknown-order
// handling need durable state this package deliberately has none of,
// and belong to the notice worker (N10, billing-engine#296) — see the
// statelessness note on VerifyNotice.
package newebpayadapter
