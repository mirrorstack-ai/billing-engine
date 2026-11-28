// Package tappaygateway speaks TapPay's back-end API: Pay by Prime,
// Record (transaction query) and Refund, over one HTTP client with one
// base URL chosen from the mode.
//
// This is PR 1 of billing-engine#310. It deliberately stops at the
// gateway client and its fake: the rail registry entry, the payment
// record, the settle-after-read-back wiring and the seller/rail choice
// (items 1, 4, 5, 6, 7, 8 of the issue) depend on billing-engine#290,
// #292, #293, #294 and #303, none of which has merged as of this
// writing, and are left to a follow-up PR per the issue's own split.
//
// Assumption (D526): the broker described in billing-engine#310 item 3
// (a TapPay caller Lambda in mirrorstack-infra#442 that alone holds the
// partner key) is not built yet either. This package therefore takes
// the partner key as a caller-supplied Config field rather than reading
// a secret itself, so that no key-reading code exists in this repo at
// all for the architecture test in internal/architecture to bound. A
// later PR wires a broker-calling client on top of this package once
// mirrorstack-infra#442 exists; this package itself is suitable to run
// inside that broker, where the key genuinely lives.
//
// Only this package may name tappaysdk.com — see
// internal/architecture/tappay_host_confinement_test.go.
package tappaygateway

// sandboxBaseURL and liveBaseURL are TapPay's two back-end hosts.
//
// liveBaseURL is unreachable today: NewClient refuses any Mode other
// than ModeSandbox until billing-engine#318 (the sandbox/live marking
// child) builds the rest of mode handling. The constant exists so the
// host table is complete and so #318 has one place to flip, not because
// selecting it works.
const (
	sandboxBaseURL = "https://sandbox.tappaysdk.com"
	liveBaseURL    = "https://prod.tappaysdk.com"
)

const (
	payByPrimePath  = "/tpc/payment/pay-by-prime"
	queryRecordPath = "/tpc/transaction/query"
	refundPath      = "/tpc/transaction/refund"
)
