// Package einvoice is the vendor-agnostic invoice-provider port.
//
// mirrorstack-ai/mirrorstack-core-v2#1850 decision D530: invoicing is a
// platform service, separate from every payment rail. This package is
// that separation made into a type boundary — it sits beside
// internal/intent/executor.Collector (internal/intent/executor/executor.go:38),
// not inside it, and executor.Collector is unchanged by anything here.
//
// A Provider never learns which rail paid. Stripe, NewebPay, TapPay and
// manual bank transfer each write a neutral payment fact
// (mirrorstack-ai/billing-engine#313); this package starts at the
// invoice that follows, not at the charge that preceded it.
//
// This package is a port: the Provider interface, the Registry that
// resolves a name to one, and the normalized Document/Result/Capabilities
// shapes every adapter speaks. It holds no vendor wire format and no
// vendor credential. The fake adapter lives in
// internal/einvoice/fakeinvoice; the first real vendor's signing and
// request library lives in internal/provider/systemleadadapter and does
// not implement Provider itself, because it has no network — see that
// package's doc comment for why the port and the wire library are two
// separate things in this issue.
//
// An invoice is a platform service, not a module (D508): its cost is
// metered as platform usage, which is mirrorstack-ai/billing-engine#314
// and not this package's concern.
//
// Stage only (D504, D520): nothing in this package or its first adapter
// reaches a production vendor host, and docs/DESIGN.md is a
// specification, not a production claim — see the banner atop this
// repository's CLAUDE.md for what that means for this PR specifically.
package einvoice
