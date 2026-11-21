// Package einvoice is the invoice-provider port.
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
// Under D627 MirrorStack runs the official MOF e-invoice Turnkey itself
// (the e首發票 vendor route, D551, is superseded). That changes what the
// port promises:
//
//   - We number each invoice ourselves, from each seller's own 字軌
//     ledger (D533), so a self-numbering provider returns "issued" with a
//     number at once (D550) — Capabilities.SelfNumbering.
//   - The MOF upload is asynchronous, so whether MOF has the invoice is a
//     separate TransmissionState (queued, sent, accepted, rejected), read
//     back with Query, never pushed to this process.
//   - The void window and the upload deadline are our own calendar rules,
//     read in Asia/Taipei (calendar.go).
//
// This package is the port only: the Provider interface, the Registry
// that resolves a name to one, the normalized Document/Result/
// Capabilities shapes and the shared validators. It holds no MIG file
// format (mirrorstack-ai/billing-engine#340), no Turnkey adapter
// (mirrorstack-ai/billing-engine#341), no seller credential
// (mirrorstack-ai/billing-engine#312) and no network code.
//
// An invoice is a platform service, not a module (D508): its cost is
// platform usage, never module usage billing.
//
// Nothing calls this port yet. Nothing in it reaches MOF or any other
// host, and docs/DESIGN.md is a specification, not a production claim.
package einvoice
