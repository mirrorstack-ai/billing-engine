// Package systemleadadapter is a pure library for e首發票 (矽聯科技
// Systemlead): it builds and signs requests, parses answers, and holds
// the constants and field tables that vendor's wire format needs — and
// nothing else. No network, no key storage.
//
// It does NOT implement einvoice.Provider. mirrorstack-ai/billing-engine#286
// item 2 asks for a library, not an adapter that calls the vendor
// itself: the calls that need a seller's vendor keys are fixed verbs in
// the key broker (mirrorstack-ai/billing-engine#292), which opens a
// seller's sealed credential for one call, asks this library to build
// and sign the request, sends it, and hands the answer back to this
// library to parse. Until that broker lands, nothing in this tree opens
// a key or makes an HTTP call to the vendor — see
// TestSystemleadadapterHasNoNetworkImport in internal/architecture,
// which is the proof rather than a comment's claim.
//
// # What is verified and what is not
//
// Verified, from the vendor's public pages:
//   - The stage base URL below (public; used for Swagger and contract
//     tests only).
//   - Auth is an "apiKey" HTTP header plus body fields CompanyID,
//     Timestamp and Signature, and Signature is SHA256 of the timestamp
//     plus a Hash Salt.
//
// NOT verified, and deliberately not guessed here (D526's rule:
// "everything about the e首發票 wire format is taken from the Swagger
// when it can be read... if the owner has not signed the stage consent
// yet, write contract tests on recorded pairs and say so in the PR"):
//   - The exact concatenation order and encoding that SHA256 is applied
//     to, so Sign is not implemented — see signer.go.
//   - Whether one apiKey can serve several CompanyIDs (owner vendor
//     question, UNVERIFIED) — irrelevant to this library's shape, since
//     the credential it is handed is an opaque sealed blob either way
//     (mirrorstack-ai/billing-engine#312).
//   - Request and response JSON shapes for Issue, Void and Allowance,
//     and the endpoint paths. These need the vendor's Swagger, which
//     this environment has no network access to read. No request or
//     response struct is defined in this package for that reason: a
//     guessed field name is worse than an absent one, because it looks
//     verified.
//   - The exact void-window cutoff day (13th, 14th or 15th depending on
//     vendor page) — voidwindow.go uses the conservative default from
//     mirrorstack-ai/billing-engine#286 item 7 and says so.
//
// The next worker with Swagger access (or a signed stage consent) adds
// the request/response types, the Sign implementation, and the
// known-answer tests this issue's acceptance criteria ask for. This PR
// does not claim those exist.
package systemleadadapter
