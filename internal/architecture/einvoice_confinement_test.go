package architecture

import (
	"strings"
	"testing"
)

// TestSystemleadadapterHasNoNetworkImport is the proof, not the claim,
// behind internal/provider/systemleadadapter's package doc: "No
// network, no key storage." A library with no way to dial a network
// address cannot be the thing that sends a seller's vendor key
// anywhere, regardless of what it holds in memory —
// mirrorstack-ai/billing-engine#286's acceptance criterion "the engine
// process holds no vendor key" rests on this for the e首發票 library
// specifically.
//
// internal/einvoice and internal/einvoice/fakeinvoice are included for
// the same reason: the port and the fake are both documented as
// in-process and network-free, and this pins that the whole invoice
// surface introduced by #286 has no outbound reach yet. The day a real
// network-backed adapter lands (through the broker, per item 3), it
// gets its own confinement entry the way providerSDKImporters does for
// Stripe — it does not get added to this list.
func TestEInvoiceSurfaceHasNoNetworkImport(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	importers, err := ScanNetworkImporters(root,
		"internal/einvoice",
		"internal/provider/systemleadadapter",
	)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(importers) > 0 {
		t.Errorf("%d file(s) in the e首發票 stage-only surface import a network package, which contradicts their own \"no network\" package docs:\n  %s",
			len(importers), strings.Join(importers, "\n  "))
	}
}
