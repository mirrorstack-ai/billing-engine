package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// 🔴 The integer handed to a provider adapter is the sealed provider
// remainder, never the gross obligation.
//
// docs/DESIGN.md:1284 states it without qualification: "The integer handed
// to an adapter is the sealed providerRemainder — never grossObligation,
// and never wallet funding."
//
// This is a SOURCE assertion rather than a behavioural one, and the reason
// matters. fundingFor currently hardcodes WalletAllocationMicros: 0, so
// ProviderRemainderMicros == TotalMicros() for every intent this tree can
// build. No behavioural test can tell the correct expression from the wrong
// one until a wallet split exists — and by then the wrong one has shipped.
//
// The failure it prevents: a 20,000,000 intent split 6,000,000 wallet /
// 14,000,000 provider, with the total handed to the adapter, charges the
// rail 20,000,000 while the wallet is drawn 6,000,000 — 26,000,000 of value
// collected for a 20,000,000 obligation. A customer double-charge, one
// wallet rail away from live.
//
// internal/intent/executor/executor.go carried exactly that expression until
// 2026-08-31, invisible because the two values were equal.
func TestAdapterIsHandedTheProviderRemainderNotTheTotal(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "internal", "intent", "executor", "executor.go")

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	// The Debit literal is the only place an amount reaches an adapter.
	text := string(body)
	start := strings.Index(text, "e.collector.Collect(ctx, Debit{")
	if start < 0 {
		t.Fatal("no Debit literal found — the collector call moved; this guard must move with it")
	}
	end := strings.Index(text[start:], "})")
	if end < 0 {
		t.Fatal("could not find the end of the Debit literal")
	}
	debit := text[start : start+end]

	// Comments legitimately name the wrong expression while explaining why
	// it is wrong, so match the assignment itself rather than the block.
	assign := regexp.MustCompile(`AmountMicros:\s*([^,\n]+)`)
	m := assign.FindStringSubmatch(debit)
	if m == nil {
		t.Fatal("the Debit literal sets no AmountMicros")
	}
	got := strings.TrimSpace(m[1])

	const want = "funding.ProviderRemainderMicros"
	if got != want {
		t.Errorf("the adapter is handed %s; want %s.\n\n"+
			"docs/DESIGN.md:1284 — the integer handed to an adapter is the sealed "+
			"providerRemainder, never grossObligation and never wallet funding.\n"+
			"These are equal only while fundingFor hardcodes a zero wallet allocation. "+
			"With a real split, handing the total charges the rail for the whole "+
			"obligation AND draws the wallet for its share.", got, want)
	}
}
