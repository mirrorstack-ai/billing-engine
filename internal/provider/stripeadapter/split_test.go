package stripeadapter

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

func line(desc string, micros int64) intent.Line {
	return intent.NewLine(desc, "ref", "1", 1, micros)
}

// 🔴 The apportioned lines must sum to the SEALED total, always.
//
// The engine rounds micros to cents once, on the provider remainder, exactly
// as the legacy boundary collector rounds once on its net
// (internal/account/cycle/charge.go:595). Rounding each line independently and
// summing gives a different integer — this repo has already measured that at
// one cent on a two-component proration. An invoice whose items do not add up
// to the sealed charge is a charge the customer cannot reconcile against the
// document they accepted.
func TestApportionedLinesAlwaysSumToTheSealedTotal(t *testing.T) {
	cases := []struct {
		name  string
		total int64
		lines []intent.Line
	}{
		{"one line", 435, []intent.Line{line("a", 4_350_000)}},
		{"two lines, exact", 500, []intent.Line{line("a", 3_000_000), line("b", 2_000_000)}},
		{"two lines, one cent to share", 435, []intent.Line{line("a", 4_137_931), line("b", 206_897)}},
		{"three lines, thirds", 100, []intent.Line{line("a", 1), line("b", 1), line("c", 1)}},
		{"four lines, boundary shape", 2001, []intent.Line{
			line("arrears", 5_000), line("base", 20_000_000),
			line("overage", 5_000_000), line("domains", 2_000_000)}},
		{"tiny total, many lines", 1, []intent.Line{line("a", 1_000), line("b", 1_000), line("c", 1_000)}},
		{"lopsided", 999_999, []intent.Line{line("a", 1), line("b", 9_999_990_000)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			items := splitCents(tc.total, tc.lines)

			var sum int64
			for _, it := range items {
				sum += it.cents
				if it.cents < 0 {
					t.Errorf("a line was apportioned a negative amount: %d", it.cents)
				}
				if it.description == "" {
					t.Error("a line has no description, so the customer's statement says nothing")
				}
			}
			if sum != tc.total {
				t.Fatalf("lines sum to %d, the intent sealed %d. The invoice does not add up "+
					"to the charge the customer accepted.", sum, tc.total)
			}
			if len(items) != len(tc.lines) {
				t.Errorf("got %d items for %d sealed lines", len(items), len(tc.lines))
			}
		})
	}
}

// Apportionment must be deterministic: a retried collection is the same
// invoice, or the idempotency keys point at items with different amounts.
func TestApportionmentIsDeterministic(t *testing.T) {
	lines := []intent.Line{line("a", 4_137_931), line("b", 206_897), line("c", 1)}
	first := splitCents(435, lines)
	for i := 0; i < 32; i++ {
		again := splitCents(435, lines)
		for j := range first {
			if first[j] != again[j] {
				t.Fatalf("run %d differs at line %d: %+v vs %+v", i, j, first[j], again[j])
			}
		}
	}
}

// A degenerate charge must still produce one item. Stripe refuses to finalize
// an invoice with no items, so returning nothing would turn a rounding edge
// into a failed collection.
func TestADegenerateChargeStillProducesOneItem(t *testing.T) {
	for name, lines := range map[string][]intent.Line{
		"no lines":   nil,
		"zero lines": {line("a", 0)},
	} {
		t.Run(name, func(t *testing.T) {
			items := splitCents(500, lines)
			if len(items) != 1 || items[0].cents != 500 {
				t.Fatalf("got %+v, want one item of 500", items)
			}
		})
	}
}

// The customer reads the leg's own words, not the digest.
//
// The proposer puts the description in Meter; discarding it made the
// verifiable rail's invoice strictly less informative than the legacy one.
func TestTheCustomerSeesTheSealedDescription(t *testing.T) {
	items := splitCents(500, []intent.Line{
		line("MirrorStack custom domain (prorated) — shop.example.com", 5_000_000),
	})
	if items[0].description != "MirrorStack custom domain (prorated) — shop.example.com" {
		t.Fatalf("description = %q; the customer sees an opaque string instead of what "+
			"the intent sealed", items[0].description)
	}
}
