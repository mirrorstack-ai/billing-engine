package einvoice_test

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/einvoice"
)

func TestRoundHalfUpNTD(t *testing.T) {
	cases := []struct {
		net  int64
		want int64
	}{
		{0, 0},
		{100, 5},
		{1, 0},  // 1 * 0.05 = 0.05 -> rounds to 0
		{10, 1}, // 10 * 0.05 = 0.5 -> rounds half-up to 1
		{30, 2}, // 30 * 0.05 = 1.5 -> rounds half-up to 2
		{-10, -1},
	}
	for _, c := range cases {
		got := einvoice.RoundHalfUpNTD(c.net)
		if got != c.want {
			t.Errorf("RoundHalfUpNTD(%d) = %d, want %d", c.net, got, c.want)
		}
	}
}

func TestDocumentTotalNTD(t *testing.T) {
	doc := einvoice.Document{
		Lines: []einvoice.Line{
			{NetNTD: 100, TaxNTD: 5, GrossNTD: 105},
			{NetNTD: 200, TaxNTD: 10, GrossNTD: 210},
		},
	}
	net, tax, gross := doc.TotalNTD()
	if net != 300 || tax != 15 || gross != 315 {
		t.Fatalf("TotalNTD() = (%d, %d, %d), want (300, 15, 315)", net, tax, gross)
	}
}
