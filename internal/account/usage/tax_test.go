package usage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEstimateAccountBillTax_RoundsHalfUpAndClampsANegativeBasis(t *testing.T) {
	cases := []struct {
		name         string
		jurisdiction string
		taxable      int64
		wantStatus   string
		wantTax      int64
		wantTaxable  int64
	}{
		{"TW: 5% of $20.00", "TW", 20_000_000, TaxStatusEstimated, 1_000_000, 20_000_000},
		{"TW: half-up at the micro (9 µ$ × 5% = 0.45 → 0)", "TW", 9, TaxStatusEstimated, 0, 9},
		{"TW: half-up at the micro (10 µ$ × 5% = 0.5 → 1)", "TW", 10, TaxStatusEstimated, 1, 10},
		{"TW: a negative projection owes no tax", "TW", -5_000_000, TaxStatusEstimated, 0, 0},
		{"unknown jurisdiction is not configured, basis still reported", "", 20_000_000, TaxStatusNotConfigured, 0, 20_000_000},
		{"a jurisdiction without a rule is not configured", "US", 20_000_000, TaxStatusNotConfigured, 0, 20_000_000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := estimateAccountBillTax(tc.jurisdiction, tc.taxable)
			require.Equal(t, tc.wantStatus, got.Status)
			require.Equal(t, tc.wantTax, got.TaxMicros)
			require.Equal(t, tc.wantTaxable, got.TaxableMicros)
			require.Equal(t, tc.jurisdiction, got.Jurisdiction)
			if tc.wantStatus == TaxStatusEstimated {
				require.EqualValues(t, 500, got.RateBps)
				require.NotEmpty(t, got.RuleRevision)
			} else {
				require.Zero(t, got.RateBps)
				require.Empty(t, got.RuleRevision)
			}
		})
	}
}
