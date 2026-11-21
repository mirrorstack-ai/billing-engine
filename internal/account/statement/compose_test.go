package statement_test

import (
	"testing"
	"time"

	"github.com/mirrorstack-ai/billing-engine/internal/account/statement"
)

func date(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// baseFX is the illustrative rate from the issue's golden vectors — not a
// real quote. RateDate 2026-12-31 is a Thursday, strictly before the
// 2027-01-01 statement date used throughout this file.
func baseFX() statement.FX {
	return statement.FX{
		Rate:     "32.1500",
		RateDate: date(2026, 12, 31),
		Source:   "illustrative, not a quote",
	}
}

func baseInput(lines []statement.Line) statement.Input {
	return statement.Input{
		StatementDate:   date(2027, 1, 1),
		UsageWindowFrom: date(2026, 12, 1),
		UsageWindowTo:   date(2027, 1, 1),
		Lines:           lines,
		FX:              baseFX(),
		DueRule:         statement.NewDueDayOfMonth(15),
	}
}

func micros(v int64) *int64 { return &v }

func TestCompose_Golden1_SingleLine200USD(t *testing.T) {
	st, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindPlatformFee, Subject: "Platform fee", AmountMicros: 200_000_000},
	}))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if st.SubtotalUSDMicros != 200_000_000 {
		t.Errorf("SubtotalUSDMicros = %d, want 200000000", st.SubtotalUSDMicros)
	}
	if st.SubtotalTWD != 6430 {
		t.Errorf("SubtotalTWD = %d, want 6430", st.SubtotalTWD)
	}
	if st.TaxTWD != 322 {
		t.Errorf("TaxTWD = %d, want 322 (321.5 rounds up)", st.TaxTWD)
	}
	if st.TotalTWD != 6752 {
		t.Errorf("TotalTWD = %d, want 6752", st.TotalTWD)
	}
	if !st.DueDate.Equal(date(2027, 1, 15)) {
		t.Errorf("DueDate = %s, want 2027-01-15", st.DueDate)
	}
	if st.FX.RateScaled != 321500 {
		t.Errorf("FX.RateScaled = %d, want 321500", st.FX.RateScaled)
	}
}

func TestCompose_Golden2_TwoLines177USD(t *testing.T) {
	st, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindAppPlan, Subject: "App plan", AmountMicros: 120_000_000},
		{Kind: statement.KindUsage, Subject: "Usage", AmountMicros: 57_000_000},
	}))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if st.SubtotalUSDMicros != 177_000_000 {
		t.Errorf("SubtotalUSDMicros = %d, want 177000000", st.SubtotalUSDMicros)
	}
	if st.SubtotalTWD != 5691 {
		t.Errorf("SubtotalTWD = %d, want 5691 (5690.55 rounds up)", st.SubtotalTWD)
	}
	if st.TaxTWD != 285 {
		t.Errorf("TaxTWD = %d, want 285 (284.55 rounds up)", st.TaxTWD)
	}
	if st.TotalTWD != 5976 {
		t.Errorf("TotalTWD = %d, want 5976", st.TotalTWD)
	}
}

func TestCompose_RoundsSubtotalOnceNotPerLine(t *testing.T) {
	// Three USD $0.01 lines (30000 micros total = USD $0.03 = NT$0.9645).
	// Rounded once on the subtotal this is NT$1; rounded per line each
	// $0.01 (NT$0.3215) would round down to NT$0.
	st, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindManual, Subject: "a", AmountMicros: 10_000},
		{Kind: statement.KindManual, Subject: "b", AmountMicros: 10_000},
		{Kind: statement.KindManual, Subject: "c", AmountMicros: 10_000},
	}))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if st.SubtotalTWD != 1 {
		t.Errorf("SubtotalTWD = %d, want 1 (single-pass rounding)", st.SubtotalTWD)
	}
}

func TestCompose_CreditLineMatchesGolden1(t *testing.T) {
	st, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindPlatformFee, Subject: "Platform fee", AmountMicros: 250_000_000},
		{Kind: statement.KindManual, Subject: "Credit", AmountMicros: -50_000_000},
	}))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if st.SubtotalUSDMicros != 200_000_000 || st.SubtotalTWD != 6430 || st.TaxTWD != 322 || st.TotalTWD != 6752 {
		t.Errorf("credit line result = %+v, want same totals as Golden 1", st)
	}
}

func TestCompose_UsageLineIdenticalRegardlessOfOtherLines(t *testing.T) {
	usageLine := statement.Line{Kind: statement.KindUsage, Subject: "App usage", AmountMicros: 42_000_000}

	alone, err := statement.Compose(baseInput([]statement.Line{usageLine}))
	if err != nil {
		t.Fatalf("Compose (alone): %v", err)
	}

	withOthers, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindPlatformFee, Subject: "Platform fee", AmountMicros: 999_000_000},
		usageLine,
		{Kind: statement.KindManual, Subject: "Manual credit", AmountMicros: -1_000_000},
	}))
	if err != nil {
		t.Fatalf("Compose (with others): %v", err)
	}

	gotAlone := alone.Lines[0]
	var gotWithOthers statement.Line
	for _, l := range withOthers.Lines {
		if l.Kind == statement.KindUsage {
			gotWithOthers = l
		}
	}
	if gotAlone != gotWithOthers {
		t.Errorf("usage line changed shape: alone=%+v withOthers=%+v", gotAlone, gotWithOthers)
	}
	if gotWithOthers.AmountMicros != 42_000_000 {
		t.Errorf("usage line amount = %d, want unchanged 42000000", gotWithOthers.AmountMicros)
	}
}

func TestCompose_Refusals(t *testing.T) {
	tests := []struct {
		name  string
		input statement.Input
	}{
		{
			name: "zero total",
			input: baseInput([]statement.Line{
				{Kind: statement.KindManual, Subject: "zero", AmountMicros: 0},
			}),
		},
		{
			name: "negative total",
			input: baseInput([]statement.Line{
				{Kind: statement.KindManual, Subject: "credit", AmountMicros: -10_000_000},
			}),
		},
		{
			name: "rate date on statement date",
			input: func() statement.Input {
				in := baseInput([]statement.Line{{Kind: statement.KindManual, AmountMicros: 1_000_000}})
				in.FX.RateDate = in.StatementDate
				return in
			}(),
		},
		{
			name: "rate date after statement date",
			input: func() statement.Input {
				in := baseInput([]statement.Line{{Kind: statement.KindManual, AmountMicros: 1_000_000}})
				in.FX.RateDate = date(2027, 1, 2)
				return in
			}(),
		},
		{
			name: "rate date is a weekend",
			input: func() statement.Input {
				in := baseInput([]statement.Line{{Kind: statement.KindManual, AmountMicros: 1_000_000}})
				in.FX.RateDate = date(2026, 12, 27) // Sunday
				return in
			}(),
		},
		{
			name: "rate zero",
			input: func() statement.Input {
				in := baseInput([]statement.Line{{Kind: statement.KindManual, AmountMicros: 1_000_000}})
				in.FX.Rate = "0"
				return in
			}(),
		},
		{
			name: "rate more than 4 decimals",
			input: func() statement.Input {
				in := baseInput([]statement.Line{{Kind: statement.KindManual, AmountMicros: 1_000_000}})
				in.FX.Rate = "32.15005"
				return in
			}(),
		},
		{
			name: "unknown line kind",
			input: baseInput([]statement.Line{
				{Kind: statement.LineKind("late_fee"), AmountMicros: 1_000_000},
			}),
		},
		{
			name: "quantity times unit disagrees with amount",
			input: baseInput([]statement.Line{
				{Kind: statement.KindOrgSeats, Subject: "seats", Quantity: micros(5), UnitMicros: micros(10_000_000), AmountMicros: 40_000_000},
			}),
		},
		{
			name: "total above the US$1,000,000 cap",
			input: baseInput([]statement.Line{
				{Kind: statement.KindManual, AmountMicros: statement.MaxUSDSubtotalMicros + 1},
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := statement.Compose(tt.input); err == nil {
				t.Fatalf("Compose(%s): want error, got none", tt.name)
			}
		})
	}
}

func TestCompose_QuantityTimesUnitMatchingAmountIsAccepted(t *testing.T) {
	st, err := statement.Compose(baseInput([]statement.Line{
		{Kind: statement.KindOrgSeats, Subject: "seats", Quantity: micros(5), UnitMicros: micros(10_000_000), AmountMicros: 50_000_000},
	}))
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if st.SubtotalUSDMicros != 50_000_000 {
		t.Errorf("SubtotalUSDMicros = %d, want 50000000", st.SubtotalUSDMicros)
	}
}
