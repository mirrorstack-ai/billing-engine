package eligibility

import (
	"reflect"
	"testing"
)

// card returns Signals with a good first charge and clean streak, varying only
// the usable-card count — so a case exercises exactly the gate it names.
func card(n int) Signals {
	return Signals{UsableNonFraudCardCount: n, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 0}
}

func TestEvaluate(t *testing.T) {
	cases := []struct {
		name        string
		in          Signals
		wantBlocked bool
		wantReason  Reason
		wantReasons []Reason
	}{
		// --- fully eligible ---
		{
			name:       "one card, first charge paid, no failures",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 0},
			wantReason: ReasonEligible,
		},
		{
			name:       "many cards, streak of one still under the cap",
			in:         Signals{UsableNonFraudCardCount: 3, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 1},
			wantReason: ReasonEligible,
		},

		// --- gate 1: card ---
		{
			name:        "zero cards blocks even with a clean charge history",
			in:          Signals{UsableNonFraudCardCount: 0, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 0},
			wantBlocked: true,
			wantReason:  ReasonNoUsableCard,
			wantReasons: []Reason{ReasonNoUsableCard},
		},
		{
			name:        "negative card count is treated as no card (defensive)",
			in:          Signals{UsableNonFraudCardCount: -1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 0},
			wantBlocked: true,
			wantReason:  ReasonNoUsableCard,
			wantReasons: []Reason{ReasonNoUsableCard},
		},

		// --- gate 2: first charge (grace edges) ---
		{
			name:       "brand-new account, no charge yet, with a card is graced",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeNone, FailedChargeStreak: 0},
			wantReason: ReasonEligible,
		},
		{
			name:       "first charge still pending (retrying) with a card is graced",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargePending, FailedChargeStreak: 0},
			wantReason: ReasonEligible,
		},
		{
			name:        "new account with NO card is blocked on the card, not graced away",
			in:          Signals{UsableNonFraudCardCount: 0, FirstCharge: FirstChargeNone, FailedChargeStreak: 0},
			wantBlocked: true,
			wantReason:  ReasonNoUsableCard,
			wantReasons: []Reason{ReasonNoUsableCard},
		},
		{
			name:        "first charge failed blocks",
			in:          Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeFailed, FailedChargeStreak: 1},
			wantBlocked: true,
			wantReason:  ReasonFirstChargeFailed,
			wantReasons: []Reason{ReasonFirstChargeFailed},
		},

		// --- gate 3: failure streak boundary (< 2, so 2 excluded) ---
		{
			name:       "streak of one is allowed (boundary, still under 2)",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 1},
			wantReason: ReasonEligible,
		},
		{
			name:        "streak of exactly two blocks (2 excluded by < 2)",
			in:          Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 2},
			wantBlocked: true,
			wantReason:  ReasonTooManyFailures,
			wantReasons: []Reason{ReasonTooManyFailures},
		},
		{
			name:        "streak above two blocks",
			in:          Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, FailedChargeStreak: 5},
			wantBlocked: true,
			wantReason:  ReasonTooManyFailures,
			wantReasons: []Reason{ReasonTooManyFailures},
		},

		// --- gate 4: unpaid-invoice boundary (< 2, so 2 excluded) ---
		{
			name:       "no unpaid invoices is allowed",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, UnpaidInvoiceCount: 0},
			wantReason: ReasonEligible,
		},
		{
			name:       "one unpaid invoice is allowed (boundary, still under 2)",
			in:         Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, UnpaidInvoiceCount: 1},
			wantReason: ReasonEligible,
		},
		{
			name:        "exactly two unpaid invoices block (2 excluded by < 2)",
			in:          Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, UnpaidInvoiceCount: 2},
			wantBlocked: true,
			wantReason:  ReasonUnpaidInvoices,
			wantReasons: []Reason{ReasonUnpaidInvoices},
		},
		{
			name:        "many unpaid invoices block",
			in:          Signals{UsableNonFraudCardCount: 1, FirstCharge: FirstChargeSucceeded, UnpaidInvoiceCount: 7},
			wantBlocked: true,
			wantReason:  ReasonUnpaidInvoices,
			wantReasons: []Reason{ReasonUnpaidInvoices},
		},

		// --- multiple gates fail: priority + all-reasons ---
		{
			name:        "no card AND first charge failed: primary is card, both reported",
			in:          Signals{UsableNonFraudCardCount: 0, FirstCharge: FirstChargeFailed, FailedChargeStreak: 1},
			wantBlocked: true,
			wantReason:  ReasonNoUsableCard,
			wantReasons: []Reason{ReasonNoUsableCard, ReasonFirstChargeFailed},
		},
		{
			name:        "all four gates fail: card is primary, all four reported in order",
			in:          Signals{UsableNonFraudCardCount: 0, FirstCharge: FirstChargeFailed, FailedChargeStreak: 3, UnpaidInvoiceCount: 2},
			wantBlocked: true,
			wantReason:  ReasonNoUsableCard,
			wantReasons: []Reason{ReasonNoUsableCard, ReasonFirstChargeFailed, ReasonTooManyFailures, ReasonUnpaidInvoices},
		},
		{
			name:        "first charge failed AND too many failures (card ok): first-charge is primary",
			in:          Signals{UsableNonFraudCardCount: 2, FirstCharge: FirstChargeFailed, FailedChargeStreak: 2},
			wantBlocked: true,
			wantReason:  ReasonFirstChargeFailed,
			wantReasons: []Reason{ReasonFirstChargeFailed, ReasonTooManyFailures},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Evaluate(tc.in)
			if got.Blocked != tc.wantBlocked {
				t.Errorf("Blocked = %v, want %v", got.Blocked, tc.wantBlocked)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if !reflect.DeepEqual(got.Reasons, tc.wantReasons) {
				t.Errorf("Reasons = %v, want %v", got.Reasons, tc.wantReasons)
			}
		})
	}
}

// TestEvaluate_EligibleHasNoReasons pins the contract that an eligible verdict
// carries an empty Reasons slice (a UI iterates it to show "what to fix").
func TestEvaluate_EligibleHasNoReasons(t *testing.T) {
	got := Evaluate(card(1))
	if got.Blocked {
		t.Fatalf("expected eligible, got blocked: %+v", got)
	}
	if len(got.Reasons) != 0 {
		t.Errorf("eligible verdict must have no Reasons, got %v", got.Reasons)
	}
}

// TestEvaluate_PrimaryReasonIsFirstOfReasons pins that, whenever blocked, the
// primary Reason is exactly Reasons[0] — the invariant a caller relies on when
// it shows one headline cause but keeps the full list.
func TestEvaluate_PrimaryReasonIsFirstOfReasons(t *testing.T) {
	for _, n := range []int{0, 1} {
		for _, fc := range []FirstChargeState{FirstChargeNone, FirstChargeSucceeded, FirstChargePending, FirstChargeFailed} {
			for _, streak := range []int{0, 1, 2, 4} {
				v := Evaluate(Signals{UsableNonFraudCardCount: n, FirstCharge: fc, FailedChargeStreak: streak})
				if v.Blocked && v.Reason != v.Reasons[0] {
					t.Errorf("blocked verdict %+v: Reason %q != Reasons[0] %q", v, v.Reason, v.Reasons[0])
				}
				if !v.Blocked && v.Reason != ReasonEligible {
					t.Errorf("unblocked verdict must read ELIGIBLE, got %q", v.Reason)
				}
			}
		}
	}
}
