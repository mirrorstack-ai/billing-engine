package collection_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/collection"
)

// --- RiskAssess: tighten triggers -----------------------------------------

func TestRiskAssess_DelinquencyTightensToPrepaid(t *testing.T) {
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 1_000_000_000}
	d := collection.RiskAssess(acct, collection.Signals{Delinquent: true, AccruedArrearsMicros: 1}, false)

	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.True(t, d.ModeChanged, "arrears → prepaid is a transition the caller must persist")
	require.Equal(t, collection.ActionSkipPrepaid, d.Action)
	require.Equal(t, collection.ReasonTightenDelinquent, d.Reason)
}

func TestRiskAssess_OverCreditLimitTightens(t *testing.T) {
	// Accrued arrears >= credit limit → tighten to prepaid, skip the charge.
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 100_000}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 100_000}, false)

	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.True(t, d.ModeChanged)
	require.Equal(t, collection.ActionSkipPrepaid, d.Action)
	require.Equal(t, collection.ReasonTightenOverLimit, d.Reason)
}

func TestRiskAssess_AtCreditLimitBoundaryTightens(t *testing.T) {
	// Exactly at the limit counts as over (>=), so the account never charges
	// right up to the boundary.
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 500_000}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 500_000}, false)
	require.Equal(t, collection.ReasonTightenOverLimit, d.Reason)
}

func TestRiskAssess_ZeroCreditLimitAnyAccrualTightens(t *testing.T) {
	// A zero credit limit means "no off-session headroom": any positive accrual
	// tightens to prepaid (a prepaid-by-policy account).
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 0}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 1}, false)
	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.Equal(t, collection.ReasonTightenOverLimit, d.Reason)
}

func TestRiskAssess_UsageSpikeTightens(t *testing.T) {
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 1_000_000_000}
	d := collection.RiskAssess(acct, collection.Signals{UsageSpike: true, AccruedArrearsMicros: 10}, false)

	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.Equal(t, collection.ActionSkipPrepaid, d.Action)
	require.Equal(t, collection.ReasonTightenSpike, d.Reason)
}

func TestRiskAssess_TightenPrecedenceDelinquencyFirst(t *testing.T) {
	// All three triggers fire; the Reason is deterministic (delinquency wins).
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 1}
	d := collection.RiskAssess(acct, collection.Signals{
		Delinquent: true, UsageSpike: true, AccruedArrearsMicros: 1_000_000,
	}, false)
	require.Equal(t, collection.ReasonTightenDelinquent, d.Reason)
}

func TestRiskAssess_AlreadyPrepaidDelinquentNoRedundantPersist(t *testing.T) {
	// An already-prepaid + delinquent account stays prepaid but does NOT report a
	// transition (no redundant UpdateAccountCollection write).
	acct := collection.Account{Mode: collection.ModePrepaid, CreditLimitMicros: 0}
	d := collection.RiskAssess(acct, collection.Signals{Delinquent: true, AccruedArrearsMicros: 1}, false)
	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.False(t, d.ModeChanged, "already prepaid → no redundant persist")
	require.Equal(t, collection.ActionSkipPrepaid, d.Action)
}

// --- RiskAssess: charge path ----------------------------------------------

func TestRiskAssess_WithinLimitsCharges(t *testing.T) {
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 1_000_000}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 999_999}, false)

	require.Equal(t, collection.ModeArrears, d.DesiredMode)
	require.False(t, d.ModeChanged)
	require.Equal(t, collection.ActionCharge, d.Action)
	require.Equal(t, collection.ReasonWithinLimits, d.Reason)
}

func TestRiskAssess_ZeroAccrualNeverTightens(t *testing.T) {
	// Zero accrual with a zero limit must NOT count as over-limit (the >0 guard).
	acct := collection.Account{Mode: collection.ModeArrears, CreditLimitMicros: 0}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 0}, false)
	require.Equal(t, collection.ActionCharge, d.Action)
	require.Equal(t, collection.ReasonWithinLimits, d.Reason)
}

// --- RiskAssess: relax (conservative) -------------------------------------

func TestRiskAssess_PrepaidStaysPrepaidWithoutCleanVouch(t *testing.T) {
	// No tighten trigger, but no sustained-clean-standing vouch → STAY prepaid.
	// The mere absence of a fresh failure is not a reason to re-trust.
	acct := collection.Account{Mode: collection.ModePrepaid, CreditLimitMicros: 1_000_000}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 10}, false)
	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.False(t, d.ModeChanged)
	require.Equal(t, collection.ActionSkipPrepaid, d.Action)
	require.Equal(t, collection.ReasonAlreadyPrepaid, d.Reason)
}

func TestRiskAssess_PrepaidRelaxesOnCleanStanding(t *testing.T) {
	// Explicit clean-standing vouch + no trigger → relax prepaid → arrears.
	acct := collection.Account{Mode: collection.ModePrepaid, CreditLimitMicros: 1_000_000}
	d := collection.RiskAssess(acct, collection.Signals{AccruedArrearsMicros: 10}, true)
	require.Equal(t, collection.ModeArrears, d.DesiredMode)
	require.True(t, d.ModeChanged)
	require.Equal(t, collection.ActionCharge, d.Action)
	require.Equal(t, collection.ReasonRelaxClean, d.Reason)
}

func TestRiskAssess_CleanStandingNeverOverridesTighten(t *testing.T) {
	// Even with a clean-standing vouch, an active tighten trigger wins — a fresh
	// delinquency is never relaxed away in the same assessment.
	acct := collection.Account{Mode: collection.ModePrepaid, CreditLimitMicros: 1_000_000}
	d := collection.RiskAssess(acct, collection.Signals{Delinquent: true, AccruedArrearsMicros: 10}, true)
	require.Equal(t, collection.ModePrepaid, d.DesiredMode)
	require.Equal(t, collection.ReasonTightenDelinquent, d.Reason)
}

// --- ExceedsSpendCeiling --------------------------------------------------

func TestExceedsSpendCeiling(t *testing.T) {
	for _, tc := range []struct {
		name    string
		acct    collection.Account
		arrears int64
		want    bool
	}{
		{"no ceiling never breaches", collection.Account{HasSpendCeiling: false}, math.MaxInt64, false},
		{"under ceiling", collection.Account{HasSpendCeiling: true, SpendCeilingMicros: 1_000}, 999, false},
		{"at ceiling not over", collection.Account{HasSpendCeiling: true, SpendCeilingMicros: 1_000}, 1_000, false},
		{"above ceiling", collection.Account{HasSpendCeiling: true, SpendCeilingMicros: 1_000}, 1_001, true},
		{"zero ceiling any positive breaches", collection.Account{HasSpendCeiling: true, SpendCeilingMicros: 0}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, collection.ExceedsSpendCeiling(tc.acct, tc.arrears))
		})
	}
}

// --- TrustRampedCreditLimit -----------------------------------------------

func TestTrustRampedCreditLimit_NewAccountFloor(t *testing.T) {
	// Brand-new account: no history, no tenure, no verified PM → the conservative
	// floor ($25.00 = migration 016 DEFAULT).
	require.EqualValues(t, 25_000_000, collection.TrustRampedCreditLimit(0, 0, false))
}

func TestTrustRampedCreditLimit_VerifiedPMRaisesFloor(t *testing.T) {
	// A verified PM alone raises the floor (floor + bonus = $100.00).
	require.EqualValues(t, 100_000_000, collection.TrustRampedCreditLimit(0, 0, true))
}

func TestTrustRampedCreditLimit_GrowsWithHistoryAndTenure(t *testing.T) {
	// floor 25 + PM 75 + 3 invoices×50 + 2 months×10 = 25+75+150+20 = $270.00.
	got := collection.TrustRampedCreditLimit(3, 65 /* 2 full 30-day months */, true)
	require.EqualValues(t, 270_000_000, got)
}

func TestTrustRampedCreditLimit_Monotonic(t *testing.T) {
	// Every input is monotonic: more never lowers the limit.
	base := collection.TrustRampedCreditLimit(2, 60, false)
	require.GreaterOrEqual(t, collection.TrustRampedCreditLimit(3, 60, false), base, "more invoices")
	require.GreaterOrEqual(t, collection.TrustRampedCreditLimit(2, 90, false), base, "more tenure")
	require.GreaterOrEqual(t, collection.TrustRampedCreditLimit(2, 60, true), base, "verified PM")
}

func TestTrustRampedCreditLimit_NegativeInputsClampToFloor(t *testing.T) {
	require.EqualValues(t, 25_000_000, collection.TrustRampedCreditLimit(-5, -100, false))
}

func TestTrustRampedCreditLimit_SaturatesAtCapNoOverflow(t *testing.T) {
	// A pathologically large input count must saturate at the cap ($5,000.00),
	// never wrap int64.
	got := collection.TrustRampedCreditLimit(math.MaxInt32, math.MaxInt32, true)
	require.EqualValues(t, 5_000_000_000, got)
	require.Greater(t, got, int64(0), "no negative wrap")
}

func TestTrustRampedCreditLimit_PartialTenureMonthDoesNotCount(t *testing.T) {
	// 29 days = 0 full months; 30 days = 1 full month.
	require.Equal(t,
		collection.TrustRampedCreditLimit(0, 0, false),
		collection.TrustRampedCreditLimit(0, 29, false),
		"29 days adds no tenure month")
	require.Greater(t,
		collection.TrustRampedCreditLimit(0, 30, false),
		collection.TrustRampedCreditLimit(0, 29, false),
		"30 days adds one tenure month")
}
