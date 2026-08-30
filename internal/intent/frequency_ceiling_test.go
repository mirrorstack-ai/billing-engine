package intent

import (
	"errors"
	"testing"
	"time"
)

func freqAuth(t *testing.T, frequency int) BillingAuthorization {
	t.Helper()
	a, err := Authorize(AuthorizationGrant{
		ID: "auth-freq", Scope: ScopeStanding,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
		PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
		FrequencyCeiling: frequency,
		TermsRevision:    "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	return a
}

func freqIntent(t *testing.T) ChargeIntent {
	t.Helper()
	sealed, err := Seal(catalogDraft(KindAutoTopUp))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return sealed
}

// 🔴 The bound no amount ceiling implies.
//
// docs/DESIGN.md §6 names three for auto_topup — "per-attempt, frequency and
// period ceilings" — and migration 054 shipped only the two amount bounds. An
// authorization permitting $50 per attempt and $200 per period also permitted
// two hundred one-dollar attempts: inside every bound it declared, and a
// runaway. The customer agreed to an amount, not to an unbounded number of
// tries at it.
func TestTheFrequencyCeilingBindsWhereAmountCeilingsDoNot(t *testing.T) {
	auth := freqAuth(t, 3)
	sealed := freqIntent(t)
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// Well inside both amount bounds — this is the case the amount
	// ceilings cannot see.
	tiny := PriorUse{SpendMicros: 30_000, Attempts: 3}

	d := auth.Permits(sealed, at, tiny)
	if d.Permitted {
		t.Fatal("a fourth attempt was permitted under a ceiling of 3, while far inside every amount bound")
	}
	if !hasRefusal(d.Refusals, RefusalOverFrequency) {
		t.Fatalf("refusals = %v, want %s — a frequency failure must not report as a neighbouring bound",
			d.Refusals, RefusalOverFrequency)
	}
	// And it must not have tripped an amount bound, or this test would be
	// passing for the wrong reason.
	if hasRefusal(d.Refusals, RefusalOverPerCharge) || hasRefusal(d.Refusals, RefusalOverPeriod) {
		t.Fatalf("an amount ceiling also refused (%v), so this proves nothing about frequency", d.Refusals)
	}
}

// The attempt being authorised is the NEXT one, so the boundary is
// prior+1 <= ceiling. Off by one here either refuses a permitted attempt or
// permits one past the bound.
func TestTheFrequencyBoundaryCountsTheAttemptBeingAuthorised(t *testing.T) {
	auth := freqAuth(t, 3)
	sealed := freqIntent(t)
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		prior     int
		permitted bool
	}{
		{0, true},  // first attempt
		{1, true},  // second
		{2, true},  // third — the last one the ceiling allows
		{3, false}, // fourth
		{9, false},
	} {
		d := auth.Permits(sealed, at, PriorUse{Attempts: tc.prior})
		if got := !hasRefusal(d.Refusals, RefusalOverFrequency); got != tc.permitted {
			t.Fatalf("prior attempts %d: frequency-permitted = %v, want %v (ceiling 3)",
				tc.prior, got, tc.permitted)
		}
	}
}

// A standing authorization with no attempt bound is a standing authorization
// to retry forever, so Authorize must refuse it outright rather than treat
// zero as "unlimited".
func TestAStandingAuthorizationNeedsAFrequencyCeiling(t *testing.T) {
	for _, frequency := range []int{0, -1} {
		_, err := Authorize(AuthorizationGrant{
			ID: "auth-x", Scope: ScopeStanding,
			Subject:  Subject{Kind: "user", ID: "acct-1"},
			Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
			PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
			FrequencyCeiling: frequency,
			TermsRevision:    "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
			EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
			AcceptanceDigest: "accept-1",
		})
		if !errors.Is(err, ErrAuthFrequencyMissing) {
			t.Fatalf("FrequencyCeiling %d: got %v, want ErrAuthFrequencyMissing", frequency, err)
		}
	}
}

// A one-time authorization covers exactly one document by construction, so it
// needs no attempt bound and must not be forced to invent one.
func TestAOneTimeAuthorizationNeedsNoFrequencyCeiling(t *testing.T) {
	_, err := Authorize(AuthorizationGrant{
		ID: "auth-once", Scope: ScopeOneTime,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", IntentDigest: "digest-1",
		TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:        time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		AcceptanceDigest: "accept-1",
	})
	if err != nil {
		t.Fatalf("a one-time authorization was refused for lacking a frequency ceiling: %v", err)
	}
}

// Grant() is what the store persists. A bound it drops is a bound that does
// not survive a round trip — and LoadAuthorization re-runs Authorize, so a
// dropped ceiling would make every stored standing authorization unloadable.
func TestTheFrequencyCeilingSurvivesTheGrantRoundTrip(t *testing.T) {
	auth := freqAuth(t, 7)
	if got := auth.Grant().FrequencyCeiling; got != 7 {
		t.Fatalf("Grant().FrequencyCeiling = %d, want 7", got)
	}
}
