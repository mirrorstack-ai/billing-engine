package intent

import (
	"errors"
	"testing"
	"time"
)

func freqAuth(t *testing.T, frequency int) BillingAuthorization {
	t.Helper()
	a, err := AuthorizeAccepted(AuthorizationGrant{
		ID: "auth-freq", Scope: ScopeStanding,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
		PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
		FrequencyCeiling: frequency, NoticeLeadTime: 24 * time.Hour,
		TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
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
		_, err := AuthorizeAccepted(AuthorizationGrant{
			ID: "auth-x", Scope: ScopeStanding,
			Subject:  Subject{Kind: "user", ID: "acct-1"},
			Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
			PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
			FrequencyCeiling: frequency, NoticeLeadTime: 24 * time.Hour,
			TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
			EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		})
		if !errors.Is(err, ErrAuthFrequencyMissing) {
			t.Fatalf("FrequencyCeiling %d: got %v, want ErrAuthFrequencyMissing", frequency, err)
		}
	}
}

// A one-time authorization covers exactly one document by construction, so it
// needs no attempt bound and must not be forced to invent one.
func TestAOneTimeAuthorizationNeedsNoFrequencyCeiling(t *testing.T) {
	_, err := AuthorizeAccepted(AuthorizationGrant{
		ID: "auth-once", Scope: ScopeOneTime,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", IntentDigest: "digest-1",
		TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
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

// 🔴 The amount RULE, which no ceiling expresses.
//
// A ceiling says "no more than". §6's auto_topup arrangement says "when my
// balance falls below X, charge me exactly Y" — and "exactly Y" is part of
// what the customer accepted. A $5 top-up under an accepted rule of $20 sits
// inside every bound and is still not the agreement; so does $19.
func TestTheAmountRuleBindsWhereCeilingsDoNot(t *testing.T) {
	auth, err := AuthorizeAccepted(AuthorizationGrant{
		ID: "auth-rule", Scope: ScopeStanding,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
		PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
		FrequencyCeiling: 10, NoticeLeadTime: 24 * time.Hour,
		// "below $10, top up by exactly $20"
		TriggerBelowMicros: 10_000_000,
		TopUpAmountMicros:  20_000_000,
		TermsRevision:      "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	seal := func(t *testing.T, micros int64) ChargeIntent {
		t.Helper()
		d := catalogDraft(KindAutoTopUp)
		d.Lines = []Line{NewLine("top-up", "wallet", "1", 1, micros)}
		s, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal(%d): %v", micros, err)
		}
		return s
	}

	// Exactly the rule: permitted.
	if d := auth.Permits(seal(t, 20_000_000), at, PriorUse{}); hasRefusal(d.Refusals, RefusalAmountNotTheAcceptedRule) {
		t.Fatalf("the accepted amount was refused: %v", d.Refusals)
	}

	// Inside every ceiling, and not the rule.
	for _, micros := range []int64{5_000_000, 19_000_000, 20_000_001, 49_000_000} {
		d := auth.Permits(seal(t, micros), at, PriorUse{})
		if !hasRefusal(d.Refusals, RefusalAmountNotTheAcceptedRule) {
			t.Fatalf("%d micros was permitted under an accepted rule of 20_000_000; refusals = %v",
				micros, d.Refusals)
		}
		if hasRefusal(d.Refusals, RefusalOverPerCharge) {
			t.Fatalf("%d micros tripped the per-charge ceiling too, so this proves nothing "+
				"about the amount rule", micros)
		}
	}
}

// The two halves of a balance-triggered arrangement travel together. A trigger
// with no amount rule permits any size once the balance falls; an amount rule
// with no trigger permits that size at any time. Either alone is a different
// arrangement from the one the customer accepted.
func TestTheTriggerAndTheAmountRuleMustBeGivenTogether(t *testing.T) {
	base := func() AuthorizationGrant {
		return AuthorizationGrant{
			ID: "auth-pair", Scope: ScopeStanding,
			Subject:  Subject{Kind: "user", ID: "acct-1"},
			Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
			PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
			FrequencyCeiling: 10, NoticeLeadTime: 24 * time.Hour,
			TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
			EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}

	triggerOnly := base()
	triggerOnly.TriggerBelowMicros = 10_000_000
	if _, err := AuthorizeAccepted(triggerOnly); !errors.Is(err, ErrAuthTriggerIncomplete) {
		t.Fatalf("a trigger with no amount rule was accepted: %v", err)
	}

	ruleOnly := base()
	ruleOnly.TopUpAmountMicros = 20_000_000
	if _, err := AuthorizeAccepted(ruleOnly); !errors.Is(err, ErrAuthTriggerIncomplete) {
		t.Fatalf("an amount rule with no trigger was accepted: %v", err)
	}

	neither := base()
	if _, err := AuthorizeAccepted(neither); err != nil {
		t.Fatalf("a standing authorization that is not balance-triggered was refused: %v", err)
	}
}

// An amount rule above the per-charge ceiling can never be satisfied: every
// attempt refuses for being over the ceiling. That is dead on arrival, not
// restrictive, and Authorize should say so rather than mint it.
func TestAnAmountRuleAboveTheCeilingIsRefusedAtAuthorize(t *testing.T) {
	_, err := AuthorizeAccepted(AuthorizationGrant{
		ID: "auth-dead", Scope: ScopeStanding,
		Subject:  Subject{Kind: "user", ID: "acct-1"},
		Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
		PerChargeCeiling: 10_000_000, PeriodCeiling: 200_000_000,
		FrequencyCeiling: 10, NoticeLeadTime: 24 * time.Hour,
		TriggerBelowMicros: 5_000_000,
		TopUpAmountMicros:  20_000_000, // above the per-charge ceiling
		TermsRevision:      "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
		EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, ErrAuthRuleExceedsCeiling) {
		t.Fatalf("an unsatisfiable authorization was minted: %v", err)
	}
}

// 🔴 Never stack an attempt on an unknown outcome.
//
// §6 requires a standing authorization to bind its "pending-or-failed
// treatment". An attempt submitted to a provider and never confirmed either
// way MAY ALREADY HAVE TAKEN THE MONEY, so starting another is a coin flip on
// double-charging. Refusing while anything is in flight is the only treatment
// available before the outcome is known, and the only one that cannot be
// wrong.
//
// This is why it is not merely the frequency ceiling in another guise: an
// account can be far below every count and amount bound and still have one
// unresolved attempt outstanding.
func TestAnUnresolvedAttemptBlocksTheNextOne(t *testing.T) {
	auth := freqAuth(t, 10)
	sealed := freqIntent(t)
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	// Well inside every other bound — one attempt, tiny spend.
	d := auth.Permits(sealed, at, PriorUse{SpendMicros: 1_000, Attempts: 1, Unresolved: 1})

	if d.Permitted {
		t.Fatal("a new attempt was permitted while a prior one's outcome was unknown — " +
			"that is a coin flip on charging the customer twice")
	}
	if !hasRefusal(d.Refusals, RefusalAttemptUnresolved) {
		t.Fatalf("refusals = %v, want %s", d.Refusals, RefusalAttemptUnresolved)
	}
	// It must not be the frequency or amount bounds doing the work, or this
	// test would pass while the unresolved rule was absent.
	if hasRefusal(d.Refusals, RefusalOverFrequency) || hasRefusal(d.Refusals, RefusalOverPeriod) {
		t.Fatalf("another bound also refused (%v), so this proves nothing about "+
			"unresolved attempts", d.Refusals)
	}
}

// And a resolved history does not block: attempts that finished, however they
// finished, are counted by the frequency ceiling and nothing more.
func TestResolvedAttemptsDoNotBlock(t *testing.T) {
	auth := freqAuth(t, 10)
	sealed := freqIntent(t)
	at := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	d := auth.Permits(sealed, at, PriorUse{SpendMicros: 1_000, Attempts: 4, Unresolved: 0})
	if hasRefusal(d.Refusals, RefusalAttemptUnresolved) {
		t.Fatalf("a fully resolved history was treated as in flight: %v", d.Refusals)
	}
}

// §6's "provider and mandate": WHICH rail, and WHICH reusable mandate on it.
// An off-session standing authorization naming neither authorises a charge
// against whatever instrument happens to be on file later — which survives the
// customer replacing their card, and is not what anybody accepted.
func TestTheProviderAndMandateMustBeGivenTogether(t *testing.T) {
	base := func() AuthorizationGrant {
		return AuthorizationGrant{
			ID: "auth-inst", Scope: ScopeStanding,
			Subject:  Subject{Kind: "user", ID: "acct-1"},
			Currency: "usd", Kinds: []ChargeKind{KindAutoTopUp},
			PerChargeCeiling: 50_000_000, PeriodCeiling: 200_000_000,
			FrequencyCeiling: 10, NoticeLeadTime: 24 * time.Hour,
			TermsRevision: "terms-1", PriceBook: "pb-1", NoticePolicy: "email/v1",
			EffectiveFrom: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt:     time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}

	railOnly := base()
	railOnly.Provider = "stripe"
	if _, err := AuthorizeAccepted(railOnly); !errors.Is(err, ErrAuthInstrumentIncomplete) {
		t.Fatalf("a rail with no mandate was accepted: %v", err)
	}

	mandateOnly := base()
	mandateOnly.MandateReference = "pm_1"
	if _, err := AuthorizeAccepted(mandateOnly); !errors.Is(err, ErrAuthInstrumentIncomplete) {
		t.Fatalf("a mandate with no rail was accepted: %v", err)
	}

	bound := base()
	bound.Provider, bound.MandateReference = "stripe", "pm_1"
	a, err := AuthorizeAccepted(bound)
	if err != nil {
		t.Fatalf("a fully bound instrument was refused: %v", err)
	}
	if !a.InstrumentBound() {
		t.Fatal("InstrumentBound() is false for an authorization naming both")
	}

	unbound := base()
	u, err := AuthorizeAccepted(unbound)
	if err != nil {
		t.Fatalf("an authorization with no instrument was refused: %v", err)
	}
	if u.InstrumentBound() {
		t.Fatal("InstrumentBound() is true for an authorization naming neither")
	}
	if got := a.Grant().Provider; got != "stripe" {
		t.Fatalf("Grant().Provider = %q, want stripe — the binding does not survive a round trip", got)
	}
}
