package intent

import (
	"errors"
	"testing"
	"time"
)

// fixedTax applies a percentage rule so the tests exercise a real
// derivation rather than a constant. rateBps is explicit — including
// when it is zero — because the distinction this whole package rests on
// is between a determination that came out at zero and one that was
// never made, and a fixture that inferred the rule from the amount
// could not express the first.
type fixedTax struct {
	resolved bool
	rateBps  int64
	calls    int
}

func (f *fixedTax) Determine(_ Subject, _ string, subtotal int64, _ time.Time) TaxDetermination {
	f.calls++
	if !f.resolved {
		return TaxDetermination{}
	}
	return TaxDetermination{
		Resolved:     true,
		Jurisdiction: "TW",
		RuleRevision: "tax-2026-05",
		AmountMicros: subtotal * f.rateBps / 10_000,
	}
}

func resolvedTax() *fixedTax { return &fixedTax{resolved: true, rateBps: 500} }

func book(t *testing.T) PriceBookRevision {
	t.Helper()
	revision, err := NewPriceBookRevision(PriceBookDefinition{
		Revision:      "pb-2026-08",
		EffectiveFrom: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		Currency:      "USD",
		Prices: map[PriceKey]int64{
			{Meter: "quiz.render", Module: "quiz-core", ModuleVersion: "1.4.0"}: 25,
			{Meter: "quiz.grade", Module: "quiz-core", ModuleVersion: "1.4.0"}:  40,
		},
	})
	if err != nil {
		t.Fatalf("NewPriceBookRevision: %v", err)
	}
	return revision
}

func fact(key, meter string, quantity int64) UsageFact {
	return UsageFact{
		Subject:        Subject{Kind: "org", ID: "org-1"},
		Meter:          meter,
		Module:         "quiz-core",
		ModuleVersion:  "1.4.0",
		Quantity:       quantity,
		OccurredAt:     time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC),
		IdempotencyKey: key,
	}
}

func rateRequest(t *testing.T, facts ...UsageFact) RateInput {
	t.Helper()
	return RateInput{
		Facts:            facts,
		PriceBook:        book(t),
		Tax:              resolvedTax(),
		AuthorizationID:  "auth-1",
		TermsRevision:    "terms-2026-01",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		RatedAt:          now,
	}
}

func TestRateDerivesTheTotalFromFactsAndPrices(t *testing.T) {
	sealed, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100), fact("f2", "quiz.grade", 10)))
	if err != nil {
		t.Fatalf("Rate: %v", err)
	}

	wantSubtotal := int64(100*25 + 10*40)
	if got := sealed.SubtotalMicros(); got != wantSubtotal {
		t.Errorf("subtotal = %d, want %d", got, wantSubtotal)
	}
	if got, want := sealed.TotalMicros(), wantSubtotal+wantSubtotal*5/100; got != want {
		t.Errorf("total = %d, want %d", got, want)
	}
	if got := sealed.PriceBookRevision(); got != "pb-2026-08" {
		t.Errorf("price book = %q; a charge whose price source is unnamed cannot be recomputed", got)
	}
}

// The digest is an identity, so two deliveries of one batch in
// different orders must produce the same document.
func TestRateIsIndependentOfFactOrder(t *testing.T) {
	a, b, c := fact("f1", "quiz.render", 100), fact("f2", "quiz.grade", 10), fact("f3", "quiz.render", 7)

	forward, err := Rate(rateRequest(t, a, b, c))
	if err != nil {
		t.Fatal(err)
	}
	reversed, err := Rate(rateRequest(t, c, b, a))
	if err != nil {
		t.Fatal(err)
	}

	if forward.Digest() != reversed.Digest() {
		t.Fatalf("delivery order changed the document: %s vs %s", forward.Digest(), reversed.Digest())
	}
}

// Re-delivery is harmless: facts sharing an idempotency key are one
// fact, so a caller that retries does not double a bill.
func TestRateDeduplicatesOnIdempotencyKey(t *testing.T) {
	once, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100)))
	if err != nil {
		t.Fatal(err)
	}
	twice, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100), fact("f1", "quiz.render", 100)))
	if err != nil {
		t.Fatal(err)
	}

	if once.TotalMicros() != twice.TotalMicros() {
		t.Errorf("a re-delivered fact doubled the bill: %d vs %d",
			once.TotalMicros(), twice.TotalMicros())
	}
	if once.Digest() != twice.Digest() {
		t.Error("a re-delivered fact produced a different document")
	}
}

// INV-004: an unpriced meter quarantines. It must never become zero,
// because a free charge is the failure nobody notices.
func TestUnpricedMeterQuarantinesRatherThanChargingZero(t *testing.T) {
	unknown := fact("f9", "quiz.export", 1_000)

	sealed, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100), unknown))

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
	if quarantine.Reason != "price policy" {
		t.Errorf("quarantine reason = %q, want %q", quarantine.Reason, "price policy")
	}
	if len(quarantine.Facts) != 1 {
		t.Errorf("quarantine names %d facts, want 1 — an operator needs to know which", len(quarantine.Facts))
	}
	if sealed.Sealed() {
		t.Error("a quarantined batch still produced a sealed intent")
	}
}

// A price present for one module version must not be reused for
// another. A fact priced under one version stays reproducible after the
// module moves on, and the alternative silently re-rates history.
func TestPriceDoesNotCarryAcrossModuleVersions(t *testing.T) {
	moved := fact("f1", "quiz.render", 100)
	moved.ModuleVersion = "1.5.0"

	_, err := Rate(rateRequest(t, moved))

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine for an unpriced module version", err)
	}
}

// An undetermined tax quarantines. A determination that came out at
// zero does not — conflating them makes correct zero-tax customers
// unbillable.
func TestUndeterminedTaxQuarantines(t *testing.T) {
	req := rateRequest(t, fact("f1", "quiz.render", 100))
	req.Tax = &fixedTax{resolved: false}

	_, err := Rate(req)

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
	if quarantine.Reason != "tax" {
		t.Errorf("quarantine reason = %q, want %q", quarantine.Reason, "tax")
	}
}

func TestResolvedZeroTaxRates(t *testing.T) {
	req := rateRequest(t, fact("f1", "quiz.render", 100))
	req.Tax = &fixedTax{resolved: true, rateBps: 0}

	sealed, err := Rate(req)
	if err != nil {
		t.Fatalf("a resolved zero-tax determination must rate: %v", err)
	}
	if sealed.TotalMicros() != sealed.SubtotalMicros() {
		t.Errorf("total %d != subtotal %d", sealed.TotalMicros(), sealed.SubtotalMicros())
	}
}

// A nil resolver would otherwise be indistinguishable from a zero-tax
// jurisdiction, which is the exact substitution INV-004 forbids.
func TestNilTaxResolverQuarantines(t *testing.T) {
	req := rateRequest(t, fact("f1", "quiz.render", 100))
	req.Tax = nil

	_, err := Rate(req)

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
}

// One intent has one payer. Facts from two subjects rated together
// would produce a document charging one customer for another's usage.
func TestMixedPayersAreRefused(t *testing.T) {
	other := fact("f2", "quiz.render", 5)
	other.Subject = Subject{Kind: "org", ID: "org-2"}

	_, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100), other))

	if !errors.Is(err, ErrRaterMixedSubjects) {
		t.Fatalf("Rate error = %v, want %v", err, ErrRaterMixedSubjects)
	}
}

// A malformed fact quarantines with provenance, rather than being
// dropped. Silently skipping it would undercharge without telling
// anyone.
func TestMalformedFactQuarantines(t *testing.T) {
	bad := fact("f2", "", 5)

	_, err := Rate(rateRequest(t, fact("f1", "quiz.render", 100), bad))

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
	if quarantine.Reason != "usage provenance" {
		t.Errorf("quarantine reason = %q, want %q", quarantine.Reason, "usage provenance")
	}
}

func TestRateRefusesWithoutAPriceBook(t *testing.T) {
	req := rateRequest(t, fact("f1", "quiz.render", 100))
	req.PriceBook = PriceBookRevision{}

	if _, err := Rate(req); !errors.Is(err, ErrRaterBookNotLoaded) {
		t.Fatalf("Rate error = %v, want %v", err, ErrRaterBookNotLoaded)
	}
}

// The price book distinguishes "not priced" from "priced at zero". A
// lookup returning a bare int64 makes them the same value, and the
// caller that forgets to check gets a free charge rather than a
// refusal.
func TestPriceBookDistinguishesUnpricedFromZero(t *testing.T) {
	revision, err := NewPriceBookRevision(PriceBookDefinition{
		Revision:      "pb-zero",
		EffectiveFrom: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		Currency:      "USD",
		Prices: map[PriceKey]int64{
			{Meter: "free.meter", Module: "m", ModuleVersion: "1.0.0"}: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	price, priced := revision.UnitPriceMicros(PriceKey{Meter: "free.meter", Module: "m", ModuleVersion: "1.0.0"})
	if !priced || price != 0 {
		t.Errorf("a deliberate zero price read as (%d, %v), want (0, true)", price, priced)
	}

	_, priced = revision.UnitPriceMicros(PriceKey{Meter: "absent", Module: "m", ModuleVersion: "1.0.0"})
	if priced {
		t.Error("an absent key reported itself as priced")
	}
}

// An identical re-delivery is one fact — that is what the key is for.
// Two DIFFERENT facts under one key are a contradiction about what
// happened, and keeping whichever arrived first silently picks a bill
// based on network ordering.
func TestConflictingFactsUnderOneKeyQuarantine(t *testing.T) {
	first := fact("f1", "quiz.render", 100)
	conflicting := fact("f1", "quiz.render", 5_000)

	_, err := Rate(rateRequest(t, first, conflicting))

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
	if quarantine.Reason != "conflicting usage provenance" {
		t.Errorf("quarantine reason = %q, want %q", quarantine.Reason, "conflicting usage provenance")
	}
}

// The quarantine must not fire on a genuine re-delivery, or every
// retrying caller becomes unbillable.
func TestIdenticalRedeliveryIsStillOneFact(t *testing.T) {
	f := fact("f1", "quiz.render", 100)

	sealed, err := Rate(rateRequest(t, f, f, f))
	if err != nil {
		t.Fatalf("three identical deliveries must rate: %v", err)
	}
	if got, want := sealed.SubtotalMicros(), int64(100*25); got != want {
		t.Errorf("subtotal = %d, want %d", got, want)
	}
}

// A conflict is only a conflict if something actually differs — the
// comparison must cover every field, not just the quantity.
func TestConflictDetectionCoversEveryField(t *testing.T) {
	base := fact("f1", "quiz.render", 100)

	for name, mutate := range map[string]func(*UsageFact){
		"quantity":       func(f *UsageFact) { f.Quantity = 101 },
		"meter":          func(f *UsageFact) { f.Meter = "quiz.grade" },
		"module version": func(f *UsageFact) { f.ModuleVersion = "1.5.0" },
		"subject":        func(f *UsageFact) { f.Subject = Subject{Kind: "org", ID: "org-2"} },
		"occurred at":    func(f *UsageFact) { f.OccurredAt = f.OccurredAt.Add(time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			other := base
			mutate(&other)

			_, err := Rate(rateRequest(t, base, other))

			var quarantine Quarantine
			if !errors.As(err, &quarantine) {
				t.Fatalf("a differing %s under one key did not quarantine: %v", name, err)
			}
		})
	}
}

// A revision cannot price usage that happened before it took effect.
// Reproducibility is the whole claim of a versioned price book, and a
// book applied outside its own window prices history under rules that
// did not exist yet.
func TestUsageBeforeThePriceBookTookEffectQuarantines(t *testing.T) {
	early := fact("f1", "quiz.render", 100)
	early.OccurredAt = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC) // book is effective 2026-08-01

	_, err := Rate(rateRequest(t, early))

	var quarantine Quarantine
	if !errors.As(err, &quarantine) {
		t.Fatalf("Rate error = %v, want a Quarantine", err)
	}
	if quarantine.Reason != "price policy" {
		t.Errorf("quarantine reason = %q, want %q", quarantine.Reason, "price policy")
	}
}

// Usage exactly at the effective instant is inside the window: a
// revision that took effect at midnight prices midnight.
func TestUsageAtTheEffectiveInstantIsPriced(t *testing.T) {
	atBoundary := fact("f1", "quiz.render", 100)
	atBoundary.OccurredAt = time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	if _, err := Rate(rateRequest(t, atBoundary)); err != nil {
		t.Fatalf("usage at the effective instant must price: %v", err)
	}
}
