package intent

import (
	"errors"
	"math"
	"testing"
	"time"
)

var (
	windowStart = time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
)

func validDraft() Draft {
	return Draft{
		Payer:    Subject{Kind: "org", ID: "org-1"},
		Currency: "usd",
		Lines: []Line{
			NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25),
		},
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: TaxDetermination{
			Resolved:     true,
			Jurisdiction: "TW",
			RuleRevision: "tax-2026-05",
			AmountMicros: 1_250,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1"},
	}
}

// The digest is what a customer is told they approved. Two different
// intents sharing one would mean a customer can be shown one document
// and charged under another, so the encoding must be injective: no
// pair of distinct field sequences may produce the same bytes.
//
// The dangerous case is not random collision, it is structural. With
// plain concatenation, meter "ab" + module "c" and meter "a" +
// module "bc" both spell "abc". A separator character only moves the
// problem to whichever value contains the separator. These cases are
// chosen to spell the same thing when the length prefixes are removed.
func TestDigestDoesNotCollideOnFieldBoundaries(t *testing.T) {
	shift := func(meter, module string) ChargeIntent {
		d := validDraft()
		d.Lines = []Line{NewLine(meter, module, "1.4.0", 1_000, 25)}
		intent, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal: %v", err)
		}
		return intent
	}

	pairs := [][2][2]string{
		{{"ab", "c"}, {"a", "bc"}},
		{{"a|b", "c"}, {"a", "b|c"}},
		{{"", "abc"}, {"abc", ""}},
		{{"a\x00b", "c"}, {"a", "\x00bc"}},
	}
	for _, pair := range pairs {
		left := shift(pair[0][0], pair[0][1])
		right := shift(pair[1][0], pair[1][1])
		if left.Digest() == right.Digest() {
			t.Errorf("meter=%q module=%q and meter=%q module=%q share digest %s",
				pair[0][0], pair[0][1], pair[1][0], pair[1][1], left.Digest())
		}
	}
}

// A collection's length is encoded, so a sequence of lines cannot be
// confused with a different sequence that concatenates the same way.
func TestDigestDistinguishesLineGrouping(t *testing.T) {
	one := validDraft()
	one.Lines = []Line{NewLine("m", "mod", "1.0.0", 2, 10)}

	two := validDraft()
	two.Lines = []Line{
		NewLine("m", "mod", "1.0.0", 1, 10),
		NewLine("m", "mod", "1.0.0", 1, 10),
	}

	sealedOne, err := Seal(one)
	if err != nil {
		t.Fatal(err)
	}
	sealedTwo, err := Seal(two)
	if err != nil {
		t.Fatal(err)
	}

	if sealedOne.TotalMicros() != sealedTwo.TotalMicros() {
		t.Fatalf("fixture is wrong: totals differ (%d vs %d)",
			sealedOne.TotalMicros(), sealedTwo.TotalMicros())
	}
	if sealedOne.Digest() == sealedTwo.Digest() {
		t.Error("one line of 2 and two lines of 1 share a digest; the customer is shown different documents")
	}
}

// Every sealed field must reach the digest. A field outside it is a
// field that can differ between the document a customer read and the
// one that settles.
func TestEveryFieldChangesTheDigest(t *testing.T) {
	base, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*Draft){
		"payer kind":          func(d *Draft) { d.Payer.Kind = "user" },
		"payer id":            func(d *Draft) { d.Payer.ID = "org-2" },
		"currency":            func(d *Draft) { d.Currency = "twd" },
		"line meter":          func(d *Draft) { d.Lines[0].Meter = "other" },
		"line module":         func(d *Draft) { d.Lines[0].Module = "other-core" },
		"line module version": func(d *Draft) { d.Lines[0].ModuleVersion = "1.4.1" },
		"line quantity":       func(d *Draft) { d.Lines[0] = NewLine("quiz.render", "quiz-core", "1.4.0", 1_001, 25) },
		"line unit price":     func(d *Draft) { d.Lines[0] = NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 26) },
		"price book":          func(d *Draft) { d.PriceBookRevision = "pb-2026-09" },
		"terms revision":      func(d *Draft) { d.TermsRevision = "terms-2026-02" },
		"tax jurisdiction":    func(d *Draft) { d.Tax.Jurisdiction = "JP" },
		"tax rule revision":   func(d *Draft) { d.Tax.RuleRevision = "tax-2026-06" },
		"tax amount":          func(d *Draft) { d.Tax.AmountMicros = 1_251 },
		"authorization":       func(d *Draft) { d.AuthorizationID = "auth-2" },
		"notice policy":       func(d *Draft) { d.NoticePolicy = "sms/v1" },
		"window start":        func(d *Draft) { d.ExecuteNotBefore = windowStart.Add(time.Second) },
		"window end":          func(d *Draft) { d.ExecuteNotAfter = windowEnd.Add(time.Second) },
		"source facts":        func(d *Draft) { d.SourceFactKeys = []string{"fact-2"} },
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			d := validDraft()
			mutate(&d)
			changed, err := Seal(d)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if changed.Digest() == base.Digest() {
				t.Errorf("changing the %s left the digest unchanged; the field is outside what is attested", name)
			}
		})
	}
}

// Sealing the same draft twice must produce the same digest, or the
// offline verifier of docs/VERIFICATION.md §4 cannot reproduce
// anything. Nothing in the encoding may depend on map order, a clock,
// or an address.
func TestDigestIsStableAcrossSeals(t *testing.T) {
	for i := 0; i < 32; i++ {
		first, err := Seal(validDraft())
		if err != nil {
			t.Fatal(err)
		}
		second, err := Seal(validDraft())
		if err != nil {
			t.Fatal(err)
		}
		if first.Digest() != second.Digest() {
			t.Fatalf("two seals of one draft differ: %s vs %s", first.Digest(), second.Digest())
		}
	}
}

// A sealed intent is immutable. The accessors hand back copies, so a
// caller cannot reach through a returned slice into the document.
func TestSealedIntentCannotBeMutatedThroughAccessors(t *testing.T) {
	sealed, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	before := sealed.Digest()

	lines := sealed.Lines()
	lines[0].Quantity = 999_999
	lines[0].Meter = "tampered"

	keys := sealed.SourceFactKeys()
	keys[0] = "tampered"

	if sealed.Digest() != before {
		t.Fatal("the digest changed after mutating a returned slice")
	}
	if got := sealed.Lines()[0].Quantity; got != 1_000 {
		t.Errorf("quantity is now %d; the returned slice aliased the sealed intent", got)
	}
	if got := sealed.SourceFactKeys()[0]; got != "fact-1" {
		t.Errorf("source fact is now %q; the returned slice aliased the sealed intent", got)
	}
}

// INV-004: an unknown input must quarantine the intent rather than
// dispatch an effect. Nothing is defaulted — an absent currency is not
// USD, an undetermined tax is not zero, and a missing authorization is
// not permission.
func TestSealRefusesRatherThanDefaults(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Draft)
		wantErr error
	}{
		{"unknown payer kind", func(d *Draft) { d.Payer.Kind = "tenant" }, ErrPayerUnknown},
		{"no payer id", func(d *Draft) { d.Payer.ID = "" }, ErrPayerUnknown},
		{"no currency", func(d *Draft) { d.Currency = "" }, ErrCurrencyMissing},
		{"no lines", func(d *Draft) { d.Lines = nil }, ErrNoLines},
		{"negative quantity", func(d *Draft) { d.Lines[0].Quantity = -1 }, ErrNegativeLine},
		{"negative price", func(d *Draft) { d.Lines[0].UnitPriceMicros = -1 }, ErrNegativeLine},
		{"no price book", func(d *Draft) { d.PriceBookRevision = "" }, ErrPriceBookMissing},
		{"no terms revision", func(d *Draft) { d.TermsRevision = "" }, ErrTermsMissing},
		{"undetermined tax", func(d *Draft) { d.Tax.Resolved = false }, ErrTaxUnresolved},
		{"no authorization", func(d *Draft) { d.AuthorizationID = "" }, ErrAuthorizationUnset},
		{"no notice policy", func(d *Draft) { d.NoticePolicy = "" }, ErrNoticePolicyUnset},
		{"no window start", func(d *Draft) { d.ExecuteNotBefore = time.Time{} }, ErrWindowUnset},
		{"no window end", func(d *Draft) { d.ExecuteNotAfter = time.Time{} }, ErrWindowUnset},
		{"inverted window", func(d *Draft) { d.ExecuteNotAfter = windowStart.Add(-time.Second) }, ErrWindowInverted},
		{"no source facts", func(d *Draft) { d.SourceFactKeys = nil }, ErrNoSourceFacts},
		{"negative tax", func(d *Draft) { d.Tax.AmountMicros = -1 }, ErrTaxNegative},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := validDraft()
			tc.mutate(&d)

			sealed, err := Seal(d)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Seal error = %v, want %v", err, tc.wantErr)
			}
			if sealed.Sealed() {
				t.Error("a refused draft still produced a sealed intent")
			}
		})
	}
}

// A justified zero-tax result is a determination and must seal. INV-004
// forbids an unknown input becoming zero, not a determination that came
// out at zero — conflating the two would make correct zero-tax
// customers unbillable.
func TestResolvedZeroTaxSeals(t *testing.T) {
	d := validDraft()
	d.Tax = TaxDetermination{
		Resolved:     true,
		Jurisdiction: "TW",
		RuleRevision: "tax-2026-05",
		AmountMicros: 0,
	}

	sealed, err := Seal(d)
	if err != nil {
		t.Fatalf("a resolved zero-tax determination must seal: %v", err)
	}
	if sealed.TotalMicros() != sealed.SubtotalMicros() {
		t.Errorf("total %d != subtotal %d with zero tax", sealed.TotalMicros(), sealed.SubtotalMicros())
	}
}

// The total is derived from the lines and the determination, never
// supplied. There is nowhere for a caller to put a total, which is
// INV-001 made structural.
func TestTotalIsDerived(t *testing.T) {
	d := validDraft()
	d.Lines = []Line{
		NewLine("a", "mod", "1.0.0", 3, 100),
		NewLine("b", "mod", "1.0.0", 2, 250),
	}
	d.Tax.AmountMicros = 40

	sealed, err := Seal(d)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := sealed.SubtotalMicros(), int64(3*100+2*250); got != want {
		t.Errorf("subtotal = %d, want %d", got, want)
	}
	if got, want := sealed.TotalMicros(), int64(3*100+2*250+40); got != want {
		t.Errorf("total = %d, want %d", got, want)
	}
	if got, want := sealed.Lines()[0].AmountMicros(), int64(300); got != want {
		t.Errorf("line amount = %d, want %d", got, want)
	}
}

// INV-003: a one-unit change creates a new intent that supersedes the
// old one. Editing is not offered, so the replacement is a distinct
// document with its own digest that names what it replaced.
func TestSupersedeCreatesALinkedNewDocument(t *testing.T) {
	original, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}

	corrected := validDraft()
	corrected.Lines = []Line{NewLine("quiz.render", "quiz-core", "1.4.0", 1_001, 25)}

	replacement, err := original.Supersede(corrected)
	if err != nil {
		t.Fatal(err)
	}

	if replacement.Digest() == original.Digest() {
		t.Error("the replacement shares the original's digest")
	}
	if replacement.Supersedes() != original.Digest() {
		t.Errorf("replacement supersedes %q, want %q", replacement.Supersedes(), original.Digest())
	}
	if original.Supersedes() != "" {
		t.Error("the original was modified by superseding it")
	}
	// The link is attested, so a replacement pointing at a DIFFERENT
	// original is a different document even when everything else about
	// it matches.
	unrelated := validDraft()
	unrelated.SourceFactKeys = []string{"fact-9"}
	otherOriginal, err := Seal(unrelated)
	if err != nil {
		t.Fatal(err)
	}
	if otherOriginal.Digest() == original.Digest() {
		t.Fatal("fixture is wrong: the two originals are the same document")
	}

	otherReplacement, err := otherOriginal.Supersede(corrected)
	if err != nil {
		t.Fatal(err)
	}
	if otherReplacement.Digest() == replacement.Digest() {
		t.Error("two replacements with identical content but different originals share a digest; " +
			"the supersede link is not part of what is attested")
	}
}

// Line's factors are exported, so a caller can build one as a struct
// literal and skip NewLine. Seal must still derive the amount: the
// alternative is a silent zero, which undercharges, and undercharging
// is the failure nobody reports.
func TestSealDerivesLineAmountsEvenWithoutNewLine(t *testing.T) {
	d := validDraft()
	d.Lines = []Line{{
		Meter: "quiz.render", Module: "quiz-core", ModuleVersion: "1.4.0",
		Quantity: 5, UnitPriceMicros: 100,
	}}

	sealed, err := Seal(d)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := sealed.Lines()[0].AmountMicros(), int64(500); got != want {
		t.Errorf("line amount = %d, want %d", got, want)
	}
	if got, want := sealed.SubtotalMicros(), int64(500); got != want {
		t.Errorf("subtotal = %d, want %d", got, want)
	}

	// And a struct literal must seal to the same document NewLine
	// produces, or two callers building the same charge disagree.
	viaConstructor := validDraft()
	viaConstructor.Lines = []Line{NewLine("quiz.render", "quiz-core", "1.4.0", 5, 100)}
	expected, err := Seal(viaConstructor)
	if err != nil {
		t.Fatal(err)
	}
	if sealed.Digest() != expected.Digest() {
		t.Error("a struct literal and NewLine produced different documents for the same charge")
	}
}

// A caller cannot smuggle an amount in past the factors, because there
// is nowhere to put one: the field is unexported and Seal recomputes.
func TestSealIgnoresAnyAmountTheCallerManagedToSet(t *testing.T) {
	tampered := NewLine("quiz.render", "quiz-core", "1.4.0", 5, 100)
	tampered.Quantity = 1 // change a factor after construction

	d := validDraft()
	d.Lines = []Line{tampered}

	sealed, err := Seal(d)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := sealed.SubtotalMicros(), int64(100); got != want {
		t.Errorf("subtotal = %d, want %d — the amount must follow the factors, "+
			"not the value captured when the line was built", got, want)
	}
}

func TestZeroIntentIsNotSealed(t *testing.T) {
	var zero ChargeIntent
	if zero.Sealed() {
		t.Fatal("a zero ChargeIntent reported itself as sealed")
	}
	if zero.Digest() != "" {
		t.Fatal("a zero ChargeIntent has a digest")
	}
}

// int64 wraps silently, and a wrapped product does not fail loudly — it
// turns an enormous charge into a small or negative one, and every
// later check then agrees with it because the number really is small.
func TestSealRefusesAmountsThatDoNotFit(t *testing.T) {
	const maxInt64 = math.MaxInt64

	cases := map[string]func(*Draft){
		"one line's product overflows": func(d *Draft) {
			d.Lines = []Line{NewLine("m", "mod", "1.0.0", maxInt64, 2)}
		},
		"two lines that each fit overflow together": func(d *Draft) {
			d.Lines = []Line{
				NewLine("a", "mod", "1.0.0", maxInt64/2, 1),
				NewLine("b", "mod", "1.0.0", maxInt64/2, 1),
				NewLine("c", "mod", "1.0.0", 10, 1),
			}
		},
		"the subtotal fits but tax pushes it over": func(d *Draft) {
			d.Lines = []Line{NewLine("m", "mod", "1.0.0", maxInt64-10, 1)}
			d.Tax.AmountMicros = 100
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			d := validDraft()
			mutate(&d)

			sealed, err := Seal(d)
			if !errors.Is(err, ErrAmountOverflow) {
				t.Fatalf("Seal error = %v, want %v", err, ErrAmountOverflow)
			}
			if sealed.Sealed() {
				t.Error("an overflowing draft still produced a sealed intent")
			}
		})
	}
}

// The arithmetic helpers are the thing a wrong overflow check would get
// wrong, so they are exercised at their boundaries directly.
func TestOverflowHelpers(t *testing.T) {
	const maxInt64 = math.MaxInt64
	const minInt64 = math.MinInt64

	mulCases := []struct {
		a, b int64
		ok   bool
	}{
		{0, maxInt64, true},
		{maxInt64, 0, true},
		{1, maxInt64, true},
		{2, maxInt64 / 2, true},
		{2, maxInt64, false},
		{maxInt64, 2, false},
		{-1, maxInt64, true},
		{-1, minInt64, false},
	}
	for _, tc := range mulCases {
		if _, ok := mulOK(tc.a, tc.b); ok != tc.ok {
			t.Errorf("mulOK(%d, %d) ok = %v, want %v", tc.a, tc.b, ok, tc.ok)
		}
	}

	addCases := []struct {
		a, b int64
		ok   bool
	}{
		{0, 0, true},
		{maxInt64 - 1, 1, true},
		{maxInt64, 1, false},
		{minInt64 + 1, -1, true},
		{minInt64, -1, false},
		{maxInt64, -1, true},
	}
	for _, tc := range addCases {
		if _, ok := addOK(tc.a, tc.b); ok != tc.ok {
			t.Errorf("addOK(%d, %d) ok = %v, want %v", tc.a, tc.b, ok, tc.ok)
		}
	}
}
