package intent

import (
	"errors"
	"math"
	"reflect"
	"strings"
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
		Kind:              kindWalletTopUp,
		PriceBookRevision: "pb-2026-08",
		TermsRevision:     "terms-2026-01",
		Tax: TaxDetermination{
			Resolved:     true,
			Jurisdiction: "TW",
			RuleRevision: "tax-2026-05",
			AmountMicros: 1_250,
			Verification: TaxNotApplicable,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     "email/v1",
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1"},

		// Stated, not left zero. A fixture whose fields are all zero cannot
		// prove that anything carries them: an omission on the store side and
		// an omission in the fixture cancel out, and the round trip passes.
		// That is how `collects` stayed broken.
		SelectedRail:          "stripe",
		RoutingPolicyRevision: "routing-2026-08",
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
//
// 🔴 The keys are Draft field paths, not prose, and TestNoDraftFieldEscapes
// TheDigestMutations below turns that into a floor: a Draft field with no
// case here fails the build.
//
// It is not decoration. Until 2026-08-31 this map had nineteen prose keys
// and silently omitted FOUR sealed fields — Tax.Verification (canonical v2),
// WalletAllocationMicros (v3), SelectedRail and RoutingPolicyRevision (v4).
// Each was threaded through Draft, ChargeIntent, computeDigest, the store and
// a migration, and each was added without a case proving it reached the
// digest. Three supersessions, three omissions, and a green suite every time,
// because a hand-written map cannot notice what is not in it.
func TestEveryFieldChangesTheDigest(t *testing.T) {
	for name, m := range digestMutations {
		t.Run(name, func(t *testing.T) {
			// A case may supply its own base when validDraft cannot
			// express the field — see WalletAllocationMicros. Base and
			// mutation always come from the same draft, so the pair
			// still differs in exactly one field.
			build := m.base
			if build == nil {
				build = validDraft
			}
			base, err := Seal(build())
			if err != nil {
				t.Fatalf("Seal base: %v", err)
			}

			d := build()
			m.mutate(&d)
			changed, err := Seal(d)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}
			if changed.Digest() == base.Digest() {
				t.Errorf("changing %s left the digest unchanged; the field is outside what is attested", name)
			}
		})
	}
}

// digestMutations is keyed by the Draft field path each case perturbs.
//
// The path matters: TestNoDraftFieldEscapesTheDigestMutations reads the
// leading identifier of every key and requires the set to cover Draft's
// exported fields exactly.
type digestMutation struct {
	// base overrides validDraft for a field the default draft cannot
	// legally carry. Nil means validDraft.
	base   func() Draft
	mutate func(*Draft)
}

var digestMutations = map[string]digestMutation{
	"Payer.Kind":               {mutate: func(d *Draft) { d.Payer.Kind = "user" }},
	"Payer.ID":                 {mutate: func(d *Draft) { d.Payer.ID = "org-2" }},
	"Currency":                 {mutate: func(d *Draft) { d.Currency = "twd" }},
	"Lines[0].Meter":           {mutate: func(d *Draft) { d.Lines[0].Meter = "other" }},
	"Lines[0].Module":          {mutate: func(d *Draft) { d.Lines[0].Module = "other-core" }},
	"Lines[0].ModuleVersion":   {mutate: func(d *Draft) { d.Lines[0].ModuleVersion = "1.4.1" }},
	"Lines[0].Quantity":        {mutate: func(d *Draft) { d.Lines[0] = NewLine("quiz.render", "quiz-core", "1.4.0", 1_001, 25) }},
	"Lines[0].UnitPriceMicros": {mutate: func(d *Draft) { d.Lines[0] = NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 26) }},
	"PriceBookRevision":        {mutate: func(d *Draft) { d.PriceBookRevision = "pb-2026-09" }},
	"TermsRevision":            {mutate: func(d *Draft) { d.TermsRevision = "terms-2026-02" }},
	"Kind":                     {mutate: func(d *Draft) { d.Kind = KindSubscriptionStart }},
	"Tax.Jurisdiction":         {mutate: func(d *Draft) { d.Tax.Jurisdiction = "JP" }},
	"Tax.RuleRevision":         {mutate: func(d *Draft) { d.Tax.RuleRevision = "tax-2026-06" }},
	"Tax.AmountMicros":         {mutate: func(d *Draft) { d.Tax.AmountMicros = 1_251 }},
	// Canonical v2. Both remaining classes are legal for a resolved
	// determination, so this changes HOW the figure was established without
	// changing the figure — which is the whole reason the class is sealed.
	"Tax.Verification": {mutate: func(d *Draft) { d.Tax.Verification = TaxProviderAttested }},
	// Canonical v3. The gross is untouched; only the split moves, so a
	// digest that missed this would let the funding change under an
	// unchanged document.
	"WalletAllocationMicros": {
		// validDraft is an auto_topup, and §6:493-495 forbids the wallet
		// funding its own refill — so Seal refuses any allocation on it.
		// This case therefore rates a kind that MAY draw on credit, and
		// compares against the same draft with a zero allocation, so the
		// pair still differs in exactly one field.
		base:   func() Draft { d := validDraft(); d.Kind = KindModuleUsage; return d },
		mutate: func(d *Draft) { d.WalletAllocationMicros = 1 },
	},
	// Canonical v4.
	"SelectedRail":          {mutate: func(d *Draft) { d.SelectedRail = "other-rail" }},
	"RoutingPolicyRevision": {mutate: func(d *Draft) { d.RoutingPolicyRevision = "routing-2026-09" }},
	"AuthorizationID":       {mutate: func(d *Draft) { d.AuthorizationID = "auth-2" }},
	"NoticePolicy":          {mutate: func(d *Draft) { d.NoticePolicy = "sms/v1" }},
	"ExecuteNotBefore":      {mutate: func(d *Draft) { d.ExecuteNotBefore = windowStart.Add(time.Second) }},
	"ExecuteNotAfter":       {mutate: func(d *Draft) { d.ExecuteNotAfter = windowEnd.Add(time.Second) }},
	"SourceFactKeys":        {mutate: func(d *Draft) { d.SourceFactKeys = []string{"fact-2"} }},
}

// linkMutations covers the two sealed fields a Draft cannot express.
//
// collects and supersedes are set by CollectRemainderOf and Supersede AFTER
// Seal, so no entry in digestMutations can reach them. They are exactly the
// class of field this commit exists to fix — `collects` was inside the digest
// with no column, no Stored field and no Rehydrate restore — so the floor
// below requires a case for them too.
var linkMutations = map[string]func(t *testing.T) (bare, linked ChargeIntent){
	"collects": func(t *testing.T) (ChargeIntent, ChargeIntent) {
		t.Helper()
		source, err := Seal(validDraft())
		if err != nil {
			t.Fatalf("Seal source: %v", err)
		}
		d := validDraft()
		d.Kind = KindCollectReceivable
		bare, err := Seal(d)
		if err != nil {
			t.Fatalf("Seal receivable: %v", err)
		}
		linked, err := source.CollectRemainderOf(d)
		if err != nil {
			t.Fatalf("CollectRemainderOf: %v", err)
		}
		return bare, linked
	},
	"supersedes": func(t *testing.T) (ChargeIntent, ChargeIntent) {
		t.Helper()
		original, err := Seal(validDraft())
		if err != nil {
			t.Fatalf("Seal original: %v", err)
		}
		bare, err := Seal(validDraft())
		if err != nil {
			t.Fatalf("Seal bare: %v", err)
		}
		linked, err := original.Supersede(validDraft())
		if err != nil {
			t.Fatalf("Supersede: %v", err)
		}
		return bare, linked
	},
}

// Both links must move the digest, or a document does not attest what it
// replaced or what it collects.
func TestTheLinksChangeTheDigest(t *testing.T) {
	for name, build := range linkMutations {
		t.Run(name, func(t *testing.T) {
			bare, linked := build(t)
			if bare.Digest() == linked.Digest() {
				t.Errorf("setting %s left the digest unchanged; the link is outside what is attested", name)
			}
		})
	}
}

// derivedSealedFields are inside the digest but cannot have a mutation case,
// because nothing can move them independently. Keys are normalisePath form.
//
// 🔴 Writing this down is the point. subtotalMicros and totalMicros are
// functions of the lines and the tax; providerRemainderMicros is
// `totalMicros - walletAllocationMicros` (chargeintent.go:472). So the
// WalletAllocationMicros case in digestMutations CANNOT distinguish "the
// allocation is in the digest" from "the remainder is in the digest": moving
// one necessarily moves the other, and the case passes with either encoded.
//
// Review found that, not the suite — with `e.int(c.walletAllocationMicros)`
// deleted from computeDigest the whole suite stays green, because the derived
// remainder still moves. Two integers that always move together are one fact
// encoded twice, and the redundancy is what makes the pair untestable. The
// real fix is to stop encoding the derived one, which changes the canonical
// encoding and therefore belongs to the next schema supersession rather than
// to this commit. Until then this map is the record that the guard is weaker
// than it looks.
var derivedSealedFields = map[string]string{
	"subtotalmicros":          "the sum of the lines",
	"totalmicros":             "subtotal plus tax",
	"providerremaindermicros": "totalMicros - walletAllocationMicros; moves with the allocation, so the pair cannot be separated",
}

// unsealedFields are ChargeIntent fields deliberately outside the digest.
// Keys are normalisePath form.
var unsealedFields = map[string]string{
	"digest":       "it IS the digest",
	"tax.resolved": "Seal refuses an unresolved determination, so it is constant true in every sealed intent and computeDigest does not encode it",
}

// TestNoSealedFieldEscapesTheDigestMutations is the floor under both maps.
//
// 🔴 It reflects over ChargeIntent, not Draft, and it walks NESTED fields.
//
// The first version of this test did neither, and review proved it vacuous
// twice over. It reflected over Draft, so `collects` and `supersedes` — the
// very fields whose omission this commit fixes — were invisible to it. And it
// compared only the LEADING identifier of each key, so one case on any nested
// struct marked the whole struct covered: deleting the `Tax.Verification`
// case left it PASSING, and Tax.Verification is one of the four omissions the
// test was written to catch.
//
// A guard for "a field was forgotten" that cannot see a forgotten field is
// the exact defect this repository keeps finding. Comparing full leaf paths
// against the type the digest is actually taken over is what fixes it.
func TestNoSealedFieldEscapesTheDigestMutations(t *testing.T) {
	covered := map[string]bool{}
	for path := range digestMutations {
		covered[normalisePath(path)] = true
	}
	for name := range linkMutations {
		covered[normalisePath(name)] = true
	}

	leaves := sealedLeaves(reflect.TypeOf(ChargeIntent{}), "")
	if len(leaves) < 20 {
		t.Fatalf("the walk found only %d leaves in ChargeIntent; the reflection is wrong, "+
			"and a floor that enumerates almost nothing proves almost nothing", len(leaves))
	}

	known := map[string]bool{}
	for _, leaf := range leaves {
		known[normalisePath(leaf)] = true
	}

	for _, leaf := range leaves {
		key := normalisePath(leaf)
		if reason, ok := unsealedFields[key]; ok {
			if covered[key] {
				t.Errorf("%s is declared outside the digest (%q) but has a case proving it "+
					"changes the digest; one of the two is wrong", leaf, reason)
			}
			continue
		}
		if _, ok := derivedSealedFields[key]; ok {
			continue
		}
		if !covered[key] {
			t.Errorf("%s is inside the sealed document but no case proves it reaches the "+
				"digest. Add one to digestMutations or linkMutations, or declare it in "+
				"unsealedFields or derivedSealedFields with a reason. A field outside the "+
				"digest can differ between the document a customer accepted and the one "+
				"that settles.", leaf)
		}
	}

	// The covered set needs its own floor: a key that stopped naming a real
	// field would silently satisfy nothing.
	for key := range covered {
		if !known[key] {
			t.Errorf("a mutation case is keyed %q, which is not a leaf of ChargeIntent. The "+
				"key must be the field path it perturbs, or the coverage check above is "+
				"comparing against names that no longer exist.", key)
		}
	}
}

// sealedLeaves walks a type and returns every leaf field path.
//
// It descends into structs and into the element type of a slice of structs,
// because that is where the omissions were: Tax.Verification and the Line
// fields are leaves a top-level walk never reaches.
func sealedLeaves(t reflect.Type, prefix string) []string {
	var out []string
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := prefix + f.Name

		ft := f.Type
		if ft.Kind() == reflect.Slice {
			// A slice of structs contributes its element's leaves; a slice
			// of scalars is itself a leaf.
			if ft.Elem().Kind() == reflect.Struct {
				out = append(out, sealedLeaves(ft.Elem(), name+"[].")...)
				continue
			}
			out = append(out, name)
			continue
		}
		// time.Time is a struct but an opaque value here. Descending into it
		// would enumerate wall, ext and loc, which no mutation case can name.
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) {
			out = append(out, sealedLeaves(ft, name+".")...)
			continue
		}
		out = append(out, name)
	}
	return out
}

// normalisePath makes "Lines[0].Meter", "lines[].Meter" and "LINES.METER" the
// same key, so a case and the field it names match however each was written.
func normalisePath(path string) string {
	var b strings.Builder
	depth := 0
	for _, r := range path {
		switch {
		case r == '[':
			depth++
		case r == ']':
			depth--
		case depth == 0:
			b.WriteRune(r)
		}
	}
	return strings.ToLower(b.String())
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
		{"no charge kind", func(d *Draft) { d.Kind = "" }, ErrKindMissing},
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
		Verification: TaxNotApplicable,
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

// storedFrom projects a sealed intent into the shape storage returns,
// which is what a row read back looks like before anything has checked
// it.
func storedFrom(c ChargeIntent) Stored {
	return Stored{
		Digest:            c.Digest(),
		Payer:             c.Payer(),
		Currency:          c.Currency(),
		Lines:             c.Lines(),
		Kind:              c.Kind(),
		PriceBookRevision: c.PriceBookRevision(),
		TermsRevision:     c.TermsRevision(),
		Tax:               c.Tax(),
		AuthorizationID:   c.AuthorizationID(),
		NoticePolicy:      c.NoticePolicy(),
		SourceFactKeys:    c.SourceFactKeys(),
		Supersedes:        c.Supersedes(),
		Collects:          c.Collects(),
		SubtotalMicros:    c.SubtotalMicros(),
		TotalMicros:       c.TotalMicros(),

		WalletAllocationMicros:  c.WalletAllocationMicros(),
		ProviderRemainderMicros: c.ProviderRemainderMicros(),
		SelectedRail:            c.SelectedRail(),
		RoutingPolicyRevision:   c.RoutingPolicyRevision(),
	}
}

// 🔴 storedFrom must mirror the WHOLE of intent.Stored.
//
// Until this test existed it mirrored fourteen of nineteen fields, and every
// round-trip test in this file passed anyway — because validDraft left the
// missing ones at their zero values too, so an omission in the fixture and an
// omission in the helper cancelled out exactly.
//
// That is the same shape as the defect this commit fixes: `collects` was
// inside the digest, absent from Stored, absent from Rehydrate, and absent
// from every fixture, so nothing disagreed with anything. A round trip
// between two incomplete halves proves only that they are incomplete in the
// same way.
//
// The floor is: a sealed intent built from validDraft must produce a Stored
// with no zero-valued field, unless the field is declared below.
func TestStoredFromMirrorsEveryStoredField(t *testing.T) {
	// Fields legitimately zero for a plain, non-linked intent.
	alwaysZeroForThisFixture := map[string]string{
		"Supersedes":             "validDraft is an original, not a replacement",
		"Collects":               "validDraft is not a receivable",
		"WalletAllocationMicros": "validDraft is an auto_topup, which §6:493-495 forbids funding from the wallet",
	}

	sealed, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	stored := storedFrom(sealed)
	stored.ExecuteNotBefore, stored.ExecuteNotAfter = sealed.ExecutionWindow()

	v := reflect.ValueOf(stored)
	tp := v.Type()
	if tp.NumField() < 15 {
		t.Fatalf("Stored has %d fields; the reflection target looks wrong", tp.NumField())
	}
	for i := 0; i < tp.NumField(); i++ {
		f := tp.Field(i)
		if !f.IsExported() {
			continue
		}
		if _, ok := alwaysZeroForThisFixture[f.Name]; ok {
			continue
		}
		if v.Field(i).IsZero() {
			t.Errorf("storedFrom leaves Stored.%s at its zero value. Either the helper "+
				"does not copy it — in which case every round-trip test in this file is "+
				"passing on a field nobody carries — or validDraft does not state it, in "+
				"which case the fixture cannot tell a carried field from a dropped one.",
				f.Name)
		}
	}
}

func TestRehydrateRoundTrips(t *testing.T) {
	original, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	stored := storedFrom(original)
	stored.ExecuteNotBefore, stored.ExecuteNotAfter = original.ExecutionWindow()

	loaded, err := Rehydrate(stored)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if loaded.Digest() != original.Digest() {
		t.Fatalf("digest changed on round trip: %s -> %s", original.Digest(), loaded.Digest())
	}
	if loaded.TotalMicros() != original.TotalMicros() {
		t.Errorf("total changed on round trip")
	}
}

// The digest checks the DATABASE, not a caller. A trigger protects one
// table in one deployment; this protects the document wherever it has
// been — a restored backup, a replicated row, a migration that rewrote
// a column in passing, or a deliberate edit by whoever holds the
// database credential.
func TestRehydrateRefusesATamperedRow(t *testing.T) {
	original, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	base := storedFrom(original)
	base.ExecuteNotBefore, base.ExecuteNotAfter = original.ExecutionWindow()

	tampers := map[string]func(*Stored){
		"the amount":           func(s *Stored) { s.Lines[0].Quantity = 1 },
		"the unit price":       func(s *Stored) { s.Lines[0].UnitPriceMicros = 1 },
		"the payer":            func(s *Stored) { s.Payer = Subject{Kind: "org", ID: "org-2"} },
		"the currency":         func(s *Stored) { s.Currency = "TWD" },
		"the price book":       func(s *Stored) { s.PriceBookRevision = "pb-2026-09" },
		"the terms":            func(s *Stored) { s.TermsRevision = "terms-2026-02" },
		"the charge kind":      func(s *Stored) { s.Kind = KindSubscriptionStart },
		"the tax amount":       func(s *Stored) { s.Tax.AmountMicros = 1 },
		"the authorization":    func(s *Stored) { s.AuthorizationID = "auth-2" },
		"the notice policy":    func(s *Stored) { s.NoticePolicy = "sms/v1" },
		"the execution window": func(s *Stored) { s.ExecuteNotAfter = windowEnd.Add(time.Hour) },
		"the source facts":     func(s *Stored) { s.SourceFactKeys = []string{"fact-2"} },
		"the supersede link":   func(s *Stored) { s.Supersedes = "some-other-digest" },
		"the digest itself":    func(s *Stored) { s.Digest = "not-the-digest" },
		"the stored subtotal":  func(s *Stored) { s.SubtotalMicros++ },
		"the stored total":     func(s *Stored) { s.TotalMicros++ },
	}

	for name, tamper := range tampers {
		t.Run(name, func(t *testing.T) {
			stored := base
			stored.Lines = append([]Line(nil), base.Lines...)
			stored.SourceFactKeys = append([]string(nil), base.SourceFactKeys...)
			tamper(&stored)

			loaded, err := Rehydrate(stored)
			if !errors.Is(err, ErrDigestMismatch) {
				t.Fatalf("Rehydrate accepted a row with %s edited: err = %v", name, err)
			}
			if loaded.Sealed() {
				t.Error("a refused row still produced a sealed intent")
			}
		})
	}
}

// A superseding correction must round-trip too, since the link is part
// of what is attested.
func TestRehydratePreservesTheSupersedeLink(t *testing.T) {
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

	stored := storedFrom(replacement)
	stored.ExecuteNotBefore, stored.ExecuteNotAfter = replacement.ExecutionWindow()

	loaded, err := Rehydrate(stored)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}
	if loaded.Supersedes() != original.Digest() {
		t.Errorf("supersede link lost: %q", loaded.Supersedes())
	}
	if loaded.Digest() != replacement.Digest() {
		t.Error("a superseding intent did not round-trip to its own digest")
	}
}

// A row that could never have been sealed must not load, and the reason
// should say what was wrong rather than only that the hash differed.
// 🔴 A stored provider remainder that disagrees with the row it sits in must
// not load, and the digest ALONE does not catch it.
//
// Rehydrate DERIVES the remainder from the total and the wallet allocation
// (INV-002, one derivation), so rebuilt.providerRemainderMicros is correct
// whatever the column says, and the recomputed digest matches. The stored
// column is therefore checked against nothing unless something checks it
// explicitly — which nothing did until this commit.
//
// The mutation that proves this test: replace the comparison in Rehydrate
// with `if false` and only this test goes red.
func TestRehydrateRefusesAStoredRemainderThatWasNotDerived(t *testing.T) {
	original, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	stored := storedFrom(original)
	stored.ExecuteNotBefore, stored.ExecuteNotAfter = original.ExecutionWindow()

	if _, err := Rehydrate(stored); err != nil {
		t.Fatalf("the untouched fixture does not load, so this test proves nothing: %v", err)
	}

	// One column rewritten, by a migration in passing or a restored backup.
	// Every other field, and the digest, are exactly as sealed.
	stored.ProviderRemainderMicros = original.ProviderRemainderMicros() + 1

	if _, err := Rehydrate(stored); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("a row whose provider remainder was rewritten loaded anyway (%v). "+
			"The adapter is handed that number, so a row nobody derived it from is a "+
			"charge for an amount nobody computed.", err)
	}
}

func TestRehydrateRefusesARowThatCouldNeverHaveBeenSealed(t *testing.T) {
	original, err := Seal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	stored := storedFrom(original)
	stored.ExecuteNotBefore, stored.ExecuteNotAfter = original.ExecutionWindow()
	stored.Tax.Resolved = false

	_, err = Rehydrate(stored)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("err = %v, want %v", err, ErrDigestMismatch)
	}
	if !errors.Is(err, ErrTaxUnresolved) {
		t.Errorf("the error does not say the tax was unresolved: %v", err)
	}
}
