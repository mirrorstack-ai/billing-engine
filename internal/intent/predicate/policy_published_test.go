package predicate

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// The placeholder revisions the cycle proposers seal under today.
// Copied deliberately rather than imported: internal/account/cycle
// depends on this package, and the point of the test is that these
// literal strings must never buy a permit, wherever they come from.
const (
	placeholderTerms     = "unpublished/pending-decision-12"
	placeholderPriceBook = "unpublished/pending-decision-12"
	placeholderNotice    = "unpublished/pending-decision-12"
	placeholderTax       = "not-applicable/pending-decision-12"
)

// placeholderState builds the state that made this defect invisible: an
// intent sealed under the four §12 placeholders, and an authorization
// minted under the SAME placeholders.
//
// That self-consistency is the whole problem. Every equality check in
// Permits compares the intent's revision against the authorization's
// copy, so a matching placeholder satisfies all of them. Nothing was
// comparing either one against the set of published revisions, because
// no such comparison existed.
//
// Every other clause is left passing, so a refusal here can only come
// from ClausePolicyPublished. If this test ever refuses for a second
// reason, it has stopped testing what it is named for.
func placeholderState(t *testing.T) SealedState {
	t.Helper()

	sealed, err := intent.Seal(intent.Draft{
		Payer:             intent.Subject{Kind: "org", ID: "org-1"},
		Currency:          "USD",
		Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25)},
		Kind:              kind,
		PriceBookRevision: placeholderPriceBook,
		TermsRevision:     placeholderTerms,
		Tax: intent.TaxDetermination{
			Resolved: true, Jurisdiction: "TW", RuleRevision: placeholderTax, AmountMicros: 1_250,
		},
		AuthorizationID:  "auth-1",
		NoticePolicy:     placeholderNotice,
		ExecuteNotBefore: windowStart,
		ExecuteNotAfter:  windowEnd,
		SourceFactKeys:   []string{"fact-1"},
	})
	if err != nil {
		t.Fatalf("Seal under placeholders: %v", err)
	}

	auth, err := intent.Authorize(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kind},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000, FrequencyCeiling: 100,
		TermsRevision: placeholderTerms, PriceBook: placeholderPriceBook,
		NoticePolicy:     placeholderNotice,
		EffectiveFrom:    windowStart,
		ExpiresAt:        windowEnd.AddDate(1, 0, 0),
		AcceptanceDigest: "accept-1",
	})
	if err != nil {
		t.Fatalf("Authorize under placeholders: %v", err)
	}

	state := permittedState(t)
	state.Intent = sealed
	state.Authorization = auth
	state.Notice.DeliveredBytesDigest = sealed.Digest()
	state.Notice.Policy = placeholderNotice
	state.Funding.GrossMicros = sealed.TotalMicros()
	state.Funding.ProviderRemainderMicros = sealed.TotalMicros()
	return state
}

// This is the regression the whole change exists for. Before 2026-08-30
// this state was PERMITTED: every clause passed, and the engine would
// have collected under a revision that names an open decision.
func TestPlaceholderRevisionsCannotCollect(t *testing.T) {
	verdict := Evaluate(placeholderState(t))

	if verdict.Permitted {
		t.Fatal("an intent sealed under unpublished §12 placeholders was permitted to collect")
	}
	if len(verdict.Refused) != 1 || verdict.Refused[0] != ClausePolicyPublished {
		t.Fatalf("expected exactly one refusal, %s; got %v\n"+
			"a second refusal means this test is passing for an unrelated reason "+
			"and no longer proves the placeholder itself is what stops collection",
			ClausePolicyPublished, verdict.Refused)
	}
}

// The mirror of the above: the guard must not be a blanket refusal.
// If published revisions were also refused, the test above would pass
// while the clause was simply broken shut, and no leg could ever be
// enabled.
func TestPublishedRevisionsStillCollect(t *testing.T) {
	verdict := Evaluate(permittedState(t))
	if !verdict.Permitted {
		t.Fatalf("published revisions were refused: %v", verdict.Refused)
	}
}

// PolicyDigestsMatch and publication are independent halves of the same
// clause name. Neither may stand in for the other.
func TestPublicationAndDigestMatchAreBothRequired(t *testing.T) {
	t.Run("published but digests do not match", func(t *testing.T) {
		state := permittedState(t)
		state.PolicyDigestsMatch = false
		if Evaluate(state).Permitted {
			t.Fatal("a non-matching policy digest was permitted")
		}
	})

	t.Run("digests match but revisions are unpublished", func(t *testing.T) {
		state := placeholderState(t)
		state.PolicyDigestsMatch = true
		if Evaluate(state).Permitted {
			t.Fatal("an unpublished revision rode in on a matching digest")
		}
	})
}

// Each of the four sealed revisions must be able to refuse on its own.
// A guard that only reads the first one would pass every test above,
// because the fixture makes all four unpublished at once.
func TestEachSealedRevisionCanRefuseAlone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*intent.Draft)
	}{
		{"terms", func(d *intent.Draft) { d.TermsRevision = placeholderTerms }},
		{"price book", func(d *intent.Draft) { d.PriceBookRevision = placeholderPriceBook }},
		{"notice policy", func(d *intent.Draft) { d.NoticePolicy = placeholderNotice }},
		{"tax rule", func(d *intent.Draft) { d.Tax.RuleRevision = placeholderTax }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			draft := intent.Draft{
				Payer:             intent.Subject{Kind: "org", ID: "org-1"},
				Currency:          "USD",
				Lines:             []intent.Line{intent.NewLine("quiz.render", "quiz-core", "1.4.0", 1_000, 25)},
				Kind:              kind,
				PriceBookRevision: "pb-2026-08",
				TermsRevision:     "terms-2026-01",
				Tax: intent.TaxDetermination{
					Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-2026-05", AmountMicros: 1_250,
				},
				AuthorizationID:  "auth-1",
				NoticePolicy:     "email/v1",
				ExecuteNotBefore: windowStart,
				ExecuteNotAfter:  windowEnd,
				SourceFactKeys:   []string{"fact-1"},
			}
			tc.apply(&draft)

			sealed, err := intent.Seal(draft)
			if err != nil {
				t.Fatalf("Seal: %v", err)
			}

			unpublished := intent.UnpublishedRevisions(sealed)
			if len(unpublished) != 1 {
				t.Fatalf("expected exactly one unpublished revision, got %v", unpublished)
			}
		})
	}
}

// The tax revision is the one no clause read at all before this change,
// and the one whose placeholder makes a claim rather than a deferral.
// A blank rule revision on a Resolved determination must not read as
// "no tax applies" — INV-004 forbids exactly that inference.
func TestBlankTaxRuleRevisionIsNotPublished(t *testing.T) {
	if intent.RevisionPublished("") {
		t.Fatal("a blank revision was treated as published")
	}
	if intent.RevisionPublished("   ") {
		t.Fatal("a whitespace revision was treated as published")
	}
}

func TestRevisionPublishedBoundaries(t *testing.T) {
	for _, tc := range []struct {
		revision  string
		published bool
		why       string
	}{
		{"terms-2026-01", true, "an ordinary published id"},
		{"not-applicable/2026-q3-tw-v1", true, "a published decision that tax does not apply is still published"},
		{"unpublished/pending-decision-12", false, "the reserved prefix"},
		{"unpublished/anything", false, "the reserved prefix without the marker"},
		{"not-applicable/pending-decision-12", false, "the marker without the reserved prefix"},
		{"  unpublished/x  ", false, "the prefix must be seen through surrounding space"},
		{"published-pending-decision-later", false, "the marker anywhere in the string"},
	} {
		if got := intent.RevisionPublished(tc.revision); got != tc.published {
			t.Errorf("RevisionPublished(%q) = %v, want %v — %s", tc.revision, got, tc.published, tc.why)
		}
	}
}
