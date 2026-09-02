package predicate

import (
	"testing"
	"time"

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
	placeholderRouting   = "routing-2026-08"
	placeholderRouting2  = "unpublished/pending-decision-12"
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
			Verification: intent.TaxNotApplicable,
		},
		AuthorizationID:       "auth-1",
		NoticePolicy:          placeholderNotice,
		SelectedRail:          "stripe",
		RoutingPolicyRevision: placeholderRouting,
		ExecuteNotBefore:      windowStart,
		ExecuteNotAfter:       windowEnd,
		SourceFactKeys:        []string{"fact-1"},
	})
	if err != nil {
		t.Fatalf("Seal under placeholders: %v", err)
	}

	auth, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
		ID: "auth-1", Scope: intent.ScopeStanding,
		Subject:  intent.Subject{Kind: "org", ID: "org-1"},
		Currency: "USD", Kinds: []intent.ChargeKind{kind},
		PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000, FrequencyCeiling: 100, NoticeLeadTime: 24 * time.Hour, Provider: "stripe", MandateReference: "pm_test_1",
		TermsRevision: placeholderTerms, PriceBook: placeholderPriceBook,
		NoticePolicy:  placeholderNotice,
		EffectiveFrom: windowStart,
		ExpiresAt:     windowEnd.AddDate(1, 0, 0),
	})
	if err != nil {
		t.Fatalf("Authorize under placeholders: %v", err)
	}

	state := permittedState(t)
	state.Intent = sealed
	state.Authorization = auth
	// The stored acceptance follows the authorization and the payer it is
	// overridden with, or the standing gate refuses for a reason this test is
	// not about — and the refusal it IS about would be hidden behind it.
	state.StandingAcceptance = StandingAcceptance{
		Issued:           true,
		DisclosureDigest: auth.AcceptanceDigest(),
		Payer:            sealed.Payer(),
		Accepted:         true,
		ExpiresAt:        evalNow.Add(365 * 24 * time.Hour),
	}
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
		{"routing policy", func(d *intent.Draft) { d.RoutingPolicyRevision = placeholderRouting2 }},
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
					Verification: intent.TaxNotApplicable,
				},
				AuthorizationID:       "auth-1",
				NoticePolicy:          "email/v1",
				SelectedRail:          "stripe",
				RoutingPolicyRevision: "routing-2026-08",
				ExecuteNotBefore:      windowStart,
				ExecuteNotAfter:       windowEnd,
				SourceFactKeys:        []string{"fact-1"},
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

// ClauseInstrumentBinding was `return s.Unbuilt.InstrumentBinding` — a
// caller-supplied bool, the same hollowness ClausePolicyPublished carried.
//
// The executor's half is genuinely the executor's: verifying the instrument
// needs the rail, which the predicate does not have. But the other half is
// readable right here — an authorization that never named a provider and
// mandate has NO binding to verify, so a caller claiming it verified one is
// claiming something about nothing.
func TestInstrumentBindingNeedsAnAuthorizationThatNamedAnInstrument(t *testing.T) {
	t.Run("the executor claims it verified, but nothing was bound", func(t *testing.T) {
		state := permittedState(t)
		unbound, err := intent.AuthorizeAccepted(intent.AuthorizationGrant{
			ID: "auth-1", Scope: intent.ScopeStanding,
			Subject:  intent.Subject{Kind: "org", ID: "org-1"},
			Currency: "USD", Kinds: []intent.ChargeKind{kind},
			PerChargeCeiling: 1_000_000, PeriodCeiling: 5_000_000,
			FrequencyCeiling: 100, NoticeLeadTime: 24 * time.Hour,
			// Provider and MandateReference deliberately absent.
			TermsRevision: "terms-2026-01", PriceBook: "pb-2026-08",
			NoticePolicy:  "email/v1",
			EffectiveFrom: windowStart,
			ExpiresAt:     windowEnd.AddDate(1, 0, 0),
		})
		if err != nil {
			t.Fatalf("Authorize: %v", err)
		}
		state.Authorization = unbound
		state.Unbuilt.InstrumentBinding = true // the executor says it verified

		if Evaluate(state).Permitted {
			t.Fatal("an authorization that never named an instrument was permitted to collect " +
				"on an executor's word that it verified one")
		}
	})

	t.Run("bound, and the executor has not verified", func(t *testing.T) {
		state := permittedState(t)
		state.Unbuilt.InstrumentBinding = false
		if Evaluate(state).Permitted {
			t.Fatal("collection was permitted without a verified instrument binding")
		}
	})
}

// 🔴 The notice wait was whatever the caller said it was.
//
// ClauseNoticeWaitElapsed compared `now` against Notice.EligibilityNotBefore —
// a timestamp supplied by whoever built the state. Nothing checked it against
// anything the customer agreed to, so a caller could set eligibility to the
// delivery instant and the wait elapsed immediately: the customer told as
// their card was charged, and the clause reporting a satisfied notice period.
//
// The authorization carries the accepted lead time, so the receipt is now
// checked against it.
func TestTheNoticeWaitIsMeasuredAgainstTheAcceptedLeadTime(t *testing.T) {
	// The authorization in permittedState accepts a 24h lead time.
	t.Run("eligibility earlier than delivery plus the lead time", func(t *testing.T) {
		state := permittedState(t)
		state.Notice.DeliveredAt = evalNow.Add(-2 * time.Hour)
		// A caller claiming the wait is already over, two hours after
		// delivery, under a 24h accepted lead time.
		state.Notice.EligibilityNotBefore = evalNow.Add(-time.Minute)

		if Evaluate(state).Permitted {
			t.Fatal("a two-hour wait satisfied a 24-hour accepted lead time — the customer " +
				"would be told as their card was charged")
		}
	})

	t.Run("no delivery instant at all", func(t *testing.T) {
		state := permittedState(t)
		state.Notice.DeliveredAt = time.Time{}
		if Evaluate(state).Permitted {
			t.Fatal("a wait measured from nothing was treated as elapsed")
		}
	})

	t.Run("delivery plus the full lead time is enough", func(t *testing.T) {
		state := permittedState(t)
		state.Notice.DeliveredAt = evalNow.Add(-25 * time.Hour)
		state.Notice.EligibilityNotBefore = evalNow.Add(-time.Hour)
		if !Evaluate(state).Permitted {
			t.Fatalf("a receipt honouring the full 24h lead time was refused: %v",
				Evaluate(state).Refused)
		}
	})
}
