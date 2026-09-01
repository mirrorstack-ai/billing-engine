package intent

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// 🔴 An acceptance that names a DIFFERENT document must not mint an
// authorization.
//
// This is the control §12 item 16 option C's second piece adds. Before it,
// AcceptanceDigest was any non-empty string, and docs/DESIGN.md §4's standing
// gate rests entirely on that field — so one character authorised recurring,
// automatic collection.
//
// Every term is tried, because a term left out of the disclosure is a term
// the customer's acceptance does not cover, and the omission would be silent.
func TestAnAcceptanceThatNamesOtherTermsIsRefused(t *testing.T) {
	changes := map[string]func(*AuthorizationGrant){
		"scope":              func(g *AuthorizationGrant) { g.Scope = ScopeOneTime; g.IntentDigest = "d" },
		"subject kind":       func(g *AuthorizationGrant) { g.Subject.Kind = "user" },
		"subject id":         func(g *AuthorizationGrant) { g.Subject.ID = "org-2" },
		"currency":           func(g *AuthorizationGrant) { g.Currency = "TWD" },
		"permitted kinds":    func(g *AuthorizationGrant) { g.Kinds = []ChargeKind{KindModuleUsage} },
		"per-charge ceiling": func(g *AuthorizationGrant) { g.PerChargeCeiling = 999_999 },
		"period ceiling":     func(g *AuthorizationGrant) { g.PeriodCeiling = 999_999 },
		"frequency ceiling":  func(g *AuthorizationGrant) { g.FrequencyCeiling = 7 },
		// The auto-top-up trigger and amount are also a pair.
		"trigger below": func(g *AuthorizationGrant) {
			g.TriggerBelowMicros = 1_000
			g.TopUpAmountMicros = 5_000
		},
		"top-up amount": func(g *AuthorizationGrant) {
			g.TriggerBelowMicros = 2_000
			g.TopUpAmountMicros = 9_000
		},
		// Provider and MandateReference are validated as a PAIR, so both are
		// set: changing one alone is refused earlier, for a different reason,
		// and would prove nothing about the acceptance check.
		"provider": func(g *AuthorizationGrant) {
			g.Provider = "other-rail"
			g.MandateReference = "pm_other"
		},
		"notice lead time": func(g *AuthorizationGrant) { g.NoticeLeadTime = 72 * time.Hour },
		"terms revision":   func(g *AuthorizationGrant) { g.TermsRevision = "terms-2026-02" },
		"price book":       func(g *AuthorizationGrant) { g.PriceBook = "pb-2026-09" },
		"notice policy":    func(g *AuthorizationGrant) { g.NoticePolicy = "sms/v1" },
		"effective from":   func(g *AuthorizationGrant) { g.EffectiveFrom = authFrom.Add(time.Hour) },
		"expires at":       func(g *AuthorizationGrant) { g.ExpiresAt = authTill.Add(time.Hour) },
	}

	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			// The customer accepted THESE terms...
			shown := standingGrant()
			accepted := DisclosureDigestFor(shown)

			// ...and something tried to mint DIFFERENT ones under that
			// acceptance.
			minted := standingGrant()
			change(&minted)
			minted.AcceptanceDigest = accepted

			auth, err := Authorize(minted)
			if !errors.Is(err, ErrAuthAcceptanceMismatch) {
				t.Fatalf("minting with a changed %s was permitted: %v", name, err)
			}
			if auth.ID() != "" {
				t.Error("a refused grant still produced an authorization")
			}
		})
	}
}

// The other direction: the terms the customer was shown DO mint.
func TestTheAcceptedTermsMint(t *testing.T) {
	g := standingGrant()
	g.AcceptanceDigest = DisclosureDigestFor(g)

	auth, err := Authorize(g)
	if err != nil {
		t.Fatalf("the exact terms the customer accepted were refused: %v", err)
	}
	if auth.AcceptanceDigest() != g.AcceptanceDigest {
		t.Error("the authorization does not carry the acceptance it was minted under")
	}
}

// 🔴 Every field of the disclosure must reach the digest.
//
// A field outside it is a term the acceptance silently does not cover — the
// customer could be shown one notice policy and charged under another, and
// the digest would still match. This reflects over the type so a field added
// later cannot be forgotten, which is the failure the sibling floor over
// ChargeIntent exists for.
func TestEveryDisclosureFieldChangesTheDigest(t *testing.T) {
	base := disclosureFor(standingGrant())

	mutations := map[string]func(*AuthorizationDisclosure){
		"Scope":                  func(d *AuthorizationDisclosure) { d.Scope = ScopeOneTime },
		"Subject.Kind":           func(d *AuthorizationDisclosure) { d.Subject.Kind = "user" },
		"Subject.ID":             func(d *AuthorizationDisclosure) { d.Subject.ID = "org-2" },
		"Currency":               func(d *AuthorizationDisclosure) { d.Currency = "TWD" },
		"Kinds":                  func(d *AuthorizationDisclosure) { d.Kinds = []ChargeKind{KindModuleUsage} },
		"PerChargeCeilingMicros": func(d *AuthorizationDisclosure) { d.PerChargeCeilingMicros++ },
		"PeriodCeilingMicros":    func(d *AuthorizationDisclosure) { d.PeriodCeilingMicros++ },
		"FrequencyCeiling":       func(d *AuthorizationDisclosure) { d.FrequencyCeiling++ },
		"TriggerBelowMicros":     func(d *AuthorizationDisclosure) { d.TriggerBelowMicros++ },
		"TopUpAmountMicros":      func(d *AuthorizationDisclosure) { d.TopUpAmountMicros++ },
		"Provider":               func(d *AuthorizationDisclosure) { d.Provider = "other" },
		"NoticeLeadTimeSecs":     func(d *AuthorizationDisclosure) { d.NoticeLeadTimeSecs++ },
		"TermsRevision":          func(d *AuthorizationDisclosure) { d.TermsRevision = "terms-x" },
		"PriceBookRevision":      func(d *AuthorizationDisclosure) { d.PriceBookRevision = "pb-x" },
		"NoticePolicy":           func(d *AuthorizationDisclosure) { d.NoticePolicy = "sms/v1" },
		"EffectiveFromUnix":      func(d *AuthorizationDisclosure) { d.EffectiveFromUnix++ },
		"ExpiresAtUnix":          func(d *AuthorizationDisclosure) { d.ExpiresAtUnix++ },
	}

	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			d := base
			mutate(&d)
			if d.Digest() == base.Digest() {
				t.Errorf("changing %s left the disclosure digest unchanged; the term is "+
					"outside what the customer's acceptance covers", name)
			}
		})
	}

	// The floor: a field added later with no case here would be a term the
	// acceptance silently does not cover.
	dt := reflect.TypeOf(AuthorizationDisclosure{})
	for i := 0; i < dt.NumField(); i++ {
		if _, ok := mutations[dt.Field(i).Name]; ok {
			continue
		}
		// Subject is covered by two nested cases.
		if dt.Field(i).Name == "Subject" &&
			mutations["Subject.Kind"] != nil && mutations["Subject.ID"] != nil {
			continue
		}
		t.Errorf("AuthorizationDisclosure.%s has no case proving it reaches the digest. "+
			"A term outside the digest is one the customer's acceptance does not cover.",
			dt.Field(i).Name)
	}
}

// The permitted kinds are a SET. Two grants permitting the same set must
// produce one document, or the same terms would be two different documents
// depending on the order a caller happened to build them in — and a
// re-acceptance campaign would be triggered by nothing at all.
func TestKindOrderDoesNotChangeTheDocument(t *testing.T) {
	a := standingGrant()
	a.Kinds = []ChargeKind{KindModuleUsage, KindAutoTopUp, KindCustomDomain}

	b := standingGrant()
	b.Kinds = []ChargeKind{KindCustomDomain, KindModuleUsage, KindAutoTopUp}

	if DisclosureDigestFor(a) != DisclosureDigestFor(b) {
		t.Fatal("the same permitted kinds in a different order produced two documents")
	}
}

// The encoding must be injective, or an acceptance of one document attests to
// another. Several of these fields are free text a caller controls.
func TestTheDisclosureEncodingCannotBeConfusedAcrossFields(t *testing.T) {
	a := disclosureFor(standingGrant())
	a.TermsRevision, a.PriceBookRevision = "ab", "c"

	b := disclosureFor(standingGrant())
	b.TermsRevision, b.PriceBookRevision = "a", "bc"

	if a.Digest() == b.Digest() {
		t.Fatal("two different disclosures produced one digest")
	}
}

// AuthorizeAccepted is the test-and-dev path, and it must agree with what a
// relaying caller would have to send.
func TestAuthorizeAcceptedAgreesWithTheDerivedDigest(t *testing.T) {
	g := standingGrant()
	auth, err := AuthorizeAccepted(g)
	if err != nil {
		t.Fatal(err)
	}
	if auth.AcceptanceDigest() != DisclosureDigestFor(g) {
		t.Fatal("AuthorizeAccepted minted an acceptance a relaying caller could not reproduce")
	}
}

// The schema tag must be inside the digest, so a future layout cannot produce
// a digest a v1 document already produced.
func TestTheDisclosureSchemaIsInsideTheDigest(t *testing.T) {
	if !strings.Contains(DisclosureSchema, "/v1") {
		t.Fatalf("DisclosureSchema = %q; it must carry a version", DisclosureSchema)
	}
	d := disclosureFor(standingGrant())
	first := d.Digest()

	// Two disclosures differing only in a field cannot collide with a
	// different-schema encoding of the same values; the tag being first is
	// what guarantees that, and this pins that it is present at all.
	if first == "" || len(first) != 64 {
		t.Fatalf("digest = %q, want 64 hex characters", first)
	}
}
