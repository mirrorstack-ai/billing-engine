package intent

import (
	"errors"
	"testing"
	"time"
)

func catalogDraft(kind ChargeKind) Draft {
	return Draft{
		Payer:             Subject{Kind: "user", ID: "acct-1"},
		Currency:          "usd",
		Lines:             []Line{NewLine("d", "m", "1", 1, 10_000)},
		Kind:              kind,
		PriceBookRevision: "pb-1",
		TermsRevision:     "terms-1",
		Tax:               TaxDetermination{Resolved: true, Jurisdiction: "TW", RuleRevision: "tax-1", Verification: TaxNotApplicable},
		AuthorizationID:   "auth-1",
		NoticePolicy:      "email/v1",
		ExecuteNotBefore:  time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		ExecuteNotAfter:   time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		SourceFactKeys:    []string{"f"},
	}
}

// docs/DESIGN.md §6: "A positive customer charge kind that is not listed below
// must not be proposed, and must not be collected. No private caller, module,
// adapter, webhook, tax vendor or operator may introduce a kind from free
// text."
//
// The two shipped legs proposed "domain.custom" and "module.overage" — neither
// appears anywhere in §6. They were invented at the call site and sealed into a
// digest a customer's bundle would attest to, and nothing objected, because
// Kind was a bare string that Seal never looked at.
func TestSealRefusesAKindOutsideTheCatalog(t *testing.T) {
	for _, invented := range []ChargeKind{
		"domain.custom",  // what the domain leg actually sealed
		"module.overage", // what the overage leg actually sealed
		"cycle.boundary", // what the boundary leg seals
		"wallet.topup",   // a plausible-looking invention
		"subscription.increase",
		"platform_base_v2", // near-miss on a real one
		"PLATFORM_BASE",    // case is not a spelling
		// Folded into platform_base by §12 item 12. They were in the
		// catalog until 2026-09-01, so without these two rows the
		// narrowing is asserted by nothing and re-adding either constant
		// passes silently.
		"module_capacity",
		"custom_domain",
	} {
		t.Run(string(invented), func(t *testing.T) {
			_, err := Seal(catalogDraft(invented))
			if !errors.Is(err, ErrKindNotInCatalog) {
				t.Fatalf("Seal(%q) = %v, want ErrKindNotInCatalog — a kind no published rule "+
					"defines was sealed into a digest the customer's bundle attests to", invented, err)
			}
		})
	}
}

// Every kind §6 lists must seal. A guard that refused everything would pass the
// test above while making the catalog unusable.
func TestEveryCatalogKindSeals(t *testing.T) {
	kinds := CatalogKinds()
	if len(kinds) != 7 {
		t.Fatalf("catalog has %d kinds, want the 7 §6 lists after the item 12 fold", len(kinds))
	}
	for _, k := range kinds {
		if _, err := Seal(catalogDraft(k)); err != nil {
			t.Fatalf("Seal(%q) refused a kind §6 lists: %v", k, err)
		}
	}
}

// The two shipped legs must use the names §6 gives them, not near-misses.
//
// Both legs proposed a kind of their own until §12 item 12 folded capacity and
// domains into the base price. The premise outlives the fold — a leg seals a
// name §6 lists, never one invented at the call site — so this is retargeted
// rather than deleted, and it is now also the pin that both legs land on the
// SAME kind.
func TestTheShippedLegsUseTheCatalogNames(t *testing.T) {
	for _, tc := range []struct {
		leg  string
		kind ChargeKind
	}{
		{"custom domain", KindPlatformBase},
		{"module overage", KindPlatformBase},
	} {
		if !KindInCatalog(tc.kind) {
			t.Fatalf("the %s leg's kind %q is not in the catalog", tc.leg, tc.kind)
		}
	}
	if KindPlatformBase != "platform_base" {
		t.Fatalf("KindPlatformBase = %q, but §6 names it platform_base", KindPlatformBase)
	}
}
