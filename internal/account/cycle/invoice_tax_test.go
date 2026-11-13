package cycle

import (
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// setInvoiceTax repeats a sealed determination onto the mirror (migration
// 088) and never makes one up: nil stays NULL (unknown), and anything a Seal
// would refuse — or a figure the whole-cent column cannot hold — is refused.
func TestSetInvoiceTax(t *testing.T) {
	var p db.UpsertInvoiceParams
	if err := setInvoiceTax(&p, nil); err != nil {
		t.Fatalf("nil determination: %v", err)
	}
	if p.TaxAmount.Valid || p.TaxJurisdiction.Valid || p.TaxRuleRevision.Valid || p.TaxVerification.Valid {
		t.Fatalf("nil determination wrote tax columns: %+v", p)
	}

	// What every proposer in this tree seals today: a determined zero.
	na := intent.TaxDetermination{
		Resolved:     true,
		Jurisdiction: "not-applicable",
		RuleRevision: proposedTaxRuleRevision,
		Verification: intent.TaxNotApplicable,
	}
	if err := setInvoiceTax(&p, &na); err != nil {
		t.Fatalf("not_applicable: %v", err)
	}
	if !p.TaxAmount.Valid || p.TaxAmount.Int.Sign() != 0 {
		t.Fatalf("not_applicable must record an itemized ZERO, got %+v", p.TaxAmount)
	}
	if p.TaxJurisdiction.String != "not-applicable" || p.TaxRuleRevision.String != proposedTaxRuleRevision ||
		p.TaxVerification.String != "not_applicable" {
		t.Fatalf("determination not copied verbatim: %+v", p)
	}

	five := intent.TaxDetermination{Resolved: true, Jurisdiction: "TW", RuleRevision: "r1",
		AmountMicros: 1_250_000, Verification: intent.TaxIndependentlyReproducible}
	if err := setInvoiceTax(&p, &five); err != nil {
		t.Fatalf("whole cents: %v", err)
	}
	if got := p.TaxAmount.Int.Int64(); got != 125 || p.TaxAmount.Exp != 0 {
		t.Fatalf("tax_amount = %d e%d; want 125 cents", got, p.TaxAmount.Exp)
	}

	for name, bad := range map[string]intent.TaxDetermination{
		"unresolved":     {Jurisdiction: "TW", Verification: intent.TaxNotApplicable},
		"unverified":     {Resolved: true, Jurisdiction: "TW"},
		"sub-cent":       {Resolved: true, AmountMicros: 1, Verification: intent.TaxProviderAttested},
		"negative cents": {Resolved: true, AmountMicros: -10_000, Verification: intent.TaxProviderAttested},
	} {
		if err := setInvoiceTax(&db.UpsertInvoiceParams{}, &bad); err == nil {
			t.Errorf("%s: recorded a determination no intent could have sealed", name)
		}
	}
}
