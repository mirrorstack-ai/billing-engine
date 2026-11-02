package usage_test

import (
	"math"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// TestContractPriceBook_AppPlansMatchCode pins the three app-plan rows of the
// 2027-01-01 price lock fixture (billing-engine#269, core-v2#1843, Annex 1
// art 3) against usage.TermsFor — the one place an app plan's price actually
// lives. Raising or lowering any of these by a single micro, in the fixture
// OR in plans.go, turns this red: the two must always agree, because the
// fixture is the thing the owner reviews before a merge that touches price.
func TestContractPriceBook_AppPlansMatchCode(t *testing.T) {
	book := loadContractPriceBook(t)

	for _, id := range []string{"app-plan-free", "app-plan-pro", "app-plan-business"} {
		row := mustRow(t, book.Entries, id)
		if row.Source != "engine" {
			t.Errorf("%s: source = %q, want \"engine\"", id, row.Source)
		}
		if row.Allowances == nil {
			t.Fatalf("%s: no allowances", id)
		}
		plan, ok := usage.ParsePlan(row.Plan)
		if !ok {
			t.Fatalf("%s: plan %q does not parse", id, row.Plan)
		}
		terms := usage.TermsFor(plan)

		if got := terms.BaseFeeMicros; got != row.ChargedMicrosPerUnit {
			t.Errorf("%s: TermsFor(%s).BaseFeeMicros = %d, fixture charged_micros_per_unit = %d", id, plan, got, row.ChargedMicrosPerUnit)
		}
		if terms.ModulesIncluded != row.Allowances.ModulesIncluded {
			t.Errorf("%s: ModulesIncluded = %d, fixture = %d", id, terms.ModulesIncluded, row.Allowances.ModulesIncluded)
		}
		if terms.DomainsIncluded != row.Allowances.DomainsIncluded {
			t.Errorf("%s: DomainsIncluded = %d, fixture = %d", id, terms.DomainsIncluded, row.Allowances.DomainsIncluded)
		}
		if terms.MembersIncluded != row.Allowances.MembersIncluded {
			t.Errorf("%s: MembersIncluded = %d, fixture = %d", id, terms.MembersIncluded, row.Allowances.MembersIncluded)
		}
		if terms.UsageAllowanceMicros != row.Allowances.UsageAllowanceMicros {
			t.Errorf("%s: UsageAllowanceMicros = %d, fixture = %d", id, terms.UsageAllowanceMicros, row.Allowances.UsageAllowanceMicros)
		}
		if terms.MaxAppsPerOrg != row.Allowances.MaxAppsPerOrg {
			t.Errorf("%s: MaxAppsPerOrg = %d, fixture = %d", id, terms.MaxAppsPerOrg, row.Allowances.MaxAppsPerOrg)
		}
		if row.Free && row.ChargedMicrosPerUnit != 0 {
			t.Errorf("%s: free = true but charged_micros_per_unit = %d", id, row.ChargedMicrosPerUnit)
		}
	}
}

// TestContractPriceBook_AddOnsMatchCode pins the three per-unit add-ons
// (extra module block, extra custom domain, extra member) against the bill.go
// constants the contract's "Add-ons" line locks.
func TestContractPriceBook_AddOnsMatchCode(t *testing.T) {
	book := loadContractPriceBook(t)

	modules := mustRow(t, book.Entries, "addon-extra-modules")
	if modules.ChargedMicrosPerUnit != usage.ModuleBlockFeeMicros {
		t.Errorf("addon-extra-modules: charged_micros_per_unit = %d, usage.ModuleBlockFeeMicros = %d", modules.ChargedMicrosPerUnit, usage.ModuleBlockFeeMicros)
	}
	if modules.BlockSize != usage.ModuleBlockSize {
		t.Errorf("addon-extra-modules: block_size = %d, usage.ModuleBlockSize = %d", modules.BlockSize, usage.ModuleBlockSize)
	}

	domain := mustRow(t, book.Entries, "addon-extra-domain")
	if domain.ChargedMicrosPerUnit != usage.DomainFeeMicros {
		t.Errorf("addon-extra-domain: charged_micros_per_unit = %d, usage.DomainFeeMicros = %d", domain.ChargedMicrosPerUnit, usage.DomainFeeMicros)
	}

	member := mustRow(t, book.Entries, "addon-extra-member")
	if member.ChargedMicrosPerUnit != usage.ExtraMemberFeeMicros {
		t.Errorf("addon-extra-member: charged_micros_per_unit = %d, usage.ExtraMemberFeeMicros = %d", member.ChargedMicrosPerUnit, usage.ExtraMemberFeeMicros)
	}

	for _, row := range []contractPriceRow{modules, domain, member} {
		if row.Source != "engine" {
			t.Errorf("%s: source = %q, want \"engine\"", row.ID, row.Source)
		}
		if row.CostLinked {
			t.Errorf("%s: cost_linked = true; art 3's exceptions are the storage rows and the deploy rows, not app-plan add-ons", row.ID)
		}
	}
}

// TestContractPriceBook_NoFreeRowCarriesAPrice is the general guard behind
// art 3 para 4 ("free plans and free features stay free for the whole
// term"): every row marked free, anywhere in the fixture, must price at
// exactly zero. This runs over all of entries and pending_rows, not just the
// app plans, so a free row added anywhere later is still caught.
func TestContractPriceBook_NoFreeRowCarriesAPrice(t *testing.T) {
	book := loadContractPriceBook(t)

	check := func(rows []contractPriceRow) {
		for _, row := range rows {
			if !row.Free {
				continue
			}
			if row.ContractPriceUSD != 0 {
				t.Errorf("%s: free = true but contract_price_usd = %v", row.ID, row.ContractPriceUSD)
			}
			if row.ChargedMicrosPerUnit != 0 {
				t.Errorf("%s: free = true but charged_micros_per_unit = %d", row.ID, row.ChargedMicrosPerUnit)
			}
		}
	}
	check(book.Entries)
	check(book.PendingRows)
}

// TestContractPriceBook_RowsAgreeWithThemselves is a pure-arithmetic,
// DB-free consistency check over every priced row: charged_micros_per_unit
// must equal round(contract_price_usd * 1e6) UNLESS the row is named in
// known_diff, in which case it must equal exactly the known_diff's pinned
// engine_stored_micros. This is what keeps the fixture's own numbers honest
// independent of whether the thing they describe is checkable in this repo.
func TestContractPriceBook_RowsAgreeWithThemselves(t *testing.T) {
	book := loadContractPriceBook(t)

	diffs := make(map[string]contractKnownDiff, len(book.KnownDiff))
	for _, d := range book.KnownDiff {
		diffs[d.ID] = d
	}

	for _, row := range book.Entries {
		wantExact := int64(math.Round(row.ContractPriceUSD * 1_000_000))
		if row.KnownDiffRef == "" {
			if row.ChargedMicrosPerUnit != wantExact {
				t.Errorf("%s: charged_micros_per_unit = %d, contract_price_usd %.6f implies %d, and no known_diff explains the gap",
					row.ID, row.ChargedMicrosPerUnit, row.ContractPriceUSD, wantExact)
			}
			continue
		}
		d, ok := diffs[row.KnownDiffRef]
		if !ok {
			t.Errorf("%s: known_diff_ref %q has no entry in known_diff", row.ID, row.KnownDiffRef)
			continue
		}
		if d.ContractMicros != wantExact {
			t.Errorf("%s: known_diff contract_micros = %d, contract_price_usd %.6f implies %d", row.ID, d.ContractMicros, row.ContractPriceUSD, wantExact)
		}
		if d.EngineStoredMicros != row.ChargedMicrosPerUnit {
			t.Errorf("%s: known_diff engine_stored_micros = %d, row charged_micros_per_unit = %d", row.ID, d.EngineStoredMicros, row.ChargedMicrosPerUnit)
		}
		if d.ContractMicros == row.ChargedMicrosPerUnit {
			t.Errorf("%s: known_diff_ref is set but contract and engine values are identical (%d) — drop the known_diff", row.ID, d.ContractMicros)
		}
	}
}

// contractKnownDiffIDs and contractPendingRowIDs are the pinned, shrink-only
// sets billing-engine#269 requires: a fixture edit that adds a new entry to
// either list, without a matching edit here, is the change these two tests
// exist to catch.
var contractKnownDiffIDs = []string{
	"deploy-dynamic-outbound",
	"storage-video",
	"storage-ad-asset",
	"transcode-h265-standard",
	"transcode-h265-high",
}

var contractPendingRowIDs = []string{
	"org-plan-base-seat",
	"org-plan-pro-seat",
	"org-plan-enterprise-seat",
	"org-ai-credit-base",
	"org-ai-credit-pro",
	"org-ai-credit-enterprise",
	"platform-fee-monthly",
	"platform-fee-yearly",
}

// TestContractPriceBook_KnownDiffIsPinned: known_diff may only SHRINK (a
// price-rounding gap gets fixed, not grown). A new unexplained gap, or a
// known_diff id removed from this list without the corresponding fixture row
// disappearing too, is a deliberate edit this test forces.
func TestContractPriceBook_KnownDiffIsPinned(t *testing.T) {
	book := loadContractPriceBook(t)

	got := make(map[string]bool, len(book.KnownDiff))
	for _, d := range book.KnownDiff {
		got[d.ID] = true
	}
	want := make(map[string]bool, len(contractKnownDiffIDs))
	for _, id := range contractKnownDiffIDs {
		want[id] = true
	}
	for id := range got {
		if !want[id] {
			t.Errorf("known_diff has %q, which is not in the pinned set — shrinking known_diff is fine, growing it needs a deliberate edit to contractKnownDiffIDs", id)
		}
	}
	for id := range want {
		if !got[id] {
			t.Errorf("known_diff is missing the pinned %q", id)
		}
	}
}

// TestContractPriceBook_PendingRowsArePinned: billing-engine#228 and #258
// move rows out of pending_rows as they land; this test is what makes that
// move a visible diff instead of a quiet one, and what stops a new unpriced
// row from hiding in pending_rows indefinitely.
func TestContractPriceBook_PendingRowsArePinned(t *testing.T) {
	book := loadContractPriceBook(t)

	got := make(map[string]bool, len(book.PendingRows))
	for _, r := range book.PendingRows {
		got[r.ID] = true
		if r.Source != "pending" {
			t.Errorf("pending_rows: %s has source %q, want \"pending\"", r.ID, r.Source)
		}
	}
	want := make(map[string]bool, len(contractPendingRowIDs))
	for _, id := range contractPendingRowIDs {
		want[id] = true
	}
	for id := range got {
		if !want[id] {
			t.Errorf("pending_rows has %q, which is not in the pinned set", id)
		}
	}
	for id := range want {
		if !got[id] {
			t.Errorf("pending_rows is missing the pinned %q", id)
		}
	}
}

// TestContractPriceBook_PricedOutsideTableIsByReference: the AI rows
// (billing-engine#253, core-v2#1816) are cited, never re-priced, here.
func TestContractPriceBook_PricedOutsideTableIsByReference(t *testing.T) {
	book := loadContractPriceBook(t)

	if len(book.PricedOutsideTable) == 0 {
		t.Fatalf("priced_outside_table is empty; billing-engine#253 and core-v2#1816 must be cited by reference")
	}
	for _, ref := range book.PricedOutsideTable {
		if ref.Reference == "" {
			t.Errorf("priced_outside_table: %s has no reference", ref.ID)
		}
	}
}

// TestContractPriceBook_CostLinkedRowsWithoutAChangeAreUnmodified: every
// cost-linked row in the fixture today carries no change record, which is
// only valid if its charged_micros_per_unit still equals the 2027-01-01
// baseline captured by known_diff (or, lacking one, by contract_price_usd
// directly) — i.e. nothing has moved it yet. A cost-linked row that picks up
// a price edit must also pick up a change record, or this fixture-internal
// check plus TestContractPriceBook_RowsAgreeWithThemselves both go red.
func TestContractPriceBook_CostLinkedRowsWithoutAChangeAreUnmodified(t *testing.T) {
	book := loadContractPriceBook(t)

	for _, row := range book.Entries {
		if !row.CostLinked {
			continue
		}
		if row.Change == nil {
			continue
		}
		if err := validateCostLinkedChange(row); err != nil {
			t.Errorf("%v", err)
		}
	}
}

// TestValidateCostLinkedChange_RejectsBadEvidence exercises the validator
// directly against synthetic rows so the guard's behaviour is pinned even
// though the committed fixture carries no change records yet (nothing has
// re-priced since 2027-01-01). This is the test that goes red if someone
// edits a cost-linked row's price without the required 45-day notice, the
// 20% cap, or the decrease pass-through window — exactly the three cases
// billing-engine#269's acceptance criteria asks to demonstrate in CI.
func TestValidateCostLinkedChange_RejectsBadEvidence(t *testing.T) {
	base := contractPriceRow{ID: "synthetic-cost-linked-row", CostLinked: true}

	cases := []struct {
		name    string
		change  *contractCostLinkedChange
		wantErr bool
	}{
		{
			name: "valid increase at exactly 45 days and under the cap",
			change: &contractCostLinkedChange{
				Direction: "increase", EffectiveFrom: "2027-06-01", NoticeSentOn: "2027-04-17",
				SupplierEvidence: "https://aws.amazon.com/pricing-notice/2027-04-10", CumulativeRisePct: 8,
			},
			wantErr: false,
		},
		{
			name: "increase with only 44 days notice",
			change: &contractCostLinkedChange{
				Direction: "increase", EffectiveFrom: "2027-06-01", NoticeSentOn: "2027-04-18",
				SupplierEvidence: "https://aws.amazon.com/pricing-notice/2027-04-10", CumulativeRisePct: 8,
			},
			wantErr: true,
		},
		{
			name: "increase over the 20% cumulative cap",
			change: &contractCostLinkedChange{
				Direction: "increase", EffectiveFrom: "2027-06-01", NoticeSentOn: "2027-04-17",
				SupplierEvidence: "https://aws.amazon.com/pricing-notice/2027-04-10", CumulativeRisePct: 20.01,
			},
			wantErr: true,
		},
		{
			name: "increase with no supplier evidence",
			change: &contractCostLinkedChange{
				Direction: "increase", EffectiveFrom: "2027-06-01", NoticeSentOn: "2027-04-17",
				CumulativeRisePct: 8,
			},
			wantErr: true,
		},
		{
			name: "valid decrease passed through in 45 days",
			change: &contractCostLinkedChange{
				Direction: "decrease", EffectiveFrom: "2027-07-15", SupplierChangeDate: "2027-06-01",
				SupplierEvidence: "https://www.cloudflare.com/plans/pricing-notice/2027-06-01",
			},
			wantErr: false,
		},
		{
			name: "decrease passed through after 46 days",
			change: &contractCostLinkedChange{
				Direction: "decrease", EffectiveFrom: "2027-07-17", SupplierChangeDate: "2027-06-01",
				SupplierEvidence: "https://www.cloudflare.com/plans/pricing-notice/2027-06-01",
			},
			wantErr: true,
		},
		{
			name: "decrease effective before the supplier's own change",
			change: &contractCostLinkedChange{
				Direction: "decrease", EffectiveFrom: "2027-05-01", SupplierChangeDate: "2027-06-01",
				SupplierEvidence: "https://www.cloudflare.com/plans/pricing-notice/2027-06-01",
			},
			wantErr: true,
		},
		{
			name: "unknown direction",
			change: &contractCostLinkedChange{
				Direction: "sideways", EffectiveFrom: "2027-06-01",
				SupplierEvidence: "https://aws.amazon.com/pricing-notice/2027-04-10",
			},
			wantErr: true,
		},
		{
			name:    "cost-linked row with no change record at all",
			change:  nil,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			row := base
			row.Change = tc.change
			err := validateCostLinkedChange(row)
			if tc.wantErr && err == nil {
				t.Errorf("validateCostLinkedChange(%+v) = nil, want an error", tc.change)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("validateCostLinkedChange(%+v) = %v, want nil", tc.change, err)
			}
		})
	}
}

// TestValidateCostLinkedChange_RejectsANonCostLinkedRow: art 3's exceptions
// are named rows (storage + deploy); a row that is not cost_linked may never
// carry a change record at all — that would be a price rise outside the
// exceptions, which the contract forbids outright for the lock's 12 months.
func TestValidateCostLinkedChange_RejectsANonCostLinkedRow(t *testing.T) {
	row := contractPriceRow{
		ID:         "synthetic-locked-row",
		CostLinked: false,
		Change: &contractCostLinkedChange{
			Direction: "increase", EffectiveFrom: "2027-06-01", NoticeSentOn: "2027-04-17",
			SupplierEvidence: "https://example.com/notice", CumulativeRisePct: 5,
		},
	}
	if err := validateCostLinkedChange(row); err == nil {
		t.Errorf("validateCostLinkedChange on a non-cost-linked row with a change = nil, want an error")
	}
}
