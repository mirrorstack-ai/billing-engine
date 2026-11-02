package usage_test

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

// contractPriceBookPath is the pinned 2027-01-01 price lock fixture
// (billing-engine#269, core-v2#1843). TestContractPriceBook_FixtureLoads and
// every other TestContractPriceBook_* test in this package read it through
// loadContractPriceBook so a missing or malformed file fails every one of
// them rather than skipping.
const contractPriceBookPath = "testdata/contract_price_book_2027.json"

// contractPriceBook is the shape of contractPriceBookPath. It is read here
// (not exported from package usage) because nothing in the engine consumes
// it at runtime — it exists only to make a price change a visible test diff.
type contractPriceBook struct {
	Contract           contractMeta         `json:"contract"`
	Entries            []contractPriceRow   `json:"entries"`
	PendingRows        []contractPriceRow   `json:"pending_rows"`
	KnownDiff          []contractKnownDiff  `json:"known_diff"`
	PricedOutsideTable []contractOutsideRef `json:"priced_outside_table"`
}

type contractMeta struct {
	Name                         string  `json:"name"`
	EffectiveFrom                string  `json:"effective_from"`
	TermMonths                   int     `json:"term_months"`
	CostLinkedCumulativeCapPctYr float64 `json:"cost_linked_cumulative_cap_pct_per_year"`
	NoticeDays                   int     `json:"notice_days"`
}

type contractAllowances struct {
	ModulesIncluded      int     `json:"modules_included"`
	DomainsIncluded      int     `json:"domains_included"`
	MembersIncluded      int     `json:"members_included"`
	UsageAllowanceUSD    float64 `json:"usage_allowance_usd"`
	UsageAllowanceMicros int64   `json:"usage_allowance_micros"`
	MaxAppsPerOrg        int     `json:"max_apps_per_org"`
}

// contractCostLinkedChange is the evidence a cost-linked row must carry
// before its contract_price_usd / charged_micros_per_unit may move (art 3
// para 2/3). Direction is "increase" or "decrease" — the two have different
// timing rules (a 45-day ADVANCE notice for a rise, a 45-day MAXIMUM
// pass-through delay for a fall), so one struct carries both shapes and the
// validator switches on Direction.
type contractCostLinkedChange struct {
	Direction          string  `json:"direction"`
	EffectiveFrom      string  `json:"effective_from"`
	NoticeSentOn       string  `json:"notice_sent_on,omitempty"`
	SupplierChangeDate string  `json:"supplier_change_date,omitempty"`
	SupplierEvidence   string  `json:"supplier_evidence"`
	CumulativeRisePct  float64 `json:"cumulative_rise_pct,omitempty"`
}

type contractPriceRow struct {
	ID                   string                    `json:"id"`
	Article              string                    `json:"article"`
	Unit                 string                    `json:"unit"`
	ContractPriceUSD     float64                   `json:"contract_price_usd"`
	ChargedMicrosPerUnit int64                     `json:"charged_micros_per_unit"`
	CostLinked           bool                      `json:"cost_linked"`
	Free                 bool                      `json:"free"`
	Source               string                    `json:"source"`
	Plan                 string                    `json:"plan,omitempty"`
	Allowances           *contractAllowances       `json:"allowances,omitempty"`
	BlockSize            int                       `json:"block_size,omitempty"`
	Module               string                    `json:"module,omitempty"`
	Metric               string                    `json:"metric,omitempty"`
	DBUnit               string                    `json:"db_unit,omitempty"`
	ScaleFromDBUnit      int64                     `json:"scale_from_db_unit,omitempty"`
	KnownDiffRef         string                    `json:"known_diff_ref,omitempty"`
	Change               *contractCostLinkedChange `json:"change,omitempty"`
	Note                 string                    `json:"note,omitempty"`
	BlockedBy            []string                  `json:"blocked_by,omitempty"`
}

type contractKnownDiff struct {
	ID                 string  `json:"id"`
	ContractMicros     int64   `json:"contract_micros"`
	EngineExactMicros  float64 `json:"engine_exact_micros"`
	EngineStoredMicros int64   `json:"engine_stored_micros"`
	Reason             string  `json:"reason"`
}

type contractOutsideRef struct {
	ID        string `json:"id"`
	Reference string `json:"reference"`
	Note      string `json:"note"`
}

// loadContractPriceBook reads and parses contractPriceBookPath. It fails the
// test (never skips) on a missing file, a read error or invalid JSON: the
// fixture is load-bearing for the price lock, so its absence is a defect, not
// an environment quirk to tolerate.
func loadContractPriceBook(t *testing.T) contractPriceBook {
	t.Helper()
	data, err := os.ReadFile(contractPriceBookPath)
	if err != nil {
		t.Fatalf("reading %s: %v (the 2027-01-01 price lock fixture must exist and be readable)", contractPriceBookPath, err)
	}
	var book contractPriceBook
	if err := json.Unmarshal(data, &book); err != nil {
		t.Fatalf("parsing %s: %v", contractPriceBookPath, err)
	}
	if len(book.Entries) == 0 {
		t.Fatalf("%s has no entries", contractPriceBookPath)
	}
	return book
}

// mustRow returns the fixture row with the given id, failing the test if it
// is absent — a row disappearing from the fixture must fail loudly, not
// silently drop out of whatever loop was checking it.
func mustRow(t *testing.T, rows []contractPriceRow, id string) contractPriceRow {
	t.Helper()
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	t.Fatalf("%s: no row %q", contractPriceBookPath, id)
	return contractPriceRow{}
}

// validateCostLinkedChange enforces art 3 para 2/3 on one row's Change: an
// increase needs a notice at least 45 days ahead of effect and must not push
// the cumulative rise past 20%; a decrease needs the pass-through within 45
// days of the supplier's own change. Returns a descriptive error rather than
// calling t.Fatal so both the fixture-driven loop and standalone synthetic
// tests can assert on the message.
func validateCostLinkedChange(row contractPriceRow) error {
	if !row.CostLinked {
		return fmt.Errorf("%s: carries a change but cost_linked is false", row.ID)
	}
	c := row.Change
	if c == nil {
		return fmt.Errorf("%s: cost-linked row changed price with no change record", row.ID)
	}
	if c.SupplierEvidence == "" {
		return fmt.Errorf("%s: change has no supplier_evidence", row.ID)
	}
	effective, err := time.Parse("2006-01-02", c.EffectiveFrom)
	if err != nil {
		return fmt.Errorf("%s: change.effective_from %q: %v", row.ID, c.EffectiveFrom, err)
	}

	switch c.Direction {
	case "increase":
		if c.NoticeSentOn == "" {
			return fmt.Errorf("%s: increase has no notice_sent_on", row.ID)
		}
		notice, err := time.Parse("2006-01-02", c.NoticeSentOn)
		if err != nil {
			return fmt.Errorf("%s: change.notice_sent_on %q: %v", row.ID, c.NoticeSentOn, err)
		}
		if effective.Sub(notice) < 45*24*time.Hour {
			return fmt.Errorf("%s: notice_sent_on %s is less than 45 days before effective_from %s",
				row.ID, c.NoticeSentOn, c.EffectiveFrom)
		}
		if c.CumulativeRisePct <= 0 {
			return fmt.Errorf("%s: increase has no positive cumulative_rise_pct", row.ID)
		}
		if c.CumulativeRisePct > 20 {
			return fmt.Errorf("%s: cumulative_rise_pct %.4f exceeds the 20%% annual cap", row.ID, c.CumulativeRisePct)
		}
	case "decrease":
		if c.SupplierChangeDate == "" {
			return fmt.Errorf("%s: decrease has no supplier_change_date", row.ID)
		}
		supplierChange, err := time.Parse("2006-01-02", c.SupplierChangeDate)
		if err != nil {
			return fmt.Errorf("%s: change.supplier_change_date %q: %v", row.ID, c.SupplierChangeDate, err)
		}
		if effective.Before(supplierChange) {
			return fmt.Errorf("%s: effective_from %s is before supplier_change_date %s", row.ID, c.EffectiveFrom, c.SupplierChangeDate)
		}
		if effective.Sub(supplierChange) > 45*24*time.Hour {
			return fmt.Errorf("%s: pass-through on %s is more than 45 days after the supplier change on %s",
				row.ID, c.EffectiveFrom, c.SupplierChangeDate)
		}
	default:
		return fmt.Errorf("%s: change.direction %q is neither \"increase\" nor \"decrease\"", row.ID, c.Direction)
	}
	return nil
}
