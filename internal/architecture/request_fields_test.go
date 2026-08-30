package architecture

import (
	"sort"
	"strings"
	"testing"
)

// TestEveryCallerSuppliedMoneyFieldIsAccountedFor is the enumeration
// half of docs/VERIFICATION.md §5's rule that no monetary or authority
// field may sit on a public request struct.
//
// Every such field must carry a verdict: either it is a ceiling, which
// can only reduce what is chargeable and is therefore harmless, or it
// is debt against INV-001 and is counted below.
func TestEveryCallerSuppliedMoneyFieldIsAccountedFor(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	fields, err := ScanRequestFields(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(fields) == 0 {
		t.Fatal("scanned zero request fields; the scanner is broken, not the tree")
	}

	found := map[string]RequestField{}
	for _, f := range fields {
		found[f.Key()] = f
	}

	var unaccounted []string
	for key, f := range found {
		if _, ok := requestFieldVerdicts[key]; !ok {
			unaccounted = append(unaccounted, f.File+":"+itoa(f.Line)+"  "+key+" "+f.GoType)
		}
	}
	sort.Strings(unaccounted)
	if len(unaccounted) > 0 {
		t.Errorf("%d caller-supplied money or authority field(s) have no verdict.\n"+
			"A caller-supplied number that can only REDUCE a charge is a ceiling and is fine.\n"+
			"Anything the engine is required to derive is debt against INV-001.\n"+
			"Record which, in requestFieldVerdicts:\n  %s",
			len(unaccounted), strings.Join(unaccounted, "\n  "))
	}

	var stale []string
	for key := range requestFieldVerdicts {
		if _, ok := found[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d verdict(s) match no field. If the field was removed, delete the entry "+
			"— and if it was debt, that is progress worth saying in the commit:\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// TestCallerSuppliedMoneyDebtOnlyShrinks is the countdown.
//
// docs/DESIGN.md §11: a deployment is not intent-only while a caller
// can still name an amount. This number is that claim made countable.
// It must never rise, and it reaches zero when INV-001 is enforced
// rather than described.
func TestCallerSuppliedMoneyDebtOnlyShrinks(t *testing.T) {
	var debt []string
	for key, verdict := range requestFieldVerdicts {
		if verdict.Verdict == VerdictPendingMigration {
			debt = append(debt, key)
		}
	}
	sort.Strings(debt)

	const wantDebt = 7
	if len(debt) > wantDebt {
		t.Errorf("caller-supplied money/authority debt rose to %d, was %d.\n"+
			"A new field letting the caller decide what the engine must derive is a step away from INV-001:\n  %s",
			len(debt), wantDebt, strings.Join(debt, "\n  "))
	}
	if len(debt) < wantDebt {
		t.Errorf("debt fell to %d from %d — good. Update wantDebt to %d and name the migrated field in the commit:\n  %s",
			len(debt), wantDebt, len(debt), strings.Join(debt, "\n  "))
	}
}
