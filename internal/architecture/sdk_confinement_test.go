package architecture

import (
	"sort"
	"strings"
	"testing"
)

// TestProviderSDKReachIsBounded is docs/VERIFICATION.md §5's provider
// SDK confinement.
//
// A file importing the SDK can build a provider request by hand, which
// the mutation inventory in this package cannot see: that scan watches
// calls on the typed client interfaces. So the set of files with SDK
// reach is the blind spot of every other check here, and it has to be
// enumerated for those checks to mean anything.
func TestProviderSDKReachIsBounded(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	importers, err := ScanProviderSDKImporters(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(importers) == 0 {
		t.Fatal("scanned zero SDK importers; the scanner is broken, not the tree")
	}

	found := map[string]bool{}
	for _, f := range importers {
		found[f] = true
	}

	var unlisted []string
	for f := range found {
		if _, ok := providerSDKImporters[f]; !ok {
			unlisted = append(unlisted, f)
		}
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		t.Errorf("%d file(s) import the provider SDK without being listed.\n"+
			"Reach the provider through a typed interface instead, so the mutation inventory can see it.\n"+
			"If the file genuinely needs the SDK, say which kind it is — adapter, transport, or test support:\n  %s",
			len(unlisted), strings.Join(unlisted, "\n  "))
	}

	var stale []string
	for f := range providerSDKImporters {
		if !found[f] {
			stale = append(stale, f)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d listed SDK importer(s) no longer import it; remove them:\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// TestProviderSDKDebtOnlyShrinks counts the files that reach the SDK
// from outside the adapter and the callback transports. Like the
// caller-supplied-money count, it is a number that must never rise.
func TestProviderSDKDebtOnlyShrinks(t *testing.T) {
	var debt []string
	for file, why := range providerSDKImporters {
		if strings.HasPrefix(why, "DEBT:") {
			debt = append(debt, file)
		}
	}
	sort.Strings(debt)

	const wantDebt = 3
	if len(debt) > wantDebt {
		t.Errorf("provider SDK reach spread to %d files outside the adapter, was %d:\n  %s",
			len(debt), wantDebt, strings.Join(debt, "\n  "))
	}
	if len(debt) < wantDebt {
		t.Errorf("SDK debt fell to %d from %d — good. Update wantDebt to %d and name the file in the commit:\n  %s",
			len(debt), wantDebt, len(debt), strings.Join(debt, "\n  "))
	}
}
