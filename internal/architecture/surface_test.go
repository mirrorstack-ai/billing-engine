package architecture

import (
	"sort"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/account/capabilities"
)

// TestEveryProviderMutationIsInventoried is docs/DESIGN.md §11 step 2:
// "Inventory every provider mutation, and add a CI allow-list naming
// each one."
//
// It fails in both directions. An unlisted call site means a money path
// was added without anyone writing down why it exists. A listed site
// with no matching call means the inventory has drifted into describing
// code that is gone, which is worse than no inventory: docs/SECURITY.md
// is written on the premise that this enumeration is real.
func TestEveryProviderMutationIsInventoried(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	sites, err := ScanProviderMutations(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(sites) == 0 {
		t.Fatal("scanned zero provider mutations; the scanner is broken, not the tree")
	}

	found := map[string]Site{}
	for _, s := range sites {
		found[s.Key()] = s
	}

	var unlisted []string
	for key, site := range found {
		if _, ok := allowedProviderMutations[key]; !ok {
			unlisted = append(unlisted, string(site.Effect)+"  "+site.File+":"+itoa(site.Line)+"  "+site.Func+" -> "+site.Method)
		}
	}
	sort.Strings(unlisted)
	if len(unlisted) > 0 {
		t.Errorf("%d provider mutation(s) are not in the inventory.\n"+
			"Add each to allowedProviderMutations with the reason it may exist,\n"+
			"and if it can take money say what it charges for:\n  %s",
			len(unlisted), strings.Join(unlisted, "\n  "))
	}

	var stale []string
	for key := range allowedProviderMutations {
		if _, ok := found[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d inventory entr(ies) match no call site; remove them:\n  %s",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// TestCollectionSurfaceIsSmallAndNamed pins the size of the surface that
// can take money.
//
// The number is asserted rather than merely reported because the whole
// argument of docs/DESIGN.md §11 is a reachability claim: "the weakest
// reachable money path defines the guarantee". A migration that adds an
// intent executor while leaving the old collectors in place would still
// pass the inventory test above, since each site would simply be listed.
// This one makes growth visible.
//
// The count is expected to FALL as the intent executor takes over and
// the legacy collectors are deleted. When it does, update the number
// here and say in the commit which path went away.
func TestCollectionSurfaceIsSmallAndNamed(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	sites, err := ScanProviderMutations(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	var collectors []string
	for _, s := range sites {
		if s.Effect != EffectCollect {
			continue
		}
		// The adapter's own wrapped SDK calls are not independent money
		// paths, and the webhook test probe exists to detect charges
		// rather than to make them.
		if s.File == "internal/shared/stripe/client.go" ||
			strings.Contains(s.File, "/webhooktest/") {
			continue
		}
		collectors = append(collectors, s.File+" "+s.Func+" "+s.Method)
	}
	sort.Strings(collectors)

	// The expectation is the number the service reports about itself,
	// so there is exactly one place to change when a money path is
	// deleted — and changing it is what makes the deletion visible on
	// the Capabilities surface.
	wantCollectors := capabilities.LegacyMoneyPaths
	if len(collectors) != wantCollectors {
		t.Errorf("the surface that can take money has %d call sites, but capabilities.LegacyMoneyPaths says %d.\n"+
			"If this grew, a new way to charge a customer was added.\n"+
			"If it shrank, a legacy money path was removed — update the constant and say which:\n  %s",
			len(collectors), wantCollectors, strings.Join(collectors, "\n  "))
	}

}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// TestReportedLegacyMoneyPathCountIsTrue ties the number this service
// reports about itself to the tree it was built from.
//
// docs/VERIFICATION.md §2 makes legacyMoneyPaths the field the
// intent-only claim rests on, and docs/SECURITY.md §3 says a check the
// private caller can satisfy with a statement about itself is not a
// control. The same applies to a service reporting on its own
// architecture: capabilities.LegacyMoneyPaths would be an assertion if
// nothing checked it. This is the check.
func TestReportedLegacyMoneyPathCountIsTrue(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	sites, err := ScanProviderMutations(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	actual := 0
	for _, s := range sites {
		if s.Effect != EffectCollect {
			continue
		}
		if s.File == "internal/shared/stripe/client.go" || strings.Contains(s.File, "/webhooktest/") {
			continue
		}
		actual++
	}

	if capabilities.LegacyMoneyPaths != actual {
		t.Fatalf("Capabilities reports legacyMoneyPaths=%d, but this tree has %d call sites that can take money.\n"+
			"The reported number must be the true one — it is what the intent-only claim rests on.",
			capabilities.LegacyMoneyPaths, actual)
	}
}
