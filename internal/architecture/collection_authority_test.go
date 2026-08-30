package architecture

import (
	"sort"
	"strings"
	"testing"
)

// TestEveryCollectionAuthorityGrantIsNamed enumerates who may originate
// an automatic card charge.
//
// internal/account/credit now denies collection unless a call tree was
// explicitly granted it. That inversion is only worth having if the
// grants are visible: an opt-in nobody counts drifts back into the
// opt-out it replaced, one convenient call at a time.
func TestEveryCollectionAuthorityGrantIsNamed(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	grants, err := ScanCollectionAuthorityGrants(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(grants) == 0 {
		t.Fatal("scanned zero collection-authority grants; either the scanner is broken " +
			"or nothing can charge, and both are worth failing on")
	}

	found := map[string]Site{}
	for _, g := range grants {
		found[g.Key()] = g
	}

	var unnamed []string
	for key, site := range found {
		if _, ok := collectionAuthorityGrants[key]; !ok {
			unnamed = append(unnamed, site.File+":"+itoa(site.Line)+"  "+site.Func)
		}
	}
	sort.Strings(unnamed)
	if len(unnamed) > 0 {
		t.Errorf("%d path(s) granted themselves the capability to charge a card without being named.\n"+
			"Say whether it is an EVENT that legitimately refills, or a GAP:\n  %s",
			len(unnamed), strings.Join(unnamed, "\n  "))
	}

	var stale []string
	for key := range collectionAuthorityGrants {
		if _, ok := found[key]; !ok {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d named grant(s) match no call site. If one was removed, that closes a gap "+
			"— say so in the commit:\n  %s", len(stale), strings.Join(stale, "\n  "))
	}
}

// TestReadPathsThatCanChargeOnlyShrink counts the grants held by paths
// that are reads rather than events.
//
// docs/SECURITY.md §2: "A status read is not capability-safe when it
// can move money." One such grant remains, and it must reach zero
// before that row can close.
func TestReadPathsThatCanChargeOnlyShrink(t *testing.T) {
	var gaps []string
	for key, why := range collectionAuthorityGrants {
		if strings.HasPrefix(why, "GAP:") {
			gaps = append(gaps, key)
		}
	}
	sort.Strings(gaps)

	const wantGaps = 1
	if len(gaps) > wantGaps {
		t.Errorf("%d read path(s) can now charge a card, was %d. A read that collects is not "+
			"capability-safe however carefully its callers behave:\n  %s",
			len(gaps), wantGaps, strings.Join(gaps, "\n  "))
	}
	if len(gaps) < wantGaps {
		t.Errorf("read paths that can charge fell to %d from %d — that closes a gap register row. "+
			"Update wantGaps to %d and say which path in the commit:\n  %s",
			len(gaps), wantGaps, len(gaps), strings.Join(gaps, "\n  "))
	}
}
