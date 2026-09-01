package architecture

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// 🔴 A COLLECTOR MAY ONLY BE DELETED ONCE SOMETHING ELSE CAN COLLECT IT.
//
// docs/DESIGN.md §11 step 8 is "migrate every caller to intents, THEN delete the
// direct charge code". The executor enforces the second half — it refuses to
// start while capabilities.LegacyMoneyPaths != 0 — so the new rail only ever
// goes live at the moment the old one is deleted. There is no state in which
// both exist and the new one collects.
//
// That makes the deletion the riskiest single change in this repository, and
// scripts/legacy-drop-preconditions.sql does NOT cover the risk that matters
// here. It measures DURABLE STATE — whether a charge is mid-flight — and every
// row can be clear while a leg has no replacement in the code at all. A clean
// run of that script therefore reads as "safe to delete" for a leg whose
// deletion would leave its charge kind with NO collector, permanently. Not
// stranded mid-flight: uncollectable from then on, silently, per kind.
//
// So this asserts the other half, from the code: every leg that still collects
// either PROPOSES (a replacement exists and the deletion is safe to schedule)
// or is named below with the reason it cannot yet, and why that is not an
// oversight.
//
// It is deliberately a statement about ROUTING, not about enabling. A routed
// leg still collects today; the flag that arms it is off. Routing is what makes
// the deletion survivable, which is the question this test answers.

// unroutedLegs are the collecting legs with no proposer branch, and why.
//
// Both are blocked on a decision rather than on effort. Neither can be routed by
// writing more code, which is exactly why they belong in a pinned list instead
// of a TODO: the next person to attempt the drop must be told, by a failing
// test, that deleting these two removes the only collector their charge kinds
// have.
// 🔴 EMPTY. Every collecting leg proposes.
//
// It held two entries this morning. creditpurchase came off when the owner
// accepted a changed browser contract; billing/unpaid.go came off when a
// settlement started recording WHICH provider object it moved through
// (migration 069) — without that, a receivable had no source intent to link to
// and the leg was not routable by any amount of effort.
//
// An empty list is not the end of the guard's job. It now asserts that nothing
// is ADDED without a replacement, and the test below still refuses a
// LegacyMoneyPaths of 0 while anything is listed.
var unroutedLegs = map[string]string{}

func TestEveryCollectingLegCanBeReplacedOrIsNamed(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	files := collectingLegFiles()
	if len(files) == 0 {
		t.Fatal("no collecting legs were found in the allowlist, so this test would pass vacuously")
	}

	var unrouted []string
	for _, rel := range files {
		src, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		// A leg proposes when it branches on an installed proposer. Every
		// cut-over leg does this the same way, after its own arming claim.
		if strings.Contains(string(src), ".proposer != nil") {
			if reason, named := unroutedLegs[rel]; named {
				t.Errorf("%s now has a proposer branch but is still listed as unrouted.\n"+
					"Remove its entry — a stale exemption hides the next leg that genuinely "+
					"cannot be routed.\nIts recorded reason was: %s", rel, reason)
			}
			continue
		}
		if _, named := unroutedLegs[rel]; named {
			continue
		}
		unrouted = append(unrouted, rel)
	}

	if len(unrouted) > 0 {
		sort.Strings(unrouted)
		t.Fatalf("%d collecting leg(s) neither propose nor are named as deliberately unrouted:\n  %s\n\n"+
			"Deleting a collector whose charge kind has no replacement does not strand a charge "+
			"mid-flight — it makes that kind uncollectable from then on, silently. "+
			"scripts/legacy-drop-preconditions.sql cannot see this: it measures durable state, "+
			"and every row can be clear while the code has no replacement at all.\n"+
			"Either route the leg, or add it to unroutedLegs with the reason it cannot be routed yet.",
			len(unrouted), strings.Join(unrouted, "\n  "))
	}
}

// The drop cannot be complete while a leg is unrouted, and this says so in the
// one number the executor actually reads.
//
// capabilities.LegacyMoneyPaths reaching zero is what lets cmd/intent-executor
// start. It must not reach zero while unroutedLegs is non-empty: that would mean
// every collector was deleted including the two with no replacement, and the
// deployment would come up believing it is intent-only while two charge kinds
// have no way to collect at all.
func TestTheLegacyCountCannotReachZeroWhileALegIsUnrouted(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(filepath.Join(root, "internal", "account", "capabilities", "capabilities.go"))
	if err != nil {
		t.Fatal(err)
	}

	zero := strings.Contains(string(src), "LegacyMoneyPaths = 0")
	if zero && len(unroutedLegs) > 0 {
		var legs []string
		for leg := range unroutedLegs {
			legs = append(legs, leg)
		}
		sort.Strings(legs)
		t.Fatalf("LegacyMoneyPaths is 0 — which lets cmd/intent-executor start and asserts this "+
			"deployment is intent-only — while %d leg(s) still have no replacement:\n  %s\n\n"+
			"Their charge kinds would have no collector at all. Route them, or do not claim zero.",
			len(unroutedLegs), strings.Join(legs, "\n  "))
	}
}

// collectingLegFiles is every file the COLLECT allowlist names, deduplicated.
//
// Derived from the allowlist rather than hardcoded, so a leg added there is
// covered by this test automatically — the failure mode being guarded against
// is someone adding a collector and nobody noticing it has no replacement.
func collectingLegFiles() []string {
	seen := map[string]bool{}
	var out []string
	for key, reason := range allowedProviderMutations {
		if !strings.HasPrefix(reason, "COLLECT:") {
			continue
		}
		file := key
		if i := strings.Index(key, " "); i > 0 {
			file = key[:i]
		}
		// The shared client is the transport every leg calls through, not a
		// leg: it has no charge of its own to replace.
		if strings.HasPrefix(file, "internal/shared/") {
			continue
		}
		if seen[file] {
			continue
		}
		seen[file] = true
		out = append(out, file)
	}
	sort.Strings(out)
	return out
}
