package architecture

import (
	"sort"
	"strings"
	"testing"
)

// TestExecutionPredicateHasAtMostOneCaller keeps the decision from
// spreading.
//
// docs/DESIGN.md §4 calls the predicate "the single copy", and the
// value of that is it being the only route rather than merely existing.
// A second caller is how the coordinator's seven-site weave came to
// exist — nobody adds seven at once.
//
// "At most" rather than "exactly", because the executor that will call
// it is a later wave. Zero callers means the predicate is not yet
// wired, which the count below states rather than hides.
func TestExecutionPredicateHasAtMostOneCaller(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	callers, err := ScanPredicateCallers(root, "internal", "cmd")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	if len(callers) > 1 {
		var where []string
		for _, c := range callers {
			where = append(where, c.File+":"+itoa(c.Line)+"  "+c.Func)
		}
		sort.Strings(where)
		t.Fatalf("the execution predicate has %d callers. It is meant to be the only route to a "+
			"charge, and a second caller is how a decision becomes a weave:\n  %s",
			len(callers), strings.Join(where, "\n  "))
	}

	// Not a failure — a status. The predicate lands before the executor
	// that calls it, and a test that demanded a caller would force the
	// wiring to arrive with the gate rather than after it.
	if len(callers) == 0 {
		t.Log("the execution predicate has no caller yet; it is built but not wired")
	}
}
