package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ciSkipEnvVars are the environment variables a test can require, which CI
// does not set — so any test gated on one is invisible to CI.
//
// REQUIRE_DOCKER is deliberately absent: .github/workflows/ci.yml DOES set it,
// so a Docker-gated test runs there.
var ciSkipEnvVars = []string{"REQUIRE_STRIPE", "STRIPE_SECRET_KEY"}

// 🔴 TestsCIcannotSeeAreInventoried names every test CI skips.
//
// A test that skips in CI reports green while proving nothing, and its
// breakage is discovered by a person, at a time nobody chose. That is not
// hypothetical here: the engine-issued acceptance control converted every
// standing fixture in the tree except the Stripe sandbox E2E, which skips
// because ci.yml sets neither REQUIRE_STRIPE nor STRIPE_SECRET_KEY. It stayed
// green in CI and was found only by running the suite by hand afterwards.
//
// This does not make those tests run — a Stripe key in CI is a decision with
// a cost, not an oversight to fix in a test file. What it does is stop the set
// from growing silently. A new Stripe-gated test is a deliberate addition to
// the list below, with the same trade made knowingly.
func TestTestsCIcannotSeeAreInventoried(t *testing.T) {
	// Files whose tests CI does not run, and which somebody has therefore
	// accepted responsibility for running by hand.
	known := map[string]string{
		"internal/provider/stripeadapter/sandbox_integration_test.go": "the one end-to-end against a real provider; needs a test key",
		"internal/shared/testutil/sandbox.go":                         "the skip helper itself",
		"internal/architecture/ci_skipped_tests_test.go":              "this inventory names the variables it looks for",
	}

	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	gated := map[string]bool{}
	var scanned int
	pattern := regexp.MustCompile(strings.Join(ciSkipEnvVars, "|"))

	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if name := info.Name(); name == ".git" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		// Only TEST files and the skip helpers they call. A production file
		// that READS STRIPE_SECRET_KEY is doing its job — it is not gating a
		// test on it, and naming it here would be noise that trains a reader
		// to ignore the list.
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		isTest := strings.HasSuffix(path, "_test.go")
		isHelper := strings.HasPrefix(rel, "internal/shared/testutil/")
		if !isTest && !isHelper {
			return nil
		}
		body, rerr := os.ReadFile(path)
		if rerr != nil {
			return nil
		}
		scanned++
		if pattern.Match(body) {
			gated[rel] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if scanned < 40 {
		t.Fatalf("the scan read only %d test files; it is not looking at the tree", scanned)
	}

	var unlisted, stale []string
	for file := range gated {
		if _, ok := known[file]; !ok {
			unlisted = append(unlisted, file)
		}
	}
	for file := range known {
		if !gated[file] {
			stale = append(stale, file)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)

	if len(unlisted) > 0 {
		t.Errorf("%d file(s) gate tests on a variable CI does not set, and are not in the "+
			"inventory:\n  %s\n\nCI will report them green while running nothing. Add them "+
			"here with who runs them and why, or remove the gate.",
			len(unlisted), strings.Join(unlisted, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("%d file(s) are inventoried as CI-skipped but no longer gate on one of %v:\n  %s\n\n"+
			"If CI now runs them, remove the entry — an inventory that overstates what is "+
			"unwatched is as misleading as one that understates it.",
			len(stale), ciSkipEnvVars, strings.Join(stale, "\n  "))
	}
}
