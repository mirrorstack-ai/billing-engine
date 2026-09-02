package architecture

import (
	"os/exec"
	"strings"
	"testing"
)

// TestTheShadowRunnerCannotMoveMoney asserts what cmd/intent-shadow
// links against.
//
// docs/DESIGN.md §11 step 3 asks for shadow intents that "notify nobody
// and move no money". A comment saying so is worth nothing on the day
// someone adds a convenient import — this makes the claim a property of
// the dependency graph, checked by the compiler's own view of it.
//
// The binary is meant to be safe to run against PRODUCTION, which is
// the whole reason it exists: it is the gate every wave-5 cutover waits
// on, and it is the only part of that work that can be run before the
// cutovers rather than after.
func TestTheShadowRunnerCannotMoveMoney(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command("go", "list", "-deps", "./cmd/intent-shadow")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v", err)
	}

	forbidden := map[string]string{
		"github.com/stripe/stripe-go":                                         "the provider SDK",
		"github.com/mirrorstack-ai/billing-engine/internal/shared/stripe":     "the provider adapter surface",
		"github.com/mirrorstack-ai/billing-engine/internal/provider":          "a provider adapter",
		"github.com/mirrorstack-ai/billing-engine/internal/intent/executor":   "the executor",
		"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup": "the auto-top-up executor",
		"github.com/mirrorstack-ai/billing-engine/internal/account/cycle":     "the legacy charge cycle",
	}

	var linked []string
	for _, dep := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		for prefix, what := range forbidden {
			if strings.HasPrefix(dep, prefix) {
				linked = append(linked, dep+"  ("+what+")")
			}
		}
	}

	if len(linked) > 0 {
		t.Errorf("cmd/intent-shadow links against %d thing(s) that can change state.\n"+
			"It is meant to be safe to run against production, and that safety is supposed to be "+
			"structural rather than a promise about what the code does:\n  %s",
			len(linked), strings.Join(linked, "\n  "))
	}

	// A binary that linked nothing would also pass the check above, so
	// confirm it really is the shadow runner being measured.
	if !strings.Contains(string(out), "internal/intent/shadow") {
		t.Error("cmd/intent-shadow does not link the shadow package; this check is measuring the wrong thing")
	}
}
