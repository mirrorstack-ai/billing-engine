package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// 🔴 EVERY FIELD OF THE PREDICATE'S INPUT MUST BE ASSIGNED, OR NAMED HERE.
//
// On 2026-09-01 one unassigned field disarmed three controls at once.
// predicate.SealedState carried intent.PriorUse, BillingAuthorization.Permits
// read it, and the executor's literal simply did not set it — so it was the
// zero value on every real evaluation:
//
//   - the PERIOD ceiling became a per-charge ceiling: `0 + total > ceiling`
//     admits one charge at the cap, then another, without limit;
//   - the FREQUENCY ceiling never refused at all, since `0 + 1 > ceiling` is
//     false for every ceiling a grant can carry;
//   - RefusalAttemptUnresolved never fired.
//
// Every ceiling test passed throughout, because each one CONSTRUCTS a PriorUse
// by hand and calls Permits directly. They tested the predicate, which was
// correct. Nothing tested the wiring.
//
// intent.PriorUse's own doc comment shows why this needs a test rather than
// care: it is a struct "so that adding a bound is a COMPILE ERROR at every call
// site rather than a silently-defaulted zero". That protects the call sites
// that CONSTRUCT one. Nothing protects the site that never mentions the field —
// a struct literal with a field missing compiles, and the field is zero.
// Omission is invisible exactly where addition is loud.
//
// So the rule is: the executor's SealedState literal names every field of the
// struct. A field deliberately left at its zero value is listed below with the
// reason, which turns "we forgot" into "we decided".
var sealedStateIntentionallyUnset = map[string]string{
	"Acceptance": "the customer-present authority mode is unbuilt: nothing produces a " +
		"ScopeOneTime authorization and no store loader for an AcceptanceReceipt exists, " +
		"so this can only ever be zero. It fails CLOSED — authorityEvidenceBinds requires a " +
		"non-empty DisclosureDigest in that mode (predicate.go:296) and refuses without it — " +
		"and in standing mode a zero receipt is REQUIRED (predicate.go:329). Assigning it " +
		"needs the mode, not a wire-up.",
}

func TestEveryPredicateInputIsWiredOrNamed(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	declared, err := sealedStateFields(filepath.Join(root, "internal", "intent", "predicate", "state.go"))
	if err != nil {
		t.Fatalf("read the SealedState declaration: %v", err)
	}
	if len(declared) == 0 {
		t.Fatal("no SealedState fields were found, so this test would pass vacuously")
	}

	assigned, err := sealedStateLiteralFields(filepath.Join(root, "internal", "intent", "executor", "executor.go"))
	if err != nil {
		t.Fatalf("read the executor's SealedState literal: %v", err)
	}
	if len(assigned) == 0 {
		t.Fatal("the executor's SealedState literal was not found, so this test would pass vacuously")
	}

	var missing []string
	for _, f := range declared {
		if assigned[f] {
			continue
		}
		if _, named := sealedStateIntentionallyUnset[f]; named {
			continue
		}
		missing = append(missing, f)
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("predicate.SealedState has %d field(s) the executor never assigns:\n  %s\n\n"+
			"A missing field is not a loose control, it is an absent one, and its unit tests "+
			"still pass: they construct the input by hand. Either assign it in the executor's "+
			"literal, or add it to sealedStateIntentionallyUnset with the reason its zero value "+
			"is correct.",
			len(missing), strings.Join(missing, "\n  "))
	}

	// The allowlist must not outlive the field it excuses, or it becomes a
	// permanent hole named after a problem that was fixed.
	declaredSet := map[string]bool{}
	for _, f := range declared {
		declaredSet[f] = true
	}
	for f := range sealedStateIntentionallyUnset {
		if !declaredSet[f] {
			t.Errorf("sealedStateIntentionallyUnset names %q, which is not a SealedState field. "+
				"Remove it: an allowlist entry for a field that no longer exists excuses nothing "+
				"and hides the next one.", f)
		}
		if assigned[f] {
			t.Errorf("sealedStateIntentionallyUnset names %q, but the executor now DOES assign it. "+
				"Remove the entry so the field is checked like every other.", f)
		}
	}
}

// sealedStateFields returns the exported field names of predicate.SealedState.
func sealedStateFields(path string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, err
	}

	var out []string
	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "SealedState" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			return false
		}
		for _, f := range st.Fields.List {
			for _, name := range f.Names {
				if name.IsExported() {
					out = append(out, name.Name)
				}
			}
		}
		return false
	})
	return out, nil
}

// sealedStateLiteralFields returns the field names the executor's
// predicate.SealedState composite literal assigns.
func sealedStateLiteralFields(path string) (map[string]bool, error) {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return nil, err
	}

	out := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "SealedState" {
			return true
		}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if key, ok := kv.Key.(*ast.Ident); ok {
				out[key.Name] = true
			}
		}
		return true
	})
	return out, nil
}
