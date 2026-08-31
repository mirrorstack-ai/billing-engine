package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
)

// TestProducibleKindsMatchTheirWriters keeps evidence.Kind.Producible honest.
//
// 🔴 Producible is a hand-written claim in source, and this repository's
// standing lesson is that a hand-written claim decays: migration 054's sealed
// column tuple, the digest mutation map, and storedFrom all said something
// true when written and false three changes later.
//
// It is also load-bearing. An empty evidence table is consistent with both
// "nothing has happened" and "nothing can happen", and Producible is the only
// thing that distinguishes them — for the Capabilities surface, for a
// reviewer, and for the four members of migration 064's CHECK that have no
// writer.
//
// So this SCANS for the kinds actually minted in non-test code and fails when
// the two sets disagree, in either direction:
//
//   - a kind that gained a writer without an entry here understates the
//     deployment, and the surface reports less than is true;
//   - an entry with no writer overstates it, which is the "declared but not
//     implemented" failure docs/SECURITY.md exists to expose, in the one place
//     whose job is to say what is true.
func TestProducibleKindsMatchTheirWriters(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	claimed := map[string]bool{}
	for _, k := range evidence.AllKinds {
		if k.Producible() {
			claimed[string(k)] = true
		}
	}

	minted := mintedEvidenceKinds(t, root)

	if len(minted) == 0 {
		t.Fatal("the scan found no evidence kind minted anywhere. Either the wiring is " +
			"gone or the scanner is broken, and a scan that finds nothing agrees with " +
			"every claim.")
	}

	for kind := range claimed {
		if !minted[kind] {
			t.Errorf("evidence.Kind(%q).Producible() is true, but no non-test caller mints "+
				"it. Either wire a writer or set it false: a surface reporting a kind "+
				"nothing can produce is asserting a subsystem that does not run.", kind)
		}
	}
	for kind := range minted {
		if !claimed[kind] {
			t.Errorf("%q is minted in non-test code but Producible() returns false, so "+
				"evidence.Recorder.Seal refuses it and the write fails at runtime. Add it "+
				"to Producible.", kind)
		}
	}

	if t.Failed() {
		t.Logf("claimed: %v", sortedKeys(claimed))
		t.Logf("minted:  %v", sortedKeys(minted))
	}
}

// mintedEvidenceKinds finds every `Kind: evidence.KindX` written in a
// composite literal in non-test Go source.
//
// It over-approximates deliberately — a constant referenced anywhere in a
// non-test file counts — because the safe direction for THIS check is to
// notice too much. An over-count makes the test complain that Producible is
// missing an entry, which a human resolves; an under-count would silently
// bless a false claim.
func mintedEvidenceKinds(t *testing.T, root string) map[string]bool {
	t.Helper()

	byConst := map[string]string{}
	for _, k := range evidence.AllKinds {
		byConst[constNameFor(string(k))] = string(k)
	}

	found := map[string]bool{}
	fset := token.NewFileSet()

	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			// The evidence package declares the constants; referencing them
			// there is not minting one.
			if strings.Contains(path, filepath.Join("internal", "intent", "evidence")) {
				return nil
			}

			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return nil // a file this scanner cannot parse is not evidence of anything
			}
			ast.Inspect(file, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				pkg, ok := sel.X.(*ast.Ident)
				if !ok || pkg.Name != "evidence" {
					return true
				}
				if kind, ok := byConst[sel.Sel.Name]; ok {
					found[kind] = true
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}
	return found
}

// constNameFor turns "sealed_intent" into "KindSealedIntent".
func constNameFor(value string) string {
	out := "Kind"
	for _, part := range strings.Split(value, "_") {
		if part == "" {
			continue
		}
		out += strings.ToUpper(part[:1]) + part[1:]
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
