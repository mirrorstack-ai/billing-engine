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
)

// evidenceFreeWrites are the store methods that change billing state WITHOUT
// writing an INV-014 evidence record.
//
// Each has an evidence-writing sibling that production uses:
//
//	SaveIntent    ->  SaveIntentWithEvidence
//	RecordOutcome ->  RecordOutcomeWithEvidence
//
// They survive because the integration suites need to seed state without
// standing up a signing key for every fixture, and deleting them would mean
// threading a recorder through roughly thirty call sites that are not testing
// evidence at all.
var evidenceFreeWrites = map[string]string{
	"SaveIntent":    "seeds a sealed intent; production uses SaveIntentWithEvidence",
	"RecordOutcome": "records a settlement outcome; production uses RecordOutcomeWithEvidence",
}

// 🔴 TestNoProductionCallerWritesWithoutEvidence keeps the evidence-free
// writes test-only.
//
// docs/DESIGN.md:398 makes an evidence record "a durable side effect of the
// money moving, rather than a report the relay chooses to render". That is
// enforced by the WithEvidence methods committing both in one transaction —
// and it is only enforced while nothing can route around them.
//
// These two predate the outbox. They are not deprecated aliases: they are
// complete, working write paths that leave no trace, and a future caller
// reaching for the shorter name would silently produce charge documents and
// settlements the customer has no independent record of. Nothing in the type
// system prevents that, which is why this scan exists.
//
// The failure would be invisible in the worst way: everything works, every
// test passes, and the evidence table is simply emptier than the billing
// state it is supposed to attest.
//
// Same shape as predicate_single_caller.go and the AuthorizeAccepted pin, for
// the same reason: a rule enforced in one place stays enforced only while
// nothing else can go around it.
func TestNoProductionCallerWritesWithoutEvidence(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}

	var offenders []string
	var scanned int
	fset := token.NewFileSet()

	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			// The store declares them, and its own WithEvidence methods
			// delegate through the same private helper — declaring is not
			// calling.
			if strings.HasPrefix(rel, "internal/intent/store/") {
				return nil
			}
			scanned++

			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				return nil
			}
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if _, watched := evidenceFreeWrites[sel.Sel.Name]; !watched {
					return true
				}
				pos := fset.Position(call.Pos())
				offenders = append(offenders,
					rel+":"+itoa(pos.Line)+" calls "+sel.Sel.Name)
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	// A scan that reads nothing agrees with every claim.
	if scanned < 50 {
		t.Fatalf("the scan read only %d non-test files; it is not looking at the tree", scanned)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		var names []string
		for name, why := range evidenceFreeWrites {
			names = append(names, name+" ("+why+")")
		}
		sort.Strings(names)
		t.Errorf("%d production call(s) write billing state without an evidence record:\n  %s\n\n"+
			"docs/DESIGN.md:398 makes an evidence record a durable side effect of the money "+
			"moving. These methods leave no trace, so the charge documents and settlements "+
			"they produce would have no independent record — and nothing would say so: every "+
			"test passes and the evidence table is simply emptier than the state it attests.\n\n"+
			"Use the WithEvidence sibling:\n  %s",
			len(offenders), strings.Join(offenders, "\n  "), strings.Join(names, "\n  "))
	}
}
