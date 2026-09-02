package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 🔴 auto-top-up must have ONE installation site, not six.
//
// Until 2026-08-30 the same five lines — NewExecutor(NewStore(pool),
// creditledger.NewStore(pool), NewAutoTopUpClient(stripeKey)) — were repeated
// in six binaries: account-api, account-webhook,
// account-webhook-eventbridge, billing-cycle, infra-egress-sync and
// infra-ssr-compute-sync.
//
// That matters for the intent migration specifically. cycle.Service has one
// constructor, so its cutover had one hang point. Auto-top-up had six chances
// to miss one — and SECURITY.md records that four ordinary read and ingest
// paths reach this executor, so a missed site is not a cosmetic inconsistency,
// it is a path that still collects after the leg is believed cut over.
//
// This test fails if a seventh appears, or if one of the six drifts back to
// building its own.
func TestAutoTopUpIsInstalledThroughOneConstructor(t *testing.T) {
	var offenders []string

	err := filepath.Walk("../../cmd", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
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
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "autotopup" || sel.Sel.Name != "NewExecutor" {
				return true
			}
			offenders = append(offenders,
				filepath.ToSlash(path)+":"+fset.Position(call.Pos()).String())
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk cmd/: %v", err)
	}

	if len(offenders) > 0 {
		t.Fatalf("a binary builds its own auto-top-up executor instead of using "+
			"autotopup.NewStandardExecutor:\n  %s\n\n"+
			"Every installation site is a place the intent cutover has to find. Four "+
			"ordinary read and ingest paths reach this executor, so a missed site is a "+
			"path that still collects.", strings.Join(offenders, "\n  "))
	}
}
