package statement_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/architecture"
)

var allowedPackageImports = map[string]bool{
	"fmt":      true,
	"math/big": true,
	"strconv":  true,
	"strings":  true,
	"time":     true,
}

var floatPattern = regexp.MustCompile(`\bfloat(32|64)\b`)
var bannedIdentPattern = regexp.MustCompile(`(?i)latefee|late_fee|interest|penalty`)

func packageDir(t *testing.T) string {
	t.Helper()
	root, err := architecture.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	return filepath.Join(root, "internal", "account", "statement")
}

func allGoFiles(t *testing.T) []string {
	t.Helper()
	dir := packageDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var files []string
	for _, e := range entries {
		// grep_test.go itself necessarily names what it looks for — in its
		// error messages, not just the regexes above — so it is excluded
		// from its own scan rather than made to self-match.
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && e.Name() != "grep_test.go" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files
}

func nonTestGoFiles(t *testing.T) []string {
	t.Helper()
	var files []string
	for _, f := range allGoFiles(t) {
		if !strings.HasSuffix(f, "_test.go") {
			files = append(files, f)
		}
	}
	return files
}

// TestPackage_NoFloatAnywhere covers production AND test code: a float
// creeping into even a test fixture would mean a test can no longer prove
// the package's arithmetic is float-free.
func TestPackage_NoFloatAnywhere(t *testing.T) {
	for _, path := range allGoFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if floatPattern.Match(data) {
			t.Errorf("%s: contains float32/float64 — money in this package must stay integer/big.Int only", path)
		}
	}
}

// TestPackage_OnlyStdlibImports enforces "no I/O, no clock, no provider
// client": the production files import nothing beyond a small stdlib
// allowlist — in particular no database/sql, no pgx, no time-faking seam,
// and no other internal package.
func TestPackage_OnlyStdlibImports(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t) {
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			if !allowedPackageImports[value] {
				t.Errorf("%s: imports %q — this package must stay pure (no I/O, no clock, no provider client)", path, value)
			}
		}
	}
}

// TestPackage_NoLateFeeInterestOrPenaltyIdentifiers scans actual Go
// identifiers (not string literals, not comments) in production code only —
// compose_test.go's refusal test for an unknown line kind deliberately
// strings a forbidden word into a *string literal*, which this must not flag.
func TestPackage_NoLateFeeInterestOrPenaltyIdentifiers(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range nonTestGoFiles(t) {
		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			id, ok := n.(*ast.Ident)
			if !ok {
				return true
			}
			if bannedIdentPattern.MatchString(id.Name) {
				t.Errorf("%s: identifier %q looks like a late fee / interest / penalty — this package composes a payment notice, not a finance charge", path, id.Name)
			}
			return true
		})
	}
}
