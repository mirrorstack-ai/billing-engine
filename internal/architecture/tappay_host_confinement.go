package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// tappayHost is the host every TapPay back-end call reaches, in both
// its sandbox and (unreachable today) production forms. Only
// internal/provider/tappaygateway may name it: see
// tappay_host_confinement_test.go and billing-engine#310 item 9.
const tappayHost = "tappaysdk.com"

// tappayHostConfinedPackage is the one package allowed to name
// tappayHost. A prefix rather than an enumerated file map (contrast
// providerSDKImporters above): this is one package with one role, not
// several kinds of reach to distinguish.
const tappayHostConfinedPackage = "internal/provider/tappaygateway/"

// tappayHostConfinementScannerFile is this file itself. It names
// tappayHost to define the constant the scan matches against, not to
// construct a request — excluded explicitly rather than widening
// tappayHostConfinedPackage, so the allowlist stays "one adapter
// package, plus the one file that says what it's checking for".
const tappayHostConfinementScannerFile = "internal/architecture/tappay_host_confinement.go"

// ScanTapPayHostNamers returns every .go file under the given roots
// (relative to repoRoot) whose source names tappayHost in a string
// literal.
//
// Test files are included deliberately: a test asserting on the
// sandbox host (as tappaygateway's own tests do) is exactly the kind
// of reach this confinement allows from the one package that owns it,
// and a test in a different package naming the host is exactly the
// kind of leak it exists to catch.
func ScanTapPayHostNamers(repoRoot string, roots ...string) ([]string, error) {
	var out []string
	fset := token.NewFileSet()

	for _, root := range roots {
		err := filepath.Walk(filepath.Join(repoRoot, root), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if info.Name() == "testdata" || info.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") {
				return nil
			}
			file, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			found := false
			ast.Inspect(file, func(n ast.Node) bool {
				if found {
					return false
				}
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					return true
				}
				value, unquoteErr := strconv.Unquote(lit.Value)
				if unquoteErr != nil {
					return true
				}
				if strings.Contains(value, tappayHost) {
					found = true
					return false
				}
				return true
			})
			if found {
				rel, relErr := filepath.Rel(repoRoot, path)
				if relErr != nil {
					return relErr
				}
				out = append(out, filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
