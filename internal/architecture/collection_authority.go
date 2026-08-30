package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// collectionAuthorityGrant is the function that hands a call tree the
// capability to originate an automatic card charge.
const collectionAuthorityGrant = "authorizeCollection"

// ScanCollectionAuthorityGrants returns every place that grants the
// capability to collect.
//
// internal/account/credit denies collection by default and requires an
// explicit grant, which moves the question from "did every caller
// remember to opt out?" to "who opted in?". That second question only
// has value if the answer is written down, which is what this scan and
// the table below are for.
func ScanCollectionAuthorityGrants(repoRoot string, roots ...string) ([]Site, error) {
	var sites []Site
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, relErr := filepath.Rel(repoRoot, path)
			if relErr != nil {
				return relErr
			}
			file, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil || fn.Name.Name == collectionAuthorityGrant {
					continue
				}
				name := funcName(fn)
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					ident, ok := call.Fun.(*ast.Ident)
					if !ok || ident.Name != collectionAuthorityGrant {
						return true
					}
					sites = append(sites, Site{
						File:   filepath.ToSlash(rel),
						Func:   name,
						Method: collectionAuthorityGrant,
						Effect: EffectCollect,
						Line:   fset.Position(ident.Pos()).Line,
					})
					return true
				})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return sites, nil
}

// collectionAuthorityGrants names every entrypoint that may originate
// an automatic card charge, and says why it is one.
//
// Three are events: money is owed or a balance has changed, and a
// refill is the point of the wallet. One is a read, and it is the gap
// docs/SECURITY.md §2 records.
var collectionAuthorityGrants = map[string]string{
	"internal/account/credit/coordinator.go (*Coordinator).EvaluateCreditUsage authorizeCollection": "EVENT: usage arriving is how spend reaches the wallet; refusing a refill here blocks a credits customer rather than protecting one",
	"internal/account/credit/coordinator.go (*Coordinator).ObserveAccount authorizeCollection":      "EVENT: a settlement observed against the wallet can leave a balance needing a refill",
	"internal/account/credit/coordinator.go (*Coordinator).ReconcileBoundary authorizeCollection":   "EVENT: the scheduled period boundary, which is the run that charges",

	"internal/account/credit/coordinator.go (*Coordinator).OutOfCredits authorizeCollection": "GAP: the read behind the platform's service-block gate. Removing it deadlocks — a blocked account serves nothing, records no usage, and usage is what drives the other refill path. The remedy is the intent executor of docs/DESIGN.md §11.",
}
