// Package architecture holds the static checks that constrain this
// tree's money-moving surface.
//
// docs/VERIFICATION.md §5 lists what CI must enforce and records that
// none of it ran at the 78b5c69 baseline: the whole gate was go vet, go
// build and go test. These checks are that section becoming executable.
//
// They are deliberately static. A test that boots the service can only
// show that some path did not charge on the input it was given; a check
// over the source shows that no path can reach a provider mutation
// except the ones written down here. The second is the claim
// docs/DESIGN.md §11 needs, because "the weakest reachable money path
// defines the guarantee" is a statement about reachability, not about a
// sampled run.
package architecture

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// Effect classifies what a provider method does, mirroring
// internal/shared/stripe/stripetest. It is redeclared rather than
// imported so that this package — the thing that decides which call
// sites are permitted — does not depend on the test double whose
// classification it is meant to corroborate.
type Effect string

const (
	EffectMutate  Effect = "mutate"
	EffectCollect Effect = "collect"
)

// providerEffects maps every state-changing provider method to its
// effect. Reads are absent by design: this package exists to constrain
// the calls that change something.
var providerEffects = map[string]Effect{
	"CreateCustomer":                     EffectMutate,
	"UpdateCustomerEmail":                EffectMutate,
	"CreateCheckoutSession":              EffectMutate,
	"DetachPaymentMethod":                EffectMutate,
	"SetDefaultPaymentMethod":            EffectMutate,
	"CreateDraftInvoice":                 EffectMutate,
	"CreateCreditPurchaseInvoice":        EffectMutate,
	"CreateAutoTopUpInvoice":             EffectMutate,
	"CreateInvoiceItem":                  EffectMutate,
	"CreateCombinedProrationInvoiceItem": EffectMutate,
	"FinalizeInvoiceWithoutAutoAdvance":  EffectMutate,
	"VoidInvoice":                        EffectMutate,
	"DeleteDraftInvoice":                 EffectMutate,

	// These three can take money from a stored payment method.
	"FinalizeInvoice":      EffectCollect,
	"PayInvoice":           EffectCollect,
	"PayInvoiceWithMethod": EffectCollect,
}

// Site is one provider-mutation call found in the tree.
type Site struct {
	// File is repo-relative.
	File string
	// Func is the enclosing function or method, as "Name" or
	// "(Receiver).Name". Sites are anchored on the enclosing function
	// rather than a line number, so ordinary edits above a call do not
	// churn the allow-list while a call moving to a different function
	// does.
	Func   string
	Method string
	Effect Effect
	Line   int
}

// Key is the allow-list identity of a site.
func (s Site) Key() string { return s.File + " " + s.Func + " " + s.Method }

// ScanProviderMutations returns every provider-mutation call site under
// the given roots, excluding test files.
//
// It matches on the selector name — x.FinalizeInvoice(...) — without
// resolving x's type. That over-approximates: an unrelated method that
// happens to share a name is reported. Over-approximating is the right
// error for this check, because a missed site is a money path nobody
// reviewed while a spurious one costs an allow-list entry and a comment
// explaining why it is harmless.
func ScanProviderMutations(repoRoot string, roots ...string) ([]Site, error) {
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
			sites = append(sites, scanFile(fset, file, filepath.ToSlash(rel))...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return sites, nil
}

func scanFile(fset *token.FileSet, file *ast.File, rel string) []Site {
	var sites []Site

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		name := funcName(fn)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			effect, ok := providerEffects[sel.Sel.Name]
			if !ok {
				return true
			}
			sites = append(sites, Site{
				File:   rel,
				Func:   name,
				Method: sel.Sel.Name,
				Effect: effect,
				Line:   fset.Position(sel.Sel.Pos()).Line,
			})
			return true
		})
	}
	return sites
}

func funcName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return "(" + typeName(fn.Recv.List[0].Type) + ")." + fn.Name.Name
}

func typeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.StarExpr:
		return "*" + typeName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return typeName(t.X)
	case *ast.SelectorExpr:
		return typeName(t.X) + "." + t.Sel.Name
	}
	return "?"
}

// RepoRoot walks up from the working directory to the directory holding
// go.mod.
func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the working directory")
		}
		dir = parent
	}
}
