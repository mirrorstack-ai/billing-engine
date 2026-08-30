package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// predicatePackage is the import path of the execution predicate, and
// predicateEntry is the one function that decides whether money moves.
const (
	predicatePackage = "github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
	predicateEntry   = "Evaluate"
)

// ScanPredicateCallers returns every non-test call of the execution
// predicate.
//
// docs/DESIGN.md §4 calls it "the single copy of the predicate". The
// value of that is not the function existing — it is the function being
// the only route. internal/account/credit/coordinator.go is the
// counter-example this whole design is written against: the charge
// decision reached through seven call sites across four public methods,
// which is where docs/SECURITY.md §2's capability leak lives. Nothing
// stops that shape reappearing except a check that notices.
//
// Matching is on a selector against the package's local import name, so
// a caller that aliases the import is still seen.
func ScanPredicateCallers(repoRoot string, roots ...string) ([]Site, error) {
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

			local, imported := localNameFor(file, predicatePackage)
			if !imported {
				return nil
			}
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
					if !ok || sel.Sel.Name != predicateEntry {
						return true
					}
					pkg, ok := sel.X.(*ast.Ident)
					if !ok || pkg.Name != local {
						return true
					}
					sites = append(sites, Site{
						File:   filepath.ToSlash(rel),
						Func:   name,
						Method: predicateEntry,
						Effect: EffectCollect,
						Line:   fset.Position(sel.Sel.Pos()).Line,
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

// localNameFor returns the identifier a file refers to an imported
// package by, honouring an alias, and whether it imports it at all.
func localNameFor(file *ast.File, importPath string) (string, bool) {
	quoted := `"` + importPath + `"`
	for _, imp := range file.Imports {
		if imp.Path.Value != quoted {
			continue
		}
		if imp.Name != nil {
			return imp.Name.Name, true
		}
		// No alias: the local name is the last path element.
		parts := strings.Split(importPath, "/")
		return parts[len(parts)-1], true
	}
	return "", false
}
