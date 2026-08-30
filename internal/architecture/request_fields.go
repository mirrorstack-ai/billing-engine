package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// RequestField is one field on a request type that the caller fills in.
type RequestField struct {
	File  string
	Type  string
	Field string
	// GoType is the declared type, so that a reviewer can tell a
	// *int64 ceiling from an int64 charge amount without opening the
	// file.
	GoType string
	Line   int
}

// Key is the identity used by the exemption table.
func (f RequestField) Key() string { return f.Type + "." + f.Field }

// moneyFieldNames are the field-name stems that denote an amount of
// money. A request struct is the caller's side of the wire, so a field
// named for money is the caller proposing a number the engine should
// use — which INV-001 forbids: the engine derives every financial
// field from a sealed intent and one catalog selection.
var moneyFieldNames = []string{
	"Micros", "Cents", "Amount", "Price", "Fee", "Total",
}

// A bare "Limit" is pagination, not money. Money limits in this tree
// are always denominated, so they carry Micros or Cents and are caught
// by the stems above. Listing "Limit" would flag every list request and
// train reviewers to ignore the check.

// authorityFieldNames are field-name stems by which a caller asserts a
// fact about authority rather than supplying an input.
//
// docs/SECURITY.md §3 puts it flatly: a check the private caller can
// satisfy with a statement about itself is not a control, it is a
// claim. A field reading customer_approved, notice_sent, or actor must
// grant no authority whatever.
var authorityFieldNames = []string{
	"Actor", "Approved", "Authorized", "Consent", "NoticeSent", "OnBehalfOf", "Confirmed",
}

// ScanRequestFields returns every field on a *Request type under the
// given roots whose name denotes money or asserted authority.
func ScanRequestFields(repoRoot string, roots ...string) ([]RequestField, error) {
	var out []RequestField
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
			out = append(out, scanRequestTypes(fset, file, filepath.ToSlash(rel))...)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scanRequestTypes(fset *token.FileSet, file *ast.File, rel string) []RequestField {
	var out []RequestField

	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || !strings.HasSuffix(ts.Name.Name, "Request") {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok || st.Fields == nil {
			return true
		}
		for _, field := range st.Fields.List {
			for _, name := range field.Names {
				if !name.IsExported() {
					continue
				}
				goType := exprString(field.Type)
				// The declared type is checked as well as the field
				// name: Overrides []InfraPriceOverride is a caller
				// supplying prices, and only the type says so.
				if !denotesMoney(name.Name) && !denotesMoney(goType) && !denotesAuthority(name.Name) {
					continue
				}
				out = append(out, RequestField{
					File:   rel,
					Type:   ts.Name.Name,
					Field:  name.Name,
					GoType: goType,
					Line:   fset.Position(name.Pos()).Line,
				})
			}
		}
		return true
	})
	return out
}

func denotesMoney(name string) bool     { return containsAny(name, moneyFieldNames) }
func denotesAuthority(name string) bool { return containsAny(name, authorityFieldNames) }

func containsAny(name string, stems []string) bool {
	for _, stem := range stems {
		if strings.Contains(name, stem) {
			return true
		}
	}
	return false
}

func exprString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	case *ast.ArrayType:
		return "[]" + exprString(t.Elt)
	case *ast.SelectorExpr:
		return exprString(t.X) + "." + t.Sel.Name
	case *ast.MapType:
		return "map[" + exprString(t.Key) + "]" + exprString(t.Value)
	}
	return "?"
}
