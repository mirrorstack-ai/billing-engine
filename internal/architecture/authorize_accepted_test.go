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

// 🔴 intent.AuthorizeAccepted must have NO non-test caller.
//
// Authorize requires the acceptance digest to ARRIVE and compares it against
// the document the grant constitutes. That comparison is the whole control
// added by §12 item 16 option C's second piece: a caller that showed a
// customer one set of ceilings cannot mint another.
//
// AuthorizeAccepted computes the digest from the grant instead, which is
// correct only when the same code both rendered the terms and is minting the
// authorization — true in a test, never true in production, where the terms
// are shown by api-platform and relayed back.
//
// So a production caller would be a hole straight through the check it sits
// beside: the engine would agree with itself about any terms at all. Nothing
// in the type system prevents it, which is why this scan exists.
//
// The repository already has this shape for predicate.Evaluate
// (predicate_single_caller.go), and for the same reason: a rule enforced in
// one place stays enforced only while nothing else can route around it.
func TestAuthorizeAcceptedHasNoProductionCaller(t *testing.T) {
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
			// Its own declaration is not a call.
			if strings.HasSuffix(path, filepath.Join("internal", "intent", "authorization.go")) {
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
				var name string
				switch fn := call.Fun.(type) {
				case *ast.SelectorExpr:
					name = fn.Sel.Name
				case *ast.Ident:
					name = fn.Name
				}
				if name == "AuthorizeAccepted" {
					rel, _ := filepath.Rel(root, path)
					offenders = append(offenders, rel+":"+
						fset.Position(call.Pos()).String()[len(fset.Position(call.Pos()).Filename)+1:])
				}
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

	if len(offenders) > 0 {
		t.Errorf("intent.AuthorizeAccepted is called from %d non-test file(s):\n  %s\n\n"+
			"That function fills in the acceptance digest from the grant itself, so the "+
			"engine ends up agreeing with itself about whatever terms it was handed. In "+
			"production the digest must arrive from whoever showed the customer the "+
			"document, and Authorize must compare it. Use Authorize.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}
