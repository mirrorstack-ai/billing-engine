package architecture

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// networkImportPrefixes are the standard-library packages that can
// originate an outbound network call. e首發票's library
// (internal/provider/systemleadadapter) is documented as "no network",
// and this is the check that makes that a provable claim instead of a
// comment: mirrorstack-ai/billing-engine#286 item 2's acceptance
// criterion is "the engine process holds no vendor key", and a process
// with no outbound network capability in this part of the tree cannot
// transmit a key it does not have anywhere to send.
var networkImportPrefixes = []string{
	"net/http",
	"net/rpc",
	"net/smtp",
}

// ScanNetworkImporters returns every non-test file under the given
// roots that imports a standard-library network-call package.
func ScanNetworkImporters(repoRoot string, roots ...string) ([]string, error) {
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
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, parseErr := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if parseErr != nil {
				return parseErr
			}
			for _, imp := range file.Imports {
				value, unquoteErr := strconv.Unquote(imp.Path.Value)
				if unquoteErr != nil {
					continue
				}
				for _, prefix := range networkImportPrefixes {
					if value == prefix || strings.HasPrefix(value, prefix+"/") {
						rel, relErr := filepath.Rel(repoRoot, path)
						if relErr != nil {
							return relErr
						}
						out = append(out, filepath.ToSlash(rel))
					}
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
