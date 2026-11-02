package main

import (
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

var floatPattern = regexp.MustCompile(`\bfloat(32|64)\b`)
var sqlWritePattern = regexp.MustCompile(`(?i)\b(insert\s+into|update\s+\S+\s+set|delete\s+from)\b`)
var readWritePattern = regexp.MustCompile(`pgx\.ReadWrite`)
var readOnlyPattern = regexp.MustCompile(`pgx\.ReadOnly`)

func thisDir(t *testing.T) string {
	t.Helper()
	root, err := architecture.RepoRoot()
	if err != nil {
		t.Fatalf("RepoRoot: %v", err)
	}
	return filepath.Join(root, "cmd", "statement-lines")
}

func goFiles(t *testing.T) []string {
	t.Helper()
	dir := thisDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var files []string
	for _, e := range entries {
		// grep_test.go names what it looks for in its own error messages, so
		// it is excluded from its own scan rather than made to self-match.
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".go") && e.Name() != "grep_test.go" {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	return files
}

func TestCommand_NoFloatAnywhere(t *testing.T) {
	for _, path := range goFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if floatPattern.Match(data) {
			t.Errorf("%s: contains float32/float64 — this command must stay integer-only", path)
		}
	}
}

func TestCommand_NoProviderClientImport(t *testing.T) {
	fset := token.NewFileSet()
	for _, path := range goFiles(t) {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			value, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			if strings.Contains(value, "stripe") {
				t.Errorf("%s: imports provider SDK path %q — this command must have no provider client", path, value)
			}
		}
	}
}

func TestCommand_NoWriteTransactionOrRawSQLWrite(t *testing.T) {
	sawReadOnly := false
	for _, path := range goFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if readWritePattern.Match(data) {
			t.Errorf("%s: contains pgx.ReadWrite — this command opens only read-only transactions", path)
		}
		if sqlWritePattern.Match(data) {
			t.Errorf("%s: contains a raw SQL write verb (INSERT/UPDATE/DELETE)", path)
		}
		if readOnlyPattern.Match(data) {
			sawReadOnly = true
		}
	}
	if !sawReadOnly {
		t.Error("no file opens a pgx.ReadOnly transaction — expected withReadOnlyTx's BeginTx call")
	}
}
