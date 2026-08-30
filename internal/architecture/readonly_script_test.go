package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// mutatingVerb matches SQL that changes something, anywhere in a
// statement rather than only at the start of a line.
//
// Line-anchoring was the first attempt and it was too weak: a write
// indented under a CTE, or continuing a previous line, would pass. This
// runs over statements with comments already stripped, so the words
// appearing in the file's own prose — unavoidable in a file about
// deletions — do not match.
var mutatingVerb = regexp.MustCompile(
	`(?i)\b(INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|DROP\s+\w|ALTER\s+\w|TRUNCATE|CREATE\s+\w|GRANT\s+\w|REVOKE\s+\w|COPY\s+\w|SELECT\s+.*\bINTO\b)`)

// TestLegacyDropPreconditionsAreReadOnly keeps the production
// precondition script incapable of changing what it measures.
//
// scripts/legacy-drop-preconditions.sql is run by hand against
// production by someone deciding whether a deletion is safe. A script
// that could write is a script that could create the very "READY"
// answer it reports, and the standing rule in this workspace is that
// nothing writes to the production database by hand.
//
// Comments and \echo lines are stripped before matching, so the words
// appearing in the file's own prose — which are unavoidable, since it
// is a file about deletions — do not trip it.
func TestLegacyDropPreconditionsAreReadOnly(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "scripts", "legacy-drop-preconditions.sql")

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	var offending []string
	var selects int
	var statement strings.Builder

	for i, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, `\`) {
			continue
		}
		// Strip a trailing comment so prose after real SQL cannot hide
		// a verb from the check or add a false one.
		if idx := strings.Index(trimmed, "--"); idx >= 0 {
			trimmed = strings.TrimSpace(trimmed[:idx])
			if trimmed == "" {
				continue
			}
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
			selects++
		}
		statement.WriteString(" ")
		statement.WriteString(trimmed)

		if strings.HasSuffix(trimmed, ";") {
			if mutatingVerb.MatchString(statement.String()) {
				offending = append(offending, itoa(i+1)+": "+strings.TrimSpace(statement.String()))
			}
			statement.Reset()
		}
	}
	// A trailing statement with no semicolon is still a statement.
	if rest := strings.TrimSpace(statement.String()); rest != "" && mutatingVerb.MatchString(rest) {
		offending = append(offending, "unterminated: "+rest)
	}

	if len(offending) > 0 {
		t.Errorf("the precondition script contains %d mutating statement(s). It is run against "+
			"production to decide whether a deletion is safe, and a script that can write can "+
			"create the READY answer it reports:\n  %s",
			len(offending), strings.Join(offending, "\n  "))
	}

	// A file of nothing but comments would also contain no mutating
	// statements. This is what stops the check passing on an empty one.
	if selects == 0 {
		t.Error("the precondition script asks no questions at all")
	}
}
