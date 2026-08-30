package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// mutatingStatement matches SQL that changes something. Anchored to the
// start of a statement so that the words appearing in prose do not.
var mutatingStatement = regexp.MustCompile(
	`(?i)^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|COPY)\b`)

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
	for i, line := range strings.Split(string(body), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, `\`) {
			continue
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") {
			selects++
		}
		if mutatingStatement.MatchString(line) {
			offending = append(offending, itoa(i+1)+": "+trimmed)
		}
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
