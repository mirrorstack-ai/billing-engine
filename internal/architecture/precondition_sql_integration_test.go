//go:build integration

package architecture

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TestPreconditionQueriesRunAgainstTheRealSchema executes every query
// in the legacy-drop precondition script against a database with all
// migrations applied.
//
// The script is read by hand, in production, at the moment someone is
// deciding whether deleting a money path is safe. A column name that
// drifted since it was written turns that moment into a debugging
// session. Worse, the names in it came from a plan rather than from the
// schema, and two of them were wrong on the first pass — the tables are
// app_module_overage_timers and app_combined_proration_attempts, not
// module_timers and combined_proration_attempts.
//
// So the queries are executed rather than eyeballed. The row counts do
// not matter here: an empty test database answers zero to all of them.
// What is being checked is that each query is valid SQL against the
// real schema.
func TestPreconditionQueriesRunAgainstTheRealSchema(t *testing.T) {
	pool := testutil.NewTestDB(t)

	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, "scripts", "legacy-drop-preconditions.sql"))
	if err != nil {
		t.Fatalf("read script: %v", err)
	}

	queries := selectStatements(string(body))
	if len(queries) == 0 {
		t.Fatal("no SELECT statements found; the parser is broken or the script is empty")
	}

	for _, q := range queries {
		rows, err := pool.Query(context.Background(), q)
		if err != nil {
			t.Errorf("query failed against the real schema:\n%s\n  %v", q, err)
			continue
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Errorf("query errored while reading:\n%s\n  %v", q, err)
		}
	}

	// Every deletion in the script must be represented. If a query is
	// dropped, the operator loses a check without being told.
	const wantQueries = 7
	if len(queries) != wantQueries {
		t.Errorf("the script runs %d precondition queries, expected %d. "+
			"A removed query is a deletion nobody is checking.", len(queries), wantQueries)
	}
}

// selectStatements splits the script into executable SELECTs, dropping
// comments and psql meta-commands (\pset, \echo), which pgx cannot run.
func selectStatements(body string) []string {
	var out []string
	var current strings.Builder

	for _, line := range strings.Split(body, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, `\`) {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			out = append(out, strings.TrimSpace(current.String()))
			current.Reset()
		}
	}
	return out
}
