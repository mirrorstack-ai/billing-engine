package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/scripts"
)

// Precondition is one legacy-drop question and production's answer.
//
// Counts and verdicts only. The queries in scripts/legacy-drop-preconditions.sql
// were written to return a label, a count and a verdict — never a customer row —
// so this type can carry the whole answer without carrying anybody's data.
type Precondition struct {
	Deletion     string `json:"deletion"`
	BlockingRows int64  `json:"blocking_rows"`
	Verdict      string `json:"verdict"`
	What         string `json:"what"`
}

// Blocked reports whether this deletion must not proceed.
func (p Precondition) Blocked() bool { return p.Verdict != "READY" }

// statementSplit finds the boundaries between the script's SELECTs.
//
// The script is a sequence of top-level statements with no dollar-quoting and
// no semicolons inside literals — asserted by the read-only test in
// internal/architecture, which is also what keeps this splitter honest.
var statementSplit = regexp.MustCompile(`;\s*(?:\n|$)`)

// preconditionQueries returns the script's statements.
//
// EMBEDDED, not read from disk. Under Lambda the working directory is
// /var/task and a relative path does not resolve; embedding also guarantees
// that what runs against production is what a reviewer reads in the
// repository, with no second copy to drift.
func preconditionQueries() ([]string, error) {
	var out []string
	for _, stmt := range statementSplit.Split(scripts.LegacyDropPreconditions, -1) {
		if sql := sqlOnly(stmt); sql != "" {
			out = append(out, sql)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the embedded preconditions script contains no statements")
	}
	return out, nil
}

// sqlOnly strips what psql understands and a database driver does not:
// comments, blank lines, and backslash meta-commands like \pset and \echo.
//
// The script is meant to be readable AND runnable by an operator with psql, so
// it carries formatting directives. Sending one to the server is a syntax
// error, and stripping them here is what lets the one file serve both.
func sqlOnly(stmt string) string {
	var kept []string
	for _, line := range strings.Split(stmt, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") || strings.HasPrefix(trimmed, `\`) {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// runPreconditions asks production the seven legacy-drop questions.
//
// Every statement runs inside the caller's read-only transaction. It is passed
// in rather than opened here so there is exactly one place that decides how
// this tool talks to a database, and so a future edit cannot quietly acquire a
// writable handle.
func runPreconditions(ctx context.Context, tx pgx.Tx) ([]Precondition, error) {
	queries, err := preconditionQueries()
	if err != nil {
		return nil, err
	}

	var out []Precondition
	for i, q := range queries {
		rows, err := tx.Query(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("precondition %d: %w", i+1, err)
		}
		found := 0
		for rows.Next() {
			var p Precondition
			if err := rows.Scan(&p.Deletion, &p.BlockingRows, &p.Verdict, &p.What); err != nil {
				rows.Close()
				return nil, fmt.Errorf("precondition %d: scan: %w", i+1, err)
			}
			out = append(out, p)
			found++
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("precondition %d: %w", i+1, err)
		}
		rows.Close()

		// Every statement is an ungrouped aggregate, so exactly one row is
		// the only correct answer. Zero means the query stopped matching the
		// schema and would otherwise read as "nothing is blocking" — a
		// confident green produced by a broken question.
		if found != 1 {
			return nil, fmt.Errorf(
				"precondition %d returned %d rows, want exactly 1 — the query no longer "+
					"matches the schema, and a missing answer must not read as READY", i+1, found)
		}
	}
	return out, nil
}

// String renders the answers for a human deciding whether to delete.
func preconditionReport(ps []Precondition) string {
	var b strings.Builder
	blocked := 0
	b.WriteString("legacy-drop preconditions\n")
	for _, p := range ps {
		if p.Blocked() {
			blocked++
		}
		b.WriteString(fmt.Sprintf("  [%-7s] %-46s %6d  %s\n",
			p.Verdict, p.Deletion, p.BlockingRows, p.What))
	}
	b.WriteString(fmt.Sprintf("\n  %d of %d ready to delete\n", len(ps)-blocked, len(ps)))
	if blocked > 0 {
		b.WriteString("\n  A BLOCKED deletion owns in-flight state. Deleting its code strands a\n" +
			"  charge nobody can finish or prove. Wait for it to drain.\n")
	}
	return b.String()
}
