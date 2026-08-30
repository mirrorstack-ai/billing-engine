package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/scripts"
)

// CensusRow is one measurement of how much billing history production holds.
//
// Aggregates only, for the same reason as Precondition: this runs against
// production and its answers get pasted into places the invoke permission does
// not reach. A subject, a count, and a sentence saying what was counted.
type CensusRow struct {
	Subject string `json:"subject"`
	Total   int64  `json:"total"`
	Detail  string `json:"detail"`
}

// censusQueries returns the embedded census script's statements.
//
// Shares sqlOnly and statementSplit with the preconditions reader, so both
// scripts are parsed by exactly one splitter. A second parser is a second
// thing that can disagree with psql about where a statement ends.
func censusQueries() ([]string, error) {
	var out []string
	for _, stmt := range statementSplit.Split(scripts.BillingCensus, -1) {
		if sql := sqlOnly(stmt); sql != "" {
			out = append(out, sql)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("the embedded census script contains no statements")
	}
	return out, nil
}

// runCensus asks production how much billing history it holds.
//
// This exists because the shadow reconciliation refuses to run when the price
// catalog is empty, and an empty catalog has two opposite causes that look
// identical from outside: the rail never carried traffic (so DESIGN §11's gate
// is vacuous), or history exists and the diagnostic cannot see it (so the gate
// is genuinely unmet). Deciding between them by inference is the reasoning
// that let migration 058 look applied.
//
// Runs inside the caller's read-only transaction, passed in for the same
// reason runPreconditions takes one: exactly one place decides how this tool
// talks to a database.
func runCensus(ctx context.Context, tx pgx.Tx) ([]CensusRow, error) {
	queries, err := censusQueries()
	if err != nil {
		return nil, err
	}

	var out []CensusRow
	for i, q := range queries {
		rows, err := tx.Query(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("census %d: %w", i+1, err)
		}
		found := 0
		for rows.Next() {
			var c CensusRow
			if err := rows.Scan(&c.Subject, &c.Total, &c.Detail); err != nil {
				rows.Close()
				return nil, fmt.Errorf("census %d: scan: %w", i+1, err)
			}
			out = append(out, c)
			found++
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("census %d: %w", i+1, err)
		}
		rows.Close()

		// Same rule as the preconditions: every statement is an ungrouped
		// aggregate, so exactly one row is the only correct answer. A query
		// that stopped matching the schema must not read as "zero rows in
		// that table" — that is the difference between "nothing happened"
		// and "the question broke", and they imply opposite decisions.
		if found != 1 {
			return nil, fmt.Errorf(
				"census %d returned %d rows, want exactly 1 — the query no longer matches "+
					"the schema, and a missing answer must not read as an empty table", i+1, found)
		}
	}
	return out, nil
}
