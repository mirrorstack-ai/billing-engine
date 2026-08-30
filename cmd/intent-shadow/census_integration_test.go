//go:build integration

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// seededByMigrations names the census subjects whose tables the migrations
// populate, so a non-zero count there is the BASELINE and not evidence that
// the system has been used.
var seededByMigrations = map[string]bool{
	"metric_definitions":  true,
	"metric_model_prices": true,
}

// Every census question must actually run against the real schema.
//
// An integration test for the same reason the preconditions have one: these
// queries name real tables, and the only thing that catches a drifted name is
// executing them. A census is especially exposed to this — a wrong table name
// would surface in production as a failed invocation at the exact moment
// somebody is trying to decide whether it is safe to delete code.
func TestEveryCensusQuestionRunsAgainstTheRealSchema(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var rows []CensusRow
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			var err error
			rows, err = runCensus(ctx, tx)
			return err
		}))

	require.NotEmpty(t, rows, "the census asks nothing")

	seen := map[string]bool{}
	for _, r := range rows {
		require.NotEmpty(t, r.Subject, "a census row names no subject")
		require.NotEmpty(t, r.Detail, "a census row says nothing about what was counted")
		require.GreaterOrEqual(t, r.Total, int64(0), "%s reported a negative count", r.Subject)
		// A fresh migrated database has no billing HISTORY — with one
		// exception the census exists to make legible.
		//
		// TWO catalogs are SEEDED by migrations and are therefore non-empty
		// before anything has ever happened:
		//
		//   metric_definitions  — seeded by 017, 018, 019, 020, 045, 046, 051
		//   metric_model_prices — seeded by 018 (the AI per-model prices)
		//
		// This test found BOTH by asserting zero and failing, which is exactly
		// the misreading an operator makes in production: "the pricing tables
		// have rows, so the system has been used". Neither is an activity
		// signal. Only metric_version_prices is, because only it is written at
		// runtime (module-version publish) and never seeded.
		if seededByMigrations[r.Subject] {
			require.Positive(t, r.Total,
				"%s is seeded by migrations and must not be empty — if it is, the "+
					"seeds stopped applying and an empty metric_version_prices no "+
					"longer means what this census says it means", r.Subject)
			continue
		}
		require.Zero(t, r.Total, "%s reported rows on an empty database", r.Subject)
		require.False(t, seen[r.Subject], "duplicate census subject %q", r.Subject)
		seen[r.Subject] = true
	}

	// The subject that motivated the whole action. If this one ever stops
	// being asked, the census no longer answers the question it exists for.
	require.True(t, seen["metric_version_prices"],
		"the census does not count metric_version_prices — the empty table that "+
			"blocks the shadow reconciliation and the reason this action exists")
}

// 🔴 A census row must carry no customer data.
//
// Same rule as the preconditions: this travels back from production, and a
// subject or detail string that interpolated a row would put customer data
// somewhere the invoke permission does not bound. The queries are ungrouped
// aggregates over constant labels, so every string is a literal from the
// script — this asserts that stays true.
func TestCensusCarriesNoCustomerData(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var rows []CensusRow
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			var err error
			rows, err = runCensus(ctx, tx)
			return err
		}))

	for _, r := range rows {
		for _, field := range []string{r.Subject, r.Detail} {
			require.NotContains(t, field, "@", "a census string contains an email-like value: %q", field)
			require.False(t, strings.Contains(field, "cus_") || strings.Contains(field, "acct_"),
				"a census string contains a provider identifier: %q", field)
			// A UUID would mean a label was built from a row rather than
			// written as a literal in the script.
			require.NotRegexp(t,
				`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`,
				field, "a census string contains a UUID: %q", field)
		}
	}
}
