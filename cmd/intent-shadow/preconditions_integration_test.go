//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// The seven questions must actually run against the real schema.
//
// An integration test rather than a unit test on purpose: these queries name
// real tables and columns, and the only thing that catches a drifted name is
// executing them. Three wrong table/column names and one impossible status
// value were caught exactly this way when the script was written.
func TestEveryPreconditionRunsAgainstTheRealSchema(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var answers []Precondition
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			var err error
			answers, err = runPreconditions(ctx, tx)
			return err
		}))

	require.Len(t, answers, 7, "the script asks seven questions, one per deletion")

	for _, a := range answers {
		require.NotEmpty(t, a.Deletion, "an answer names no deletion")
		require.NotEmpty(t, a.What, "an answer says nothing about what blocks")
		require.Contains(t, []string{"READY", "BLOCKED", "REVIEW"}, a.Verdict,
			"unexpected verdict %q for %s", a.Verdict, a.Deletion)
		// An empty database has nothing in flight.
		require.Zero(t, a.BlockingRows, "%s reported rows on an empty database", a.Deletion)
	}
}

// 🔴 The answers must carry no customer data.
//
// Under Lambda these can reach a log, and a log is a wider audience than the
// invoke permission implies. The queries were written to return counts and
// verdicts; this asserts the Go side did not widen that.
func TestPreconditionAnswersCarryNoCustomerData(t *testing.T) {
	pool := testutil.NewTestDB(t)

	var answers []Precondition
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			var err error
			answers, err = runPreconditions(ctx, tx)
			return err
		}))

	report := preconditionReport(answers)
	// A UUID is the shape of every account, app and timer id in this schema.
	require.NotRegexp(t, `[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`, report,
		"an identifier reached the precondition report")
	require.NotContains(t, report, "@", "an email address reached the precondition report")
}

// The queries come from the file, not a second copy in Go. A duplicate would
// drift, and the copy that drifted would be the one nobody ran.
func TestThePreconditionsComeFromTheScriptFile(t *testing.T) {
	queries, err := preconditionQueries()
	require.NoError(t, err)
	require.Len(t, queries, 7)
	for _, q := range queries {
		require.Contains(t, q, "SELECT")
	}
}
