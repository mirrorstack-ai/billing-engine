//go:build integration

package main

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// grantsVia runs the diagnostic AS billing_ro.
//
// 🔴 SET ROLE, and it is not decoration. In production this Lambda CONNECTS as
// billing_ro; the test pool connects as the owner, which holds everything. A
// test that revoked billing_ro's privileges and then queried as the owner would
// find every table readable and prove nothing — and mutation testing caught
// exactly that: adding a real SELECT to runGrants survived, because the owner
// could still do it.
//
// SET ROLE makes the session subject to billing_ro's privileges for the rest of
// the transaction, which is the closest this harness gets to how the function
// actually runs.
func grantsVia(t *testing.T, pool *pgxpool.Pool) []GrantRow {
	t.Helper()
	var out []GrantRow
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SET LOCAL ROLE billing_ro`); err != nil {
				return err
			}
			rows, err := runGrants(ctx, tx)
			out = rows
			return err
		}))
	return out
}

// grantsAsOwner is for the one case where billing_ro does not exist, so SET
// ROLE to it is impossible.
func grantsAsOwner(t *testing.T, pool *pgxpool.Pool) []GrantRow {
	t.Helper()
	var out []GrantRow
	require.NoError(t, withReadOnlyTx(context.Background(), pool,
		func(ctx context.Context, tx pgx.Tx) error {
			rows, err := runGrants(ctx, tx)
			out = rows
			return err
		}))
	return out
}

func rowFor(t *testing.T, rows []GrantRow, check string) GrantRow {
	t.Helper()
	for _, r := range rows {
		if r.Check == check {
			return r
		}
	}
	t.Fatalf("no answer for %q; the diagnostic does not cover what it claims to", check)
	return GrantRow{}
}

// On a correctly-migrated database every answer is OK.
func TestTheGrantsDiagnosticPassesOnAHealthyDatabase(t *testing.T) {
	pool := testutil.NewTestDB(t)
	rows := grantsVia(t, pool)
	require.NotEmpty(t, rows)

	for _, r := range rows {
		require.Truef(t, r.OK, "%s = %s, want %s", r.Check, r.Actual, r.Want)
	}
	require.True(t, rowFor(t, rows, "role exists").OK)
	require.False(t, rowFor(t, rows, "can SELECT ms_billing.evidence_records").Actual == "yes",
		"the evidence outbox is readable by the ops role")
}

// 🔴 THE STATE IT EXISTS FOR: the role is present and holds NOTHING.
//
// This is what production looked like — 058 and 064 applied before the role was
// created, both gated, both recorded applied having done nothing. Every other
// action of this Lambda fails on its first SELECT with permission denied. This
// one has to return a real answer from exactly that state, because it is the
// only thing that can tell an operator WHY.
func TestTheGrantsDiagnosticAnswersWhenThePrivilegesAreMissing(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	for _, stmt := range []string{
		`ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro`,
		`REVOKE ALL ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro`,
		`REVOKE ALL ON SCHEMA ms_billing FROM billing_ro`,
	} {
		_, err := pool.Exec(ctx, stmt)
		require.NoErrorf(t, err, "could not strip privileges with %q", stmt)
	}

	rows := grantsVia(t, pool)

	// It answered at all — that is the property under test.
	require.NotEmpty(t, rows, "the diagnostic returned nothing from the state it exists for")
	require.True(t, rowFor(t, rows, "role exists").OK,
		"the role is gone; this test is reproducing the wrong failure")

	sel := rowFor(t, rows, "can SELECT ms_billing.invoices")
	require.False(t, sel.OK, "the diagnostic reported readable while the grants were stripped")
	require.Equal(t, "no", sel.Actual)

	// And it must not mistake "cannot read anything" for "correctly revoked".
	// The evidence row is OK here for the wrong reason, which is why the
	// invoices row above is what distinguishes the two states.
	require.True(t, rowFor(t, rows, "can SELECT ms_billing.evidence_records").OK)
}

// A missing role must be reported, not crashed on, and nothing after it should
// be invented.
func TestTheGrantsDiagnosticReportsAMissingRole(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		REVOKE ALL ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro;
		ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro;
		REVOKE ALL ON SCHEMA ms_billing FROM billing_ro;
		DROP ROLE billing_ro;`)
	require.NoError(t, err)

	rows := grantsAsOwner(t, pool)
	require.Len(t, rows, 1,
		"with no role there is exactly one answerable question; anything more is invented")
	require.Equal(t, "role exists", rows[0].Check)
	require.False(t, rows[0].OK)
	require.Equal(t, "no", rows[0].Actual)
}

// The diagnostic must not itself need the privileges it is reporting on — it
// reads the catalog, never a billing table. Proven by the test above passing
// with every table privilege revoked; asserted here as an explicit statement so
// a future edit that adds a real query to runGrants fails loudly.
func TestTheGrantsDiagnosticReadsNoBillingTable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `REVOKE ALL ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro`)
	require.NoError(t, err)

	require.NotPanics(t, func() { grantsVia(t, pool) })
	rows := grantsVia(t, pool)
	require.NotEmpty(t, rows,
		"runGrants now depends on a table privilege it is meant to be diagnosing; it must "+
			"answer from the catalog alone or it is useless in the state that needs it")
}

// 🔴 The outbox check must be able to FAIL.
//
// On a correctly-migrated database evidence_records is already unreadable, so
// every assertion about it passes whether the check works or not — mutation
// testing showed that hardcoding its OK to true survived the whole suite. This
// grants the table back and requires the diagnostic to report it.
//
// That is the state migration 068 exists to prevent: re-issuing 058's grants
// without 064's revoke leaves the INV-014 evidence outbox readable by the ops
// role, and this is the check that would notice.
func TestTheGrantsDiagnosticFailsWhenTheOutboxIsReadable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `GRANT SELECT ON ms_billing.evidence_records TO billing_ro`)
	require.NoError(t, err)

	rows := grantsVia(t, pool)
	row := rowFor(t, rows, "can SELECT ms_billing.evidence_records")
	require.False(t, row.OK,
		"the diagnostic reported the evidence outbox as fine while billing_ro could read it")
	require.Equal(t, "yes", row.Actual)

	// And the overall verdict must follow the individual answer, or an
	// operator reading only "ok" would miss it.
	var anyBad bool
	for _, r := range rows {
		if !r.OK {
			anyBad = true
		}
	}
	require.True(t, anyBad)
}
