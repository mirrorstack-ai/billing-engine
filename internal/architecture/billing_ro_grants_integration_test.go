//go:build integration

package architecture

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 THE READ-ONLY ROLE'S PRIVILEGES, EXERCISED FOR THE FIRST TIME.
//
// Migration 058 grants the read-only ops role its SELECT set, and 064 revokes
// the INV-014 evidence outbox from it. Both are wrapped in
// "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro')", and
// nothing in this repository creates that role — it is minted by db-bootstrap
// from mirrorstack-infra's config.DbServices.
//
// So in production both took the ELSE branch, raised a NOTICE, exited 0, and
// were recorded as APPLIED while granting and revoking nothing. A migration is
// applied once, so neither can ever re-run: creating the role later does not
// fix it. Migration 068 re-issues both, in an order where the REVOKE follows
// the GRANT, and refuses rather than skipping if the role is absent.
//
// The test harness now mints the role before migrating, which is why this test
// can exist at all. Before that, the grant path had never been executed
// anywhere — not in production, and not here.
func TestTheReadOnlyRoleCanReadTheBooksAndNotTheEvidence(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	readable := []string{
		"ms_billing.invoices",
		"ms_billing.charge_intents",
		"ms_billing.billing_runs",
		"ms_billing.usage_aggregates",
	}
	for _, table := range readable {
		var ok bool
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT has_table_privilege('billing_ro', $1, 'SELECT')`, table).Scan(&ok))
		require.Truef(t, ok,
			"billing_ro cannot SELECT %s. The read-only ops path exists to answer questions "+
				"about production billing state; a role that cannot read the books answers none.",
			table)
	}

	// The outbox is the exception, and it must stay one.
	var evidenceReadable bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT')`,
	).Scan(&evidenceReadable))
	require.False(t, evidenceReadable,
		"billing_ro can SELECT ms_billing.evidence_records. 058's ALTER DEFAULT PRIVILEGES "+
			"makes every table in the schema readable by this role, so the INV-014 evidence "+
			"outbox is exposed unless a REVOKE takes it back — and re-issuing the grants "+
			"without re-issuing the revoke is exactly how that protection gets undone.")
}

// Read-only means read-only. A role that can write is not a safer way to read.
func TestTheReadOnlyRoleHoldsNoWritePrivilege(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	for _, priv := range []string{"INSERT", "UPDATE", "DELETE", "TRUNCATE"} {
		var ok bool
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT has_table_privilege('billing_ro', 'ms_billing.invoices', $1)`, priv).Scan(&ok))
		require.Falsef(t, ok, "billing_ro holds %s on ms_billing.invoices", priv)
	}

	// 058 deliberately does not grant sequence USAGE: "a read-only role that
	// can advance a sequence is not read-only." Asserted rather than trusted,
	// because a later blanket GRANT is exactly how it would come back.
	var seqs int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		  FROM information_schema.sequences s
		 WHERE s.sequence_schema = 'ms_billing'
		   AND has_sequence_privilege('billing_ro', s.sequence_schema||'.'||s.sequence_name, 'USAGE')`,
	).Scan(&seqs))
	require.Zero(t, seqs, "billing_ro can advance %d sequence(s) in ms_billing", seqs)
}

// A table added AFTER the grants must still be readable, or the ops path goes
// blind on exactly the newest data. That is what ALTER DEFAULT PRIVILEGES is
// for, and it is the half of 058 its own header calls easy to omit.
func TestATableCreatedAfterTheGrantsIsStillReadable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `CREATE TABLE ms_billing.zz_default_priv_probe (id int)`)
	require.NoError(t, err)

	var ok bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.zz_default_priv_probe', 'SELECT')`,
	).Scan(&ok))
	require.True(t, ok,
		"a table created after the grants is not readable by billing_ro, so ALTER DEFAULT "+
			"PRIVILEGES did not take. Every future table would need its own GRANT, and the "+
			"one nobody remembers is the one the ops path needs.")
}

// 🔴 THE PRODUCTION CASE: 058 AND 064 SKIPPED, AND 068 HAS TO REPAIR IT.
//
// The three tests above assert the END STATE, and in this harness that state is
// reached by migration 058 — the role exists before it runs, so it grants. That
// is NOT what happened in production, where the role did not exist, 058 and 064
// took their ELSE branches, and both were recorded as applied and can never run
// again.
//
// So the tests above cannot tell 068's work from 058's. Mutation testing showed
// it: deleting 068's ALTER DEFAULT PRIVILEGES left them all green, because
// 058's copy had already taken effect.
//
// This reproduces production instead. It strips the privileges back to what a
// skipped 058/064 leaves — nothing — and then applies 068 alone, which is
// exactly the repair the production database needs. Every assertion here is
// therefore attributable to 068 and to nothing else.
func TestMigration068RepairsASkipped058(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	// Back to the state a skipped 058 leaves: no schema usage, no table
	// selects, and no default privilege for future tables.
	for _, stmt := range []string{
		`ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro`,
		`REVOKE ALL ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro`,
		`REVOKE ALL ON SCHEMA ms_billing FROM billing_ro`,
	} {
		_, err := pool.Exec(ctx, stmt)
		require.NoErrorf(t, err, "could not reset privileges with %q", stmt)
	}

	var before bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.invoices', 'SELECT')`).Scan(&before))
	require.False(t, before, "the reset did not take, so this test would prove nothing")

	// Now the repair, and only the repair.
	root, err := repoRootForTest()
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join(root, "migrations", "billing",
		"068_billing_ro_grants_reissued.up.sql"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(body))
	require.NoError(t, err, "migration 068 failed against a database in the production shape")

	// Reading the books.
	var invoices bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.invoices', 'SELECT')`).Scan(&invoices))
	require.True(t, invoices, "068 did not restore SELECT; the ops path is still blind")

	// Not the evidence outbox. This is the ordering assertion: 068 grants ON
	// ALL TABLES first, which covers evidence_records because it already
	// exists, and the REVOKE after it is what takes it back.
	var evidence bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT')`).Scan(&evidence))
	require.False(t, evidence,
		"068 left the INV-014 evidence outbox readable. Its GRANT ON ALL TABLES covers an "+
			"existing table, so without the REVOKE after it — or with the two reversed — "+
			"repairing the grants silently undoes the outbox protection.")

	// And the default-privileges half, which only 068 can be responsible for
	// now that the reset removed 058's.
	_, err = pool.Exec(ctx, `CREATE TABLE ms_billing.zz_repair_probe (id int)`)
	require.NoError(t, err)
	var future bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.zz_repair_probe', 'SELECT')`).Scan(&future))
	require.True(t, future,
		"a table created after 068 is not readable, so 068 omitted ALTER DEFAULT PRIVILEGES. "+
			"Every future table would then need its own GRANT, and the one nobody remembers "+
			"is the one the ops path needs.")
}

// 🔴 068 must REFUSE when the role is absent, not skip.
//
// Skipping is what 058 did, and the cost was a migration recorded as applied
// that granted nothing and can never re-run. A migration that cannot do its job
// must stop the deploy so the ordering against role creation is explicit.
func TestMigration068RefusesWhenTheRoleIsMissing(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		REVOKE ALL ON ALL TABLES IN SCHEMA ms_billing FROM billing_ro;
		ALTER DEFAULT PRIVILEGES IN SCHEMA ms_billing REVOKE SELECT ON TABLES FROM billing_ro;
		REVOKE ALL ON SCHEMA ms_billing FROM billing_ro;
		DROP ROLE billing_ro;`)
	require.NoError(t, err, "could not remove the role to reproduce the pre-bootstrap state")

	root, err := repoRootForTest()
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join(root, "migrations", "billing",
		"068_billing_ro_grants_reissued.up.sql"))
	require.NoError(t, err)

	_, err = pool.Exec(ctx, string(body))
	require.Error(t, err,
		"068 succeeded with no billing_ro role. It must RAISE: skipping is what 058 did, and "+
			"a skipped migration is recorded as applied and never runs again.")
	require.Contains(t, err.Error(), "billing_ro",
		"the failure does not name the missing role, so an operator cannot act on it")
}

func repoRootForTest() (string, error) { return RepoRoot() }
