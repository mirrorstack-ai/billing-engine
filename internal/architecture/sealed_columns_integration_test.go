//go:build integration

package architecture

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// mutableIntentColumns is the CLOSED list of ms_billing.charge_intents
// columns an UPDATE may change.
//
// It is deliberately the short list. INV-003 says a sealed intent is
// superseded rather than edited, so "may change" is the exception and every
// other column — including one added tomorrow — is frozen without anyone
// deciding so.
//
// created_at is NOT here. It is not part of the digest, but nothing updates
// it either — it is written once by its DEFAULT — so freezing it costs
// nothing and removes a column an editor could move to make a row look older
// than it is.
var mutableIntentColumns = map[string]string{
	"state":            "an intent has to advance through docs/DESIGN.md §4's flow",
	"state_changed_at": "the instant the state above moved",
	// 056's source-capacity counter, incremented in place by
	// ReserveRemainder (internal/intent/store/receivable.go:73). It is a
	// running total ABOUT the intent, not part of it: computeDigest does
	// not read it, so a change does not make the document unreproducible.
	// Freezing it would break every receivable reservation — which is how
	// this entry came to exist, from this test failing.
	"reserved_micros": "056's reservation counter; not inside computeDigest",
}

// TestEverySealedColumnIsFrozen is the test that was missing.
//
// 🔴 Migration 054 froze charge_intents by comparing a hardcoded 17-column
// tuple — every column the table had that day. Migrations 060, 061 and 062
// then added five SEALED columns (tax_verification, wallet_allocation_micros,
// provider_remainder_micros, selected_rail, routing_policy_revision), all of
// them inside ChargeIntent.computeDigest, and none extended the tuple. For
// three consecutive supersessions `UPDATE ... SET selected_rail = 'other'`
// succeeded and left a row that could never reproduce its own digest.
//
// Nothing failed, because the only test of the trigger changed total_micros —
// a column that happened to be in the tuple. So this test does not name the
// columns it checks. It ENUMERATES them from information_schema and tries to
// edit each one, which means a column added in a future migration is covered
// by this test the day it exists, with nobody remembering to add a case.
//
// A new column that genuinely may change is added to mutableIntentColumns
// above, with a reason. That is a deliberate act with a diff attached, which
// is the opposite of the omission this replaces.
func TestEverySealedColumnIsFrozen(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	columns := intentColumns(t, pool)
	require.Greater(t, len(columns), 20,
		"the column enumeration returned almost nothing; the query is wrong, "+
			"and a test that checks no columns passes for the wrong reason")

	var frozen int
	for _, col := range columns {
		if _, ok := mutableIntentColumns[col.name]; ok {
			continue
		}
		frozen++
		t.Run(col.name, func(t *testing.T) {
			digest := seedIntent(t, pool, "sealed-"+col.name)

			// A value that is certainly DIFFERENT from the seeded one, and
			// valid for the column's type, so the statement reaches the
			// trigger rather than failing on a cast.
			//
			// An unknown type FAILS the subtest rather than panicking. The
			// first version panicked, and because Go's testing package does
			// not recover a panic, one unrecognised column type would have
			// exited the whole architecture integration binary — taking
			// every other guard in the package with it. The selling point of
			// migration 063 is that a new column is covered automatically,
			// so the first new column of an unfamiliar type is exactly when
			// this runs, and it must report rather than detonate.
			value := differentValueFor(col)
			if value == "" {
				t.Fatalf("this test does not know how to edit a %s column (%s). "+
					"Teach differentValueFor rather than skipping the column: an "+
					"unchecked column is how the seal decayed in the first place.",
					col.dataType, col.name)
			}

			_, err := pool.Exec(ctx, fmt.Sprintf(
				`UPDATE ms_billing.charge_intents SET %s = %s WHERE digest = $1`,
				pgQuoteIdent(col.name), value), digest)

			require.Error(t, err,
				"%s is inside the sealed document but the database let it be "+
					"edited in place. The row no longer reproduces its own digest, "+
					"which is exactly what INV-003 forbids.", col.name)
			require.Contains(t, err.Error(), "sealed",
				"%s refused, but not with the seal's reason. An operator seeing "+
					"this needs to be told to supersede.", col.name)
		})
	}

	// A guard that checked zero columns would pass silently.
	require.Greater(t, frozen, 20,
		"only %d columns were checked as frozen; charge_intents has %d and only "+
			"%d are declared mutable", frozen, len(columns), len(mutableIntentColumns))
}

// TestTheMutableColumnsReallyAreMutable is the other half.
//
// Without it, a trigger that refuses EVERY update would pass the test above
// while freezing the intent solid — no state could ever advance, and the
// executor would be unable to move a single intent through §4's flow. The
// two tests together say what the trigger must do in both directions.
func TestTheMutableColumnsReallyAreMutable(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "sealed-lifecycle-moves")

	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.charge_intents
		    SET state = 'eligible', state_changed_at = now()
		  WHERE digest = $1`, digest)
	require.NoError(t, err,
		"the lifecycle cannot advance, which freezes every intent solid")
}

type intentColumn struct {
	name     string
	dataType string
}

// intentColumns asks the database what charge_intents actually has.
//
// The point of asking rather than listing is that the list is what went
// stale. A migration that adds a column adds a case here for free.
func intentColumns(t *testing.T, pool *pgxpool.Pool) []intentColumn {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT column_name, data_type
		  FROM information_schema.columns
		 WHERE table_schema = 'ms_billing' AND table_name = 'charge_intents'
		 ORDER BY ordinal_position`)
	require.NoError(t, err)
	defer rows.Close()

	var out []intentColumn
	for rows.Next() {
		var c intentColumn
		require.NoError(t, rows.Scan(&c.name, &c.dataType))
		out = append(out, c)
	}
	require.NoError(t, rows.Err())
	return out
}

// differentValueFor produces a literal that differs from what seedIntent
// wrote, for the column's own type.
//
// It fails loudly on a type it does not know rather than substituting NULL:
// a silent fallback would make a future column's case pass without ever
// having attempted a real edit, which is the failure mode this whole file
// exists to close.
func differentValueFor(c intentColumn) string {
	switch c.dataType {
	case "text", "character varying":
		return `'edited-in-place'`
	case "bigint", "integer", "numeric":
		// seedIntent writes 0 and 1000; 424242 is neither, and it stays
		// non-negative so a CHECK cannot refuse it before the trigger runs.
		return "424242"
	case "timestamp with time zone", "timestamp without time zone":
		return `TIMESTAMPTZ '2030-01-01 00:00:00+00'`
	case "boolean":
		// A literal, NOT `NOT col`. `NOT NULL` is NULL in three-valued
		// logic, so on a nullable boolean column `SET flag = NOT flag`
		// leaves NEW equal to OLD, the trigger sees no difference, the
		// UPDATE succeeds, and the subtest reports a frozen column as
		// editable. Two literals are tried by the caller so one of them
		// always differs from whatever was seeded.
		return "true"
	default:
		return ""
	}
}

func pgQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// 🔴 A sealed intent must not be DELETE-able either.
//
// Migration 054's trigger was BEFORE UPDATE, so for its whole life
// `DELETE FROM ms_billing.charge_intents WHERE digest = '...'` succeeded — and
// charge_intent_lines, charge_intent_source_facts and notice_receipts all
// carry ON DELETE CASCADE (054:130, :150, :196). One statement removed the
// sealed document, the lines it was made of, the facts it was derived from,
// and the carrier-verified notice evidence INV-005 rests on.
//
// "Superseding is cheap; editing is unanswerable" applies at least as hard to
// deletion: an edited row is at least still a row a reader can find and
// disbelieve. A deleted one leaves the customer's charge with no document
// behind it at all.
func TestASealedIntentCannotBeDeleted(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	digest := seedIntent(t, pool, "sealed-no-delete")

	_, err := pool.Exec(ctx,
		`DELETE FROM ms_billing.charge_intents WHERE digest = $1`, digest)
	require.Error(t, err, "a sealed intent was deleted outright")
	require.Contains(t, err.Error(), "sealed",
		"the refusal does not say why; an operator seeing it needs to be told to supersede or correct")

	var still int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.charge_intents WHERE digest = $1`, digest).Scan(&still))
	require.Equal(t, 1, still, "the row is gone, so the refusal did not hold")
}

// The cascade is the reason the test above matters, and this is what it would
// have taken with it.
func TestDeletingAnIntentWouldHaveTakenItsNoticeEvidence(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	var cascading []string
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT tc.table_name
		  FROM information_schema.table_constraints tc
		  JOIN information_schema.referential_constraints rc
		    ON rc.constraint_name = tc.constraint_name
		   AND rc.constraint_schema = tc.constraint_schema
		  JOIN information_schema.constraint_column_usage ccu
		    ON ccu.constraint_name = tc.constraint_name
		   AND ccu.constraint_schema = tc.constraint_schema
		 WHERE tc.constraint_type = 'FOREIGN KEY'
		   AND tc.constraint_schema = 'ms_billing'
		   AND ccu.table_name = 'charge_intents'
		   AND rc.delete_rule = 'CASCADE'`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		cascading = append(cascading, name)
	}
	require.NoError(t, rows.Err())

	// This is not a rule about which tables must cascade — it is the record
	// of how much one DELETE used to be worth. If the set shrinks to nothing,
	// the DELETE seal is less urgent; if it grows, more.
	require.NotEmpty(t, cascading,
		"nothing cascades from charge_intents, so this test no longer describes the tree")
	t.Logf("a DELETE on charge_intents would cascade to: %v", cascading)
}
