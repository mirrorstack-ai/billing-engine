//go:build integration

package architecture

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// The intent schema's controls are constraints and a trigger, which means
// they hold against a concurrent executor, a replayed message, a second
// deployment and a hand-run statement alike — in a way a code path does not.
//
// A constraint nobody tries to violate is indistinguishable from one that
// was never created, so each is violated here.

func TestSealedIntentCannotBeEdited(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "digest-immutable")

	// INV-003: a one-unit change creates a NEW intent that supersedes the
	// old one. Editing is not offered.
	_, err := pool.Exec(context.Background(),
		`UPDATE ms_billing.charge_intents SET total_micros = total_micros + 1 WHERE digest = $1`,
		digest)
	require.Error(t, err, "a sealed intent was edited in place")
	require.Contains(t, err.Error(), "sealed",
		"the refusal does not say why; an operator seeing it needs to know to supersede")

	// The lifecycle columns are deliberately not frozen: an intent has to be
	// able to move through its states.
	_, err = pool.Exec(context.Background(),
		`UPDATE ms_billing.charge_intents SET state = 'eligible', state_changed_at = now()
		 WHERE digest = $1`, digest)
	require.NoError(t, err, "the state machine cannot advance, which freezes the intent solid")
}

// INV-008: one intent settles at most once, across all providers. As a
// primary key, a second settlement is an integrity violation rather than a
// race the code has to win.
func TestOneIntentSettlesAtMostOnce(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "digest-claim")

	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.intent_settlement_claims (intent_digest, claimed_by)
		 VALUES ($1, 'executor-a')`, digest)
	require.NoError(t, err)

	_, err = pool.Exec(context.Background(),
		`INSERT INTO ms_billing.intent_settlement_claims (intent_digest, claimed_by)
		 VALUES ($1, 'executor-b')`, digest)
	require.Error(t, err, "two executors both claimed one intent")
}

// INV-005: the bytes the customer was sent are the bytes collected against.
func TestNoticeMustDeliverTheDocumentItGates(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "digest-notice")
	other := seedIntent(t, pool, "digest-other")

	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.notice_receipts
		   (intent_digest, delivered_digest, policy, terminal_status,
		    eligibility_not_before, revocation_path_fresh)
		 VALUES ($1, $2, 'email/v1', 'delivered', now(), true)`,
		digest, other)
	require.Error(t, err, "a notice delivering a different document was accepted as this one's")

	// Queue acceptance is not delivery.
	_, err = pool.Exec(context.Background(),
		`INSERT INTO ms_billing.notice_receipts
		   (intent_digest, delivered_digest, policy, terminal_status,
		    eligibility_not_before, revocation_path_fresh)
		 VALUES ($1, $1, 'email/v1', 'queued', now(), true)`, digest)
	require.Error(t, err, "'queued' counted as delivered; handing a message to a queue "+
		"proves only that we tried")
}

// A line's amount must equal its own factors, so the number a customer reads
// cannot drift from the quantity and price beside it.
func TestLineAmountMustEqualItsFactors(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "digest-lines")

	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.charge_intent_lines
		   (intent_digest, line_index, meter, module, module_version,
		    quantity, unit_price_micros, amount_micros)
		 VALUES ($1, 0, 'm', 'mod', '1.0.0', 10, 25, 999)`, digest)
	require.Error(t, err, "a line was stored whose amount disagrees with its own factors")

	_, err = pool.Exec(context.Background(),
		`INSERT INTO ms_billing.charge_intent_lines
		   (intent_digest, line_index, meter, module, module_version,
		    quantity, unit_price_micros, amount_micros)
		 VALUES ($1, 0, 'm', 'mod', '1.0.0', 10, 25, 250)`, digest)
	require.NoError(t, err)
}

// A standing authorization must declare what it permits and how far. One
// without a ceiling is not unlimited — it is refused.
func TestStandingAuthorizationMustBeBounded(t *testing.T) {
	pool := testutil.NewTestDB(t)

	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.billing_authorizations
		   (id, scope, subject_kind, subject_id, currency, terms_revision,
		    price_book_revision, notice_policy, effective_from, expires_at,
		    acceptance_digest)
		 VALUES ('a1','standing','org','org-1','USD','terms-1','pb-1','email/v1',
		         now(), now() + interval '1 year', 'accept-1')`)
	require.Error(t, err, "a standing authorization with no kinds and no ceiling was stored")

	// A one-time authorization must name the document it permits.
	_, err = pool.Exec(context.Background(),
		`INSERT INTO ms_billing.billing_authorizations
		   (id, scope, subject_kind, subject_id, currency, terms_revision,
		    price_book_revision, notice_policy, effective_from, expires_at,
		    acceptance_digest)
		 VALUES ('a2','one-time','org','org-1','USD','terms-1','pb-1','email/v1',
		         now(), now() + interval '1 year', 'accept-1')`)
	require.Error(t, err, "a one-time authorization naming no intent was stored")
}

// A claim with an outcome must say when, so an in-flight claim and a settled
// one stay distinguishable. DESIGN §4: ambiguous evidence RETAINS the claim.
func TestClaimOutcomeMustCarryItsTime(t *testing.T) {
	pool := testutil.NewTestDB(t)
	digest := seedIntent(t, pool, "digest-outcome")

	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.intent_settlement_claims
		   (intent_digest, claimed_by, outcome) VALUES ($1, 'exec', 'succeeded')`, digest)
	require.Error(t, err, "a settled claim was stored with no settlement time")
}

func seedIntent(t *testing.T, pool *pgxpool.Pool, digest string) string {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.charge_intents
		   (digest, payer_kind, payer_id, currency, kind, price_book_revision,
		    terms_revision, notice_policy, tax_jurisdiction, tax_rule_revision,
		    tax_amount_micros, tax_verification, subtotal_micros, total_micros,
		    authorization_id, execute_not_before, execute_not_after)
		 VALUES ($1,'org','org-1','USD','usage.cycle','pb-1','terms-1','email/v1','TW','tax-1',
		         0, 'not_applicable', 1000, 1000, 'auth-1', $2, $3)`,
		digest,
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err, "seeding a valid intent failed; the fixture is wrong, not the schema")
	return digest
}

// 🔴 The database must refuse an unknown tax verification class too.
//
// The Go side refuses it at Seal, but the store is not the only writer a
// production database can ever have. Migration 060's CHECK is what holds if
// a row is inserted by hand, by a future query, or by a service that skips
// the sealing path — and the empty string is excluded deliberately, because
// it is the Go zero value that Seal already rejects.
//
// A stored class the engine would not have produced is a document whose
// digest cannot be reproduced, which is the whole failure the field exists
// to prevent.
func TestStoredTaxVerificationClassIsConstrained(t *testing.T) {
	pool := testutil.NewTestDB(t)

	for _, bad := range []string{"", "reproducible", "INDEPENDENTLY_REPRODUCIBLE", "guessed"} {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO ms_billing.charge_intents
			   (digest, payer_kind, payer_id, currency, kind, price_book_revision,
			    terms_revision, notice_policy, tax_jurisdiction, tax_rule_revision,
			    tax_amount_micros, tax_verification, subtotal_micros, total_micros,
			    authorization_id, execute_not_before, execute_not_after)
			 VALUES ($1,'org','org-1','USD','usage.cycle','pb-1','terms-1','email/v1','TW','tax-1',
			         0, $2, 1000, 1000, 'auth-1', $3, $4)`,
			"digest-bad-"+bad,
			bad,
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		)
		require.Error(t, err,
			"the schema accepted tax_verification %q; a class the engine cannot "+
				"produce must not be storable", bad)
	}

	// And every class the Go side CAN seal must be storable, or the two
	// vocabularies have drifted apart in the other direction.
	for i, good := range []string{"independently_reproducible", "provider_attested", "not_applicable"} {
		_, err := pool.Exec(context.Background(),
			`INSERT INTO ms_billing.charge_intents
			   (digest, payer_kind, payer_id, currency, kind, price_book_revision,
			    terms_revision, notice_policy, tax_jurisdiction, tax_rule_revision,
			    tax_amount_micros, tax_verification, subtotal_micros, total_micros,
			    authorization_id, execute_not_before, execute_not_after)
			 VALUES ($1,'org','org-1','USD','usage.cycle','pb-1','terms-1','email/v1','TW','tax-1',
			         0, $2, 1000, 1000, 'auth-1', $3, $4)`,
			fmt.Sprintf("digest-good-%d", i),
			good,
			time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC),
		)
		require.NoError(t, err, "the schema refused sealable class %q", good)
	}
}

// The migration must actually be applied by the harness, or every test above
// would fail for the wrong reason.
func TestIntentTablesExist(t *testing.T) {
	pool := testutil.NewTestDB(t)

	for _, table := range []string{
		"charge_intents", "charge_intent_lines", "charge_intent_source_facts",
		"billing_authorizations", "notice_receipts", "intent_settlement_claims",
	} {
		var exists bool
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			                WHERE table_schema = 'ms_billing' AND table_name = $1)`,
			table).Scan(&exists))
		require.Truef(t, exists, "ms_billing.%s was not created", table)
	}
}
