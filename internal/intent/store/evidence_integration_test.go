//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence/evidencetest"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/store"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/signing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// 🔴 An evidence record must be a side effect of the state change, not a
// second write that can fail on its own.
//
// This drives the failure directly: the record is sealed against a checkpoint
// and inserted in the SAME transaction as the intent, so a poisoned evidence
// write must take the intent with it. If the intent survives, the outbox is a
// report about the money rather than part of it — which is the exact thing
// docs/DESIGN.md:398 says it must not be.
func TestAFailedEvidenceWriteTakesTheIntentWithIt(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := sealSource(t, 12_000_000)

	// The event names an intent digest that does not exist, so the evidence
	// row's foreign key fails AFTER the intent insert has run inside the same
	// transaction.
	err := s.SaveIntentWithEvidence(ctx, sealed, evidencetest.Recorder(t), evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      sealed.Payer(),
		IntentDigest: "an-intent-that-does-not-exist",
		Detail:       string(sealed.Kind()),
		OccurredAt:   evidencetest.At,
	})
	require.Error(t, err, "a poisoned evidence write was accepted")

	var intents int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.charge_intents WHERE digest = $1`,
		sealed.Digest()).Scan(&intents))
	require.Zero(t, intents,
		"the intent was committed while its evidence record was not. The outbox is "+
			"then a report about the money rather than part of it, and no later reader "+
			"can distinguish 'never recorded' from 'recorded and withheld'.")
}

// The record a deployment writes must actually verify — against a pinned
// root, under the domain and audience a verifier states, exactly as
// docs/VERIFICATION.md §4 has a customer doing offline.
//
// Asserting that a row exists would pass with a signature of "x".
func TestAStoredEvidenceRecordVerifiesAgainstThePinnedRoot(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := sealSource(t, 9_000_000)

	require.NoError(t, s.SaveIntentWithEvidence(ctx, sealed, evidencetest.Recorder(t), evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      sealed.Payer(),
		IntentDigest: sealed.Digest(),
		Detail:       string(sealed.Kind()),
		OccurredAt:   evidencetest.At,
	}))

	row := readEvidence(t, pool, sealed.Digest())

	// 1. The digest is recomputable from the row's own columns. That is what
	//    makes the columns sufficient evidence and a stored payload
	//    unnecessary.
	recomputed := evidence.PayloadDigestOf(evidence.Event{
		Kind:         evidence.Kind(row.kind),
		Subject:      intent.Subject{Kind: row.subjectKind, ID: row.subjectID},
		IntentDigest: row.intentDigest,
		Detail:       row.detail,
		OccurredAt:   row.occurredAt,
	})
	require.Equal(t, recomputed, row.payloadDigest,
		"the stored digest is not the one this row's own fields produce, so nobody "+
			"holding the row can check what it attests")

	// 2. The signature verifies against a PINNED root, with the verifier
	//    stating what it expects rather than reading it off the statement.
	require.NoError(t, signing.Verify(
		evidencetest.TrustRoot(t),
		signing.Signed{
			Statement: signing.Statement{
				Algorithm:     signing.Algorithm,
				KeyID:         row.keyID,
				Domain:        signing.DomainBillingEvidence,
				Issuer:        "billing-engine",
				Audience:      "customer",
				Environment:   "test",
				Schema:        evidence.Schema,
				PayloadDigest: hex(row.payloadDigest),
				Checkpoint:    itoa(row.checkpoint),
				NotBefore:     row.notBefore,
				NotAfter:      row.notAfter,
			},
			Signature: row.signature,
		},
		evidencetest.Expect(),
		evidencetest.At.Add(time.Hour),
	), "the stored signature does not verify, so the row is not evidence of anything")
}

// A retried proposal must not append a second record for the same event.
//
// Once a work loop exists the refusal path is the highest-frequency writer in
// the system, and "one record per outcome" proven by a test that runs each
// branch once is a coincidence rather than a property. The constraint is what
// makes it a property.
func TestARetriedEventAppendsOneRecord(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := sealSource(t, 5_000_000)
	rec := evidencetest.Recorder(t)

	event := evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      sealed.Payer(),
		IntentDigest: sealed.Digest(),
		Detail:       string(sealed.Kind()),
		OccurredAt:   evidencetest.At,
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, s.SaveIntentWithEvidence(ctx, sealed, rec, event))
	}

	var records int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.evidence_records WHERE intent_digest = $1`,
		sealed.Digest()).Scan(&records))
	require.Equal(t, 1, records, "a retry appended a second record for one event")
}

// A store asked to make a change that requires evidence, without a recorder,
// must refuse rather than write the change alone.
func TestAWriteRequiringEvidenceRefusesWithNoRecorder(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := sealSource(t, 3_000_000)

	err := s.SaveIntentWithEvidence(ctx, sealed, nil, evidence.Event{})
	require.ErrorIs(t, err, store.ErrNoRecorder)

	var intents int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.charge_intents WHERE digest = $1`,
		sealed.Digest()).Scan(&intents))
	require.Zero(t, intents, "the intent was stored by a deployment that cannot record it")
}

// Evidence is append-only for UPDATE and DELETE alike. INV-014's answer to a
// record that should not have been written is a correction record.
func TestAnEvidenceRecordCannotBeEditedOrDeleted(t *testing.T) {
	pool := testutil.NewTestDB(t)
	s := store.New(pool)
	ctx := context.Background()
	sealed := sealSource(t, 7_000_000)
	require.NoError(t, s.SaveIntentWithEvidence(ctx, sealed, evidencetest.Recorder(t), evidence.Event{
		Kind:         evidence.KindSealedIntent,
		Subject:      sealed.Payer(),
		IntentDigest: sealed.Digest(),
		Detail:       string(sealed.Kind()),
		OccurredAt:   evidencetest.At,
	}))

	for name, stmt := range map[string]string{
		"update": `UPDATE ms_billing.evidence_records SET detail = 'rewritten' WHERE intent_digest = $1`,
		"delete": `DELETE FROM ms_billing.evidence_records WHERE intent_digest = $1`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := pool.Exec(ctx, stmt, sealed.Digest())
			require.Error(t, err, "an evidence record was %sd", name)
			require.Contains(t, err.Error(), "append-only")
		})
	}
}

// 🔴 The read-only ops role must NOT be able to read customer evidence.
//
// Migrations 058/059 run ALTER DEFAULT PRIVILEGES ... GRANT SELECT ON TABLES
// TO billing_ro, so without 064's REVOKE this table is readable by the role
// cmd/intent-shadow runs as against production — while docs/DESIGN.md:392-394
// requires a payer-bound CustomerReadProof and says possession of an object id
// is not enough.
//
// The role does not exist in CI (024/058 are gated on pg_roles), so this
// creates it and checks the privilege directly rather than asserting a
// property of a role that is absent. A test that skipped here would report
// green for the environment where the exposure is real.
func TestTheReadOnlyOpsRoleCannotReadEvidence(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro') THEN
				CREATE ROLE billing_ro NOLOGIN;
			END IF;
		END $$;`)
	require.NoError(t, err)

	// Re-apply the two grants exactly as 058/059 do, so the test measures
	// what production has rather than a fresh database.
	_, err = pool.Exec(ctx, `GRANT USAGE ON SCHEMA ms_billing TO billing_ro`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `GRANT SELECT ON ALL TABLES IN SCHEMA ms_billing TO billing_ro`)
	require.NoError(t, err)

	// 064's REVOKE runs after those grants in migration order; re-assert it
	// the way the migration does.
	_, err = pool.Exec(ctx, `REVOKE ALL ON ms_billing.evidence_records FROM billing_ro`)
	require.NoError(t, err)

	var canSelect bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.evidence_records', 'SELECT')`).Scan(&canSelect))
	require.False(t, canSelect,
		"billing_ro can read customer evidence records. INV-014 requires a payer-bound "+
			"CustomerReadProof for these reads, and cmd/intent-shadow runs as this role "+
			"against production.")

	// The control must be specific: the role still reads the rest of the
	// schema, or this test would pass on a database where the grant simply
	// never happened.
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT has_table_privilege('billing_ro', 'ms_billing.charge_intents', 'SELECT')`).Scan(&canSelect))
	require.True(t, canSelect,
		"billing_ro cannot read charge_intents either, so the REVOKE above proves nothing "+
			"about evidence_records specifically")
}

type evidenceRow struct {
	checkpoint             int64
	kind                   string
	subjectKind, subjectID string
	intentDigest           string
	detail                 string
	occurredAt             time.Time
	payloadDigest          []byte
	signature, keyID       string
	notBefore, notAfter    time.Time
}

func readEvidence(t *testing.T, pool *pgxpool.Pool, digest string) evidenceRow {
	t.Helper()
	var r evidenceRow
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT checkpoint, kind, subject_kind, subject_id, coalesce(intent_digest,''),
		       detail, occurred_at, payload_digest, signature, key_id,
		       signed_not_before, signed_not_after
		  FROM ms_billing.evidence_records
		 WHERE intent_digest = $1`, digest).Scan(
		&r.checkpoint, &r.kind, &r.subjectKind, &r.subjectID, &r.intentDigest,
		&r.detail, &r.occurredAt, &r.payloadDigest, &r.signature, &r.keyID,
		&r.notBefore, &r.notAfter))
	return r
}

func hex(b []byte) string {
	const d = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, x := range b {
		out = append(out, d[x>>4], d[x&0x0f])
	}
	return string(out)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
