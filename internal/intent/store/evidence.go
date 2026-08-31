package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
)

// ErrNoRecorder is returned when a write that must produce evidence is asked
// to run without one.
//
// It is not a fallback path. docs/DESIGN.md:398 makes evidence "a durable side
// effect of the money moving", so a state change that cannot record one does
// not happen. A store method that quietly skipped the record when the
// deployment had no key would be the reporter-that-declines-to-run this design
// exists to remove.
var ErrNoRecorder = errors.New("store: this deployment holds no evidence recorder, so it cannot make a change that requires evidence")

// appendEvidence allocates a checkpoint, seals the event against it, and
// writes the row — all inside the caller's transaction.
//
// The order is forced by the signature. docs/VERIFICATION.md requires a signed
// statement to carry its checkpoint, and only the transaction about to write
// the row can allocate one, so the sequence is drawn first and the signature
// covers it. nextval is transactional in the sense that matters here — two
// concurrent writers never receive the same value — and non-transactional in
// the sense the migration header records: a rollback consumes the number, so
// the log is monotonic and gappy.
func appendEvidence(ctx context.Context, tx pgx.Tx, rec *evidence.Recorder, e evidence.Event) error {
	if rec == nil {
		return ErrNoRecorder
	}

	var checkpoint int64
	if err := tx.QueryRow(ctx,
		`SELECT nextval('ms_billing.evidence_records_checkpoint_seq')`).Scan(&checkpoint); err != nil {
		return fmt.Errorf("allocate evidence checkpoint: %w", err)
	}

	record, err := rec.Seal(e, checkpoint)
	if err != nil {
		return fmt.Errorf("seal evidence record: %w", err)
	}

	var intentDigest *string
	if record.IntentDigest != "" {
		d := record.IntentDigest
		intentDigest = &d
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO ms_billing.evidence_records
		  (checkpoint, kind, subject_kind, subject_id, intent_digest, detail,
		   occurred_at, payload_digest, signature, key_id,
		   signed_not_before, signed_not_after)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT ON CONSTRAINT evidence_records_one_per_outcome DO NOTHING`,
		record.Checkpoint, string(record.Kind),
		record.Subject.Kind, record.Subject.ID, intentDigest, record.Detail,
		record.OccurredAt, record.PayloadDigest,
		record.Signed.Signature, record.Signed.Statement.KeyID,
		record.Signed.Statement.NotBefore, record.Signed.Statement.NotAfter,
	)
	if err != nil {
		return fmt.Errorf("append evidence record: %w", err)
	}
	// A conflict is a retry of an event already recorded — the same kind, the
	// same intent, the same attested payload. Nothing to reconcile: the
	// existing row IS this event, and writing a second would turn one thing
	// that happened into two.
	return nil
}

// AppendEvidence writes one evidence record in its own transaction.
//
// For events that accompany no other state change — a refusal, which
// docs/DESIGN.md §4 requires to mutate nothing else ("a refusal here mutates
// no provider"). Events that DO accompany a state change go through the method
// that makes that change, so the two commit together.
func (s *Store) AppendEvidence(ctx context.Context, rec *evidence.Recorder, e evidence.Event) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return appendEvidence(ctx, tx, rec, e)
	})
}

// RecordOutcomeWithEvidence records a settlement outcome and its evidence in
// ONE transaction.
//
// 🔴 It deliberately does NOT also advance the lifecycle state.
//
// AdvanceState's WHERE clause requires the state read BEFORE the provider was
// called, and the executor discards its ErrStateChanged on both terminal
// branches — by then the money has moved and the outcome is already committed
// by an independent write. Folding that into this transaction would invert the
// meaning: a concurrent state change would stop losing a cosmetic update and
// start rolling back the outcome record AND its evidence, producing "money
// moved and nothing was recorded", which is the exact failure joining the
// evidence write to the outcome is here to prevent.
func (s *Store) RecordOutcomeWithEvidence(
	ctx context.Context,
	rec *evidence.Recorder,
	digest, outcome string,
	e evidence.Event,
) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE ms_billing.intent_settlement_claims
			   SET outcome = $2, outcome_at = $3
			 WHERE intent_digest = $1 AND outcome IS NULL`,
			digest, outcome, e.OccurredAt)
		if err != nil {
			return fmt.Errorf("record outcome: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s already has an outcome, or is unclaimed", ErrStateChanged, digest)
		}
		return appendEvidence(ctx, tx, rec, e)
	})
}
