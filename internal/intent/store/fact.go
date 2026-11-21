package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/paymentfact"
)

// RecordSettlementWithFact records a SUCCEEDED settlement outcome, its
// evidence record, and its payment fact (billing-engine#313) — all in ONE
// transaction.
//
// It exists beside RecordOutcomeWithEvidence rather than folding into it
// because a payment fact only has meaning on a succeeded claim: a 'failed' or
// 'voided' outcome never collected anything, so writing a fact for it would
// assert a payment that never happened. RecordOutcomeWithEvidence keeps those
// two outcomes; this is the third branch, with one more write than the other
// two need.
//
// billing-engine#313's own rule is "there is never a settled payment without a
// fact": a fact-write failure must fail the settlement transaction, so this
// does the outcome UPDATE, the evidence INSERT and the paymentfact.RecordTx
// INSERT inside one pgx.BeginFunc, same as RecordOutcomeWithEvidence does for
// the first two.
func (s *Store) RecordSettlementWithFact(
	ctx context.Context,
	rec *evidence.Recorder,
	digest, providerReference string,
	e evidence.Event,
	fact paymentfact.New,
) error {
	if providerReference == "" {
		return fmt.Errorf(
			"store: refusing to record intent %s as succeeded with no provider reference — "+
				"the money moved through something, and a settlement that cannot name it "+
				"cannot be reconciled or collected against", digest)
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE ms_billing.intent_settlement_claims
			   SET outcome = 'succeeded', outcome_at = $2,
			       provider_reference = NULLIF($3, '')
			 WHERE intent_digest = $1 AND outcome IS NULL`,
			digest, e.OccurredAt, providerReference)
		if err != nil {
			return fmt.Errorf("record outcome: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: %s already has an outcome, or is unclaimed", ErrStateChanged, digest)
		}
		if err := appendEvidence(ctx, tx, rec, e); err != nil {
			return err
		}
		if _, _, err := paymentfact.RecordTx(ctx, tx, fact); err != nil {
			return fmt.Errorf("record payment fact: %w", err)
		}
		return nil
	})
}
