package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// ErrRemainderUnavailable is returned when a receivable would claim more of
// its source than is left unreserved.
var ErrRemainderUnavailable = errors.New("store: the source has no such remainder left to reserve")

// ReserveRemainder claims part of a source intent for one receivable.
//
// docs/DESIGN.md §6 funds collect_receivable with "a linked intent for the
// remaining amount only, under a source-capacity reservation". Without the
// reservation two receivables can each claim the whole remainder of one intent
// and both collect: they are DIFFERENT documents, each individually valid, so
// INV-008's one-settlement-per-intent guard sees two intents rather than one
// obligation claimed twice.
//
// The claim and the running total move in ONE statement each, inside one
// transaction, and the ceiling is a database CHECK rather than a comparison in
// Go — a guard that reads then writes is one racing process away from useless,
// and this is the guard that stops a customer being charged twice for the same
// debt.
//
// Reserving is idempotent per receivable: the link table's primary key means a
// replayed proposal reserves once. A replay returns nil rather than an error,
// because "already reserved by exactly this receivable" is success.
func (s *Store) ReserveRemainder(ctx context.Context, receivable intent.ChargeIntent) error {
	if !receivable.Sealed() {
		return errors.New("store: cannot reserve for an unsealed receivable")
	}
	source := receivable.Collects()
	if source == "" {
		return errors.New("store: the receivable names no source to collect")
	}
	amount := receivable.TotalMicros()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("reserve remainder: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The link first: its primary key is what makes a replay a no-op, and
	// taking it before the running total means a replay cannot reach the
	// UPDATE at all.
	tag, err := tx.Exec(ctx, `
		INSERT INTO ms_billing.intent_receivable_links
		  (receivable_digest, source_digest, reserved_micros)
		VALUES ($1,$2,$3)
		ON CONFLICT (receivable_digest) DO NOTHING`,
		receivable.Digest(), source, amount)
	if err != nil {
		return fmt.Errorf("reserve remainder: link: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Already reserved by this exact receivable.
		return tx.Commit(ctx)
	}

	// The ceiling lives in the WHERE clause AND in a CHECK constraint. The
	// WHERE gives a clean refusal; the CHECK is what holds if anyone ever
	// writes this column from somewhere else.
	tag, err = tx.Exec(ctx, `
		UPDATE ms_billing.charge_intents
		   SET reserved_micros = reserved_micros + $2
		 WHERE digest = $1
		   AND reserved_micros + $2 <= total_micros`,
		source, amount)
	if err != nil {
		return fmt.Errorf("reserve remainder: claim: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %d micros against %s", ErrRemainderUnavailable, amount, source)
	}
	return tx.Commit(ctx)
}

// UnreservedRemainder reports how much of a source intent is still unclaimed.
func (s *Store) UnreservedRemainder(ctx context.Context, sourceDigest string) (int64, error) {
	var remaining int64
	err := s.pool.QueryRow(ctx, `
		SELECT total_micros - reserved_micros
		  FROM ms_billing.charge_intents
		 WHERE digest = $1`, sourceDigest).Scan(&remaining)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("%w: intent %s", ErrNotFound, sourceDigest)
	}
	if err != nil {
		return 0, fmt.Errorf("unreserved remainder: %w", err)
	}
	return remaining, nil
}
