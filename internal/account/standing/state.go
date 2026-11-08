package standing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PriorState is the standing verdict recorded before an observation. A nil
// *PriorState from Observe means the owner had no row (first observation).
type PriorState struct {
	Blocked bool
	Reason  string
}

// StateStore remembers the last observed verdict per owner so the notifier can
// tell a transition from a repeat (migration 093, ms_billing.standing_state).
type StateStore interface {
	// Observe records the verdict atomically and returns what was recorded
	// before it (nil on first observation) and the changed_at of the current
	// blocked epoch. Concurrent calls for one owner serialize on the owner row,
	// so exactly one of them sees a given flip.
	Observe(ctx context.Context, ownerKey string, blocked bool, reason string, now time.Time) (*PriorState, time.Time, error)
}

type pgxStateStore struct{ pool *pgxpool.Pool }

// NewStateStore returns the Postgres-backed StateStore.
func NewStateStore(pool *pgxpool.Pool) StateStore { return &pgxStateStore{pool: pool} }

func (s *pgxStateStore) Observe(ctx context.Context, ownerKey string, blocked bool, reason string, now time.Time) (prior *PriorState, changedAt time.Time, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("standing state: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// First observation: insert-or-nothing, so two racing first events cannot
	// both believe they are first (the loser blocks on the winner's row).
	tag, err := tx.Exec(ctx,
		`INSERT INTO ms_billing.standing_state (owner_key, blocked, reason, changed_at, updated_at)
		 VALUES ($1, $2, $3, $4, $4)
		 ON CONFLICT (owner_key) DO NOTHING`, ownerKey, blocked, reason, now)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("standing state: insert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return nil, time.Time{}, fmt.Errorf("standing state: commit: %w", err)
		}
		return nil, now, nil
	}

	var p PriorState
	var prevChanged time.Time
	err = tx.QueryRow(ctx,
		`SELECT blocked, reason, changed_at FROM ms_billing.standing_state
		 WHERE owner_key = $1 FOR UPDATE`, ownerKey).Scan(&p.Blocked, &p.Reason, &prevChanged)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) { // deleted between the two statements
			return nil, time.Time{}, fmt.Errorf("standing state: row vanished: %w", err)
		}
		return nil, time.Time{}, fmt.Errorf("standing state: lock: %w", err)
	}

	changedAt = prevChanged
	switch {
	case p.Blocked != blocked:
		changedAt = now
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.standing_state
			 SET blocked = $2, reason = $3, changed_at = $4, updated_at = $4 WHERE owner_key = $1`,
			ownerKey, blocked, reason, now)
	case p.Reason != reason:
		_, err = tx.Exec(ctx,
			`UPDATE ms_billing.standing_state SET reason = $2, updated_at = $3 WHERE owner_key = $1`,
			ownerKey, reason, now)
	}
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("standing state: update: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, time.Time{}, fmt.Errorf("standing state: commit: %w", err)
	}
	return &p, changedAt, nil
}
