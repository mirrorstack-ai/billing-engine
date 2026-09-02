package store

import (
	"context"
	"fmt"
)

// PendingExecution returns the digests of intents that are candidates for
// execution, oldest first.
//
// 🔴 CANDIDATES, not permitted intents. The predicate decides what may
// execute, and this query must not anticipate it: a store that pre-filtered
// on anything the predicate also checks would be a second, weaker copy of the
// rule, and the two would drift. So this selects on the LIFECYCLE only —
// docs/DESIGN.md §4's states from which execution is even conceivable — and
// every one it returns is evaluated in full.
//
// The consequence is deliberate: on a deployment where every gate is false,
// this returns work and the executor refuses all of it. That is the honest
// shape. An executor that found nothing to do and one that refused everything
// it found look identical from the outside, and only the second is telling
// the truth about a deployment whose evidence records do not exist yet.
//
// Ordered by created_at so a backlog drains oldest-first and a poisoned
// intent cannot starve the ones behind it — it is refused, its refusal is
// recorded, and the batch moves on.
func (s *Store) PendingExecution(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("store: pending execution limit must be positive, got %d", limit)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT ci.digest
		  FROM ms_billing.charge_intents ci
		  LEFT JOIN ms_billing.intent_settlement_claims c
		    ON c.intent_digest = ci.digest
		 WHERE ci.state IN ('proposed', 'notice_pending', 'disclosed', 'eligible')
		   -- INV-008: one intent settles at most once. A claimed intent is
		   -- either in flight or already resolved, and either way a second
		   -- executor must not pick it up.
		   AND c.intent_digest IS NULL
		 ORDER BY ci.created_at, ci.digest
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("pending execution: %w", err)
	}
	defer rows.Close()

	var digests []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, fmt.Errorf("scan pending digest: %w", err)
		}
		digests = append(digests, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pending execution rows: %w", err)
	}
	return digests, nil
}
