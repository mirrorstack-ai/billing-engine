package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// SaveIntentGroup records that a set of intents settles on one invoice.
//
// Written by the leg that proposed them, in the same transaction as nothing
// else: grouping is a statement about intent, not a lock, and the executor
// takes the claims that make a collection exclusive.
//
// Idempotent on the intent. Re-proposing the same set is the same grouping,
// and the primary key means an intent that somehow arrived in two groups is
// refused rather than silently settling on two invoices.
func (s *Store) SaveIntentGroup(ctx context.Context, groupID string, digests []string) error {
	if groupID == "" {
		return fmt.Errorf("store: refusing to group intents under an empty id")
	}
	if len(digests) == 0 {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, d := range digests {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ms_billing.intent_groups (intent_digest, group_id)
				VALUES ($1, $2)
				ON CONFLICT (intent_digest) DO NOTHING`, d, groupID); err != nil {
				return fmt.Errorf("group intent %s: %w", d, err)
			}
		}
		return nil
	})
}

// PendingExecutionGrouped returns candidate intents assembled into the sets
// that must settle together.
//
// 🔴 A GROUP IS ALL-OR-NOTHING, so a group is only returned when EVERY one of
// its intents is a candidate.
//
// If one member has already been claimed, or has reached a terminal state,
// the rest are held back rather than collected without it. Collecting the
// remainder would produce an invoice for part of a charge — the customer pays
// some of what they owe, the books balance nowhere, and nothing in the
// system records that a group was split.
//
// Ungrouped intents come back as groups of one, so a caller has a single
// shape to handle and the ordinary path is not a special case.
//
// The limit counts INTENTS, not groups, so a batch cannot be enlarged by
// grouping — a bound that a caller's data can widen is not a bound.
func (s *Store) PendingExecutionGrouped(ctx context.Context, limit int) ([][]string, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("store: pending execution limit must be positive, got %d", limit)
	}

	candidates, err := s.PendingExecution(ctx, limit)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Which of the candidates are grouped, and with what.
	groupOf := map[string]string{}
	rows, err := s.pool.Query(ctx, `
		SELECT intent_digest, group_id
		  FROM ms_billing.intent_groups
		 WHERE intent_digest = ANY($1)`, candidates)
	if err != nil {
		return nil, fmt.Errorf("load intent groups: %w", err)
	}
	for rows.Next() {
		var digest, group string
		if err := rows.Scan(&digest, &group); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan intent group: %w", err)
		}
		groupOf[digest] = group
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("intent group rows: %w", err)
	}

	// For every group touched, its FULL membership — the candidates alone are
	// not enough to know whether a group is complete.
	members := map[string][]string{}
	if len(groupOf) > 0 {
		ids := make([]string, 0, len(groupOf))
		seen := map[string]bool{}
		for _, g := range groupOf {
			if !seen[g] {
				seen[g] = true
				ids = append(ids, g)
			}
		}
		mrows, err := s.pool.Query(ctx, `
			SELECT group_id, intent_digest
			  FROM ms_billing.intent_groups
			 WHERE group_id = ANY($1)
			 ORDER BY group_id, intent_digest`, ids)
		if err != nil {
			return nil, fmt.Errorf("load group members: %w", err)
		}
		for mrows.Next() {
			var group, digest string
			if err := mrows.Scan(&group, &digest); err != nil {
				mrows.Close()
				return nil, fmt.Errorf("scan group member: %w", err)
			}
			members[group] = append(members[group], digest)
		}
		mrows.Close()
		if err := mrows.Err(); err != nil {
			return nil, fmt.Errorf("group member rows: %w", err)
		}
	}

	candidate := map[string]bool{}
	for _, d := range candidates {
		candidate[d] = true
	}

	var out [][]string
	emitted := map[string]bool{}
	for _, d := range candidates {
		group, grouped := groupOf[d]
		if !grouped {
			out = append(out, []string{d})
			continue
		}
		if emitted[group] {
			continue
		}
		emitted[group] = true

		// Every member must be a candidate, or the group waits.
		complete := true
		for _, m := range members[group] {
			if !candidate[m] {
				complete = false
				break
			}
		}
		if complete {
			out = append(out, members[group])
		}
	}
	return out, nil
}
