package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// idleChecker looks up whether a function was IDLE (zero invocations) in the
// window immediately preceding the earliest window this run is about to
// sweep — via the SAME deterministic event_id (ssrEventID) a previous run
// would have already recorded it under for the infra.compute.ssr.request.count
// metric.
//
// This is a direct, minimal point lookup against ms_billing.usage_events —
// deliberately NOT added to the shared internal/account/usage.Store
// interface, which every OTHER Store implementation (including every test
// fake across the whole usage package) would otherwise have to grow a case
// for, for a concern only this one producer has (design doc §8 MEDIUM
// finding "job cost scales with inventory, not active traffic").
type idleChecker interface {
	// WasIdle reports whether a request.count event already exists for the
	// given deterministic event_id with value == 0. found (the bool return)
	// distinguishes "confirmed zero" from "nothing recorded yet" — an
	// unswept window is conservatively treated as NOT idle (so a function is
	// never wrongly skipped just because a previous run hasn't gotten to it
	// yet; the pre-filter only ever skips CONFIRMED-idle functions).
	WasIdle(ctx context.Context, eventID string) (idle bool, err error)
}

// pgxIdleChecker is the real idleChecker, backed by the same pgxpool the
// usage.Service already writes through.
type pgxIdleChecker struct {
	pool *pgxpool.Pool
}

func newPgxIdleChecker(pool *pgxpool.Pool) *pgxIdleChecker {
	return &pgxIdleChecker{pool: pool}
}

func (c *pgxIdleChecker) WasIdle(ctx context.Context, eventID string) (bool, error) {
	var value float64
	err := c.pool.QueryRow(ctx,
		`SELECT value FROM ms_billing.usage_events WHERE event_id = $1`, eventID,
	).Scan(&value)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return value == 0, nil
}
