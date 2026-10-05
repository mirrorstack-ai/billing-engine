package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tableSizesSQL is the sampler's first fixed query. It reads the system catalog
// only: pg_total_relation_size needs no privilege on the table it measures, so
// the read-only role holds no grant on any customer table.
//
// relkind 'r' (ordinary, and every partition child) and 'm' (materialized view)
// are the relations with storage; 'p' (a partitioned parent) has none and its
// children are counted as themselves. The size includes indexes and TOAST, which
// is what the database bills the platform for. The schema regex is the same
// canonical shape parseAppSchema re-checks in Go.
const tableSizesSQL = `
SELECT n.nspname, c.relname, pg_catalog.pg_total_relation_size(c.oid)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname ~ '^app_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}$'
  AND c.relkind IN ('r', 'm')`

// installsSQLFmt is the second fixed query, one app schema at a time. The
// schema is the only part that varies and is never taken from the catalog or the
// environment: installs builds it from a parsed UUID and quotes it as an
// identifier. module_id alone: the table prefix is derived from it (modulePrefix),
// never read from the display-only prefix column, so the grant is one column.
const installsSQLFmt = `SELECT module_id FROM %s.module_install`

// readTimeoutsSQL bounds every read. SET LOCAL scopes both to the read-only
// transaction, so a stuck catalog scan or a lock queue behind DDL (an app
// install running ALTER) fails this run in seconds, instead of hanging the
// Lambda until its own timeout, and a pooled connection never carries them on.
const readTimeoutsSQL = `SET LOCAL statement_timeout = '20s'; SET LOCAL lock_timeout = '3s'`

// rowScanner is the one pgx.Rows method scanTableSize needs, so the NULL-size
// rule is unit-testable without a database.
type rowScanner interface{ Scan(dest ...any) error }

// scanTableSize reads one catalog row. pg_total_relation_size is NULL for a
// relation dropped between the catalog scan and the size call: that table has no
// bytes to bill, and one racing DROP must not fail the whole read (ok=false).
func scanTableSize(row rowScanner) (t tableSize, ok bool, err error) {
	var size *int64
	if err = row.Scan(&t.Schema, &t.Table, &size); err != nil {
		return tableSize{}, false, err
	}
	if size == nil {
		return tableSize{}, false, nil
	}
	t.Bytes = *size
	return t, true, nil
}

// pgReader implements dbReader over a pool opened under the read-only role.
// Every read runs in a READ ONLY transaction, so even a future edit that put a
// write in here would be refused by the server.
type pgReader struct{ pool *pgxpool.Pool }

func (r pgReader) TableSizes(ctx context.Context) ([]tableSize, error) {
	var out []tableSize
	err := r.readOnly(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, tableSizesSQL)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			t, ok, err := scanTableSize(rows)
			if err != nil {
				return err
			}
			if ok {
				out = append(out, t)
			}
		}
		return rows.Err()
	})
	return out, err
}

func (r pgReader) Installs(ctx context.Context, app uuid.UUID) ([]install, error) {
	schema := pgx.Identifier{appSchemaName(app)}.Sanitize()
	var out []install
	err := r.readOnly(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, fmt.Sprintf(installsSQLFmt, schema))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var in install
			if err := rows.Scan(&in.Module); err != nil {
				return err
			}
			out = append(out, in)
		}
		return rows.Err()
	})
	return out, err
}

func (r pgReader) readOnly(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, readTimeoutsSQL); err != nil {
		return err
	}
	return fn(tx)
}

// lastLevelsSQL is the billing-side read behind levelHistory: the newest
// infra.db.gib_hours sample of each (app, module) since $2, kept when above 0.
// A pair already zeroed is not returned, so a vanished module is zeroed once and
// then left alone. ms_billing.usage_events is read by the same service role that
// writes it; no new grant.
const lastLevelsSQL = `
SELECT app_id, module_id FROM (
    SELECT DISTINCT ON (app_id, module_id) app_id, module_id, value
    FROM ms_billing.usage_events
    WHERE metric = $1 AND recorded_at >= $2
    ORDER BY app_id, module_id, recorded_at DESC, ingested_at DESC
) last
WHERE value > 0`

// billingHistory implements levelHistory over the billing database pool.
type billingHistory struct{ pool *pgxpool.Pool }

func (h billingHistory) LivePairs(ctx context.Context, since time.Time) ([]pair, error) {
	rows, err := h.pool.Query(ctx, lastLevelsSQL, dbMetric, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.App, &p.Module); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
