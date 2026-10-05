package main

import (
	"context"
	"fmt"

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
// identifier.
const installsSQLFmt = `SELECT module_id, prefix FROM %s.module_install`

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
			var t tableSize
			if err := rows.Scan(&t.Schema, &t.Table, &t.Bytes); err != nil {
				return err
			}
			out = append(out, t)
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
			if err := rows.Scan(&in.Module, &in.Prefix); err != nil {
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
	return fn(tx)
}
