//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestMigration055_UpDownUp_RoundTrips(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	require.True(t, columnExists(t, pool, "metric_definitions", "aggregation_key"))
	require.True(t, columnExists(t, pool, "usage_events", "subject"))
	require.True(t, columnExists(t, pool, "usage_events", "metadata"))
	require.True(t, columnExists(t, pool, "usage_events", "occurred_at"))
	require.True(t, columnExists(t, pool, "usage_events", "billable_at"))
	require.True(t, columnExists(t, pool, "usage_events", "payload_fingerprint"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "aggregation_key"))
	var keyedIndex string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(to_regclass('ms_billing.usage_events_keyed_subject_peak_idx')::text, '')`).Scan(&keyedIndex))
	require.Equal(t, "ms_billing.usage_events_keyed_subject_peak_idx", keyedIndex)
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, app_id, module_id, metric, kind, value, recorded_at,
		     observation_version, occurred_at, billable_at, occurrence_policy)
		VALUES ('v2-without-fingerprint', gen_random_uuid(), gen_random_uuid(),
		        'users.active', 'peak', 1, now(), 2, now(), now(), 'on_time')`)
	require.Error(t, err, "every v2 row must carry its canonical fingerprint")

	// 073's aggregate uniqueness index is defined over 055's aggregation_key, so
	// it comes off first — otherwise 055.down cascade-drops it while leaving the
	// dev_served column, and the re-applied stack ends up with an ON CONFLICT
	// target no index matches. Descendants first, exactly as in the 023 test.
	_, err = pool.Exec(ctx, migrationSQL(t, "073_dev_served_usage.down.sql"))
	require.NoError(t, err)
	require.False(t, columnExists(t, pool, "usage_aggregates", "dev_served"))

	// Once the keyed contract is used, rollback must fail before silently
	// reinterpreting or dropping billable state.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.metric_definitions (module_id, metric, kind, aggregation_key)
		VALUES (gen_random_uuid(), 'users.active', 'peak', 'subject')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, migrationSQL(t, "055_keyed_meter_observations.down.sql"))
	require.ErrorContains(t, err, "forward-only after v2/keyed-meter use")
	require.True(t, columnExists(t, pool, "usage_events", "occurred_at"),
		"failed rollback must leave the schema intact")
	_, err = pool.Exec(ctx, `DELETE FROM ms_billing.metric_definitions WHERE metric = 'users.active'`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, migrationSQL(t, "055_keyed_meter_observations.down.sql"))
	require.NoError(t, err)
	require.False(t, columnExists(t, pool, "usage_events", "occurred_at"))
	require.False(t, columnExists(t, pool, "usage_aggregates", "aggregation_key"))
	var rejectionTable string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(to_regclass('ms_billing.usage_observation_rejections')::text, '')`).Scan(&rejectionTable))
	require.Empty(t, rejectionTable)

	// Recreate the real pre-055 cutover state: historical rollup/charge wrote
	// durable aggregates and runs but left every billing_period status 'open'.
	// The up migration must make those boundaries immutable before any v2
	// producer can admit occurrence-windowed late usage.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id)
		VALUES ('55000000-0000-0000-0000-000000000001', 'user',
		        '55000000-0000-0000-0000-000000000002');

		INSERT INTO ms_billing.billing_periods (id, account_id, period_start, period_end)
		VALUES
		 ('55000000-0000-0000-0000-000000000101', '55000000-0000-0000-0000-000000000001', '2026-01-01Z', '2026-02-01Z'),
		 ('55000000-0000-0000-0000-000000000102', '55000000-0000-0000-0000-000000000001', '2026-02-01Z', '2026-03-01Z'),
		 ('55000000-0000-0000-0000-000000000103', '55000000-0000-0000-0000-000000000001', '2026-03-01Z', '2026-04-01Z'),
		 ('55000000-0000-0000-0000-000000000104', '55000000-0000-0000-0000-000000000001', '2026-04-01Z', '2026-05-01Z');

		INSERT INTO ms_billing.usage_aggregates
		    (period_id, account_id, app_id, module_id, metric, kind, billable_quantity)
		VALUES
		    ('55000000-0000-0000-0000-000000000101',
		     '55000000-0000-0000-0000-000000000001', gen_random_uuid(),
		     gen_random_uuid(), 'pre055.aggregate', 'count', 1);

		INSERT INTO ms_billing.billing_runs
		    (account_id, period_start, period_end, status)
		VALUES
		    ('55000000-0000-0000-0000-000000000001', '2026-02-01Z', '2026-03-01Z', 'pending'),
		    ('55000000-0000-0000-0000-000000000001', '2026-03-01Z', '2026-04-01Z', 'invoiced');
	`)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, migrationSQL(t, "055_keyed_meter_observations.up.sql"))
	require.NoError(t, err)
	require.True(t, columnExists(t, pool, "usage_events", "occurred_at"))
	require.True(t, columnExists(t, pool, "usage_aggregates", "aggregation_key"))
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT COALESCE(to_regclass('ms_billing.usage_observation_rejections')::text, '')`).Scan(&rejectionTable))
	require.Equal(t, "ms_billing.usage_observation_rejections", rejectionTable)
	rows, err := pool.Query(ctx, `
		SELECT period_start::date::text, status::text
		FROM ms_billing.billing_periods
		WHERE account_id='55000000-0000-0000-0000-000000000001'
		ORDER BY period_start`)
	require.NoError(t, err)
	defer rows.Close()
	gotStatuses := map[string]string{}
	for rows.Next() {
		var start, status string
		require.NoError(t, rows.Scan(&start, &status))
		gotStatuses[start] = status
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]string{
		"2026-01-01": "closing",  // aggregate snapshot
		"2026-02-01": "closing",  // pending/reclaimable run
		"2026-03-01": "invoiced", // terminal run
		"2026-04-01": "open",     // no durable rollup/charge state
	}, gotStatuses)

	// Re-apply the 073 descendant so the round trip lands back on the real
	// current schema rather than a 055-shaped one.
	_, err = pool.Exec(ctx, migrationSQL(t, "073_dev_served_usage.up.sql"))
	require.NoError(t, err)
	require.True(t, columnExists(t, pool, "usage_aggregates", "dev_served"))

	// Catalog owns the keyed mode: only peak + subject is admitted.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.metric_definitions (module_id, metric, kind, aggregation_key)
		VALUES (gen_random_uuid(), 'bad.count', 'count', 'subject')`)
	require.Error(t, err)
}
