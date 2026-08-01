//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestOrgsWithUnsweptUsage_ConvergesAfterSweep(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	orgID, accountID, appID, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	activatedAt := mustTime(t, "2026-07-06T12:00:00Z")
	now := mustTime(t, "2026-07-20T12:00:00Z")
	originalRecordedAt := mustTime(t, "2026-06-10T08:30:00Z")
	secondRecordedAt := mustTime(t, "2026-06-11T09:45:00Z")
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id, activated_at)
		VALUES ($1, 'org', $2, $3)`, accountID.String(), orgID.String(), activatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations (org_id, funding, updated_by)
		VALUES ($1, 'org', $2)`, orgID.String(), uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, owner_org_id, module_count, created_module_count, created_at
		) VALUES ($1, NULL, $2, 0, 0, $3)`, appID.String(), orgID.String(), originalRecordedAt)
	require.NoError(t, err)
	seedMetricDef(t, pool, moduleID, "orders.placed", usage.KindCount, 1_000)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 3, $4)`,
		uuid.NewString(), appID.String(), moduleID.String(), originalRecordedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 4, $4)`,
		uuid.NewString(), appID.String(), moduleID.String(), secondRecordedAt)
	require.NoError(t, err)

	orgs, err := store.OrgsWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{orgID}, orgs)

	summary, err := cycle.NewService(store, nil).WithNow(func() time.Time { return now }).
		SweepUnattachedOrgUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, &cycle.OrgUsageSweepSummary{
		Orgs: 1, Swept: 1, AttachedApps: 1, RepointedEvents: 2,
	}, summary)

	var rosterAccountID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT account_id FROM ms_billing.apps WHERE app_id = $1`, appID.String()).Scan(&rosterAccountID))
	require.Equal(t, accountID, rosterAccountID)

	// The clamp moves each pre-designation event forward to the START of the
	// account's currently OPEN anchored window — anchor day 6, derived from
	// activated_at (2026-07-06), evaluated at `now` (2026-07-20) — so the
	// backfilled usage bills in the first period that closes after designation
	// (decision 1), never retroactively. repointed_from keeps the true instant.
	openWindowStart := mustTime(t, "2026-07-06T00:00:00Z")
	rows, err := pool.Query(ctx, `
		SELECT account_id, recorded_at, repointed_from
		FROM ms_billing.usage_events WHERE app_id = $1
		ORDER BY repointed_from`, appID.String())
	require.NoError(t, err)
	defer rows.Close()
	var repointed []time.Time
	for rows.Next() {
		var eventAccountID uuid.UUID
		var recordedAt, repointedFrom time.Time
		require.NoError(t, rows.Scan(&eventAccountID, &recordedAt, &repointedFrom))
		require.Equal(t, accountID, eventAccountID)
		require.True(t, openWindowStart.Equal(recordedAt),
			"recorded_at clamped to the open window start: want %s, got %s", openWindowStart, recordedAt.UTC())
		repointed = append(repointed, repointedFrom.UTC())
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []time.Time{originalRecordedAt, secondRecordedAt}, repointed)

	orgs, err = store.OrgsWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Empty(t, orgs, "a successful attach must remove the org from the work list")
}

func TestOrgsWithUnsweptUsage_ExcludesUnactivatedOrgAccount(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	orgID, accountID, appID, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id, activated_at)
		VALUES ($1, 'org', $2, NULL)`, accountID.String(), orgID.String())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations (org_id, funding, updated_by)
		VALUES ($1, 'org', $2)`, orgID.String(), uuid.NewString())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, owner_org_id, module_count, created_module_count, created_at
		) VALUES ($1, NULL, $2, 0, 0, $3)`, appID.String(), orgID.String(), mustTime(t, pStart))
	require.NoError(t, err)
	seedMetricDef(t, pool, moduleID, "orders.placed", usage.KindCount, 1_000)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 1, $4)`,
		uuid.NewString(), appID.String(), moduleID.String(), mustTime(t, pStart))
	require.NoError(t, err)

	orgs, err := store.OrgsWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.NotContains(t, orgs, orgID)
}
