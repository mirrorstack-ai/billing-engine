//go:build integration

package cycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/meteringlock"
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
	v2RecordedAt := mustTime(t, "2026-07-10T10:00:00Z")
	v2OccurredAt := mustTime(t, "2026-06-30T10:00:00Z")
	firstEventID, secondEventID, v2EventID := uuid.NewString(), uuid.NewString(), uuid.NewString()
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
		firstEventID, appID.String(), moduleID.String(), originalRecordedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 4, $4)`,
		secondEventID, appID.String(), moduleID.String(), secondRecordedAt)
	require.NoError(t, err)
	// The receipt is already inside the future funded window, but v2 occurrence
	// predates it. The sweep must still clamp billing time to the first funded
	// period while preserving occurred_at for audit.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
		     observation_version, occurred_at, billable_at, payload_fingerprint, occurrence_policy)
		VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', 5, $4,
		        2, $5, $5, $6, 'late_open')`,
		v2EventID, appID.String(), moduleID.String(), v2RecordedAt, v2OccurredAt, make([]byte, 32))
	require.NoError(t, err)

	orgs, err := store.OrgsWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{orgID}, orgs)

	summary, err := cycle.NewService(store, nil).WithNow(func() time.Time { return now }).
		SweepUnattachedOrgUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, &cycle.OrgUsageSweepSummary{
		Orgs: 1, Swept: 1, AttachedApps: 1, RepointedEvents: 3,
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
		FROM ms_billing.usage_events WHERE event_id IN ($1, $2)
		ORDER BY repointed_from`, firstEventID, secondEventID)
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

	var v2AccountID uuid.UUID
	var storedReceipt, storedOccurrence, billableAt time.Time
	var repointedFrom *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT account_id, recorded_at, occurred_at, repointed_from, billable_at
		FROM ms_billing.usage_events WHERE event_id = $1`, v2EventID).Scan(
		&v2AccountID, &storedReceipt, &storedOccurrence, &repointedFrom,
		&billableAt,
	))
	require.Equal(t, accountID, v2AccountID)
	require.True(t, v2RecordedAt.Equal(storedReceipt), "receipt inside the window remains unchanged")
	require.True(t, v2OccurredAt.Equal(storedOccurrence), "original occurrence remains immutable audit evidence")
	require.Nil(t, repointedFrom, "migration-041 receipt clamp marker stays scoped to changed receipts")
	require.True(t, openWindowStart.Equal(billableAt), "v2 usage bills in the first funded period")

	orgs, err = store.OrgsWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Empty(t, orgs, "a successful attach must remove the org from the work list")
}

func TestRepointOrgUsage_QueuedBehindRollupAdvancesToOpenPeriod(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	orgID, accountID, appID, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	activatedAt := mustTime(t, "2026-07-06T12:00:00Z")
	now := mustTime(t, "2026-07-20T12:00:00Z")
	oldStart := mustTime(t, "2026-07-06T00:00:00Z")
	oldEnd := mustTime(t, "2026-08-06T00:00:00Z")
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_org_id, activated_at)
		VALUES ($1, 'org', $2, $3)`, accountID, orgID, activatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.org_billing_designations (org_id, funding, updated_by)
		VALUES ($1, 'org', $2)`, orgID, uuid.New())
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count, created_module_count, created_at)
		VALUES ($1, NULL, $2, 0, 0, $3)`, appID, orgID, mustTime(t, "2026-06-01T00:00:00Z"))
	require.NoError(t, err)
	seedMetricDef(t, pool, moduleID, "orders.placed", usage.KindCount, 1_000)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
		     observation_version, occurred_at, billable_at, payload_fingerprint, occurrence_policy)
		VALUES ('org-rollup-race', NULL, $1, $2, 'orders.placed', 'count', 1, $3,
		        2, $4, $4, $5, 'late_open')`,
		appID, moduleID, mustTime(t, "2026-07-10T00:00:00Z"),
		mustTime(t, "2026-06-20T00:00:00Z"), make([]byte, 32))
	require.NoError(t, err)
	_, err = store.OpenPeriodForAccount(ctx, accountID, oldStart, oldEnd)
	require.NoError(t, err)

	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, meteringlock.AdvisorySQL, meteringlock.PeriodKey(accountID, oldStart))
	require.NoError(t, err)
	rawDone := make(chan error, 1)
	go func() {
		rows, rawErr := store.RawAggregates(ctx, accountID, oldStart, oldEnd)
		if rawErr == nil && len(rows) != 0 {
			rawErr = errors.New("NULL-account backlog entered pre-repoint rollup")
		}
		rawDone <- rawErr
	}()
	waitForCycleAdvisoryWaiters(t, pool, 1)

	sweepDone := make(chan error, 1)
	go func() {
		_, sweepErr := cycle.NewService(store, nil).WithNow(func() time.Time { return now }).
			SweepUnattachedOrgUsage(ctx)
		sweepDone <- sweepErr
	}()
	waitForCycleAdvisoryWaiters(t, pool, 2)
	require.NoError(t, blocker.Commit(ctx))
	require.NoError(t, <-rawDone)
	require.NoError(t, <-sweepDone)

	var billableAt time.Time
	var policy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT billable_at, occurrence_policy
		FROM ms_billing.usage_events WHERE event_id='org-rollup-race'`).Scan(&billableAt, &policy))
	require.True(t, oldEnd.Equal(billableAt),
		"repoint queued behind closing advances to the next open period")
	require.Equal(t, "first_funded", policy)
}

func waitForCycleAdvisoryWaiters(t *testing.T, pool *pgxpool.Pool, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiters int
		if err := pool.QueryRow(context.Background(), `
			SELECT count(*) FROM pg_locks
			WHERE locktype='advisory' AND NOT granted`).Scan(&waiters); err != nil {
			return false
		}
		return waiters >= want
	}, 3*time.Second, 10*time.Millisecond)
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

func TestOrgUnbilledBacklog_KeyedSubjectIsAppScoped(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()
	orgID, appA, appB, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	for _, appID := range []uuid.UUID{appA, appB} {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.apps
			    (app_id, account_id, owner_org_id, module_count, created_module_count, created_at)
			VALUES ($1, NULL, $2, 0, 0, '2027-02-01T00:00:00Z')`, appID, orgID)
		require.NoError(t, err)
	}
	seedMetricDef(t, pool, moduleID, "users.monthly_active", usage.KindPeak, 100)
	_, err := pool.Exec(ctx, `
		UPDATE ms_billing.metric_definitions
		SET aggregation_key='subject'
		WHERE module_id=$1 AND metric='users.monthly_active'`, moduleID)
	require.NoError(t, err)

	for _, appID := range []uuid.UUID{appA, appB} {
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.usage_events
			    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
			     observation_version, subject, metadata, occurred_at, billable_at,
			     aggregation_key, payload_fingerprint, occurrence_policy)
			VALUES ($1, NULL, $2, $3, 'users.monthly_active', 'peak', 1, '2027-02-10T09:00:00Z',
			        2, 'same-end-user', '{}'::json, '2027-02-10T09:00:00Z',
			        '2027-02-10T09:00:00Z', 'subject', $4, 'on_time')`,
			uuid.NewString(), appID, moduleID, make([]byte, 32))
		require.NoError(t, err)
	}

	backlog, err := store.OrgUnbilledBacklogMicros(ctx, orgID)
	require.NoError(t, err)
	require.Equal(t, int64(200), backlog,
		"the same subject contributes once in each authoritative app scope")
}
