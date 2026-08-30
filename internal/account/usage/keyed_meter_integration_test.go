//go:build integration

package usage_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/meteringlock"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func keyedIntegrationAccount(t *testing.T, pool *pgxpool.Pool, activatedAt time.Time) (uuid.UUID, uuid.UUID) {
	t.Helper()
	accountID, ownerID := uuid.New(), uuid.New()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, $3)`, accountID, ownerID, activatedAt)
	require.NoError(t, err)
	return accountID, ownerID
}

func requireBillingCode(t *testing.T, err error, code billing.Code) {
	t.Helper()
	require.Error(t, err)
	var billErr *billing.Error
	require.True(t, errors.As(err, &billErr))
	require.Equal(t, code, billErr.Code)
}

func TestKeyedMeterObservation_PostgresEndToEnd(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerID := keyedIntegrationAccount(t, pool, time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC))
	appID, moduleID := uuid.New(), uuid.New()
	usageStore := usage.NewStore(pool)
	now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	usageSvc := usage.NewService(usageStore).WithNow(func() time.Time { return now })
	_, err := usageSvc.SetMetricDefinitions(ctx, usage.SetMetricDefinitionsRequest{
		ModuleID: moduleID,
		Metrics: []usage.MetricDef{{
			Metric: "users.monthly_active", Kind: usage.KindPeak,
			AggregationKey: usage.AggregationKeySubject,
			Unit:           "user", UnitPriceMicros: 100, Priced: true, Active: true,
		}},
	})
	require.NoError(t, err)

	record := func(id, subject, version string, value float64, occurredAt time.Time, metadata string) (*usage.RecordUsageResponse, error) {
		return usageSvc.RecordUsage(ctx, usage.RecordUsageRequest{
			Version: 2, EventID: id, AppID: appID, ModuleID: moduleID,
			OwnerUserID: ownerID, Metric: "users.monthly_active", Value: value,
			Subject: subject, Metadata: json.RawMessage(metadata), OccurredAt: occurredAt,
			RecordedAt: now, ModuleVersion: version,
		})
	}

	augustAt := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	first, err := record("aug-a-google", "user-a", "", 1, augustAt, `{"provider":"google","attempt":1}`)
	require.NoError(t, err)
	require.True(t, first.Recorded)
	_, err = record("aug-a-github", "user-a", "", 1, augustAt.Add(time.Hour), `{"provider":"github"}`)
	require.NoError(t, err)
	_, err = record("aug-b", "user-b", "", 1, augustAt.Add(2*time.Hour), `{}`)
	require.NoError(t, err)

	// Semantic metadata order/number spelling is idempotent; changing any
	// canonical field under the same id is a conflict.
	retry, err := record("aug-a-google", "user-a", "", 1, augustAt, ` {"attempt":1.0,"provider":"google"} `)
	require.NoError(t, err)
	require.False(t, retry.Recorded)
	// kind/aggregation mode are mutable catalog snapshots, not wire identity.
	// A later catalog edit must not turn the identical delivery into a conflict.
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.metric_definitions
		SET kind='count', aggregation_key=NULL
		WHERE module_id=$1 AND metric='users.monthly_active'`, moduleID)
	require.NoError(t, err)
	retry, err = record("aug-a-google", "user-a", "", 1, augustAt, `{"provider":"google","attempt":1}`)
	require.NoError(t, err)
	require.False(t, retry.Recorded)
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.metric_definitions
		SET kind='peak', aggregation_key='subject'
		WHERE module_id=$1 AND metric='users.monthly_active'`, moduleID)
	require.NoError(t, err)
	_, err = record("aug-a-google", "user-a", "", 1, augustAt, `{"attempt":2,"provider":"google"}`)
	requireBillingCode(t, err, billing.CodeConflict)

	var storedMetadata string
	var storedOccurred time.Time
	var fingerprintBytes int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT metadata::text, occurred_at, octet_length(payload_fingerprint)
		FROM ms_billing.usage_events WHERE event_id = 'aug-a-google'`).
		Scan(&storedMetadata, &storedOccurred, &fingerprintBytes))
	require.JSONEq(t, `{"attempt":1,"provider":"google"}`, storedMetadata)
	require.True(t, storedOccurred.Equal(augustAt))
	require.Equal(t, 32, fingerprintBytes)

	// Live reconciliation uses keyed MAX already: provider duplicates for A
	// plus B are two users, not three raw events.
	live, err := usageSvc.GetAppUsageSummary(ctx, usage.GetAppUsageSummaryRequest{OwnerUserID: ownerID, AppID: appID})
	require.NoError(t, err)
	require.Len(t, live.Metrics, 1)
	require.Equal(t, 2.0, live.Metrics[0].BillableQuantity)
	require.Equal(t, int64(200), live.Metrics[0].ChargedMicros)

	cycleSvc := cycle.NewService(cycle.NewStore(pool), nil)
	augustStart := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	augustEnd := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	rolled, err := cycleSvc.RollupPeriod(ctx, accountID, augustStart, augustEnd)
	require.NoError(t, err)
	require.Len(t, rolled.Aggregates, 1)
	require.Equal(t, usage.AggregationKeySubject, rolled.Aggregates[0].AggregationKey)
	require.Equal(t, "2", rolled.Aggregates[0].BillableQuantity)
	require.Equal(t, int64(200), rolled.Aggregates[0].ChargedMicros)
	require.Nil(t, rolled.Aggregates[0].ActiveSeconds, "cardinality peak is never level-window prorated")
	require.Nil(t, rolled.Aggregates[0].PeriodDays)

	// The rolled branch reconciles to the same exact amount.
	rolledRead, err := usageSvc.GetAppUsageSummary(ctx, usage.GetAppUsageSummaryRequest{OwnerUserID: ownerID, AppID: appID})
	require.NoError(t, err)
	require.Len(t, rolledRead.Metrics, 1)
	require.Equal(t, live.Metrics[0].BillableQuantity, rolledRead.Metrics[0].BillableQuantity)
	require.Equal(t, live.Metrics[0].ChargedMicros, rolledRead.Metrics[0].ChargedMicros)

	// Identical accepted retry stays idempotent after closing. A new payload in
	// the closed period is rejected and audited without its diagnostic metadata.
	retry, err = record("aug-a-google", "user-a", "", 1, augustAt, `{"provider":"google","attempt":1}`)
	require.NoError(t, err)
	require.False(t, retry.Recorded)
	_, err = record("aug-closed", "user-c", "", 1, augustAt, `{"secret":"must-not-persist"}`)
	requireBillingCode(t, err, billing.CodeConflict)

	futureAt := now.Add(5*time.Minute + time.Nanosecond)
	_, err = record("future-rejected", "user-future", "", 1, futureAt, `{"secret":"also-not-persisted"}`)
	requireBillingCode(t, err, billing.CodeInvalidInput)
	// Same rejected id + same payload is a stable rejection without audit spam.
	_, err = record("future-rejected", "user-future", "", 1, futureAt, `{"secret":"also-not-persisted"}`)
	requireBillingCode(t, err, billing.CodeInvalidInput)
	_, err = record("future-rejected", "user-other", "", 1, futureAt, `{"secret":"changed"}`)
	requireBillingCode(t, err, billing.CodeConflict)
	// The rejection reserves the id globally, including against the legacy
	// insertion path; v1 cannot silently claim it with different semantics.
	_, err = usageStore.InsertUsageEvent(ctx, usage.UsageEvent{
		EventID: "future-rejected", AccountID: accountID, AppID: appID,
		ModuleID: moduleID, Metric: "users.monthly_active", Kind: usage.KindPeak,
		Value: 1, RecordedAt: now,
	})
	require.ErrorIs(t, err, usage.ErrUsageEventConflict)
	pastAt := now.Add(-35*24*time.Hour - time.Nanosecond)
	_, err = record("past-rejected", "user-past", "", 1, pastAt, `{"secret":"old"}`)
	requireBillingCode(t, err, billing.CodeInvalidInput)

	rows, err := pool.Query(ctx, `
		SELECT event_id, reason, (to_jsonb(r) ? 'metadata') AS has_metadata
		FROM ms_billing.usage_observation_rejections r
		WHERE event_id IN ('aug-closed', 'future-rejected', 'past-rejected') ORDER BY event_id`)
	require.NoError(t, err)
	defer rows.Close()
	rejections := map[string]string{}
	for rows.Next() {
		var id, reason string
		var hasMetadata bool
		require.NoError(t, rows.Scan(&id, &reason, &hasMetadata))
		require.False(t, hasMetadata)
		rejections[id] = reason
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]string{
		"aug-closed": "period_closed", "future-rejected": "occurred_at_future",
		"past-rejected": "occurred_at_too_old",
	}, rejections)

	var futureAuditCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.usage_observation_rejections WHERE event_id='future-rejected'`).Scan(&futureAuditCount))
	require.Equal(t, 1, futureAuditCount)

	// September proves period independence and the authoritative version/model
	// bill-line boundary: the same subject is deduped inside v1, but counted once
	// again in v2 because it is a distinct immutable price definition.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.metric_version_prices (module_id, metric, module_version, unit_price_micros)
		VALUES ($1, 'users.monthly_active', '1.0.0', 100),
		       ($1, 'users.monthly_active', '2.0.0', 200),
		       ($1, 'users.monthly_active', 'mode', 100)`, moduleID)
	require.NoError(t, err)
	now = time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	septemberAt := time.Date(2026, 9, 2, 10, 0, 0, 0, time.UTC)
	_, err = record("sep-v1-a", "cross-version", "1.0.0", 1, septemberAt, `{"provider":"google"}`)
	require.NoError(t, err)
	_, err = record("sep-v1-b", "cross-version", "1.0.0", 1, septemberAt.Add(time.Hour), `{"provider":"github"}`)
	require.NoError(t, err)
	_, err = record("sep-v2", "cross-version", "2.0.0", 1, septemberAt.Add(2*time.Hour), `{}`)
	require.NoError(t, err)
	_, err = record("sep-repeat-period", "user-a", "", 1, septemberAt.Add(3*time.Hour), `{}`)
	require.NoError(t, err)

	septemberStart := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	septemberEnd := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	september, err := cycleSvc.RollupPeriod(ctx, accountID, septemberStart, septemberEnd)
	require.NoError(t, err)
	require.Len(t, september.Aggregates, 3)
	byVersion := map[string]cycle.MetricAggregate{}
	for _, aggregate := range september.Aggregates {
		byVersion[aggregate.ModuleVersion] = aggregate
		require.Equal(t, "1", aggregate.BillableQuantity)
		require.Equal(t, usage.AggregationKeySubject, aggregate.AggregationKey)
	}
	require.Equal(t, int64(100), byVersion["1.0.0"].ChargedMicros)
	require.Equal(t, int64(200), byVersion["2.0.0"].ChargedMicros)
	require.Equal(t, int64(100), byVersion[""].ChargedMicros)

	// Explicit mid-period mode-change coexistence: a legacy standard-peak line
	// and a keyed line with otherwise identical dimensions must not overwrite.
	now = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	octoberStart := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	octoberEnd := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at, module_version)
		VALUES ('oct-legacy-peak', $1, $2, $3, 'users.monthly_active', 'peak', 5, $4, 'mode')`,
		accountID, appID, moduleID, octoberStart.Add(24*time.Hour))
	require.NoError(t, err)
	_, err = record("oct-key-a", "mode-a", "mode", 1, octoberStart.Add(48*time.Hour), `{}`)
	require.NoError(t, err)
	_, err = record("oct-key-b", "mode-b", "mode", 1, octoberStart.Add(49*time.Hour), `{}`)
	require.NoError(t, err)

	october, err := cycleSvc.RollupPeriod(ctx, accountID, octoberStart, octoberEnd)
	require.NoError(t, err)
	require.Len(t, october.Aggregates, 2)
	byMode := map[usage.AggregationKey]cycle.MetricAggregate{}
	for _, aggregate := range october.Aggregates {
		byMode[aggregate.AggregationKey] = aggregate
	}
	require.Equal(t, "5", byMode[""].BillableQuantity)
	require.Equal(t, int64(500), byMode[""].ChargedMicros)
	require.Equal(t, "2", byMode[usage.AggregationKeySubject].BillableQuantity)
	require.Equal(t, int64(200), byMode[usage.AggregationKeySubject].ChargedMicros)

	// A rerun is deterministic and monotone: still exactly the same two rows.
	_, err = cycleSvc.RollupPeriod(ctx, accountID, octoberStart, octoberEnd)
	require.NoError(t, err)
	var modeRows int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM ms_billing.usage_aggregates ua
		JOIN ms_billing.billing_periods bp ON bp.id=ua.period_id
		WHERE ua.account_id=$1 AND ua.app_id=$2 AND ua.module_id=$3
		  AND ua.metric='users.monthly_active' AND ua.module_version='mode'
		  AND bp.period_start=$4`, accountID, appID, moduleID, octoberStart).Scan(&modeRows))
	require.Equal(t, 2, modeRows)

	// Database constraints independently defend the trust boundary even if an
	// application path is bypassed.
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.usage_events
		(event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
		 observation_version, occurred_at, billable_at, metadata, payload_fingerprint, occurrence_policy)
		VALUES ('bad-metadata', $1,$2,$3,'users.monthly_active','peak',1,$4,2,$4,$4,$5::json,$6,'on_time')`,
		accountID, appID, moduleID, now, `{"value":"`+strings.Repeat("x", 4097)+`"}`, make([]byte, 32))
	require.Error(t, err)
}

func TestKeyedMeterObservation_AppScopedLiveAndRollup(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerID := keyedIntegrationAccount(t, pool,
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	moduleID, appA, appB := uuid.New(), uuid.New(), uuid.New()
	now := time.Date(2027, 2, 20, 12, 0, 0, 0, time.UTC)
	usageService := usage.NewService(usage.NewStore(pool)).WithNow(func() time.Time { return now })
	_, err := usageService.SetMetricDefinitions(ctx, usage.SetMetricDefinitionsRequest{
		ModuleID: moduleID,
		Metrics: []usage.MetricDef{{
			Metric: "users.monthly_active", Kind: usage.KindPeak,
			AggregationKey: usage.AggregationKeySubject,
			Unit:           "user", UnitPriceMicros: 100, Priced: true, Active: true,
		}},
	})
	require.NoError(t, err)

	record := func(id string, appID uuid.UUID, value float64, offset time.Duration) {
		t.Helper()
		response, recordErr := usageService.RecordUsage(ctx, usage.RecordUsageRequest{
			Version: 2, EventID: id, AppID: appID, ModuleID: moduleID,
			OwnerUserID: ownerID, Metric: "users.monthly_active", Value: value,
			Subject: "same-end-user", Metadata: json.RawMessage(`{}`),
			OccurredAt: time.Date(2027, 2, 10, 9, 0, 0, 0, time.UTC).Add(offset),
			RecordedAt: now,
		})
		require.NoError(t, recordErr)
		require.True(t, response.Recorded)
	}
	record("app-a-low", appA, 1, 0)
	record("app-a-high", appA, 2, time.Hour)
	record("app-b-low", appB, 1, 2*time.Hour)
	record("app-b-high", appB, 3, 3*time.Hour)

	// Subject identity is scoped by app. The account summary may sum apps out
	// only after each app has independently contributed MAX(value) for the same
	// opaque subject.
	live, err := usageService.GetUsageSummary(ctx, usage.GetUsageSummaryRequest{OwnerUserID: ownerID})
	require.NoError(t, err)
	require.Len(t, live.Metrics, 1)
	require.Equal(t, 5.0, live.Metrics[0].Quantity)
	require.Equal(t, int64(500), live.Metrics[0].ChargedMicros)

	periodStart := time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC)
	rolled, err := cycle.NewService(cycle.NewStore(pool), nil).
		RollupPeriod(ctx, accountID, periodStart, periodEnd)
	require.NoError(t, err)
	require.Len(t, rolled.Aggregates, 2)
	byApp := map[uuid.UUID]string{}
	for _, aggregate := range rolled.Aggregates {
		byApp[aggregate.AppID] = aggregate.BillableQuantity
	}
	require.Equal(t, map[uuid.UUID]string{appA: "2", appB: "3"}, byApp)

	var rolledQuantity float64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COALESCE(sum(aggregate.billable_quantity), 0)::float8
		FROM ms_billing.usage_aggregates aggregate
		JOIN ms_billing.billing_periods period ON period.id=aggregate.period_id
		WHERE aggregate.account_id=$1 AND period.period_start=$2`, accountID, periodStart).
		Scan(&rolledQuantity))
	require.Equal(t, live.Metrics[0].Quantity, rolledQuantity,
		"live account disclosure and immutable per-app rollup must reconcile")
}

func TestKeyedMeterObservation_AdmissionLockRace(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerID := keyedIntegrationAccount(t, pool, time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC))
	appID, moduleID := uuid.New(), uuid.New()
	usageStore := usage.NewStore(pool)
	now := time.Date(2026, 12, 10, 12, 0, 0, 0, time.UTC)
	usageSvc := usage.NewService(usageStore).WithNow(func() time.Time { return now })
	_, err := usageSvc.SetMetricDefinitions(ctx, usage.SetMetricDefinitionsRequest{
		ModuleID: moduleID,
		Metrics: []usage.MetricDef{{
			Metric: "users.concurrent", Kind: usage.KindPeak,
			AggregationKey: usage.AggregationKeySubject,
			Unit:           "user", UnitPriceMicros: 100, Priced: true, Active: true,
		}},
	})
	require.NoError(t, err)
	record := func(callCtx context.Context, id, subject string) (*usage.RecordUsageResponse, error) {
		return usageSvc.RecordUsage(callCtx, usage.RecordUsageRequest{
			Version: 2, EventID: id, AppID: appID, ModuleID: moduleID,
			OwnerUserID: ownerID, Metric: "users.concurrent", Value: 1,
			Subject: subject, Metadata: json.RawMessage(`{}`),
			OccurredAt: now.Add(-time.Hour), RecordedAt: now,
		})
	}
	first, err := record(ctx, "race-before-rollup", "user-a")
	require.NoError(t, err)
	require.True(t, first.Recorded)

	periodStart := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	periodEnd := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	periodKey := meteringlock.PeriodKey(accountID, periodStart)

	// A held shared admission lock must not serialize an unrelated subject.
	sharedTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = sharedTx.Exec(ctx, meteringlock.SharedAdvisorySQL, periodKey)
	require.NoError(t, err)
	concurrentCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	second, err := record(concurrentCtx, "race-concurrent-ingest", "user-b")
	cancel()
	require.NoError(t, err)
	require.True(t, second.Recorded)
	require.NoError(t, sharedTx.Rollback(ctx))

	cycleStore := cycle.NewStore(pool)
	_, err = cycleStore.OpenPeriodForAccount(ctx, accountID, periodStart, periodEnd)
	require.NoError(t, err)
	blocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, meteringlock.AdvisorySQL, periodKey)
	require.NoError(t, err)

	type rawResult struct {
		rows []cycle.RawAggregate
		err  error
	}
	rawDone := make(chan rawResult, 1)
	go func() {
		rows, rawErr := cycleStore.RawAggregates(ctx, accountID, periodStart, periodEnd)
		rawDone <- rawResult{rows: rows, err: rawErr}
	}()
	waitForAdvisoryWaiters(t, pool, 1)

	type recordResult struct {
		response *usage.RecordUsageResponse
		err      error
	}
	recordDone := make(chan recordResult, 1)
	go func() {
		response, recordErr := record(ctx, "race-behind-rollup", "user-c")
		recordDone <- recordResult{response: response, err: recordErr}
	}()
	waitForAdvisoryWaiters(t, pool, 2)
	require.NoError(t, blocker.Commit(ctx))

	raw := <-rawDone
	require.NoError(t, raw.err)
	require.Len(t, raw.rows, 1)
	require.Equal(t, "2", raw.rows[0].BillableQuantity,
		"both observations committed before the rollup barrier belong to its snapshot")
	queued := <-recordDone
	require.Nil(t, queued.response)
	requireBillingCode(t, queued.err, billing.CodeConflict)
	var rejectionReason string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT reason FROM ms_billing.usage_observation_rejections
		WHERE event_id='race-behind-rollup'`).Scan(&rejectionReason))
	require.Equal(t, "period_closed", rejectionReason,
		"an insert queued behind rollup observes closing and leaves audit evidence")

	// A first-card activation creates no cycle that can ever close the prior
	// anchored window. Preserve occurrence for audit but clamp billing into the
	// first funded period so a valid 35-day late observation is not orphaned.
	clampAccount, clampOwner := keyedIntegrationAccount(t, pool,
		time.Date(2027, 1, 15, 14, 0, 0, 0, time.UTC))
	clampApp := uuid.New()
	clampNow := time.Date(2027, 1, 16, 12, 0, 0, 0, time.UTC)
	clampSvc := usage.NewService(usageStore).WithNow(func() time.Time { return clampNow })
	clampOccurred := time.Date(2027, 1, 10, 9, 0, 0, 0, time.UTC)
	clamped, err := clampSvc.RecordUsage(ctx, usage.RecordUsageRequest{
		Version: 2, EventID: "pre-activation-clamp", AppID: clampApp, ModuleID: moduleID,
		OwnerUserID: clampOwner, Metric: "users.concurrent", Value: 1,
		Subject: "user-pre-funded", Metadata: json.RawMessage(`{}`),
		OccurredAt: clampOccurred, RecordedAt: clampNow,
	})
	require.NoError(t, err)
	require.True(t, clamped.Recorded)
	var storedOccurrence, storedBillable time.Time
	var storedPolicy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT occurred_at, billable_at, occurrence_policy
		FROM ms_billing.usage_events
		WHERE event_id='pre-activation-clamp' AND account_id=$1`, clampAccount).
		Scan(&storedOccurrence, &storedBillable, &storedPolicy))
	require.True(t, clampOccurred.Equal(storedOccurrence))
	require.True(t, time.Date(2027, 1, 15, 0, 0, 0, 0, time.UTC).Equal(storedBillable))
	require.Equal(t, "first_funded", storedPolicy)

	// A period two windows behind can no longer be selected by the standard
	// cycle. Missing billing_period state after an empty/missed sweep must not
	// make that logically closed window look open.
	staleAccount, staleOwner := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, NULL)`, staleAccount, staleOwner)
	require.NoError(t, err)
	staleNow := time.Date(2027, 3, 2, 12, 0, 0, 0, time.UTC)
	staleSvc := usage.NewService(usageStore).WithNow(func() time.Time { return staleNow })
	_, err = staleSvc.RecordUsage(ctx, usage.RecordUsageRequest{
		Version: 2, EventID: "logically-closed-no-row", AppID: uuid.New(), ModuleID: moduleID,
		OwnerUserID: staleOwner, Metric: "users.concurrent", Value: 1,
		Subject: "stale-user", Metadata: json.RawMessage(`{}`),
		OccurredAt: time.Date(2027, 1, 31, 12, 0, 0, 0, time.UTC),
		RecordedAt: staleNow,
	})
	requireBillingCode(t, err, billing.CodeConflict)
	var staleReason string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT reason FROM ms_billing.usage_observation_rejections
		WHERE event_id='logically-closed-no-row' AND account_id=$1`, staleAccount).Scan(&staleReason))
	require.Equal(t, "period_closed", staleReason)
}

func TestKeyedMeterObservation_ActivationRaceAndRewindow(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	usageStore := usage.NewStore(pool)
	moduleID := uuid.New()
	_, err := usage.NewService(usageStore).SetMetricDefinitions(ctx, usage.SetMetricDefinitionsRequest{
		ModuleID: moduleID,
		Metrics: []usage.MetricDef{{
			Metric: "users.monthly_active", Kind: usage.KindPeak,
			AggregationKey: usage.AggregationKeySubject,
			Unit:           "user", UnitPriceMicros: 100, Priced: true, Active: true,
		}},
	})
	require.NoError(t, err)

	// Activation wins after the service's non-locking anchor read but before
	// admission takes the account-row lock. The first attempt must wait, detect
	// the changed immutable anchor, and retry into the first funded window.
	racingAccount, racingOwner := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, NULL)`, racingAccount, racingOwner)
	require.NoError(t, err)
	racingApp := uuid.New()
	racingNow := time.Date(2027, 8, 16, 12, 0, 0, 0, time.UTC)
	racingOccurred := time.Date(2027, 8, 10, 9, 0, 0, 0, time.UTC)
	racingActivation := time.Date(2027, 8, 15, 14, 0, 0, 0, time.UTC)
	racingService := usage.NewService(usageStore).WithNow(func() time.Time { return racingNow })

	activationTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = activationTx.Exec(ctx,
		`UPDATE ms_billing.accounts SET activated_at=$2 WHERE id=$1`,
		racingAccount, racingActivation)
	require.NoError(t, err)

	type activationRecordResult struct {
		response *usage.RecordUsageResponse
		err      error
	}
	recordDone := make(chan activationRecordResult, 1)
	go func() {
		response, recordErr := racingService.RecordUsage(ctx, usage.RecordUsageRequest{
			Version: 2, EventID: "activation-wins-race", AppID: racingApp, ModuleID: moduleID,
			OwnerUserID: racingOwner, Metric: "users.monthly_active", Value: 1,
			Subject: "racing-user", Metadata: json.RawMessage(`{}`),
			OccurredAt: racingOccurred, RecordedAt: racingNow,
		})
		recordDone <- activationRecordResult{response: response, err: recordErr}
	}()
	waitForAccountActivationWaiter(t, pool)
	require.NoError(t, activationTx.Commit(ctx))

	traced := <-recordDone
	require.NoError(t, traced.err)
	require.True(t, traced.response.Recorded)
	var storedOccurred, storedBillable time.Time
	var storedPolicy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT occurred_at, billable_at, occurrence_policy
		FROM ms_billing.usage_events WHERE event_id='activation-wins-race'`).
		Scan(&storedOccurred, &storedBillable, &storedPolicy))
	require.True(t, racingOccurred.Equal(storedOccurred))
	require.True(t, time.Date(2027, 8, 15, 0, 0, 0, 0, time.UTC).Equal(storedBillable))
	require.Equal(t, "first_funded", storedPolicy)

	// Admission can also commit first. The real activation writer waits for its
	// shared account lock, then atomically rewinds every unrolled v2 observation
	// from the provisional calendar window into the first funded window.
	rewindowAccount, rewindowOwner := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, NULL)`, rewindowAccount, rewindowOwner)
	require.NoError(t, err)
	rewindowNow := time.Date(2027, 8, 20, 12, 0, 0, 0, time.UTC)
	rewindowOccurred := time.Date(2027, 8, 10, 9, 0, 0, 0, time.UTC)
	rewindowService := usage.NewService(usageStore).WithNow(func() time.Time { return rewindowNow })
	response, err := rewindowService.RecordUsage(ctx, usage.RecordUsageRequest{
		Version: 2, EventID: "ingest-wins-activation", AppID: uuid.New(), ModuleID: moduleID,
		OwnerUserID: rewindowOwner, Metric: "users.monthly_active", Value: 1,
		Subject: "pre-funded-user", Metadata: json.RawMessage(`{}`),
		OccurredAt: rewindowOccurred, RecordedAt: rewindowNow,
	})
	require.NoError(t, err)
	require.True(t, response.Recorded)

	// The existing card-less monthly rollup may win before activation. It must
	// continue freezing v1 behavior, but leave v2 observations pending so the
	// first funded anchor can be applied deterministically later.
	calendarStart := time.Date(2027, 8, 1, 0, 0, 0, 0, time.UTC)
	calendarEnd := time.Date(2027, 9, 1, 0, 0, 0, 0, time.UTC)
	rewindowActivation := time.Date(2027, 9, 15, 14, 0, 0, 0, time.UTC)
	periodBlocker, err := pool.Begin(ctx)
	require.NoError(t, err)
	_, err = periodBlocker.Exec(ctx, meteringlock.AdvisorySQL,
		meteringlock.PeriodKey(rewindowAccount, calendarStart))
	require.NoError(t, err)
	type activationRollupResult struct {
		summary *cycle.RollupSummary
		err     error
	}
	rollupDone := make(chan activationRollupResult, 1)
	go func() {
		summary, rollupErr := cycle.NewService(cycle.NewStore(pool), nil).
			RollupPeriod(ctx, rewindowAccount, calendarStart, calendarEnd)
		rollupDone <- activationRollupResult{summary: summary, err: rollupErr}
	}()
	waitForAdvisoryWaiters(t, pool, 1)

	activationDone := make(chan error, 1)
	go func() {
		activationDone <- cycle.NewStore(pool).
			ActivateAccountIfUnset(ctx, rewindowAccount, rewindowActivation)
	}()
	waitForNamedLockWaiter(t, pool, "ActivateAccountIfUnset")
	require.NoError(t, periodBlocker.Commit(ctx))

	unactivatedRollup := <-rollupDone
	require.NoError(t, unactivatedRollup.err)
	require.Empty(t, unactivatedRollup.summary.Aggregates,
		"unactivated rollup must leave v2 observations pending activation")
	require.NoError(t, <-activationDone)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT occurred_at, billable_at, occurrence_policy
		FROM ms_billing.usage_events WHERE event_id='ingest-wins-activation'`).
		Scan(&storedOccurred, &storedBillable, &storedPolicy))
	require.True(t, rewindowOccurred.Equal(storedOccurred))
	require.True(t, time.Date(2027, 9, 15, 0, 0, 0, 0, time.UTC).Equal(storedBillable))
	require.Equal(t, "first_funded", storedPolicy)

	// The first funded anchored rollup now sees the moved row exactly once; the
	// old closing calendar period has no keyed aggregate to orphan or charge.
	fundedEnd := time.Date(2027, 10, 15, 0, 0, 0, 0, time.UTC)
	fundedRollup, err := cycle.NewService(cycle.NewStore(pool), nil).RollupPeriod(
		ctx, rewindowAccount, storedBillable, fundedEnd,
	)
	require.NoError(t, err)
	require.Len(t, fundedRollup.Aggregates, 1)
	require.Equal(t, "1", fundedRollup.Aggregates[0].BillableQuantity)
	var oldKeyedAggregates int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*)
		FROM ms_billing.usage_aggregates aggregate
		JOIN ms_billing.billing_periods period ON period.id=aggregate.period_id
		WHERE aggregate.account_id=$1
		  AND period.period_start=$2
		  AND aggregate.aggregation_key='subject'`, rewindowAccount, calendarStart).
		Scan(&oldKeyedAggregates))
	require.Zero(t, oldKeyedAggregates)
}

func waitForAdvisoryWaiters(t *testing.T, pool *pgxpool.Pool, want int) {
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

func waitForAccountActivationWaiter(t *testing.T, pool *pgxpool.Pool) {
	waitForNamedLockWaiter(t, pool, "LockUsageAccountActivation")
}

func waitForNamedLockWaiter(t *testing.T, pool *pgxpool.Pool, queryName string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiters int
		if err := pool.QueryRow(context.Background(), `
			SELECT count(*)
			FROM pg_stat_activity
			WHERE wait_event_type='Lock'
			  AND query LIKE '%' || $1 || '%'`, queryName).Scan(&waiters); err != nil {
			return false
		}
		return waiters >= 1
	}, 3*time.Second, 10*time.Millisecond)
}
