//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// seedLazyRow writes one NULL-account usage row in the ingest-written v2 shape
// (billable_at = occurred_at = recorded_at = at). ownerUserID is the migration
// 088 stamp — nil writes the unstamped row every pre-088 or non-user lazy row
// is. A non-empty subject makes it a subject-keyed peak observation.
func seedLazyRow(
	t *testing.T, pool *pgxpool.Pool, appID, moduleID uuid.UUID, ownerUserID any,
	metric string, kind usage.Kind, value float64, at time.Time, subject string, devServed bool,
) string {
	t.Helper()
	eventID := uuid.NewString()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO ms_billing.usage_events
		    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at,
		     observation_version, subject, occurred_at, billable_at, aggregation_key,
		     payload_fingerprint, occurrence_policy, dev_served, owner_user_id)
		VALUES ($1, NULL, $2, $3, $4, $5, $6, $7,
		        2, NULLIF($8::text, ''), $7, $7,
		        CASE WHEN $8::text = '' THEN NULL ELSE 'subject' END,
		        $9, 'on_time', $10, $11)`,
		eventID, appID, moduleID, metric, string(kind), value, at, subject,
		make([]byte, 32), devServed, ownerUserID)
	require.NoError(t, err)
	return eventID
}

// The user attach sweep against real Postgres (migration 088, core-v2#340): a
// stamped row inside the account's open window is repointed; a stamped row
// older than the window stays NULL (D1d); a row stamped for another user and
// an unstamped NULL row are never touched; and the disclosure figure prices the
// user's rows exactly as the org disclosure prices the same rows.
func TestRepointUserNullAccountEvents_OpenWindowOnlyKeyedOnTheStamp(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	userID, accountID := uuid.New(), uuid.New()
	otherUserID, otherAccountID := uuid.New(), uuid.New()
	userApp, orgApp, orgID, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	// Activated 2026-07-06 12:00 → anchor day 6; at `now` the open window is
	// [2026-07-06, 2026-08-06). The lazy rows are what ingest wrote before the
	// user's account row existed — the in-window one earlier the same UTC day.
	activatedAt := mustTime(t, "2026-07-06T12:00:00Z")
	now := mustTime(t, "2026-07-20T12:00:00Z")
	windowStart := mustTime(t, "2026-07-06T00:00:00Z")
	inWindowAt := mustTime(t, "2026-07-06T08:00:00Z")
	beforeWindowAt := mustTime(t, "2026-07-05T23:00:00Z")

	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, $3), ($4, 'user', $5, NULL)`,
		accountID, userID, activatedAt, otherAccountID, otherUserID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps
		    (app_id, account_id, owner_org_id, module_count, created_module_count, created_at)
		VALUES ($1, $2, NULL, 0, 0, $4), ($3, NULL, $5, 0, 0, $4)`,
		userApp, accountID, orgApp, mustTime(t, "2026-06-01T00:00:00Z"), orgID)
	require.NoError(t, err)
	seedMetricDef(t, pool, moduleID, "orders.placed", usage.KindCount, 1_000)
	seedMetricDef(t, pool, moduleID, "users.monthly_active", usage.KindPeak, 100)
	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.metric_definitions SET aggregation_key='subject'
		WHERE module_id=$1 AND metric='users.monthly_active'`, moduleID)
	require.NoError(t, err)

	// The user's priced rows, seeded twice: stamped on the user's app, and
	// unstamped on an org-rostered app so OrgUnbilledBacklogMicros prices the
	// identical set — the parity the user query claims.
	type pricedRow struct {
		metric  string
		kind    usage.Kind
		value   float64
		at      time.Time
		subject string
		dev     bool
	}
	priced := []pricedRow{
		{"orders.placed", usage.KindCount, 3, inWindowAt, "", false},     // 3 000, pending
		{"orders.placed", usage.KindCount, 4, beforeWindowAt, "", false}, // 4 000, D1d
		{"users.monthly_active", usage.KindPeak, 1, inWindowAt, "end-user", false},
		{"users.monthly_active", usage.KindPeak, 2, inWindowAt.Add(time.Hour), "end-user", false}, // peak 2 → 200
		{"orders.placed", usage.KindCount, 7, inWindowAt, "", true},                               // dev_served: never priced
	}
	var stamped []string
	for _, r := range priced {
		stamped = append(stamped, seedLazyRow(t, pool, userApp, moduleID, userID, r.metric, r.kind, r.value, r.at, r.subject, r.dev))
		seedLazyRow(t, pool, orgApp, moduleID, nil, r.metric, r.kind, r.value, r.at, r.subject, r.dev)
	}
	inWindowID, beforeWindowID := stamped[0], stamped[1]
	otherUserRow := seedLazyRow(t, pool, userApp, moduleID, otherUserID, "orders.placed", usage.KindCount, 5, inWindowAt, "", false)
	unstampedRow := seedLazyRow(t, pool, userApp, moduleID, nil, "orders.placed", usage.KindCount, 6, inWindowAt, "", false)

	all, err := store.UserUnbilledBacklogMicros(ctx, userID, time.Time{})
	require.NoError(t, err)
	require.Equal(t, int64(7_200), all)
	orgAll, err := store.OrgUnbilledBacklogMicros(ctx, orgID)
	require.NoError(t, err)
	require.Equal(t, orgAll, all, "the user disclosure prices a row set exactly as the org disclosure does")
	pending, err := store.UserUnbilledBacklogMicros(ctx, userID, windowStart)
	require.NoError(t, err)
	require.Equal(t, int64(3_200), pending)

	// The unactivated other user is never on the work list, however recent
	// its stamped rows.
	work, err := store.UserAccountsWithUnsweptUsage(ctx, now)
	require.NoError(t, err)
	require.Equal(t, []cycle.UserUsageAccount{{AccountID: accountID, UserID: userID}}, work)

	summary, err := cycle.NewService(store, nil).WithNow(func() time.Time { return now }).
		SweepUnattachedUserUsage(ctx)
	require.NoError(t, err)
	// The three priced in-window rows and the dev_served one: the rollup, not
	// the repoint, is what declines to bill a tunnel-served row.
	require.Equal(t, &cycle.UserUsageSweepSummary{Accounts: 1, Swept: 1, RepointedEvents: 4}, summary)

	accountOf := func(eventID string) *uuid.UUID {
		var id *uuid.UUID
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT account_id FROM ms_billing.usage_events WHERE event_id = $1`, eventID).Scan(&id))
		return id
	}
	require.Equal(t, &accountID, accountOf(inWindowID))
	require.Nil(t, accountOf(beforeWindowID), "D1d: a row older than the open window is never caught up")
	require.Nil(t, accountOf(otherUserRow), "a row stamped for another user is not this account's")
	require.Nil(t, accountOf(unstampedRow), "an unstamped row is never attributed through the roster")

	// On an ingest-written row inside the window the verbatim SET changes
	// nothing but the account: billable_at already sits in the window, the
	// receipt is not clamped, and repointed_from stays unset.
	var billableAt, recordedAt time.Time
	var repointedFrom *time.Time
	var policy string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT billable_at, recorded_at, repointed_from, occurrence_policy
		FROM ms_billing.usage_events WHERE event_id = $1`, inWindowID).Scan(
		&billableAt, &recordedAt, &repointedFrom, &policy))
	require.True(t, inWindowAt.Equal(billableAt))
	require.True(t, inWindowAt.Equal(recordedAt))
	require.Nil(t, repointedFrom)
	require.Equal(t, "on_time", policy)

	left, err := store.UserUnbilledBacklogMicros(ctx, userID, time.Time{})
	require.NoError(t, err)
	require.Equal(t, int64(4_000), left, "only the D1d row is left unbilled")
	work, err = store.UserAccountsWithUnsweptUsage(ctx, now)
	require.NoError(t, err)
	require.Empty(t, work, "a row older than any open window drops the account from the work list")

	again, err := store.RepointUserNullAccountEvents(ctx, userID, accountID, windowStart)
	require.NoError(t, err)
	require.Zero(t, again, "idempotent: a swept row never matches again")
}
