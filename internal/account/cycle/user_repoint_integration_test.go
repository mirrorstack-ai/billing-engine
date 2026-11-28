//go:build integration

package cycle_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// The user twin takes its rostered apps' NULL rows inside the open window,
// leaves older rows NULL and priced as backlog, never touches an org app's
// rows, and drops off the work list once nothing it can take remains.
func TestRepointUserNullAccountEvents_OpenWindowOnlyAndConverges(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	userID, accountID, appID, orgAppID, moduleID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	activatedAt := mustTime(t, "2026-07-06T12:00:00Z")
	windowStart := mustTime(t, "2026-07-06T00:00:00Z") // anchor day 6, as of Jul 20
	_, err := pool.Exec(ctx, `
		INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, activated_at)
		VALUES ($1, 'user', $2, $3)`, accountID.String(), userID.String(), activatedAt)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO ms_billing.apps (
		    app_id, account_id, owner_org_id, module_count, created_module_count, created_at
		) VALUES ($1, $2, NULL, 0, 0, $4), ($3, NULL, $5, 0, 0, $4)`,
		appID.String(), accountID.String(), orgAppID.String(), activatedAt, uuid.NewString())
	require.NoError(t, err)
	seedMetricDef(t, pool, moduleID, "orders.placed", usage.KindCount, 1_000)

	insert := func(app uuid.UUID, value int, recordedAt string) string {
		id := uuid.NewString()
		_, err := pool.Exec(ctx, `
			INSERT INTO ms_billing.usage_events
			    (event_id, account_id, app_id, module_id, metric, kind, value, recorded_at)
			VALUES ($1, NULL, $2, $3, 'orders.placed', 'count', $4, $5)`,
			id, app.String(), moduleID.String(), value, mustTime(t, recordedAt))
		require.NoError(t, err)
		return id
	}
	before := insert(appID, 3, "2026-06-10T08:30:00Z") // before the account's first window
	inside := insert(appID, 4, "2026-07-10T10:00:00Z")
	orgRow := insert(orgAppID, 5, "2026-07-10T10:00:00Z")

	accounts, err := store.UsersWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{accountID}, accounts)

	n, err := store.RepointUserNullAccountEvents(ctx, accountID, windowStart)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	accountOf := func(eventID string) *string {
		var acct *string
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT account_id::text FROM ms_billing.usage_events WHERE event_id = $1`, eventID).Scan(&acct))
		return acct
	}
	require.NotNil(t, accountOf(inside))
	require.Equal(t, accountID.String(), *accountOf(inside))
	require.Nil(t, accountOf(before), "D1d: a row older than the open window stays unbilled")
	require.Nil(t, accountOf(orgRow), "an org app's backlog is the org sweep's")

	backlog, err := store.UserUnbilledBacklogMicros(ctx, accountID, windowStart)
	require.NoError(t, err)
	require.EqualValues(t, 3*1_000, backlog)

	accounts, err = store.UsersWithUnsweptUsage(ctx)
	require.NoError(t, err)
	require.Empty(t, accounts, "the D1d row must not re-list the account every day")

	n, err = store.RepointUserNullAccountEvents(ctx, accountID, windowStart)
	require.NoError(t, err)
	require.Zero(t, n)
}
