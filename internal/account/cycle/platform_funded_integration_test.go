//go:build integration

package cycle_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/cycle"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded/pftest"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// TB-FUND (billing-engine#348), "creates no charge" against the REAL schema.
// The unit tests prove the create gate admits a platform-funded org as an
// UNBILLED roster row; this proves the other half in Postgres: that row has no
// account, no account is opened for the org, and the charge sweep's own query
// does not select it — while the same query DOES select a billed row, so the
// absence is not an empty-table accident.
func TestPlatformFundedApp_RegistersUnbilledAndTheChargeSweepNeverSelectsIt(t *testing.T) {
	uses := pftest.Capture(t)
	pool := testutil.NewTestDB(t)
	store := cycle.NewStore(pool)
	ctx := context.Background()

	org, appID := uuid.New(), uuid.New()
	set, err := platformfunded.Parse(org.String())
	require.NoError(t, err)
	svc := cycle.NewService(store, newFakeStripe()).WithPlatformOrgs(set)

	resp, err := svc.RegisterApp(ctx, cycle.RegisterAppRequest{
		OwnerOrgID: org, AppID: appID, Plan: "pro", ModuleCount: 1,
		CreatedAt: time.Now().UTC().AddDate(0, 0, -10), // far past grace: a billed twin WOULD be pending
	})
	require.NoError(t, err)
	require.Equal(t, uuid.Nil, resp.AccountID)

	// The path ran: the audit line is the receiving-side evidence.
	require.Equal(t, []pftest.Use{{Gate: platformfunded.GateRegisterApp, OrgID: org.String()}}, uses())

	// The roster row is UNBILLED (account_id NULL), owned by the org.
	var unbilled bool
	var owner string
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT account_id IS NULL, owner_org_id::text FROM ms_billing.apps WHERE app_id = $1`,
		appID.String()).Scan(&unbilled, &owner))
	require.True(t, unbilled)
	require.Equal(t, org.String(), owner)

	// An unbilled row synthesizes no module overage timers (their account_id is
	// NOT NULL): nothing chargeable hangs off it even with modules installed.
	var timers int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.app_module_overage_timers WHERE app_id = $1`, appID.String()).Scan(&timers))
	require.Zero(t, timers)

	// No billing account was opened for the org.
	var accounts int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM ms_billing.accounts WHERE owner_org_id = $1`, org.String()).Scan(&accounts))
	require.Zero(t, accounts)

	// The charge sweep's query: a billed control row is pending, the platform-
	// funded row is not.
	billed := seedPlanChangeApp(t, pool, seedAccount(t, pool), usage.PlanFree, false)
	pending, err := store.AppsPendingProration(ctx, time.Now().UTC())
	require.NoError(t, err)
	require.Contains(t, pending, billed, "control: the sweep query does select a billed row")
	require.NotContains(t, pending, appID, "the platform-funded roster row is never swept")
}
