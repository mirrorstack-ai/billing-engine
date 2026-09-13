//go:build integration

package usage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// Migrations 079 (the 'deploy' display group) and 080 (the CDN request and R2
// read seeds, and the regroup of the deploy-side keys). DATABASE claims: the
// enum value exists, the two rows are seeded at the ruled prices in the group,
// and the four existing deploy-side keys moved into it while the retired
// infra.egress.bytes did not.
func TestMigration080_DeployGroupAndRequestSeeds(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	var hasDeploy bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid
		 WHERE t.typname = 'metric_group' AND e.enumlabel = 'deploy')`).Scan(&hasDeploy))
	require.True(t, hasDeploy, "079 adds the 'deploy' value")

	type row struct {
		kind, unit, group string
		price             int64
		active            bool
	}
	read := func(metric string) row {
		var r row
		require.NoError(t, pool.QueryRow(ctx,
			`SELECT kind::text, unit, display_group::text, unit_price_micros, active
			 FROM ms_billing.metric_definitions
			 WHERE module_id = '00000000-0000-0000-0000-000000000000' AND metric = $1`, metric).
			Scan(&r.kind, &r.unit, &r.group, &r.price, &r.active))
		return r
	}
	require.Equal(t, row{"count", "per-1k-requests", "deploy", 300, true}, read("infra.cdn.request.count"), "CF requests $0.30/M as raw COGS per 1k")
	require.Equal(t, row{"count", "per-1k-reads", "deploy", 360, true}, read("infra.cdn.r2.read.count"), "R2 class-B $0.36/M as raw COGS per 1k")
	for _, m := range []string{"infra.egress.cdn.bytes", "infra.compute.ssr.gb_seconds", "infra.compute.ssr.request.count", "infra.compute.ssr.egress.bytes"} {
		require.Equal(t, "deploy", read(m).group, "%s joins the deploy group", m)
	}
	require.Equal(t, "network", read("infra.egress.bytes").group, "the retired key is history, not a deploy line")
	require.Equal(t, "network", read("infra.egress.api.bytes").group, "platform-side API egress stays out of 部署用量 (owner scope)")
}
