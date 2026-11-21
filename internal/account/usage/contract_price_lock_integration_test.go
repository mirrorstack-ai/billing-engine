//go:build integration

package usage_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// contractPrintedPrecision is how many decimal places every contract_price_usd
// in the 2027-01-01 price book fixture is printed at (checked against the
// fixture directly in TestContractPriceBook_DeployAndStorageRowsMatchDB: none
// of the seven cost-linked rows needs a fifth decimal). "check the printed
// price after rounding at the printed precision" (billing-engine#269) means
// round to this many places before comparing, not compare raw micros.
const contractPrintedPrecision = 4

// TestContractPriceBook_DeployAndStorageRowsMatchDB is the integration half of
// billing-engine#269: it reads the live ms_billing.metric_definitions rows
// behind the five deploy prices (Annex 2 art 3 para 1) and the two cost-linked
// storage prices (Annex 2 art 2 para 1 — video storage, ad asset storage),
// applies the platform-infra x12/10 markup, and checks the result against the
// printed contract price at printed precision. It fails (does not skip) on a
// missing fixture, a missing row, or an inactive row — a cost-linked price the
// engine cannot currently prove is exactly the gap this test exists to catch.
func TestContractPriceBook_DeployAndStorageRowsMatchDB(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	book := loadContractPriceBook(t)

	sentinel := usage.PlatformInfraModuleID().String()
	checked := 0

	for _, row := range book.Entries {
		if row.Source != "engine" || !row.CostLinked || row.Metric == "" {
			continue
		}
		checked++

		var rawMicros int64
		var active bool
		err := pool.QueryRow(ctx,
			`SELECT unit_price_micros, active FROM ms_billing.metric_definitions
			 WHERE module_id = $1 AND metric = $2`,
			sentinel, row.Metric,
		).Scan(&rawMicros, &active)
		require.NoErrorf(t, err, "%s: metric_definitions row for metric %q", row.ID, row.Metric)
		require.Truef(t, active, "%s: metric %q is not active", row.ID, row.Metric)
		require.Positivef(t, row.ScaleFromDBUnit, "%s: fixture is missing scale_from_db_unit", row.ID)

		chargedPerUnit := float64(rawMicros*usage.InfraMarkupNum) / float64(usage.InfraMarkupDen) * float64(row.ScaleFromDBUnit)
		chargedUSD := chargedPerUnit / 1_000_000

		scale := math.Pow(10, contractPrintedPrecision)
		roundedUSD := math.Round(chargedUSD*scale) / scale
		roundedContract := math.Round(row.ContractPriceUSD*scale) / scale

		require.InDeltaf(t, roundedContract, roundedUSD, 1e-9,
			"%s (metric %q): engine charges %.6f USD per %s (raw %d x %d/%d x scale %d), contract prints %.4f",
			row.ID, row.Metric, chargedUSD, row.Unit, rawMicros, usage.InfraMarkupNum, usage.InfraMarkupDen, row.ScaleFromDBUnit, row.ContractPriceUSD)
	}

	require.Equal(t, 7, checked, "expected exactly the five deploy rows and two storage rows to be DB-checkable")
}
