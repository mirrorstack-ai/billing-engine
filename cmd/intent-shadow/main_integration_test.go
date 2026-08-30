//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/intent/shadow"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

// A shadow run against a period the catalog prices exactly must agree.
// If it cannot agree when everything lines up, every real difference it
// reports later is noise.
func TestAPeriodPricedFromTheCatalogAgrees(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	seedPricedPeriod(t, pool, seed{
		metric: "quiz.render", version: "1.4.0",
		quantity: 1_000, catalogPrice: 25,
		chargedMicros: 1_000 * 25, // what the legacy rollup recorded
	})

	report, err := run(ctx, shadow.NewSource(pool), 10)
	require.NoError(t, err)
	require.Equal(t, 1, report.Compared)
	require.Equal(t, 1, report.Agreed)
	require.Zero(t, report.Unexplained())
	require.True(t, report.Ready())
}

// 🔴 The case the whole exercise exists for: the legacy path charged
// something the catalog does not derive. The run must report it, and
// must NOT be ready.
func TestAChargeTheCatalogDoesNotDeriveIsReported(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	seedPricedPeriod(t, pool, seed{
		metric: "quiz.render", version: "1.4.0",
		quantity: 1_000, catalogPrice: 25,
		// 20% more than quantity x catalog price — the shape of the
		// infrastructure markup docs/SECURITY.md §2 records.
		chargedMicros: 1_000 * 25 * 12 / 10,
	})

	report, err := run(ctx, shadow.NewSource(pool), 10)
	require.NoError(t, err)
	require.Equal(t, 1, report.Compared)
	require.Equal(t, 1, report.Unexplained())
	require.False(t, report.Ready(), "a cutover would have been permitted over an unexplained difference")

	require.Contains(t, report.String(), "unexplained")
	require.Contains(t, report.String(), "delta=-5000",
		"the report does not say how far apart the two figures are")
}

// A meter the catalog has no price for must quarantine and be counted
// as a whole-amount difference, not skipped. Skipping would remove the
// periods the rater cannot handle from the count meant to reveal them.
func TestAnUnpricedMeterIsCountedNotSkipped(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	s := seed{
		metric: "quiz.render", version: "1.4.0",
		quantity: 1_000, catalogPrice: 25, chargedMicros: 25_000,
	}
	s.usageMetric = "quiz.export" // charged, but absent from the catalog
	seedPricedPeriod(t, pool, s)

	report, err := run(ctx, shadow.NewSource(pool), 10)
	require.NoError(t, err)
	require.Equal(t, 1, report.Compared, "the quarantined period was dropped from the comparison")
	require.Equal(t, 1, report.Unexplained())
	require.Contains(t, report.String(), "quarantined")
}

// Nothing to compare is not a pass.
func TestAnEmptyDatabaseIsNotReady(t *testing.T) {
	pool := testutil.NewTestDB(t)

	report, err := run(context.Background(), shadow.NewSource(pool), 10)
	require.NoError(t, err)
	require.Zero(t, report.Compared)
	require.False(t, report.Ready(), "a run that compared nothing reported itself ready")
	require.Contains(t, report.String(), "NOT READY")
}

// An empty catalog would quarantine every period and report a clean
// sheet of nothing, so it is refused outright.
func TestAnEmptyCatalogIsAnError(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()

	s := seed{metric: "quiz.render", version: "1.4.0", quantity: 10, chargedMicros: 100}
	s.skipCatalog = true
	seedPricedPeriod(t, pool, s)

	_, err := run(ctx, shadow.NewSource(pool), 10)
	require.Error(t, err)
	require.Contains(t, err.Error(), "price catalog is empty")
}

// --- fixtures ---

type seed struct {
	metric        string
	usageMetric   string
	version       string
	quantity      int64
	catalogPrice  int64
	chargedMicros int64
	skipCatalog   bool
	// rawCost is the pre-markup base. Zero means "no markup".
	rawCost int64
}

// rawCostMicros is the PRE-markup figure the real rollup writes beside
// charged_micros (migrations/billing/009_usage_aggregates.up.sql:28:
// charged_micros = round_half_up(raw_cost_micros * num / den)).
//
// A fixture that leaves it at its DEFAULT 0 is not a smaller fixture, it
// is a different one: the shadow comparison is against the base, so a
// zero base makes every delta equal the whole shadow figure. Tests that
// omitted it were measuring the default, not the metric.
//
// Unless a case sets it explicitly, there is no markup and base ==
// charged, which is what an ordinary non-infra metric looks like.
func (s seed) rawCostMicros() int64 {
	if s.rawCost != 0 {
		return s.rawCost
	}
	return s.chargedMicros
}

func seedPricedPeriod(t *testing.T, pool *pgxpool.Pool, s seed) {
	t.Helper()
	ctx := context.Background()

	accountID, moduleID, appID := uuid.New(), uuid.New(), uuid.New()
	start := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1,'user',$2,$3)`, accountID, accountID, "cus_shadow")
	require.NoError(t, err)

	var periodID uuid.UUID
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO ms_billing.billing_periods (account_id, period_start, period_end)
		 VALUES ($1,$2,$3) RETURNING id`, accountID, start, end).Scan(&periodID))

	usageMetric := s.usageMetric
	if usageMetric == "" {
		usageMetric = s.metric
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.usage_aggregates
		   (period_id, account_id, app_id, module_id, metric, module_version,
		    kind, billable_quantity, unit_price_micros, raw_cost_micros, charged_micros)
		 VALUES ($1,$2,$3,$4,$5,$6,'count',$7,$8,$9,$10)`,
		periodID, accountID, appID, moduleID, usageMetric, s.version,
		s.quantity, s.catalogPrice, s.rawCostMicros(), s.chargedMicros)
	require.NoError(t, err)

	if !s.skipCatalog {
		_, err = pool.Exec(ctx,
			`INSERT INTO ms_billing.metric_version_prices
			   (module_id, metric, module_version, unit_price_micros)
			 VALUES ($1,$2,$3,$4)`,
			moduleID, s.metric, s.version, s.catalogPrice)
		require.NoError(t, err)
	}
}
