package cycle

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Service implements the period rollup + developer settlement. It composes a
// Store.
type Service struct {
	store Store
}

// NewService wires a Service. store is required; passing nil panics at the
// first call site.
func NewService(store Store) *Service {
	return &Service{store: store}
}

// RollupPeriod turns the account's raw usage_events for [periodStart,
// periodEnd) into the immutable, priced billable record (usage_aggregates).
// It:
//  1. opens (idempotently) the billing_periods row for the window,
//  2. aggregates events per (app, module, metric) by kind (count/sum → SUM,
//     peak → MAX, time_weighted → integral) — done in SQL,
//  3. prices each aggregate per the pricing plane: a custom metric at its
//     declared unit_price with NO markup (10/10); a reserved infra.* /
//     platform.* metric at cost × 12/10 (those rows arrive in PR #10, but the
//     plane logic is implemented),
//  4. snapshots unit_price + the markup multiplier + raw_cost + charged onto
//     usage_aggregates via an idempotent upsert keyed (period, app, module,
//     metric) — a re-run upserts the identical row, never duplicates.
//
// Money is integer micro-dollars, round-half-up; quantity stays NUMERIC.
func (s *Service) RollupPeriod(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (*RollupSummary, error) {
	if accountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	if periodStart.IsZero() || periodEnd.IsZero() || !periodEnd.After(periodStart) {
		return nil, billing.InvalidInput("period_end must be after period_start")
	}

	periodID, err := s.store.OpenPeriodForAccount(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("open billing period failed", err)
	}

	raws, err := s.store.RawAggregates(ctx, accountID, periodStart, periodEnd)
	if err != nil {
		return nil, billing.Internal("aggregate usage events failed", err)
	}

	summary := &RollupSummary{
		PeriodID:   periodID,
		Aggregates: make([]MetricAggregate, 0, len(raws)),
	}
	for _, raw := range raws {
		priceMicros, _, err := s.store.MetricPriceMicros(ctx, raw.ModuleID, raw.Metric)
		if err != nil {
			return nil, billing.Internal("metric price lookup failed", err)
		}

		// Pricing plane: a reserved infra.* / platform.* metric is a
		// platform-billed line marked up 12/10; every other (custom) metric
		// carries the identity 10/10 (no markup — the platform's third-party
		// cut is the dev-side margin-share at settlement). infra.* metrics are
		// not ingested until PR #10, so in practice this is always custom now;
		// the plane is implemented + defaults safely.
		num, den := customMarkupNum, customMarkupDen
		if isReservedMetric(raw.Metric) {
			num, den = infraMarkupNum, infraMarkupDen
		}

		// raw_cost = quantity × unit_price (the un-marked-up cost, snapshotted
		// for reproducibility); charged applies the markup in ONE rounding pass
		// over the full product (NOT a second round on raw_cost) so a fractional
		// quantity bills the single declared round (see chargedMicros).
		rawCost, err := rawCostMicros(raw.BillableQuantity, priceMicros)
		if err != nil {
			return nil, billing.Internal("compute raw cost failed", err)
		}
		charged, err := chargedMicros(raw.BillableQuantity, priceMicros, num, den)
		if err != nil {
			return nil, billing.Internal("compute charged cost failed", err)
		}

		agg := MetricAggregate{
			AppID:            raw.AppID,
			ModuleID:         raw.ModuleID,
			Metric:           raw.Metric,
			Kind:             raw.Kind,
			BillableQuantity: raw.BillableQuantity,
			UnitPriceMicros:  priceMicros,
			MarkupNum:        num,
			MarkupDen:        den,
			RawCostMicros:    rawCost,
			ChargedMicros:    charged,
		}
		if err := s.store.UpsertUsageAggregate(ctx, periodID, accountID, agg); err != nil {
			return nil, billing.Internal("upsert usage aggregate failed", err)
		}
		summary.Aggregates = append(summary.Aggregates, agg)
		summary.TotalChargedMicros += charged
	}

	return summary, nil
}

// SettleDevelopers accrues the developer revenue-share ledger for a rolled-up
// period. Per module:
//   - income = Σ charged_micros for the module's metrics in the period,
//   - infra  = COGS = 0 for now (platform-infra COGS lands with PR #10),
//   - margin_share = 15% published / 30% private, from module_visibility,
//     defaulting to private (30%) when unknown so the platform never
//     under-collects on a lagging publish,
//   - platform_take    = round_half_up(margin_share × (income − infra)),
//   - developer_owed   = (income − infra) − platform_take,
//   - status           = 'accrued' (payout deferred to B2B2B; developer_id
//     NULL until a module→developer sync exists).
//
// It must run AFTER RollupPeriod for the same period (income reads
// usage_aggregates). Idempotent: a re-run upserts the identical settlement
// rows keyed (period, module).
func (s *Service) SettleDevelopers(ctx context.Context, accountID, periodID uuid.UUID) (*SettlementSummary, error) {
	if accountID == uuid.Nil {
		return nil, billing.InvalidInput("account_id required")
	}
	if periodID == uuid.Nil {
		return nil, billing.InvalidInput("period_id required")
	}

	incomes, err := s.store.ModuleIncome(ctx, periodID)
	if err != nil {
		return nil, billing.Internal("module income query failed", err)
	}

	summary := &SettlementSummary{
		PeriodID:    periodID,
		Settlements: make([]ModuleSettlement, 0, len(incomes)),
	}
	for _, inc := range incomes {
		vis, found, err := s.store.ModuleVisibility(ctx, inc.ModuleID)
		if err != nil {
			return nil, billing.Internal("module visibility lookup failed", err)
		}
		// Default private (30% take) when the visibility is unknown OR carries
		// an unexpected value — err toward the higher platform take so a lagging
		// publish never under-collects (design §7-B).
		num, den := privateTakeNum, privateTakeDen
		class := usage.VisibilityPrivate
		if found && vis == usage.VisibilityPublished {
			num, den = publishedTakeNum, publishedTakeDen
			class = usage.VisibilityPublished
		}

		const infraMicros = 0 // COGS = 0 until PR #10 ingests platform-infra
		base := inc.IncomeMicros - infraMicros
		if base < 0 {
			base = 0 // defensive: income is Σ of non-negative charges
		}
		take, err := takeMicros(base, num, den)
		if err != nil {
			return nil, billing.Internal("compute platform take failed", err)
		}
		owed := base - take

		set := ModuleSettlement{
			ModuleID:            inc.ModuleID,
			IncomeMicros:        inc.IncomeMicros,
			InfraMicros:         infraMicros,
			MarginShareClass:    class,
			PlatformTakeMicros:  take,
			DeveloperOwedMicros: owed,
		}
		if err := s.store.UpsertDeveloperSettlement(ctx, periodID, accountID, set); err != nil {
			return nil, billing.Internal("upsert developer settlement failed", err)
		}
		summary.Settlements = append(summary.Settlements, set)
	}

	return summary, nil
}
