package shadow

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// Source reads what the legacy path charged and the usage behind it.
//
// Read-only, structurally: it holds a pool and issues SELECTs, and
// there is no provider client and no writer anywhere in the type.
// docs/DESIGN.md §11 step 3 asks for shadow intents that "notify nobody
// and move no money", and the way to be sure of that is for the code
// to have no way to do either.
type Source struct {
	q Querier
}

// NewSource returns a read-only reader over the legacy billing tables.
func NewSource(pool *pgxpool.Pool) *Source { return &Source{q: pool} }

// Querier is the read surface this package needs: two methods, both reads.
//
// Both *pgxpool.Pool and pgx.Tx satisfy it, which is the point — the ops
// function runs every query inside ONE read-only transaction, and a Source
// bound to a pool would open its own connections outside that guard. Narrowing
// to an interface with no Exec means this package cannot write even by
// accident.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// NewSourceFrom builds a Source over any querier — in practice a read-only
// transaction.
func NewSourceFrom(q Querier) *Source { return &Source{q: q} }

// Period is one closed billing period to reconcile.
type Period struct {
	PeriodID  string
	AccountID string
	Start     time.Time
	End       time.Time
	// LegacyMicros is what the legacy rollup recorded as CHARGED for
	// usage in this period — post-markup.
	LegacyMicros int64

	// LegacyBaseMicros is the same usage PRE-markup.
	//
	// The two are different numbers and comparing the wrong one makes
	// this whole tool lie. migrations/billing/009_usage_aggregates.up.sql:28
	// states the relationship the rollup writes:
	//
	//     charged_micros = round_half_up( raw_cost_micros * num / den )
	//
	// The intent rater derives quantity x unit_price, which is the
	// PRE-markup figure. Comparing it against charged_micros makes every
	// marked-up metric — every platform-infra line carries 12/10 —
	// disagree systematically, for a reason that has nothing to do with
	// the rater. That is not a discrepancy, it is an artefact, and it
	// would swamp the signal this tool exists to produce.
	LegacyBaseMicros int64
}

// ClosedPeriods returns periods whose usage has been rolled up,
// most recent first.
//
// Only periods with a recorded charge are returned: a period with
// nothing charged has nothing to disagree about, and including them
// would inflate the "compared" count with rows that can only agree.
//
// dev_served rows are excluded (migration 073), here and in FactsFor,
// and the two exclusions have to move together. This tool exists to
// compare what the legacy rollup CHARGED against what the intent rater
// derives; tunnel-served usage is charged by neither, so leaving it in
// the legacy side alone would manufacture a discrepancy on every module
// a developer tunnelled — noise in exactly the signal the tool is for.
func (s *Source) ClosedPeriods(ctx context.Context, limit int) ([]Period, error) {
	rows, err := s.q.Query(ctx, `
		SELECT p.id::text, p.account_id::text, p.period_start, p.period_end,
		       COALESCE(SUM(ua.charged_micros), 0)::bigint,
		       COALESCE(SUM(ua.raw_cost_micros), 0)::bigint
		  FROM ms_billing.billing_periods p
		  JOIN ms_billing.usage_aggregates ua ON ua.period_id = p.id
		 WHERE p.period_end < now()
		   AND ua.dev_served = false
		 GROUP BY p.id, p.account_id, p.period_start, p.period_end
		HAVING COALESCE(SUM(ua.charged_micros), 0) > 0
		 ORDER BY p.period_end DESC
		 LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("read closed periods: %w", err)
	}
	defer rows.Close()

	var out []Period
	for rows.Next() {
		var p Period
		if err := rows.Scan(&p.PeriodID, &p.AccountID, &p.Start, &p.End, &p.LegacyMicros, &p.LegacyBaseMicros); err != nil {
			return nil, fmt.Errorf("scan period: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// FactsFor returns the usage of one period as facts the rater accepts.
//
// The aggregate rows are the engine's own record of what happened, so
// this is a projection rather than a reconstruction: quantity, meter,
// module and version come across unchanged. What is deliberately NOT
// carried over is the price — the whole point of shadow rating is to
// derive that again from the catalog and see whether it matches.
func (s *Source) FactsFor(ctx context.Context, p Period) ([]intent.UsageFact, error) {
	rows, err := s.q.Query(ctx, `
		SELECT metric, module_id::text, module_version, billable_quantity
		  FROM ms_billing.usage_aggregates
		 WHERE period_id = $1
		   AND dev_served = false
		 ORDER BY module_id, metric`, p.PeriodID)
	if err != nil {
		return nil, fmt.Errorf("read usage for period %s: %w", p.PeriodID, err)
	}
	defer rows.Close()

	var facts []intent.UsageFact
	for rows.Next() {
		var (
			meter, moduleID, moduleVersion string
			quantity                       float64
		)
		if err := rows.Scan(&meter, &moduleID, &moduleVersion, &quantity); err != nil {
			return nil, fmt.Errorf("scan usage row: %w", err)
		}
		facts = append(facts, intent.UsageFact{
			Subject:       intent.Subject{Kind: "user", ID: p.AccountID},
			Meter:         meter,
			Module:        moduleID,
			ModuleVersion: moduleVersion,
			// billable_quantity is NUMERIC in the legacy schema and an
			// integer in the intent model. Truncating here would hide a
			// real difference in the rounding, so it is rounded and the
			// fractional part becomes part of what reconciliation has
			// to explain.
			Quantity:   int64(quantity + 0.5),
			OccurredAt: p.Start,
			// The aggregate is one row per (period, app, module,
			// metric), so that tuple is its identity.
			IdempotencyKey: p.PeriodID + "/" + moduleID + "/" + meter,
		})
	}
	return facts, rows.Err()
}

// PriceBookFor builds a price book from the authoritative per-version
// catalog.
//
// Prices come from ms_billing.metric_version_prices rather than from
// the usage rows themselves. Using the row's own unit_price_micros
// would make the comparison circular — the rater would reproduce
// whatever was charged and every period would agree, which is a
// reconciliation that cannot fail and therefore says nothing.
func (s *Source) PriceBookFor(ctx context.Context, effectiveFrom time.Time) (intent.PriceBookRevision, error) {
	rows, err := s.q.Query(ctx, `
		SELECT metric, module_id::text, module_version, unit_price_micros
		  FROM ms_billing.metric_version_prices`)
	if err != nil {
		return intent.PriceBookRevision{}, fmt.Errorf("read price catalog: %w", err)
	}
	defer rows.Close()

	prices := map[intent.PriceKey]int64{}
	for rows.Next() {
		var (
			meter, moduleID, moduleVersion string
			unitPrice                      int64
		)
		if err := rows.Scan(&meter, &moduleID, &moduleVersion, &unitPrice); err != nil {
			return intent.PriceBookRevision{}, fmt.Errorf("scan price: %w", err)
		}
		prices[intent.PriceKey{
			Meter: meter, Module: moduleID, ModuleVersion: moduleVersion,
		}] = unitPrice
	}
	if err := rows.Err(); err != nil {
		return intent.PriceBookRevision{}, err
	}
	if len(prices) == 0 {
		return intent.PriceBookRevision{}, fmt.Errorf(
			"the price catalog is empty; a shadow run against it would quarantine every period " +
				"and report a clean sheet of nothing")
	}

	return intent.NewPriceBookRevision(intent.PriceBookDefinition{
		Revision:      "catalog@metric_version_prices",
		EffectiveFrom: effectiveFrom,
		Currency:      "USD",
		Prices:        prices,
	})
}
