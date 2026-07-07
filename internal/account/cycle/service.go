package cycle

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// Service implements the period rollup + developer settlement + the Stripe
// charge cycle + the app base-fee surface (RegisterApp / SyncAppModules). It
// composes a Store and (for the charge legs) a Stripe Client. nowFn is
// injectable for deterministic tests (RegisterApp windows "now" into the
// account's current anchored period).
type Service struct {
	store  Store
	stripe billingstripe.Client
	nowFn  func() time.Time
	// bill is the ONE audited account-bill pricing spine (usage.Service),
	// injected via WithAccountBill. ListSponsoredOrgs reuses it to price each
	// sponsored org's current-window total instead of growing a second rollup.
	// nil until wired — only ListSponsoredOrgs depends on it (the charge/rollup
	// legs never touch it), so rollup-only wiring leaves it nil.
	bill AccountBillReader
}

// AccountBillReader is the account-bill read ListSponsoredOrgs reuses to price
// a sponsored org's current-window total — satisfied by *usage.Service. Narrow
// on purpose: only GetAccountBill's TotalMicros feeds the sponsored roster.
type AccountBillReader interface {
	GetAccountBill(ctx context.Context, req usage.GetAccountBillRequest) (*usage.GetAccountBillResponse, error)
}

// NewService wires a Service. store is required; passing nil panics at the
// first call site. stripe is required only for the charge legs (RunBillingCycle
// and RegisterApp's creation-proration invoice); RollupPeriod / SettleDevelopers
// never touch Stripe, so a nil stripe is tolerated for rollup-only wiring (it
// panics only if a charge leg is then called). Mirrors the billing.Service
// constructor pattern.
func NewService(store Store, stripe billingstripe.Client) *Service {
	return &Service{store: store, stripe: stripe, nowFn: time.Now}
}

// periodClosedByActivation is the D1d no-retroactive-catch-up decision shared
// by the creation-proration leg and the module-overage leg: given the anchored
// period CONTAINING anchorInstant (an app's created_at, or a module timer's
// installed_at) resolved against the account's activation anchor, report
// whether the account only activated AT OR AFTER that period's end — i.e. was
// never chargeable for the period's ENTIRE duration, so charging now (however
// late the sweep runs) would be retroactive catch-up. Compared against
// activatedAt, NOT "now": ordinary sweep/grace delay pushing a charge a few
// days past periodEnd for an already-activated account is expected, not
// retroactive, and must still charge normally — only the caller decides what
// to persist when closed=true (each leg's own permanent-skip marker).
func periodClosedByActivation(anchorInstant, activatedAt time.Time) (periodStart, periodEnd time.Time, closed bool) {
	anchorDay := billingperiod.AnchorDay(activatedAt)
	periodStart, periodEnd = billingperiod.AnchoredPeriodWindow(anchorInstant.UTC(), anchorDay)
	return periodStart, periodEnd, !activatedAt.Before(periodEnd)
}

// offSessionChargePermitted is the collection-mode gate shared by the
// creation-proration and per-module-overage legs (review 2026-07-06, H10): an
// account in PREPAID mode is never auto-charged off-session — by ANY leg, not
// only the boundary spine (which gates itself inside RunBillingCycle). The
// skip must be TRANSIENT (retried, nothing resolved/armed): the webhook-driven
// relax can flip the account back to arrears, at which point the deferred
// charges fire through their unchanged deterministic idem keys.
func (s *Service) offSessionChargePermitted(ctx context.Context, accountID uuid.UUID) (bool, error) {
	acct, err := s.store.AccountCollection(ctx, accountID)
	if err != nil {
		return false, billing.Internal("account collection lookup failed", err)
	}
	return acct.Mode != BillingModePrepaid, nil
}

// resolveChargeableCustomer is the PM-gate + Stripe-customer resolution shared
// by all three charge legs (RunBillingCycle, ChargeCreationProration,
// ChargeModuleOverage): no usable default PM -> ok=false (the caller's own
// skip status, transient/retried); a usable PM implies a Stripe Customer (a
// card can't attach without one) -> an empty custID here is an anomaly,
// surfaced as an error rather than silently auto-creating a Customer.
//
// The FUNDING HOP (org-billing D1): an org account whose designation names a
// sponsor gates on — and charges — the SPONSOR's default PM + Stripe
// customer; every other account funds itself. Resolved here, at charge time,
// so a designation switch re-routes only future charges; everything else
// (run rows, invoice mirror, ms_charge_ref) stays keyed to accountID, and a
// sponsor revoke degrades to the ordinary transient no_pm skip.
func (s *Service) resolveChargeableCustomer(ctx context.Context, accountID uuid.UUID) (custID string, ok bool, err error) {
	fundingID, err := s.store.ChargeFundingAccount(ctx, accountID)
	if err != nil {
		return "", false, billing.Internal("funding account lookup failed", err)
	}
	hasPM, err := s.store.HasUsableDefaultPM(ctx, fundingID)
	if err != nil {
		return "", false, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		return "", false, nil
	}
	custID, err = s.store.AccountStripeCustomer(ctx, fundingID)
	if err != nil {
		return "", false, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		return "", false, billing.Internal("account has a usable PM but no Stripe customer id", nil)
	}
	return custID, true, nil
}

// recoveryCustomer resolves the Stripe customer a CRASHED charge attempt's
// invoice is searched under (FindInvoiceByRef) — through the SAME funding hop
// the fresh-charge path applied when it created the invoice, so recovery and
// charge always look at the same customer. For a sponsor-funded org account
// the attribution account usually has NO Stripe customer at all; searching it
// would turn every recovery into a loud error, or a false "nothing" that
// re-charges fresh. Deliberately NOT PM-gated, matching each recovery leg's
// reconcile-before-gates posture.
//
// Known narrow gap: a funding SWITCH between the crashed attempt and this
// recovery resolves the NEW instrument, so the old instrument's invoice is
// invisible here — the deterministic idempotency keys (~24h) then return the
// ORIGINAL invoice on the re-charge attempt, the same backstop the Search-lag
// note in RunBillingCycle already relies on. The recorded-instrument
// hardening (stamping the funding account onto the attempt markers) travels
// with the org-billing W2 transfer wave.
func (s *Service) recoveryCustomer(ctx context.Context, accountID uuid.UUID) (string, error) {
	fundingID, err := s.store.ChargeFundingAccount(ctx, accountID)
	if err != nil {
		return "", err
	}
	return s.store.AccountStripeCustomer(ctx, fundingID)
}

// WithNow overrides the Service clock — deterministic-test hook only (mirrors
// the usage.Service nowFn seam). Returns the Service for chaining.
func (s *Service) WithNow(now func() time.Time) *Service {
	s.nowFn = now
	return s
}

// WithAccountBill injects the account-bill pricing spine ListSponsoredOrgs
// reuses to compute each sponsored org's current-window total (the *usage.Service
// wired in main). Optional dependency — mirrors WithBudgetEvaluator; only
// ListSponsoredOrgs needs it. Returns the Service for chaining.
func (s *Service) WithAccountBill(bill AccountBillReader) *Service {
	s.bill = bill
	return s
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
		// Resolve the per-unit price by (module, metric, model): an infra.ai.*
		// event carries a model → priced PER MODEL from metric_model_prices, with
		// the catalog row as fallback; every other metric carries model="" →
		// resolved straight from the catalog (migration 018).
		priceMicros, priced, err := s.store.MetricPriceMicros(ctx, raw.ModuleID, raw.Metric, raw.Model)
		if err != nil {
			// A RETIRED per-model price (active=false) must fail the cycle loud,
			// not silently fall back to the cheaper catalog floor and under-bill
			// the retired model. Surface it as a distinct, ops-actionable error.
			if errors.Is(err, ErrInactiveModelPrice) {
				return nil, billing.Internal("infra metric "+raw.Metric+" has a RETIRED per-model price for model "+raw.Model+" (active=false) — re-activate or re-price it in metric_model_prices; refusing to silently bill the catalog fallback", err)
			}
			return nil, billing.Internal("metric price lookup failed", err)
		}

		// Pricing plane: a reserved infra.* / platform.* metric is a
		// platform-billed line marked up 12/10; every other (custom) metric
		// carries the identity 10/10 (no markup — the platform's third-party
		// cut is the dev-side margin-share at settlement).
		num, den := customMarkupNum, customMarkupDen
		if isReservedMetric(raw.Metric) {
			num, den = infraMarkupNum, infraMarkupDen
			// An infra event in the rollup MUST resolve a seeded cost (migration
			// 017's sentinel metric_definitions rows). A custom metric with no
			// catalog price legitimately prices to 0 (metered-but-unpriced); a
			// RESERVED metric with no price means migration 017 is missing or was
			// rolled back without re-seeding — the platform incurred the cloud cost
			// but would bill 0 (a silent revenue leak that still marks the cycle
			// invoiced). Fail loud instead so ops catches it, never zero-charge it.
			if !priced {
				return nil, billing.Internal("infra metric "+raw.Metric+" has no seeded price — migration 017 or 018 missing or rolled back", nil)
			}
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
			Model:            raw.Model,
			ModuleVersion:    raw.ModuleVersion,
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
		// Guard the cross-metric sum: each charged is non-negative + int64, so a
		// wrap shows as the sum going DOWN. Per-metric overflow is already caught
		// in chargedMicros; this catches the (impossible-at-real-scale) total.
		if summary.TotalChargedMicros+charged < summary.TotalChargedMicros {
			return nil, billing.Internal("period charged total overflows int64 micros", nil)
		}
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
