package cycle

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// ErrInactiveModelPrice is returned by MetricPriceMicros when a usage row's
// (metric, model) has a per-model price ROW that has been retired (active =
// false). It is deliberately NOT pgx.ErrNoRows: a missing row legitimately falls
// back to the catalog price, but a retired model price must fail the cycle loud
// rather than silently bill at the cheaper catalog (Haiku-floor) fallback — that
// would under-bill a deliberately-retired model and defeat the rollup's loud
// revenue-leak guard. The Service maps this to a loud Internal error.
var ErrInactiveModelPrice = errors.New("per-model price is retired (active=false)")

// Store is the persistence interface the rollup + settlement Service depends
// on. Narrow on purpose — every method maps to a specific rollup step — so
// tests satisfy it with a small in-memory fake (see service_test.go).
type Store interface {
	// OpenPeriodForAccount upserts the billing_periods row keyed
	// (account_id, period_start) and returns its id. Idempotent: a re-run for
	// the same window returns the existing row's id rather than duplicating it.
	// period_end is the anchored-period window end (the next card-binding-day
	// boundary — ADR 0005), supplied by the caller.
	OpenPeriodForAccount(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, error)

	// RawAggregates aggregates the account's usage_events in [periodStart,
	// periodEnd) per (app, module, metric) by kind: count/sum → SUM(value),
	// peak → MAX(value), time_weighted → ∫ v dt (step-function integral). The
	// billable_quantity is returned as the exact NUMERIC string so the priced
	// row re-encodes it without a float round-trip.
	RawAggregates(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]RawAggregate, error)

	// MetricPriceMicros returns the per-unit customer price snapshotted onto the
	// aggregate. When model != "" it resolves the AUTHORITATIVE per-(metric,
	// model) price from metric_model_prices (migration 018) first, falling back
	// to the (module, metric) catalog row when no per-model price exists; model
	// == "" resolves the catalog row directly. priced=false (NULL/absent price)
	// → the metric is metered-but-unpriced and prices to 0. A per-model row that
	// EXISTS but is RETIRED (active=false) returns ErrInactiveModelPrice instead
	// of silently falling back to the cheaper catalog floor — the Service fails
	// the cycle loud rather than under-bill a deliberately-retired model.
	MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model string) (micros int64, priced bool, err error)

	// UpsertUsageAggregate writes one snapshotted billable record idempotently
	// on (period_id, app_id, module_id, metric). A re-run upserts the identical
	// row.
	UpsertUsageAggregate(ctx context.Context, periodID, accountID uuid.UUID, agg MetricAggregate) error

	// ModuleIncome returns Σ charged_micros per module across the period's
	// usage_aggregates — the settlement income input, keyed by module.
	ModuleIncome(ctx context.Context, periodID uuid.UUID) ([]ModuleIncome, error)

	// ModuleVisibility returns a module's developer margin-share class. found=
	// false → no visibility row; the caller defaults to private (30% take) so
	// the platform never under-collects on a lagging publish (design §7-B).
	ModuleVisibility(ctx context.Context, moduleID uuid.UUID) (Visibility, bool, error)

	// UpsertDeveloperSettlement writes one accrued settlement ledger row
	// idempotently on (period_id, module_id). developer_id is NULL (no
	// module→developer sync yet); status defaults 'accrued'.
	UpsertDeveloperSettlement(ctx context.Context, periodID, accountID uuid.UUID, s ModuleSettlement) error

	// InsertBillingRun is the charge idempotency gate: one run row per
	// (account, period window). It inserts a 'pending' row, or on conflict
	// RECLAIMS the existing row when it is non-terminal (a 'pending' run that
	// died mid-flight, 'skipped_no_pm', or 'failed'). shouldCharge=true (with the
	// run id) means this attempt must proceed to charge — the reclaimed row keeps
	// its id so the deterministic Stripe Idempotency-Keys stay stable across
	// attempts. shouldCharge=false means the window already has an 'invoiced'
	// (terminal-success) run and the cycle must NOT re-charge.
	InsertBillingRun(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (runID uuid.UUID, shouldCharge bool, err error)

	// AccountsWithUsageEvents returns the accounts with raw usage_events in the
	// window [periodStart, periodEnd) — the rollup-phase work list for
	// cmd/billing-cycle (phase 1: roll each up into usage_aggregates before the
	// charge phase reads them).
	AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// PeriodChargedTotal returns Σ usage_aggregates.charged_micros for the
	// account's period window — the arrears input before allowance-netting.
	PeriodChargedTotal(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (int64, error)

	// HasUsableDefaultPM is the no-PM charge gate: true iff the account has an
	// active, not-expired payment method. Mirrors the billing hot-path gate.
	HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error)

	// AccountStripeCustomer returns the account's Stripe Customer id (empty when
	// none exists yet). The charge never auto-creates a Customer — an empty id
	// at the charge leg is an anomaly the caller surfaces.
	AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error)

	// AccountCollection loads the account's risk-graded collection state (PR #9):
	// the usage_billing_mode, credit_limit, optional spend_ceiling, and the
	// account's created_at (so the risk-judge derives tenure WITHOUT a
	// cross-schema read into ms_account).
	AccountCollection(ctx context.Context, accountID uuid.UUID) (AccountCollection, error)

	// UpdateAccountCollection persists the mode transition only — it carries the
	// existing credit_limit / spend_ceiling through unchanged. The trust-ramp
	// RECOMPUTE of credit_limit (collection.TrustRampedCreditLimit) is a deferred
	// follow-up (it must run on a tenure/history-driven schedule, not the charge
	// path), so this write never grows the limit. ErrAccountNotFound when the row
	// is gone.
	UpdateAccountCollection(ctx context.Context, accountID uuid.UUID, c AccountCollection) error

	// TightenAndMarkRun ATOMICALLY persists a risk-judge mode tighten AND marks
	// the billing run skipped in ONE transaction. The two writes must not be
	// split: a crash between them would leave the account tightened but the run
	// row 'pending', so the next cycle reclaims the pending row and writes a
	// SECOND skip row for the same period — a phantom duplicate in the audit
	// trail. Wrapping both in a transaction makes the tighten+mark all-or-nothing.
	// ErrAccountNotFound when the account row is gone (the tx rolls back, the run
	// stays 'pending', and the cycle re-attempts).
	TightenAndMarkRun(ctx context.Context, accountID uuid.UUID, c AccountCollection, runID uuid.UUID, status BillingRunStatus) error

	// HasUnpaidInvoice is the delinquency signal (mirrors billing.Ensure's #7
	// derivation): true when the account has an open/uncollectible invoice in the
	// mirror. The risk-judge tightens toward prepaid on it.
	HasUnpaidInvoice(ctx context.Context, accountID uuid.UUID) (bool, error)

	// UpsertInvoice mirrors a created Stripe invoice into ms_billing.invoices,
	// idempotent on stripe_invoice_id (a deterministic Idempotency-Key re-run
	// returns the same invoice → the same mirror row).
	UpsertInvoice(ctx context.Context, inv InvoiceMirror) error

	// MarkBillingRun sets a run's terminal status, the Stripe invoice id
	// (empty → NULL), and the charged total in whole cents.
	MarkBillingRun(ctx context.Context, runID uuid.UUID, status BillingRunStatus, stripeInvoiceID string, totalCents int64) error

	// AccountsWithUnbilledUsage returns the accounts that have usage_aggregates
	// in a closed period window [periodStart, periodEnd) with no billing_run yet
	// — the work list for cmd/billing-cycle.
	AccountsWithUnbilledUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// ActivatedAccounts returns every account that has bound a card (a non-NULL
	// activated_at anchor, migration 025) with its anchor instant. Under
	// anchoring each account closes on its OWN card-binding day, so cmd/billing-
	// cycle iterates these and derives a per-account just-closed window rather
	// than sharing one batch window. Un-activated accounts (no card) are omitted.
	ActivatedAccounts(ctx context.Context) ([]AccountAnchor, error)

	// LatestClosedPeriodEnd returns the newest billing_periods.period_end for an
	// account and whether one exists — the cutover STRADDLE-CLAMP input. found=
	// false (no period yet) means no clamp is needed. Read-only.
	LatestClosedPeriodEnd(ctx context.Context, accountID uuid.UUID) (end time.Time, found bool, err error)

	// EnsureAccountForUser resolves the user's billing account, creating the
	// row if none exists yet — the SAME per-user-advisory-lock get-or-create
	// billing.Ensure uses (the one established account-creation path; no
	// Stripe Customer is created here). RegisterApp needs it because the apps
	// mirror row carries a NOT NULL account FK and app creation must never be
	// blocked on the user having visited billing first (D1c: the platform
	// fires RegisterApp fire-and-forget).
	EnsureAccountForUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, error)

	// AccountActivation returns the account's activated_at anchor (migration
	// 025) and whether it is set. activated=false → the account never bound a
	// card and is NEVER charged (D1d — the same posture as the spine's
	// unactivated skip); RegisterApp then records the mirror row without a
	// proration invoice.
	AccountActivation(ctx context.Context, accountID uuid.UUID) (activatedAt time.Time, activated bool, err error)

	// InsertAppMirror registers a ms_billing.apps roster row idempotently
	// (ON CONFLICT (app_id) DO NOTHING — a retry never rewrites the original
	// created_at / module_count, which anchor the proration).
	InsertAppMirror(ctx context.Context, appID, accountID uuid.UUID, moduleCount int, createdAt time.Time) error

	// AppMirror reads one roster row (deleted rows included — the caller owns
	// deletion semantics). found=false → the app was never registered.
	AppMirror(ctx context.Context, appID uuid.UUID) (AppMirror, bool, error)

	// SetAppProrationInvoice arms the ONE-SHOT creation-proration guard: it
	// records the Stripe invoice id, first-charge-wins (UPDATE … WHERE
	// proration_invoice_id IS NULL). An already-armed guard is NOT an error —
	// the write is a no-op and the original invoice id survives.
	SetAppProrationInvoice(ctx context.Context, appID uuid.UUID, stripeInvoiceID string) error

	// SetAppModuleCount snapshots a new installed-module count. A deleted
	// app's count is frozen (the UPDATE's WHERE deleted_at IS NULL no-ops —
	// D1e: no future base, so no tier to move).
	SetAppModuleCount(ctx context.Context, appID uuid.UUID, moduleCount int) error

	// MarkAppDeleted soft-deletes the roster row out of future advance base
	// fees. Idempotent — the first deletion instant is kept.
	MarkAppDeleted(ctx context.Context, appID uuid.UUID) error

	// LiveAppsCreatedBefore returns every LIVE (deleted_at IS NULL) app on the
	// account created STRICTLY BEFORE createdBefore, with its module_count —
	// the boundary charge's advance-base input. createdBefore is the NEW
	// period's start (the closed window's period_end): an app created inside
	// the new period is EXCLUDED because RegisterApp's creation-proration leg
	// already owns that period's base (full or prorated) — it joins the
	// advance leg at the NEXT boundary. Empty for a pre-backfill account →
	// advance base 0 (pre-027 behavior).
	LiveAppsCreatedBefore(ctx context.Context, accountID uuid.UUID, createdBefore time.Time) ([]AppModuleCount, error)

	// UpsertProrationBaseSnapshot persists the creation-proration leg's
	// per-app-period base snapshot (migration 028, source='proration'), keyed
	// (app_id, period_start). Idempotent — a retry overwrites with identical
	// values — and on a key collision with an 'advance' row the proration row
	// WINS (the more specific charge for a creation period).
	UpsertProrationBaseSnapshot(ctx context.Context, snap AppBaseSnapshot) error

	// InsertAdvanceBaseSnapshot persists the boundary advance leg's
	// per-app-period base snapshot (migration 028, source='advance') with
	// ON CONFLICT (app_id, period_start) DO NOTHING — an existing row (a
	// proration snapshot, or a prior reclaimed attempt's own row) wins, so a
	// re-run never rewrites what was already recorded as billed.
	InsertAdvanceBaseSnapshot(ctx context.Context, snap AppBaseSnapshot) error

	// --- account-wide POOLED module overage (migration 030) -----------------

	// PooledModuleCount returns the account-wide pooled installed-module count:
	// SUM(module_count) over the account's LIVE apps. The overage timer recompute
	// and the mid-period grace sweep tier on it (overage = $3 × max(0, this −
	// IncludedModules)).
	PooledModuleCount(ctx context.Context, accountID uuid.UUID) (int, error)

	// StartAccountOverage stamps the account's grace-timer anchor (overage_since)
	// the FIRST time its pool crosses IncludedModules — WHERE overage_since IS
	// NULL, so it is first-crossing-wins/idempotent (a later recompute that finds
	// it already armed is a no-op).
	StartAccountOverage(ctx context.Context, accountID uuid.UUID, since time.Time) error

	// ClearAccountOverage disarms the grace timer (overage_since → NULL) when the
	// pool drops back to ≤ IncludedModules — WHERE overage_since IS NOT NULL, so
	// it is idempotent. No refund (D1e): clearing only stops FUTURE accrual.
	ClearAccountOverage(ctx context.Context, accountID uuid.UUID) error

	// AccountsInOverageGrace returns every account whose grace timer has EXPIRED
	// as of cutoff (overage_since <= cutoff) and that is chargeable (activated) —
	// the mid-period grace sweep's work list, with each account's overage_since
	// (grace anchor) and activated_at (period anchor).
	AccountsInOverageGrace(ctx context.Context, cutoff time.Time) ([]OverageGraceCandidate, error)

	// AccountOverageSnapshot reads the frozen pooled overage a charge leg billed
	// for ONE (account, period) — the double-charge guard both the grace sweep
	// and the boundary consult (found=true → this period's pooled overage was
	// already billed, skip it). found=false → never charged.
	AccountOverageSnapshot(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (snap AccountOverageSnapshot, found bool, err error)

	// InsertAccountOverageSnapshot freezes what a charge leg billed the account
	// for one period's pooled overage (migration 030) with ON CONFLICT
	// (account_id, period_start) DO NOTHING — an existing row (a prior grace
	// charge, or a reclaimed boundary attempt's own row) wins, so a re-run never
	// rewrites what was already recorded as billed.
	InsertAccountOverageSnapshot(ctx context.Context, snap AccountOverageSnapshot) error
}

// OverageGraceCandidate is one account the mid-period grace sweep evaluates: its
// id plus the two anchors the sweep needs — OverageSince (the grace timer's
// start; grace ends at OverageSince + the grace window) and ActivatedAt (the
// billing-period anchor, ADR 0005, used to resolve the current window).
type OverageGraceCandidate struct {
	ID           uuid.UUID
	OverageSince time.Time
	ActivatedAt  time.Time
}

// AccountOverageSnapshot is the in-memory form of a
// ms_billing.account_overage_snapshots row (migration 030): what one charge leg
// billed one account for one period's POOLED module overage. PeriodStart is the
// display + double-charge lookup key; ChargedMicros is the exact overage the
// invoice billed (prorated for a 'grace' row, full for an 'advance' row);
// OverCount is the pooled over-count it tiered on; Source is 'grace' or
// 'advance'; InvoiceItemID is the Stripe item id (empty for a 0-cent charge).
type AccountOverageSnapshot struct {
	AccountID     uuid.UUID
	PeriodStart   time.Time
	PeriodEnd     time.Time
	OverCount     int
	ChargedMicros int64
	Source        string
	InvoiceItemID string
}

// AppModuleCount pairs one live roster app with its module_count snapshot —
// one advance-base input row. The boundary leg needs the app id (not just the
// count) to write the per-app-period base snapshot it bills (migration 028).
type AppModuleCount struct {
	AppID       uuid.UUID
	ModuleCount int
}

// AppBaseSnapshot is the in-memory form of a ms_billing.app_base_snapshots
// row (migration 028): what one charge leg actually billed one app for one
// period. PeriodStart/PeriodEnd are the FULL anchored window — period_start
// is the display lookup key — and for a proration snapshot BaseMicros is the
// PRORATED partial-window amount actually invoiced. GetAppBill prefers these
// rows over the live-count math so a later SyncAppModules can never drift the
// displayed base away from what was invoiced.
type AppBaseSnapshot struct {
	AppID       uuid.UUID
	PeriodStart time.Time
	PeriodEnd   time.Time
	ModuleCount int
	BaseMicros  int64
}

// AppMirror is the in-memory form of a ms_billing.apps roster row (migration
// 027). ProrationInvoiceID is "" while the one-shot creation-proration guard
// is unarmed; DeletedAt is meaningful only when Deleted is true.
type AppMirror struct {
	AppID              uuid.UUID
	AccountID          uuid.UUID
	ModuleCount        int
	CreatedAt          time.Time
	ProrationInvoiceID string
	Deleted            bool
	DeletedAt          time.Time
}

// AccountAnchor pairs an account with its billing-period anchor instant (the
// first-card-bind time, migration 025). cmd/billing-cycle derives the anchor
// DAY-OF-MONTH from ActivatedAt (billingperiod.AnchorDay) and closes that
// account's just-ended anchored period.
type AccountAnchor struct {
	ID          uuid.UUID
	ActivatedAt time.Time
}

// ErrAccountNotFound is returned by UpdateAccountCollection when no accounts row
// matches the id (the UPDATE affected zero rows).
var ErrAccountNotFound = errors.New("billing account not found")

// AccountCollection is the in-memory form of the risk-graded collection columns
// on ms_billing.accounts (PR #9). Money is integer micros. SpendCeilingMicros is
// only meaningful when HasSpendCeiling is true (the column is NULL = no ceiling).
// CreatedAt feeds the risk-judge's tenure derivation without a cross-schema read.
type AccountCollection struct {
	Mode               BillingMode
	CreditLimitMicros  int64
	HasSpendCeiling    bool
	SpendCeilingMicros int64
	CreatedAt          time.Time
}

// BillingMode mirrors ms_billing.usage_billing_mode (and collection.Mode)
// one-for-one. Kept as a cycle-package type so the charge spine doesn't import
// the db enum directly; the store maps it to/from db.MsBillingUsageBillingMode.
type BillingMode string

const (
	// BillingModeArrears: off-session arrears charging permitted (gated).
	BillingModeArrears BillingMode = "arrears"
	// BillingModePrepaid: off-session arrears charging NOT permitted (skip +
	// retain; prepaid wallet deferred).
	BillingModePrepaid BillingMode = "prepaid"
)

// InvoiceMirror is the in-memory form of a ms_billing.invoices row the charge
// spine writes after creating a Stripe invoice. Amounts are whole cents (Stripe
// minor units).
type InvoiceMirror struct {
	AccountID       uuid.UUID
	StripeInvoiceID string
	Status          string
	AmountDueCents  int64
	AmountPaidCents int64
	Currency        string
	PeriodStart     time.Time
	PeriodEnd       time.Time
}

// RawAggregate is one per-kind aggregated row from the rollup SELECTs, before
// pricing. BillableQuantity is the exact NUMERIC string (count/sum SUM, peak
// MAX, time_weighted integral). Model is the AI pricing dimension the rollup
// groups by (migration 018): empty for non-AI metrics (the rollup's
// COALESCE(model, ”)), a roster model id for infra.ai.* events. It selects the
// price source in MetricPriceMicros (per-model vs catalog). ModuleVersion is
// the version-attribution dimension the rollup ALSO groups by (migration
// 023): empty for a version-less event, the emitting module's version
// otherwise. It never affects pricing — it is carried straight through onto
// the aggregate for reporting only.
type RawAggregate struct {
	AppID            uuid.UUID
	ModuleID         uuid.UUID
	Metric           string
	Kind             Kind
	Model            string
	ModuleVersion    string
	BillableQuantity string
}

// ModuleIncome pairs a module with its period income (Σ charged_micros).
type ModuleIncome struct {
	ModuleID     uuid.UUID
	IncomeMicros int64
}

// NewStore returns a Store backed by the given pgxpool.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{pool: pool, q: db.New(pool)}
}

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func (s *pgxStore) OpenPeriodForAccount(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, error) {
	row, err := s.q.OpenPeriodForAccount(ctx, db.OpenPeriodForAccountParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(row.ID)
}

// RawAggregates issues the three per-kind rollup SELECTs sequentially, NOT in a
// single snapshot transaction. This is safe because rollup is single-writer per
// account per period: PR #6's billing_runs UNIQUE(account, period) + the batch
// cycle job guarantee no concurrent rollup races a usage_events INSERT between
// the first and third query. Re-evaluate if rollup ever becomes concurrent.
func (s *pgxStore) RawAggregates(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]RawAggregate, error) {
	acct := pgtype.UUID{Bytes: accountID, Valid: true}

	sumRows, err := s.q.RollupSumKinds(ctx, db.RollupSumKindsParams{
		AccountID: acct, RecordedAt: periodStart, RecordedAt_2: periodEnd,
	})
	if err != nil {
		return nil, err
	}
	peakRows, err := s.q.RollupPeakKind(ctx, db.RollupPeakKindParams{
		AccountID: acct, RecordedAt: periodStart, RecordedAt_2: periodEnd,
	})
	if err != nil {
		return nil, err
	}
	twRows, err := s.q.RollupTimeWeightedKind(ctx, db.RollupTimeWeightedKindParams{
		AccountID: acct, RecordedAt: periodStart, Column3: periodEnd,
	})
	if err != nil {
		return nil, err
	}

	out := make([]RawAggregate, 0, len(sumRows)+len(peakRows)+len(twRows))
	appendRow := func(appID, moduleID, metric string, kind db.MsBillingMetricKind, model, moduleVersion string, qty pgtype.Numeric) error {
		app, err := uuid.Parse(appID)
		if err != nil {
			return err
		}
		mod, err := uuid.Parse(moduleID)
		if err != nil {
			return err
		}
		out = append(out, RawAggregate{
			AppID:            app,
			ModuleID:         mod,
			Metric:           metric,
			Kind:             Kind(kind),
			Model:            model,         // "" for non-AI rows (COALESCE(model, ''))
			ModuleVersion:    moduleVersion, // "" for version-less rows (COALESCE(module_version, ''))
			BillableQuantity: numericString(qty),
		})
		return nil
	}
	for _, r := range sumRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range peakRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range twRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *pgxStore) MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model string) (int64, bool, error) {
	// PER-MODEL FIRST: an event that carries a model (the infra.ai.* family,
	// migration 018) is priced from the AUTHORITATIVE (metric, model) side-table.
	// A MISSING row (pgx.ErrNoRows) is NOT unpriced — it falls through to the
	// catalog row below (the sentinel metric_definitions fallback), so a model
	// with no per-model price still bills at the metric's fallback rate rather
	// than zero-charging. A row that EXISTS but is RETIRED (active = false) is a
	// different case: it must NOT silently fall back to the cheaper catalog floor
	// (that would under-bill a deliberately-retired model), so it returns
	// ErrInactiveModelPrice and the Service fails the cycle loud.
	if model != "" {
		row, err := s.q.LookupModelPrice(ctx, db.LookupModelPriceParams{
			Metric: metric,
			Model:  model,
		})
		if err == nil {
			if !row.Active {
				return 0, false, fmt.Errorf("%w: metric=%s model=%s", ErrInactiveModelPrice, metric, model)
			}
			return row.UnitPriceMicros, true, nil // NOT NULL column → a row means priced
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, err
		}
		// pgx.ErrNoRows → no per-model price row at all; fall back to the catalog.
	}

	price, err := s.q.LookupMetricPrice(ctx, db.LookupMetricPriceParams{
		ModuleID: moduleID.String(),
		Metric:   metric,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// No per-(module, metric) catalog row. For a RESERVED infra.* / platform.*
		// metric ATTRIBUTED to a real incurring module (module_id <> the sentinel),
		// fall back to the SENTINEL row that seeds every infra metric's COGS
		// (migrations 017/020) — the frozen-path half of decision 19's resolution
		// chain: (module, metric) → (SENTINEL, metric). Without this fallback an
		// attributed infra event with no per-module override row would resolve to 0
		// and trip the revenue-leak guard in service.go (which fails the cycle loud
		// for an unpriced reserved metric). A CUSTOM (non-reserved) metric keeps the
		// unpriced-→0 behavior (its absence is a legitimate metered-but-unpriced
		// case). The sentinel itself already looked up its own row above, so guard on
		// moduleID != the sentinel to avoid a redundant second lookup.
		if isReservedMetric(metric) && moduleID != usage.PlatformInfraModuleID() {
			price, err = s.q.LookupMetricPrice(ctx, db.LookupMetricPriceParams{
				ModuleID: usage.PlatformInfraModuleID().String(),
				Metric:   metric,
			})
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, false, nil // no seeded sentinel row → unpriced (guard fires loud)
			}
			if err != nil {
				return 0, false, err
			}
			if !price.Valid {
				return 0, false, nil // sentinel row metered-but-unpriced
			}
			return price.Int64, true, nil
		}
		// No catalog row at rollup time → treat as unpriced (0). An undeclared
		// metric never reaches usage_events (RecordUsage rejects it), so this
		// is a defensive guard, not a normal path.
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !price.Valid {
		return 0, false, nil // metered-but-unpriced
	}
	return price.Int64, true, nil
}

func (s *pgxStore) UpsertUsageAggregate(ctx context.Context, periodID, accountID uuid.UUID, agg MetricAggregate) error {
	qty, err := numericFromString(agg.BillableQuantity)
	if err != nil {
		return err
	}
	return s.q.UpsertUsageAggregate(ctx, db.UpsertUsageAggregateParams{
		PeriodID:          periodID.String(),
		AccountID:         accountID.String(),
		AppID:             agg.AppID.String(),
		ModuleID:          agg.ModuleID.String(),
		Metric:            agg.Metric,
		Model:             agg.Model,
		ModuleVersion:     agg.ModuleVersion,
		Kind:              db.MsBillingMetricKind(agg.Kind),
		BillableQuantity:  qty,
		UnitPriceMicros:   agg.UnitPriceMicros,
		CustomerMarkupNum: int32(agg.MarkupNum),
		CustomerMarkupDen: int32(agg.MarkupDen),
		RawCostMicros:     agg.RawCostMicros,
		ChargedMicros:     agg.ChargedMicros,
	})
}

func (s *pgxStore) ModuleIncome(ctx context.Context, periodID uuid.UUID) ([]ModuleIncome, error) {
	rows, err := s.q.ModuleIncomeForPeriod(ctx, periodID.String())
	if err != nil {
		return nil, err
	}
	out := make([]ModuleIncome, 0, len(rows))
	for _, r := range rows {
		mod, err := uuid.Parse(r.ModuleID)
		if err != nil {
			return nil, err
		}
		out = append(out, ModuleIncome{ModuleID: mod, IncomeMicros: r.IncomeMicros})
	}
	return out, nil
}

func (s *pgxStore) ModuleVisibility(ctx context.Context, moduleID uuid.UUID) (Visibility, bool, error) {
	vis, err := s.q.ModuleVisibility(ctx, moduleID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return Visibility(vis), true, nil
}

func (s *pgxStore) UpsertDeveloperSettlement(ctx context.Context, periodID, accountID uuid.UUID, set ModuleSettlement) error {
	return s.q.UpsertDeveloperSettlement(ctx, db.UpsertDeveloperSettlementParams{
		PeriodID:            periodID.String(),
		AccountID:           accountID.String(),
		ModuleID:            set.ModuleID.String(),
		DeveloperID:         pgtype.UUID{}, // NULL: no module→developer sync yet
		IncomeMicros:        set.IncomeMicros,
		InfraMicros:         set.InfraMicros,
		MarginShareClass:    db.MsBillingMarginShareClass(set.MarginShareClass),
		PlatformTakeMicros:  set.PlatformTakeMicros,
		DeveloperOwedMicros: set.DeveloperOwedMicros,
	})
}

func (s *pgxStore) InsertBillingRun(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, bool, error) {
	id, err := s.q.InsertBillingRun(ctx, db.InsertBillingRunParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// The DO UPDATE's WHERE excluded the row → the existing run is 'invoiced'
		// (terminal success). The window was already charged; do not re-charge.
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	runID, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, false, err
	}
	return runID, true, nil
}

func (s *pgxStore) PeriodChargedTotal(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (int64, error) {
	return s.q.PeriodChargedTotal(ctx, db.PeriodChargedTotalParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
}

func (s *pgxStore) HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.HasUsableDefaultPM(ctx, accountID.String())
}

func (s *pgxStore) AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error) {
	return s.q.AccountStripeCustomer(ctx, accountID.String())
}

func (s *pgxStore) AccountCollection(ctx context.Context, accountID uuid.UUID) (AccountCollection, error) {
	row, err := s.q.AccountCollectionFields(ctx, accountID.String())
	if err != nil {
		return AccountCollection{}, err
	}
	return AccountCollection{
		Mode:               BillingMode(row.UsageBillingMode),
		CreditLimitMicros:  row.CreditLimitMicros,
		HasSpendCeiling:    row.SpendCeilingMicros.Valid,
		SpendCeilingMicros: row.SpendCeilingMicros.Int64,
		CreatedAt:          row.CreatedAt,
	}, nil
}

func (s *pgxStore) UpdateAccountCollection(ctx context.Context, accountID uuid.UUID, c AccountCollection) error {
	rows, err := s.q.UpdateAccountCollection(ctx, db.UpdateAccountCollectionParams{
		ID:                 accountID.String(),
		UsageBillingMode:   db.MsBillingUsageBillingMode(c.Mode),
		CreditLimitMicros:  c.CreditLimitMicros,
		SpendCeilingMicros: pgtype.Int8{Int64: c.SpendCeilingMicros, Valid: c.HasSpendCeiling},
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrAccountNotFound
	}
	return nil
}

// TightenAndMarkRun runs UpdateAccountCollection + MarkBillingRun inside a single
// transaction so the mode tighten and the run-mark commit together or not at all
// — no crash window can leave the account tightened with the run row still
// 'pending' (which would re-fire the gate next cycle and write a duplicate skip
// row for the same period). The whole tx aborts if the account row is gone.
func (s *pgxStore) TightenAndMarkRun(ctx context.Context, accountID uuid.UUID, c AccountCollection, runID uuid.UUID, status BillingRunStatus) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	rows, err := qtx.UpdateAccountCollection(ctx, db.UpdateAccountCollectionParams{
		ID:                 accountID.String(),
		UsageBillingMode:   db.MsBillingUsageBillingMode(c.Mode),
		CreditLimitMicros:  c.CreditLimitMicros,
		SpendCeilingMicros: pgtype.Int8{Int64: c.SpendCeilingMicros, Valid: c.HasSpendCeiling},
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrAccountNotFound
	}

	total, err := centsNumeric(0) // a skip mark carries no charged total / invoice id
	if err != nil {
		return err
	}
	if err := qtx.MarkBillingRun(ctx, db.MarkBillingRunParams{
		ID:              runID.String(),
		Status:          string(status),
		StripeInvoiceID: pgtype.Text{}, // NULL: no Stripe invoice on a skip
		TotalAmount:     total,
	}); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *pgxStore) HasUnpaidInvoice(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.AccountHasUnpaidInvoice(ctx, accountID.String())
}

func (s *pgxStore) UpsertInvoice(ctx context.Context, inv InvoiceMirror) error {
	due, err := centsNumeric(inv.AmountDueCents)
	if err != nil {
		return err
	}
	paid, err := centsNumeric(inv.AmountPaidCents)
	if err != nil {
		return err
	}
	return s.q.UpsertInvoice(ctx, db.UpsertInvoiceParams{
		AccountID:       inv.AccountID.String(),
		StripeInvoiceID: inv.StripeInvoiceID,
		Status:          inv.Status,
		AmountDue:       due,
		AmountPaid:      paid,
		Currency:        inv.Currency,
		PeriodStart:     pgtype.Timestamptz{Time: inv.PeriodStart, Valid: !inv.PeriodStart.IsZero()},
		PeriodEnd:       pgtype.Timestamptz{Time: inv.PeriodEnd, Valid: !inv.PeriodEnd.IsZero()},
	})
}

func (s *pgxStore) MarkBillingRun(ctx context.Context, runID uuid.UUID, status BillingRunStatus, stripeInvoiceID string, totalCents int64) error {
	total, err := centsNumeric(totalCents)
	if err != nil {
		return err
	}
	return s.q.MarkBillingRun(ctx, db.MarkBillingRunParams{
		ID:              runID.String(),
		Status:          string(status),
		StripeInvoiceID: pgtype.Text{String: stripeInvoiceID, Valid: stripeInvoiceID != ""},
		TotalAmount:     total,
	})
}

func (s *pgxStore) AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AccountsWithUsageEvents(ctx, db.AccountsWithUsageEventsParams{
		RecordedAt:   periodStart,
		RecordedAt_2: periodEnd,
	})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) AccountsWithUnbilledUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AccountsWithUnbilledUsage(ctx, db.AccountsWithUnbilledUsageParams{
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) ActivatedAccounts(ctx context.Context) ([]AccountAnchor, error) {
	rows, err := s.q.ActivatedAccounts(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]AccountAnchor, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, err
		}
		// The query filters activated_at IS NOT NULL, so a non-Valid value here
		// would be a driver anomaly; skip it defensively rather than anchor on the
		// zero time (which would window January-1).
		if !r.ActivatedAt.Valid {
			continue
		}
		out = append(out, AccountAnchor{ID: id, ActivatedAt: r.ActivatedAt.Time})
	}
	return out, nil
}

func (s *pgxStore) LatestClosedPeriodEnd(ctx context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	end, err := s.q.LatestClosedPeriodEnd(ctx, accountID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return end, true, nil
}

func (s *pgxStore) EnsureAccountForUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		// billing.EnsureAccount and this get-or-create insert the SAME
		// accounts row, so both serialize on the single exported
		// (namespace, key) pair — the accounts table has no owner UNIQUE
		// constraint; the advisory lock IS the uniqueness guard.
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: billing.AdvisoryLockNamespaceBillingAccountUser,
			Column2: userID.String(),
		}); err != nil {
			return err
		}
		existing, err := qtx.SelectAccountByUser(ctx, pgtype.UUID{Bytes: userID, Valid: true})
		if err == nil {
			id = existing.ID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		inserted, err := qtx.InsertUserAccount(ctx, pgtype.UUID{Bytes: userID, Valid: true})
		if err != nil {
			return err
		}
		id = inserted.ID
		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.Parse(id)
}

func (s *pgxStore) AccountActivation(ctx context.Context, accountID uuid.UUID) (time.Time, bool, error) {
	at, err := s.q.AccountActivatedAt(ctx, accountID.String())
	if err != nil {
		return time.Time{}, false, err
	}
	if !at.Valid {
		return time.Time{}, false, nil // never bound a card → never charged (D1d)
	}
	return at.Time, true, nil
}

func (s *pgxStore) InsertAppMirror(ctx context.Context, appID, accountID uuid.UUID, moduleCount int, createdAt time.Time) error {
	// RowsAffected 0 = a retry hit ON CONFLICT DO NOTHING — success either way.
	_, err := s.q.InsertAppMirror(ctx, db.InsertAppMirrorParams{
		AppID:       appID.String(),
		AccountID:   accountID.String(),
		ModuleCount: int32(moduleCount), //nolint:gosec // RegisterApp validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		CreatedAt:   createdAt,
	})
	return err
}

func (s *pgxStore) AppMirror(ctx context.Context, appID uuid.UUID) (AppMirror, bool, error) {
	row, err := s.q.SelectAppMirror(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return AppMirror{}, false, nil
	}
	if err != nil {
		return AppMirror{}, false, err
	}
	app, err := uuid.Parse(row.AppID)
	if err != nil {
		return AppMirror{}, false, err
	}
	acct, err := uuid.Parse(row.AccountID)
	if err != nil {
		return AppMirror{}, false, err
	}
	return AppMirror{
		AppID:              app,
		AccountID:          acct,
		ModuleCount:        int(row.ModuleCount),
		CreatedAt:          row.CreatedAt,
		ProrationInvoiceID: row.ProrationInvoiceID.String, // "" when NULL (guard unarmed)
		Deleted:            row.DeletedAt.Valid,
		DeletedAt:          row.DeletedAt.Time,
	}, true, nil
}

func (s *pgxStore) SetAppProrationInvoice(ctx context.Context, appID uuid.UUID, stripeInvoiceID string) error {
	// 0 rows = the guard was already armed (first-charge-wins) — not an error:
	// the deterministic Stripe idem keys guarantee the concurrent charger
	// created the SAME invoice, so the surviving id is the right one.
	_, err := s.q.SetAppProrationInvoice(ctx, db.SetAppProrationInvoiceParams{
		AppID:              appID.String(),
		ProrationInvoiceID: pgtype.Text{String: stripeInvoiceID, Valid: true},
	})
	return err
}

func (s *pgxStore) SetAppModuleCount(ctx context.Context, appID uuid.UUID, moduleCount int) error {
	// 0 rows = the app is deleted (count frozen, D1e); existence was already
	// checked by the service via AppMirror, so this is a legitimate no-op.
	_, err := s.q.SetAppModuleCount(ctx, db.SetAppModuleCountParams{
		AppID:       appID.String(),
		ModuleCount: int32(moduleCount), //nolint:gosec // SyncAppModules validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
	})
	return err
}

func (s *pgxStore) MarkAppDeleted(ctx context.Context, appID uuid.UUID) error {
	// 0 rows = already deleted — idempotent, the first deletion instant stays.
	_, err := s.q.MarkAppDeleted(ctx, appID.String())
	return err
}

func (s *pgxStore) LiveAppsCreatedBefore(ctx context.Context, accountID uuid.UUID, createdBefore time.Time) ([]AppModuleCount, error) {
	rows, err := s.q.LiveAppModuleCountsCreatedBefore(ctx, db.LiveAppModuleCountsCreatedBeforeParams{
		AccountID: accountID.String(),
		CreatedAt: createdBefore,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AppModuleCount, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.AppID)
		if err != nil {
			return nil, err
		}
		out = append(out, AppModuleCount{AppID: id, ModuleCount: int(r.ModuleCount)})
	}
	return out, nil
}

func (s *pgxStore) UpsertProrationBaseSnapshot(ctx context.Context, snap AppBaseSnapshot) error {
	return s.q.UpsertProrationBaseSnapshot(ctx, db.UpsertProrationBaseSnapshotParams{
		AppID:       snap.AppID.String(),
		PeriodStart: snap.PeriodStart,
		PeriodEnd:   snap.PeriodEnd,
		ModuleCount: int32(snap.ModuleCount), //nolint:gosec // count comes from the apps row, whose writers (RegisterApp/SyncAppModules) validate 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		BaseMicros:  snap.BaseMicros,
	})
}

func (s *pgxStore) InsertAdvanceBaseSnapshot(ctx context.Context, snap AppBaseSnapshot) error {
	// 0 rows = ON CONFLICT DO NOTHING kept an existing row (a proration
	// snapshot, or a prior reclaimed attempt's write) — success either way.
	_, err := s.q.InsertAdvanceBaseSnapshot(ctx, db.InsertAdvanceBaseSnapshotParams{
		AppID:       snap.AppID.String(),
		PeriodStart: snap.PeriodStart,
		PeriodEnd:   snap.PeriodEnd,
		ModuleCount: int32(snap.ModuleCount), //nolint:gosec // count comes from the apps row, whose writers (RegisterApp/SyncAppModules) validate 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		BaseMicros:  snap.BaseMicros,
	})
	return err
}

// --- account-wide POOLED module overage (migration 030) --------------------

func (s *pgxStore) PooledModuleCount(ctx context.Context, accountID uuid.UUID) (int, error) {
	sum, err := s.q.SumLiveModuleCount(ctx, accountID.String())
	if err != nil {
		return 0, err
	}
	return int(sum), nil
}

func (s *pgxStore) StartAccountOverage(ctx context.Context, accountID uuid.UUID, since time.Time) error {
	// 0 rows = already armed (first-crossing-wins, WHERE overage_since IS NULL) —
	// a no-op, not an error: the original anchor survives.
	_, err := s.q.StartAccountOverage(ctx, db.StartAccountOverageParams{
		ID:           accountID.String(),
		OverageSince: pgtype.Timestamptz{Time: since, Valid: true},
	})
	return err
}

func (s *pgxStore) ClearAccountOverage(ctx context.Context, accountID uuid.UUID) error {
	// 0 rows = already clear (WHERE overage_since IS NOT NULL) — idempotent no-op.
	_, err := s.q.ClearAccountOverage(ctx, accountID.String())
	return err
}

func (s *pgxStore) AccountsInOverageGrace(ctx context.Context, cutoff time.Time) ([]OverageGraceCandidate, error) {
	rows, err := s.q.AccountsInOverageGrace(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
	if err != nil {
		return nil, err
	}
	out := make([]OverageGraceCandidate, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, err
		}
		// The query filters both columns NOT NULL, so a non-Valid value here is a
		// driver anomaly; skip it defensively rather than anchor on the zero time.
		if !r.OverageSince.Valid || !r.ActivatedAt.Valid {
			continue
		}
		out = append(out, OverageGraceCandidate{
			ID:           id,
			OverageSince: r.OverageSince.Time,
			ActivatedAt:  r.ActivatedAt.Time,
		})
	}
	return out, nil
}

func (s *pgxStore) AccountOverageSnapshot(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (AccountOverageSnapshot, bool, error) {
	row, err := s.q.SelectAccountOverageSnapshot(ctx, db.SelectAccountOverageSnapshotParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return AccountOverageSnapshot{}, false, nil
	}
	if err != nil {
		return AccountOverageSnapshot{}, false, err
	}
	return AccountOverageSnapshot{
		AccountID:     accountID,
		PeriodStart:   periodStart,
		OverCount:     int(row.OverCount),
		ChargedMicros: row.ChargedMicros,
		Source:        row.Source,
	}, true, nil
}

func (s *pgxStore) InsertAccountOverageSnapshot(ctx context.Context, snap AccountOverageSnapshot) error {
	// 0 rows = ON CONFLICT DO NOTHING kept an existing row (a prior grace charge,
	// or a reclaimed boundary attempt's write) — success either way.
	_, err := s.q.InsertAccountOverageSnapshot(ctx, db.InsertAccountOverageSnapshotParams{
		AccountID:     snap.AccountID.String(),
		PeriodStart:   snap.PeriodStart,
		PeriodEnd:     snap.PeriodEnd,
		OverCount:     int32(snap.OverCount), //nolint:gosec // over_count = pooled sum − IncludedModules; the pool is Σ validated module_counts (each ≤ maxModuleCount), far below int32 max
		ChargedMicros: snap.ChargedMicros,
		Source:        snap.Source,
		InvoiceItemID: pgtype.Text{String: snap.InvoiceItemID, Valid: snap.InvoiceItemID != ""},
	})
	return err
}

// parseUUIDs parses a slice of UUID-as-string account ids (the form the sqlc
// NOT NULL uuid → string override yields) into uuid.UUID.
func parseUUIDs(ids []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, 0, len(ids))
	for _, s := range ids {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// centsNumeric encodes a whole-cent int64 as the pgtype.Numeric the invoices /
// billing_runs NUMERIC money columns expect. Cents are whole integers, so the
// numeric is exact (no scale).
func centsNumeric(cents int64) (pgtype.Numeric, error) {
	return numericFromString(strconv.FormatInt(cents, 10))
}
