package usage

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// Store is the persistence interface Service depends on. Narrow on
// purpose — every method maps to a specific RPC need — so tests satisfy
// it with a small in-memory fake (see service_test.go).
type Store interface {
	// LookupMetricDefinition returns the authoritative kind + per-unit
	// customer price for a (module, metric), and whether the metric is
	// active. found=false when no catalog row exists; the service REJECTS
	// an undeclared metric (declaration-first — design §1).
	LookupMetricDefinition(ctx context.Context, moduleID uuid.UUID, metric string) (def MetricDefinition, found bool, err error)

	// UpsertMetricDefinitions syncs a module's full set of metric
	// declarations from the manifest into the catalog (keyed
	// module_id+metric) in a SINGLE transaction — all-or-nothing. A failure
	// on any row rolls back every upsert in the batch, so the catalog is
	// never left partially synced (a partial catalog would accept some
	// declared metrics at ingest and reject others until the next sync —
	// design §1 declaration-first correctness). Idempotent; a re-sync
	// updates kind/unit/price/active in place.
	UpsertMetricDefinitions(ctx context.Context, defs []MetricDeclaration) error

	// UpsertInfraPriceOverrides writes a module's per-metric price OVERRIDES
	// for the reserved platform-infra metrics (decision 19 §4.3), keyed
	// (module_id, metric) with the REAL module_id. Each row is PRICE-ONLY:
	// kind + unit are copied from the SENTINEL base catalog row (the seeded
	// platform-infra row) in one INSERT ... SELECT, so the caller never
	// supplies them. All-or-nothing (one transaction); idempotent (a re-sync
	// updates unit_price_micros in place). A metric with no sentinel catalog
	// row (a seed drift — the service already gated it as registered) makes
	// the INSERT ... SELECT affect 0 rows, which the method surfaces as an
	// error rather than silently writing nothing.
	UpsertInfraPriceOverrides(ctx context.Context, moduleID uuid.UUID, overrides []InfraPriceOverride) error

	// InsertUsageEvent writes one raw metered fact, idempotent on
	// event_id. recorded=false means ON CONFLICT(event_id) DO NOTHING
	// deduped an at-least-once retry — NOT an error. accountID is the
	// zero UUID for a lazy (account-less) event, stored as NULL.
	InsertUsageEvent(ctx context.Context, ev UsageEvent) (recorded bool, err error)

	// AccountByOwner resolves the billing account for an owner principal
	// (user or org), or (Nil, false) when none exists yet. Read-only;
	// missing-account is a normal lazy-state outcome, not an error.
	AccountByOwner(ctx context.Context, owner Owner) (uuid.UUID, bool, error)

	// AccountAnchorDay returns the account's billing-period anchor day (1..31):
	// the day-of-month it bound its first credit card (activated_at, migration
	// 025), derived in UTC. An account with no activation yet (NULL activated_at,
	// or a missing row) falls back to billingperiod.DefaultAnchorDay (1 = the UTC
	// calendar month, the pre-025 window). Read once per RPC so each read windows
	// the account's OWN anchored period rather than the calendar month.
	AccountAnchorDay(ctx context.Context, accountID uuid.UUID) (int, error)

	// CurrentPeriodUsage sums raw usage_events for the account in
	// [periodStart, periodEnd), joined to metric_definitions, projecting
	// raw cost per metric = quantity × unit_price. For custom metrics this
	// declared price IS the customer charge (no blanket markup).
	CurrentPeriodUsage(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]MetricUsageRaw, error)

	// UpsertModuleVisibility records a module's published/private
	// visibility (developer margin-share dimension; NEVER a customer
	// markup). Idempotent on module_id.
	UpsertModuleVisibility(ctx context.Context, moduleID uuid.UUID, vis Visibility) error

	// UsageHistory reads the IMMUTABLE billable record (usage_aggregates,
	// joined to its billing_periods row) for every closed period whose
	// period_start falls in [windowStart, windowEnd) — the multi-month
	// trend-chart read GetUsageHistory serves. Rows are grouped at (period,
	// metric, kind), summed across every model / module_version split, and
	// returned ordered oldest-to-newest by period then metric. A period with
	// no usage_aggregates rows yet (not rolled up, or truly zero usage)
	// simply contributes no rows.
	UsageHistory(ctx context.Context, accountID uuid.UUID, windowStart, windowEnd time.Time) ([]PeriodMetricUsageRaw, error)

	// VersionBreakdown sums usage_aggregates grouped by module_version for
	// ONE period — the period whose billing_periods row is keyed
	// (accountID, periodStart). moduleID == uuid.Nil means "every one of the
	// owner's modules"; a non-zero moduleID narrows to that module's
	// versions only. A period not yet rolled up returns no rows (not an
	// error).
	VersionBreakdown(ctx context.Context, accountID uuid.UUID, periodStart time.Time, moduleID uuid.UUID) ([]VersionUsageRaw, error)

	// AppUsage is the APP-OWNER's bill for ONE app in the current period:
	// one row per (module_id, metric, model, module_version) with
	// billable_quantity + unit_price_micros + charged_micros. accountID gates
	// the payer; appID filters to the one app. It reads the IMMUTABLE billable
	// record (usage_aggregates, joined to its billing_periods row by
	// periodStart) once this app+period is rolled up, else estimates LIVE from
	// raw usage_events in [periodStart, periodEnd) — the same rolled-up-else-live
	// fast path CurrentPeriodUsageSummary uses, extended to the app scope and the
	// model / module_version dimensions. There is NO customer markup by module
	// visibility: charged is the declared unit_price × quantity.
	AppUsage(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppMetricUsageRaw, error)

	// AppBill reads EVERY usage line for the full-structure app-owner bill for
	// ONE (account, app, period) — the read behind GetAppBill. It is AppUsage
	// widened to the WHOLE pricing plane: it returns BOTH the customer module-usage
	// lines (custom metrics, priced at the declared unit_price with NO markup) AND
	// the platform-infra lines (reserved infra.* / platform.* metrics, priced at
	// the 1.2× infra markup — applied inline on the live branch, frozen on the
	// rolled branch). The service partitions the rows by name (isReservedMetric):
	// non-reserved → 模組使用量 module usage, reserved → 基礎設施 infrastructure
	// total. Rolled-up-else-live and uninstall-safe exactly like AppUsage (reads
	// only the usage_aggregates / usage_events ledgers, NEVER an install table).
	AppBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppMetricUsageRaw, error)

	// AppInfraBill returns the per-metric 基礎設施 (infrastructure) RESIDUAL breakdown
	// for the app-owner bill — one AppInfraUsage per ACTIVE declared infra metric (the
	// platform-infra sentinel catalog rows), including declared-but-unused ones at
	// qty 0 · $0, because the read is CATALOG-anchored (FROM metric_definitions
	// LEFT JOIN the rolled-else-live infra usage). Since decision 19 the usage side is
	// the SENTINEL-attributed residual only (module_id = the platform-infra sentinel);
	// infra attributed to a real module is read by AppModuleInfraBill. ChargedMicros
	// already carries the ×1.2 infra markup applied ONCE in SQL; UnitPriceMicros is the
	// raw catalog COGS. accountID may be uuid.Nil for a lazy (account-less) app — every
	// declared metric then resolves to $0 rather than dropping out. Ledger-only /
	// uninstall-safe exactly like AppBill.
	AppInfraBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppInfraUsage, error)

	// AppModuleInfraBill returns the per-MODULE 基礎設施 breakdown (decision 19): reserved
	// infra.* / platform.* usage ATTRIBUTED to a real incurring module (module_id <> the
	// platform-infra sentinel), one AppModuleInfraUsage per (module_id, module_version,
	// metric). It is USAGE-anchored (only modules that actually incurred infra appear —
	// unlike the catalog-anchored residual above) and carries the DUAL price: the SENTINEL
	// default plus the per-module override (nil when the module has no override row).
	// ChargedMicros carries the ×1.2 infra markup applied ONCE in SQL. Rolled-up-else-live
	// / uninstall-safe exactly like AppInfraBill. Together with AppInfraBill it partitions
	// the reserved namespace with no overlap (residual = sentinel, module = non-sentinel).
	AppModuleInfraBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppModuleInfraUsage, error)

	// ListBillingPeriods returns an account's real billing_periods rows
	// newest-first (the closed periods behind the web 週期 selector).
	// currentMonthStart is the current anchored-period start
	// (billingperiod.AnchoredPeriodWindow(now, anchorDay).start); a row whose
	// period_start equals it is flagged IsCurrent. The service prepends a synthetic
	// current live period when none is flagged (the in-progress period has no
	// billing_periods row yet).
	ListBillingPeriods(ctx context.Context, accountID uuid.UUID, currentMonthStart time.Time) ([]BillingPeriodRaw, error)

	// BillingPeriodWindow resolves ONE billing_periods row's [start, end) window
	// by (accountID, periodID). found=false (pgx.ErrNoRows) → the service returns
	// NOT_FOUND; the account gate prevents cross-account period enumeration.
	BillingPeriodWindow(ctx context.Context, accountID, periodID uuid.UUID) (start, end time.Time, found bool, err error)

	// ListInvoices reads ONE keyset page of the account's mirrored Stripe
	// invoices (ms_billing.invoices) ordered (created_at, id) DESC, EXCLUDING
	// status='draft' rows (a draft was never billed to the customer and can
	// still mutate Stripe-side — the SQL owns that filter so the LIMIT counts
	// only renderable rows). cursor==nil starts at the newest invoice; a
	// non-nil cursor resumes strictly AFTER that position in DESC order.
	// limit passes through verbatim — the service clamps the page size and
	// asks for page+1 rows to detect a further page. Money comes back already
	// converted mirror NUMERIC whole cents → int64 micros (×10_000) so micros
	// stay the only money unit above the store.
	ListInvoices(ctx context.Context, accountID uuid.UUID, limit int32, cursor *InvoiceCursor) ([]InvoiceMirrorRaw, error)
	// AppMirror reads the app's ms_billing.apps roster row (migration 027) —
	// the authoritative base-fee inputs GetAppBill displays: the synced
	// installed-module count (overage tier), the platform creation instant
	// (creation-period proration), and the soft-delete state (a pre-period
	// deletion zeroes the base, D1e). found=false → the app is not mirrored
	// yet (pre-backfill) and the caller keeps the pre-027 fallback math.
	// Deleted rows ARE returned (found=true) — deletion semantics are the
	// caller's, not the read's.
	AppMirror(ctx context.Context, appID uuid.UUID) (AppMirrorInfo, bool, error)

	// AppBaseSnapshot reads the frozen per-app-period base charge written by
	// the charge legs (ms_billing.app_base_snapshots, migration 028) for the
	// period starting EXACTLY at periodStart — the AUTHORITATIVE display value
	// for a charged period's 基本費用: what the invoice actually billed,
	// immune to later SyncAppModules count drift. found=false → the period was
	// never base-charged (pre-feature history, an unactivated account, or an
	// in-progress creation period not yet prorated) and the caller falls back
	// to the live-count DISPLAY ESTIMATE.
	AppBaseSnapshot(ctx context.Context, appID uuid.UUID, periodStart time.Time) (AppBaseSnapshotInfo, bool, error)

	// AppIDsWithUsage enumerates the DISTINCT app_ids with ANY usage for the
	// account in [periodStart, periodEnd) — the usage half of GetAccountBill's
	// app roster. It is the enumeration projection of the same rolled-up-else-
	// live gate AppBill reads through (an app appears iff it has FROZEN
	// usage_aggregates rows for the period OR live usage_events in the window
	// — the per-app gate then picks which ledger bills it), ledger-only /
	// uninstall-safe like every bill read. Deduped; order is the store's scan
	// order (the service re-sorts after merging the mirror half).
	AppIDsWithUsage(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// MirroredAppIDs enumerates the account's ms_billing.apps roster rows
	// (migration 027) whose [created_at, deleted_at) interval overlaps
	// [periodStart, periodEnd) — the mirror half of GetAccountBill's app
	// roster, so a just-created zero-usage app still shows its (prorated) base
	// and an app deleted DURING the period keeps its spent base (D1e); an app
	// deleted BEFORE the period opened is excluded (base 0, no new usage —
	// residual ledger rows still enumerate through AppIDsWithUsage).
	MirroredAppIDs(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// PooledModuleCount returns the account-wide POOLED installed-module count:
	// SUM(module_count) over the account's LIVE apps (migration 032). It is the
	// live input to GetAccountBill's account-wide overage ESTIMATE when the
	// current period has no frozen account_overage_snapshots row yet.
	PooledModuleCount(ctx context.Context, accountID uuid.UUID) (int, error)

	// AccountOverageSnapshot reads the frozen pooled overage a charge leg billed
	// for ONE (account, period) — the AUTHORITATIVE display value for
	// GetAccountBill's account-wide overage line (what the grace sweep or the
	// boundary actually invoiced). found=false → the period's pooled overage was
	// never charged (pre-032 history, an account under the pool, or an
	// in-progress period no leg has billed yet) and the caller falls back to the
	// live pooled ESTIMATE.
	AccountOverageSnapshot(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (chargedMicros int64, found bool, err error)
}

// AppBaseSnapshotInfo is the display-read projection of a
// ms_billing.app_base_snapshots row (migration 028): the base amount
// (integer micros) actually invoiced for the app-period. The row's
// module_count and source ('proration' vs 'advance') matter only to the
// write-side ON CONFLICT precedence — the display never branches on them,
// so they are deliberately not projected.
type AppBaseSnapshotInfo struct {
	BaseMicros int64
}

// AppMirrorInfo is the display-read projection of a ms_billing.apps roster row
// (migration 027). DeletedAt is meaningful only when Deleted is true.
type AppMirrorInfo struct {
	ModuleCount int
	CreatedAt   time.Time
	Deleted     bool
	DeletedAt   time.Time
}

// MetricDefinition is the catalog projection the ingest path resolves
// the declared kind + customer price from at record time. UnitPriceMicros
// is the per-unit customer price; Priced is false when the metric is
// metered-but-unpriced (catalog price is NULL).
type MetricDefinition struct {
	Kind            Kind
	Unit            string
	UnitPriceMicros int64
	Priced          bool
	Active          bool
}

// MetricDeclaration is one metric synced from the module manifest into the
// catalog via UpsertMetricDefinition. UnitPriceMicros is the developer's
// declared per-unit customer price; Priced=false declares a
// metered-but-unpriced metric (stored as a NULL price).
type MetricDeclaration struct {
	ModuleID        uuid.UUID
	Metric          string
	Kind            Kind
	Unit            string
	UnitPriceMicros int64
	Priced          bool
	Active          bool
}

// Owner is a polymorphic owner principal. Exactly one of UserID / OrgID
// is set; both Nil means a lazy (account-less) event.
type Owner struct {
	UserID uuid.UUID
	OrgID  uuid.UUID
}

// IsZero reports the lazy / no-owner case (neither principal set).
func (o Owner) IsZero() bool { return o.UserID == uuid.Nil && o.OrgID == uuid.Nil }

// UsageEvent is the raw fact handed to InsertUsageEvent. AccountID is Nil
// for the lazy case (persisted as a NULL account_id). Model is the optional AI
// pricing dimension (migration 018): empty for every non-AI event, persisted as
// a NULL usage_events.model. ModuleVersion is the optional version-attribution
// dimension (migration 023, purely reporting — never priced): empty when no
// version is carried, persisted as a NULL usage_events.module_version.
type UsageEvent struct {
	EventID       string
	AccountID     uuid.UUID
	AppID         uuid.UUID
	ModuleID      uuid.UUID
	Metric        string
	Kind          Kind
	Value         float64
	RecordedAt    time.Time
	Model         string
	ModuleVersion string
}

// MetricUsageRaw is one grouped row from the live current-period query.
// RawCostMicros = quantity × unit_price, rounded to whole micro-dollars
// (round-half-up) at the store boundary. For custom metrics this IS the
// customer charge (no blanket markup applied by the service).
type MetricUsageRaw struct {
	// ModuleID is the module that emitted the metric (the query now groups by
	// it, widened from PR #3's (metric, kind) only — see CurrentPeriodUsage).
	ModuleID        uuid.UUID
	Metric          string
	Kind            Kind
	Quantity        float64
	UnitPriceMicros int64
	RawCostMicros   int64
	// Group is the §11 display-group taxonomy bucket from the catalog
	// (metric_definitions.display_group), COALESCE'd to 'other' in the query
	// for a missing/ungrouped row. Carried verbatim to MetricUsage.Group.
	Group string
	// Visibility is the module's published/private margin-share class
	// (module_visibility, migration 010), COALESCE'd to 'private' in the
	// query for a module with no visibility row yet (design §7-B default).
	Visibility Visibility
}

// PeriodMetricUsageRaw is one grouped row from the multi-month history query:
// a module-metric's rolled-up totals within ONE billing period, summed across
// every model / module_version split. Money fields are already whole int64
// micros (unlike MetricUsageRaw's live-estimate NUMERIC decode) because
// usage_aggregates snapshots money as BIGINT, so SUM() stays exact.
type PeriodMetricUsageRaw struct {
	PeriodStart time.Time
	PeriodEnd   time.Time
	// ModuleID is the module that emitted the metric — same per-module
	// granularity as MetricUsageRaw so history rows scope to one module.
	ModuleID        uuid.UUID
	Metric          string
	Kind            Kind
	Quantity        float64
	UnitPriceMicros int64
	RawCostMicros   int64
	ChargedMicros   int64
	Group           string
	// Visibility is the module's published/private margin-share class
	// (module_visibility, migration 010), COALESCE'd to 'private' in the
	// query for a module with no visibility row yet (design §7-B default).
	Visibility Visibility
}

// VersionUsageRaw is one grouped row from the version-breakdown query: a
// module_version's summed totals across every metric (and every model split)
// in the resolved period.
type VersionUsageRaw struct {
	ModuleVersion    string
	BillableQuantity float64
	RawCostMicros    int64
	ChargedMicros    int64
}

// AppMetricUsageRaw is one grouped row from the app-owner bill query
// (AppUsage): a single app's usage of one (module, metric, model,
// module_version) in the current period. BillableQuantity decodes the NUMERIC
// quantity (a display value); ChargedMicros is the customer charge decoded
// half-up through the shared micros rounding point (a no-op on the already-
// integer rolled-up branch, the single rounding point on the live SUM(value ×
// unit_price) branch). No markup: charged == declared unit_price × quantity.
type AppMetricUsageRaw struct {
	ModuleID uuid.UUID
	Metric   string
	Kind     Kind
	// Model is the AI pricing dimension (migration 018), '' for every non-AI
	// row. ModuleVersion is the version-attribution dimension (migration 023),
	// '' for a version-less row. Both are carried so the UI can split per-model
	// / per-version sub-lines.
	Model            string
	ModuleVersion    string
	BillableQuantity float64
	UnitPriceMicros  int64
	ChargedMicros    int64
}

// BillingPeriodRaw is one row of ListBillingPeriods: a real billing_periods
// window plus whether it is the current calendar month (IsCurrent). It is the
// per-period entry the web 週期 (period) selector renders.
type BillingPeriodRaw struct {
	ID          uuid.UUID
	PeriodStart time.Time
	PeriodEnd   time.Time
	IsCurrent   bool
}

// InvoiceCursor is a DECODED keyset position in the (created_at, id) DESC
// invoice ordering: the last row of the previous page. On the wire it travels
// only as the opaque base64 token ListInvoices mints and parses (see
// invoices.go) — the store sees the decoded tuple, never the token.
type InvoiceCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

// InvoiceMirrorRaw is one decoded ms_billing.invoices row (the Stripe invoice
// mirror, 011 + 026). Money is int64 micro-USD — the mirror's NUMERIC whole
// cents ×10_000, converted at the store boundary so micros stay the only
// money unit above it. Number / HostedInvoiceURL / InvoicePDF are "" until
// the finalization webhook enriched the row (historic pre-026 rows stay
// empty); PeriodStart / PeriodEnd are nil for a non-period (manual) invoice.
type InvoiceMirrorRaw struct {
	ID               uuid.UUID
	StripeInvoiceID  string
	Number           string
	Status           string
	AmountDueMicros  int64
	AmountPaidMicros int64
	Currency         string
	PeriodStart      *time.Time
	PeriodEnd        *time.Time
	CreatedAt        time.Time
	HostedInvoiceURL string
	InvoicePDF       string
	// IsLargeAutoCollect is the server-computed post-hoc disclosure flag
	// (migration 034): true when this invoice's off-session charge exceeded the
	// account's auto-collect threshold that applied when it fired. Read-through
	// from the mirror for the billing page's large-charge disclosure surface.
	IsLargeAutoCollect bool
}

// NewStore returns a Store backed by the given pgxpool. The pool is
// retained so the batch catalog sync can run inside a single transaction.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{pool: pool, q: db.New(pool)}
}

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func (s *pgxStore) LookupMetricDefinition(ctx context.Context, moduleID uuid.UUID, metric string) (MetricDefinition, bool, error) {
	row, err := s.q.LookupMetricDefinition(ctx, db.LookupMetricDefinitionParams{
		ModuleID: moduleID.String(),
		Metric:   metric,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return MetricDefinition{}, false, nil
	}
	if err != nil {
		return MetricDefinition{}, false, err
	}
	return MetricDefinition{
		Kind:            Kind(row.Kind),
		Unit:            row.Unit,
		UnitPriceMicros: row.UnitPriceMicros.Int64,
		Priced:          row.UnitPriceMicros.Valid,
		Active:          row.Active,
	}, true, nil
}

func (s *pgxStore) UpsertMetricDefinitions(ctx context.Context, defs []MetricDeclaration) error {
	if len(defs) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	// Rollback is a no-op after a successful Commit, so the deferred rollback
	// safely covers every early-return error path without double-handling.
	defer tx.Rollback(ctx)

	qtx := s.q.WithTx(tx)
	for _, def := range defs {
		if err := qtx.UpsertMetricDefinition(ctx, db.UpsertMetricDefinitionParams{
			ModuleID:        def.ModuleID.String(),
			Metric:          def.Metric,
			Kind:            db.MsBillingMetricKind(def.Kind),
			Unit:            def.Unit,
			UnitPriceMicros: nullablePriceMicros(def.UnitPriceMicros, def.Priced),
			Active:          def.Active,
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// UpsertInfraPriceOverrides upserts each price-only infra override in one
// transaction (all-or-nothing, mirroring UpsertMetricDefinitions). The
// generated UpsertInfraPriceOverride copies kind + unit from the sentinel
// catalog row via INSERT ... SELECT and returns the affected-row count:
// 0 means the sentinel row was absent (a seed drift the service could not
// catch — it validates against the in-Go platformInfraKind registry, not the
// DB), so surface it as an error instead of a silent no-op.
func (s *pgxStore) UpsertInfraPriceOverrides(ctx context.Context, moduleID uuid.UUID, overrides []InfraPriceOverride) error {
	if len(overrides) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	// Rollback is a no-op after a successful Commit, so the deferred rollback
	// safely covers every early-return error path without double-handling.
	defer tx.Rollback(ctx)

	qtx := s.q.WithTx(tx)
	for _, o := range overrides {
		affected, err := qtx.UpsertInfraPriceOverride(ctx, db.UpsertInfraPriceOverrideParams{
			ModuleID:        moduleID.String(),
			Metric:          o.Metric,
			UnitPriceMicros: o.UnitPriceMicros,
		})
		if err != nil {
			return err
		}
		if affected == 0 {
			return fmt.Errorf("no sentinel catalog row for infra metric %q (seed drift): cannot inherit kind/unit for the override", o.Metric)
		}
	}
	return tx.Commit(ctx)
}

func (s *pgxStore) InsertUsageEvent(ctx context.Context, ev UsageEvent) (bool, error) {
	value, err := numericFromFloat(ev.Value)
	if err != nil {
		return false, err
	}
	rows, err := s.q.InsertUsageEvent(ctx, db.InsertUsageEventParams{
		EventID:       ev.EventID,
		AccountID:     nullableAccountID(ev.AccountID),
		AppID:         ev.AppID.String(),
		ModuleID:      ev.ModuleID.String(),
		Metric:        ev.Metric,
		Kind:          db.MsBillingMetricKind(ev.Kind),
		Value:         value,
		RecordedAt:    ev.RecordedAt,
		Model:         nullableModel(ev.Model),
		ModuleVersion: nullableModuleVersion(ev.ModuleVersion),
	})
	if err != nil {
		return false, err
	}
	// :execrows returns 1 on a fresh insert, 0 when ON CONFLICT deduped.
	return rows > 0, nil
}

func (s *pgxStore) AccountByOwner(ctx context.Context, owner Owner) (uuid.UUID, bool, error) {
	// v1 ships the user-owned path (SelectAccountByUser). Org-owned
	// accounts (owner_org_id) land with the org billing milestone; until
	// then an org owner resolves to "no account yet" (lazy), which the
	// service handles gracefully (records the event NULL-account).
	if owner.OrgID != uuid.Nil {
		return uuid.Nil, false, nil
	}
	row, err := s.q.SelectAccountByUser(ctx, pgtype.UUID{Bytes: owner.UserID, Valid: true})
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	parsed, err := uuid.Parse(row.ID)
	if err != nil {
		return uuid.Nil, false, err
	}
	return parsed, true, nil
}

func (s *pgxStore) AppMirror(ctx context.Context, appID uuid.UUID) (AppMirrorInfo, bool, error) {
	row, err := s.q.SelectAppMirror(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return AppMirrorInfo{}, false, nil // not mirrored yet → pre-027 fallback
	}
	if err != nil {
		return AppMirrorInfo{}, false, err
	}
	return AppMirrorInfo{
		ModuleCount: int(row.ModuleCount),
		CreatedAt:   row.CreatedAt,
		Deleted:     row.DeletedAt.Valid,
		DeletedAt:   row.DeletedAt.Time,
	}, true, nil
}

func (s *pgxStore) AppBaseSnapshot(ctx context.Context, appID uuid.UUID, periodStart time.Time) (AppBaseSnapshotInfo, bool, error) {
	row, err := s.q.SelectAppBaseSnapshot(ctx, db.SelectAppBaseSnapshotParams{
		AppID:       appID.String(),
		PeriodStart: periodStart,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return AppBaseSnapshotInfo{}, false, nil // never base-charged → live estimate
	}
	if err != nil {
		return AppBaseSnapshotInfo{}, false, err
	}
	return AppBaseSnapshotInfo{BaseMicros: row.BaseMicros}, true, nil
}

// AppIDsWithUsage enumerates the account's app_ids with any usage in the
// window (frozen aggregates for the period ∪ live events in [start, end)) —
// see the Store interface doc. The generated query returns text app_ids
// (sqlc's NOT NULL uuid → string override); parse at this boundary like every
// other id decode.
func (s *pgxStore) AppIDsWithUsage(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AppIDsWithUsage(ctx, db.AppIDsWithUsageParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	return parseAppIDs(rows)
}

// MirroredAppIDs enumerates the account's mirror roster rows overlapping the
// window — see the Store interface doc.
func (s *pgxStore) MirroredAppIDs(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.MirroredAppIDsOverlappingWindow(ctx, db.MirroredAppIDsOverlappingWindowParams{
		AccountID:   accountID.String(),
		PeriodEnd:   periodEnd,
		PeriodStart: periodStart,
	})
	if err != nil {
		return nil, err
	}
	return parseAppIDs(rows)
}

// PooledModuleCount sums module_count over the account's live apps (migration
// 030) — the live input to GetAccountBill's account-wide overage estimate.
func (s *pgxStore) PooledModuleCount(ctx context.Context, accountID uuid.UUID) (int, error) {
	sum, err := s.q.SumLiveModuleCount(ctx, accountID.String())
	if err != nil {
		return 0, err
	}
	return int(sum), nil
}

// AccountOverageSnapshot reads the frozen pooled overage charged for one
// (account, period) — pgx.ErrNoRows → found=false (the service falls back to
// the live pooled estimate).
func (s *pgxStore) AccountOverageSnapshot(ctx context.Context, accountID uuid.UUID, periodStart time.Time) (int64, bool, error) {
	row, err := s.q.SelectAccountOverageSnapshot(ctx, db.SelectAccountOverageSnapshotParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil // never overage-charged → live estimate
	}
	if err != nil {
		return 0, false, err
	}
	return row.ChargedMicros, true, nil
}

// parseAppIDs decodes a generated query's text app_id column into uuid.UUIDs,
// shared by the two GetAccountBill enumeration reads.
func parseAppIDs(rows []string) ([]uuid.UUID, error) {
	out := make([]uuid.UUID, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r)
		if err != nil {
			return nil, fmt.Errorf("decode app_id %q: %w", r, err)
		}
		out = append(out, id)
	}
	return out, nil
}

// AccountAnchorDay reads the account's activated_at and derives its anchor day.
// A NULL anchor (never activated) or a missing row (defensive; the caller passes
// an already-resolved account id) falls back to the calendar-month default so the
// read never fails on an un-activated account.
func (s *pgxStore) AccountAnchorDay(ctx context.Context, accountID uuid.UUID) (int, error) {
	at, err := s.q.AccountActivatedAt(ctx, accountID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return billingperiod.DefaultAnchorDay, nil
	}
	if err != nil {
		return 0, err
	}
	if !at.Valid {
		return billingperiod.DefaultAnchorDay, nil
	}
	return billingperiod.AnchorDay(at.Time), nil
}

func (s *pgxStore) CurrentPeriodUsage(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]MetricUsageRaw, error) {
	rows, err := s.q.CurrentPeriodUsageSummary(ctx, db.CurrentPeriodUsageSummaryParams{
		AccountID:    pgtype.UUID{Bytes: accountID, Valid: true},
		RecordedAt:   periodStart,
		RecordedAt_2: periodEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]MetricUsageRaw, 0, len(rows))
	for _, r := range rows {
		qty, err := floatFromNumeric(r.TotalQuantity)
		if err != nil {
			return nil, fmt.Errorf("decode total_quantity for metric %q: %w", r.Metric, err)
		}
		rawCost, err := microsFromNumeric(r.RawCostMicros)
		if err != nil {
			return nil, fmt.Errorf("decode raw_cost_micros for metric %q: %w", r.Metric, err)
		}
		moduleID, err := uuid.Parse(r.ModuleID)
		if err != nil {
			return nil, fmt.Errorf("decode module_id for metric %q: %w", r.Metric, err)
		}
		out = append(out, MetricUsageRaw{
			ModuleID:        moduleID,
			Metric:          r.Metric,
			Kind:            Kind(r.Kind),
			Quantity:        qty,
			UnitPriceMicros: r.UnitPriceMicros,
			RawCostMicros:   rawCost,
			Group:           string(r.DisplayGroup),
			Visibility:      Visibility(r.Visibility),
		})
	}
	return out, nil
}

// UsageHistory reads the immutable billable record (usage_aggregates joined
// to billing_periods) for the trailing window — see the Store interface doc.
func (s *pgxStore) UsageHistory(ctx context.Context, accountID uuid.UUID, windowStart, windowEnd time.Time) ([]PeriodMetricUsageRaw, error) {
	rows, err := s.q.UsageHistoryForAccount(ctx, db.UsageHistoryForAccountParams{
		AccountID:     accountID.String(),
		PeriodStart:   windowStart,
		PeriodStart_2: windowEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]PeriodMetricUsageRaw, 0, len(rows))
	for _, r := range rows {
		qty, err := floatFromNumeric(r.TotalQuantity)
		if err != nil {
			return nil, fmt.Errorf("decode total_quantity for metric %q: %w", r.Metric, err)
		}
		moduleID, err := uuid.Parse(r.ModuleID)
		if err != nil {
			return nil, fmt.Errorf("decode module_id for metric %q: %w", r.Metric, err)
		}
		out = append(out, PeriodMetricUsageRaw{
			PeriodStart:     r.PeriodStart,
			PeriodEnd:       r.PeriodEnd,
			ModuleID:        moduleID,
			Metric:          r.Metric,
			Kind:            Kind(r.Kind),
			Quantity:        qty,
			UnitPriceMicros: r.UnitPriceMicros,
			RawCostMicros:   r.RawCostMicros,
			ChargedMicros:   r.ChargedMicros,
			Group:           string(r.DisplayGroup),
			Visibility:      Visibility(r.Visibility),
		})
	}
	return out, nil
}

// VersionBreakdown sums usage_aggregates grouped by module_version for one
// period — see the Store interface doc. moduleID == uuid.Nil disables the
// module filter (every one of the owner's modules is included).
func (s *pgxStore) VersionBreakdown(ctx context.Context, accountID uuid.UUID, periodStart time.Time, moduleID uuid.UUID) ([]VersionUsageRaw, error) {
	hasFilter := moduleID != uuid.Nil
	// $3::boolean short-circuits the OR before $4 is ever compared when
	// hasFilter is false, so moduleID (uuid.Nil in that case) is just an inert
	// well-formed placeholder satisfying the NOT NULL uuid column type.
	rows, err := s.q.VersionBreakdownForAccount(ctx, db.VersionBreakdownForAccountParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		Column3:     hasFilter,
		ModuleID:    moduleID.String(),
	})
	if err != nil {
		return nil, err
	}
	out := make([]VersionUsageRaw, 0, len(rows))
	for _, r := range rows {
		qty, err := floatFromNumeric(r.TotalQuantity)
		if err != nil {
			return nil, fmt.Errorf("decode total_quantity for module_version %q: %w", r.ModuleVersion, err)
		}
		out = append(out, VersionUsageRaw{
			ModuleVersion:    r.ModuleVersion,
			BillableQuantity: qty,
			RawCostMicros:    r.RawCostMicros,
			ChargedMicros:    r.ChargedMicros,
		})
	}
	return out, nil
}

// AppUsage reads the app-owner bill for ONE app in the current period —
// rolled-up-else-live per the AppUsageSummary query. accountID gates the payer,
// appID filters to the one app; periodStart doubles as the current period's
// billing_periods anchor (the rolled branch) and the live events' lower bound.
func (s *pgxStore) AppUsage(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppMetricUsageRaw, error) {
	rows, err := s.q.AppUsageSummary(ctx, db.AppUsageSummaryParams{
		AccountID:   accountID.String(),
		AppID:       appID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AppMetricUsageRaw, 0, len(rows))
	for _, r := range rows {
		line, err := appMetricUsageRaw(r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity, r.ChargedMicros, r.UnitPriceMicros)
		if err != nil {
			return nil, err
		}
		out = append(out, line)
	}
	return out, nil
}

// AppBill reads the full-structure app-owner bill lines for ONE app+period,
// rolled-up-else-live per the AppBillLines query — BOTH the module-usage lines
// and the reserved infra.* / platform.* lines (the latter carrying the 1.2× infra
// markup). The service (GetAppBill) partitions the returned rows by name.
func (s *pgxStore) AppBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppMetricUsageRaw, error) {
	rows, err := s.q.AppBillLines(ctx, db.AppBillLinesParams{
		AccountID:   accountID.String(),
		AppID:       appID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AppMetricUsageRaw, 0, len(rows))
	for _, r := range rows {
		line, err := appMetricUsageRaw(r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity, r.ChargedMicros, r.UnitPriceMicros)
		if err != nil {
			return nil, err
		}
		out = append(out, line)
	}
	return out, nil
}

// AppInfraBill reads the per-metric infrastructure breakdown for ONE app+period,
// catalog-anchored per the AppInfraBillLines query — one row per active declared
// infra metric (the platform-infra sentinel catalog), including declared-but-unused
// metrics at qty 0 · $0. ChargedMicros is decoded half-up through the shared micros
// decoder (the single per-metric rounding point on the live SUM(value × unit_price)
// × 12/10 branch; a no-op on the already-integer rolled branch). UnitPriceMicros is
// the raw catalog COGS (pre-markup) read straight from metric_definitions.
func (s *pgxStore) AppInfraBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppInfraUsage, error) {
	rows, err := s.q.AppInfraBillLines(ctx, db.AppInfraBillLinesParams{
		AccountID:   accountID.String(),
		AppID:       appID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AppInfraUsage, 0, len(rows))
	for _, r := range rows {
		qty, err := floatFromNumeric(r.BillableQuantity)
		if err != nil {
			return nil, fmt.Errorf("decode billable_quantity for infra metric %q: %w", r.Metric, err)
		}
		charged, err := microsFromNumeric(r.ChargedMicros)
		if err != nil {
			return nil, fmt.Errorf("decode charged_micros for infra metric %q: %w", r.Metric, err)
		}
		out = append(out, AppInfraUsage{
			Metric:           r.Metric,
			Kind:             Kind(r.Kind),
			Unit:             r.Unit,
			Group:            string(r.DisplayGroup),
			UnitPriceMicros:  r.UnitPriceMicros,
			BillableQuantity: qty,
			ChargedMicros:    charged,
		})
	}
	return out, nil
}

// AppModuleInfraBill reads the per-module infrastructure breakdown for ONE
// app+period, dual-priced per the AppModuleInfraBillLines query — one row per
// (module_id, module_version, metric) of reserved infra usage attributed to a real
// incurring module. module_unit_price_micros is decoded as a NULLABLE *int64: NULL
// (no per-module override row) leaves it nil so the wire carries "no override" (the
// UI's plain-vs-adjusted switch) rather than zero-filling to the default. charged_micros
// is decoded half-up through the shared micros decoder (the single per-line rounding
// point on the live SUM(value × price) × 12/10 branch; a no-op on the already-integer
// rolled branch); default_unit_price_micros is the raw SENTINEL COGS (pre-markup).
func (s *pgxStore) AppModuleInfraBill(ctx context.Context, accountID, appID uuid.UUID, periodStart, periodEnd time.Time) ([]AppModuleInfraUsage, error) {
	rows, err := s.q.AppModuleInfraBillLines(ctx, db.AppModuleInfraBillLinesParams{
		AccountID:   accountID.String(),
		AppID:       appID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return nil, err
	}
	out := make([]AppModuleInfraUsage, 0, len(rows))
	for _, r := range rows {
		moduleID, err := uuid.Parse(r.ModuleID)
		if err != nil {
			return nil, fmt.Errorf("decode module_id for infra metric %q: %w", r.Metric, err)
		}
		qty, err := floatFromNumeric(r.BillableQuantity)
		if err != nil {
			return nil, fmt.Errorf("decode billable_quantity for infra metric %q: %w", r.Metric, err)
		}
		charged, err := microsFromNumeric(r.ChargedMicros)
		if err != nil {
			return nil, fmt.Errorf("decode charged_micros for infra metric %q: %w", r.Metric, err)
		}
		// NULL module override → leave nil (plain mode on the wire). A non-NULL
		// value (incl. an explicit 0 = ms.Price(0) full absorb) points to a copy.
		var modulePrice *int64
		if r.ModuleUnitPriceMicros.Valid {
			mp := r.ModuleUnitPriceMicros.Int64
			modulePrice = &mp
		}
		out = append(out, AppModuleInfraUsage{
			ModuleID:               moduleID,
			ModuleVersion:          r.ModuleVersion,
			Metric:                 r.Metric,
			Label:                  r.Metric, // no friendly-label registry here; metric id is the label
			Kind:                   Kind(r.Kind),
			Unit:                   r.Unit,
			Group:                  string(r.DisplayGroup),
			BillableQuantity:       qty,
			DefaultUnitPriceMicros: r.DefaultUnitPriceMicros,
			ModuleUnitPriceMicros:  modulePrice,
			ChargedMicros:          charged,
		})
	}
	return out, nil
}

// ListBillingPeriods reads an account's real billing_periods rows newest-first,
// flagging the row whose period_start equals currentMonthStart as IsCurrent.
func (s *pgxStore) ListBillingPeriods(ctx context.Context, accountID uuid.UUID, currentMonthStart time.Time) ([]BillingPeriodRaw, error) {
	rows, err := s.q.ListBillingPeriods(ctx, db.ListBillingPeriodsParams{
		AccountID:         accountID.String(),
		CurrentMonthStart: currentMonthStart,
	})
	if err != nil {
		return nil, err
	}
	out := make([]BillingPeriodRaw, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, fmt.Errorf("decode billing period id %q: %w", r.ID, err)
		}
		out = append(out, BillingPeriodRaw{
			ID:          id,
			PeriodStart: r.PeriodStart,
			PeriodEnd:   r.PeriodEnd,
			IsCurrent:   r.IsCurrent,
		})
	}
	return out, nil
}

// BillingPeriodWindow resolves one billing_periods row's [start, end) window by
// (accountID, periodID). pgx.ErrNoRows → found=false (the service maps it to
// NOT_FOUND); the account gate keeps a caller to its own periods.
func (s *pgxStore) BillingPeriodWindow(ctx context.Context, accountID, periodID uuid.UUID) (time.Time, time.Time, bool, error) {
	row, err := s.q.BillingPeriodWindow(ctx, db.BillingPeriodWindowParams{
		PeriodID:  periodID.String(),
		AccountID: accountID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	return row.PeriodStart, row.PeriodEnd, true, nil
}

// ListInvoices reads one keyset page of the invoices mirror — see the Store
// interface doc. The cursor arrives pre-decoded; hasCursor=false makes the
// SQL gate short-circuit, so the zero cursor values are inert well-formed
// placeholders (the VersionBreakdown gate pattern). The mirror's money
// columns are NUMERIC whole cents (Stripe minor units, 011): each decodes
// through centsNumericToMicros so the ×10_000 cents→micros conversion happens
// exactly once, at this boundary.
func (s *pgxStore) ListInvoices(ctx context.Context, accountID uuid.UUID, limit int32, cursor *InvoiceCursor) ([]InvoiceMirrorRaw, error) {
	params := db.ListInvoicesForAccountParams{
		AccountID: accountID.String(),
		RowLimit:  limit,
		CursorID:  uuid.Nil.String(), // inert placeholder; gated off by HasCursor
	}
	if cursor != nil {
		params.HasCursor = true
		params.CursorCreatedAt = cursor.CreatedAt
		params.CursorID = cursor.ID.String()
	}
	rows, err := s.q.ListInvoicesForAccount(ctx, params)
	if err != nil {
		return nil, err
	}
	out := make([]InvoiceMirrorRaw, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, fmt.Errorf("decode id for invoice %q: %w", r.StripeInvoiceID, err)
		}
		due, err := centsNumericToMicros(r.AmountDue)
		if err != nil {
			return nil, fmt.Errorf("decode amount_due for invoice %q: %w", r.StripeInvoiceID, err)
		}
		paid, err := centsNumericToMicros(r.AmountPaid)
		if err != nil {
			return nil, fmt.Errorf("decode amount_paid for invoice %q: %w", r.StripeInvoiceID, err)
		}
		out = append(out, InvoiceMirrorRaw{
			ID:              id,
			StripeInvoiceID: r.StripeInvoiceID,
			// pgtype.Text zero-values String to "" when NULL, which is exactly
			// the "not enriched yet" contract InvoiceMirrorRaw documents.
			Number:             r.Number.String,
			Status:             r.Status,
			AmountDueMicros:    due,
			AmountPaidMicros:   paid,
			Currency:           r.Currency,
			PeriodStart:        timePtrFromTimestamptz(r.PeriodStart),
			PeriodEnd:          timePtrFromTimestamptz(r.PeriodEnd),
			CreatedAt:          r.CreatedAt,
			HostedInvoiceURL:   r.HostedInvoiceUrl.String,
			InvoicePDF:         r.InvoicePdf.String,
			IsLargeAutoCollect: r.IsLargeAutoCollect,
		})
	}
	return out, nil
}

// appMetricUsageRaw decodes one generated app-usage/app-bill row into the
// AppMetricUsageRaw the service consumes: the NUMERIC billable_quantity as a
// display float, charged_micros half-up through the shared micros decoder (a
// no-op on the already-integer rolled branch, the single rounding point on the
// live SUM(value × unit_price [× markup]) branch), and module_id parsed from its
// text form. Shared by AppUsage + AppBill (identical generated row shapes).
func appMetricUsageRaw(moduleID, metric string, kind db.MsBillingMetricKind, model, moduleVersion string, quantity, charged pgtype.Numeric, unitPriceMicros int64) (AppMetricUsageRaw, error) {
	qty, err := floatFromNumeric(quantity)
	if err != nil {
		return AppMetricUsageRaw{}, fmt.Errorf("decode billable_quantity for metric %q: %w", metric, err)
	}
	chargedMicros, err := microsFromNumeric(charged)
	if err != nil {
		return AppMetricUsageRaw{}, fmt.Errorf("decode charged_micros for metric %q: %w", metric, err)
	}
	mod, err := uuid.Parse(moduleID)
	if err != nil {
		return AppMetricUsageRaw{}, fmt.Errorf("decode module_id for metric %q: %w", metric, err)
	}
	return AppMetricUsageRaw{
		ModuleID:         mod,
		Metric:           metric,
		Kind:             Kind(kind),
		Model:            model,
		ModuleVersion:    moduleVersion,
		BillableQuantity: qty,
		UnitPriceMicros:  unitPriceMicros,
		ChargedMicros:    chargedMicros,
	}, nil
}

func (s *pgxStore) UpsertModuleVisibility(ctx context.Context, moduleID uuid.UUID, vis Visibility) error {
	return s.q.UpsertModuleVisibility(ctx, db.UpsertModuleVisibilityParams{
		ModuleID:   moduleID.String(),
		Visibility: db.MsBillingMarginShareClass(vis),
	})
}

// nullableAccountID maps a Nil account UUID to a SQL NULL (the lazy
// account case) and a real UUID to a valid pgtype.UUID.
func nullableAccountID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{} // Valid: false → NULL
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

// nullableModel maps the optional AI model dimension to the nullable TEXT
// usage_events.model column: an empty model (every non-AI event) → SQL NULL, a
// non-empty model → a valid pgtype.Text. Keeps NULL (not "") as the canonical
// "no model" so the rollup's COALESCE(model, ”) and the metric_model_prices
// lookup agree on the absent case.
func nullableModel(model string) pgtype.Text {
	if model == "" {
		return pgtype.Text{} // Valid: false → NULL
	}
	return pgtype.Text{String: model, Valid: true}
}

// nullableModuleVersion maps the optional version-attribution dimension to
// the nullable TEXT usage_events.module_version column: an empty version
// (every event that doesn't report one) → SQL NULL, matching nullableModel's
// contract for the analogous model column.
func nullableModuleVersion(version string) pgtype.Text {
	if version == "" {
		return pgtype.Text{} // Valid: false → NULL
	}
	return pgtype.Text{String: version, Valid: true}
}

// nullablePriceMicros maps a declared price to the nullable BIGINT the
// catalog column expects: priced=false (metered-but-unpriced) → SQL NULL.
func nullablePriceMicros(micros int64, priced bool) pgtype.Int8 {
	if !priced {
		return pgtype.Int8{} // Valid: false → NULL
	}
	return pgtype.Int8{Int64: micros, Valid: true}
}

// numericFromFloat builds the pgtype.Numeric the generated query expects
// for the NUMERIC value column. Formatting via strconv (not float-bits)
// keeps the decimal representation exact for the magnitudes meters carry.
func numericFromFloat(v float64) (pgtype.Numeric, error) {
	var n pgtype.Numeric
	if err := n.Scan(strconv.FormatFloat(v, 'f', -1, 64)); err != nil {
		return pgtype.Numeric{}, fmt.Errorf("encode numeric from %v: %w", v, err)
	}
	return n, nil
}

// floatFromNumeric reads a NUMERIC quantity into a float64 for the live
// estimate. Quantities are display values (not money) so float is
// acceptable here; money stays integer micros end-to-end.
func floatFromNumeric(n pgtype.Numeric) (float64, error) {
	if !n.Valid {
		return 0, nil
	}
	fv, err := n.Float64Value()
	if err != nil {
		return 0, err
	}
	return fv.Float64, nil
}

// timePtrFromTimestamptz maps a nullable timestamptz to *time.Time: NULL →
// nil (so the wire's omitempty drops the field), else a pointer to the value.
func timePtrFromTimestamptz(ts pgtype.Timestamptz) *time.Time {
	if !ts.Valid {
		return nil
	}
	t := ts.Time
	return &t
}

// centsNumericToMicros converts a mirror NUMERIC whole-cent amount (Stripe
// minor units — the invoices money columns, 011) to int64 micro-dollars:
// cents × 10_000. Implemented by shifting the NUMERIC's decimal exponent by
// +4 and reusing microsFromNumeric, so even a (theoretical) fractional-cent
// mirror value rounds half-up through the single shared money rounding point
// instead of a second ad-hoc conversion path.
func centsNumericToMicros(n pgtype.Numeric) (int64, error) {
	// n is a value copy; bumping Exp scales the copy only (inert when
	// !n.Valid, which microsFromNumeric already answers with 0), and
	// microsFromNumeric never mutates the shared *big.Int.
	n.Exp += 4
	return microsFromNumeric(n)
}

// MicrosFromNumeric is the exported entry to microsFromNumeric so sibling
// billing packages (the budget engine sums the SAME value × unit_price
// NUMERIC) decode money through this single rounding point rather than
// duplicating the big.Rat logic.
func MicrosFromNumeric(n pgtype.Numeric) (int64, error) { return microsFromNumeric(n) }

// microsFromNumeric converts a NUMERIC micro-dollar amount to int64,
// rounding half-up to the whole micro deterministically (matching the
// agent cents precedent — money never carries sub-micro fractions). The
// SQL SUM(value * unit_price) can be fractional when value is fractional;
// this is the single rounding point on the read path.
func microsFromNumeric(n pgtype.Numeric) (int64, error) {
	if !n.Valid {
		return 0, nil
	}
	// pgtype.Numeric is Int * 10^Exp. Reconstruct as a big.Rat for exact
	// rounding rather than going through float64 (which loses precision
	// for large micro totals).
	if n.NaN {
		return 0, errors.New("numeric is NaN")
	}
	if n.InfinityModifier != 0 {
		return 0, errors.New("numeric is infinite")
	}
	// A zero-value Numeric{Valid:true} carries a nil Int (big.Rat.SetInt(nil)
	// panics). The COALESCE in the query makes this unreachable from the DB
	// path today, but guard it so the function is safe to call in isolation.
	if n.Int == nil {
		return 0, nil
	}
	rat := new(big.Rat).SetInt(n.Int)
	if n.Exp >= 0 {
		mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil)
		rat.Mul(rat, new(big.Rat).SetInt(mul))
	} else {
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-n.Exp)), nil)
		rat.Quo(rat, new(big.Rat).SetInt(div))
	}
	return roundRatHalfUp(rat)
}

// roundRatHalfUp rounds a big.Rat to the nearest integer, halves up
// (toward +∞ on a .5 tie). Usage costs are non-negative, so half-up is
// the conventional "round .5 up" merchants expect.
//
// It returns an error rather than silently wrapping when the result does not
// fit in int64: big.Int.Int64() returns the low 64 bits on overflow, which
// would silently corrupt the live-summary money read. The live summary sums
// SUM(value × unit_price) as a NUMERIC; an infra.egress.bytes accumulation at
// planetary scale (or many high-priced events) can exceed int64 micros, so
// guard it identically to cycle.roundRatHalfUp.
func roundRatHalfUp(r *big.Rat) (int64, error) {
	// floor(r) then compare the remainder to 1/2.
	num := new(big.Int).Set(r.Num())
	den := r.Denom()
	q := new(big.Int)
	rem := new(big.Int)
	q.QuoRem(num, den, rem) // truncates toward zero
	// Normalize toward floor for negative values (defensive; costs ≥ 0).
	if rem.Sign() < 0 {
		q.Sub(q, big.NewInt(1))
		rem.Add(rem, den)
	}
	// remainder*2 >= denom → round up.
	if new(big.Int).Mul(rem, big.NewInt(2)).Cmp(den) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	// int64 holds values whose magnitude needs ≤ 63 bits; anything wider would
	// wrap silently in Int64().
	if q.BitLen() > 63 {
		return 0, fmt.Errorf("money value %s overflows int64 micros", q.String())
	}
	return q.Int64(), nil
}
