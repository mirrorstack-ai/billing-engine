package cycle

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

// Store is the persistence interface the rollup + settlement Service depends
// on. Narrow on purpose — every method maps to a specific rollup step — so
// tests satisfy it with a small in-memory fake (see service_test.go).
type Store interface {
	// OpenPeriodForAccount upserts the billing_periods row keyed
	// (account_id, period_start) and returns its id. Idempotent: a re-run for
	// the same window returns the existing row's id rather than duplicating it.
	// period_end is the signup-day anniversary window end (period_start + 1
	// month), supplied by the caller.
	OpenPeriodForAccount(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, error)

	// RawAggregates aggregates the account's usage_events in [periodStart,
	// periodEnd) per (app, module, metric) by kind: count/sum → SUM(value),
	// peak → MAX(value), time_weighted → ∫ v dt (step-function integral). The
	// billable_quantity is returned as the exact NUMERIC string so the priced
	// row re-encodes it without a float round-trip.
	RawAggregates(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]RawAggregate, error)

	// MetricPriceMicros returns the per-unit customer price snapshotted onto
	// the aggregate. priced=false (NULL catalog price) → the metric is
	// metered-but-unpriced and prices to 0.
	MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric string) (micros int64, priced bool, err error)

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
}

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
// MAX, time_weighted integral).
type RawAggregate struct {
	AppID            uuid.UUID
	ModuleID         uuid.UUID
	Metric           string
	Kind             Kind
	BillableQuantity string
}

// ModuleIncome pairs a module with its period income (Σ charged_micros).
type ModuleIncome struct {
	ModuleID     uuid.UUID
	IncomeMicros int64
}

// NewStore returns a Store backed by the given pgxpool.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{q: db.New(pool)}
}

type pgxStore struct {
	q *db.Queries
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
	appendRow := func(appID, moduleID, metric string, kind db.MsBillingMetricKind, qty pgtype.Numeric) error {
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
			BillableQuantity: numericString(qty),
		})
		return nil
	}
	for _, r := range sumRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range peakRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range twRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *pgxStore) MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric string) (int64, bool, error) {
	price, err := s.q.LookupMetricPrice(ctx, db.LookupMetricPriceParams{
		ModuleID: moduleID.String(),
		Metric:   metric,
	})
	if errors.Is(err, pgx.ErrNoRows) {
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
