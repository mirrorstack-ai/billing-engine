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

	// InsertUsageEvent writes one raw metered fact, idempotent on
	// event_id. recorded=false means ON CONFLICT(event_id) DO NOTHING
	// deduped an at-least-once retry — NOT an error. accountID is the
	// zero UUID for a lazy (account-less) event, stored as NULL.
	InsertUsageEvent(ctx context.Context, ev UsageEvent) (recorded bool, err error)

	// AccountByOwner resolves the billing account for an owner principal
	// (user or org), or (Nil, false) when none exists yet. Read-only;
	// missing-account is a normal lazy-state outcome, not an error.
	AccountByOwner(ctx context.Context, owner Owner) (uuid.UUID, bool, error)

	// CurrentPeriodUsage sums raw usage_events for the account in
	// [periodStart, periodEnd), joined to metric_definitions, projecting
	// raw cost per metric = quantity × unit_price. For custom metrics this
	// declared price IS the customer charge (no blanket markup).
	CurrentPeriodUsage(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]MetricUsageRaw, error)

	// UpsertModuleVisibility records a module's published/private
	// visibility (developer margin-share dimension; NEVER a customer
	// markup). Idempotent on module_id.
	UpsertModuleVisibility(ctx context.Context, moduleID uuid.UUID, vis Visibility) error
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
// a NULL usage_events.model.
type UsageEvent struct {
	EventID    string
	AccountID  uuid.UUID
	AppID      uuid.UUID
	ModuleID   uuid.UUID
	Metric     string
	Kind       Kind
	Value      float64
	RecordedAt time.Time
	Model      string
}

// MetricUsageRaw is one grouped row from the live current-period query.
// RawCostMicros = quantity × unit_price, rounded to whole micro-dollars
// (round-half-up) at the store boundary. For custom metrics this IS the
// customer charge (no blanket markup applied by the service).
type MetricUsageRaw struct {
	Metric          string
	Kind            Kind
	Quantity        float64
	UnitPriceMicros int64
	RawCostMicros   int64
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

func (s *pgxStore) InsertUsageEvent(ctx context.Context, ev UsageEvent) (bool, error) {
	value, err := numericFromFloat(ev.Value)
	if err != nil {
		return false, err
	}
	rows, err := s.q.InsertUsageEvent(ctx, db.InsertUsageEventParams{
		EventID:    ev.EventID,
		AccountID:  nullableAccountID(ev.AccountID),
		AppID:      ev.AppID.String(),
		ModuleID:   ev.ModuleID.String(),
		Metric:     ev.Metric,
		Kind:       db.MsBillingMetricKind(ev.Kind),
		Value:      value,
		RecordedAt: ev.RecordedAt,
		Model:      nullableModel(ev.Model),
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
		out = append(out, MetricUsageRaw{
			Metric:          r.Metric,
			Kind:            Kind(r.Kind),
			Quantity:        qty,
			UnitPriceMicros: r.UnitPriceMicros,
			RawCostMicros:   rawCost,
		})
	}
	return out, nil
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
