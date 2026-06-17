package budget

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// Store is the persistence interface Service depends on. Narrow on purpose —
// every method maps to a specific RPC / hook need — so tests satisfy it with
// a small in-memory fake (see service_test.go).
type Store interface {
	// UpsertBudget writes one budget keyed (scope, scope_id); a re-set
	// updates limit/alert_percents/active/account_id in place. Returns the
	// persisted row.
	UpsertBudget(ctx context.Context, b Budget) (Budget, error)

	// GetBudget resolves the budget for a (scope, scope_id), or (zero, false)
	// when none exists. found=false is a normal "no budget configured"
	// outcome, not an error.
	GetBudget(ctx context.Context, scope Scope, scopeID uuid.UUID) (Budget, bool, error)

	// AppPeriodSpendMicros sums the app's spend (Σ value × unit_price) in
	// [periodStart, periodEnd), in micro-dollars (NULL price → 0). Decoded
	// through the same single rounding point the usage summary uses.
	AppPeriodSpendMicros(ctx context.Context, appID uuid.UUID, periodStart, periodEnd time.Time) (int64, error)

	// InsertBudgetAlerts records a batch of threshold crossings in ONE
	// transaction (all-or-nothing): either every row is committed or none are,
	// so a partial set of alerts with an inconsistent spend snapshot is never
	// persisted. Each insert is idempotent on (budget, period, percent) via
	// ON CONFLICT DO NOTHING. Returns the percents that were freshly recorded
	// this call, ascending; an already-recorded crossing is skipped (not in
	// the result). An empty batch is a no-op returning nil.
	InsertBudgetAlerts(ctx context.Context, records []AlertRecord) (fired []int, err error)

	// ListBudgetAlerts returns a budget's recorded crossings for a period.
	ListBudgetAlerts(ctx context.Context, budgetID uuid.UUID, periodStart time.Time) ([]BudgetAlert, error)
}

// AlertRecord is the raw crossing handed to InsertBudgetAlert.
type AlertRecord struct {
	BudgetID    uuid.UUID
	PeriodStart time.Time
	Percent     int
	SpendMicros int64
	LimitMicros int64
}

// NewStore returns a Store backed by the given pgxpool.
func NewStore(pool *pgxpool.Pool) Store {
	return &pgxStore{pool: pool, q: db.New(pool)}
}

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func (s *pgxStore) UpsertBudget(ctx context.Context, b Budget) (Budget, error) {
	row, err := s.q.UpsertBudget(ctx, db.UpsertBudgetParams{
		Scope:         db.MsBillingBudgetScope(b.Scope),
		ScopeID:       b.ScopeID.String(),
		AccountID:     nullableAccountID(b.AccountID),
		LimitMicros:   b.LimitMicros,
		AlertPercents: intsToInt32(b.AlertPercents),
		Active:        b.Active,
	})
	if err != nil {
		return Budget{}, err
	}
	return budgetFromRow(row)
}

func (s *pgxStore) GetBudget(ctx context.Context, scope Scope, scopeID uuid.UUID) (Budget, bool, error) {
	row, err := s.q.GetBudget(ctx, db.GetBudgetParams{
		Scope:   db.MsBillingBudgetScope(scope),
		ScopeID: scopeID.String(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Budget{}, false, nil
	}
	if err != nil {
		return Budget{}, false, err
	}
	b, err := budgetFromRow(row)
	if err != nil {
		return Budget{}, false, err
	}
	return b, true, nil
}

func (s *pgxStore) AppPeriodSpendMicros(ctx context.Context, appID uuid.UUID, periodStart, periodEnd time.Time) (int64, error) {
	n, err := s.q.AppPeriodSpendMicros(ctx, db.AppPeriodSpendMicrosParams{
		AppID:        appID.String(),
		RecordedAt:   periodStart,
		RecordedAt_2: periodEnd,
	})
	if err != nil {
		return 0, err
	}
	// Decode through the SAME rounding point CurrentPeriodUsage uses — money
	// never goes through float64.
	return usage.MicrosFromNumeric(n)
}

func (s *pgxStore) InsertBudgetAlerts(ctx context.Context, records []AlertRecord) ([]int, error) {
	if len(records) == 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	// Rollback is a no-op after a successful Commit, so the deferred rollback
	// safely covers every early-return error path without double-handling.
	defer tx.Rollback(ctx)

	qtx := s.q.WithTx(tx)
	var fired []int
	for _, a := range records {
		rows, err := qtx.InsertBudgetAlert(ctx, db.InsertBudgetAlertParams{
			BudgetID:    a.BudgetID.String(),
			PeriodStart: a.PeriodStart,
			Percent:     int32(a.Percent),
			SpendMicros: a.SpendMicros,
			LimitMicros: a.LimitMicros,
		})
		if err != nil {
			return nil, err
		}
		// :execrows returns 1 on a fresh insert, 0 when ON CONFLICT deduped.
		if rows > 0 {
			fired = append(fired, a.Percent)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return fired, nil
}

func (s *pgxStore) ListBudgetAlerts(ctx context.Context, budgetID uuid.UUID, periodStart time.Time) ([]BudgetAlert, error) {
	rows, err := s.q.ListBudgetAlerts(ctx, db.ListBudgetAlertsParams{
		BudgetID:    budgetID.String(),
		PeriodStart: periodStart,
	})
	if err != nil {
		return nil, err
	}
	out := make([]BudgetAlert, 0, len(rows))
	for _, r := range rows {
		out = append(out, BudgetAlert{
			Percent:     int(r.Percent),
			SpendMicros: r.SpendMicros,
			LimitMicros: r.LimitMicros,
			PeriodStart: r.PeriodStart,
			FiredAt:     r.FiredAt,
		})
	}
	return out, nil
}

// budgetFromRow maps a generated row to the domain Budget, parsing the UUID
// PKs and the nullable account UUID.
func budgetFromRow(row db.MsBillingBudget) (Budget, error) {
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return Budget{}, err
	}
	scopeID, err := uuid.Parse(row.ScopeID)
	if err != nil {
		return Budget{}, err
	}
	accountID := uuid.Nil
	if row.AccountID.Valid {
		accountID = row.AccountID.Bytes
	}
	return Budget{
		ID:            id,
		Scope:         Scope(row.Scope),
		ScopeID:       scopeID,
		AccountID:     accountID,
		LimitMicros:   row.LimitMicros,
		AlertPercents: int32sToInt(row.AlertPercents),
		Active:        row.Active,
	}, nil
}

// nullableAccountID maps a Nil account UUID to a SQL NULL (the lazy budget
// case) and a real UUID to a valid pgtype.UUID.
func nullableAccountID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{} // Valid: false → NULL
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func intsToInt32(in []int) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v)
	}
	return out
}

func int32sToInt(in []int32) []int {
	out := make([]int, len(in))
	for i, v := range in {
		out[i] = int(v)
	}
	return out
}
