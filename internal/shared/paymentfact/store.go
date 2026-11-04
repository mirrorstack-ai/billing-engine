package paymentfact

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RecordTx writes a payment fact on the caller's own transaction, or returns
// the fact that already exists for (rail, rail_ref, mode).
//
// This is the function every writer calls from INSIDE the transaction that
// makes the money real (billing-engine#313: "there is never a settled payment
// without a fact"). It is intentionally a free function over a pgx.Tx rather
// than a method on a *Store holding its own pool: the executor's store, a
// webhook store and an operator's statement store each manage their own
// transaction already, and a payment fact has to land INSIDE it, not in a
// second round trip that could commit alone after the first one fails.
//
// Returns created=true when this call inserted the row, false when a prior
// delivery already had — the whole point being that a rail's at-least-once
// notice (NewebPay has no event id at all) replays into the same row rather
// than a second fact.
func RecordTx(ctx context.Context, tx pgx.Tx, n New) (Fact, bool, error) {
	if err := n.validate(); err != nil {
		return Fact{}, false, err
	}

	var reason *string
	if n.DecisionReason != "" {
		reason = &n.DecisionReason
	}

	var (
		id        uuid.UUID
		createdAt time.Time
	)
	err := tx.QueryRow(ctx, `
		INSERT INTO ms_billing.payment_facts
		  (rail, rail_ref, mode, source_kind, payer_account_id, seller_id,
		   currency, net_amount_micros, tax_amount_micros, gross_amount_micros,
		   paid_at, buyer_snapshot, invoice_decision, decision_reason)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (rail, rail_ref, mode) DO NOTHING
		RETURNING id, created_at`,
		string(n.Rail), n.RailRef, string(n.Mode), string(n.SourceKind),
		n.PayerAccountID, n.SellerID,
		n.Currency, n.NetAmountMicros, n.TaxAmountMicros, n.GrossAmountMicros,
		n.PaidAt, nullableBytes(n.BuyerSnapshot), string(n.InvoiceDecision), reason,
	).Scan(&id, &createdAt)

	switch {
	case err == nil:
		return Fact{New: n, ID: id, CreatedAt: createdAt}, true, nil
	case errors.Is(err, pgx.ErrNoRows):
		existing, loadErr := loadTx(ctx, tx, n.Rail, n.RailRef, n.Mode)
		if loadErr != nil {
			return Fact{}, false, fmt.Errorf("load existing payment fact after conflict: %w", loadErr)
		}
		return existing, false, nil
	default:
		return Fact{}, false, fmt.Errorf("insert payment fact: %w", err)
	}
}

// Store is the standalone entry point for a writer that is not already
// inside a transaction of its own — e.g. a one-off backfill or a test. Every
// production writer that settles money should prefer RecordTx on its own
// transaction instead, so the fact commits with the settlement rather than
// after it.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore returns a Store over the given pool.
func NewStore(pool *pgxpool.Pool) *Store {
	if pool == nil {
		panic("paymentfact.NewStore: pool must not be nil")
	}
	return &Store{pool: pool}
}

// Record writes a payment fact in its own transaction.
func (s *Store) Record(ctx context.Context, n New) (Fact, bool, error) {
	var (
		f       Fact
		created bool
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var txErr error
		f, created, txErr = RecordTx(ctx, tx, n)
		return txErr
	})
	if err != nil {
		return Fact{}, false, err
	}
	return f, created, nil
}

// Load returns the fact for (rail, rail_ref, mode), or an error wrapping
// pgx.ErrNoRows when none exists.
func (s *Store) Load(ctx context.Context, rail Rail, railRef string, mode Mode) (Fact, error) {
	var f Fact
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var txErr error
		f, txErr = loadTx(ctx, tx, rail, railRef, mode)
		return txErr
	})
	return f, err
}

func loadTx(ctx context.Context, tx pgx.Tx, rail Rail, railRef string, mode Mode) (Fact, error) {
	var (
		id            uuid.UUID
		accountID     uuid.UUID
		sellerID      *uuid.UUID
		currency      string
		netM          int64
		taxM          int64
		grossM        int64
		paidAt        time.Time
		buyerSnapshot []byte
		decision      string
		reason        *string
		createdAt     time.Time
	)
	err := tx.QueryRow(ctx, `
		SELECT id, payer_account_id, seller_id, currency,
		       net_amount_micros, tax_amount_micros, gross_amount_micros,
		       paid_at, buyer_snapshot, invoice_decision, decision_reason, created_at
		  FROM ms_billing.payment_facts
		 WHERE rail = $1 AND rail_ref = $2 AND mode = $3`,
		string(rail), railRef, string(mode),
	).Scan(&id, &accountID, &sellerID, &currency, &netM, &taxM, &grossM,
		&paidAt, &buyerSnapshot, &decision, &reason, &createdAt)
	if err != nil {
		return Fact{}, err
	}
	n := New{
		Rail: rail, RailRef: railRef, Mode: mode,
		PayerAccountID: accountID, SellerID: sellerID, Currency: currency,
		NetAmountMicros: netM, TaxAmountMicros: taxM, GrossAmountMicros: grossM,
		PaidAt: paidAt, BuyerSnapshot: buyerSnapshot,
		InvoiceDecision: InvoiceDecision(decision),
	}
	if reason != nil {
		n.DecisionReason = *reason
	}
	return Fact{New: n, ID: id, CreatedAt: createdAt}, nil
}

func nullableBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
