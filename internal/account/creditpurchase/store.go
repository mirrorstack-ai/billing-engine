package creditpurchase

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

type pgxStore struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

func NewStore(pool *pgxpool.Pool) Store {
	if pool == nil {
		panic("creditpurchase.NewStore: pool must not be nil")
	}
	return &pgxStore{pool: pool, q: db.New(pool)}
}

func (s *pgxStore) Get(
	ctx context.Context,
	accountID, attemptID uuid.UUID,
) (Attempt, error) {
	row, err := s.q.GetCreditPurchaseByID(ctx, db.GetCreditPurchaseByIDParams{
		PurchaseID: attemptID.String(),
		AccountID:  accountID.String(),
	})
	if err != nil {
		return Attempt{}, err
	}
	return decodeAttempt(
		row.ID,
		row.AccountID,
		row.AmountMicros,
		row.Status,
		row.StripeInvoiceID,
		row.ReceiptUrl,
	)
}

func (s *pgxStore) FindByStripeInvoice(
	ctx context.Context,
	stripeInvoiceID string,
) (Attempt, bool, error) {
	if stripeInvoiceID == "" {
		return Attempt{}, false, nil
	}
	hint, err := s.q.FindCreditAttemptByStripeInvoice(ctx, stripeInvoiceID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, false, nil
	}
	if err != nil {
		return Attempt{}, false, err
	}
	if hint.Type != "purchase" {
		return Attempt{}, false, nil
	}
	accountID, err := uuid.Parse(hint.AccountID)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("parse manual purchase account id: %w", err)
	}
	attemptID, err := uuid.Parse(hint.ID)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("parse manual purchase ledger id: %w", err)
	}
	attempt, err := s.Get(ctx, accountID, attemptID)
	if err != nil {
		return Attempt{}, false, err
	}
	if attempt.StripeInvoiceID != stripeInvoiceID {
		return Attempt{}, false, fmt.Errorf(
			"manual purchase %s is attached to invoice %q, not %q",
			attempt.ID,
			attempt.StripeInvoiceID,
			stripeInvoiceID,
		)
	}
	return attempt, true, nil
}

func (s *pgxStore) AttachInvoice(
	ctx context.Context,
	attempt Attempt,
	invoice billingstripe.Invoice,
) (Attempt, error) {
	if invoice.ID == "" {
		return Attempt{}, fmt.Errorf("stripe invoice id required")
	}
	row, err := s.q.AttachCreditPurchaseInvoice(ctx, db.AttachCreditPurchaseInvoiceParams{
		StripeInvoiceID: invoice.ID,
		ReceiptUrl:      invoice.HostedInvoiceURL,
		PurchaseID:      attempt.ID.String(),
		AccountID:       attempt.AccountID.String(),
	})
	if err != nil {
		return Attempt{}, err
	}
	current, err := decodeAttempt(
		row.ID,
		row.AccountID,
		row.AmountMicros,
		row.Status,
		row.StripeInvoiceID,
		row.ReceiptUrl,
	)
	if err != nil {
		return Attempt{}, err
	}
	if current.StripeInvoiceID != invoice.ID {
		return Attempt{}, fmt.Errorf(
			"manual purchase %s is already attached to invoice %q",
			attempt.ID,
			current.StripeInvoiceID,
		)
	}
	current.StripeCustomerID = invoice.CustomerID
	return current, nil
}

func (s *pgxStore) Fail(
	ctx context.Context,
	attempt Attempt,
	receiptURL string,
) (Attempt, bool, error) {
	var (
		current      Attempt
		transitioned bool
	)
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		balance, err := qtx.LockCreditAccountBalance(
			ctx,
			attempt.AccountID.String(),
		)
		if err != nil {
			return err
		}
		row, err := qtx.GetCreditPurchaseByID(
			ctx,
			db.GetCreditPurchaseByIDParams{
				PurchaseID: attempt.ID.String(),
				AccountID:  attempt.AccountID.String(),
			},
		)
		if err != nil {
			return err
		}
		current, err = decodeAttempt(
			row.ID,
			row.AccountID,
			row.AmountMicros,
			row.Status,
			row.StripeInvoiceID,
			row.ReceiptUrl,
		)
		if err != nil {
			return err
		}
		if current.StripeInvoiceID != attempt.StripeInvoiceID {
			return fmt.Errorf(
				"manual purchase %s is attached to invoice %q, not %q",
				attempt.ID,
				current.StripeInvoiceID,
				attempt.StripeInvoiceID,
			)
		}
		if current.Status != "pending" {
			return nil
		}
		failed, err := qtx.FinalizeCreditPurchase(
			ctx,
			db.FinalizeCreditPurchaseParams{
				Status:             "failed",
				BalanceAfterMicros: balance.BalanceMicros,
				ReceiptUrl:         receiptURL,
				PurchaseID:         attempt.ID.String(),
				AccountID:          attempt.AccountID.String(),
			},
		)
		if err != nil {
			return err
		}
		current, err = decodeAttempt(
			failed.ID,
			failed.AccountID,
			failed.AmountMicros,
			failed.Status,
			failed.StripeInvoiceID,
			failed.ReceiptUrl,
		)
		if err != nil {
			return err
		}
		transitioned = true
		return nil
	})
	if err != nil {
		return Attempt{}, false, err
	}
	return current, transitioned, nil
}

func decodeAttempt(
	idRaw, accountIDRaw string,
	amountMicros int64,
	status, stripeInvoiceID, receiptURL string,
) (Attempt, error) {
	id, err := uuid.Parse(idRaw)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse manual purchase ledger id: %w", err)
	}
	accountID, err := uuid.Parse(accountIDRaw)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse manual purchase account id: %w", err)
	}
	return Attempt{
		ID:              id,
		AccountID:       accountID,
		AmountMicros:    amountMicros,
		Status:          status,
		StripeInvoiceID: stripeInvoiceID,
		ReceiptURL:      receiptURL,
	}, nil
}
