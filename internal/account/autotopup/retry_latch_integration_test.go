//go:build integration

package autotopup_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	stripego "github.com/stripe/stripe-go/v85"

	"github.com/mirrorstack-ai/billing-engine/internal/account/autotopup"
	"github.com/mirrorstack-ai/billing-engine/internal/account/creditledger"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestExecutorTrigger_DeterministicDeclineLatchesUntilConfigRevisionIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)

	var triggerNow time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT updated_at
		FROM ms_billing.credit_auto_topup_configs
		WHERE account_id = $1`,
		accountID,
	).Scan(&triggerNow))

	stripe := newDecliningAutoTopUpStripe()
	executor := autotopup.NewExecutor(
		autotopup.NewStore(pool),
		neverAutoTopUpSettler{},
		stripe,
	).WithNow(func() time.Time { return triggerNow })

	first, err := executor.Trigger(ctx, accountID, 1)
	require.NoError(t, err)
	require.True(t, first.Triggered)
	require.True(t, first.NewAttempt)
	require.Equal(t, "failed", first.Status)
	require.Equal(t, "insufficient_funds", first.FailureCode)
	require.Equal(t, 1, stripe.createCalls)
	require.Equal(t, 1, stripe.payCalls)
	require.Equal(t, 1, stripe.voidCalls)

	for i := 1; i <= 5; i++ {
		triggerNow = triggerNow.Add(time.Second)
		result, triggerErr := executor.Trigger(ctx, accountID, 1)
		require.NoError(t, triggerErr)
		require.Equal(t, autotopup.Result{}, result,
			"the failed config revision must turn repeated Trigger calls into no-ops")
	}
	require.Equal(t, 1, stripe.createCalls,
		"repeated status/usage probes must not create another Stripe invoice")
	require.Equal(t, 1, countAutoTopUps(t, pool, accountID))

	var rearmedRevision time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		UPDATE ms_billing.credit_auto_topup_configs
		SET enabled = enabled
		WHERE account_id = $1
		RETURNING updated_at`,
		accountID,
	).Scan(&rearmedRevision))
	require.True(t, rearmedRevision.After(firstAttemptCreatedAt(t, pool, first.AttemptID)))
	triggerNow = rearmedRevision

	retry, err := executor.Trigger(ctx, accountID, 1)
	require.NoError(t, err)
	require.True(t, retry.Triggered)
	require.True(t, retry.NewAttempt)
	require.NotEqual(t, first.AttemptID, retry.AttemptID)
	require.Equal(t, "failed", retry.Status)
	require.Equal(t, 2, stripe.createCalls,
		"an explicit config revision re-arms exactly one Stripe attempt")
	require.Equal(t, 2, stripe.payCalls)
	require.Equal(t, 2, stripe.voidCalls)
	require.Equal(t, 2, countAutoTopUps(t, pool, accountID))

	triggerNow = triggerNow.Add(time.Second)
	latchedAgain, err := executor.Trigger(ctx, accountID, 1)
	require.NoError(t, err)
	require.Equal(t, autotopup.Result{}, latchedAgain)
	require.Equal(t, 2, stripe.createCalls)
	require.Equal(t, 2, countAutoTopUps(t, pool, accountID))
}

func TestExecutorTrigger_ClockBehindPolicyCannotBypassFailureLatchIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)

	var policyRevision time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT updated_at
		FROM ms_billing.credit_auto_topup_configs
		WHERE account_id = $1`,
		accountID,
	).Scan(&policyRevision))

	appNow := policyRevision.Add(-6 * time.Hour)
	stripe := newDecliningAutoTopUpStripe()
	executor := autotopup.NewExecutor(
		autotopup.NewStore(pool),
		neverAutoTopUpSettler{},
		stripe,
	).WithNow(func() time.Time { return appNow })

	first, err := executor.Trigger(ctx, accountID, 1)
	require.NoError(t, err)
	require.Equal(t, "failed", first.Status)
	require.Equal(t, 1, stripe.createCalls)

	var createdAt, expiresAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT created_at, attempt_expires_at
		FROM ms_billing.credit_ledger
		WHERE id = $1`,
		first.AttemptID,
	).Scan(&createdAt, &expiresAt))
	require.False(t, createdAt.Before(policyRevision),
		"attempt time must be anchored to the DB policy revision")
	require.Equal(t, autotopup.PendingGrace, expiresAt.Sub(createdAt))

	for range 3 {
		appNow = appNow.Add(time.Minute)
		result, triggerErr := executor.Trigger(ctx, accountID, 1)
		require.NoError(t, triggerErr)
		require.Equal(t, autotopup.Result{}, result)
	}
	require.Equal(t, 1, stripe.createCalls,
		"a lagging application clock must not re-arm the same failed policy")
	require.Equal(t, 1, countAutoTopUps(t, pool, accountID))
}

func TestStoreAcquire_DatabaseClockBoundsPendingGraceAcrossApplicationSkewIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
	store := autotopup.NewStore(pool)

	var dbBefore time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT CURRENT_TIMESTAMP`).Scan(&dbBefore))
	appClockAhead := dbBefore.Add(6 * time.Hour)
	attempt, kind, err := store.Acquire(ctx, accountID, 1, appClockAhead)
	require.NoError(t, err)
	require.Equal(t, autotopup.AcquireNew, kind)
	require.False(t, attempt.Expired(appClockAhead),
		"an application clock ahead of Aurora must not expire a fresh attempt")
	require.False(t, attempt.CreatedAt.Before(dbBefore))
	require.Less(t, attempt.CreatedAt, appClockAhead.Add(-5*time.Hour),
		"created_at must come from Aurora rather than the future application clock")
	require.Equal(t, autotopup.PendingGrace, attempt.ExpiresAt.Sub(attempt.CreatedAt))

	_, err = pool.Exec(ctx, `
		UPDATE ms_billing.credit_ledger
		SET created_at = CURRENT_TIMESTAMP - INTERVAL '11 minutes',
		    attempt_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
		WHERE id = $1`,
		attempt.ID,
	)
	require.NoError(t, err)

	expired, found, err := store.Pending(ctx, accountID)
	require.NoError(t, err)
	require.True(t, found)
	appClockBehind := dbBefore.Add(-6 * time.Hour)
	require.True(t, expired.Expired(appClockBehind),
		"an application clock behind Aurora must not extend an expired durable attempt")
}

func TestStoreAcquire_GraceClockSampleOccursAfterAccountLockWaitIntegration(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, _ := seedEligibleAutoTopUp(t, pool, 0, true)
	store := autotopup.NewStore(pool)

	lockTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockTx.Rollback(ctx) })
	_, err = lockTx.Exec(
		ctx,
		`SELECT id FROM ms_billing.accounts WHERE id = $1 FOR UPDATE`,
		accountID,
	)
	require.NoError(t, err)

	type acquireResult struct {
		attempt autotopup.Attempt
		kind    autotopup.AcquireKind
		err     error
	}
	resultCh := make(chan acquireResult, 1)
	go func() {
		attempt, kind, acquireErr := store.Acquire(
			ctx,
			accountID,
			1,
			time.Now().UTC().Add(6*time.Hour),
		)
		resultCh <- acquireResult{attempt: attempt, kind: kind, err: acquireErr}
	}()

	require.Eventually(t, func() bool {
		var waiting bool
		queryErr := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%LockAutoTopUpAccount%'
			)`,
		).Scan(&waiting)
		return queryErr == nil && waiting
	}, 5*time.Second, 10*time.Millisecond,
		"Acquire must be observed waiting on the account lock")

	releasedAt := time.Now().UTC()
	require.NoError(t, lockTx.Commit(ctx))
	result := <-resultCh
	require.NoError(t, result.err)
	require.Equal(t, autotopup.AcquireNew, result.kind)
	require.False(t, result.attempt.CreatedAt.Before(releasedAt.Add(-25*time.Millisecond)),
		"created_at must sample DB wall time after the account lock wait")
	require.Equal(
		t,
		autotopup.PendingGrace,
		result.attempt.ExpiresAt.Sub(result.attempt.CreatedAt),
	)
}

func firstAttemptCreatedAt(t *testing.T, pool *pgxpool.Pool, attemptID uuid.UUID) time.Time {
	t.Helper()
	var createdAt time.Time
	require.NoError(t, pool.QueryRow(
		context.Background(),
		`SELECT created_at FROM ms_billing.credit_ledger WHERE id = $1`,
		attemptID,
	).Scan(&createdAt))
	return createdAt
}

type neverAutoTopUpSettler struct{}

func (neverAutoTopUpSettler) SettleStripeInvoice(
	context.Context,
	string,
	int64,
	string,
	string,
) (creditledger.Settlement, error) {
	return creditledger.Settlement{}, fmt.Errorf("unexpected paid settlement")
}

type decliningAutoTopUpStripe struct {
	invoices    map[string]billingstripe.Invoice
	items       map[string][]billingstripe.InvoiceItem
	createCalls int
	payCalls    int
	voidCalls   int
}

func newDecliningAutoTopUpStripe() *decliningAutoTopUpStripe {
	return &decliningAutoTopUpStripe{
		invoices: make(map[string]billingstripe.Invoice),
		items:    make(map[string][]billingstripe.InvoiceItem),
	}
}

func (s *decliningAutoTopUpStripe) CreateAutoTopUpInvoice(
	_ context.Context,
	customerID string,
	paymentMethodID string,
	accountID string,
	ledgerID string,
	_ string,
) (billingstripe.Invoice, error) {
	s.createCalls++
	id := fmt.Sprintf("in_retry_latch_%d", s.createCalls)
	invoice := billingstripe.Invoice{
		ID:                     id,
		CustomerID:             customerID,
		Status:                 "draft",
		CollectionMethod:       string(stripego.InvoiceCollectionMethodChargeAutomatically),
		AutoAdvance:            false,
		DefaultPaymentMethodID: paymentMethodID,
		ChargeRef:              "credit-auto-topup:" + ledgerID,
		CreditOperation:        "auto_topup",
		CreditAccountID:        accountID,
		CreditLedgerID:         ledgerID,
		HostedInvoiceURL:       "https://stripe.test/" + id,
	}
	s.invoices[id] = invoice
	return invoice, nil
}

func (s *decliningAutoTopUpStripe) CreateInvoiceItem(
	_ context.Context,
	_ string,
	invoiceID string,
	amountCents int64,
	currency string,
	_ string,
	_ billingstripe.LinePeriod,
	_ string,
) (billingstripe.InvoiceItem, error) {
	item := billingstripe.InvoiceItem{
		ID:          "ii_" + invoiceID,
		AmountCents: amountCents,
		Currency:    currency,
	}
	s.items[invoiceID] = []billingstripe.InvoiceItem{item}
	invoice := s.invoices[invoiceID]
	invoice.AmountDue = amountCents
	invoice.Total = amountCents
	invoice.Currency = currency
	s.invoices[invoiceID] = invoice
	return item, nil
}

func (s *decliningAutoTopUpStripe) ListInvoiceItems(
	_ context.Context,
	invoiceID string,
) ([]billingstripe.InvoiceItem, error) {
	return append([]billingstripe.InvoiceItem(nil), s.items[invoiceID]...), nil
}

func (*decliningAutoTopUpStripe) ListInvoicePayments(
	context.Context,
	string,
) ([]billingstripe.InvoicePaymentProof, error) {
	return nil, nil
}

func (s *decliningAutoTopUpStripe) FinalizeInvoiceWithoutAutoAdvance(
	_ context.Context,
	invoiceID string,
	_ string,
) (billingstripe.Invoice, error) {
	invoice := s.invoices[invoiceID]
	invoice.Status = "open"
	s.invoices[invoiceID] = invoice
	return invoice, nil
}

func (s *decliningAutoTopUpStripe) PayInvoiceWithMethod(
	_ context.Context,
	invoiceID string,
	_ string,
	_ string,
) (billingstripe.Invoice, error) {
	s.payCalls++
	return s.invoices[invoiceID], &stripego.Error{
		Type:        stripego.ErrorTypeCard,
		DeclineCode: stripego.DeclineCode("insufficient_funds"),
	}
}

func (s *decliningAutoTopUpStripe) GetInvoice(
	_ context.Context,
	invoiceID string,
) (billingstripe.Invoice, error) {
	return s.invoices[invoiceID], nil
}

func (*decliningAutoTopUpStripe) FindInvoiceByRef(
	context.Context,
	string,
	string,
) (billingstripe.Invoice, bool, error) {
	return billingstripe.Invoice{}, false, nil
}

func (s *decliningAutoTopUpStripe) VoidInvoice(
	_ context.Context,
	invoiceID string,
	_ string,
) (billingstripe.Invoice, error) {
	s.voidCalls++
	invoice := s.invoices[invoiceID]
	invoice.Status = "void"
	s.invoices[invoiceID] = invoice
	return invoice, nil
}

func (*decliningAutoTopUpStripe) DeleteDraftInvoice(
	context.Context,
	string,
) (billingstripe.Invoice, error) {
	return billingstripe.Invoice{}, fmt.Errorf("unexpected draft deletion")
}
