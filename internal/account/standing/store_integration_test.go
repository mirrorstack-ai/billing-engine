//go:build integration

package standing_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/standing"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestOwnerByStripeInvoice_OrdinaryRouteNeverReadsCreditLedger(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID, ownerUserID := uuid.New(), uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerUserID, "cus_"+accountID.String(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.invoices (
			id, account_id, charge_funding_account_id, charge_funding_generation,
			stripe_invoice_id, status,
			amount_due, amount_paid, currency
		 ) SELECT $1, $2, auth.funding_account_id, auth.generation,
		          'in_ordinary_only', 'paid', 500, 500, 'usd'
		   FROM ms_billing.account_funding_authorizations auth
		   WHERE auth.account_id = $2`,
		uuid.New(), accountID,
	)
	require.NoError(t, err)

	lock, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = lock.Rollback(ctx) }()
	_, err = lock.Exec(ctx, `LOCK TABLE ms_billing.credit_ledger IN ACCESS EXCLUSIVE MODE`)
	require.NoError(t, err)

	store := standing.NewStore(pool)
	ordinaryCtx, cancelOrdinary := context.WithTimeout(ctx, time.Second)
	defer cancelOrdinary()
	owner, found, err := store.OwnerByStripeInvoice(ordinaryCtx, "in_ordinary_only")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)

	controlCtx, cancelControl := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelControl()
	_, _, err = store.OwnerByCreditInvoice(controlCtx, "in_missing_credit")
	require.Error(t, err,
		"the explicit credit route must hit the locked ledger, proving the no-touch tripwire")
}

func TestOwnerByCreditInvoice_UsesExplicitCreditLedgerRoute(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	accountID := uuid.New()
	ownerUserID := uuid.New()
	_, err := pool.Exec(ctx,
		`INSERT INTO ms_billing.accounts
		    (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, ownerUserID, "cus_"+accountID.String(),
	)
	require.NoError(t, err)
	_, err = pool.Exec(ctx,
		`INSERT INTO ms_billing.credit_ledger
		   (account_id, amount_micros, type, status, balance_after_micros,
		    actor, idempotency_key, stripe_invoice_id,
		    attempt_stripe_customer_id, charge_funding_account_id,
		    charge_funding_generation)
		 SELECT $1, 5000000, 'purchase', 'pending', 5000000,
		        'self', $2, 'in_ledger_only', account.stripe_customer_id,
		        funding.funding_account_id, funding.generation
		 FROM ms_billing.account_funding_authorizations funding
		 JOIN ms_billing.accounts account ON account.id=funding.funding_account_id
		 WHERE funding.account_id=$1`,
		accountID, "purchase:"+uuid.NewString(),
	)
	require.NoError(t, err)

	owner, found, err := standing.NewStore(pool).OwnerByCreditInvoice(ctx, "in_ledger_only")

	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, ownerUserID, owner.UserID)
	require.Equal(t, uuid.Nil, owner.OrgID)
}

// TestEpisodeStore_DueAndFailureCounter pins the sweep's work list and the
// failed-send counter against the real schema (migrations 093 + 094): an open
// episode is due when its notice is unsent or its 14 days are over, a closed
// one never is, and the owner columns come back for the re-evaluation.
func TestEpisodeStore_DueAndFailureCounter(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	store := standing.NewEpisodeStore(pool)
	now := time.Date(2026, 11, 8, 3, 0, 0, 0, time.UTC)

	mk := func(notice, closed *time.Time) (uuid.UUID, uuid.UUID, uuid.UUID) {
		acct, user, ep := uuid.New(), uuid.New(), uuid.New()
		_, err := pool.Exec(ctx,
			`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
			 VALUES ($1, 'user', $2, $3)`, acct, user, "cus_"+acct.String())
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			`INSERT INTO ms_billing.unpaid_episodes (id, account_id, started_at, notice_sent_at, closed_at)
			 VALUES ($1, $2, $3, $4, $5)`, ep, acct, now.Add(-30*24*time.Hour), notice, closed)
		require.NoError(t, err)
		return acct, user, ep
	}
	day := func(d float64) *time.Time { t := now.Add(-time.Duration(d * 24 * float64(time.Hour))); return &t }

	_, _, unsent := mk(nil, nil)
	_, userOver, over := mk(day(14), nil) // 14 d exactly: due
	_, _, within := mk(day(13.9), nil)    // inside the grace: not due
	_, _, closedEp := mk(nil, day(1))     // closed: never due

	due, err := store.DueUnpaidEpisodes(ctx, now, uuid.Nil, 10)
	require.NoError(t, err)
	got := map[uuid.UUID]standing.Owner{}
	for _, d := range due {
		got[d.ID] = d.Owner
	}
	require.Contains(t, got, unsent)
	require.Contains(t, got, over)
	require.Equal(t, userOver, got[over].UserID)
	require.NotContains(t, got, within)
	require.NotContains(t, got, closedEp)

	n, err := store.RecordNoticeFailure(ctx, unsent)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = store.RecordNoticeFailure(ctx, unsent)
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

// TestUnpaidGracePreviewScriptRuns executes the operator preview against the
// real schema: it must parse, stay read-only, and list nobody on an empty DB.
func TestUnpaidGracePreviewScriptRuns(t *testing.T) {
	pool := testutil.NewTestDB(t)
	ctx := context.Background()
	raw, err := os.ReadFile("../../../scripts/unpaid-grace-preview.sql")
	require.NoError(t, err)
	for _, verb := range []string{"INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "ALTER ", "DROP "} {
		require.NotContains(t, strings.ToUpper(string(raw)), verb, "the preview must be read-only")
	}
	rows, err := pool.Query(ctx, string(raw))
	require.NoError(t, err)
	defer rows.Close()
	require.False(t, rows.Next(), "an empty database has nobody to list")
	require.NoError(t, rows.Err())
}
