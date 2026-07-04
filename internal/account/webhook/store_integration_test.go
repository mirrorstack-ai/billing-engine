//go:build integration

package webhook_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/webhook"
	"github.com/mirrorstack-ai/billing-engine/internal/shared/testutil"
)

func TestPgxStore_MarkEventProcessed_FirstTimeAndDuplicate(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	firstTime, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.True(t, firstTime)

	firstTime2, err := store.MarkEventProcessed(context.Background(), "evt_test_1", "customer.created")
	require.NoError(t, err)
	require.False(t, firstTime2, "second insert of same event_id should return firstTime=false")
}

func TestPgxStore_TouchAccountByStripeCustomer_FoundAndNotFound(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_touch_a")

	found, err := store.TouchAccountByStripeCustomer(context.Background(), "cus_touch_a")
	require.NoError(t, err)
	require.True(t, found)

	found, err = store.TouchAccountByStripeCustomer(context.Background(), "cus_nonexistent")
	require.NoError(t, err)
	require.False(t, found, "non-existent stripe_customer_id should return found=false")
}

func TestPgxStore_InsertPaymentMethod_Success(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_pm_x")

	found, err := store.InsertPaymentMethod(context.Background(), "cus_pm_x", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_new_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_new_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_InsertPaymentMethod_Drift_NoAccountsRow(t *testing.T) {
	// The CTE inserts via `SELECT acct.id ... FROM acct` — if the acct
	// CTE produces zero rows (no matching stripe_customer_id), nothing
	// is inserted and RowsAffected=0. The follow-up existence check
	// confirms drift; store returns found=false.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	found, err := store.InsertPaymentMethod(context.Background(), "cus_orphan", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_orphan",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.False(t, found, "expected drift signal (no accounts row)")

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_orphan").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "no mirror row should have been inserted")
}

func TestPgxStore_InsertPaymentMethod_Idempotent_AccountExistsPMExists(t *testing.T) {
	// PM already in mirror → ON CONFLICT DO NOTHING → RowsAffected=0
	// BUT the follow-up existence check confirms the accounts row IS
	// present, so found=true (idempotent retry; mirror unchanged).
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	_ = seedAccount(t, pool, "cus_idem")

	found, err := store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found)

	// Duplicate insert (simulated Stripe webhook retry).
	found, err = store.InsertPaymentMethod(context.Background(), "cus_idem", webhook.InsertPaymentMethodParams{
		StripePaymentMethodID: "pm_idem_1",
		Brand:                 "visa",
		Last4:                 "4242",
		ExpMonth:              12,
		ExpYear:               2099,
	})
	require.NoError(t, err)
	require.True(t, found, "duplicate insert should return found=true (idempotent)")

	var count int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_idem_1").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestPgxStore_SoftDeletePaymentMethod_FoundAndIdempotent(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_del")
	seedActivePM(t, pool, accountID, "pm_del_1", 12, 2099)

	found, err := store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.True(t, found)

	// Second call: row is already soft-deleted → found=false (idempotent).
	found, err = store.SoftDeletePaymentMethod(context.Background(), "pm_del_1")
	require.NoError(t, err)
	require.False(t, found, "soft-delete on already-deleted row is a no-op")

	var hasDeleted bool
	err = pool.QueryRow(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		"pm_del_1").Scan(&hasDeleted)
	require.NoError(t, err)
	require.True(t, hasDeleted)
}

func TestPgxStore_SetDefaultPaymentMethod_FlipsOneAndUnflipsRest(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_def")
	seedActivePM(t, pool, accountID, "pm_def_a", 12, 2099)
	seedActivePM(t, pool, accountID, "pm_def_b", 6, 2099)
	seedActivePM(t, pool, accountID, "pm_def_c", 1, 2099)

	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_b"))

	defaults := readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.True(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])

	// Flip default to pm_def_c.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", "pm_def_c"))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"], "previous default should flip back to false")
	require.True(t, defaults["pm_def_c"])

	// Clear default everywhere.
	require.NoError(t, store.SetDefaultPaymentMethod(context.Background(), "cus_def", ""))

	defaults = readDefaultFlags(t, pool, accountID)
	require.False(t, defaults["pm_def_a"])
	require.False(t, defaults["pm_def_b"])
	require.False(t, defaults["pm_def_c"])
}

func TestPgxStore_ResolvePendingAddCardRequest_CompletedThenDuplicate(t *testing.T) {
	// Add-card lifecycle, two passes:
	//   1. Fresh card → resolver flips request to 'completed' and points
	//      payment_method_id at the just-mirrored row.
	//   2. Same card re-attached (new pm_* id, same fingerprint) → resolver
	//      flips the second request to 'duplicate', points payment_method_id
	//      at the EXISTING surviving row, and soft-deletes the just-mirrored
	//      duplicate row so the UI shows one card per real-world fingerprint.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_dup")
	ctx := context.Background()

	// First pass: mirror a fresh card and resolve a pending request.
	firstReqID := seedPendingAddCardRequest(t, pool, accountID, "pm_dup_1")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_dup_1", "fp_abc")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_dup_1"))

	status, pmID, deleted := readAddCardRequest(t, pool, firstReqID)
	require.Equal(t, "completed", status)
	require.NotNil(t, pmID, "completed request should point at the mirrored row")
	require.False(t, deleted, "the first card's mirror row stays active")

	// Second pass: same fingerprint, fresh pm_* id (Stripe's normal
	// re-attach behavior) → duplicate.
	secondReqID := seedPendingAddCardRequest(t, pool, accountID, "pm_dup_2")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_dup_2", "fp_abc")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_dup_2"))

	status, pmID, deleted = readAddCardRequest(t, pool, secondReqID)
	require.Equal(t, "duplicate", status)
	require.NotNil(t, pmID, "duplicate request still resolves to a PM (the surviving one)")

	// The just-mirrored pm_dup_2 row should be soft-deleted.
	require.True(t, isMirrorRowDeleted(t, pool, "pm_dup_2"))
	require.False(t, isMirrorRowDeleted(t, pool, "pm_dup_1"),
		"the pre-existing card should survive unchanged")

	// And ListPaymentMethods (the UI query) sees exactly one row for this account.
	var activeCount int
	err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM ms_billing.payment_methods_mirror
		 WHERE account_id = $1 AND deleted_at IS NULL`, accountID).Scan(&activeCount)
	require.NoError(t, err)
	require.Equal(t, 1, activeCount)
}

func TestPgxStore_ResolvePendingAddCardRequest_NoMirrorRow_IsNoOp(t *testing.T) {
	// Event ordering: setup_intent.succeeded can arrive before
	// payment_method.attached. The resolver should bail out cleanly when
	// the mirror row isn't there yet — the partner handler resolves later.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_noop")
	reqID := seedPendingAddCardRequest(t, pool, accountID, "pm_noop")

	require.NoError(t, store.ResolvePendingAddCardRequest(context.Background(), "pm_noop"))

	status, _, _ := readAddCardRequest(t, pool, reqID)
	require.Equal(t, "pending", status, "request stays pending when mirror row absent")
}

func TestPgxStore_ResolvePendingAddCardRequest_NullFingerprint_NeverDeduplicates(t *testing.T) {
	// Legacy rows pre-migration 005 have fingerprint=NULL. A new card
	// inserted alongside one of those must always resolve to 'completed'
	// — NULL fingerprints don't collide with anything.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_legacy")
	ctx := context.Background()

	// Legacy mirror row, no fingerprint.
	seedActivePM(t, pool, accountID, "pm_legacy", 12, 2099)

	// New attach with a real fingerprint.
	reqID := seedPendingAddCardRequest(t, pool, accountID, "pm_new")
	insertMirrorWithFingerprint(t, pool, accountID, "pm_new", "fp_real")
	require.NoError(t, store.ResolvePendingAddCardRequest(ctx, "pm_new"))

	status, _, _ := readAddCardRequest(t, pool, reqID)
	require.Equal(t, "completed", status)
}

// --- ApplyInvoiceStatus reconciliation ------------------------------------

func TestPgxStore_ApplyInvoiceStatus_ForwardTransitions(t *testing.T) {
	// draft → open → paid: each forward step lands; amount_paid is recorded on
	// paid.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_fwd")
	seedInvoice(t, pool, accountID, "in_fwd", "draft", 0, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_fwd", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "open", readInvoiceStatus(t, pool, "in_fwd"))

	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_fwd", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	status, paid := readInvoiceStatusAndPaid(t, pool, "in_fwd")
	require.Equal(t, "paid", status)
	require.Equal(t, int64(1200), paid)
}

func TestPgxStore_ApplyInvoiceStatus_OutOfOrder_LateFinalizedDoesNotRegressPaid(t *testing.T) {
	// Stripe delivers out of order: invoice.paid lands first, then a late
	// invoice.finalized (open). The monotonic guard must keep the row 'paid'.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_ooo")
	seedInvoice(t, pool, accountID, "in_ooo", "open", 0, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_ooo", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)

	// Late finalized (open) arrives — must be rejected by the guard.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_ooo", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "a late finalized must NOT regress a paid row")

	status, paid := readInvoiceStatusAndPaid(t, pool, "in_ooo")
	require.Equal(t, "paid", status, "row stays paid")
	require.Equal(t, int64(1200), paid, "amount_paid is not zeroed by the stale event")
}

func TestPgxStore_ApplyInvoiceStatus_TerminalNotOverwritten(t *testing.T) {
	// paid is terminal; a void event (also terminal, equal rank but different
	// status) must not overwrite it — terminal-once-set holds.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_term")
	seedInvoice(t, pool, accountID, "in_term", "paid", 1200, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_term", Status: "void", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "void must not overwrite a paid invoice")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_term"))
}

func TestPgxStore_ApplyInvoiceStatus_IdempotentReplay(t *testing.T) {
	// A replayed invoice.paid (same status) is allowed through to refresh the
	// amounts idempotently — the row stays paid with the same values.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_replay")
	seedInvoice(t, pool, accountID, "in_replay", "paid", 1200, 1200)
	ctx := context.Background()

	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_replay", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "identical re-apply is allowed (idempotent amount refresh)")
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_replay"))
}

func TestPgxStore_ApplyInvoiceStatus_PresentmentEnrichment_SetNeverCleared(t *testing.T) {
	// Migration 026 semantics: the presentment fields land when an event
	// carries them (finalized) and are NEVER cleared by a later event whose
	// payload omits them — COALESCE(NULLIF(new,''), old) keeps the stored
	// values while the status still advances.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_pres")
	seedInvoice(t, pool, accountID, "in_pres", "draft", 0, 1200)
	ctx := context.Background()

	// Pre-finalization event: no presentment fields → columns stay NULL.
	found, err := store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "draft", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	number, hostedURL, pdf := readInvoicePresentment(t, pool, "in_pres")
	require.Empty(t, number)
	require.Empty(t, hostedURL)
	require.Empty(t, pdf)

	// Finalized: Stripe assigned the fields → they enrich the mirror.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "open", AmountPaidCents: 0, AmountDueCents: 1200,
		Number:           "813C8918-0001",
		HostedInvoiceURL: "https://invoice.stripe.com/i/in_pres",
		InvoicePDF:       "https://pay.stripe.com/invoice/in_pres/pdf",
	})
	require.NoError(t, err)
	require.True(t, found)
	number, hostedURL, pdf = readInvoicePresentment(t, pool, "in_pres")
	require.Equal(t, "813C8918-0001", number)
	require.Equal(t, "https://invoice.stripe.com/i/in_pres", hostedURL)
	require.Equal(t, "https://pay.stripe.com/invoice/in_pres/pdf", pdf)

	// A later event with EMPTY presentment fields advances the status but
	// must not un-enrich the row.
	found, err = store.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_pres", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "paid", readInvoiceStatus(t, pool, "in_pres"))
	number, hostedURL, pdf = readInvoicePresentment(t, pool, "in_pres")
	require.Equal(t, "813C8918-0001", number, "empty payload value must not clear number")
	require.Equal(t, "https://invoice.stripe.com/i/in_pres", hostedURL, "empty payload value must not clear hosted_invoice_url")
	require.Equal(t, "https://pay.stripe.com/invoice/in_pres/pdf", pdf, "empty payload value must not clear invoice_pdf")
}

func TestPgxStore_ApplyInvoiceStatus_UnknownInvoiceID_NoOp(t *testing.T) {
	// An event for an invoice the charge spine never mirrored → 0 rows,
	// found=false, no crash.
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)

	found, err := store.ApplyInvoiceStatus(context.Background(), webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_does_not_exist", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.False(t, found, "unknown stripe_invoice_id is a safe no-op")
}

// --- HasUnpaidInvoice (delinquency signal) ---------------------------------

func TestPgxStore_HasUnpaidInvoice_DerivesFromInvoiceState(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_delinq")
	ctx := context.Background()

	// No invoices → not delinquent.
	has, err := store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has)

	// A paid invoice → still not delinquent.
	seedInvoice(t, pool, accountID, "in_paid", "paid", 1200, 1200)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "paid invoice is clean")

	// A draft invoice → not delinquent (no collection attempt yet).
	seedInvoice(t, pool, accountID, "in_draft", "draft", 0, 500)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "draft is excluded — not finalized")

	// An open invoice (payment_failed leaves it open) → delinquent.
	seedInvoice(t, pool, accountID, "in_open", "open", 0, 800)
	has, err = store.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "an open invoice surfaces the delinquency signal")
}

func TestPgxStore_HasUnpaidInvoice_PaymentFailedThenPaid_ClearsSignal(t *testing.T) {
	// payment_failed leaves the invoice 'open' (delinquent); a later paid
	// flips it to 'paid' and the signal clears.
	pool := testutil.NewTestDB(t)
	billingStore := billing.NewStore(pool)
	webhookStore := webhook.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_recover")
	seedInvoice(t, pool, accountID, "in_recover", "open", 0, 1200)
	ctx := context.Background()

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "open invoice → delinquent")

	// Subsequent invoice.paid reconciles via the webhook store.
	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_recover", Status: "paid", AmountPaidCents: 1200, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found)

	has, err = billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "paid clears the delinquency signal")
}

func TestPgxStore_ApplyInvoiceStatus_VoidClearsDelinquencySignal(t *testing.T) {
	// An admin voids a finalized invoice (open → void), or Stripe voids one on
	// subscription cancellation. invoice.voided MUST flip the mirror to 'void'
	// so HasUnpaidInvoice stops reporting the account delinquent — the debt was
	// forgiven and there is no other recovery path (the event is ACKed 200).
	pool := testutil.NewTestDB(t)
	webhookStore := webhook.NewStore(pool)
	billingStore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_void")
	seedInvoice(t, pool, accountID, "in_void", "open", 0, 1200)
	ctx := context.Background()

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "open invoice → delinquent")

	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_void", Status: "void", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "void must advance an open invoice (open<terminal)")
	require.Equal(t, "void", readInvoiceStatus(t, pool, "in_void"))

	has, err = billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.False(t, has, "void clears the delinquency signal")
}

func TestPgxStore_ApplyInvoiceStatus_UncollectibleKeepsDelinquencySignal(t *testing.T) {
	// Stripe gives up collecting (open → uncollectible). The mirror must record
	// the precise terminal state, and the delinquency signal stays TRUE because
	// the account still owes the money.
	pool := testutil.NewTestDB(t)
	webhookStore := webhook.NewStore(pool)
	billingStore := billing.NewStore(pool)
	accountID := seedAccount(t, pool, "cus_inv_unc")
	seedInvoice(t, pool, accountID, "in_unc", "open", 0, 1200)
	ctx := context.Background()

	found, err := webhookStore.ApplyInvoiceStatus(ctx, webhook.ApplyInvoiceStatusParams{
		StripeInvoiceID: "in_unc", Status: "uncollectible", AmountPaidCents: 0, AmountDueCents: 1200,
	})
	require.NoError(t, err)
	require.True(t, found, "uncollectible must advance an open invoice (open<terminal)")
	require.Equal(t, "uncollectible", readInvoiceStatus(t, pool, "in_unc"))

	has, err := billingStore.HasUnpaidInvoice(ctx, accountID)
	require.NoError(t, err)
	require.True(t, has, "uncollectible keeps the delinquency signal (account still owes)")
}

// --- helpers --------------------------------------------------------------

func seedInvoice(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, stripeInvoiceID, status string, amountPaid, amountDue int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.invoices
		   (account_id, stripe_invoice_id, status, amount_paid, amount_due, currency)
		 VALUES ($1, $2, $3, $4, $5, 'usd')`,
		accountID, stripeInvoiceID, status, amountPaid, amountDue,
	)
	require.NoError(t, err)
}

func readInvoiceStatus(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) string {
	t.Helper()
	var status string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&status))
	return status
}

// readInvoicePresentment reads the migration-026 presentment columns,
// COALESCEd to "" so NULL and empty assert identically.
func readInvoicePresentment(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) (number, hostedURL, pdf string) {
	t.Helper()
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT COALESCE(number, ''), COALESCE(hosted_invoice_url, ''), COALESCE(invoice_pdf, '')
		 FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&number, &hostedURL, &pdf))
	return number, hostedURL, pdf
}

func readInvoiceStatusAndPaid(t *testing.T, pool *pgxpool.Pool, stripeInvoiceID string) (string, int64) {
	t.Helper()
	var status string
	var paid int64
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status, amount_paid FROM ms_billing.invoices WHERE stripe_invoice_id = $1`,
		stripeInvoiceID).Scan(&status, &paid))
	return status, paid
}

func seedAccount(t *testing.T, pool *pgxpool.Pool, stripeCustomerID string) uuid.UUID {
	t.Helper()
	accountID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.accounts (id, owner_kind, owner_user_id, stripe_customer_id)
		 VALUES ($1, 'user', $2, $3)`,
		accountID, uuid.New(), stripeCustomerID,
	)
	require.NoError(t, err)
	return accountID
}

func seedActivePM(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID string, expMonth, expYear int) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default)
		 VALUES ($1, $2, 'visa', '4242', $3, $4, false)`,
		accountID, pmID, expMonth, expYear,
	)
	require.NoError(t, err)
}

func insertMirrorWithFingerprint(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, pmID, fingerprint string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.payment_methods_mirror
		   (account_id, stripe_payment_method_id, brand, last4, exp_month, exp_year, is_default, fingerprint)
		 VALUES ($1, $2, 'visa', '4242', 12, 2099, false, $3)`,
		accountID, pmID, fingerprint,
	)
	require.NoError(t, err)
}

func seedPendingAddCardRequest(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID, stripePMID string) uuid.UUID {
	t.Helper()
	reqID := uuid.New()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO ms_billing.add_card_requests (id, account_id, stripe_pm_id, status)
		 VALUES ($1, $2, $3, 'pending')`,
		reqID, accountID, stripePMID,
	)
	require.NoError(t, err)
	return reqID
}

func readAddCardRequest(t *testing.T, pool *pgxpool.Pool, reqID uuid.UUID) (status string, paymentMethodID *uuid.UUID, mirrorDeleted bool) {
	t.Helper()
	var pmID *uuid.UUID
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT status::text, payment_method_id FROM ms_billing.add_card_requests WHERE id = $1`,
		reqID).Scan(&status, &pmID))
	if pmID != nil {
		var del bool
		require.NoError(t, pool.QueryRow(context.Background(),
			`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE id = $1`,
			*pmID).Scan(&del))
		mirrorDeleted = del
	}
	return status, pmID, mirrorDeleted
}

func isMirrorRowDeleted(t *testing.T, pool *pgxpool.Pool, stripePMID string) bool {
	t.Helper()
	var deleted bool
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT deleted_at IS NOT NULL FROM ms_billing.payment_methods_mirror WHERE stripe_payment_method_id = $1`,
		stripePMID).Scan(&deleted))
	return deleted
}

func readDefaultFlags(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) map[string]bool {
	t.Helper()
	rows, err := pool.Query(context.Background(),
		`SELECT stripe_payment_method_id, is_default FROM ms_billing.payment_methods_mirror
		 WHERE account_id = $1 AND deleted_at IS NULL`,
		accountID,
	)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var pmID string
		var def bool
		require.NoError(t, rows.Scan(&pmID, &def))
		out[pmID] = def
	}
	require.NoError(t, rows.Err())
	return out
}

// TestPgxStore_RelaxCollectionOnPaidInvoice verifies the risk-graded RELAX
// driver (PR #9): a paid invoice re-trusts a PREPAID account back to arrears, but
// ONLY when no open/uncollectible invoice remains. It never touches an arrears
// account and is a no-op while delinquency persists.
func TestPgxStore_RelaxCollectionOnPaidInvoice(t *testing.T) {
	pool := testutil.NewTestDB(t)
	store := webhook.NewStore(pool)
	ctx := context.Background()

	accountID := seedAccount(t, pool, "cus_relax")
	// Tighten the account to prepaid (the state a relax must reverse).
	_, err := pool.Exec(ctx,
		`UPDATE ms_billing.accounts SET usage_billing_mode = 'prepaid' WHERE id = $1`, accountID.String())
	require.NoError(t, err)

	// The just-paid invoice + a SECOND still-open invoice: relax must NOT fire
	// while another invoice is unpaid.
	seedInvoice(t, pool, accountID, "in_relax_paid", "paid", 1200, 1200)
	seedInvoice(t, pool, accountID, "in_relax_open", "open", 0, 800)

	relaxed, err := store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.False(t, relaxed, "still delinquent (another open invoice) → no relax")
	require.Equal(t, "prepaid", readMode(t, pool, accountID))

	// Pay off the remaining invoice; now the relax fires.
	_, err = pool.Exec(ctx,
		`UPDATE ms_billing.invoices SET status = 'paid' WHERE stripe_invoice_id = $1`, "in_relax_open")
	require.NoError(t, err)

	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.True(t, relaxed, "no remaining delinquency → relax prepaid → arrears")
	require.Equal(t, "arrears", readMode(t, pool, accountID))

	// Idempotent: a second relax on an already-arrears account is a no-op.
	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_relax_paid")
	require.NoError(t, err)
	require.False(t, relaxed, "already arrears → no-op")

	// Unknown invoice id → safe no-op.
	relaxed, err = store.RelaxCollectionOnPaidInvoice(ctx, "in_does_not_exist")
	require.NoError(t, err)
	require.False(t, relaxed)
}

func readMode(t *testing.T, pool *pgxpool.Pool, accountID uuid.UUID) string {
	t.Helper()
	var mode string
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT usage_billing_mode::text FROM ms_billing.accounts WHERE id = $1`,
		accountID.String()).Scan(&mode))
	return mode
}
