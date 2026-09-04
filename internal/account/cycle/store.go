package cycle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
	"github.com/mirrorstack-ai/billing-engine/internal/meteringlock"
)

// ErrInactiveModelPrice is returned by MetricPriceMicros when a usage row's
// (metric, model) has a per-model price ROW that has been retired (active =
// false). It is deliberately NOT pgx.ErrNoRows: a missing row legitimately falls
// back to the catalog price, but a retired model price must fail the cycle loud
// rather than silently bill at the cheaper catalog (Haiku-floor) fallback — that
// would under-bill a deliberately-retired model and defeat the rollup's loud
// revenue-leak guard. The Service maps this to a loud Internal error.
var (
	ErrInactiveModelPrice = errors.New("per-model price is retired (active=false)")

	// ErrTaskGPUModelPrice means task GPU usage cannot resolve a positive active
	// exact price. It never falls back to another pricing source.
	ErrTaskGPUModelPrice = errors.New("task GPU model is not admitted or has no positive active exact price")

	// ErrCreditRailRequired means a fresh Stripe claim lost the account-lock
	// race to a standard→credits transition. No Stripe marker or network call
	// exists; the service must retry the charge through the wallet rail.
	ErrCreditRailRequired = errors.New("durable credits rail now owns the fresh charge")
)

// StripeRailClaimOutcome is shared by every pre-network durable Stripe claim.
// Migration 050 introduces it with combined creation-proration ownership; the
// other money legs adopt the same account-lock result in the following durable
// rail hardening.
type StripeRailClaimOutcome uint8

const (
	StripeRailClaimed StripeRailClaimOutcome = iota
	StripeRailWalletRequired
	StripeRailNoPaymentMethod
	StripeRailStale
)

// StripeChargeClaim is the exact funding authority persisted by an atomic
// pre-Stripe arm. Recovery uses FundingAccountID rather than following a later
// org designation.
type StripeChargeClaim struct {
	FundingAccountID  uuid.UUID
	FundingGeneration uuid.UUID
	StripeCustomerID  string
	Outcome           StripeRailClaimOutcome
}

func stripeChargeClaim(fundingAccountID, generation, customerID string) (StripeChargeClaim, error) {
	funder, err := uuid.Parse(fundingAccountID)
	if err != nil {
		return StripeChargeClaim{}, fmt.Errorf("parse charge funding account: %w", err)
	}
	gen, err := uuid.Parse(generation)
	if err != nil {
		return StripeChargeClaim{}, fmt.Errorf("parse charge funding generation: %w", err)
	}
	return StripeChargeClaim{
		FundingAccountID: funder, FundingGeneration: gen, StripeCustomerID: customerID,
	}, nil
}

// Store is the persistence interface the rollup + settlement Service depends
// on. Narrow on purpose — every method maps to a specific rollup step — so
// tests satisfy it with a small in-memory fake (see service_test.go).
type Store interface {
	// FinalizeOrgDeletionBilling atomically retires every future-billing
	// surface for one organization while retaining its financial history. The
	// operation id is the immutable idempotency identity: the same operation
	// replays successfully and a different operation fails closed. The store
	// performs the final collectible-invoice and in-flight-money rechecks while
	// holding the shared org lifecycle lock.
	FinalizeOrgDeletionBilling(ctx context.Context, orgID, operationID uuid.UUID, finalizedAt time.Time) (OrgDeletionFinalizationOutcome, error)

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
	// aggregate. Resolution order (usage-time-pricing Phase 1, migration 044):
	//  1. TASK-GPU FIRST: infra.task.gpu.hours requires an admitted model and
	//     an active exact metric_model_prices row. It never uses a version,
	//     per-module, or catalog fallback.
	//  2. VERSION-FIRST: when moduleVersion != "", try the IMMUTABLE
	//     per-(module, metric, module_version) snapshot (metric_version_prices).
	//     A hit wins outright — this is what stops a later version's re-price
	//     from retroactively re-billing an earlier version's already-accrued
	//     usage (the snapshot, once written, can never be overwritten). A miss
	//     (no snapshot for this version) falls through to step 3.
	//  3. When model != "" it resolves the AUTHORITATIVE per-(metric, model)
	//     price from metric_model_prices (migration 018), falling back to the
	//     catalog row when no per-model price exists.
	//  4. model == "" (and no version snapshot) resolves the (module, metric)
	//     catalog row directly.
	// priced=false (NULL/absent price) → the metric is metered-but-unpriced and
	// prices to 0. A per-model row that EXISTS but is RETIRED (active=false)
	// returns ErrInactiveModelPrice instead of silently falling back to the
	// cheaper catalog floor — the Service fails the cycle loud rather than
	// under-bill a deliberately-retired model.
	MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model, moduleVersion string) (micros int64, priced bool, err error)

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
	InsertBillingRun(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (runID uuid.UUID, shouldCharge, reclaimed bool, err error)

	// AccountsWithUsageEvents returns the accounts with raw usage_events in the
	// window [periodStart, periodEnd) — the rollup-phase work list for
	// cmd/billing-cycle (phase 1: roll each up into usage_aggregates before the
	// charge phase reads them).
	AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// UnactivatedAccountsWithUsage returns card-less accounts with raw events in
	// the window for rollup only. The driver must never hand this list to the
	// charge phase.
	UnactivatedAccountsWithUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error)

	// PeriodChargedTotal returns Σ usage_aggregates.charged_micros for the
	// account's period window — the arrears input before allowance-netting.
	PeriodChargedTotal(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (int64, error)

	// WalletCreditState is the cheap pre-boundary probe used to preserve the
	// legacy collection fast path for standard accounts with no wallet balance.
	// PeriodDrawnMicros makes a reclaimed run re-enter the wallet path even when
	// its first debit exhausted the available lots.
	WalletCreditState(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (WalletCreditState, error)

	// DrawWalletCredits atomically applies this boundary's wallet debit after
	// the true boundary total has been priced, but before collection/PM gates.
	// A debit is idempotent for (account, period): a reclaimed billing run
	// returns the already-recorded draw instead of consuming newly-added credit.
	// Standard accounts consume at most their positive spendable lots; credits
	// accounts debit the full amount and may therefore end with a negative
	// balance. allowNew=false is the crash-recovery posture for a run whose
	// Stripe request was already frozen: existing draw rows are returned, but a
	// new wallet debit must not be introduced beside money that may have moved.
	DrawWalletCredits(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time, amountMicros int64, allowNew bool) (WalletDrawdown, error)

	// DrawBillingRunWalletCredits is the crash-safe boundary variant. In the
	// SAME transaction as any first wallet debit, it freezes on billing_runs
	// the exact Stripe remainder (possibly zero) and description determinant.
	// The marker is deliberately stored on the legacy run row: a later reclaim
	// can finish safely while true wallet master-off executes zero SQL naming
	// accounts.billing_mode or credit_ledger. Existing draws are returned
	// idempotently and acquire the marker when boundaryTotalMicros is supplied.
	DrawBillingRunWalletCredits(
		ctx context.Context,
		runID, accountID uuid.UUID,
		periodStart, periodEnd time.Time,
		boundaryTotalMicros int64,
		withBase, allowNew bool,
	) (WalletDrawdown, error)

	// HasUsableDefaultPM is the no-PM charge gate: true iff the account has an
	// active, not-expired payment method. Mirrors the billing hot-path gate.
	HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error)

	// AccountStripeCustomer returns the account's Stripe Customer id (empty when
	// none exists yet). The charge never auto-creates a Customer — an empty id
	// at the charge leg is an anomaly the caller surfaces.
	AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error)

	// UsableNonFraudCardCount is THE standing card predicate — the SAME
	// usable_card_count the service-block gate reads (billing.sql
	// ServiceBlockSignals: active, NOT fraud_blocked, not expired), reused
	// verbatim so RegisterApp's create gate (funding-gates design) and
	// GetServiceStatus can never disagree on card quality.
	UsableNonFraudCardCount(ctx context.Context, accountID uuid.UUID) (int, error)

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

	// MarkBillingRunInvoicedIfUnfrozen terminally marks a ZERO-total run
	// 'invoiced' (no Stripe call happened) — guarded on the run still being
	// UNFROZEN (wave 2, D7): a concurrent reclaim may have frozen + charged
	// after this process's top-of-run frozen read, and an unguarded terminal
	// mark would bury that charge forever. ok=false → the guard lost; the
	// caller must back off and leave the run reclaimable.
	MarkBillingRunInvoicedIfUnfrozen(ctx context.Context, runID uuid.UUID) (bool, error)

	// FreezeBillingRunCharge records — BEFORE the boundary run's first Stripe
	// charge — the exact amount + base/overage description determinant it will send
	// under the deterministic idem keys ii-<run>/inv-<run> (migration 035).
	// First-write-wins, and it returns the SURVIVING row value: when a concurrent
	// second daemon reclaimed the same run and froze first, the loser's write
	// no-ops and it MUST adopt the returned winner value — charging a locally
	// computed amount under the shared idem keys would send Stripe two different
	// bodies for the same key (the H6 race). The retry path likewise never sends
	// a request that differs from what a prior attempt froze.
	FreezeBillingRunCharge(ctx context.Context, runID uuid.UUID, charge FrozenBoundaryCharge) (FrozenBoundaryCharge, StripeRailClaimOutcome, error)

	// BillingRunFrozenCharge reads a run's frozen boundary charge; ok=false when no
	// prior attempt reached the Stripe call (a fresh run). On a reclaim it is the
	// amount already charged, which the retry REUSES verbatim.
	BillingRunFrozenCharge(ctx context.Context, runID uuid.UUID) (charge FrozenBoundaryCharge, ok bool, err error)

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

	// AccountActivation returns the account's activated_at anchor (migration
	// 025) and whether it is set. activated=false → the account never bound a
	// card and is NEVER charged (D1d — the same posture as the spine's
	// unactivated skip); RegisterApp then records the mirror row without a
	// proration invoice.
	AccountActivation(ctx context.Context, accountID uuid.UUID) (activatedAt time.Time, activated bool, err error)

	// InsertAppMirror registers a ms_billing.apps roster row idempotently
	// (ON CONFLICT (app_id) DO NOTHING — a retry never rewrites the original
	// created_at / module_count / name, which anchor the proration + freeze the
	// display name). name "" writes NULL. accountID uuid.Nil registers an
	// UNBILLED org roster row (NULL account, migration 041); ownerOrgID is the
	// org principal for org-owned apps (uuid.Nil = user-owned).
	InsertAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount int, createdAt time.Time, name string) error

	// EnsureOrgAccount resolves the org's billing account, creating the row if
	// none exists yet — the org twin of EnsureAccountForUser (advisory-locked
	// get-or-create, namespace 'lbto'). No Stripe Customer is created here.
	EnsureOrgAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, error)

	// EnsureUserAccount is the user twin of EnsureOrgAccount: resolve the
	// user's billing account, creating the row if none exists. Used ONLY by
	// TransferApp, which may not refuse an unfunded destination — see
	// Service.transferTargetAccount for why that is deliberately not
	// fundedOwnerAccount.
	EnsureUserAccount(ctx context.Context, userID uuid.UUID) (uuid.UUID, error)

	// TransferApp re-points one app's billing account in a single transaction.
	// The outcome discriminates the refusals so the store never builds a wire
	// error (same shape as FinalizeOrgDeletionBilling).
	TransferApp(ctx context.Context, p TransferAppParams) (*TransferAppResponse, TransferOutcome, error)

	// AccountIDByUser resolves a user's EXISTING billing account (Nil, false
	// when none). The sponsor-designation lookup: a sponsor must already have
	// an account with a usable default PM — designation never creates one.
	AccountIDByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error)

	// OrgAccountID resolves the org's EXISTING account row (Nil, false when
	// none), regardless of designation or activation — the read behind
	// GetOrgDesignation's account echo.
	OrgAccountID(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error)

	// OrgDesignation reads the org's funding designation row. found=false →
	// the org never designated (unbilled).
	OrgDesignation(ctx context.Context, orgID uuid.UUID) (OrgDesignation, bool, error)

	// UpsertOrgDesignation writes the org's funding choice; a re-designation
	// overwrites in place (funding switches change only which instrument
	// future invoice finalization charges — attribution never moves, D1).
	UpsertOrgDesignation(ctx context.Context, d OrgDesignation) error

	// DeleteOrgDesignation is the sponsor self-revoke: the org drops back to
	// unbilled until re-designation. deleted=false when no row existed
	// (idempotent revoke).
	DeleteOrgDesignation(ctx context.Context, orgID uuid.UUID) (bool, error)

	// ResolveOrgFundedAccount is THE org account resolution (ingest, reads,
	// RegisterApp): the org's own account, gated on a designation row existing
	// AND the account being activated — "the pointer never flips to an
	// unfunded account" (D1). found=false → the org is unbilled.
	ResolveOrgFundedAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error)

	// OrgsWithUnsweptUsage returns funded, activated orgs whose roster or usage
	// still lacks its account id, making the fire-and-forget attach RPC converge.
	OrgsWithUnsweptUsage(ctx context.Context) ([]uuid.UUID, error)

	// ActivateAccountIfUnset stamps the ADR-0006 activation anchor when the
	// org account activates by SPONSOR designation (anchor = designation day;
	// the card-bind webhook stamps the funding='org' case). Idempotent — the
	// anchor is immutable once set.
	ActivateAccountIfUnset(ctx context.Context, accountID uuid.UUID, at time.Time) error

	// OrgUnbilledBacklogMicros estimates the org's pre-designation unbilled
	// backlog (NULL-account events scoped through the roster's owner_org_id),
	// priced like the live bill display — the DISCLOSURE figure shown before
	// the designating user confirms.
	OrgUnbilledBacklogMicros(ctx context.Context, orgID uuid.UUID) (int64, error)

	// OrgIsDistributor reports whether orgID distributes at least one customer
	// org (migration 053). Derived from the links, never a stored flag.
	OrgIsDistributor(ctx context.Context, orgID uuid.UUID) (bool, error)
	// OrgDistributor returns the org that distributes orgID and the link's
	// provenance ('registration' | 'manual'). found=false when unlinked.
	OrgDistributor(ctx context.Context, orgID uuid.UUID) (distributorOrgID uuid.UUID, source string, found bool, err error)

	// AttachOrgAppsToAccount backfills account_id onto the org's unbilled
	// roster rows (the roster half of the RepointOrgUsage sweep); returns the
	// attached-row count.
	AttachOrgAppsToAccount(ctx context.Context, orgID, accountID uuid.UUID) (int64, error)

	// RepointOrgNullAccountEvents folds the org's NULL-account events into its
	// funded account, clamping recorded_at up to windowStart (the account's
	// current open window) so every backfilled event bills in the first period
	// that closes after designation — original instants audit to
	// repointed_from (migration 041). Returns the swept-event count.
	RepointOrgNullAccountEvents(ctx context.Context, orgID, accountID uuid.UUID, windowStart time.Time) (int64, error)

	// OrgLiveAppIDs lists the org's live roster rows — the attach sweep
	// reconciles each one's timers after account_id backfills.
	OrgLiveAppIDs(ctx context.Context, orgID uuid.UUID) ([]uuid.UUID, error)

	// ListSponsoredOrgIDs lists the orgs a user sponsors (funding='sponsor',
	// activated account) — the roster behind the /me sponsored-orgs read.
	ListSponsoredOrgIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)

	// ChargeFundingAccount maps an account to the account whose Stripe
	// customer / default PM pays its invoices: itself, unless it is an org
	// account whose designation names a sponsor (D1 funding hop). Resolved at
	// charge time by resolveChargeableCustomer.
	ChargeFundingAccount(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error)

	// SetAppName updates the frozen display name (SyncAppModules rename);
	// no-op once the app is deleted (WHERE deleted_at IS NULL), freezing the
	// last-known name for the bill.
	SetAppName(ctx context.Context, appID uuid.UUID, name string) error

	// AppMirror reads one roster row (deleted rows included — the caller owns
	// deletion semantics). found=false → the app was never registered.
	AppMirror(ctx context.Context, appID uuid.UUID) (AppMirror, bool, error)

	// AppsPendingProration returns the app ids past the creation grace window
	// (created_at <= createdBefore = now − GraceDays) that are still LIVE
	// (deleted_at IS NULL), NOT yet charged (proration_invoice_id IS NULL), and
	// NOT permanently skipped (proration_skipped_at IS NULL, migration 031) —
	// the creation-proration sweep's work list. An app deleted within grace,
	// already charged, or already determined to be a would-be retroactive
	// catch-up (D1d) is excluded.
	AppsPendingProration(ctx context.Context, createdBefore time.Time) ([]uuid.UUID, error)

	// ChargeProrationLocked runs the creation-proration charge for ONE app. It
	// briefly SELECT ... FOR UPDATE-locks the roster row to re-verify the row is
	// still chargeable (deleted_at IS NULL AND proration_invoice_id IS NULL) and
	// read its frozen state, then RELEASES the lock before invoking charge —
	// which performs the (potentially slow) Stripe network calls OUTSIDE any
	// lock or transaction — and finally persists the mirrored invoice, the base
	// snapshot, and the one-shot guard in a second short transaction. The lock is
	// deliberately NOT held across the Stripe call (a prior version did; it could
	// block a concurrent SyncAppModules/MarkAppDeleted write for the Stripe SDK's
	// full ~80s-per-call timeout): a soft-delete that commits while the charge
	// callback is in flight does NOT unwind an already-succeeded Stripe charge
	// (D1e already forbids refunds — the money moved), so the persist step
	// writes the invoice/snapshot/guard unconditionally on success. A second,
	// genuinely concurrent charge attempt for the SAME app converges on the SAME
	// Stripe objects (the deterministic per-app Idempotency-Keys) and the guard's
	// first-write-wins UPDATE, so this stays race-safe without a lock spanning
	// both phases. charge returning (nil, nil) means "nothing to charge" (0
	// cents) → nothing is persisted. The returned invoice id is the armed (or
	// pre-armed) guard's.
	ChargeProrationLocked(ctx context.Context, appID uuid.UUID, charge func(locked AppMirror) (*ProrationCharge, error)) (ProrationOutcome, string, error)
	// MarkCombinedProrationProposed is the terminal stamp for a proposed
	// attempt, so the app stops being re-swept.
	//
	// 🔴 Its absence is not a missing convenience. AppsPendingProration selects
	// on proration_invoice_id AND proration_skipped_at both being NULL, so an
	// attempt that seals an intent and stamps nothing is re-derived on every
	// sweep — a new intent per sweep for one charge. The domain and overage
	// legs each stamp; this is the same act.
	MarkCombinedProrationProposed(ctx context.Context, appID uuid.UUID, at time.Time, intentReference string) error

	// DrawCreationProrationFromWallet settles ONE app's creation proration through
	// the universal credit wallet (migration 048, billing-engine #99) ATOMICALLY:
	// under the app row lock it re-verifies the row is still chargeable (deleted_at
	// IS NULL AND proration_invoice_id IS NULL), draws charge.AmountMicros from the
	// append-only credit ledger (per-app idempotency keys, credits-mode unsecured
	// remainder), and — ONLY when the wallet FULLY covers the amount — freezes the
	// base snapshot(s) and arms the one-shot guard (with charge.Ref), all in a
	// SINGLE transaction. Because the draw and the guard-arm commit together, a
	// crash can never strand a partial settlement: either the whole thing committed
	// (guard armed, never re-swept) or nothing did (the guard remains unarmed and a
	// later sweep retries). The account lock re-reads the durable billing mode: if
	// a concurrent mode change made the account non-credits after the caller's
	// unlocked classification, this returns ProrationWalletDeferToStripe BEFORE
	// reading or writing the ledger. Credits mode fully covers via its unsecured
	// remainder. Unlike the boundary draw this debit carries NO period_id — it is
	// keyed per app, so it never collides with the period's boundary draw.
	DrawCreationProrationFromWallet(ctx context.Context, appID uuid.UUID, charge ProrationWalletCharge) (ProrationOutcome, string, error)

	// SetAppProrationInvoice arms the ONE-SHOT creation-proration guard: it
	// records the Stripe invoice id or wallet settlement reference,
	// first-charge-wins (UPDATE … WHERE
	// proration_invoice_id IS NULL). An already-armed guard is NOT an error —
	// the write is a no-op and the original invoice id survives.
	SetAppProrationInvoice(ctx context.Context, appID uuid.UUID, stripeInvoiceID string) error

	// SetAppProrationSkipped arms the PERMANENT creation-proration skip marker
	// (migration 031, D1d): the account only activated at/after this app's
	// anchored creation period had already closed, so the app is EXCLUDED from
	// every future sweep rather than left pending forever (proration_invoice_id
	// stays NULL, so without this marker AppsPendingProration would resurface it
	// on every sweep indefinitely). First-write-wins and a no-op if the app was
	// somehow already charged in the meantime — never an error.
	SetAppProrationSkipped(ctx context.Context, appID uuid.UUID) error

	// SetAppModuleCount snapshots a new installed-module count. A deleted
	// app's count is frozen (the UPDATE's WHERE deleted_at IS NULL no-ops —
	// D1e: no future base, so no tier to move).
	SetAppModuleCount(ctx context.Context, appID uuid.UUID, moduleCount int) error

	// MarkAppDeleted soft-deletes the roster row out of future advance base
	// fees. Idempotent — the first deletion instant is kept.
	MarkAppDeleted(ctx context.Context, appID uuid.UUID) error

	// LiveAppsCreatedBefore returns every LIVE (deleted_at IS NULL) app on the
	// account that has JOINED the advance-base mechanism by createdBefore (the
	// NEW period's start, i.e. the closed window's period_end), with its
	// module_count — the boundary charge's advance-base input. An app is
	// excluded when created inside the new period (its creation-proration leg
	// owns that period's base) OR when its creation grace (graceDays) had not
	// yet elapsed by createdBefore (it hasn't survived grace — deleted-in-grace
	// is never charged — and when it survives, its creation charge covers
	// through the END of the period its grace elapses into). It joins the
	// advance leg at the NEXT boundary. Empty for a pre-backfill account →
	// advance base 0 (pre-027 behavior).
	LiveAppsCreatedBefore(ctx context.Context, accountID uuid.UUID, createdBefore time.Time, graceDays int) ([]AppModuleCount, error)

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

	// --- per-module-instance overage timers (migration 033) -----------------

	// LiveModuleTimerCountForApp returns the count of an app's currently-live
	// (removed_at IS NULL) install timers — the reconciliation input RegisterApp
	// / SyncAppModules use to bring the live-timer set into line with the app's
	// module_count idempotently across fire-and-forget retries.
	LiveModuleTimerCountForApp(ctx context.Context, appID uuid.UUID) (int, error)

	// InsertModuleOverageTimers inserts n identical install timers for one app,
	// all anchored at installedAt with grace expiring at graceExpiresAt (=
	// installedAt + the 3-day grace window). n <= 0 is a no-op.
	InsertModuleOverageTimers(ctx context.Context, accountID, appID uuid.UUID, installedAt, graceExpiresAt time.Time, n int) error

	// SoftRemoveNewestModuleTimers LIFO-soft-removes the n NEWEST currently-live
	// install timers for one app (a SyncAppModules shrink removes what was added
	// most recently). n <= 0 is a no-op.
	SoftRemoveNewestModuleTimers(ctx context.Context, appID uuid.UUID, n int, removedAt time.Time) error

	// SoftRemoveAllModuleTimersForApp soft-removes every still-live install timer
	// for an app — the app-deletion path. Idempotent (WHERE removed_at IS NULL).
	SoftRemoveAllModuleTimersForApp(ctx context.Context, appID uuid.UUID, removedAt time.Time) error

	// ArmModuleTimerStripeCharge atomically stamps the migration-036 recovery
	// marker and exact rotating funding authority before the first Stripe call.
	// The claim also serializes against wallet settlement and stale candidates.
	ArmModuleTimerStripeCharge(ctx context.Context, timerID uuid.UUID, at time.Time) (StripeChargeClaim, error)

	// ModuleTimerStillPending re-verifies, immediately before acting on a sweep
	// candidate, that the timer is STILL live and unresolved — the work list is
	// read once and can be minutes stale by the time a late candidate is
	// processed (M2).
	ModuleTimerStillPending(ctx context.Context, timerID uuid.UUID) (bool, error)

	// MarkAppProrationAttempted stamps the migration-036 recovery marker for the
	// creation-proration leg — first-write-wins, never cleared.
	MarkAppProrationAttempted(ctx context.Context, appID uuid.UUID, at time.Time) error

	// FreezeCombinedProrationAttempt atomically freezes the exact combined
	// app-base + co-created-timer Stripe request before its first network call.
	// The store locks the app, returns an existing first-write winner verbatim,
	// or selects + row-locks + rechecks the live FIFO-over timers before inserting
	// one header (including an intentionally empty set), its child timer IDs, and
	// apps.proration_attempted_at in one transaction. A legacy attempted app with
	// no header returns ErrCombinedProrationAttemptUnknown. When
	// creditRailEnabled is true, the transaction locks the owning account before
	// the app/timers: a fresh credits-mode charge returns
	// StripeRailWalletRequired with no header/marker, while an existing header
	// remains recovery-authoritative regardless of the current mode.
	FreezeCombinedProrationAttempt(
		ctx context.Context,
		appID uuid.UUID,
		at time.Time,
		shape CombinedProrationChargeShape,
		creditRailEnabled bool,
	) (CombinedProrationAttempt, StripeRailClaimOutcome, error)

	// CombinedProrationAttempt reads one immutable attempt and its complete
	// frozen timer set. found=false means no header; callers must interpret that
	// as fresh only when apps.proration_attempted_at is also absent.
	CombinedProrationAttempt(ctx context.Context, appID uuid.UUID) (attempt CombinedProrationAttempt, found bool, err error)

	// UnresolvedCombinedProrationAttempts returns exact raw frozen money for the
	// strict bill/runtime projection. It fails closed if a header's declared
	// timer_count differs from its durable child rows.
	UnresolvedCombinedProrationAttempts(ctx context.Context, accountID uuid.UUID) ([]UnresolvedCombinedProrationAmount, error)

	// TimerHasUnresolvedCombinedProrationOwner is the durable standalone-rail
	// exclusion: a frozen timer belongs to the combined attempt until the same
	// terminal transaction resolves the header, app, and timer guards.
	TimerHasUnresolvedCombinedProrationOwner(ctx context.Context, timerID uuid.UUID) (bool, error)

	// ReconcileModuleTimersToTarget brings an app's live install-timer set into
	// line with its CURRENT roster row, ATOMICALLY under a per-app advisory
	// transaction lock (review 2026-07-06, H7; hardened in wave 2, D8/D9): the
	// target count, owning account, and deleted state are all read from the
	// apps row INSIDE the locked transaction — never caller-supplied — so a
	// late fire-and-forget retry can neither shrink timers to a stale
	// module_count (D8) nor resurrect timers for an app deleted after its
	// mirror read (D9: a deleted row reconciles to zero, removing any live
	// orphans instead of inserting). A grow inserts the deficit anchored at
	// installedAt/graceExpiresAt, a shrink LIFO-soft-removes the surplus at
	// removedAt. The lock also serializes concurrent executions so two retries
	// can never both insert the full deficit (phantom timers).
	ReconcileModuleTimersToTarget(ctx context.Context, appID uuid.UUID, installedAt, graceExpiresAt, removedAt time.Time) error

	// MarkAppDeletedAndRemoveTimers soft-deletes the roster row AND soft-removes
	// every still-live install timer in ONE transaction under the SAME per-app
	// advisory lock the reconcile takes (wave 2, D9) — a crash can no longer
	// separate the two writes, and a concurrent synthesis retry serializes
	// behind (or ahead of, then gets corrected by) the deletion. Idempotent:
	// re-fire keeps the first deletion instant and affects already-removed
	// timers zero times.
	MarkAppDeletedAndRemoveTimers(ctx context.Context, appID uuid.UUID, removedAt time.Time) error

	// ModuleOverageTimersPastGrace is Leg 1's work list: live, unresolved install
	// timers whose grace window has elapsed as of `at`, on chargeable (activated)
	// accounts — each with the account's activation anchor so the sweep resolves
	// the install's period window without a second read.
	ModuleOverageTimersPastGrace(ctx context.Context, at time.Time) ([]ModuleOverageCandidate, error)

	// LiveModuleTimerRankBefore returns the 0-based FIFO rank of one install timer
	// among the account's currently-live timers ordered (installed_at ASC, id
	// ASC): the count of live timers ordering STRICTLY BEFORE it. rank <
	// IncludedModules ⇒ "included"; rank >= IncludedModules ⇒ "over". Computed
	// fresh at every grace-check (never cached).
	LiveModuleTimerRankBefore(ctx context.Context, accountID, timerID uuid.UUID, installedAt time.Time) (int, error)

	// MarkModuleTimerIncluded stamps the TERMINAL "included" verdict
	// (grace_resolved=true, no charge) — first-write-wins (WHERE grace_resolved
	// IS false). Monotonicity makes it permanent; the row is never re-checked.
	MarkModuleTimerIncluded(ctx context.Context, timerID uuid.UUID) error

	// MarkModuleTimerCharged stamps the TERMINAL "over and charged" verdict once
	// Leg 1's Stripe charge succeeded: grace_charged_at + grace_resolved=true and
	// the GENUINE Stripe invoice / invoice-item ids (never idempotency-key
	// strings). WHERE grace_resolved IS false keeps a crash-retry idempotent.
	MarkModuleTimerCharged(ctx context.Context, timerID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error

	// DrawModuleOverageFromWallet settles ONE per-module install timer's overage
	// through the universal credit wallet (billing-engine Job 3, mirrors
	// DrawCreationProrationFromWallet #99) ATOMICALLY: under the timer row lock it
	// re-verifies the row is still chargeable (removed_at IS NULL AND grace_resolved
	// = false) and that no concurrent Stripe attempt is in flight (charge_attempted_at
	// IS NULL), draws charge.AmountMicros from the append-only credit ledger (per-timer
	// idempotency key, credits-mode unsecured remainder), and — ONLY when the wallet
	// FULLY covers the amount — arms the SAME per-timer guard the Stripe leg arms
	// (grace_charged_at + grace_invoice_id = charge.Ref, grace_invoice_item_id NULL),
	// all in a SINGLE transaction. Because the draw and the guard-arm commit together,
	// a committed settlement short-circuits every retry at the grace_resolved re-check,
	// and a crash before commit rolls back leaving no ledger rows. The account lock
	// re-reads the durable billing mode: if a concurrent mode change
	// made the account non-credits after the caller's unlocked classification, this
	// returns ModuleOverageWalletDeferToStripe BEFORE reading or writing the ledger.
	// Credits mode fully covers via its unsecured remainder. Like the creation draw
	// this debit carries NO period_id — it is keyed per timer, so it never collides
	// with the period's boundary draw.
	DrawModuleOverageFromWallet(ctx context.Context, timerID uuid.UUID, charge ModuleOverageWalletCharge) (ModuleOverageWalletOutcome, string, error)

	// CountOngoingOverModuleTimers is Leg 2's boundary-precharge input (scenario
	// 6): the count of the account's live timers that are "over" (live-FIFO rank
	// >= includedModules) AND owed a full precharge for the NEW period opening at
	// periodEnd — installed before it, grace elapsed before it (a straddling
	// grace's new period is Leg 1's coverage), and grace terminally resolved
	// (charged, OR resolved-uncharged via the D1d period-closed posture — those
	// still owe every post-activation period). See the query comment for the full
	// coverage contract.
	CountOngoingOverModuleTimers(ctx context.Context, accountID uuid.UUID, includedModules int, periodEnd time.Time) (int, error)

	// CoCreatedOverModuleTimers backs the scenario-3 combined creation invoice: the
	// ids of an app's live, unresolved install timers whose install instant equals
	// the app's createdAt (co-created at app creation) AND that are "over" (live-FIFO
	// rank >= includedModules) — the co-created over-modules folded onto the app's
	// own creation-proration invoice, priced from the same day-0 window.
	CoCreatedOverModuleTimers(ctx context.Context, accountID, appID uuid.UUID, createdAt time.Time, includedModules int) ([]uuid.UUID, error)

	// --- custom-domain charges (migration 047) -------------------------------

	// InsertDomain records one custom-domain activation idempotently on the
	// partial live-hostname key. A retry never rewrites the winning row's stable
	// account/app/activation identity.
	InsertDomain(ctx context.Context, accountID, appID uuid.UUID, hostname string, activatedAt time.Time) error

	// DomainByHostname returns the currently-live activation when one exists,
	// otherwise the newest historical activation. found=false means the hostname
	// has never been mirrored.
	DomainByHostname(ctx context.Context, hostname string) (Domain, bool, error)

	// RemoveDomain soft-removes a live (app, hostname) activation. Idempotent:
	// the first removal instant is kept, and an already-removed row is untouched.
	RemoveDomain(ctx context.Context, appID uuid.UUID, hostname string, removedAt time.Time) error

	// DomainsPendingCharge is the activation-period work list: live, unresolved
	// domains activated by at on card-bound accounts. Each candidate includes
	// the owning account's activation anchor and charge-attempt recovery marker.
	DomainsPendingCharge(ctx context.Context, at time.Time) ([]DomainChargeCandidate, error)

	// DomainStillPending re-verifies immediately before a sweep action that the
	// domain remains live and unresolved.
	DomainStillPending(ctx context.Context, domainID uuid.UUID) (bool, error)

	// ArmDomainStripeCharge atomically stamps the durable recovery marker and
	// exact rotating funding authority before the first Stripe call.
	ArmDomainStripeCharge(ctx context.Context, domainID uuid.UUID, at time.Time) (StripeChargeClaim, error)

	// MarkDomainChargeResolved stamps the terminal no-charge D1d verdict for an
	// activation period that closed before the owning account activated.
	MarkDomainChargeResolved(ctx context.Context, domainID uuid.UUID) error

	// MarkDomainCharged terminally records a successful activation-period charge
	// and its genuine Stripe invoice/invoice-item ids.
	MarkDomainCharged(ctx context.Context, domainID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error

	// CountLiveDomainsActivatedBefore is the boundary advance input. It counts
	// live domains activated before periodEnd and deliberately ignores mutable
	// charge_resolved state so sweep ordering cannot create a coverage gap.
	CountLiveDomainsActivatedBefore(ctx context.Context, accountID uuid.UUID, periodEnd time.Time) (int, error)
}

// Domain is one custom-domain mirror row (migration 047). RemovedAt is
// meaningful only when Removed is true.
type Domain struct {
	ID          uuid.UUID
	AccountID   uuid.UUID
	AppID       uuid.UUID
	Hostname    string
	ActivatedAt time.Time
	Removed     bool
	RemovedAt   time.Time
	CreatedAt   time.Time
}

// DomainChargeCandidate is one live, unresolved domain activation the
// activation-period sweep evaluates. ActivatedAt is the domain activation;
// AccountActivatedAt is the account's anchored-period activation instant.
type DomainChargeCandidate struct {
	ID                 uuid.UUID
	AccountID          uuid.UUID
	AppID              uuid.UUID
	Hostname           string
	ActivatedAt        time.Time
	AccountActivatedAt time.Time
	// ChargeAttemptedAt is zero until an attempt reaches its Stripe section.
	ChargeAttemptedAt       time.Time
	ChargeFundingAccountID  uuid.UUID
	ChargeFundingGeneration uuid.UUID
}

// ModuleOverageCandidate is one per-module-instance install timer the Leg 1
// grace sweep evaluates (migration 033): its surrogate id + app/account, the
// InstalledAt anchor (FIFO key AND proration anchor), GraceExpiresAt (already
// elapsed for a candidate), and the owning account's ActivatedAt (the billing-
// period anchor, ADR 0005, used to resolve the install's period window).
type ModuleOverageCandidate struct {
	ID             uuid.UUID
	AccountID      uuid.UUID
	AppID          uuid.UUID
	InstalledAt    time.Time
	GraceExpiresAt time.Time
	ActivatedAt    time.Time
	// ChargeAttemptedAt: a prior charge attempt reached its Stripe section
	// (migration 036 recovery marker); zero = never attempted. A retried
	// candidate reconciles against Stripe BEFORE recomputing any live verdict.
	ChargeAttemptedAt       time.Time
	ChargeFundingAccountID  uuid.UUID
	ChargeFundingGeneration uuid.UUID
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
// CreatedModuleCount (migration 030) is the module count FROZEN at
// RegisterApp time — immutable, never touched by SyncAppModules — and is what
// ChargeCreationProration prices the historical creation-period window from;
// ModuleCount is the LIVE count SyncAppModules keeps current and is what the
// boundary advance leg (and the display read for all FUTURE periods) uses.
// ProrationSkipped (migration 031) is true once the app's creation-proration
// charge has been PERMANENTLY skipped as a would-be retroactive catch-up
// (D1d): the account only activated at/after the app's anchored creation
// period had already closed.
type AppMirror struct {
	AppID              uuid.UUID
	AccountID          uuid.UUID
	ModuleCount        int
	CreatedModuleCount int
	CreatedAt          time.Time
	// Name: the frozen app display name (migration 037) — "" when NULL. Written
	// by RegisterApp / SyncAppModules (freeze-on-delete) so a deleted app's bill
	// still shows its last-known name.
	Name               string
	ProrationInvoiceID string
	ProrationSkipped   bool
	// ProrationAttempted: a prior creation-proration charge attempt reached its
	// Stripe section (migration 036 recovery marker) — a retry with this set and
	// an unarmed guard reconciles against Stripe before minting new objects.
	ProrationAttempted bool
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

// FrozenBoundaryCharge is the boundary run's Stripe request FROZEN before its
// first charge (migration 035): the whole-cent amount and whether the line
// includes advance base/overage (the description determinant). Both feed the
// deterministic idem keys ii-<run>/inv-<run>, so a reclaimed run reuses this
// frozen tuple verbatim rather than re-deriving a possibly-drifted live total —
// keeping every retry's Stripe request byte-identical under the stable key.
type FrozenBoundaryCharge struct {
	Cents                   int64
	WithBase                bool
	ChargeFundingAccountID  uuid.UUID
	ChargeFundingGeneration uuid.UUID
}

// CombinedProrationChargeShape is the complete immutable app-base + per-timer
// request and snapshot shape frozen before the first Stripe call (migration
// 050). Descriptions and line coverage are included because they are part of
// Stripe's idempotent request body; raw micros independently feed strict
// prospective-credit projections.
type CombinedProrationChargeShape struct {
	AccountID          uuid.UUID
	Currency           string
	BaseChargeMicros   int64
	BaseChargeCents    int64
	ModuleChargeMicros int64
	ModuleChargeCents  int64
	CoverageStart      time.Time
	CoverageEnd        time.Time
	BaseDescription    string
	ModuleDescription  string
	Snapshot           AppBaseSnapshot
	StraddleSnapshot   *AppBaseSnapshot
}

// CombinedProrationAttempt is one durable first-write winner. Header presence
// proves the timer set is known; TimerIDs may legitimately be empty. ResolvedAt
// and ResolvedInvoiceID are both empty until the same transaction that arms the
// app and timer terminal guards commits.
type CombinedProrationAttempt struct {
	AppID                   uuid.UUID
	AttemptedAt             time.Time
	ChargeFundingAccountID  uuid.UUID
	ChargeFundingGeneration uuid.UUID
	Shape                   CombinedProrationChargeShape
	TimerIDs                []uuid.UUID
	ResolvedAt              time.Time
	ResolvedInvoiceID       string
}

// UnresolvedCombinedProrationAmount is the exact raw amount owned by one
// unresolved frozen attempt. The live pending projection must use this in place
// of recomputing that app/timer set.
type UnresolvedCombinedProrationAmount struct {
	AppID              uuid.UUID
	BaseChargeMicros   int64
	ModuleChargeMicros int64
	TimerCount         int
	TotalMicros        int64
}

var (
	// ErrCombinedProrationAttemptUnknown marks a legacy/incomplete split state:
	// apps.proration_attempted_at exists but migration-050 ownership does not.
	// Exact recovery/projection is impossible, so callers must fail closed.
	ErrCombinedProrationAttemptUnknown = billing.ErrCombinedProrationAttemptUnknown
	// ErrCombinedProrationSelectionChanged means a timer selected at the FIFO
	// statement boundary was removed, resolved, or claimed by standalone Leg 1
	// before its row lock/recheck. No header/marker is committed; retry selects a
	// new coherent winner.
	ErrCombinedProrationSelectionChanged = errors.New("combined proration timer selection changed before freeze")
)

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
	// AutoCollectThresholdMicros is the per-account large-charge disclosure
	// threshold (migration 034), nil when the account uses the platform default
	// (collection.DefaultAutoCollectThresholdMicros). Resolved AT CHARGE TIME by
	// collection.IsLargeAutoCollect to freeze the post-hoc disclosure flag.
	AutoCollectThresholdMicros *int64
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
	AccountID               uuid.UUID
	ChargeFundingAccountID  uuid.UUID
	ChargeFundingGeneration uuid.UUID
	StripeInvoiceID         string
	Status                  string
	AmountDueCents          int64
	AmountPaidCents         int64
	Currency                string
	PeriodStart             time.Time
	PeriodEnd               time.Time
	// IsLargeAutoCollect is the server-computed post-hoc disclosure flag
	// (migration 034): true iff the charged amount exceeded the account's
	// resolved auto-collect threshold WHEN THE CHARGE FIRED. Set by every
	// off-session charge call site; false for anything below the threshold.
	IsLargeAutoCollect bool
	// EverFailed feeds the sticky ever_failed OR-latch in UpsertInvoice. The
	// charge spine ALWAYS passes false here: finalize (auto_advance) returns
	// "open" for a success-bound async off-session charge too, so finalize
	// status cannot distinguish failure. ever_failed is authored solely by the
	// webhook latch (invoice.payment_failed / marked_uncollectible); this field
	// exists only so a later spine mirror racing that webhook can't clear a
	// latched true (existing OR EXCLUDED) — core#135.
	EverFailed bool
}

// RawAggregate is one per-kind aggregated row from the rollup SELECTs, before
// pricing. BillableQuantity is the exact NUMERIC string (count/sum SUM, peak
// MAX, time_weighted integral). Model is the AI pricing dimension the rollup
// groups by (migration 018): empty for non-AI metrics (the rollup's
// COALESCE(model, ”)), a roster model id for infra.ai.* events. It selects the
// price source in MetricPriceMicros (per-model vs catalog). ModuleVersion is
// the version-attribution dimension the rollup ALSO groups by (migration
// 023): empty for a version-less event, the emitting module's version
// otherwise — and (usage-time-pricing Phase 1, migration 044) it is now ALSO
// a PRICING key: MetricPriceMicros tries the version-first snapshot before
// falling back to model/catalog. ActiveSeconds (migration 044) is the
// version's active window (window_v, seconds) for peak/time_weighted rows
// ONLY — "" for count/sum (proration never applies to additive kinds).
type RawAggregate struct {
	AppID            uuid.UUID
	ModuleID         uuid.UUID
	Metric           string
	Kind             Kind
	AggregationKey   AggregationKey
	Model            string
	ModuleVersion    string
	BillableQuantity string
	ActiveSeconds    string
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

// RawAggregates takes the shared account-period admission lock, transitions the
// period to closing, and reads every kind in one transaction. A v2 insertion
// that committed before the lock is visible; one waiting behind it observes the
// closing state and is rejected with durable audit evidence after commit.
func (s *pgxStore) RawAggregates(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) ([]RawAggregate, error) {
	acct := pgtype.UUID{Bytes: accountID, Valid: true}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	qtx := s.q.WithTx(tx)
	// First-card activation rewinds unrolled observations while holding this
	// account row exclusively. Taking the matching shared lock before the period
	// barrier prevents rollup from freezing a pre-activation window while that
	// rewindow is in flight.
	activation, err := qtx.LockUsageAccountActivation(ctx, accountID.String())
	if err != nil {
		return nil, err
	}
	includeV2 := v2RollupWindowMatchesActivation(activation, periodStart, periodEnd)
	if _, err := tx.Exec(ctx, meteringlock.AdvisorySQL, meteringlock.PeriodKey(accountID, periodStart)); err != nil {
		return nil, err
	}
	if err := qtx.CloseBillingPeriodForRollup(ctx, db.CloseBillingPeriodForRollupParams{
		AccountID: accountID.String(), PeriodStart: periodStart, PeriodEnd: periodEnd,
	}); err != nil {
		return nil, err
	}

	sumRows, err := qtx.RollupSumKinds(ctx, db.RollupSumKindsParams{
		AccountID:    acct,
		BillableAt:   pgtype.Timestamptz{Time: periodStart, Valid: true},
		BillableAt_2: pgtype.Timestamptz{Time: periodEnd, Valid: true},
		IncludeV2:    includeV2,
	})
	if err != nil {
		return nil, err
	}
	peakRows, err := qtx.RollupPeakKind(ctx, db.RollupPeakKindParams{
		AccountID: acct, PeriodStart: periodStart, PeriodEnd: periodEnd, IncludeV2: includeV2,
	})
	if err != nil {
		return nil, err
	}
	keyedPeakRows, err := qtx.RollupKeyedPeakKind(ctx, db.RollupKeyedPeakKindParams{
		AccountID: accountID.String(), PeriodStart: periodStart, PeriodEnd: periodEnd, IncludeV2: includeV2,
	})
	if err != nil {
		return nil, err
	}
	twRows, err := qtx.RollupTimeWeightedKind(ctx, db.RollupTimeWeightedKindParams{
		AccountID:    acct,
		BillableAt:   pgtype.Timestamptz{Time: periodStart, Valid: true},
		BillableAt_2: pgtype.Timestamptz{Time: periodEnd, Valid: true},
		IncludeV2:    includeV2,
	})
	if err != nil {
		return nil, err
	}

	out := make([]RawAggregate, 0, len(sumRows)+len(peakRows)+len(keyedPeakRows)+len(twRows))
	// appendRow's activeSeconds param is pgtype.Numeric{} (Valid=false) for the
	// additive kinds (count/sum never carry a window); RawAggregate.ActiveSeconds
	// renders "" in that case (NOT "0" — numericString's NULL rendering — because
	// "no window data" and "a genuinely zero-length window" must stay
	// distinguishable downstream in cycle/money.go's proration).
	appendRow := func(appID, moduleID, metric string, kind db.MsBillingMetricKind, aggregationKey, model, moduleVersion string, qty, activeSeconds pgtype.Numeric) error {
		app, err := uuid.Parse(appID)
		if err != nil {
			return err
		}
		mod, err := uuid.Parse(moduleID)
		if err != nil {
			return err
		}
		as := ""
		if activeSeconds.Valid {
			as = numericString(activeSeconds)
		}
		out = append(out, RawAggregate{
			AppID:            app,
			ModuleID:         mod,
			Metric:           metric,
			Kind:             Kind(kind),
			AggregationKey:   AggregationKey(aggregationKey),
			Model:            model,         // "" for non-AI rows (COALESCE(model, ''))
			ModuleVersion:    moduleVersion, // "" for version-less rows (COALESCE(module_version, ''))
			BillableQuantity: numericString(qty),
			ActiveSeconds:    as,
		})
		return nil
	}
	for _, r := range sumRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.AggregationKey.String, r.Model, r.ModuleVersion, r.BillableQuantity, pgtype.Numeric{}); err != nil {
			return nil, err
		}
	}
	for _, r := range peakRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.AggregationKey.String, r.Model, r.ModuleVersion, r.BillableQuantity, r.ActiveSeconds); err != nil {
			return nil, err
		}
	}
	for _, r := range keyedPeakRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.AggregationKey, r.Model, r.ModuleVersion, r.BillableQuantity, pgtype.Numeric{}); err != nil {
			return nil, err
		}
	}
	for _, r := range twRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.AggregationKey.String, r.Model, r.ModuleVersion, r.BillableQuantity, r.ActiveSeconds); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

// v2RollupWindowMatchesActivation admits v2 observations only after activation
// and only into a window consistent with that immutable anchor. Card-less
// calendar rollup continues to freeze legacy observations, but deliberately
// leaves v2 rows pending so activation can rewindow them into the first funded
// period. The window check also closes the work-list race where activation can
// commit after the unactivated account list is read but before rollup begins.
func v2RollupWindowMatchesActivation(activation pgtype.Timestamptz, periodStart, periodEnd time.Time) bool {
	if !activation.Valid || !periodEnd.After(periodStart) {
		return false
	}
	anchorDay := billingperiod.AnchorDay(activation.Time)
	anchoredStart, anchoredEnd := billingperiod.AnchoredPeriodWindow(periodEnd.Add(-time.Nanosecond), anchorDay)
	return periodEnd.UTC().Equal(anchoredEnd) && !periodStart.UTC().Before(anchoredStart)
}

func (s *pgxStore) MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model, moduleVersion string) (int64, bool, error) {
	// Enforce exact task GPU pricing before every generic fallback. This also
	// catches direct ledger writes that bypass RecordInfraUsage.
	if metric == "infra.task.gpu.hours" {
		if !usage.IsAdmittedTaskGPUModel(model) {
			return 0, false, fmt.Errorf("%w: metric=%s model=%s", ErrTaskGPUModelPrice, metric, model)
		}
		row, err := s.q.LookupModelPrice(ctx, db.LookupModelPriceParams{Metric: metric, Model: model})
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, fmt.Errorf("%w: metric=%s model=%s", ErrTaskGPUModelPrice, metric, model)
		}
		if err != nil {
			return 0, false, err
		}
		if !row.Active || row.UnitPriceMicros <= 0 {
			return 0, false, fmt.Errorf("%w: metric=%s model=%s active=%t price=%d", ErrTaskGPUModelPrice, metric, model, row.Active, row.UnitPriceMicros)
		}
		return row.UnitPriceMicros, true, nil
	}

	// VERSION-FIRST (usage-time-pricing Phase 1, migration 044): an event
	// stamped with a module_version resolves its price from the IMMUTABLE
	// per-(module, metric, module_version) snapshot BEFORE anything else. A
	// hit wins outright — this is the fix for the mid-period-reprice bug: the
	// snapshot is written ONCE at version publish and never overwritten (ON
	// CONFLICT DO NOTHING), so a later version's re-price can never
	// retroactively change what an EARLIER version already resolved here. A
	// MISSING snapshot (pgx.ErrNoRows — module_version="" pre-stamping, or a
	// version published with no SetMetricVersionPrices sync) falls through to
	// the existing model/catalog chain below, unchanged.
	if moduleVersion != "" {
		price, err := s.q.LookupMetricVersionPrice(ctx, db.LookupMetricVersionPriceParams{
			ModuleID:      moduleID.String(),
			Metric:        metric,
			ModuleVersion: moduleVersion,
		})
		if err == nil {
			return price, true, nil // NOT NULL column → a row means priced
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, false, err
		}
		// pgx.ErrNoRows → no version snapshot; fall through to model/catalog.
	}

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
	activeSeconds, err := nullableNumericFromString(agg.ActiveSeconds)
	if err != nil {
		return err
	}
	periodDays, err := nullableNumericFromString(agg.PeriodDays)
	if err != nil {
		return err
	}
	return s.q.UpsertUsageAggregate(ctx, db.UpsertUsageAggregateParams{
		PeriodID:         periodID.String(),
		AccountID:        accountID.String(),
		AppID:            agg.AppID.String(),
		ModuleID:         agg.ModuleID.String(),
		Metric:           agg.Metric,
		Model:            agg.Model,
		ModuleVersion:    agg.ModuleVersion,
		Kind:             db.MsBillingMetricKind(agg.Kind),
		AggregationKey:   nullableAggregationKey(agg.AggregationKey),
		BillableQuantity: qty,
		UnitPriceMicros:  agg.UnitPriceMicros,
		// Never caller-supplied: MarkupNum/Den are one of two compile-time
		// pairs — customMarkupNum/Den 10/10 or infraMarkupNum/Den 12/10
		// (cycle/types.go) — and the column additionally CHECKs > 0
		// (migration 009). A truncation here would silently change what a
		// customer is charged, which is why the reason is stated rather
		// than assumed.
		CustomerMarkupNum: int32(agg.MarkupNum), //nolint:gosec // one of two literals, 10 or 12
		CustomerMarkupDen: int32(agg.MarkupDen), //nolint:gosec // one of two literals, both 10
		RawCostMicros:     agg.RawCostMicros,
		ChargedMicros:     agg.ChargedMicros,
		ActiveSeconds:     activeSeconds,
		PeriodDays:        periodDays,
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

func (s *pgxStore) InsertBillingRun(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (uuid.UUID, bool, bool, error) {
	row, err := s.q.InsertBillingRun(ctx, db.InsertBillingRunParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// The DO UPDATE's WHERE excluded the row → the existing run is 'invoiced'
		// (terminal success). The window was already charged; do not re-charge.
		return uuid.Nil, false, false, nil
	}
	if err != nil {
		return uuid.Nil, false, false, err
	}
	runID, err := uuid.Parse(row.ID)
	if err != nil {
		return uuid.Nil, false, false, err
	}
	return runID, true, row.Reclaimed, nil
}

func (s *pgxStore) PeriodChargedTotal(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (int64, error) {
	return s.q.PeriodChargedTotal(ctx, db.PeriodChargedTotalParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
}

// CreditBillingMode is the narrow rollout classifier. Keep this query separate
// from WalletCreditState: a selected standard account must reach the exact
// legacy Stripe path without touching credit_ledger.
func (s *pgxStore) CreditBillingMode(ctx context.Context, accountID uuid.UUID) (CreditBillingMode, error) {
	var raw string
	err := s.pool.QueryRow(ctx, `
		SELECT billing_mode::text
		FROM ms_billing.accounts
		WHERE id = $1
	`, accountID).Scan(&raw)
	if err != nil {
		return "", err
	}
	return parseCreditBillingMode(raw)
}

func (s *pgxStore) WalletCreditState(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time) (WalletCreditState, error) {
	row, err := s.q.WalletCreditState(ctx, db.WalletCreditStateParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return WalletCreditState{}, err
	}
	mode, err := parseCreditBillingMode(row.BillingMode)
	if err != nil {
		return WalletCreditState{}, err
	}
	if row.SpendableBalanceMicros < 0 || row.PeriodDrawnMicros < 0 {
		return WalletCreditState{}, fmt.Errorf(
			"wallet state contains a negative magnitude: spendable=%d period_drawn=%d",
			row.SpendableBalanceMicros, row.PeriodDrawnMicros,
		)
	}
	return WalletCreditState{
		Mode:                   mode,
		SpendableBalanceMicros: row.SpendableBalanceMicros,
		PeriodDrawnMicros:      row.PeriodDrawnMicros,
	}, nil
}

// DrawWalletCredits is the standalone allocation primitive used by focused
// store tests and non-run callers. The billing-cycle spine uses
// DrawBillingRunWalletCredits so its debit and crash marker commit atomically.
func (s *pgxStore) DrawWalletCredits(ctx context.Context, accountID uuid.UUID, periodStart, periodEnd time.Time, amountMicros int64, allowNew bool) (WalletDrawdown, error) {
	return s.drawWalletCredits(
		ctx,
		uuid.Nil,
		accountID,
		periodStart,
		periodEnd,
		amountMicros,
		false,
		allowNew,
	)
}

// DrawBillingRunWalletCredits serializes allocation per account, then appends
// one signed usage_draw row for each funding lot consumed. When any debit
// exists, it also freezes the exact post-wallet Stripe remainder on runID in
// this SAME transaction. Therefore no observable state can contain a committed
// wallet debit without the legacy billing_runs recovery marker.
//
// The boundary amount currently combines usage arrears and advance fees, so
// this method cannot honestly split usage_draw from subscription_draw; a future
// category-specific caller should own subscription_draw. Recovery nevertheless
// recognizes either type so a period can never acquire a second boundary debit.
func (s *pgxStore) DrawBillingRunWalletCredits(
	ctx context.Context,
	runID, accountID uuid.UUID,
	periodStart, periodEnd time.Time,
	boundaryTotalMicros int64,
	withBase, allowNew bool,
) (WalletDrawdown, error) {
	if runID == uuid.Nil {
		return WalletDrawdown{}, errors.New("billing run id required for boundary wallet draw")
	}
	return s.drawWalletCredits(
		ctx,
		runID,
		accountID,
		periodStart,
		periodEnd,
		boundaryTotalMicros,
		withBase,
		allowNew,
	)
}

func (s *pgxStore) drawWalletCredits(
	ctx context.Context,
	runID, accountID uuid.UUID,
	periodStart, periodEnd time.Time,
	amountMicros int64,
	withBase, allowNew bool,
) (WalletDrawdown, error) {
	if amountMicros < 0 {
		return WalletDrawdown{}, fmt.Errorf("wallet draw amount must be non-negative: %d", amountMicros)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return WalletDrawdown{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	rawMode, err := qtx.LockWalletAccount(ctx, accountID.String())
	if err != nil {
		return WalletDrawdown{}, err
	}
	mode, err := parseCreditBillingMode(rawMode)
	if err != nil {
		return WalletDrawdown{}, err
	}
	out := WalletDrawdown{Mode: mode}
	markerBlocksNew := false
	if runID != uuid.Nil {
		run, err := qtx.LockBillingRunCharge(ctx, runID.String())
		if err != nil {
			return WalletDrawdown{}, err
		}
		if BillingRunStatus(run.Status) == RunStatusInvoiced {
			return WalletDrawdown{}, fmt.Errorf(
				"billing run %s is already invoiced; refusing wallet draw",
				runID,
			)
		}
		if run.FrozenChargeCents.Valid {
			if !run.FrozenChargeWithBase.Valid {
				return WalletDrawdown{}, fmt.Errorf(
					"billing run %s has an incomplete frozen charge",
					runID,
				)
			}
			out.BoundaryCharge = FrozenBoundaryCharge{
				Cents:                   run.FrozenChargeCents.Int64,
				WithBase:                run.FrozenChargeWithBase.Bool,
				ChargeFundingAccountID:  uuidFromPg(run.ChargeFundingAccountID),
				ChargeFundingGeneration: uuidFromPg(run.ChargeFundingGeneration),
			}
			if out.BoundaryCharge.ChargeFundingAccountID == uuid.Nil ||
				out.BoundaryCharge.ChargeFundingGeneration == uuid.Nil {
				return WalletDrawdown{}, fmt.Errorf(
					"billing run %s has an incomplete frozen funding claim",
					runID,
				)
			}
			out.BoundaryChargeFrozen = true
			markerBlocksNew = true
		}
	}
	finish := func() (WalletDrawdown, error) {
		if runID != uuid.Nil && out.DrawnMicros > 0 && amountMicros > 0 {
			remainderMicros := amountMicros - out.DrawnMicros
			if remainderMicros < 0 {
				// A reclaimed period can have a larger already-durable debit
				// than a later live recomputation. The original wallet money is
				// never undone implicitly; its Stripe remainder is zero.
				remainderMicros = 0
			}
			remainderCents, err := centsFromMicros(remainderMicros)
			if err != nil {
				return WalletDrawdown{}, fmt.Errorf("freeze wallet remainder: %w", err)
			}
			// The wallet debit and Stripe remainder marker are one atomic money
			// decision. Pin the exact rotating funding authorization in this same
			// transaction even when the funder currently has no usable PM: a later
			// reclaim may proceed only against this generation, never a designation
			// that changed after the wallet money was committed.
			fundingAuth, err := qtx.StripeFundingAuthorization(ctx, accountID.String())
			if err != nil {
				return WalletDrawdown{}, err
			}
			if err := qtx.FreezeBillingRunCharge(ctx, db.FreezeBillingRunChargeParams{
				ID:                      runID.String(),
				FrozenChargeCents:       pgtype.Int8{Int64: remainderCents, Valid: true},
				FrozenChargeWithBase:    pgtype.Bool{Bool: withBase, Valid: true},
				ChargeFundingAccountID:  fundingAuth.FundingAccountID,
				ChargeFundingGeneration: fundingAuth.Generation,
			}); err != nil {
				return WalletDrawdown{}, err
			}
			row, err := qtx.BillingRunFrozenCharge(ctx, runID.String())
			if err != nil {
				return WalletDrawdown{}, err
			}
			if !row.FrozenChargeCents.Valid || !row.FrozenChargeWithBase.Valid ||
				!row.ChargeFundingAccountID.Valid || !row.ChargeFundingGeneration.Valid {
				// FreezeBillingRunCharge refuses an already-invoiced run. This
				// error rolls the transaction back, so a concurrent terminal
				// mark can never leave a debit without its recovery marker.
				return WalletDrawdown{}, fmt.Errorf(
					"billing run %s has no frozen charge after wallet draw",
					runID,
				)
			}
			out.BoundaryCharge = FrozenBoundaryCharge{
				Cents:                   row.FrozenChargeCents.Int64,
				WithBase:                row.FrozenChargeWithBase.Bool,
				ChargeFundingAccountID:  uuidFromPg(row.ChargeFundingAccountID),
				ChargeFundingGeneration: uuidFromPg(row.ChargeFundingGeneration),
			}
			out.BoundaryChargeFrozen = true
		}
		if err := tx.Commit(ctx); err != nil {
			return WalletDrawdown{}, err
		}
		return out, nil
	}
	if _, err := qtx.LockWalletLedgerEntries(ctx, accountID.String()); err != nil {
		return WalletDrawdown{}, err
	}

	period, err := qtx.WalletPeriodDraw(ctx, db.WalletPeriodDrawParams{
		AccountID:   accountID.String(),
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
	})
	if err != nil {
		return WalletDrawdown{}, err
	}
	if period.DrawnMicros < 0 {
		return WalletDrawdown{}, fmt.Errorf("wallet period draw contains a negative magnitude: %d", period.DrawnMicros)
	}
	if period.DrawnMicros > 0 {
		out.DrawnMicros = period.DrawnMicros
		return finish()
	}

	// The existing draw check deliberately comes first: a frozen reclaim passes
	// allowNew=false but must still recover a debit from the earlier attempt.
	// A pre-existing run marker is stronger still: another daemon may already
	// have started the frozen Stripe request, so a new debit beside it is
	// forbidden even when this caller's earlier marker read was stale.
	if markerBlocksNew || !allowNew || amountMicros <= 0 {
		return finish()
	}

	balanceAfter, err := qtx.WalletSettledBalance(ctx, accountID.String())
	if err != nil {
		return WalletDrawdown{}, err
	}

	left := amountMicros
	if mode == CreditBillingModeStandard {
		// Positive lots alone are not enough: settled negative adjustments lower
		// the authoritative account balance, while unused expired grants must be
		// removed from that balance before it can cap a standard draw.
		expiredMicros, err := qtx.WalletExpiredCreditBalance(ctx, accountID.String())
		if err != nil {
			return WalletDrawdown{}, err
		}
		if expiredMicros < 0 {
			return WalletDrawdown{}, fmt.Errorf("wallet expired-credit balance is negative: %d", expiredMicros)
		}
		if balanceAfter <= expiredMicros {
			left = 0
		} else if capMicros := balanceAfter - expiredMicros; left > capMicros {
			left = capMicros
		}
	}
	target := left

	var lots []db.WalletSpendableLotsRow
	if left > 0 {
		lots, err = qtx.WalletSpendableLots(ctx, accountID.String())
		if err != nil {
			return WalletDrawdown{}, err
		}
	}

	insertDraw := func(consume int64, sourceID string) error {
		if consume <= 0 {
			return fmt.Errorf("wallet draw allocation must be positive: %d", consume)
		}
		if balanceAfter < math.MinInt64+consume {
			return fmt.Errorf("wallet balance_after_micros underflow: balance=%d draw=%d", balanceAfter, consume)
		}
		balanceAfter -= consume

		source := pgtype.UUID{}
		keySource := "unsecured"
		if sourceID != "" {
			id, err := uuid.Parse(sourceID)
			if err != nil {
				return fmt.Errorf("parse wallet source credit id: %w", err)
			}
			source = pgtype.UUID{Bytes: id, Valid: true}
			keySource = id.String()
		}
		return qtx.InsertWalletDraw(ctx, db.InsertWalletDrawParams{
			AccountID:          accountID.String(),
			AmountMicros:       consume,
			BalanceAfterMicros: balanceAfter,
			IdempotencyKey: fmt.Sprintf(
				"wallet-draw:%s:%s:usage_draw:%s",
				accountID.String(), period.PeriodID, keySource,
			),
			PeriodID:       period.PeriodID,
			SourceCreditID: source,
		})
	}

	for _, lot := range lots {
		if left == 0 {
			break
		}
		if lot.RemainingMicros <= 0 {
			return WalletDrawdown{}, fmt.Errorf(
				"wallet query returned a non-positive lot remainder: source=%s remaining=%d",
				lot.ID, lot.RemainingMicros,
			)
		}
		consume := lot.RemainingMicros
		if consume > left {
			consume = left
		}
		if err := insertDraw(consume, lot.ID); err != nil {
			return WalletDrawdown{}, err
		}
		left -= consume
	}

	if mode == CreditBillingModeCredits && left > 0 {
		// Credits mode is wallet-only. Its configured credit policy owns the
		// unsecured remainder, represented by the one NULL-source period row.
		if err := insertDraw(left, ""); err != nil {
			return WalletDrawdown{}, err
		}
		left = 0
	}

	out.DrawnMicros = target - left
	return finish()
}

func parseCreditBillingMode(raw string) (CreditBillingMode, error) {
	mode := CreditBillingMode(raw)
	switch mode {
	case CreditBillingModeStandard, CreditBillingModeCredits:
		return mode, nil
	default:
		return "", fmt.Errorf("unknown account billing_mode %q", raw)
	}
}

func (s *pgxStore) HasUsableDefaultPM(ctx context.Context, accountID uuid.UUID) (bool, error) {
	return s.q.HasUsableDefaultPM(ctx, accountID.String())
}

func (s *pgxStore) AccountStripeCustomer(ctx context.Context, accountID uuid.UUID) (string, error) {
	return s.q.AccountStripeCustomer(ctx, accountID.String())
}

// UsableNonFraudCardCount projects the usable_card_count leg of the generated
// ServiceBlockSignals read — deliberately NOT a new SQL predicate (see the
// Store interface doc: one card-quality rule, shared with the standing gate).
func (s *pgxStore) UsableNonFraudCardCount(ctx context.Context, accountID uuid.UUID) (int, error) {
	row, err := s.q.ServiceBlockSignals(ctx, accountID.String())
	if err != nil {
		return 0, err
	}
	return int(row.UsableCardCount), nil
}

func (s *pgxStore) AccountCollection(ctx context.Context, accountID uuid.UUID) (AccountCollection, error) {
	row, err := s.q.AccountCollectionFields(ctx, accountID.String())
	if err != nil {
		return AccountCollection{}, err
	}
	var autoCollectThreshold *int64
	if row.AutoCollectThresholdMicros.Valid {
		v := row.AutoCollectThresholdMicros.Int64
		autoCollectThreshold = &v
	}
	return AccountCollection{
		Mode:                       BillingMode(row.UsageBillingMode),
		CreditLimitMicros:          row.CreditLimitMicros,
		HasSpendCeiling:            row.SpendCeilingMicros.Valid,
		SpendCeilingMicros:         row.SpendCeilingMicros.Int64,
		CreatedAt:                  row.CreatedAt,
		AutoCollectThresholdMicros: autoCollectThreshold,
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
	// Invoice payer identity must come from the durable pre-Stripe attempt. A
	// mirror write is never allowed to infer historical provenance from today's
	// mutable designation.
	if inv.ChargeFundingAccountID == uuid.Nil || inv.ChargeFundingGeneration == uuid.Nil {
		return errors.New("invoice mirror requires exact durable funding provenance")
	}
	return s.q.UpsertInvoice(ctx, db.UpsertInvoiceParams{
		AccountID:               inv.AccountID.String(),
		StripeInvoiceID:         inv.StripeInvoiceID,
		Status:                  inv.Status,
		AmountDue:               due,
		AmountPaid:              paid,
		Currency:                inv.Currency,
		PeriodStart:             pgtype.Timestamptz{Time: inv.PeriodStart, Valid: !inv.PeriodStart.IsZero()},
		PeriodEnd:               pgtype.Timestamptz{Time: inv.PeriodEnd, Valid: !inv.PeriodEnd.IsZero()},
		IsLargeAutoCollect:      inv.IsLargeAutoCollect,
		EverFailed:              inv.EverFailed,
		ChargeFundingAccountID:  inv.ChargeFundingAccountID.String(),
		ChargeFundingGeneration: inv.ChargeFundingGeneration.String(),
	})
}

func (s *pgxStore) MarkBillingRun(ctx context.Context, runID uuid.UUID, status BillingRunStatus, stripeInvoiceID string, totalCents int64) error {
	total, err := centsNumeric(totalCents)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	if err := qtx.MarkBillingRun(ctx, db.MarkBillingRunParams{
		ID:              runID.String(),
		Status:          string(status),
		StripeInvoiceID: pgtype.Text{String: stripeInvoiceID, Valid: stripeInvoiceID != ""},
		TotalAmount:     total,
	}); err != nil {
		return err
	}
	if status == RunStatusInvoiced {
		if err := qtx.MarkBillingPeriodInvoicedByRun(ctx, runID.String()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *pgxStore) MarkBillingRunInvoicedIfUnfrozen(ctx context.Context, runID uuid.UUID) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	rows, err := qtx.MarkBillingRunInvoicedIfUnfrozen(ctx, runID.String())
	if err != nil {
		return false, err
	}
	if rows > 0 {
		if err := qtx.MarkBillingPeriodInvoicedByRun(ctx, runID.String()); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *pgxStore) FreezeBillingRunCharge(ctx context.Context, runID uuid.UUID, charge FrozenBoundaryCharge) (FrozenBoundaryCharge, StripeRailClaimOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	locked, err := qtx.LockBillingRunFundingArm(ctx, runID.String())
	if err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	if locked.FrozenChargeCents.Valid {
		if !locked.ChargeFundingAccountID.Valid || !locked.ChargeFundingGeneration.Valid {
			return FrozenBoundaryCharge{}, StripeRailStale,
				fmt.Errorf("billing run %s is frozen without a funding authorization", runID)
		}
		surviving := FrozenBoundaryCharge{
			Cents:                   locked.FrozenChargeCents.Int64,
			WithBase:                locked.FrozenChargeWithBase.Bool,
			ChargeFundingAccountID:  uuidFromPg(locked.ChargeFundingAccountID),
			ChargeFundingGeneration: uuidFromPg(locked.ChargeFundingGeneration),
		}
		if err := tx.Commit(ctx); err != nil {
			return FrozenBoundaryCharge{}, StripeRailStale, err
		}
		return surviving, StripeRailClaimed, nil
	}
	if locked.Status == string(RunStatusInvoiced) {
		return FrozenBoundaryCharge{}, StripeRailStale, nil
	}
	fundingAuth, err := qtx.StripeFundingAuthorization(ctx, locked.AccountID)
	if err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	if !fundingAuth.HasUsablePaymentMethod {
		return FrozenBoundaryCharge{}, StripeRailNoPaymentMethod, nil
	}
	if fundingAuth.StripeCustomerID == "" {
		return FrozenBoundaryCharge{}, StripeRailStale,
			errors.New("boundary funder has a usable PM but no Stripe customer id")
	}

	// WHERE frozen_charge_cents IS NULL makes this first-write-wins; the
	// terminal-status predicate also refuses a stale freeze after another daemon
	// completed a zero-charge run. The read-back returns the surviving value
	// regardless of which freeze won. If the terminal predicate won there is no
	// value and this method errors before the caller can enter Stripe.
	if err := qtx.FreezeBillingRunCharge(ctx, db.FreezeBillingRunChargeParams{
		ID:                      runID.String(),
		FrozenChargeCents:       pgtype.Int8{Int64: charge.Cents, Valid: true},
		FrozenChargeWithBase:    pgtype.Bool{Bool: charge.WithBase, Valid: true},
		ChargeFundingAccountID:  fundingAuth.FundingAccountID,
		ChargeFundingGeneration: fundingAuth.Generation,
	}); err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	row, err := qtx.BillingRunFrozenCharge(ctx, runID.String())
	if err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	if !row.FrozenChargeCents.Valid || !row.ChargeFundingAccountID.Valid || !row.ChargeFundingGeneration.Valid {
		return FrozenBoundaryCharge{}, StripeRailStale, fmt.Errorf("billing run %s has no complete frozen funding claim immediately after freezing", runID)
	}
	surviving := FrozenBoundaryCharge{
		Cents:                   row.FrozenChargeCents.Int64,
		WithBase:                row.FrozenChargeWithBase.Bool,
		ChargeFundingAccountID:  uuidFromPg(row.ChargeFundingAccountID),
		ChargeFundingGeneration: uuidFromPg(row.ChargeFundingGeneration),
	}
	if err := tx.Commit(ctx); err != nil {
		return FrozenBoundaryCharge{}, StripeRailStale, err
	}
	return surviving, StripeRailClaimed, nil
}

func (s *pgxStore) BillingRunFrozenCharge(ctx context.Context, runID uuid.UUID) (FrozenBoundaryCharge, bool, error) {
	row, err := s.q.BillingRunFrozenCharge(ctx, runID.String())
	if err != nil {
		return FrozenBoundaryCharge{}, false, err
	}
	if !row.FrozenChargeCents.Valid {
		return FrozenBoundaryCharge{}, false, nil // fresh run — no prior attempt froze
	}
	if !row.ChargeFundingAccountID.Valid || !row.ChargeFundingGeneration.Valid {
		return FrozenBoundaryCharge{}, false, fmt.Errorf("billing run %s is frozen without a complete funding claim", runID)
	}
	return FrozenBoundaryCharge{
		Cents:                   row.FrozenChargeCents.Int64,
		WithBase:                row.FrozenChargeWithBase.Bool,
		ChargeFundingAccountID:  uuidFromPg(row.ChargeFundingAccountID),
		ChargeFundingGeneration: uuidFromPg(row.ChargeFundingGeneration),
	}, true, nil
}

func (s *pgxStore) AccountsWithUsageEvents(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AccountsWithUsageEvents(ctx, db.AccountsWithUsageEventsParams{
		BillableAt:   pgtype.Timestamptz{Time: periodStart, Valid: true},
		BillableAt_2: pgtype.Timestamptz{Time: periodEnd, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) UnactivatedAccountsWithUsage(ctx context.Context, periodStart, periodEnd time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.UnactivatedAccountsWithUsage(ctx, db.UnactivatedAccountsWithUsageParams{
		BillableAt:   pgtype.Timestamptz{Time: periodStart, Valid: true},
		BillableAt_2: pgtype.Timestamptz{Time: periodEnd, Valid: true},
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

func (s *pgxStore) InsertAppMirror(ctx context.Context, appID, accountID, ownerOrgID uuid.UUID, moduleCount int, createdAt time.Time, name string) error {
	// RowsAffected 0 = a retry hit ON CONFLICT DO NOTHING — success either way.
	// accountID uuid.Nil → NULL: an UNBILLED org roster row awaiting funding
	// designation (migration 041); ownerOrgID uuid.Nil → NULL for user-owned apps.
	_, err := s.q.InsertAppMirror(ctx, db.InsertAppMirrorParams{
		AppID:       appID.String(),
		AccountID:   pgUUIDOrNull(accountID),
		OwnerOrgID:  pgUUIDOrNull(ownerOrgID),
		ModuleCount: int32(moduleCount), //nolint:gosec // RegisterApp validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		CreatedAt:   createdAt,
		Name:        pgtype.Text{String: name, Valid: name != ""}, // NULL when the caller omits a name (frontend falls back)
	})
	return err
}

func (s *pgxStore) SetAppName(ctx context.Context, appID uuid.UUID, name string) error {
	// 0 rows = the app is deleted (frozen name, WHERE deleted_at IS NULL) — a
	// documented no-op, the same posture as SetAppModuleCount on a deleted app.
	_, err := s.q.SetAppName(ctx, db.SetAppNameParams{
		AppID: appID.String(),
		Name:  pgtype.Text{String: name, Valid: name != ""},
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
	return AppMirror{
		AppID: app,
		// Nil = an UNBILLED org roster row (NULL account, migration 041) — the
		// charge legs and the timer reconcile all skip it until attach.
		AccountID:          uuidFromPg(row.AccountID),
		ModuleCount:        int(row.ModuleCount),
		CreatedModuleCount: int(row.CreatedModuleCount),
		CreatedAt:          row.CreatedAt,
		Name:               row.Name.String,               // "" when NULL (pre-037 / unnamed)
		ProrationInvoiceID: row.ProrationInvoiceID.String, // "" when NULL (guard unarmed)
		ProrationSkipped:   row.ProrationSkippedAt.Valid,
		ProrationAttempted: row.ProrationAttemptedAt.Valid,
		Deleted:            row.DeletedAt.Valid,
		DeletedAt:          row.DeletedAt.Time,
	}, true, nil
}

func (s *pgxStore) AppsPendingProration(ctx context.Context, createdBefore time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AppsPendingProration(ctx, db.AppsPendingProrationParams{
		CreatedBefore: createdBefore,
		// hours, not days (D5): the SQL grace cutoff must match the Go legs'
		// fixed 24h-per-day UTC window regardless of the session timezone.
		GraceHours: usage.GraceDays * 24,
	})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

// deferredRollback rolls back tx using a short-lived DETACHED context rather
// than reusing ctx verbatim. ctx may already be cancelled or past its deadline
// by the time this runs (e.g. the surrounding Lambda invocation timed out
// while a Stripe call the caller was awaiting stalled) — Rollback on a dead
// context can fail silently, leaving the row lock / transaction open until
// Postgres's own dead-connection detection eventually reclaims it. Stripping
// cancellation (context.WithoutCancel) while keeping request-scoped values,
// then applying a fresh short timeout, lets cleanup reach Postgres either way.
func deferredRollback(ctx context.Context, tx pgx.Tx) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(rctx) // no-op after a successful Commit
}

// lockAndReadChargeableApp briefly SELECT ... FOR UPDATE-locks the roster row,
// re-verifies it is still chargeable (deleted_at IS NULL AND
// proration_invoice_id IS NULL), and releases the lock (the transaction
// commits either way — there is nothing left to write once the terminal
// checks pass, so a plain commit is equivalent to and cheaper than a rollback
// here). proceed=false means the caller must return (outcome, invID, nil)
// immediately without invoking charge; proceed=true carries the locked
// snapshot (including the frozen created_module_count) charge prices from.
func (s *pgxStore) lockAndReadChargeableApp(ctx context.Context, appID uuid.UUID) (locked AppMirror, outcome ProrationOutcome, invID string, proceed bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppMirror{}, 0, "", false, err
	}
	defer deferredRollback(ctx, tx)

	qtx := s.q.WithTx(tx)
	row, err := qtx.SelectAppMirrorForUpdate(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return AppMirror{}, ProrationLockedNotFound, "", false, nil
	}
	if err != nil {
		return AppMirror{}, 0, "", false, err
	}
	// Deleted WITHIN grace = never charged (scenario 1). Deleted AFTER the
	// grace elapsed SURVIVED it (wave 2, D11) — the creation charge is owed
	// (grace only delays WHEN it fires; the H2 boundary exclusion means no
	// other leg has a backstop for this window), so the charge proceeds.
	if row.DeletedAt.Valid && row.DeletedAt.Time.Before(moduleGraceExpiry(row.CreatedAt.UTC())) {
		return AppMirror{}, ProrationLockedDeleted, "", false, nil
	}
	if row.ProrationInvoiceID.Valid {
		return AppMirror{}, ProrationLockedAlreadyCharged, row.ProrationInvoiceID.String, false, nil
	}

	app, err := uuid.Parse(row.AppID)
	if err != nil {
		return AppMirror{}, 0, "", false, err
	}
	locked = AppMirror{
		AppID: app,
		// Nil (NULL account — unbilled org roster row) never reaches the charge:
		// AppsPendingProration excludes such rows and ChargeCreationProration
		// guards the direct path, so this decode just carries the state through.
		AccountID:          uuidFromPg(row.AccountID),
		ModuleCount:        int(row.ModuleCount),
		CreatedModuleCount: int(row.CreatedModuleCount),
		CreatedAt:          row.CreatedAt,
		Name:               row.Name.String,
		ProrationAttempted: row.ProrationAttemptedAt.Valid,
	}

	if err := tx.Commit(ctx); err != nil {
		return AppMirror{}, 0, "", false, err
	}
	return locked, 0, "", true, nil
}

// persistProrationCharge mirrors a SUCCESSFULLY-created Stripe charge (the
// invoice, the migration-028 base snapshot, and the one-shot guard) inside one
// short transaction. Called AFTER the Stripe network call has already
// completed — the money has already moved — so this always persists on a
// non-nil pc: a concurrent soft-delete that raced in during the (now-released)
// window between the lock and this write does NOT unwind an already-succeeded
// charge (D1e forbids refunds), and a genuinely concurrent second charge
// attempt for the same app converges on identical values (the deterministic
// per-app Stripe Idempotency-Keys guarantee the SAME invoice id, and every
// write here is itself idempotent / first-write-wins).
func (s *pgxStore) persistProrationCharge(ctx context.Context, appID uuid.UUID, pc *ProrationCharge) (ProrationOutcome, string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, "", err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	// Match freeze's app→header lock order. Besides serializing concurrent
	// terminal writers, this makes any app guard/header disagreement a loud
	// rollback instead of silently resolving only part of the ownership graph.
	appRow, err := qtx.SelectAppMirrorForUpdate(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return ProrationLockedNotFound, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	attempt, found, err := readCombinedProrationAttempt(ctx, qtx, appID)
	if err != nil {
		return 0, "", err
	}
	if !found {
		return 0, "", fmt.Errorf(
			"cannot persist app %s proration without durable combined-attempt ownership",
			appID,
		)
	}
	if uuidFromPg(appRow.AccountID) != attempt.Shape.AccountID ||
		!appRow.ProrationAttemptedAt.Valid ||
		appRow.ProrationSkippedAt.Valid {
		return 0, "", fmt.Errorf(
			"combined proration attempt %s disagrees with the locked app marker/account",
			appID,
		)
	}
	if attempt.ResolvedInvoiceID != "" {
		if !appRow.ProrationInvoiceID.Valid ||
			appRow.ProrationInvoiceID.String != attempt.ResolvedInvoiceID {
			return 0, "", fmt.Errorf(
				"resolved combined proration attempt %s disagrees with the app invoice guard",
				appID,
			)
		}
		return ProrationLockedCharged, attempt.ResolvedInvoiceID, nil
	}
	if appRow.ProrationInvoiceID.Valid {
		return 0, "", fmt.Errorf(
			"unresolved combined proration attempt %s has an already-armed app invoice guard",
			appID,
		)
	}
	if pc.ResolvedAt.IsZero() {
		return 0, "", errors.New("combined proration resolved_at required")
	}
	if err := validateProrationChargeMatchesAttempt(attempt, pc); err != nil {
		return 0, "", err
	}
	expectedTimers := make(map[uuid.UUID]struct{}, len(attempt.TimerIDs))
	for _, timerID := range attempt.TimerIDs {
		expectedTimers[timerID] = struct{}{}
	}
	if len(pc.TimerCharges) != len(expectedTimers) {
		return 0, "", fmt.Errorf(
			"combined proration attempt %s owns %d timers but persistence received %d",
			appID, len(expectedTimers), len(pc.TimerCharges),
		)
	}
	seenTimers := make(map[uuid.UUID]struct{}, len(pc.TimerCharges))
	for _, timerCharge := range pc.TimerCharges {
		if _, ok := expectedTimers[timerCharge.TimerID]; !ok {
			return 0, "", fmt.Errorf(
				"combined proration attempt %s does not own timer %s",
				appID, timerCharge.TimerID,
			)
		}
		if _, duplicate := seenTimers[timerCharge.TimerID]; duplicate {
			return 0, "", fmt.Errorf(
				"combined proration attempt %s received duplicate timer %s",
				appID, timerCharge.TimerID,
			)
		}
		seenTimers[timerCharge.TimerID] = struct{}{}
	}

	due, err := centsNumeric(pc.Invoice.AmountDueCents)
	if err != nil {
		return 0, "", err
	}
	paid, err := centsNumeric(pc.Invoice.AmountPaidCents)
	if err != nil {
		return 0, "", err
	}
	if err := qtx.UpsertInvoice(ctx, db.UpsertInvoiceParams{
		AccountID:               pc.Invoice.AccountID.String(),
		ChargeFundingAccountID:  attempt.ChargeFundingAccountID.String(),
		ChargeFundingGeneration: attempt.ChargeFundingGeneration.String(),
		StripeInvoiceID:         pc.Invoice.StripeInvoiceID,
		Status:                  pc.Invoice.Status,
		AmountDue:               due,
		AmountPaid:              paid,
		Currency:                pc.Invoice.Currency,
		PeriodStart:             pgtype.Timestamptz{Time: pc.Invoice.PeriodStart, Valid: !pc.Invoice.PeriodStart.IsZero()},
		PeriodEnd:               pgtype.Timestamptz{Time: pc.Invoice.PeriodEnd, Valid: !pc.Invoice.PeriodEnd.IsZero()},
		// Scenario 5 — the disclosure flag the charge callback computed for the FULL
		// combined debit (base + co-created overage lines). Dropping it here would
		// silently write false for every creation/combined invoice.
		IsLargeAutoCollect: pc.Invoice.IsLargeAutoCollect,
		EverFailed:         pc.Invoice.EverFailed,
	}); err != nil {
		return 0, "", err
	}
	if err := qtx.UpsertProrationBaseSnapshot(ctx, db.UpsertProrationBaseSnapshotParams{
		AppID:       pc.Snapshot.AppID.String(),
		PeriodStart: pc.Snapshot.PeriodStart,
		PeriodEnd:   pc.Snapshot.PeriodEnd,
		ModuleCount: int32(pc.Snapshot.ModuleCount), //nolint:gosec // count comes from the locked apps row, whose writers validate 0 ≤ count ≤ maxModuleCount
		BaseMicros:  pc.Snapshot.BaseMicros,
	}); err != nil {
		return 0, "", err
	}
	// A creation grace that straddled the period boundary billed the straddled
	// period IN FULL on this same invoice — freeze its snapshot too (the boundary
	// leg excluded the app there and writes nothing for that period).
	if pc.StraddleSnapshot != nil {
		if err := qtx.UpsertProrationBaseSnapshot(ctx, db.UpsertProrationBaseSnapshotParams{
			AppID:       pc.StraddleSnapshot.AppID.String(),
			PeriodStart: pc.StraddleSnapshot.PeriodStart,
			PeriodEnd:   pc.StraddleSnapshot.PeriodEnd,
			ModuleCount: int32(pc.StraddleSnapshot.ModuleCount), //nolint:gosec // same validated apps-row count as above
			BaseMicros:  pc.StraddleSnapshot.BaseMicros,
		}); err != nil {
			return 0, "", err
		}
	}
	// Resolve the header before its children inside THIS transaction. Other
	// transactions continue to see it unresolved until commit, so ownership is
	// never externally released early. The internal order allows migration
	// 050's mixed-version guard to distinguish this exact owner writer from an
	// old standalone worker. Any later child mismatch rolls this update back.
	resolved, err := qtx.ResolveCombinedProrationAttempt(ctx, db.ResolveCombinedProrationAttemptParams{
		ResolvedAt:        pc.ResolvedAt,
		ResolvedInvoiceID: pgtype.Text{String: pc.InvoiceID, Valid: pc.InvoiceID != ""},
		AppID:             appID.String(),
	})
	if err != nil {
		return 0, "", err
	}
	if resolved != 1 {
		return 0, "", fmt.Errorf(
			"combined proration attempt %s could not resolve to invoice %s",
			appID, pc.InvoiceID,
		)
	}

	// Arm the app guard only after the header is internally resolved, allowing
	// migration 050's mixed-version trigger to reject legacy app workers while
	// admitting this exact terminal transaction. First-write-wins must affect
	// exactly one locked row; otherwise all writes, including header resolve,
	// roll back.
	armed, err := qtx.SetAppProrationInvoice(ctx, db.SetAppProrationInvoiceParams{
		AppID:              appID.String(),
		ProrationInvoiceID: pgtype.Text{String: pc.InvoiceID, Valid: true},
	})
	if err != nil {
		return 0, "", err
	}
	if armed != 1 {
		return 0, "", fmt.Errorf(
			"combined proration attempt %s could not arm app invoice %s",
			appID, pc.InvoiceID,
		)
	}

	// Scenario 3 — stamp every exact child billed on this SAME invoice. Unlike
	// the generic legacy mark, this query proves child membership and returns a
	// row count. A removed/resolved/stolen child is a loud atomic rollback; the
	// header can never be buried while a timer guard disagrees.
	for _, tc := range pc.TimerCharges {
		marked, err := qtx.MarkCombinedProrationTimerCharged(ctx, db.MarkCombinedProrationTimerChargedParams{
			GraceChargedAt:     tc.ChargedAt,
			GraceInvoiceID:     pgtype.Text{String: tc.InvoiceID, Valid: tc.InvoiceID != ""},
			GraceInvoiceItemID: pgtype.Text{String: tc.InvoiceItemID, Valid: tc.InvoiceItemID != ""},
			TimerID:            tc.TimerID.String(),
			AppID:              appID.String(),
			ResolvedInvoiceID:  pgtype.Text{String: pc.InvoiceID, Valid: true},
		})
		if err != nil {
			return 0, "", err
		}
		if marked != 1 {
			return 0, "", fmt.Errorf(
				"combined proration attempt %s could not terminally mark timer %s",
				appID, tc.TimerID,
			)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", err
	}
	return ProrationLockedCharged, pc.InvoiceID, nil
}

func validateProrationChargeMatchesAttempt(attempt CombinedProrationAttempt, pc *ProrationCharge) error {
	if pc == nil {
		return errors.New("combined proration terminal payload required")
	}
	expectedCents := attempt.Shape.BaseChargeCents
	timerCount := int64(len(attempt.TimerIDs))
	if timerCount > 0 {
		if attempt.Shape.ModuleChargeCents > (math.MaxInt64-expectedCents)/timerCount {
			return errors.New("combined proration frozen cents overflow")
		}
		expectedCents += attempt.Shape.ModuleChargeCents * timerCount
	}
	sameSnapshot := func(got AppBaseSnapshot, want AppBaseSnapshot) bool {
		return got.AppID == want.AppID &&
			got.PeriodStart.Equal(want.PeriodStart) &&
			got.PeriodEnd.Equal(want.PeriodEnd) &&
			got.ModuleCount == want.ModuleCount &&
			got.BaseMicros == want.BaseMicros
	}
	switch {
	case pc.InvoiceID == "":
		return errors.New("combined proration invoice id required")
	case pc.Cents != attempt.Shape.BaseChargeCents:
		return fmt.Errorf("combined proration base cents %d do not match frozen %d", pc.Cents, attempt.Shape.BaseChargeCents)
	case pc.Invoice.StripeInvoiceID != pc.InvoiceID:
		return errors.New("combined proration invoice mirror id disagrees with terminal id")
	case pc.Invoice.AccountID != attempt.Shape.AccountID:
		return errors.New("combined proration invoice account disagrees with frozen account")
	case pc.Invoice.AmountDueCents != expectedCents:
		return fmt.Errorf("combined proration invoice amount %d does not match frozen %d", pc.Invoice.AmountDueCents, expectedCents)
	case pc.Invoice.Currency != attempt.Shape.Currency:
		return errors.New("combined proration invoice currency disagrees with frozen currency")
	case !pc.Invoice.PeriodStart.Equal(attempt.Shape.CoverageStart) ||
		!pc.Invoice.PeriodEnd.Equal(attempt.Shape.CoverageEnd):
		return errors.New("combined proration invoice period disagrees with frozen coverage")
	case !sameSnapshot(pc.Snapshot, attempt.Shape.Snapshot):
		return errors.New("combined proration primary snapshot disagrees with frozen snapshot")
	case (pc.StraddleSnapshot == nil) != (attempt.Shape.StraddleSnapshot == nil):
		return errors.New("combined proration straddle snapshot presence disagrees with frozen snapshot")
	case pc.StraddleSnapshot != nil &&
		!sameSnapshot(*pc.StraddleSnapshot, *attempt.Shape.StraddleSnapshot):
		return errors.New("combined proration straddle snapshot disagrees with frozen snapshot")
	}
	return nil
}

func (s *pgxStore) ChargeProrationLocked(ctx context.Context, appID uuid.UUID, charge func(AppMirror) (*ProrationCharge, error)) (ProrationOutcome, string, error) {
	// Phase 1: lock just long enough to read + verify chargeable state, then
	// release — never held across the Stripe call below.
	locked, outcome, invID, proceed, err := s.lockAndReadChargeableApp(ctx, appID)
	if err != nil {
		return 0, "", err
	}
	if !proceed {
		return outcome, invID, nil
	}

	// Phase 2: the Stripe network calls, OUTSIDE any lock or transaction.
	pc, err := charge(locked)
	if err != nil {
		return 0, "", err // guard unarmed → the next sweep retries (idem keys)
	}
	if pc == nil {
		return ProrationLockedNoCharge, "", nil // 0 cents — nothing to invoice
	}

	// Phase 3: persist the successful charge.
	return s.persistProrationCharge(ctx, appID, pc)
}

// DrawCreationProrationFromWallet — see the Store interface doc. The draw and the
// guard-arm share ONE transaction (no Stripe network call to keep outside a lock,
// unlike ChargeProrationLocked), so idempotency is the atomic guard alone: a
// committed settlement short-circuits every retry at the proration_invoice_id
// re-check, and a crash before commit rolls back leaving no ledger rows.
func (s *pgxStore) DrawCreationProrationFromWallet(ctx context.Context, appID uuid.UUID, pc ProrationWalletCharge) (ProrationOutcome, string, error) {
	if pc.AmountMicros <= 0 {
		return ProrationLockedNoCharge, "", nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, "", err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	// Phase 1: lock + re-verify the app is still chargeable — the SAME terminal
	// checks as lockAndReadChargeableApp, but in THIS transaction so the draw and
	// the guard-arm are atomic.
	row, err := qtx.SelectAppMirrorForUpdate(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return ProrationLockedNotFound, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	if row.DeletedAt.Valid && row.DeletedAt.Time.Before(moduleGraceExpiry(row.CreatedAt.UTC())) {
		return ProrationLockedDeleted, "", nil
	}
	if row.ProrationInvoiceID.Valid {
		return ProrationLockedAlreadyCharged, row.ProrationInvoiceID.String, nil
	}
	if row.ProrationAttemptedAt.Valid {
		// A prior attempt already reached the Stripe leg (stamped attempted before its
		// network call). Never draw the wallet beside money that may have moved — defer to
		// the Stripe recovery path, which reconciles idempotently by ms_charge_ref.
		return ProrationWalletDeferToStripe, "", nil
	}
	accountID := uuidFromPg(row.AccountID)

	// Phase 2: allocate the draw under the wallet account + ledger locks. The
	// account FOR UPDATE also serializes concurrent child ledger INSERTs through
	// the FK, exactly as DrawWalletCredits relies on. (App-row lock is taken FIRST,
	// then the account lock — a consistent order that never inverts the account→
	// ledger order the boundary draw uses, so the two can not deadlock.)
	rawMode, err := qtx.LockWalletAccount(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	mode, err := parseCreditBillingMode(rawMode)
	if err != nil {
		return 0, "", err
	}
	if mode != CreditBillingModeCredits {
		// The caller selected the wallet rail from an unlocked snapshot. A
		// concurrent credits→standard change may have committed before this
		// account lock. The locked mode is authoritative: standard mid-period
		// charges belong wholly to Stripe, even when spendable lots could cover
		// them. Return before any ledger read/write so the rails cannot split.
		return ProrationWalletDeferToStripe, "", nil
	}
	if _, err := qtx.LockWalletLedgerEntries(ctx, accountID.String()); err != nil {
		return 0, "", err
	}
	balanceAfter, err := qtx.WalletSettledBalance(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	lots, err := qtx.WalletSpendableLots(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	for _, lot := range lots {
		if lot.RemainingMicros <= 0 {
			return 0, "", fmt.Errorf(
				"wallet query returned a non-positive lot remainder: source=%s remaining=%d",
				lot.ID, lot.RemainingMicros,
			)
		}
	}

	insertDraw := func(consume int64, sourceID string) error {
		if consume <= 0 {
			return fmt.Errorf("wallet draw allocation must be positive: %d", consume)
		}
		if balanceAfter < math.MinInt64+consume {
			return fmt.Errorf("wallet balance_after_micros underflow: balance=%d draw=%d", balanceAfter, consume)
		}
		balanceAfter -= consume

		source := pgtype.UUID{}
		keySource := "unsecured"
		if sourceID != "" {
			id, err := uuid.Parse(sourceID)
			if err != nil {
				return fmt.Errorf("parse wallet source credit id: %w", err)
			}
			source = pgtype.UUID{Bytes: id, Valid: true}
			keySource = id.String()
		}
		return qtx.InsertCreationWalletDraw(ctx, db.InsertCreationWalletDrawParams{
			AccountID:          accountID.String(),
			AmountMicros:       consume,
			BalanceAfterMicros: balanceAfter,
			// Per-APP idempotency (period_id is NULL) — the deterministic
			// app/source key is the sole idempotency guard for a per-app draw.
			IdempotencyKey: fmt.Sprintf(
				"wallet-draw:app-creation:%s:usage_draw:%s", appID.String(), keySource,
			),
			SourceCreditID: source,
		})
	}

	left := pc.AmountMicros
	for _, lot := range lots {
		if left == 0 {
			break
		}
		consume := lot.RemainingMicros
		if consume > left {
			consume = left
		}
		if err := insertDraw(consume, lot.ID); err != nil {
			return 0, "", err
		}
		left -= consume
	}
	if left > 0 {
		// Credits mode is wallet-only: its configured credit policy owns the
		// unsecured remainder (the single NULL-source row).
		if err := insertDraw(left, ""); err != nil {
			return 0, "", err
		}
		left = 0
	}

	// Phase 3: fully covered — freeze the display snapshot(s) and arm the one-shot
	// guard, all in this same transaction.
	if err := qtx.UpsertProrationBaseSnapshot(ctx, db.UpsertProrationBaseSnapshotParams{
		AppID:       pc.Snapshot.AppID.String(),
		PeriodStart: pc.Snapshot.PeriodStart,
		PeriodEnd:   pc.Snapshot.PeriodEnd,
		ModuleCount: int32(pc.Snapshot.ModuleCount), //nolint:gosec // count comes from the locked apps row, whose writers validate 0 ≤ count ≤ maxModuleCount
		BaseMicros:  pc.Snapshot.BaseMicros,
	}); err != nil {
		return 0, "", err
	}
	if pc.StraddleSnapshot != nil {
		if err := qtx.UpsertProrationBaseSnapshot(ctx, db.UpsertProrationBaseSnapshotParams{
			AppID:       pc.StraddleSnapshot.AppID.String(),
			PeriodStart: pc.StraddleSnapshot.PeriodStart,
			PeriodEnd:   pc.StraddleSnapshot.PeriodEnd,
			ModuleCount: int32(pc.StraddleSnapshot.ModuleCount), //nolint:gosec // same validated apps-row count
			BaseMicros:  pc.StraddleSnapshot.BaseMicros,
		}); err != nil {
			return 0, "", err
		}
	}
	// Arm the one-shot guard (first-write-wins WHERE proration_invoice_id IS NULL —
	// guaranteed NULL under this lock by the re-check above).
	if _, err := qtx.SetAppProrationInvoice(ctx, db.SetAppProrationInvoiceParams{
		AppID:              appID.String(),
		ProrationInvoiceID: pgtype.Text{String: pc.Ref, Valid: true},
	}); err != nil {
		return 0, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", err
	}
	return ProrationLockedCharged, pc.Ref, nil
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

func (s *pgxStore) SetAppProrationSkipped(ctx context.Context, appID uuid.UUID) error {
	// 0 rows = already marked, or already charged in the meantime — neither is
	// an error: the marker is a one-shot, first-write-wins terminal state.
	_, err := s.q.SetAppProrationSkipped(ctx, appID.String())
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

func (s *pgxStore) LiveAppsCreatedBefore(ctx context.Context, accountID uuid.UUID, createdBefore time.Time, graceDays int) ([]AppModuleCount, error) {
	rows, err := s.q.LiveAppModuleCountsCreatedBefore(ctx, db.LiveAppModuleCountsCreatedBeforeParams{
		AccountID:     accountID.String(),
		CreatedBefore: createdBefore,
		// hours, not days (wave 2, D5): keeps the SQL cutoff identical to the Go
		// legs' fixed 24h-per-day UTC grace regardless of the session timezone.
		GraceHours: int32(graceDays) * 24, //nolint:gosec // graceDays is the small GraceDays const (3)
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

// --- per-module-instance overage timers (migration 033) --------------------

func (s *pgxStore) LiveModuleTimerCountForApp(ctx context.Context, appID uuid.UUID) (int, error) {
	n, err := s.q.LiveModuleTimerCountForApp(ctx, appID.String())
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *pgxStore) InsertModuleOverageTimers(ctx context.Context, accountID, appID uuid.UUID, installedAt, graceExpiresAt time.Time, n int) error {
	if n <= 0 {
		return nil // generate_series(1, 0) would be a no-op anyway; skip the round-trip
	}
	return s.q.InsertModuleOverageTimers(ctx, db.InsertModuleOverageTimersParams{
		AccountID:      accountID.String(),
		AppID:          appID.String(),
		InstalledAt:    installedAt,
		GraceExpiresAt: graceExpiresAt,
		Count:          int32(n), //nolint:gosec // n = a module_count delta, bounded by maxModuleCount (100000), far below int32 max
	})
}

func (s *pgxStore) ArmModuleTimerStripeCharge(ctx context.Context, timerID uuid.UUID, at time.Time) (StripeChargeClaim, error) {
	row, err := s.q.ArmModuleTimerStripeCharge(ctx, db.ArmModuleTimerStripeChargeParams{
		TimerID: timerID.String(), AttemptedAt: at,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return StripeChargeClaim{Outcome: StripeRailStale}, nil
	}
	if err != nil {
		return StripeChargeClaim{}, err
	}
	claim, err := stripeChargeClaim(row.FundingAccountID, row.FundingGeneration, row.StripeCustomerID)
	if err != nil {
		return StripeChargeClaim{}, err
	}
	switch {
	case !row.HasUsablePaymentMethod:
		claim.Outcome = StripeRailNoPaymentMethod
	case !row.Armed:
		claim.Outcome = StripeRailStale
	default:
		claim.Outcome = StripeRailClaimed
	}
	return claim, nil
}

func (s *pgxStore) ModuleTimerStillPending(ctx context.Context, timerID uuid.UUID) (bool, error) {
	pending, err := s.q.ModuleTimerStillPending(ctx, timerID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // the row vanished — certainly not pending
	}
	return pending, err
}

func (s *pgxStore) MarkAppProrationAttempted(ctx context.Context, appID uuid.UUID, at time.Time) error {
	return s.q.MarkAppProrationAttempted(ctx, db.MarkAppProrationAttemptedParams{
		AppID:                appID.String(),
		ProrationAttemptedAt: pgtype.Timestamptz{Time: at, Valid: true},
	})
}

func readCombinedProrationAttempt(ctx context.Context, q *db.Queries, appID uuid.UUID) (CombinedProrationAttempt, bool, error) {
	row, err := q.SelectCombinedProrationAttempt(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return CombinedProrationAttempt{}, false, nil
	}
	if err != nil {
		return CombinedProrationAttempt{}, false, err
	}
	timerRows, err := q.CombinedProrationAttemptTimerIDs(ctx, appID.String())
	if err != nil {
		return CombinedProrationAttempt{}, false, err
	}
	if int(row.TimerCount) != len(timerRows) {
		return CombinedProrationAttempt{}, false, fmt.Errorf(
			"combined proration attempt %s declares %d timers but has %d child rows",
			appID, row.TimerCount, len(timerRows),
		)
	}
	accountID, err := uuid.Parse(row.AccountID)
	if err != nil {
		return CombinedProrationAttempt{}, false, err
	}
	if !row.ChargeFundingAccountID.Valid || !row.ChargeFundingGeneration.Valid {
		return CombinedProrationAttempt{}, false, fmt.Errorf(
			"combined proration attempt %s has no pinned funding authorization", appID,
		)
	}
	timerIDs := make([]uuid.UUID, 0, len(timerRows))
	for _, raw := range timerRows {
		timerID, err := uuid.Parse(raw)
		if err != nil {
			return CombinedProrationAttempt{}, false, err
		}
		timerIDs = append(timerIDs, timerID)
	}
	attempt := CombinedProrationAttempt{
		AppID:                   appID,
		AttemptedAt:             row.AttemptedAt,
		ChargeFundingAccountID:  uuidFromPg(row.ChargeFundingAccountID),
		ChargeFundingGeneration: uuidFromPg(row.ChargeFundingGeneration),
		Shape: CombinedProrationChargeShape{
			AccountID:          accountID,
			Currency:           row.Currency,
			BaseChargeMicros:   row.BaseChargeMicros,
			BaseChargeCents:    row.BaseChargeCents,
			ModuleChargeMicros: row.ModuleChargeMicros,
			ModuleChargeCents:  row.ModuleChargeCents,
			CoverageStart:      row.CoverageStart,
			CoverageEnd:        row.CoverageEnd,
			BaseDescription:    row.BaseDescription,
			ModuleDescription:  row.ModuleDescription,
			Snapshot: AppBaseSnapshot{
				AppID:       appID,
				PeriodStart: row.SnapshotPeriodStart,
				PeriodEnd:   row.SnapshotPeriodEnd,
				ModuleCount: int(row.SnapshotModuleCount),
				BaseMicros:  row.SnapshotBaseMicros,
			},
		},
		TimerIDs: timerIDs,
	}
	if row.StraddlePeriodStart.Valid || row.StraddlePeriodEnd.Valid || row.StraddleBaseMicros.Valid {
		if !row.StraddlePeriodStart.Valid || !row.StraddlePeriodEnd.Valid || !row.StraddleBaseMicros.Valid {
			return CombinedProrationAttempt{}, false, fmt.Errorf(
				"combined proration attempt %s has an incomplete straddle snapshot",
				appID,
			)
		}
		attempt.Shape.StraddleSnapshot = &AppBaseSnapshot{
			AppID:       appID,
			PeriodStart: row.StraddlePeriodStart.Time,
			PeriodEnd:   row.StraddlePeriodEnd.Time,
			ModuleCount: int(row.SnapshotModuleCount),
			BaseMicros:  row.StraddleBaseMicros.Int64,
		}
	}
	if row.ResolvedAt.Valid || row.ResolvedInvoiceID.Valid {
		if !row.ResolvedAt.Valid || !row.ResolvedInvoiceID.Valid || row.ResolvedInvoiceID.String == "" {
			return CombinedProrationAttempt{}, false, fmt.Errorf(
				"combined proration attempt %s has an incomplete terminal state",
				appID,
			)
		}
		attempt.ResolvedAt = row.ResolvedAt.Time
		attempt.ResolvedInvoiceID = row.ResolvedInvoiceID.String
	}
	return attempt, true, nil
}

func (s *pgxStore) CombinedProrationAttempt(ctx context.Context, appID uuid.UUID) (CombinedProrationAttempt, bool, error) {
	return readCombinedProrationAttempt(ctx, s.q, appID)
}

func validateCombinedProrationShape(appID, accountID uuid.UUID, shape CombinedProrationChargeShape) error {
	switch {
	case shape.AccountID != accountID:
		return fmt.Errorf("combined proration shape account %s does not match app account %s", shape.AccountID, accountID)
	case shape.Currency == "":
		return errors.New("combined proration currency required")
	case shape.BaseChargeMicros <= 0:
		return errors.New("combined proration base micros must be positive")
	case shape.BaseChargeCents <= 0:
		return errors.New("combined proration base cents must be positive")
	case shape.ModuleChargeMicros < 0 || shape.ModuleChargeCents < 0:
		return errors.New("combined proration module amount cannot be negative")
	case (shape.ModuleChargeMicros == 0) != (shape.ModuleChargeCents == 0):
		return errors.New("combined proration module micros and cents must both be zero or both be positive")
	case shape.CoverageStart.IsZero() || !shape.CoverageEnd.After(shape.CoverageStart):
		return errors.New("combined proration coverage window is invalid")
	case shape.BaseDescription == "" || shape.ModuleDescription == "":
		return errors.New("combined proration descriptions required")
	case shape.Snapshot.AppID != appID:
		return fmt.Errorf("combined proration snapshot app %s does not match %s", shape.Snapshot.AppID, appID)
	case shape.Snapshot.PeriodStart.IsZero() || !shape.Snapshot.PeriodEnd.After(shape.Snapshot.PeriodStart):
		return errors.New("combined proration primary snapshot window is invalid")
	case shape.Snapshot.ModuleCount < 0 || shape.Snapshot.ModuleCount > maxModuleCount:
		return errors.New("combined proration snapshot module count is invalid")
	case shape.Snapshot.BaseMicros < 0:
		return errors.New("combined proration snapshot micros cannot be negative")
	}
	if shape.StraddleSnapshot != nil {
		switch {
		case shape.StraddleSnapshot.AppID != appID:
			return fmt.Errorf("combined proration straddle snapshot app %s does not match %s", shape.StraddleSnapshot.AppID, appID)
		case shape.StraddleSnapshot.PeriodStart.IsZero() ||
			!shape.StraddleSnapshot.PeriodEnd.After(shape.StraddleSnapshot.PeriodStart):
			return errors.New("combined proration straddle snapshot window is invalid")
		case shape.StraddleSnapshot.ModuleCount != shape.Snapshot.ModuleCount:
			return errors.New("combined proration snapshots disagree on module count")
		case shape.StraddleSnapshot.BaseMicros <= 0:
			return errors.New("combined proration straddle snapshot micros must be positive")
		}
	}
	return nil
}

func (s *pgxStore) FreezeCombinedProrationAttempt(
	ctx context.Context,
	appID uuid.UUID,
	at time.Time,
	shape CombinedProrationChargeShape,
	creditRailEnabled bool,
) (CombinedProrationAttempt, StripeRailClaimOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	// Enforced wallet mode makes this freeze the atomic Stripe-rail claim too.
	// Resolve the account without a lock, then take the durable account mode
	// lock BEFORE advisory→app→timers. A standard→credits transition takes the
	// same account lock, so exactly one rail wins before any marker/network call.
	// When the feature/schema capability is off, preserve the exact legacy path:
	// no accounts.billing_mode read or account row lock at all.
	var probedAccountID uuid.UUID
	mode := CreditBillingModeStandard
	if creditRailEnabled {
		probe, err := qtx.SelectAppMirror(ctx, appID.String())
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && !probe.AccountID.Valid) {
			return CombinedProrationAttempt{}, StripeRailStale, nil
		}
		if err != nil {
			return CombinedProrationAttempt{}, StripeRailStale, err
		}
		probedAccountID = uuidFromPg(probe.AccountID)
		rawMode, err := qtx.LockWalletAccount(ctx, probedAccountID.String())
		if err != nil {
			return CombinedProrationAttempt{}, StripeRailStale, err
		}
		mode, err = parseCreditBillingMode(rawMode)
		if err != nil {
			return CombinedProrationAttempt{}, StripeRailStale, err
		}
	}

	// Match every existing timer-set writer after the optional account lock:
	// advisory timer-set lock, then app row. The advisory lock prevents a
	// same-app reconcile insert/remove from escaping selected-row rechecks.
	if err := lockModuleTimers(ctx, tx, appID); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	// The app row is the first-write serialization point between freezers. A
	// concurrent freezer waits here, then observes and returns the winner.
	appRow, err := qtx.SelectAppMirrorForUpdate(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return CombinedProrationAttempt{}, StripeRailStale, nil
	}
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	accountID := uuidFromPg(appRow.AccountID)
	if accountID == uuid.Nil {
		return CombinedProrationAttempt{}, StripeRailStale, nil
	}
	if creditRailEnabled && accountID != probedAccountID {
		return CombinedProrationAttempt{}, StripeRailStale, fmt.Errorf(
			"app account changed during combined Stripe claim: before=%s locked=%s",
			probedAccountID, accountID,
		)
	}
	if existing, found, err := readCombinedProrationAttempt(ctx, qtx, appID); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	} else if found {
		if !appRow.ProrationAttemptedAt.Valid ||
			existing.Shape.AccountID != accountID ||
			(existing.ResolvedInvoiceID == "" && (appRow.ProrationInvoiceID.Valid || appRow.ProrationSkippedAt.Valid)) ||
			(existing.ResolvedInvoiceID != "" &&
				(!appRow.ProrationInvoiceID.Valid || appRow.ProrationInvoiceID.String != existing.ResolvedInvoiceID)) {
			return CombinedProrationAttempt{}, StripeRailStale, fmt.Errorf(
				"combined proration attempt %s disagrees with the app marker/terminal guard",
				appID,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return CombinedProrationAttempt{}, StripeRailStale, err
		}
		// Recovery is authoritative even if the account transitioned to credits
		// after the first standard-rail claim. Money may already have moved.
		return existing, StripeRailClaimed, nil
	}

	// A marker without a migration-050 header cannot prove which timer lines
	// landed. Never reconstruct pseudo-history from current FIFO state.
	if appRow.ProrationAttemptedAt.Valid {
		return CombinedProrationAttempt{}, StripeRailStale, ErrCombinedProrationAttemptUnknown
	}
	if appRow.ProrationInvoiceID.Valid ||
		appRow.ProrationSkippedAt.Valid ||
		(appRow.DeletedAt.Valid && appRow.DeletedAt.Time.Before(moduleGraceExpiry(appRow.CreatedAt.UTC()))) {
		return CombinedProrationAttempt{}, StripeRailStale, nil
	}
	if err := validateCombinedProrationShape(appID, accountID, shape); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if creditRailEnabled && mode == CreditBillingModeCredits {
		// No header, children, or legacy app marker have been written. The caller
		// can safely retry the full charge through the wallet transaction.
		return CombinedProrationAttempt{}, StripeRailWalletRequired, nil
	}
	fundingAuth, err := qtx.StripeFundingAuthorization(ctx, accountID.String())
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if !fundingAuth.HasUsablePaymentMethod {
		return CombinedProrationAttempt{}, StripeRailNoPaymentMethod, nil
	}
	if fundingAuth.StripeCustomerID == "" {
		return CombinedProrationAttempt{}, StripeRailStale,
			errors.New("combined proration funder has a usable PM but no Stripe customer id")
	}

	var selected []string
	if shape.ModuleChargeMicros > 0 && shape.ModuleChargeCents > 0 {
		selected, err = qtx.CoCreatedOverModuleTimersForAttempt(ctx, db.CoCreatedOverModuleTimersForAttemptParams{
			AccountID:       accountID.String(),
			AppID:           appID.String(),
			CreatedAt:       appRow.CreatedAt,
			IncludedModules: int32(usage.IncludedModules),
		})
		if err != nil {
			return CombinedProrationAttempt{}, StripeRailStale, err
		}
	}

	// Lock + recheck every selected timer before the header exists. If a
	// standalone marker, resolution, or removal won after the selection
	// statement, rollback the entire freeze; a later retry selects coherently.
	lockedTimers, err := qtx.LockCombinedProrationCandidateTimers(ctx, selected)
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if len(lockedTimers) != len(selected) {
		return CombinedProrationAttempt{}, StripeRailStale, ErrCombinedProrationSelectionChanged
	}
	selectedSet := make(map[string]struct{}, len(selected))
	for _, timerID := range selected {
		selectedSet[timerID] = struct{}{}
	}
	for _, timer := range lockedTimers {
		if _, ok := selectedSet[timer.ID]; !ok ||
			timer.AccountID != accountID.String() ||
			timer.AppID != appID.String() ||
			!timer.InstalledAt.Equal(appRow.CreatedAt) ||
			timer.RemovedAt.Valid ||
			timer.GraceResolved ||
			timer.ChargeAttemptedAt.Valid {
			return CombinedProrationAttempt{}, StripeRailStale, ErrCombinedProrationSelectionChanged
		}
	}

	var straddleStart, straddleEnd pgtype.Timestamptz
	var straddleMicros pgtype.Int8
	if shape.StraddleSnapshot != nil {
		straddleStart = pgtype.Timestamptz{Time: shape.StraddleSnapshot.PeriodStart, Valid: true}
		straddleEnd = pgtype.Timestamptz{Time: shape.StraddleSnapshot.PeriodEnd, Valid: true}
		straddleMicros = pgtype.Int8{Int64: shape.StraddleSnapshot.BaseMicros, Valid: true}
	}
	if len(selected) > math.MaxInt32 {
		return CombinedProrationAttempt{}, StripeRailStale, errors.New("combined proration timer count overflows int32")
	}
	if at.IsZero() {
		return CombinedProrationAttempt{}, StripeRailStale, errors.New("combined proration attempted_at required")
	}
	at = at.UTC()
	if err := qtx.InsertCombinedProrationAttempt(ctx, db.InsertCombinedProrationAttemptParams{
		AppID:                   appID.String(),
		AccountID:               accountID.String(),
		ChargeFundingAccountID:  fundingAuth.FundingAccountID,
		ChargeFundingGeneration: fundingAuth.Generation,
		AttemptedAt:             at,
		Currency:                shape.Currency,
		BaseChargeMicros:        shape.BaseChargeMicros,
		BaseChargeCents:         shape.BaseChargeCents,
		ModuleChargeMicros:      shape.ModuleChargeMicros,
		ModuleChargeCents:       shape.ModuleChargeCents,
		TimerCount:              int32(len(selected)), //nolint:gosec // checked against MaxInt32 above
		CoverageStart:           shape.CoverageStart,
		CoverageEnd:             shape.CoverageEnd,
		BaseDescription:         shape.BaseDescription,
		ModuleDescription:       shape.ModuleDescription,
		SnapshotPeriodStart:     shape.Snapshot.PeriodStart,
		SnapshotPeriodEnd:       shape.Snapshot.PeriodEnd,
		SnapshotBaseMicros:      shape.Snapshot.BaseMicros,
		SnapshotModuleCount:     int32(shape.Snapshot.ModuleCount), //nolint:gosec // validated ≤ maxModuleCount
		StraddlePeriodStart:     straddleStart,
		StraddlePeriodEnd:       straddleEnd,
		StraddleBaseMicros:      straddleMicros,
	}); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if err := qtx.InsertCombinedProrationAttemptTimers(ctx, db.InsertCombinedProrationAttemptTimersParams{
		AppID:    appID.String(),
		TimerIds: selected,
	}); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	stamped, err := qtx.MarkAppProrationAttemptedWithFreeze(ctx, db.MarkAppProrationAttemptedWithFreezeParams{
		AttemptedAt: at,
		AppID:       appID.String(),
	})
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if stamped != 1 {
		return CombinedProrationAttempt{}, StripeRailStale, errors.New("combined proration freeze could not stamp app attempt marker")
	}
	attempt, found, err := readCombinedProrationAttempt(ctx, qtx, appID)
	if err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	if !found {
		return CombinedProrationAttempt{}, StripeRailStale, errors.New("combined proration attempt missing immediately after insert")
	}
	if err := tx.Commit(ctx); err != nil {
		return CombinedProrationAttempt{}, StripeRailStale, err
	}
	return attempt, StripeRailClaimed, nil
}

func (s *pgxStore) UnresolvedCombinedProrationAttempts(ctx context.Context, accountID uuid.UUID) ([]UnresolvedCombinedProrationAmount, error) {
	rows, err := s.q.UnresolvedCombinedProrationAttempts(ctx, accountID.String())
	if err != nil {
		return nil, err
	}
	out := make([]UnresolvedCombinedProrationAmount, 0, len(rows))
	for _, row := range rows {
		if int64(row.TimerCount) != row.FrozenTimerCount {
			return nil, fmt.Errorf(
				"combined proration attempt %s declares %d timers but projection found %d",
				row.AppID, row.TimerCount, row.FrozenTimerCount,
			)
		}
		appID, err := uuid.Parse(row.AppID)
		if err != nil {
			return nil, err
		}
		timerCount := int64(row.TimerCount)
		if timerCount > 0 && row.ModuleChargeMicros > (math.MaxInt64-row.BaseChargeMicros)/timerCount {
			return nil, fmt.Errorf("combined proration attempt %s raw amount overflows int64", row.AppID)
		}
		out = append(out, UnresolvedCombinedProrationAmount{
			AppID:              appID,
			BaseChargeMicros:   row.BaseChargeMicros,
			ModuleChargeMicros: row.ModuleChargeMicros,
			TimerCount:         int(row.TimerCount),
			TotalMicros:        row.BaseChargeMicros + row.ModuleChargeMicros*timerCount,
		})
	}
	return out, nil
}

func (s *pgxStore) TimerHasUnresolvedCombinedProrationOwner(ctx context.Context, timerID uuid.UUID) (bool, error) {
	return s.q.TimerHasUnresolvedCombinedProrationOwner(ctx, timerID.String())
}

// lockModuleTimers takes the per-app advisory xact lock every timer-set writer
// serializes on. Released automatically on commit/rollback.
func lockModuleTimers(ctx context.Context, tx pgx.Tx, appID uuid.UUID) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "module-timers:"+appID.String())
	return err
}

// ReconcileModuleTimersToTarget — roster read + count + write in ONE
// transaction serialized by the per-app advisory xact lock, so concurrent
// RegisterApp/SyncAppModules retries can never both observe the same live
// count and double-insert (H7), a stale caller can never impose an outdated
// target (D8 — the target is the row's CURRENT module_count, read under the
// lock), and a deleted row reconciles to zero instead of resurrecting (D9).
func (s *pgxStore) ReconcileModuleTimersToTarget(ctx context.Context, appID uuid.UUID, installedAt, graceExpiresAt, removedAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	if err := lockModuleTimers(ctx, tx, appID); err != nil {
		return err
	}
	row, err := qtx.SelectAppMirror(ctx, appID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // no roster row — nothing to reconcile against
	}
	if err != nil {
		return err
	}
	target := int64(row.ModuleCount)
	if row.DeletedAt.Valid {
		target = 0 // deleted apps hold no live timers — remove orphans, never insert
	}
	live, err := qtx.LiveModuleTimerCountForApp(ctx, appID.String())
	if err != nil {
		return err
	}
	switch {
	case target > live:
		// An UNBILLED org roster row (NULL account, migration 041) synthesizes
		// no timers — app_module_overage_timers.account_id is NOT NULL and there
		// is no account to tier on. The RepointOrgUsage attach sweep reconciles
		// the app again once account_id is backfilled, anchoring fresh timers at
		// the designation instant (prospective billing, org-billing D1). Shrinks
		// and removals below still run (they key on app_id only).
		if !row.AccountID.Valid {
			return tx.Commit(ctx)
		}
		if err := qtx.InsertModuleOverageTimers(ctx, db.InsertModuleOverageTimersParams{
			AccountID:      uuidFromPg(row.AccountID).String(),
			AppID:          appID.String(),
			InstalledAt:    installedAt,
			GraceExpiresAt: graceExpiresAt,
			Count:          int32(target - live), //nolint:gosec // bounded by maxModuleCount (100000)
		}); err != nil {
			return err
		}
	case target < live:
		if err := qtx.SoftRemoveNewestModuleTimers(ctx, db.SoftRemoveNewestModuleTimersParams{
			AppID:      appID.String(),
			RemovedAt:  removedAt,
			LimitCount: int32(live - target), //nolint:gosec // bounded by maxModuleCount (100000)
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// MarkAppDeletedAndRemoveTimers — the deletion write and the timer soft-remove
// in ONE transaction under the SAME advisory lock (wave 2, D9): no crash
// window between them, and no interleaving with a concurrent reconcile.
func (s *pgxStore) MarkAppDeletedAndRemoveTimers(ctx context.Context, appID uuid.UUID, removedAt time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	if err := lockModuleTimers(ctx, tx, appID); err != nil {
		return err
	}
	if _, err := qtx.MarkAppDeleted(ctx, appID.String()); err != nil {
		return err
	}
	if err := qtx.SoftRemoveAllModuleTimersForApp(ctx, db.SoftRemoveAllModuleTimersForAppParams{
		AppID:     appID.String(),
		RemovedAt: pgtype.Timestamptz{Time: removedAt, Valid: true},
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *pgxStore) SoftRemoveNewestModuleTimers(ctx context.Context, appID uuid.UUID, n int, removedAt time.Time) error {
	if n <= 0 {
		return nil
	}
	return s.q.SoftRemoveNewestModuleTimers(ctx, db.SoftRemoveNewestModuleTimersParams{
		AppID:      appID.String(),
		LimitCount: int32(n), //nolint:gosec // n = a module_count delta, bounded by maxModuleCount (100000), far below int32 max
		RemovedAt:  removedAt,
	})
}

func (s *pgxStore) SoftRemoveAllModuleTimersForApp(ctx context.Context, appID uuid.UUID, removedAt time.Time) error {
	return s.q.SoftRemoveAllModuleTimersForApp(ctx, db.SoftRemoveAllModuleTimersForAppParams{
		AppID:     appID.String(),
		RemovedAt: pgtype.Timestamptz{Time: removedAt, Valid: true},
	})
}

func (s *pgxStore) ModuleOverageTimersPastGrace(ctx context.Context, at time.Time) ([]ModuleOverageCandidate, error) {
	rows, err := s.q.ModuleOverageTimersPastGrace(ctx, at)
	if err != nil {
		return nil, err
	}
	out := make([]ModuleOverageCandidate, 0, len(rows))
	for _, r := range rows {
		id, err := uuid.Parse(r.ID)
		if err != nil {
			return nil, err
		}
		acct, err := uuid.Parse(r.AccountID)
		if err != nil {
			return nil, err
		}
		app, err := uuid.Parse(r.AppID)
		if err != nil {
			return nil, err
		}
		// The query filters activated_at IS NOT NULL, so a non-Valid value here is
		// a driver anomaly; skip it defensively rather than anchor on the zero time.
		if !r.ActivatedAt.Valid {
			continue
		}
		cand := ModuleOverageCandidate{
			ID:             id,
			AccountID:      acct,
			AppID:          app,
			InstalledAt:    r.InstalledAt,
			GraceExpiresAt: r.GraceExpiresAt,
			ActivatedAt:    r.ActivatedAt.Time,
		}
		if r.ChargeAttemptedAt.Valid {
			cand.ChargeAttemptedAt = r.ChargeAttemptedAt.Time
			cand.ChargeFundingAccountID = uuidFromPg(r.ChargeFundingAccountID)
			cand.ChargeFundingGeneration = uuidFromPg(r.ChargeFundingGeneration)
		}
		out = append(out, cand)
	}
	return out, nil
}

func (s *pgxStore) LiveModuleTimerRankBefore(ctx context.Context, accountID, timerID uuid.UUID, installedAt time.Time) (int, error) {
	rank, err := s.q.LiveModuleTimerRankBefore(ctx, db.LiveModuleTimerRankBeforeParams{
		AccountID:   accountID.String(),
		InstalledAt: installedAt,
		TimerID:     timerID.String(),
	})
	if err != nil {
		return 0, err
	}
	return int(rank), nil
}

func (s *pgxStore) MarkModuleTimerIncluded(ctx context.Context, timerID uuid.UUID) error {
	return s.q.MarkModuleTimerIncluded(ctx, timerID.String())
}

func (s *pgxStore) MarkModuleTimerCharged(ctx context.Context, timerID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error {
	return s.q.MarkModuleTimerCharged(ctx, db.MarkModuleTimerChargedParams{
		TimerID:            timerID.String(),
		GraceChargedAt:     chargedAt,
		GraceInvoiceID:     pgtype.Text{String: invoiceID, Valid: invoiceID != ""},
		GraceInvoiceItemID: pgtype.Text{String: invoiceItemID, Valid: invoiceItemID != ""},
	})
}

// DrawModuleOverageFromWallet — see the Store interface doc. Mirrors
// DrawCreationProrationFromWallet: the draw and the guard-arm share ONE
// transaction (no Stripe network call to keep outside a lock), so idempotency is
// the atomic grace_resolved guard alone — a committed settlement short-circuits
// every retry at the timer re-check, and a crash before commit rolls back leaving
// no ledger rows.
func (s *pgxStore) DrawModuleOverageFromWallet(ctx context.Context, timerID uuid.UUID, mc ModuleOverageWalletCharge) (ModuleOverageWalletOutcome, string, error) {
	if mc.AmountMicros <= 0 {
		return ModuleOverageWalletLockedNoCharge, "", nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, "", err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	// Phase 1: lock + re-verify the timer is still chargeable — the SAME terminal
	// checks the sweep's unlocked pre-checks make, but in THIS transaction so the
	// draw and the guard-arm are atomic.
	row, err := qtx.SelectModuleTimerForUpdate(ctx, timerID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return ModuleOverageWalletLockedStale, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	if row.RemovedAt.Valid || row.GraceResolved {
		// Removed, or resolved by a concurrent sweep between the work-list read and
		// this lock — nothing to settle (the M2 stale posture).
		return ModuleOverageWalletLockedStale, "", nil
	}
	owned, err := qtx.TimerHasUnresolvedCombinedProrationOwner(ctx, timerID.String())
	if err != nil {
		return 0, "", err
	}
	if owned {
		// The combined app attempt committed ownership while this wallet worker
		// waited on the timer row lock. It alone may settle the timer.
		return ModuleOverageWalletLockedStale, "", nil
	}
	if row.ChargeAttemptedAt.Valid {
		// A concurrent attempt already reached the Stripe leg (stamped attempted
		// before its network call). Never draw beside money that may have moved —
		// defer to Stripe, reconciled idempotently by the per-timer idem keys.
		return ModuleOverageWalletDeferToStripe, "", nil
	}
	accountID, err := uuid.Parse(row.AccountID)
	if err != nil {
		return 0, "", err
	}

	// Phase 2: allocate the draw under the wallet account + ledger locks. The SAME
	// lock order the creation draw uses (timer/app row → wallet account → ledger),
	// which never inverts the account→ledger order the boundary draw uses, so the
	// wallet legs can not deadlock.
	rawMode, err := qtx.LockWalletAccount(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	mode, err := parseCreditBillingMode(rawMode)
	if err != nil {
		return 0, "", err
	}
	if mode != CreditBillingModeCredits {
		// The caller selected the wallet rail from an unlocked snapshot. A
		// concurrent credits→standard change may have committed before this
		// account lock. The locked mode is authoritative: standard mid-period
		// charges belong wholly to Stripe, even when spendable lots could cover
		// them. Return before any ledger read/write so the rails cannot split.
		return ModuleOverageWalletDeferToStripe, "", nil
	}
	if _, err := qtx.LockWalletLedgerEntries(ctx, accountID.String()); err != nil {
		return 0, "", err
	}
	balanceAfter, err := qtx.WalletSettledBalance(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	lots, err := qtx.WalletSpendableLots(ctx, accountID.String())
	if err != nil {
		return 0, "", err
	}
	for _, lot := range lots {
		if lot.RemainingMicros <= 0 {
			return 0, "", fmt.Errorf(
				"wallet query returned a non-positive lot remainder: source=%s remaining=%d",
				lot.ID, lot.RemainingMicros,
			)
		}
	}

	insertDraw := func(consume int64, sourceID string) error {
		if consume <= 0 {
			return fmt.Errorf("wallet draw allocation must be positive: %d", consume)
		}
		if balanceAfter < math.MinInt64+consume {
			return fmt.Errorf("wallet balance_after_micros underflow: balance=%d draw=%d", balanceAfter, consume)
		}
		balanceAfter -= consume

		source := pgtype.UUID{}
		keySource := "unsecured"
		if sourceID != "" {
			id, err := uuid.Parse(sourceID)
			if err != nil {
				return fmt.Errorf("parse wallet source credit id: %w", err)
			}
			source = pgtype.UUID{Bytes: id, Valid: true}
			keySource = id.String()
		}
		return qtx.InsertCreationWalletDraw(ctx, db.InsertCreationWalletDrawParams{
			AccountID:          accountID.String(),
			AmountMicros:       consume,
			BalanceAfterMicros: balanceAfter,
			// Per-TIMER idempotency (period_id is NULL) — a module-overage draw is
			// keyed per install timer, never per billing period, so it never collides
			// with the period's boundary draw or a sibling creation draw against the
			// same funding lot. The deterministic timer/source key is the sole guard.
			IdempotencyKey: fmt.Sprintf(
				"wallet-draw:module-overage:%s:usage_draw:%s", timerID.String(), keySource,
			),
			SourceCreditID: source,
		})
	}

	left := mc.AmountMicros
	for _, lot := range lots {
		if left == 0 {
			break
		}
		consume := lot.RemainingMicros
		if consume > left {
			consume = left
		}
		if err := insertDraw(consume, lot.ID); err != nil {
			return 0, "", err
		}
		left -= consume
	}
	if left > 0 {
		// Credits mode is wallet-only: its configured credit policy owns the
		// unsecured remainder (the single NULL-source row).
		if err := insertDraw(left, ""); err != nil {
			return 0, "", err
		}
		left = 0
	}

	// Phase 3: fully covered — arm the SAME per-timer guard the Stripe leg arms,
	// with the synthetic wallet ref (grace_invoice_id) and NULL invoice-item id, in
	// this same transaction. WHERE grace_resolved = false is guaranteed true under
	// the lock by the re-check above.
	if err := qtx.MarkModuleTimerCharged(ctx, db.MarkModuleTimerChargedParams{
		TimerID:            timerID.String(),
		GraceChargedAt:     mc.ChargedAt,
		GraceInvoiceID:     pgtype.Text{String: mc.Ref, Valid: true},
		GraceInvoiceItemID: pgtype.Text{},
	}); err != nil {
		return 0, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", err
	}
	return ModuleOverageWalletLockedCharged, mc.Ref, nil
}

func (s *pgxStore) CountOngoingOverModuleTimers(ctx context.Context, accountID uuid.UUID, includedModules int, periodEnd time.Time) (int, error) {
	n, err := s.q.CountOngoingOverModuleTimers(ctx, db.CountOngoingOverModuleTimersParams{
		AccountID:       accountID.String(),
		IncludedModules: int32(includedModules), //nolint:gosec // includedModules is the small IncludedModules const (5)
		PeriodEnd:       periodEnd,
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

func (s *pgxStore) CoCreatedOverModuleTimers(ctx context.Context, accountID, appID uuid.UUID, createdAt time.Time, includedModules int) ([]uuid.UUID, error) {
	ids, err := s.q.CoCreatedOverModuleTimers(ctx, db.CoCreatedOverModuleTimersParams{
		AccountID:       accountID.String(),
		AppID:           appID.String(),
		CreatedAt:       createdAt,
		IncludedModules: int32(includedModules), //nolint:gosec // includedModules is the small IncludedModules const (5)
	})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(ids)
}

// --- custom-domain charges (migration 047) --------------------------------

func (s *pgxStore) InsertDomain(ctx context.Context, accountID, appID uuid.UUID, hostname string, activatedAt time.Time) error {
	return s.q.InsertDomain(ctx, db.InsertDomainParams{
		AccountID:   accountID.String(),
		AppID:       appID.String(),
		Hostname:    hostname,
		ActivatedAt: activatedAt,
	})
}

func (s *pgxStore) DomainByHostname(ctx context.Context, hostname string) (Domain, bool, error) {
	row, err := s.q.DomainByHostname(ctx, hostname)
	if errors.Is(err, pgx.ErrNoRows) {
		return Domain{}, false, nil
	}
	if err != nil {
		return Domain{}, false, err
	}
	id, err := uuid.Parse(row.ID)
	if err != nil {
		return Domain{}, false, err
	}
	accountID, err := uuid.Parse(row.AccountID)
	if err != nil {
		return Domain{}, false, err
	}
	appID, err := uuid.Parse(row.AppID)
	if err != nil {
		return Domain{}, false, err
	}
	return Domain{
		ID:          id,
		AccountID:   accountID,
		AppID:       appID,
		Hostname:    row.Hostname,
		ActivatedAt: row.ActivatedAt,
		Removed:     row.RemovedAt.Valid,
		RemovedAt:   row.RemovedAt.Time,
		CreatedAt:   row.CreatedAt,
	}, true, nil
}

func (s *pgxStore) RemoveDomain(ctx context.Context, appID uuid.UUID, hostname string, removedAt time.Time) error {
	return s.q.RemoveDomain(ctx, db.RemoveDomainParams{
		AppID:     appID.String(),
		Hostname:  hostname,
		RemovedAt: removedAt,
	})
}

func (s *pgxStore) DomainsPendingCharge(ctx context.Context, at time.Time) ([]DomainChargeCandidate, error) {
	rows, err := s.q.DomainsPendingCharge(ctx, at)
	if err != nil {
		return nil, err
	}
	out := make([]DomainChargeCandidate, 0, len(rows))
	for _, row := range rows {
		id, err := uuid.Parse(row.ID)
		if err != nil {
			return nil, err
		}
		accountID, err := uuid.Parse(row.AccountID)
		if err != nil {
			return nil, err
		}
		appID, err := uuid.Parse(row.AppID)
		if err != nil {
			return nil, err
		}
		// The query filters account activated_at IS NOT NULL. Skip a driver
		// anomaly defensively rather than derive periods from the zero time.
		if !row.AccountActivatedAt.Valid {
			continue
		}
		cand := DomainChargeCandidate{
			ID:                 id,
			AccountID:          accountID,
			AppID:              appID,
			Hostname:           row.Hostname,
			ActivatedAt:        row.ActivatedAt,
			AccountActivatedAt: row.AccountActivatedAt.Time,
		}
		if row.ChargeAttemptedAt.Valid {
			cand.ChargeAttemptedAt = row.ChargeAttemptedAt.Time
			cand.ChargeFundingAccountID = uuidFromPg(row.ChargeFundingAccountID)
			cand.ChargeFundingGeneration = uuidFromPg(row.ChargeFundingGeneration)
		}
		out = append(out, cand)
	}
	return out, nil
}

func (s *pgxStore) DomainStillPending(ctx context.Context, domainID uuid.UUID) (bool, error) {
	pending, err := s.q.DomainStillPending(ctx, domainID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return pending, err
}

func (s *pgxStore) ArmDomainStripeCharge(ctx context.Context, domainID uuid.UUID, at time.Time) (StripeChargeClaim, error) {
	row, err := s.q.ArmDomainStripeCharge(ctx, db.ArmDomainStripeChargeParams{
		DomainID: domainID.String(), AttemptedAt: at,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return StripeChargeClaim{Outcome: StripeRailStale}, nil
	}
	if err != nil {
		return StripeChargeClaim{}, err
	}
	claim, err := stripeChargeClaim(row.FundingAccountID, row.FundingGeneration, row.StripeCustomerID)
	if err != nil {
		return StripeChargeClaim{}, err
	}
	switch {
	case !row.HasUsablePaymentMethod:
		claim.Outcome = StripeRailNoPaymentMethod
	case !row.Armed:
		claim.Outcome = StripeRailStale
	default:
		claim.Outcome = StripeRailClaimed
	}
	return claim, nil
}

func (s *pgxStore) MarkDomainChargeResolved(ctx context.Context, domainID uuid.UUID) error {
	return s.q.MarkDomainChargeResolved(ctx, domainID.String())
}

func (s *pgxStore) MarkDomainCharged(ctx context.Context, domainID uuid.UUID, chargedAt time.Time, invoiceID, invoiceItemID string) error {
	return s.q.MarkDomainCharged(ctx, db.MarkDomainChargedParams{
		DomainID:            domainID.String(),
		ChargedAt:           chargedAt,
		ChargeInvoiceID:     pgtype.Text{String: invoiceID, Valid: invoiceID != ""},
		ChargeInvoiceItemID: pgtype.Text{String: invoiceItemID, Valid: invoiceItemID != ""},
	})
}

func (s *pgxStore) CountLiveDomainsActivatedBefore(ctx context.Context, accountID uuid.UUID, periodEnd time.Time) (int, error) {
	n, err := s.q.CountLiveDomainsActivatedBefore(ctx, db.CountLiveDomainsActivatedBeforeParams{
		AccountID: accountID.String(),
		PeriodEnd: periodEnd,
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// EnsureOrgAccount mirrors EnsureAccountForUser on the org leg: the SAME
// advisory-locked get-or-create shape, serialized on the exported org
// namespace ('lbto') because the accounts table has no owner UNIQUE
// constraint — the lock IS the uniqueness guard.
func (s *pgxStore) EnsureOrgAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: billing.AdvisoryLockNamespaceBillingAccountOrg,
			Column2: orgID.String(),
		}); err != nil {
			return err
		}
		existing, err := qtx.SelectAccountByOrg(ctx, pgtype.UUID{Bytes: orgID, Valid: true})
		if err == nil {
			id = existing.ID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		inserted, err := qtx.InsertOrgAccount(ctx, pgtype.UUID{Bytes: orgID, Valid: true})
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

func (s *pgxStore) AccountIDByUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, bool, error) {
	id, err := s.q.AccountIDByUser(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	return uuidRowFound(id, err)
}

func (s *pgxStore) OrgAccountID(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	row, err := s.q.SelectAccountByOrg(ctx, pgtype.UUID{Bytes: orgID, Valid: true})
	return uuidRowFound(row.ID, err)
}

func (s *pgxStore) OrgDesignation(ctx context.Context, orgID uuid.UUID) (OrgDesignation, bool, error) {
	row, err := s.q.GetOrgDesignation(ctx, orgID.String())
	if errors.Is(err, pgx.ErrNoRows) {
		return OrgDesignation{}, false, nil
	}
	if err != nil {
		return OrgDesignation{}, false, err
	}
	org, err := uuid.Parse(row.OrgID)
	if err != nil {
		return OrgDesignation{}, false, err
	}
	updatedBy, err := uuid.Parse(row.UpdatedBy)
	if err != nil {
		return OrgDesignation{}, false, err
	}
	return OrgDesignation{
		OrgID:                  org,
		Funding:                OrgFunding(row.Funding),
		SponsorAccountID:       uuidFromPg(row.SponsorAccountID),
		SponsorUserID:          uuidFromPg(row.SponsorUserID),
		DisclosedBacklogMicros: row.DisclosedBacklogMicros,
		UpdatedBy:              updatedBy,
	}, true, nil
}

func (s *pgxStore) UpsertOrgDesignation(ctx context.Context, d OrgDesignation) error {
	return s.q.UpsertOrgDesignation(ctx, db.UpsertOrgDesignationParams{
		OrgID:                  d.OrgID.String(),
		Funding:                string(d.Funding),
		SponsorAccountID:       pgUUIDOrNull(d.SponsorAccountID),
		SponsorUserID:          pgUUIDOrNull(d.SponsorUserID),
		DisclosedBacklogMicros: d.DisclosedBacklogMicros,
		UpdatedBy:              d.UpdatedBy.String(),
	})
}

func (s *pgxStore) DeleteOrgDesignation(ctx context.Context, orgID uuid.UUID) (bool, error) {
	rows, err := s.q.DeleteOrgDesignation(ctx, orgID.String())
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *pgxStore) ResolveOrgFundedAccount(ctx context.Context, orgID uuid.UUID) (uuid.UUID, bool, error) {
	// ErrNoRows = no designation, or not yet activated — unbilled, normal.
	id, err := s.q.ResolveOrgFundedAccount(ctx, orgID.String())
	return uuidRowFound(id, err)
}

func (s *pgxStore) OrgsWithUnsweptUsage(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := s.q.OrgsWithUnsweptUsage(ctx)
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) ActivateAccountIfUnset(ctx context.Context, accountID uuid.UUID, at time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)

	// The UPDATE holds the account row exclusively. v2 ingest and rollup both
	// take FOR SHARE, making this transition atomic with the rewindow below:
	// observations admitted before activation are visible here, while later
	// admissions derive the immutable activated anchor.
	rows, err := qtx.ActivateAccountIfUnset(ctx, db.ActivateAccountIfUnsetParams{
		ID:          accountID.String(),
		ActivatedAt: pgtype.Timestamptz{Time: at.UTC(), Valid: true},
	})
	if err != nil {
		return err
	}
	if rows > 0 {
		firstFundedStart, _ := billingperiod.AnchoredPeriodWindow(at, billingperiod.AnchorDay(at))
		if _, err := qtx.RewindowAccountV2UsageAtActivation(ctx, db.RewindowAccountV2UsageAtActivationParams{
			FirstFundedStart: firstFundedStart,
			AccountID:        accountID.String(),
		}); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *pgxStore) OrgIsDistributor(ctx context.Context, orgID uuid.UUID) (bool, error) {
	return s.q.OrgIsDistributor(ctx, orgID.String())
}

func (s *pgxStore) OrgDistributor(ctx context.Context, orgID uuid.UUID) (uuid.UUID, string, bool, error) {
	row, err := s.q.GetOrgDistributor(ctx, orgID.String())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return uuid.Nil, "", false, nil
		}
		return uuid.Nil, "", false, err
	}
	id, err := uuid.Parse(row.DistributorOrgID)
	if err != nil {
		return uuid.Nil, "", false, err
	}
	return id, row.Source, true, nil
}

func (s *pgxStore) OrgUnbilledBacklogMicros(ctx context.Context, orgID uuid.UUID) (int64, error) {
	n, err := s.q.OrgUnbilledBacklogMicros(ctx, pgtype.UUID{Bytes: orgID, Valid: true})
	if err != nil {
		return 0, err
	}
	// Same single decode-and-round point as every live money read.
	return usage.MicrosFromNumeric(n)
}

func (s *pgxStore) AttachOrgAppsToAccount(ctx context.Context, orgID, accountID uuid.UUID) (int64, error) {
	return s.q.AttachOrgAppsToAccount(ctx, db.AttachOrgAppsToAccountParams{
		OwnerOrgID: pgtype.UUID{Bytes: orgID, Valid: true},
		AccountID:  pgtype.UUID{Bytes: accountID, Valid: true},
	})
}

func (s *pgxStore) RepointOrgNullAccountEvents(ctx context.Context, orgID, accountID uuid.UUID, windowStart time.Time) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer deferredRollback(ctx, tx)
	qtx := s.q.WithTx(tx)
	// Keep the target anchor stable while choosing the destination window and
	// repointing account-less observations. This follows the same account-row →
	// period-lock order as ingest and rollup.
	if _, err := qtx.LockUsageAccountActivation(ctx, accountID.String()); err != nil {
		return 0, err
	}

	// Repoint is another admission writer. Share the period barrier with v2
	// ingest; if rollup won and closed the caller's window, advance along the
	// authoritative period rows until reaching an open/unmaterialized window.
	targetStart := windowStart.UTC()
	for advances := 0; ; advances++ {
		if advances > 24 {
			return 0, errors.New("repoint traversed more than 24 closed billing periods")
		}
		if _, err := tx.Exec(ctx, meteringlock.SharedAdvisorySQL, meteringlock.PeriodKey(accountID, targetStart)); err != nil {
			return 0, err
		}
		nextStart, err := qtx.ClosedBillingPeriodEndAtStart(ctx, db.ClosedBillingPeriodEndAtStartParams{
			AccountID: accountID.String(), PeriodStart: targetStart,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			break
		}
		if err != nil {
			return 0, err
		}
		if !nextStart.After(targetStart) {
			return 0, fmt.Errorf("closed billing period at %s has non-advancing end %s", targetStart, nextStart)
		}
		targetStart = nextStart.UTC()
	}

	rows, err := qtx.RepointOrgNullAccountEvents(ctx, db.RepointOrgNullAccountEventsParams{
		AccountID:   accountID.String(),
		WindowStart: targetStart,
		OrgID:       orgID.String(),
	})
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return rows, nil
}

func (s *pgxStore) OrgLiveAppIDs(ctx context.Context, orgID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.q.OrgLiveAppIDs(ctx, pgtype.UUID{Bytes: orgID, Valid: true})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) ListSponsoredOrgIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	rows, err := s.q.ListSponsoredOrgIDs(ctx, pgtype.UUID{Bytes: userID, Valid: true})
	if err != nil {
		return nil, err
	}
	return parseUUIDs(rows)
}

func (s *pgxStore) ChargeFundingAccount(ctx context.Context, accountID uuid.UUID) (uuid.UUID, error) {
	id, err := s.q.ChargeFundingAccount(ctx, accountID.String())
	if err != nil {
		return uuid.Nil, err // incl. ErrNoRows: a missing accounts row is a code bug, not a skip
	}
	return uuid.Parse(id)
}

// uuidRowFound decodes the (uuid-as-string, error) shape every single-row
// account-resolution query yields: ErrNoRows → (Nil, false, nil) — a normal
// lazy/missing outcome, not an error — else the parsed id.
func uuidRowFound(id string, err error) (uuid.UUID, bool, error) {
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	if err != nil {
		return uuid.Nil, false, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return uuid.Nil, false, err
	}
	return parsed, true, nil
}

// pgUUIDOrNull maps uuid.Nil to a SQL NULL and a real UUID to a valid
// pgtype.UUID — the encode twin of uuidFromPg.
func pgUUIDOrNull(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{} // Valid: false → NULL
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func nullableAggregationKey(key AggregationKey) pgtype.Text {
	if key == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: string(key), Valid: true}
}

// uuidFromPg decodes a nullable uuid column: NULL → uuid.Nil.
func uuidFromPg(u pgtype.UUID) uuid.UUID {
	if !u.Valid {
		return uuid.Nil
	}
	return uuid.UUID(u.Bytes)
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

// MarkCombinedProrationProposed resolves the attempt against its intent rather
// than an invoice.
//
// The reference is prefixed "intent:" for the reason the sibling legs give: a
// digest is not a Stripe invoice id, and a row where the two are
// indistinguishable is one where a later reconciler cannot tell which rail
// settled it.
//
// It resolves the HEADER only. persistProrationCharge also mirrors invoice
// children and timer charges, and there are none here — no invoice exists, and
// stamping timers as charged would claim money moved.
func (s *pgxStore) MarkCombinedProrationProposed(
	ctx context.Context,
	appID uuid.UUID,
	at time.Time,
	intentReference string,
) error {
	if intentReference == "" {
		return errors.New("cycle: a proposed proration needs its intent reference")
	}
	resolved, err := s.q.ResolveCombinedProrationAttempt(ctx, db.ResolveCombinedProrationAttemptParams{
		ResolvedAt:        at.UTC(),
		ResolvedInvoiceID: pgtype.Text{String: intentReference, Valid: true},
		AppID:             appID.String(),
	})
	if err != nil {
		return fmt.Errorf("resolve combined proration attempt as proposed: %w", err)
	}
	if resolved != 1 {
		return fmt.Errorf(
			"combined proration attempt %s could not resolve to intent %s", appID, intentReference)
	}
	return nil
}
