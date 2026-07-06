package cycle

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
)

// ErrInactiveModelPrice is returned by MetricPriceMicros when a usage row's
// (metric, model) has a per-model price ROW that has been retired (active =
// false). It is deliberately NOT pgx.ErrNoRows: a missing row legitimately falls
// back to the catalog price, but a retired model price must fail the cycle loud
// rather than silently bill at the cheaper catalog (Haiku-floor) fallback — that
// would under-bill a deliberately-retired model and defeat the rollup's loud
// revenue-leak guard. The Service maps this to a loud Internal error.
var ErrInactiveModelPrice = errors.New("per-model price is retired (active=false)")

// Store is the persistence interface the rollup + settlement Service depends
// on. Narrow on purpose — every method maps to a specific rollup step — so
// tests satisfy it with a small in-memory fake (see service_test.go).
type Store interface {
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
	// aggregate. When model != "" it resolves the AUTHORITATIVE per-(metric,
	// model) price from metric_model_prices (migration 018) first, falling back
	// to the (module, metric) catalog row when no per-model price exists; model
	// == "" resolves the catalog row directly. priced=false (NULL/absent price)
	// → the metric is metered-but-unpriced and prices to 0. A per-model row that
	// EXISTS but is RETIRED (active=false) returns ErrInactiveModelPrice instead
	// of silently falling back to the cheaper catalog floor — the Service fails
	// the cycle loud rather than under-bill a deliberately-retired model.
	MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model string) (micros int64, priced bool, err error)

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
	FreezeBillingRunCharge(ctx context.Context, runID uuid.UUID, charge FrozenBoundaryCharge) (FrozenBoundaryCharge, error)

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

	// EnsureAccountForUser resolves the user's billing account, creating the
	// row if none exists yet — the SAME per-user-advisory-lock get-or-create
	// billing.Ensure uses (the one established account-creation path; no
	// Stripe Customer is created here). RegisterApp needs it because the apps
	// mirror row carries a NOT NULL account FK and app creation must never be
	// blocked on the user having visited billing first (D1c: the platform
	// fires RegisterApp fire-and-forget).
	EnsureAccountForUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, error)

	// AccountActivation returns the account's activated_at anchor (migration
	// 025) and whether it is set. activated=false → the account never bound a
	// card and is NEVER charged (D1d — the same posture as the spine's
	// unactivated skip); RegisterApp then records the mirror row without a
	// proration invoice.
	AccountActivation(ctx context.Context, accountID uuid.UUID) (activatedAt time.Time, activated bool, err error)

	// InsertAppMirror registers a ms_billing.apps roster row idempotently
	// (ON CONFLICT (app_id) DO NOTHING — a retry never rewrites the original
	// created_at / module_count, which anchor the proration).
	InsertAppMirror(ctx context.Context, appID, accountID uuid.UUID, moduleCount int, createdAt time.Time) error

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

	// SetAppProrationInvoice arms the ONE-SHOT creation-proration guard: it
	// records the Stripe invoice id, first-charge-wins (UPDATE … WHERE
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

	// MarkModuleTimerChargeAttempted stamps the migration-036 recovery marker
	// BEFORE a charge attempt's first Stripe call — first-write-wins, never
	// cleared. A later retry seeing it set reconciles against Stripe (the
	// ms_charge_ref anchor) before recomputing any live verdict or minting new
	// Stripe objects.
	MarkModuleTimerChargeAttempted(ctx context.Context, timerID uuid.UUID, at time.Time) error

	// ModuleTimerStillPending re-verifies, immediately before acting on a sweep
	// candidate, that the timer is STILL live and unresolved — the work list is
	// read once and can be minutes stale by the time a late candidate is
	// processed (M2).
	ModuleTimerStillPending(ctx context.Context, timerID uuid.UUID) (bool, error)

	// MarkAppProrationAttempted stamps the migration-036 recovery marker for the
	// creation-proration leg — first-write-wins, never cleared.
	MarkAppProrationAttempted(ctx context.Context, appID uuid.UUID, at time.Time) error

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
	// can never both insert the full deficit (phantom $3 timers).
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
	ChargeAttemptedAt time.Time
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
	Cents    int64
	WithBase bool
}

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
	AccountID       uuid.UUID
	StripeInvoiceID string
	Status          string
	AmountDueCents  int64
	AmountPaidCents int64
	Currency        string
	PeriodStart     time.Time
	PeriodEnd       time.Time
	// IsLargeAutoCollect is the server-computed post-hoc disclosure flag
	// (migration 034): true iff the charged amount exceeded the account's
	// resolved auto-collect threshold WHEN THE CHARGE FIRED. Set by every
	// off-session charge call site; false for anything below the threshold.
	IsLargeAutoCollect bool
}

// RawAggregate is one per-kind aggregated row from the rollup SELECTs, before
// pricing. BillableQuantity is the exact NUMERIC string (count/sum SUM, peak
// MAX, time_weighted integral). Model is the AI pricing dimension the rollup
// groups by (migration 018): empty for non-AI metrics (the rollup's
// COALESCE(model, ”)), a roster model id for infra.ai.* events. It selects the
// price source in MetricPriceMicros (per-model vs catalog). ModuleVersion is
// the version-attribution dimension the rollup ALSO groups by (migration
// 023): empty for a version-less event, the emitting module's version
// otherwise. It never affects pricing — it is carried straight through onto
// the aggregate for reporting only.
type RawAggregate struct {
	AppID            uuid.UUID
	ModuleID         uuid.UUID
	Metric           string
	Kind             Kind
	Model            string
	ModuleVersion    string
	BillableQuantity string
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
	appendRow := func(appID, moduleID, metric string, kind db.MsBillingMetricKind, model, moduleVersion string, qty pgtype.Numeric) error {
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
			Model:            model,         // "" for non-AI rows (COALESCE(model, ''))
			ModuleVersion:    moduleVersion, // "" for version-less rows (COALESCE(module_version, ''))
			BillableQuantity: numericString(qty),
		})
		return nil
	}
	for _, r := range sumRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range peakRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	for _, r := range twRows {
		if err := appendRow(r.AppID, r.ModuleID, r.Metric, r.Kind, r.Model, r.ModuleVersion, r.BillableQuantity); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *pgxStore) MetricPriceMicros(ctx context.Context, moduleID uuid.UUID, metric, model string) (int64, bool, error) {
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
	return s.q.UpsertUsageAggregate(ctx, db.UpsertUsageAggregateParams{
		PeriodID:          periodID.String(),
		AccountID:         accountID.String(),
		AppID:             agg.AppID.String(),
		ModuleID:          agg.ModuleID.String(),
		Metric:            agg.Metric,
		Model:             agg.Model,
		ModuleVersion:     agg.ModuleVersion,
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
	return s.q.UpsertInvoice(ctx, db.UpsertInvoiceParams{
		AccountID:          inv.AccountID.String(),
		StripeInvoiceID:    inv.StripeInvoiceID,
		Status:             inv.Status,
		AmountDue:          due,
		AmountPaid:         paid,
		Currency:           inv.Currency,
		PeriodStart:        pgtype.Timestamptz{Time: inv.PeriodStart, Valid: !inv.PeriodStart.IsZero()},
		PeriodEnd:          pgtype.Timestamptz{Time: inv.PeriodEnd, Valid: !inv.PeriodEnd.IsZero()},
		IsLargeAutoCollect: inv.IsLargeAutoCollect,
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

func (s *pgxStore) MarkBillingRunInvoicedIfUnfrozen(ctx context.Context, runID uuid.UUID) (bool, error) {
	rows, err := s.q.MarkBillingRunInvoicedIfUnfrozen(ctx, runID.String())
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *pgxStore) FreezeBillingRunCharge(ctx context.Context, runID uuid.UUID, charge FrozenBoundaryCharge) (FrozenBoundaryCharge, error) {
	// WHERE frozen_charge_cents IS NULL (in the query) makes this first-write-wins:
	// a run that already froze (an earlier attempt, or a CONCURRENT daemon that got
	// here first) affects 0 rows and keeps the ORIGINAL frozen amount. The read-back
	// below returns the SURVIVING value regardless of which write won, and the
	// caller charges THAT — never its locally computed amount — so two racing
	// processes can never send different bodies under the shared idem keys.
	if err := s.q.FreezeBillingRunCharge(ctx, db.FreezeBillingRunChargeParams{
		ID:                   runID.String(),
		FrozenChargeCents:    pgtype.Int8{Int64: charge.Cents, Valid: true},
		FrozenChargeWithBase: pgtype.Bool{Bool: charge.WithBase, Valid: true},
	}); err != nil {
		return FrozenBoundaryCharge{}, err
	}
	surviving, ok, err := s.BillingRunFrozenCharge(ctx, runID)
	if err != nil {
		return FrozenBoundaryCharge{}, err
	}
	if !ok {
		return FrozenBoundaryCharge{}, fmt.Errorf("billing run %s has no frozen charge immediately after freezing", runID)
	}
	return surviving, nil
}

func (s *pgxStore) BillingRunFrozenCharge(ctx context.Context, runID uuid.UUID) (FrozenBoundaryCharge, bool, error) {
	row, err := s.q.BillingRunFrozenCharge(ctx, runID.String())
	if err != nil {
		return FrozenBoundaryCharge{}, false, err
	}
	if !row.FrozenChargeCents.Valid {
		return FrozenBoundaryCharge{}, false, nil // fresh run — no prior attempt froze
	}
	return FrozenBoundaryCharge{
		Cents:    row.FrozenChargeCents.Int64,
		WithBase: row.FrozenChargeWithBase.Bool,
	}, true, nil
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

func (s *pgxStore) EnsureAccountForUser(ctx context.Context, userID uuid.UUID) (uuid.UUID, error) {
	var id string
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		// billing.EnsureAccount and this get-or-create insert the SAME
		// accounts row, so both serialize on the single exported
		// (namespace, key) pair — the accounts table has no owner UNIQUE
		// constraint; the advisory lock IS the uniqueness guard.
		if err := qtx.AcquireBillingAccountUserLock(ctx, db.AcquireBillingAccountUserLockParams{
			Column1: billing.AdvisoryLockNamespaceBillingAccountUser,
			Column2: userID.String(),
		}); err != nil {
			return err
		}
		existing, err := qtx.SelectAccountByUser(ctx, pgtype.UUID{Bytes: userID, Valid: true})
		if err == nil {
			id = existing.ID
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		inserted, err := qtx.InsertUserAccount(ctx, pgtype.UUID{Bytes: userID, Valid: true})
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

func (s *pgxStore) InsertAppMirror(ctx context.Context, appID, accountID uuid.UUID, moduleCount int, createdAt time.Time) error {
	// RowsAffected 0 = a retry hit ON CONFLICT DO NOTHING — success either way.
	_, err := s.q.InsertAppMirror(ctx, db.InsertAppMirrorParams{
		AppID:       appID.String(),
		AccountID:   accountID.String(),
		ModuleCount: int32(moduleCount), //nolint:gosec // RegisterApp validates 0 ≤ count ≤ maxModuleCount (100000), far below int32 max
		CreatedAt:   createdAt,
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
	acct, err := uuid.Parse(row.AccountID)
	if err != nil {
		return AppMirror{}, false, err
	}
	return AppMirror{
		AppID:              app,
		AccountID:          acct,
		ModuleCount:        int(row.ModuleCount),
		CreatedModuleCount: int(row.CreatedModuleCount),
		CreatedAt:          row.CreatedAt,
		ProrationInvoiceID: row.ProrationInvoiceID.String, // "" when NULL (guard unarmed)
		ProrationSkipped:   row.ProrationSkippedAt.Valid,
		ProrationAttempted: row.ProrationAttemptedAt.Valid,
		Deleted:            row.DeletedAt.Valid,
		DeletedAt:          row.DeletedAt.Time,
	}, true, nil
}

func (s *pgxStore) AppsPendingProration(ctx context.Context, createdBefore time.Time) ([]uuid.UUID, error) {
	rows, err := s.q.AppsPendingProration(ctx, createdBefore)
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
	if row.DeletedAt.Valid {
		return AppMirror{}, ProrationLockedDeleted, "", false, nil
	}
	if row.ProrationInvoiceID.Valid {
		return AppMirror{}, ProrationLockedAlreadyCharged, row.ProrationInvoiceID.String, false, nil
	}

	app, err := uuid.Parse(row.AppID)
	if err != nil {
		return AppMirror{}, 0, "", false, err
	}
	acct, err := uuid.Parse(row.AccountID)
	if err != nil {
		return AppMirror{}, 0, "", false, err
	}
	locked = AppMirror{
		AppID:              app,
		AccountID:          acct,
		ModuleCount:        int(row.ModuleCount),
		CreatedModuleCount: int(row.CreatedModuleCount),
		CreatedAt:          row.CreatedAt,
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

	due, err := centsNumeric(pc.Invoice.AmountDueCents)
	if err != nil {
		return 0, "", err
	}
	paid, err := centsNumeric(pc.Invoice.AmountPaidCents)
	if err != nil {
		return 0, "", err
	}
	if err := qtx.UpsertInvoice(ctx, db.UpsertInvoiceParams{
		AccountID:       pc.Invoice.AccountID.String(),
		StripeInvoiceID: pc.Invoice.StripeInvoiceID,
		Status:          pc.Invoice.Status,
		AmountDue:       due,
		AmountPaid:      paid,
		Currency:        pc.Invoice.Currency,
		PeriodStart:     pgtype.Timestamptz{Time: pc.Invoice.PeriodStart, Valid: !pc.Invoice.PeriodStart.IsZero()},
		PeriodEnd:       pgtype.Timestamptz{Time: pc.Invoice.PeriodEnd, Valid: !pc.Invoice.PeriodEnd.IsZero()},
		// Scenario 5 — the disclosure flag the charge callback computed for the FULL
		// combined debit (base + co-created overage lines). Dropping it here would
		// silently write false for every creation/combined invoice.
		IsLargeAutoCollect: pc.Invoice.IsLargeAutoCollect,
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
	// Arm the one-shot guard. First-write-wins (WHERE proration_invoice_id IS
	// NULL): a concurrent second attempt for the same app affects 0 rows here
	// and keeps the winner's (identical, by construction) invoice id.
	if _, err := qtx.SetAppProrationInvoice(ctx, db.SetAppProrationInvoiceParams{
		AppID:              appID.String(),
		ProrationInvoiceID: pgtype.Text{String: pc.InvoiceID, Valid: true},
	}); err != nil {
		return 0, "", err
	}

	// Scenario 3 — stamp the co-created over-module timers billed on this SAME
	// combined invoice as terminally charged, in the SAME transaction as the guard
	// arm (all-or-nothing: an over-module and the app base are marked together).
	// WHERE grace_resolved = false is first-write-wins — a Leg 1 sweep that already
	// resolved a timer (its own invoice) affects 0 rows here, keeping the winner's ids.
	for _, tc := range pc.TimerCharges {
		if err := qtx.MarkModuleTimerCharged(ctx, db.MarkModuleTimerChargedParams{
			TimerID:            tc.TimerID.String(),
			GraceChargedAt:     tc.ChargedAt,
			GraceInvoiceID:     pgtype.Text{String: tc.InvoiceID, Valid: tc.InvoiceID != ""},
			GraceInvoiceItemID: pgtype.Text{String: tc.InvoiceItemID, Valid: tc.InvoiceItemID != ""},
		}); err != nil {
			return 0, "", err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, "", err
	}
	return ProrationLockedCharged, pc.InvoiceID, nil
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

func (s *pgxStore) MarkModuleTimerChargeAttempted(ctx context.Context, timerID uuid.UUID, at time.Time) error {
	return s.q.MarkModuleTimerChargeAttempted(ctx, db.MarkModuleTimerChargeAttemptedParams{
		ID:                timerID.String(),
		ChargeAttemptedAt: pgtype.Timestamptz{Time: at, Valid: true},
	})
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
		if err := qtx.InsertModuleOverageTimers(ctx, db.InsertModuleOverageTimersParams{
			AccountID:      row.AccountID,
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
