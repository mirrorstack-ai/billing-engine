package cycle

// RegisterApp / SyncAppModules — the ms_billing.apps mirror writers (base-fee
// v1, DESIGN.md "Base fee — v1 spec", owner spec 2026-07-05, D1c). Called by
// api-platform's applications-service fire-and-forget (with retry) on app
// create / module install / uninstall / app delete, so BOTH RPCs are
// idempotent end to end: a retry can re-register or re-sync without a second
// charge or a moved timestamp.
//
// They live in the cycle package because RegisterApp's creation-proration
// charge IS a charge-spine leg: it reuses the SAME Stripe invoice plumbing
// (CreateInvoiceItem + CreateInvoice with deterministic Idempotency-Keys),
// the SAME micros→cents boundary (centsFromMicros), and the SAME invoice
// mirror (UpsertInvoice) as RunBillingCycle — never a second Stripe path.

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/collection"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// maxModuleCount bounds the installed-module count BOTH mirror RPCs accept.
// It guards the int → int32 narrowing at the store boundary (a count beyond
// int32 would silently truncate into a wrong — possibly negative — tier) and
// is orders of magnitude above any real app's module roster. Anything larger
// is a malformed or hostile payload → billing.InvalidInput, never a truncated
// write.
const maxModuleCount = 100_000

// RegisterAppRequest is the payload of the RegisterApp RPC. Owner fields
// mirror the other owner-keyed RPCs (exactly one set); v1 resolves USER
// owners only — org billing is out of scope (D6), matching billing.Ensure
// (the one account-creation path, user-keyed).
type RegisterAppRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`

	// AppID is the platform app id (ms_apps.id), mirrored verbatim.
	AppID uuid.UUID `json:"app_id"`

	// ModuleCount is the installed-module count at creation (default 0).
	ModuleCount int `json:"module_count,omitempty"`

	// CreatedAt is the platform app-creation instant (RFC3339 on the wire);
	// zero → the server's now. It anchors the creation-proration window, and
	// the FIRST registration's value is immutable across retries.
	CreatedAt time.Time `json:"created_at,omitempty"`
}

// RegisterAppResponse reports the mirror write + what the proration leg did.
// ProrationInvoiceID is "" when no proration invoice exists (unactivated
// account, no usable PM, a creation period that already closed — the boundary
// advance leg owns every later period — or the amount rounded to 0 cents; all
// legitimate no-charge outcomes, D1d).
type RegisterAppResponse struct {
	AppID              uuid.UUID `json:"app_id"`
	AccountID          uuid.UUID `json:"account_id"`
	ProrationInvoiceID string    `json:"proration_invoice_id,omitempty"`
	ProrationCents     int64     `json:"proration_cents,omitempty"`
}

// SyncAppModulesRequest is the payload of the SyncAppModules RPC. ModuleCount
// is a POINTER so "sync count to 0" and "no count in this call" (a pure
// delete signal) stay distinguishable on the wire.
type SyncAppModulesRequest struct {
	AppID       uuid.UUID `json:"app_id"`
	ModuleCount *int      `json:"module_count,omitempty"`
	Deleted     bool      `json:"deleted,omitempty"`
}

// SyncAppModulesResponse echoes the roster row's post-sync state.
type SyncAppModulesResponse struct {
	AppID       uuid.UUID `json:"app_id"`
	ModuleCount int       `json:"module_count"`
	Deleted     bool      `json:"deleted"`
}

// RegisterApp mirrors a freshly created platform app into ms_billing.apps and
// charges the creation-period proration (owner spec 2026-07-05):
//
//  1. resolve-or-create the owner's billing account via the SAME
//     advisory-locked get-or-create billing.Ensure uses (no Stripe Customer is
//     created — just the accounts row, so app creation is never blocked on the
//     user having visited billing);
//
//  2. insert the roster row idempotently (ON CONFLICT (app_id) DO NOTHING —
//     a retry keeps the FIRST registration's created_at / module_count, the
//     stable proration anchor), then read the row back;
//
//  3. proration leg, gated exactly like the spine (D1d): the account must be
//     ACTIVATED (activated_at set — migration 025) AND have a usable default
//     PM; otherwise the row is recorded and NO invoice is created (no
//     retroactive catch-up on activation in v1). The one-shot guard
//     proration_invoice_id short-circuits a retry that already charged.
//     Amount = ProratedBaseMicros(AppBaseFeeMicros(base, module_count),
//     created_at, the anchored period CONTAINING created_at) — whole UTC
//     days, creation day inclusive, round-half-up — converted micros → whole
//     cents at the Stripe boundary like every other charge. 0 cents → row
//     recorded, no invoice.
//
//     INVARIANT (charge-leg ownership): the proration window is derived from
//     the read-back mirror row's created_at (the stable first-registration
//     anchor), NEVER from "now" — and if that creation period has already
//     ENDED, the proration charge is SKIPPED entirely (charge 0, guard left
//     unarmed), deterministically on this and every future retry. Every
//     period after the creation period belongs to the boundary advance leg
//     (which bills the apps that existed before the period opened), so a
//     late retry recomputing against a newer window would double-charge a
//     period the boundary already billed — and v1 does no retroactive
//     catch-up either way (D1d).
//
//  4. charge via the spine's plumbing with app-scoped deterministic
//     Idempotency-Keys (app-ii-<app>/app-inv-<app> — the app id is the stable
//     charge identity here, as the run id is for the boundary leg), mirror the
//     invoice with the PARTIAL window [creation day, period end), write the
//     per-app-period base snapshot (migration 028, source='proration' — the
//     frozen display value for the creation period), then arm the one-shot
//     guard.
//
// A Stripe/mirror/snapshot failure after the row insert returns an error with
// the guard UNARMED — the platform's retry re-attempts the charge through the
// same idem keys, so the failure mode is "retry-safe", never "double-charge".
func (s *Service) RegisterApp(ctx context.Context, req RegisterAppRequest) (*RegisterAppResponse, error) {
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if req.OwnerOrgID != uuid.Nil {
		// v1 has no org-owned billing accounts (D6); the ONE account-creation
		// path (billing.Ensure / EnsureAccountForUser) is user-keyed. Loud
		// rather than silently dropping the mirror row.
		return nil, billing.InvalidInput("org-owned billing accounts are not supported yet (v1 resolves user owners only)")
	}
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.ModuleCount < 0 {
		return nil, billing.InvalidInput("module_count must be non-negative")
	}
	if req.ModuleCount > maxModuleCount {
		return nil, billing.InvalidInput("module_count exceeds the maximum supported count (100000)")
	}

	createdAt := req.CreatedAt
	if createdAt.IsZero() {
		createdAt = s.nowFn().UTC()
	}

	accountID, err := s.store.EnsureAccountForUser(ctx, req.OwnerUserID)
	if err != nil {
		return nil, billing.Internal("ensure billing account failed", err)
	}

	if err := s.store.InsertAppMirror(ctx, req.AppID, accountID, req.ModuleCount, createdAt); err != nil {
		return nil, billing.Internal("insert app mirror failed", err)
	}

	// Read the row BACK: a retry must prorate from the FIRST registration's
	// created_at / module_count (the insert's ON CONFLICT DO NOTHING preserved
	// them), and the one-shot guard state decides whether to charge at all.
	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return nil, billing.Internal("app mirror row missing immediately after insert", nil)
	}
	resp := &RegisterAppResponse{AppID: app.AppID, AccountID: app.AccountID}

	// One-shot guard: a prior attempt already charged (or a concurrent one
	// won). Idempotent success — NEVER a second invoice.
	if app.ProrationInvoiceID != "" {
		resp.ProrationInvoiceID = app.ProrationInvoiceID
		return resp, nil
	}

	// Activation gate (D1d): unactivated accounts are never charged — the row
	// is recorded and the guard stays unarmed (no retroactive catch-up in v1).
	activatedAt, activated, err := s.store.AccountActivation(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	}
	if !activated {
		return resp, nil
	}
	hasPM, err := s.store.HasUsableDefaultPM(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("usable PM check failed", err)
	}
	if !hasPM {
		return resp, nil // same skipped_no_pm posture as the spine — row kept, no invoice
	}

	// The creation period is the anchored window CONTAINING the app's
	// created_at (ADR 0005 anchor from the activation day) — windowed from the
	// read-back mirror row's created_at, the stable first-registration anchor,
	// NEVER from now: a retry landing after the boundary must not recompute
	// against the NEW period (ProratedBaseMicros would return the FULL base for
	// a window the boundary advance leg already billed → double charge, and the
	// legitimate creation-period proration would be lost). The proration bills
	// the window's remaining whole UTC days, creation day inclusive.
	// TODO(plan): plan-resolved base once a plan resolver exists (v1 = default).
	periodStart, periodEnd := billingperiod.AnchoredPeriodWindow(app.CreatedAt.UTC(), billingperiod.AnchorDay(activatedAt))

	// INVARIANT: the proration leg bills ONLY the creation period. If that
	// window has already ended, skip the charge entirely — charge 0, guard
	// left UNARMED — deterministically on this and every future retry. Every
	// later period is owned by the boundary advance leg (it bills the apps
	// that existed before the period opened), and v1 does no retroactive
	// catch-up (D1d), so a late retry must never invoice anything here.
	if !s.nowFn().UTC().Before(periodEnd) {
		return resp, nil
	}

	prorated := usage.ProratedBaseMicros(
		usage.AppBaseFeeMicros(usage.BaseFeeMicros, app.ModuleCount),
		app.CreatedAt, periodStart, periodEnd,
	)
	cents, err := centsFromMicros(prorated)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		// Rounded to 0 cents — nothing to invoice; the row stands and the
		// boundary leg picks the app up from the next period on.
		return resp, nil
	}

	if s.stripe == nil {
		return nil, billing.Internal("RegisterApp proration requires a Stripe client", nil)
	}
	custID, err := s.store.AccountStripeCustomer(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("stripe customer lookup failed", err)
	}
	if custID == "" {
		// A usable PM implies a Customer (same anomaly posture as the spine).
		return nil, billing.Internal("account has a usable PM but no Stripe customer id", nil)
	}

	desc := fmt.Sprintf("MirrorStack app base fee (prorated) — app %s", app.AppID)
	if _, err := s.stripe.CreateInvoiceItem(ctx, custID, cents, chargeCurrency, desc, appProrationItemIdemKey(app.AppID)); err != nil {
		return nil, billing.StripeError("proration invoice item failed", err)
	}
	inv, err := s.stripe.CreateInvoice(ctx, custID, true /* autoAdvance */, appProrationInvoiceIdemKey(app.AppID))
	if err != nil {
		return nil, billing.StripeError("proration invoice failed", err)
	}

	// Resolve the account's large-charge disclosure threshold AT CHARGE TIME so
	// the proration invoice's flag reflects what applied when it fired (migration
	// 031) — resolved HERE, immediately AFTER the Stripe calls above succeeded,
	// which is the SAME point relative to the actual charge that RunBillingCycle's
	// boundary leg now resolves its threshold (charge.go, re-resolved right after
	// its own Stripe call succeeds rather than reusing a pre-charge snapshot). Both
	// charge legs agreeing on this point means a threshold edit landing concurrently
	// with a charge is honored identically by both, not one way here and another on
	// the boundary leg. A prorated app base fee rarely crosses the $100 default, but
	// the flag is computed uniformly at every off-session charge call site so no
	// successful large debit escapes disclosure.
	acct, err := s.store.AccountCollection(ctx, app.AccountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed", err)
	}

	// Mirror with the PARTIAL window: [creation day (UTC midnight), period
	// end) — the SAME coverage-start instant ProratedBaseMicros priced, so
	// the mirrored window and the charged amount agree by construction.
	partialStart := usage.ProrationCoverageStart(app.CreatedAt, periodStart)
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:          app.AccountID,
		StripeInvoiceID:    inv.ID,
		Status:             inv.Status,
		AmountDueCents:     inv.AmountDue,
		AmountPaidCents:    inv.AmountPaid,
		Currency:           chargeCurrency,
		PeriodStart:        partialStart,
		PeriodEnd:          periodEnd,
		IsLargeAutoCollect: collection.IsLargeAutoCollect(prorated, acct.AutoCollectThresholdMicros),
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}

	// Freeze what this charge actually billed (migration 028) BEFORE arming the
	// guard: the snapshot is the display's authoritative base for the creation
	// period, so a later SyncAppModules can never drift the shown base away
	// from this invoice. Keyed by the FULL anchored period_start (the display
	// identity); BaseMicros is the prorated partial-window amount. Idempotent —
	// a retry that failed between here and the guard re-upserts identical
	// values through the same Stripe idem keys.
	if err := s.store.UpsertProrationBaseSnapshot(ctx, AppBaseSnapshot{
		AppID:       app.AppID,
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		ModuleCount: app.ModuleCount,
		BaseMicros:  prorated,
	}); err != nil {
		return nil, billing.Internal("proration base snapshot upsert failed", err)
	}

	if err := s.store.SetAppProrationInvoice(ctx, app.AppID, inv.ID); err != nil {
		return nil, billing.Internal("arm proration guard failed", err)
	}

	resp.ProrationInvoiceID = inv.ID
	resp.ProrationCents = cents
	return resp, nil
}

// SyncAppModules updates an app's roster row: a new installed-module-count
// snapshot and/or the soft-delete flag (D1b/D1e). Semantics:
//
//   - deleted=true sets deleted_at = now() IF NULL (idempotent — the first
//     deletion instant is kept; no refunds, the current period's base is
//     spent, future boundary legs stop billing the app);
//   - a module_count update on a DELETED app is a NO-OP (its count is frozen
//     — there is no future base for the tier to move);
//   - an unknown app_id is NOT_FOUND (the platform must RegisterApp first).
//
// Count changes take effect at the NEXT boundary charge (D1b — no mid-period
// micro-invoices for module #6, no mid-period refunds for uninstalls).
func (s *Service) SyncAppModules(ctx context.Context, req SyncAppModulesRequest) (*SyncAppModulesResponse, error) {
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.ModuleCount != nil && *req.ModuleCount < 0 {
		return nil, billing.InvalidInput("module_count must be non-negative")
	}
	if req.ModuleCount != nil && *req.ModuleCount > maxModuleCount {
		return nil, billing.InvalidInput("module_count exceeds the maximum supported count (100000)")
	}

	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return nil, billing.NotFound("app not registered (RegisterApp must run first)")
	}

	if req.Deleted && !app.Deleted {
		if err := s.store.MarkAppDeleted(ctx, req.AppID); err != nil {
			return nil, billing.Internal("mark app deleted failed", err)
		}
		app.Deleted = true
	}

	// Count update — no-op once deleted (frozen tier, D1e). req.Deleted in the
	// same call counts as deleted: deletion wins over the count.
	if req.ModuleCount != nil && !app.Deleted {
		if err := s.store.SetAppModuleCount(ctx, req.AppID, *req.ModuleCount); err != nil {
			return nil, billing.Internal("set app module count failed", err)
		}
		app.ModuleCount = *req.ModuleCount
	}

	return &SyncAppModulesResponse{
		AppID:       app.AppID,
		ModuleCount: app.ModuleCount,
		Deleted:     app.Deleted,
	}, nil
}

// appProrationItemIdemKey / appProrationInvoiceIdemKey build the deterministic
// Stripe Idempotency-Keys for the creation-proration charge. The APP id is the
// stable charge identity (each app prorates at most once — the one-shot
// proration_invoice_id guard), exactly as the RUN id is for the boundary leg's
// ii-/inv- keys: a RegisterApp retry reuses the SAME Stripe objects and can
// never double-charge even before the guard is armed.
func appProrationItemIdemKey(appID uuid.UUID) string    { return "app-ii-" + appID.String() }
func appProrationInvoiceIdemKey(appID uuid.UUID) string { return "app-inv-" + appID.String() }
