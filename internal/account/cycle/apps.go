package cycle

// RegisterApp / SyncAppModules — the ms_billing.apps mirror writers (base-fee
// v1, DESIGN.md "Base fee — v1 spec", owner spec 2026-07-05, D1c). Called by
// api-platform's applications-service fire-and-forget (with retry) on app
// create / module install / uninstall / app delete, so BOTH RPCs are
// idempotent end to end: a retry can re-register or re-sync without a moved
// timestamp.
//
// Neither RPC charges Stripe. The creation-period base is charged by the grace
// sweep (proration.go) once an app survives GraceDays — see ChargeCreationProration
// for the charge leg that reuses the SAME Stripe plumbing, micros→cents boundary,
// and invoice mirror as RunBillingCycle.

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
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

// RegisterAppResponse reports the mirror write. RegisterApp charges nothing
// (creation grace); the creation-proration invoice is minted later by the sweep
// (proration.go). ProrationInvoiceID is therefore "" for a fresh registration
// and carries the armed guard's invoice id only for a retry that lands AFTER the
// sweep already charged (idempotent visibility). ProrationCents stays 0 here (no
// charge happens in this RPC) — the fields are retained for wire back-compat.
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

// RegisterApp mirrors a freshly created platform app into ms_billing.apps. It
// charges NOTHING (creation grace, owner spec 2026-07-05, D1e follow-up): a
// newly created app enters a grace window and is charged its creation-period
// base only later, by the sweep (ChargeCreationProration / SweepCreationProrations
// in proration.go), and only if it SURVIVES grace — so an app deleted within
// grace is never billed. RegisterApp:
//
//  1. resolve-or-create the owner's billing account via the SAME
//     advisory-locked get-or-create billing.Ensure uses (no Stripe Customer is
//     created — just the accounts row, so app creation is never blocked on the
//     user having visited billing);
//
//  2. insert the roster row idempotently (ON CONFLICT (app_id) DO NOTHING —
//     a retry keeps the FIRST registration's created_at / module_count, the
//     stable proration anchor the sweep later prices from), then read it back.
//
// The response echoes the resolved account id and — for a retry that lands after
// the sweep already charged — the armed guard's invoice id (idempotent
// visibility). ProrationCents is never set here (no charge happens in this RPC).
// api-platform fires this fire-and-forget with retry; every step is idempotent.
//
// Finally, AFTER the mirror insert, it synthesizes K per-module install timers
// (migration 033) anchored at created_at — one row per co-created module, each on
// its own independent overage grace timer. Reconcile-to-target (insert only the
// deficit vs. the app's live timer count) keeps it idempotent across
// fire-and-forget retries and self-heals a crashed first attempt. The timers are
// charged (if "over") only later, by the Leg 1 sweep (overage.go), and only after
// each survives its OWN grace — independent of the deferred creation-proration
// base charge (proration.go).
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

	// Read the row BACK so the response reflects the FIRST registration's stable
	// account + guard state (the insert's ON CONFLICT DO NOTHING preserved them):
	// a retry that lands after the sweep charged echoes the armed invoice id.
	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		return nil, billing.Internal("app mirror row missing immediately after insert", nil)
	}
	resp := &RegisterAppResponse{
		AppID:              app.AppID,
		AccountID:          app.AccountID,
		ProrationInvoiceID: app.ProrationInvoiceID, // "" until the sweep charges
	}

	// Synthesize the app's per-module install timers (migration 033), all anchored
	// at the app's created_at (the read-back mirror's stable first-registration
	// value) — the RPC carries only an integer module_count, so K identical timer
	// rows stand in for the K co-created module instances. The reconcile runs
	// under a per-app advisory lock (H7 — a concurrent retry can never
	// double-insert the deficit) and makes a fire-and-forget retry a no-op once
	// the first registration already synthesized them. NEVER for a deleted app
	// (review 2026-07-06, H4): a late retry — or a billing-backfill
	// re-registration — that lands after the app was deleted must not resurrect
	// live timers for it (deletion freezes module_count, so the deficit would
	// look real and the phantom timers would charge forever).
	if !app.Deleted {
		if err := s.store.ReconcileModuleTimersToTarget(ctx, app.AccountID, app.AppID,
			app.ModuleCount, app.CreatedAt, moduleGraceExpiry(app.CreatedAt), s.nowFn().UTC()); err != nil {
			return nil, billing.Internal("reconcile module overage timers failed", err)
		}
	}
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
// After any module_count / delete write it synthesizes the app's per-module
// install timers (migration 033): a grow (M>N) inserts M−N new timers anchored
// at now (each a genuine new install on its OWN overage grace timer); a shrink
// (M<N) LIFO-soft-removes N−M timers (the newest installs first — the customer
// presumably removed what they added most recently); a delete soft-removes ALL
// the app's still-live timers. A removed timer never charges (matching the
// delete-in-grace = never-charged posture), and no refund is issued for a timer
// already charged this period (D1e). The per-app FLAT base still takes effect at
// the NEXT boundary (no mid-period base micro-invoice / refund).
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

	now := s.nowFn().UTC()

	if req.Deleted {
		if !app.Deleted {
			if err := s.store.MarkAppDeleted(ctx, req.AppID); err != nil {
				return nil, billing.Internal("mark app deleted failed", err)
			}
			app.Deleted = true
		}
		// Deletion soft-removes ALL the app's still-live install timers — they drop
		// out of the FIFO and the Leg 1 sweep, so a module deleted with its app is
		// never charged its overage (no refund of anything already charged, D1e).
		// Re-fired on EVERY delete signal, not only the first (review 2026-07-06,
		// H4): the two writes are not transactional, so a crash between them
		// leaves the app deleted with live orphaned timers — and a retry gated on
		// !app.Deleted would skip this block forever, leaving the orphans in the
		// FIFO (demoting other apps' modules to "over") and chargeable. The
		// soft-remove is idempotent (WHERE removed_at IS NULL), so the re-fire is
		// free on the happy path and self-heals the crashed one.
		if err := s.store.SoftRemoveAllModuleTimersForApp(ctx, req.AppID, now); err != nil {
			return nil, billing.Internal("soft-remove app module timers failed", err)
		}
	}

	// Count update — no-op once deleted (frozen tier, D1e). req.Deleted in the
	// same call counts as deleted: deletion wins over the count.
	if req.ModuleCount != nil && !app.Deleted {
		if err := s.store.SetAppModuleCount(ctx, req.AppID, *req.ModuleCount); err != nil {
			return nil, billing.Internal("set app module count failed", err)
		}
		app.ModuleCount = *req.ModuleCount
		// Reconcile the app's live install timers to the new count under the
		// per-app advisory lock (H7): grow inserts (anchored at now — genuine new
		// installs), shrink LIFO-removes the newest. Reconciling against the LIVE
		// timer count (not the app's prior module_count) makes it idempotent
		// across SyncAppModules retries and self-healing after a crash between
		// the count write and the timer write.
		if err := s.store.ReconcileModuleTimersToTarget(ctx, app.AccountID, req.AppID,
			*req.ModuleCount, now, moduleGraceExpiry(now), now); err != nil {
			return nil, billing.Internal("reconcile module overage timers failed", err)
		}
	}

	return &SyncAppModulesResponse{
		AppID:       app.AppID,
		ModuleCount: app.ModuleCount,
		Deleted:     app.Deleted,
	}, nil
}
