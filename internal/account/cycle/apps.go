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
//  3. AFTER the mirror insert, recompute the account's pooled-overage grace
//     timer (recomputeAccountOverage): a creation whose module_count pushes the
//     account's live pool over IncludedModules arms accounts.overage_since
//     (migration 032). This is independent of the deferred creation-proration
//     base charge (which the grace sweep fires later, and which bills the FLAT
//     base only — module overage is account-wide pooled, migration 032).
//     Idempotent (Start/Clear are first-crossing-wins / no-op), so a retry
//     re-runs it harmlessly.
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

	// Recompute the account's pooled overage timer (migration 032) after the
	// insert: the new app's module_count may push the account's live pool over
	// IncludedModules (arming accounts.overage_since). RegisterApp charges
	// NOTHING here (creation grace) — the FLAT-base creation proration is the
	// grace sweep's job (proration.go), and the module overage rides the same
	// grace mechanism. Idempotent (Start/Clear are first-crossing-wins / no-op),
	// so a retry re-runs it harmlessly.
	if err := s.recomputeAccountOverage(ctx, app.AccountID); err != nil {
		return nil, err
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
// After any module_count / delete write it recomputes the account's pooled-
// overage grace timer (recomputeAccountOverage). The per-app FLAT base still
// takes effect at the NEXT boundary (no mid-period base micro-invoice / refund),
// but the ACCOUNT-WIDE pooled overage is now the DELIBERATE exception to the old
// D1b "no mid-period charges" rule: crossing the pooled IncludedModules arms a
// grace timer, and the mid-period sweep charges the pooled overage once the
// grace window elapses (migration 032). Dropping back under the pool clears the
// timer (no refund of anything already charged, D1e).
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

	// Recompute the account-wide pooled-overage grace timer (migration 032) after
	// the write: a count bump or delete can push the pool over IncludedModules
	// (arm overage_since) or drop it back under (clear it). Idempotent, so a
	// pure-delete-of-an-already-deleted-app retry re-runs it harmlessly.
	if err := s.recomputeAccountOverage(ctx, app.AccountID); err != nil {
		return nil, err
	}

	return &SyncAppModulesResponse{
		AppID:       app.AppID,
		ModuleCount: app.ModuleCount,
		Deleted:     app.Deleted,
	}, nil
}
