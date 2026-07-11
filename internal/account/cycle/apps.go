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
// mirror the other owner-keyed RPCs (exactly one set). Both owner kinds pass
// the FUNDING GATE for a NEW app_id (docs-temp/billing-funding-gates/design.md,
// DECIDED 2026-07-11): a USER owner's account must be activated with a usable
// non-fraud card; an ORG owner must resolve through its funding designation
// (migration 041) to such an account — unresolved designation → PAYMENT_
// REQUIRED. The old UNBILLED org registration path (NULL account_id roster
// row awaiting the attach sweep) is retired for NEW creates; the attach-sweep
// machinery stays for legacy rows.
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

	// Name is the platform app display name, FROZEN into the billing mirror on
	// first registration (migration 037) so a later-deleted app's bill still
	// shows its name. Empty → NULL (the frontend falls back to its registry
	// lookup). SyncAppModules updates it while the app is live.
	Name string `json:"name,omitempty"`
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
	// Name, when non-nil, is an app rename: updates the frozen mirror name while
	// the app is LIVE (a no-op once deleted, freezing the last-known name). nil
	// = no name change this sync (same nil-vs-value pattern as ModuleCount).
	Name *string `json:"name,omitempty"`
}

// SyncAppModulesResponse echoes the roster row's post-sync state.
type SyncAppModulesResponse struct {
	AppID       uuid.UUID `json:"app_id"`
	ModuleCount int       `json:"module_count"`
	Deleted     bool      `json:"deleted"`
	Name        string    `json:"name"`
}

// RegisterApp mirrors a freshly created platform app into ms_billing.apps. It
// charges NOTHING (creation grace, owner spec 2026-07-05, D1e follow-up): a
// newly created app enters a grace window and is charged its creation-period
// base only later, by the sweep (ChargeCreationProration / SweepCreationProrations
// in proration.go), and only if it SURVIVES grace — so an app deleted within
// grace is never billed. The grace keeps its original meaning (create→delete
// inside it is never billed); it no longer implies create-without-card.
// RegisterApp:
//
//  1. for a NEW app_id, resolve the owner's billing account and apply the
//     FUNDING GATE (docs-temp/billing-funding-gates/design.md, DECIDED
//     2026-07-11, inverting the earlier "creation never blocks on billing"
//     invariant): the account must be activated (activated_at NOT NULL) with
//     >= 1 usable non-fraud card — the standing gate's card predicate, reused
//     — else billing.PaymentRequired. A RE-register of an existing app_id
//     SKIPS the gate entirely (the platform's fire-and-forget retry must stay
//     idempotent even if the owner's funding lapsed since the first
//     registration);
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

	// Idempotency first: an app_id that is ALREADY mirrored is a fire-and-forget
	// retry — it must never suddenly fail the funding gate (the owner's funding
	// may have lapsed since the first registration; serving standing, not
	// registration, polices that). Only a genuinely NEW app_id is gated.
	app, found, err := s.store.AppMirror(ctx, req.AppID)
	if err != nil {
		return nil, billing.Internal("app mirror lookup failed", err)
	}
	if !found {
		accountID, err := s.fundedOwnerAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
		if err != nil {
			return nil, err
		}

		if err := s.store.InsertAppMirror(ctx, req.AppID, accountID, req.OwnerOrgID, req.ModuleCount, createdAt, req.Name); err != nil {
			return nil, billing.Internal("insert app mirror failed", err)
		}

		// Read the row BACK so the response reflects the FIRST registration's
		// stable account + guard state (a concurrent duplicate may have won the
		// ON CONFLICT DO NOTHING insert; its values stand).
		app, found, err = s.store.AppMirror(ctx, req.AppID)
		if err != nil {
			return nil, billing.Internal("app mirror lookup failed", err)
		}
		if !found {
			return nil, billing.Internal("app mirror row missing immediately after insert", nil)
		}
	}
	resp := &RegisterAppResponse{
		AppID:              app.AppID,
		AccountID:          app.AccountID,
		ProrationInvoiceID: app.ProrationInvoiceID, // "" until the sweep charges
	}

	// Synthesize the app's per-module install timers (migration 033), all anchored
	// at the app's created_at (the read-back mirror's stable first-registration
	// value) — the RPC carries only an integer module_count, so K identical timer
	// rows stand in for the K co-created module instances. The reconcile derives
	// the target count, owning account, AND deleted state from the roster row
	// INSIDE its per-app advisory-locked transaction (H7 + wave 2 D8/D9): a
	// concurrent retry can never double-insert the deficit, a late retry whose
	// mirror read is stale can never shrink to an outdated count, and a retry
	// landing after deletion removes orphans instead of resurrecting timers.
	if err := s.store.ReconcileModuleTimersToTarget(ctx, app.AppID,
		app.CreatedAt, moduleGraceExpiry(app.CreatedAt), s.nowFn().UTC()); err != nil {
		return nil, billing.Internal("reconcile module overage timers failed", err)
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
		// Deletion + timer soft-removal commit in ONE advisory-locked transaction
		// (wave 2, D9): no crash window can leave the app deleted with live
		// orphaned timers, no concurrent synthesis retry can interleave a
		// resurrection, and a re-fired delete signal self-heals idempotently
		// (first deletion instant kept; already-removed timers affected 0 times).
		// A module deleted with its app is never charged its overage; no refund
		// of anything already charged (D1e).
		if err := s.store.MarkAppDeletedAndRemoveTimers(ctx, req.AppID, now); err != nil {
			return nil, billing.Internal("mark app deleted + remove timers failed", err)
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
		// Reconcile the app's live install timers under the per-app advisory
		// lock (H7): grow inserts (anchored at now — genuine new installs),
		// shrink LIFO-removes the newest. The target is read from the roster row
		// INSIDE the locked transaction (wave 2, D8) — the count write above is
		// already committed, so the reconcile sees it (or a NEWER one, which is
		// equally correct), never a stale caller value.
		if err := s.store.ReconcileModuleTimersToTarget(ctx, req.AppID,
			now, moduleGraceExpiry(now), now); err != nil {
			return nil, billing.Internal("reconcile module overage timers failed", err)
		}
	}

	// Rename — no-op once deleted (frozen name, D1e-style), the same gate as the
	// count update above: a live app's bill tracks its current name; a deleted
	// app keeps its last-known name for the historical bill (migration 037).
	if req.Name != nil && !app.Deleted {
		if err := s.store.SetAppName(ctx, req.AppID, *req.Name); err != nil {
			return nil, billing.Internal("set app name failed", err)
		}
		app.Name = *req.Name
	}

	return &SyncAppModulesResponse{
		AppID:       app.AppID,
		ModuleCount: app.ModuleCount,
		Deleted:     app.Deleted,
		Name:        app.Name,
	}, nil
}

// fundedOwnerAccount is RegisterApp's create gate (funding-gates design,
// DECIDED 2026-07-11): resolve the (user XOR org) owner's billing account and
// verify it is FUNDED — activated (activated_at NOT NULL, the first-card-bind
// anchor) with >= 1 usable non-fraud card on the FUNDING account (the sponsor
// hop for sponsor-funded orgs: the org account owns no cards, the sponsor's
// does — the same ChargeFundingAccount hop every charge leg applies). Any
// unfunded shape returns billing.PaymentRequired, which api-platform maps to
// HTTP 402 on the create-app path.
//
// Deliberately read-only: a rejected create never get-or-creates an accounts
// row (a funded owner necessarily already has one — activation happens at
// card bind), and the card predicate is the standing gate's usable_card_count
// reused verbatim (Store.UsableNonFraudCardCount), never a second rule.
func (s *Service) fundedOwnerAccount(ctx context.Context, ownerUserID, ownerOrgID uuid.UUID) (uuid.UUID, error) {
	var accountID uuid.UUID
	if ownerUserID != uuid.Nil {
		id, found, err := s.store.AccountIDByUser(ctx, ownerUserID)
		if err != nil {
			return uuid.Nil, billing.Internal("owner account lookup failed", err)
		}
		if !found {
			return uuid.Nil, billing.PaymentRequired("billing account not set up: add a payment card before creating an app")
		}
		_, activated, err := s.store.AccountActivation(ctx, id)
		if err != nil {
			return uuid.Nil, billing.Internal("account activation lookup failed", err)
		}
		if !activated {
			return uuid.Nil, billing.PaymentRequired("billing account not activated: add a payment card before creating an app")
		}
		accountID = id
	} else {
		// Org leg: ResolveOrgFundedAccount already gates designation existence
		// AND activation ("the pointer never flips to an unfunded account").
		id, funded, err := s.store.ResolveOrgFundedAccount(ctx, ownerOrgID)
		if err != nil {
			return uuid.Nil, billing.Internal("org account resolution failed", err)
		}
		if !funded {
			return uuid.Nil, billing.PaymentRequired("organization has no funding: designate funding before creating an app")
		}
		accountID = id
	}

	fundingID, err := s.store.ChargeFundingAccount(ctx, accountID)
	if err != nil {
		return uuid.Nil, billing.Internal("funding account lookup failed", err)
	}
	cards, err := s.store.UsableNonFraudCardCount(ctx, fundingID)
	if err != nil {
		return uuid.Nil, billing.Internal("usable card count read failed", err)
	}
	if cards < 1 {
		return uuid.Nil, billing.PaymentRequired("no usable payment card on file: add a card before creating an app")
	}
	return accountID, nil
}
