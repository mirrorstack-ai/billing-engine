package cycle

// Org funding designation + the RepointOrgUsage attach sweep (org-billing W0
// substrate, workspace docs-temp/org-billing/design.md D1).
//
// One org = ONE ms_billing.accounts row; the designation picks only the
// FUNDING INSTRUMENT for that account's invoices (sponsor's card vs the org's
// own). Attribution — periods, aggregates, roster, overage pool, budgets,
// collection state, invoice mirror — always lives on the org account itself,
// so funding switches never move frozen state; they re-route only future
// charges (resolveChargeableCustomer's ChargeFundingAccount hop).
//
// AUTHZ CONTRACT: billing-engine trusts api-platform (internal RPC). The
// caller has already verified org membership + CanManage (owner|admin) and
// step-up reauth; this service enforces only the invariants it owns — the
// sponsor is the ACTING user's own account (never another member's wallet)
// and only the current sponsor may self-revoke.

import (
	"context"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// OrgFunding is the designation's funding instrument choice.
type OrgFunding string

const (
	// OrgFundingSponsor — invoices charge the sponsor's Stripe customer /
	// default PM; the org account activates immediately (anchor = designation
	// day, ADR 0006 amendment).
	OrgFundingSponsor OrgFunding = "sponsor"
	// OrgFundingOrg — invoices charge the org's own Stripe customer / card;
	// resolution stays gated on activated_at (stamped at card bind), so the
	// org remains unbilled until the bind completes.
	OrgFundingOrg OrgFunding = "org"
)

// OrgDesignation is the ms_billing.org_billing_designations row (migration
// 041). SponsorAccountID / SponsorUserID are Nil unless Funding is sponsor.
type OrgDesignation struct {
	OrgID                  uuid.UUID
	Funding                OrgFunding
	SponsorAccountID       uuid.UUID
	SponsorUserID          uuid.UUID
	DisclosedBacklogMicros int64
	UpdatedBy              uuid.UUID
}

// SetOrgDesignationRequest is the payload of the SetOrgDesignation RPC.
// ActingUserID is the org owner/admin performing the designation — for
// sponsor funding it IS the sponsor (billing-engine validates the sponsor
// account belongs to it; api-platform re-verified membership + CanManage +
// reauth before proxying).
type SetOrgDesignationRequest struct {
	OrgID        uuid.UUID `json:"org_id"`
	ActingUserID uuid.UUID `json:"acting_user_id"`
	Funding      string    `json:"funding"`
}

// SetOrgDesignationResponse reports the designation outcome. Activated is
// false only on a funding='org' designation whose card has not bound yet —
// the org stays unbilled until it does. DisclosedBacklogMicros echoes the
// recorded pre-designation backlog estimate (decision 1 disclosure);
// AttachedApps / RepointedEvents report the attach sweep (0 when not funded
// yet).
type SetOrgDesignationResponse struct {
	AccountID              uuid.UUID `json:"account_id"`
	Funding                string    `json:"funding"`
	Activated              bool      `json:"activated"`
	DisclosedBacklogMicros int64     `json:"disclosed_backlog_micros"`
	AttachedApps           int64     `json:"attached_apps"`
	RepointedEvents        int64     `json:"repointed_events"`
}

// GetOrgDesignationRequest reads the org's designation + funding state.
type GetOrgDesignationRequest struct {
	OrgID uuid.UUID `json:"org_id"`
}

// GetOrgDesignationResponse: Found=false → the org never designated.
// PendingBacklogMicros is the live unbilled-backlog estimate — the number the
// designation UI must show before the user confirms (decision 1). AccountID
// is Nil until the org account row exists. SponsorUserID lets api-platform
// enforce the sponsor-leave/demote guard and gate the self-revoke.
type GetOrgDesignationResponse struct {
	Found                bool      `json:"found"`
	Funding              string    `json:"funding,omitempty"`
	SponsorUserID        uuid.UUID `json:"sponsor_user_id,omitempty"`
	AccountID            uuid.UUID `json:"account_id,omitempty"`
	Activated            bool      `json:"activated"`
	PendingBacklogMicros int64     `json:"pending_backlog_micros"`
}

// RevokeSponsorshipRequest is the sponsor SELF-revoke: authorized by BEING
// the current sponsor (not by CanManage — a demoted sponsor must still be
// able to withdraw their own card).
type RevokeSponsorshipRequest struct {
	OrgID        uuid.UUID `json:"org_id"`
	ActingUserID uuid.UUID `json:"acting_user_id"`
}

// RevokeSponsorshipResponse: Revoked=false → no designation row existed
// (idempotent revoke).
type RevokeSponsorshipResponse struct {
	Revoked bool `json:"revoked"`
}

// RepointOrgUsageRequest triggers the attach/backfill sweep for an org whose
// funded account just resolved (fired by api-platform after a funding='org'
// card bind completes; SetOrgDesignation runs the same sweep inline for the
// sponsor path). Idempotent — attached rows and swept events never match
// again.
type RepointOrgUsageRequest struct {
	OrgID uuid.UUID `json:"org_id"`
}

// RepointOrgUsageResponse: Funded=false → the org has no funded designation
// yet (nothing swept; not an error — the caller may fire optimistically).
type RepointOrgUsageResponse struct {
	AccountID       uuid.UUID `json:"account_id,omitempty"`
	Funded          bool      `json:"funded"`
	AttachedApps    int64     `json:"attached_apps"`
	RepointedEvents int64     `json:"repointed_events"`
}

// ListSponsoredOrgsRequest is the payload of the /me sponsored-orgs read (the
// orgs SponsorUserID pays for). There is deliberately NO period selector: a
// billing_periods id is scoped to a SINGLE account, so the caller's personal
// period id has no meaning in a sponsored org's own period history — each org
// is always priced for its OWN current window. (A historical cross-account
// total would need date-range → per-org-period mapping, not an id; deferred.)
//
// AUTHZ CONTRACT: billing-engine trusts api-platform to have verified WHO the
// session user is; this read re-verifies nothing and simply filters by
// SponsorUserID — a user only ever sees the orgs their OWN account sponsors.
type ListSponsoredOrgsRequest struct {
	SponsorUserID uuid.UUID `json:"sponsor_user_id"`
}

// SponsoredOrg is one sponsored org's identity + current-window total. No
// name / slug / theme — billing-engine structurally lacks them; api-platform
// joins those from ms_organizations before serving the account endpoint.
type SponsoredOrg struct {
	OrgID       uuid.UUID `json:"org_id"`
	TotalMicros int64     `json:"total_micros"`
}

// ListSponsoredOrgsResponse carries the sponsored roster (empty slice, never
// nil, when the user sponsors nothing).
type ListSponsoredOrgsResponse struct {
	Orgs []SponsoredOrg `json:"orgs"`
}

// SetOrgDesignation writes the org's funding choice (design D1):
//
//   - sponsor: validates the sponsor is the ACTING user's own account with a
//     usable default PM, records the designation with the computed backlog
//     disclosure, activates the org account if this is its first funding
//     (anchor = designation day), and runs the attach sweep inline.
//
//   - org: records the designation; the account row is ensured but NOT
//     activated here — the card-bind webhook stamps activation, after which
//     api-platform fires RepointOrgUsage. When the account is ALREADY
//     activated (a funding switch back to the org's own card), the attach
//     sweep runs inline since resolution is already live.
func (s *Service) SetOrgDesignation(ctx context.Context, req SetOrgDesignationRequest) (*SetOrgDesignationResponse, error) {
	if req.OrgID == uuid.Nil {
		return nil, billing.InvalidInput("org_id required")
	}
	if req.ActingUserID == uuid.Nil {
		return nil, billing.InvalidInput("acting_user_id required")
	}
	funding := OrgFunding(req.Funding)
	if funding != OrgFundingSponsor && funding != OrgFundingOrg {
		return nil, billing.InvalidInput("funding must be 'sponsor' or 'org'")
	}

	accountID, err := s.store.EnsureOrgAccount(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("ensure org account failed", err)
	}

	d := OrgDesignation{
		OrgID:     req.OrgID,
		Funding:   funding,
		UpdatedBy: req.ActingUserID,
	}

	if funding == OrgFundingSponsor {
		// The sponsor is the acting user's OWN account — designating another
		// member's wallet is structurally impossible (the request carries no
		// sponsor field). It must already exist with a usable default PM: a
		// card-less sponsor would activate the org into a state where every
		// charge run skips no_pm.
		sponsorAccountID, found, err := s.store.AccountIDByUser(ctx, req.ActingUserID)
		if err != nil {
			return nil, billing.Internal("sponsor account lookup failed", err)
		}
		if !found {
			return nil, billing.InvalidInput("sponsor has no billing account — add a payment method first")
		}
		hasPM, err := s.store.HasUsableDefaultPM(ctx, sponsorAccountID)
		if err != nil {
			return nil, billing.Internal("sponsor payment-method check failed", err)
		}
		if !hasPM {
			return nil, billing.InvalidInput("sponsor has no usable default payment method — add a card first")
		}
		d.SponsorAccountID = sponsorAccountID
		d.SponsorUserID = req.ActingUserID
	}

	// Record the backlog estimate the UI disclosed pre-confirm (decision 1).
	// Computed server-side at write time so the recorded figure is the sweep's
	// contemporaneous view, not a stale client echo.
	backlog, err := s.store.OrgUnbilledBacklogMicros(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("unbilled backlog estimate failed", err)
	}
	d.DisclosedBacklogMicros = backlog

	if funding == OrgFundingSponsor {
		// Sponsor funding activates immediately — a usable instrument exists.
		// Activation is written BEFORE the designation row: a crash between the
		// two leaves an activated-but-undesignated account (invisible, unbilled,
		// fixed by retrying) rather than a designation that LOOKS configured but
		// never bills (the resolution gate requires activation). Idempotent —
		// the anchor is immutable once set (ADR 0006).
		if err := s.store.ActivateAccountIfUnset(ctx, accountID, s.nowFn().UTC()); err != nil {
			return nil, billing.Internal("activate org account failed", err)
		}
	}

	if err := s.store.UpsertOrgDesignation(ctx, d); err != nil {
		return nil, billing.Internal("write org designation failed", err)
	}

	resp := &SetOrgDesignationResponse{
		AccountID:              accountID,
		Funding:                string(funding),
		DisclosedBacklogMicros: backlog,
	}

	// Attach sweep — only once resolution is live (activated). For a fresh
	// funding='org' designation this is deferred to the card-bind completion
	// (api-platform fires RepointOrgUsage then).
	_, activated, err := s.store.AccountActivation(ctx, accountID)
	if err != nil {
		return nil, billing.Internal("account activation lookup failed", err)
	}
	resp.Activated = activated
	if activated {
		attached, repointed, err := s.attachOrgBilling(ctx, req.OrgID, accountID)
		if err != nil {
			return nil, err
		}
		resp.AttachedApps, resp.RepointedEvents = attached, repointed
	}
	return resp, nil
}

// GetOrgDesignation reads the org's designation + funding state, including
// the live pending-backlog estimate the designation UI must disclose.
func (s *Service) GetOrgDesignation(ctx context.Context, req GetOrgDesignationRequest) (*GetOrgDesignationResponse, error) {
	if req.OrgID == uuid.Nil {
		return nil, billing.InvalidInput("org_id required")
	}
	resp := &GetOrgDesignationResponse{}

	backlog, err := s.store.OrgUnbilledBacklogMicros(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("unbilled backlog estimate failed", err)
	}
	resp.PendingBacklogMicros = backlog

	accountID, accountFound, err := s.store.OrgAccountID(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("org account lookup failed", err)
	}
	if accountFound {
		resp.AccountID = accountID
		_, resp.Activated, err = s.store.AccountActivation(ctx, accountID)
		if err != nil {
			return nil, billing.Internal("account activation lookup failed", err)
		}
	}

	d, found, err := s.store.OrgDesignation(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("org designation lookup failed", err)
	}
	if !found {
		return resp, nil
	}
	resp.Found = true
	resp.Funding = string(d.Funding)
	resp.SponsorUserID = d.SponsorUserID
	return resp, nil
}

// RevokeSponsorship is the sponsor self-revoke: only the CURRENT sponsor may
// withdraw their card; the org drops to unbilled (new events record
// NULL-account) until it re-designates. Frozen attribution never rewrites —
// roster rows keep their account_id; already-attached state simply stops
// being fundable, which the charge legs surface as transient no_pm skips.
func (s *Service) RevokeSponsorship(ctx context.Context, req RevokeSponsorshipRequest) (*RevokeSponsorshipResponse, error) {
	if req.OrgID == uuid.Nil {
		return nil, billing.InvalidInput("org_id required")
	}
	if req.ActingUserID == uuid.Nil {
		return nil, billing.InvalidInput("acting_user_id required")
	}
	d, found, err := s.store.OrgDesignation(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("org designation lookup failed", err)
	}
	if !found {
		return &RevokeSponsorshipResponse{Revoked: false}, nil // idempotent
	}
	if d.Funding != OrgFundingSponsor {
		return nil, billing.InvalidInput("designation is not sponsor-funded")
	}
	if d.SponsorUserID != req.ActingUserID {
		return nil, billing.InvalidInput("only the current sponsor may revoke sponsorship")
	}
	revoked, err := s.store.DeleteOrgDesignation(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("delete org designation failed", err)
	}
	return &RevokeSponsorshipResponse{Revoked: revoked}, nil
}

// RepointOrgUsage runs the attach/backfill sweep for an org whose funded
// account resolves — fired by api-platform after a funding='org' card bind
// completes (the sponsor path sweeps inline in SetOrgDesignation). Unfunded
// orgs report Funded=false without error so optimistic fire-and-forget
// callers need no state machine.
func (s *Service) RepointOrgUsage(ctx context.Context, req RepointOrgUsageRequest) (*RepointOrgUsageResponse, error) {
	if req.OrgID == uuid.Nil {
		return nil, billing.InvalidInput("org_id required")
	}
	accountID, funded, err := s.store.ResolveOrgFundedAccount(ctx, req.OrgID)
	if err != nil {
		return nil, billing.Internal("org account resolution failed", err)
	}
	if !funded {
		return &RepointOrgUsageResponse{Funded: false}, nil
	}
	attached, repointed, err := s.attachOrgBilling(ctx, req.OrgID, accountID)
	if err != nil {
		return nil, err
	}
	return &RepointOrgUsageResponse{
		AccountID:       accountID,
		Funded:          true,
		AttachedApps:    attached,
		RepointedEvents: repointed,
	}, nil
}

// ListSponsoredOrgs returns the orgs the user sponsors, each with its
// current-window total. It resolves the sponsored
// org_ids from the designation table, then prices EACH org through the SAME
// audited account-bill spine the /me and /orgs bills read (GetAccountBill for
// OwnerOrgID) — one pricing path, no second rollup. Empty (never nil) when the
// user sponsors nothing. Identity (name/slug/theme) is joined downstream by
// api-platform; billing-engine returns only org_id + total_micros.
func (s *Service) ListSponsoredOrgs(ctx context.Context, req ListSponsoredOrgsRequest) (*ListSponsoredOrgsResponse, error) {
	if req.SponsorUserID == uuid.Nil {
		return nil, billing.InvalidInput("sponsor_user_id required")
	}
	if s.bill == nil {
		return nil, billing.Internal("account-bill dependency not wired (WithAccountBill)", nil)
	}

	orgIDs, err := s.store.ListSponsoredOrgIDs(ctx, req.SponsorUserID)
	if err != nil {
		return nil, billing.Internal("sponsored org lookup failed", err)
	}

	orgs := make([]SponsoredOrg, 0, len(orgIDs))
	for _, orgID := range orgIDs {
		bill, err := s.bill.GetAccountBill(ctx, usage.GetAccountBillRequest{
			OwnerOrgID: orgID,
		})
		if err != nil {
			return nil, err
		}
		orgs = append(orgs, SponsoredOrg{OrgID: orgID, TotalMicros: bill.TotalMicros})
	}
	return &ListSponsoredOrgsResponse{Orgs: orgs}, nil
}

// attachOrgBilling is the shared attach/backfill sweep body: backfill
// account_id onto the org's unbilled roster rows, fold its NULL-account
// events into the account's CURRENT OPEN window (clamped so they bill in the
// first period that closes after designation — decision 1), then synthesize
// each live app's overage timers fresh, anchored NOW (prospective billing:
// grace runs from designation, never retroactively). Every step is
// idempotent, so a crashed sweep re-runs safely via RepointOrgUsage.
func (s *Service) attachOrgBilling(ctx context.Context, orgID, accountID uuid.UUID) (attached, repointed int64, err error) {
	attached, err = s.store.AttachOrgAppsToAccount(ctx, orgID, accountID)
	if err != nil {
		return 0, 0, billing.Internal("attach org apps failed", err)
	}

	activatedAt, activated, err := s.store.AccountActivation(ctx, accountID)
	if err != nil {
		return 0, 0, billing.Internal("account activation lookup failed", err)
	}
	if !activated {
		// Callers gate on funded resolution (which requires activation); an
		// unactivated account here is a code bug, not a skip.
		return 0, 0, billing.Internal("attach sweep reached an unactivated account", nil)
	}
	now := s.nowFn().UTC()
	windowStart, _ := billingperiod.AnchoredPeriodWindow(now, billingperiod.AnchorDay(activatedAt))

	repointed, err = s.store.RepointOrgNullAccountEvents(ctx, orgID, accountID, windowStart)
	if err != nil {
		return 0, 0, billing.Internal("repoint org usage events failed", err)
	}

	// Fresh timers for every live app — the reconcile derives count + account
	// from the roster row inside its own advisory-locked transaction, so a
	// concurrent RegisterApp/SyncAppModules retry can never double-insert.
	apps, err := s.store.OrgLiveAppIDs(ctx, orgID)
	if err != nil {
		return 0, 0, billing.Internal("org app enumeration failed", err)
	}
	for _, appID := range apps {
		if err := s.store.ReconcileModuleTimersToTarget(ctx, appID, now, moduleGraceExpiry(now), now); err != nil {
			return 0, 0, billing.Internal("reconcile module overage timers failed", err)
		}
	}
	return attached, repointed, nil
}
