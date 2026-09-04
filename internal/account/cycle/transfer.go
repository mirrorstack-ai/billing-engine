package cycle

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
)

// TransferAppRequest moves an app's billing account to another owner.
//
// Flat snake_case with the owner fields as a XOR pair, matching
// RegisterAppRequest — api-platform already mirrors that shape, and the RPC
// surface here is `POST /v1/billing.TransferApp` with the app id in the BODY.
// This service serves no REST path parameters.
type TransferAppRequest struct {
	// AppID is the platform app id (ms_apps.id), as mirrored by RegisterApp.
	AppID uuid.UUID `json:"app_id"`

	// Exactly one of these is non-zero: the owner the billing account moves TO.
	OwnerUserID uuid.UUID `json:"owner_user_id,omitempty"`
	OwnerOrgID  uuid.UUID `json:"owner_org_id,omitempty"`

	// Mode decides what happens to USAGE only, and is never defaulted —
	// omitting it is INVALID_INPUT rather than a silent "keep", because the
	// two modes bill different accounts and a caller that forgot the field has
	// not chosen.
	//
	//   keep — no event moves. The old account keeps everything it recorded.
	//   move — this app's not-yet-invoiced usage in the overlapping open window
	//          is re-attributed to the new account.
	//
	// The account re-key itself is NOT conditional on mode: see TransferApp.
	Mode string `json:"mode"`

	// RequestID is api-platform's transfer id, used as the idempotency key. A
	// replay with the same RequestID returns the stored result; the same
	// RequestID against a different target is a CONFLICT, never a second
	// transfer.
	RequestID uuid.UUID `json:"request_id"`
}

// TransferAppResponse reports what the transfer did.
//
// Every instant is a time.Time and marshals as an RFC3339 UTC timestamp —
// api-platform decodes the whole response into typed fields, so a bare date
// string here would fail its decode AFTER the money had already moved.
type TransferAppResponse struct {
	// AccountID is the app's billing account after the transfer.
	AccountID uuid.UUID `json:"account_id"`

	// MovedEventCount is how many usage/infra events were re-attributed.
	// Always 0 for mode="keep". On a replay this is the STORED count, not a
	// recount — the second caller must see what the first one did.
	MovedEventCount int64 `json:"moved_event_count"`

	// OpenPeriod is the TARGET account's open window: the period the app now
	// bills in. On a replay this, like RecurringFrom, is the STORED window
	// from the first call, not one recomputed from the replay's clock.
	OpenPeriod TransferPeriod `json:"open_period"`

	// RecurringFrom is the new account's next anchored boundary — the first
	// boundary at which this app's recurring fees bill to the new account.
	//
	// It is NOT the transfer instant, and the gap is not a rounding error.
	// Recurring fees are PREPAID at the boundary that OPENS a period
	// (charge.go: boundaryTotal = arrears + advanceBase + advanceOverage +
	// advanceDomains, where the advance legs cover the period that is just
	// starting). So the period containing this transfer was already paid for by
	// the OLD account and cannot move: no refund, no proration, matching this
	// schema's prospective-removal posture. Because periods are anchored PER
	// ACCOUNT, the two boundaries differ, leaving at most one whole unit of gap
	// or overlap in recurring coverage. Accepted for v1 and surfaced here so
	// the console can tell the customer the date rather than let them discover
	// it on a bill.
	RecurringFrom time.Time `json:"recurring_from"`
}

// TransferPeriod is a half-open [start, end) billing window.
type TransferPeriod struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// Transfer modes. Closed set; anything else is INVALID_INPUT.
const (
	TransferModeKeep = "keep"
	TransferModeMove = "move"
)

// TransferApp re-points an app's billing account to another owner.
//
// WHAT ALWAYS HAPPENS, in both modes and in ONE transaction: ms_billing.apps
// (account_id + the owner columns) and the app's live app_module_overage_timers
// and app_custom_domains rows all move to the new account. That is unconditional
// because the expensive direction is being LATE, not early: an app whose roster
// row still names the old account at the next boundary makes that account prepay
// another whole period of base, module-overage and domain fees for an app it no
// longer owns — against a schema that never credits an already-charged period.
//
// WHAT MODE DECIDES: usage only. See TransferAppRequest.Mode.
//
// WHAT NEVER HAPPENS: an issued or closed invoice is never touched, and no
// usage is re-attributed into a period the target account has already closed
// (INV-011 — a transfer may not rewrite a fact that has already been billed).
//
// Idempotency: the (request_id) row in app_transfer_events is the record. A
// replay returns it verbatim — the count, the window and recurring_from are
// all read from the row, none recomputed — and the same request_id aimed at
// a different target is a conflict.
func (s *Service) TransferApp(ctx context.Context, req TransferAppRequest) (*TransferAppResponse, error) {
	if req.AppID == uuid.Nil {
		return nil, billing.InvalidInput("app_id required")
	}
	if req.RequestID == uuid.Nil {
		return nil, billing.InvalidInput("request_id required")
	}
	if req.OwnerUserID == uuid.Nil && req.OwnerOrgID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id or owner_org_id required")
	}
	if req.OwnerUserID != uuid.Nil && req.OwnerOrgID != uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id and owner_org_id are mutually exclusive")
	}
	if req.Mode != TransferModeKeep && req.Mode != TransferModeMove {
		return nil, billing.InvalidInput(`mode must be "keep" or "move"`)
	}

	target, err := s.transferTargetAccount(ctx, req.OwnerUserID, req.OwnerOrgID)
	if err != nil {
		return nil, err
	}

	resp, outcome, err := s.store.TransferApp(ctx, TransferAppParams{
		AppID:       req.AppID,
		RequestID:   req.RequestID,
		ToAccount:   target,
		OwnerUserID: req.OwnerUserID,
		OwnerOrgID:  req.OwnerOrgID,
		Mode:        req.Mode,
		At:          s.nowFn().UTC(),
	})
	if err != nil {
		return nil, billing.Internal("app transfer failed", err)
	}

	// billing.Code is a CLOSED set, so the token travels as a message prefix
	// and api-platform matches on the CODE. Documented on the RPC contract.
	switch outcome {
	case TransferAppUnknown:
		return nil, billing.NotFound("app_unknown: no billing mirror for this app")
	case TransferRequestConflict:
		return nil, billing.Conflict("app_transfer_conflict: this request_id already transferred a different app or target")
	case TransferPeriodClosed:
		return nil, billing.Conflict("app_transfer_period_closed: a billing period for one of these accounts closed while the transfer ran; retry")
	case TransferChargesPending:
		return nil, billing.Conflict("app_transfer_charges_pending: a one-time charge for this app is still settling; retry after the sweeps run")
	}
	return resp, nil
}

// transferTargetAccount resolves — and creates if absent — the billing account
// of the owner an app is being transferred TO.
//
// 🔴 THIS IS DELIBERATELY NOT fundedOwnerAccount, AND MUST NOT BECOME IT.
// That function refuses an owner with no account, no activation, or no usable
// card, because refusing to CREATE an app is cheap and reversible. Refusing to
// TRANSFER one is not: the app already exists and is already accruing, so a
// refusal here would strand it on an owner who has asked to stop paying for it.
// The owner's decision (2026-09-04) is that an unfunded destination is allowed —
// the account simply reads blocked and the existing serving-block does its job.
//
// It is a separate function rather than a flag on fundedOwnerAccount because
// that function has exactly one caller and four tests pinning its refusals;
// parameterising it would put the app-CREATION funding gate one boolean away
// from being switched off by a future caller.
func (s *Service) transferTargetAccount(ctx context.Context, ownerUserID, ownerOrgID uuid.UUID) (uuid.UUID, error) {
	if ownerOrgID != uuid.Nil {
		id, err := s.store.EnsureOrgAccount(ctx, ownerOrgID)
		if err != nil {
			return uuid.Nil, billing.Internal("org account resolution failed", err)
		}
		return id, nil
	}
	id, err := s.store.EnsureUserAccount(ctx, ownerUserID)
	if err != nil {
		return uuid.Nil, billing.Internal("owner account resolution failed", err)
	}
	return id, nil
}
