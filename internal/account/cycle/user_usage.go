package cycle

// The lazy USER usage sweep (migration 088, core-v2#340) — the user twin of
// the org attach sweep in org.go.
//
// A user-owned app's usage recorded while its owner had no ms_billing.accounts
// row lands with account_id NULL (usage.RecordUsage / RecordInfraUsage). Every
// bill read filters on account_id, so until something gives the row an account
// it is retained and never billed. The org path has always had that something
// (RepointOrgNullAccountEvents, found through the roster's owner_org_id); the
// user path had nothing, and the row did not even name its user. Migration 088
// stamps it (usage_events.owner_user_id), and this file is what reads the
// stamp: a daily self-healing pass that hands the stamped rows inside the
// user's open window to the user's account once it activates, and the
// disclosure read that says what that pass will and will not bill.
//
// 🔴 ATTRIBUTION IS THE INGEST STAMP, NEVER THE APPS ROSTER. The roster's
// account_id names an app's PAYER, and during api-platform's payer re-seat —
// before TransferApp lands, or while it is refused (TransferTargetUnfunded) —
// it still names the OLD payer while ingest already stamps the NEW one. A
// roster-keyed sweep would bill the old payer for the new payer's usage. The
// transfer's own NULL-row rules (transfer_store.go, app_transfer.sql
// RepointAppNullAccountEventsOnTransfer) rest on the same fact.
//
// 🔴 ONLY THE OPEN WINDOW IS CAUGHT UP (D1d; decision D125). A stamped row
// older than the account's open window stays NULL and unbilled forever — the
// rule the transfer applies to the same rows. The org sweep instead clamps its
// whole backlog forward, because an org DESIGNATED funding after that backlog
// was disclosed to it; a user binding a card made no such choice.
// GetUserUnbilledBacklog reports both figures, so the difference is never
// silent.

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// UserUsageAccount is one entry of the user sweep's work list: an activated
// user account and the owner user its lazy rows were stamped with.
type UserUsageAccount struct {
	AccountID uuid.UUID
	UserID    uuid.UUID
}

// UserUsageSweepSummary reports what one user sweep pass did.
type UserUsageSweepSummary struct {
	Accounts        int   // accounts the work list returned
	Swept           int   // accounts whose repoint completed (0 rows included)
	RepointedEvents int64 // stamped lazy events handed to the user's account
	Failed          int   // per-account failures (counted, never abort the pass)
}

// SweepUnattachedUserUsage is the user half of the self-healing attach path,
// run beside SweepUnattachedOrgUsage by cmd/billing-cycle. Nothing fires it
// per user — no RPC marks the moment a user's account activates for this
// purpose — so this pass is the whole mechanism: each run repoints, for every
// activated user account with stamped lazy rows in reach, the rows inside the
// account's CURRENT open window. There is no timer reconcile: a user's apps
// are rostered to the user's account at registration, so there is nothing to
// attach.
func (s *Service) SweepUnattachedUserUsage(ctx context.Context) (*UserUsageSweepSummary, error) {
	now := s.nowFn().UTC()
	accounts, err := s.store.UserAccountsWithUnsweptUsage(ctx, now)
	if err != nil {
		return nil, billing.Internal("list user accounts with unswept usage failed", err)
	}
	summary := &UserUsageSweepSummary{Accounts: len(accounts)}
	for _, a := range accounts {
		activatedAt, activated, err := s.store.AccountActivation(ctx, a.AccountID)
		if err != nil {
			slog.ErrorContext(ctx, "account activation lookup for user usage sweep failed",
				"account_id", a.AccountID, "user_id", a.UserID, "error", err)
			summary.Failed++
			continue
		}
		if !activated {
			// The work list selects activated accounts only and the anchor is
			// immutable once set, so this is unreachable short of a bug. Skip
			// rather than fail: an unactivated account is never billed (D1d),
			// and its rows wait for the activation that opens a window.
			continue
		}
		windowStart := userSweepWindowStart(now, activatedAt)
		repointed, err := s.store.RepointUserNullAccountEvents(ctx, a.UserID, a.AccountID, windowStart)
		if err != nil {
			slog.ErrorContext(ctx, "user usage attach sweep failed",
				"account_id", a.AccountID, "user_id", a.UserID, "error", err)
			summary.Failed++
			continue
		}
		summary.Swept++
		summary.RepointedEvents += repointed
	}
	return summary, nil
}

// userSweepWindowStart is the open window the sweep bills stamped rows into —
// the account's anchored window at now, derived exactly as attachOrgBilling
// derives the org account's.
func userSweepWindowStart(now, activatedAt time.Time) time.Time {
	start, _ := billingperiod.AnchoredPeriodWindow(now, billingperiod.AnchorDay(activatedAt))
	return start
}

// GetUserUnbilledBacklogRequest reads the unbilled lazy usage stamped for one
// user.
type GetUserUnbilledBacklogRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

// GetUserUnbilledBacklogResponse is the user twin of the org disclosure
// (GetOrgDesignationResponse.PendingBacklogMicros), priced exactly like it.
//
// PendingBacklogMicros is the DISCLOSURE figure: the part of the stamped
// backlog the user sweep WILL bill — the rows inside the account's current
// open window, or, for a user with no account or an unactivated one, inside
// the window a card bound NOW would open. 🔴 A customer is never charged an
// accrued amount the UI could not show it first, so this is the number to
// show before a card is bound.
//
// UnbilledBacklogMicros is every stamped lazy row, whatever its age.
// Unbilled − pending is what D1d leaves unbilled for good: usage recorded
// before the window the account can bill into.
type GetUserUnbilledBacklogResponse struct {
	AccountID             uuid.UUID `json:"account_id,omitempty"`
	Activated             bool      `json:"activated"`
	UnbilledBacklogMicros int64     `json:"unbilled_backlog_micros"`
	PendingBacklogMicros  int64     `json:"pending_backlog_micros"`
}

// GetUserUnbilledBacklog reports the user's stamped lazy backlog and the part
// of it the sweep will bill. Read-only; the sweep itself runs from
// cmd/billing-cycle.
func (s *Service) GetUserUnbilledBacklog(ctx context.Context, req GetUserUnbilledBacklogRequest) (*GetUserUnbilledBacklogResponse, error) {
	if req.UserID == uuid.Nil {
		return nil, billing.InvalidInput("user_id required")
	}
	resp := &GetUserUnbilledBacklogResponse{}
	now := s.nowFn().UTC()

	accountID, found, err := s.store.AccountIDByUser(ctx, req.UserID)
	if err != nil {
		return nil, billing.Internal("user account lookup failed", err)
	}
	// With no activated account the pending window is the one a card bound now
	// would open: activation anchors at the bind instant, so its first window
	// starts on today's UTC day.
	windowStart := userSweepWindowStart(now, now)
	if found {
		resp.AccountID = accountID
		activatedAt, activated, err := s.store.AccountActivation(ctx, accountID)
		if err != nil {
			return nil, billing.Internal("account activation lookup failed", err)
		}
		resp.Activated = activated
		if activated {
			windowStart = userSweepWindowStart(now, activatedAt)
		}
	}

	unbilled, err := s.store.UserUnbilledBacklogMicros(ctx, req.UserID, time.Time{})
	if err != nil {
		return nil, billing.Internal("unbilled backlog estimate failed", err)
	}
	pending, err := s.store.UserUnbilledBacklogMicros(ctx, req.UserID, windowStart)
	if err != nil {
		return nil, billing.Internal("pending backlog estimate failed", err)
	}
	resp.UnbilledBacklogMicros = unbilled
	resp.PendingBacklogMicros = pending
	return resp, nil
}
