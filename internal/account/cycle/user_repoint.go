package cycle

// The user twin of the org attach sweep's events half (core-v2#340). Usage a
// user-rostered app records while its payer has no account lands with
// account_id NULL, and the org sweep — scoped through apps.owner_org_id —
// never reaches it. It is driven from the same two places as the org twin:
// the daily billing-cycle pass (SweepUnattachedUserUsage) and an on-demand
// RPC (RepointUserUsage). There is no roster half: a user-rostered row always
// carries its account (migration 041's CHECK), so only events can be stranded.

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/billingperiod"
)

// UserUsageSweepSummary reports what one user repoint pass did.
type UserUsageSweepSummary struct {
	Accounts        int   // user accounts the work list returned
	Swept           int   // accounts whose repoint completed
	RepointedEvents int64 // NULL-account events handed to their account
	Failed          int   // per-account failures (counted, never abort the pass)
}

// RepointUserUsageRequest triggers the user repoint for one user's account.
// Idempotent — swept events never match again.
type RepointUserUsageRequest struct {
	OwnerUserID uuid.UUID `json:"owner_user_id"`
}

// RepointUserUsageResponse: Funded=false → the user has no activated account
// yet (nothing swept; not an error — the caller may fire optimistically).
// UnbilledBacklogMicros prices the NULL rows older than the open window,
// which the repoint leaves unbilled (D1d).
type RepointUserUsageResponse struct {
	AccountID             uuid.UUID `json:"account_id,omitempty"`
	Funded                bool      `json:"funded"`
	RepointedEvents       int64     `json:"repointed_events"`
	UnbilledBacklogMicros int64     `json:"unbilled_backlog_micros"`
}

// SweepUnattachedUserUsage is the self-healing pass: every activated user
// account the work list names gets its open-window NULL rows repointed.
func (s *Service) SweepUnattachedUserUsage(ctx context.Context) (*UserUsageSweepSummary, error) {
	accounts, err := s.store.UsersWithUnsweptUsage(ctx)
	if err != nil {
		return nil, billing.Internal("list users with unswept usage failed", err)
	}
	summary := &UserUsageSweepSummary{Accounts: len(accounts)}
	for _, accountID := range accounts {
		repointed, _, activated, err := s.repointUserBilling(ctx, accountID, false)
		if err == nil && !activated {
			// The work list selects activated accounts only.
			err = billing.Internal("user repoint reached an unactivated account", nil)
		}
		if err != nil {
			slog.ErrorContext(ctx, "user usage repoint failed", "account_id", accountID, "error", err)
			summary.Failed++
			continue
		}
		summary.Swept++
		summary.RepointedEvents += repointed
	}
	return summary, nil
}

// RepointUserUsage runs the user repoint for the user's account when it is
// activated, and reports the backlog it leaves behind.
func (s *Service) RepointUserUsage(ctx context.Context, req RepointUserUsageRequest) (*RepointUserUsageResponse, error) {
	if req.OwnerUserID == uuid.Nil {
		return nil, billing.InvalidInput("owner_user_id required")
	}
	accountID, found, err := s.store.AccountIDByUser(ctx, req.OwnerUserID)
	if err != nil {
		return nil, billing.Internal("user account lookup failed", err)
	}
	if !found {
		return &RepointUserUsageResponse{Funded: false}, nil
	}
	repointed, backlog, activated, err := s.repointUserBilling(ctx, accountID, true)
	if err != nil {
		return nil, err
	}
	if !activated {
		return &RepointUserUsageResponse{Funded: false}, nil
	}
	return &RepointUserUsageResponse{
		AccountID:             accountID,
		Funded:                true,
		RepointedEvents:       repointed,
		UnbilledBacklogMicros: backlog,
	}, nil
}

// repointUserBilling is the shared body: resolve the account's open window
// from its activation anchor, repoint into it, and (withBacklog) price what
// stays behind. activated=false → no anchor, nothing touched.
func (s *Service) repointUserBilling(ctx context.Context, accountID uuid.UUID, withBacklog bool) (repointed, backlog int64, activated bool, err error) {
	activatedAt, activated, err := s.store.AccountActivation(ctx, accountID)
	if err != nil || !activated {
		if err != nil {
			err = billing.Internal("account activation lookup failed", err)
		}
		return 0, 0, false, err
	}
	windowStart, _ := billingperiod.AnchoredPeriodWindow(s.nowFn().UTC(), billingperiod.AnchorDay(activatedAt))

	repointed, err = s.store.RepointUserNullAccountEvents(ctx, accountID, windowStart)
	if err != nil {
		return 0, 0, true, billing.Internal("repoint user usage events failed", err)
	}
	if withBacklog {
		backlog, err = s.store.UserUnbilledBacklogMicros(ctx, accountID, windowStart)
		if err != nil {
			return 0, 0, true, billing.Internal("user unbilled backlog failed", err)
		}
	}
	return repointed, backlog, true, nil
}
