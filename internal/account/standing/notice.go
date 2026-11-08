package standing

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// standingNoticeBody is the wire body of api-platform's
// POST /internal/apps/standing-notice for kind "suspended" (api-platform#1090;
// pinned by testdata/standing_notice_suspended.request.json, which matches that
// route's golden request shape). Exactly one owner field is set; api-platform
// resolves an organization's owners itself, so no address is ever sent.
type standingNoticeBody struct {
	OwnerUserID    string `json:"owner_user_id,omitempty"`
	OwnerOrgID     string `json:"owner_org_id,omitempty"`
	Kind           string `json:"kind"`
	ReasonCode     string `json:"reason_code"`
	IdempotencyKey string `json:"idempotency_key"`
}

func ownerKey(o Owner) string {
	if o.OrgID != uuid.Nil {
		return "org:" + o.OrgID.String()
	}
	return "user:" + o.UserID.String()
}

// observeTransition records the verdict and reports whether a "suspended"
// mail is due. It never fails the caller: a state-store error is logged (no
// owner id) and means "no mail" — a missed mail is recoverable by hand, a
// duplicate one is not.
//
// Mail is due only on eligible -> blocked. A first observation counts as a
// transition into blocked unless it is NO_USABLE_CARD (an account that never
// added a card is new, not suspended). blocked -> blocked (a reason change),
// blocked -> eligible and not-blocked verdicts (grace included) never mail.
func (n *Notifier) observeTransition(ctx context.Context, owner Owner, blocked bool, reason string) (standingNoticeBody, bool) {
	if n.state == nil {
		return standingNoticeBody{}, false
	}
	now := n.now()
	prior, changedAt, err := n.state.Observe(ctx, ownerKey(owner), blocked, reason, now)
	if err != nil {
		n.log.ErrorContext(ctx, "standing state: record failed (no suspension mail this event)", "error", err)
		return standingNoticeBody{}, false
	}
	if !blocked {
		return standingNoticeBody{}, false
	}
	if prior == nil {
		if reason == reasonNoUsableCard {
			return standingNoticeBody{}, false
		}
	} else if prior.Blocked {
		return standingNoticeBody{}, false
	}
	if !n.noticeOn {
		return standingNoticeBody{}, false
	}
	body := standingNoticeBody{
		Kind:       "suspended",
		ReasonCode: reason,
	}
	id := owner.UserID.String()
	if owner.OrgID != uuid.Nil {
		id = owner.OrgID.String()
		body.OwnerOrgID = id
	} else {
		body.OwnerUserID = id
	}
	body.IdempotencyKey = fmt.Sprintf("blocked:%s:%d", id, changedAt.Unix())
	return body, true
}

// sendSuspensionNotice POSTs the notice. Best-effort: a failure (502 from the
// sender, 404 before the route is deployed, timeout) is logged ALERT-level with
// the idempotency key — and nothing else identifying — so the operator can
// replay the same request by hand; it never touches the state row, the
// serving-block push or the webhook's result.
func (n *Notifier) sendSuspensionNotice(ctx context.Context, body standingNoticeBody) {
	if err := n.postJSON(ctx, standingNoticePath, body); err != nil {
		n.log.ErrorContext(ctx, "standing notice: suspension mail NOT sent (best-effort; resend by hand with this key)",
			"alert", true, "idempotency_key", body.IdempotencyKey, "reason", body.ReasonCode, "error", err)
		return
	}
	n.log.InfoContext(ctx, "standing notice: suspension mail requested", "reason", body.ReasonCode)
}
