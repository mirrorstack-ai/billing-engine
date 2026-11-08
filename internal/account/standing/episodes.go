package standing

// The 14-day unpaid notice (billing-engine#330, core-v2#1881, owner order D592).
//
// The Terms promise 14 days' notice by email before paid features are limited
// for an unpaid bill. billing-engine#329 made the gate hold a block back until
// 14 days after a notice was sent (ms_billing.unpaid_episodes.notice_sent_at);
// this file is the writer: it opens an episode when an unpaid-bill gate starts
// failing, sends the notice through api-platform, stamps notice_sent_at, closes
// the episode when the account pays, and gives the daily sweep its work list.
//
// Everything here is behind UNPAID_GRACE=on (default off): flag off means no
// episode row, no standing-notice POST and no sweep work. Failure always falls
// toward service: a failed notice leaves notice_sent_at NULL, the gate then
// never blocks on the unpaid-bill reasons, and the next event or sweep retries.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/eligibility"
)

// standingNoticePath is api-platform's internal email route (api-platform#1090),
// joined onto APPLICATIONS_INTERNAL_URL.
const standingNoticePath = "/internal/apps/standing-notice"

// noticeKindUnpaid is the standing-notice kind for the pre-block notice.
const noticeKindUnpaid = "unpaid_notice"

// noticeAlertAfter is the failed-send count per episode at which an ALERT-level
// log line is raised (and on every later failure).
const noticeAlertAfter = 3

// Sweep bounds: pages of sweepBatch episodes, at most sweepMaxPages per run.
const (
	sweepBatch    = 200
	sweepMaxPages = 10
)

// DueEpisode is one open episode the sweep must look at, with the owner to
// re-evaluate.
type DueEpisode struct {
	ID    uuid.UUID
	Owner Owner
}

// EpisodeStore is the persistence the episode writer and the sweep need.
type EpisodeStore interface {
	// AccountForOwner resolves the account GetServiceStatus reads for this owner
	// (the same resolution, so the episode lives on the account the gates use).
	AccountForOwner(ctx context.Context, owner Owner) (uuid.UUID, bool, error)
	// OpenUnpaidEpisodeIfNone returns the account's open episode, opening one
	// started at `at` when none is open.
	OpenUnpaidEpisodeIfNone(ctx context.Context, accountID uuid.UUID, at time.Time) (billing.UnpaidEpisode, error)
	// CloseUnpaidEpisode closes the account's open episode.
	CloseUnpaidEpisode(ctx context.Context, accountID uuid.UUID, at time.Time) (bool, error)
	// SetUnpaidEpisodeNotice stamps notice_sent_at on the open episode, once.
	SetUnpaidEpisodeNotice(ctx context.Context, accountID uuid.UUID, at time.Time) (bool, error)
	// RecordNoticeFailure bumps the episode's failed-send counter and returns it.
	RecordNoticeFailure(ctx context.Context, episodeID uuid.UUID) (int, error)
	// DueUnpaidEpisodes lists open episodes, ordered by id after `after`, whose
	// notice is unsent or whose 14 days are over at `now`.
	DueUnpaidEpisodes(ctx context.Context, now time.Time, after uuid.UUID, limit int) ([]DueEpisode, error)
}

// WithEpisodes wires the episode store. A nil store leaves the episode writer
// and the sweep inert.
func (n *Notifier) WithEpisodes(s EpisodeStore) *Notifier {
	n.episodes = s
	return n
}

// WithUnpaidGrace pins the grace on or off, overriding the UNPAID_GRACE env
// flag (tests, explicit wiring).
func (n *Notifier) WithUnpaidGrace(enabled bool) *Notifier {
	n.grace = &enabled
	return n
}

// WithClock replaces the notifier clock (default time.Now). Tests only.
func (n *Notifier) WithClock(now func() time.Time) *Notifier {
	n.now = now
	return n
}

// GraceActive reports whether the episode writer and the sweep will run.
func (n *Notifier) GraceActive() bool {
	return n != nil && n.episodes != nil && n.Enabled() && n.graceOn()
}

// graceOn mirrors billing.Service: UNPAID_GRACE=on enables; anything else is off.
func (n *Notifier) graceOn() bool {
	if n.grace != nil {
		return *n.grace
	}
	return os.Getenv("UNPAID_GRACE") == "on"
}

func (n *Notifier) clock() time.Time {
	if n.now != nil {
		return n.now()
	}
	return time.Now()
}

// unpaidGateReasons are the gates (2-4) that the 14-day notice covers.
var unpaidGateReasons = []string{
	string(eligibility.ReasonFirstChargeFailed),
	string(eligibility.ReasonTooManyFailures),
	string(eligibility.ReasonUnpaidInvoices),
}

// primaryUnpaidReason is the first failing unpaid-bill gate in priority order,
// "" when none fails.
func primaryUnpaidReason(reasons []string) string {
	for _, want := range unpaidGateReasons {
		for _, got := range reasons {
			if got == want {
				return want
			}
		}
	}
	return ""
}

func hasReasonCode(reasons []string, code eligibility.Reason) bool {
	for _, r := range reasons {
		if r == string(code) {
			return true
		}
	}
	return false
}

// trackEpisode is the episode writer, run by notify after the verdict is read.
// Best-effort: it logs and returns, never changing the verdict push.
//
//   - no unpaid-bill gate failing → close the open episode (paying within the
//     window cancels the block);
//   - gate 1 (no usable card) or the wallet gate decides the verdict → touch
//     nothing: those block at once and are not what the notice is about;
//   - otherwise open an episode if none is open, and send the notice if it has
//     not gone out.
func (n *Notifier) trackEpisode(ctx context.Context, owner Owner, verdict *billing.GetServiceStatusResponse, refKind, refID string) {
	acct, found, err := n.episodes.AccountForOwner(ctx, owner)
	if err != nil {
		n.log.ErrorContext(ctx, "unpaid episode: account resolution failed", refKind, refID, "error", err)
		return
	}
	if !found {
		return
	}
	now := n.clock().UTC()
	reason := primaryUnpaidReason(verdict.Reasons)
	if reason == "" {
		if _, err := n.episodes.CloseUnpaidEpisode(ctx, acct, now); err != nil {
			n.log.ErrorContext(ctx, "unpaid episode: close failed", refKind, refID, "error", err)
		}
		return
	}
	if hasReasonCode(verdict.Reasons, eligibility.ReasonNoUsableCard) ||
		verdict.Reason == string(eligibility.ReasonOutOfCredits) {
		return
	}
	ep, err := n.episodes.OpenUnpaidEpisodeIfNone(ctx, acct, now)
	if err != nil {
		n.log.ErrorContext(ctx, "unpaid episode: open failed", refKind, refID, "error", err)
		return
	}
	if ep.NoticeSentAt != nil {
		return
	}
	n.sendUnpaidNotice(ctx, owner, acct, ep, reason)
}

// standingNoticeBody is the api-platform#1090 wire body for kind unpaid_notice.
type standingNoticeBody struct {
	OwnerUserID    string `json:"owner_user_id,omitempty"`
	OwnerOrgID     string `json:"owner_org_id,omitempty"`
	Kind           string `json:"kind"`
	ReasonCode     string `json:"reason_code"`
	LimitAt        string `json:"limit_at"`
	IdempotencyKey string `json:"idempotency_key"`
}

// sendUnpaidNotice sends the notice and records it. The instant is computed
// ONCE: notice_sent_at is sentAt and the email carries limit_at = sentAt + 14 d,
// which is exactly the instant the gate (notice_sent_at + 14 d) blocks at.
// The idempotency key is per episode, so a retry after a lost response does not
// send a second email.
func (n *Notifier) sendUnpaidNotice(ctx context.Context, owner Owner, accountID uuid.UUID, ep billing.UnpaidEpisode, reason string) {
	sentAt := n.clock().UTC().Truncate(time.Second)
	limit := sentAt.Add(eligibility.UnpaidNoticePeriod)
	body := standingNoticeBody{
		Kind:           noticeKindUnpaid,
		ReasonCode:     reason,
		LimitAt:        limit.Format(time.RFC3339),
		IdempotencyKey: "unpaid:" + ep.ID.String(),
	}
	if owner.OrgID != uuid.Nil {
		body.OwnerOrgID = owner.OrgID.String()
	} else {
		body.OwnerUserID = owner.UserID.String()
	}
	if err := n.postJSON(ctx, standingNoticePath, body); err != nil {
		attempts, rerr := n.episodes.RecordNoticeFailure(ctx, ep.ID)
		if rerr != nil {
			n.log.ErrorContext(ctx, "unpaid episode: failed-send counter write failed", "episode_id", ep.ID, "error", rerr)
		}
		n.log.ErrorContext(ctx, "unpaid notice: send failed (account stays unblocked, sweep retries)",
			"episode_id", ep.ID, "attempts", attempts, "error", err)
		if attempts >= noticeAlertAfter {
			n.log.ErrorContext(ctx, "ALERT unpaid notice undelivered after repeated attempts — the account cannot be limited until it is sent",
				"alert", true, "episode_id", ep.ID, "attempts", attempts)
		}
		return
	}
	changed, err := n.episodes.SetUnpaidEpisodeNotice(ctx, accountID, sentAt)
	if err != nil {
		// The email went out but the stamp did not: the account stays unblocked
		// and the sweep retries; the idempotency key stops a second email.
		n.log.ErrorContext(ctx, "unpaid notice: sent but stamp failed (sweep retries)", "episode_id", ep.ID, "error", err)
		return
	}
	n.log.InfoContext(ctx, "unpaid notice sent", "episode_id", ep.ID, "reason", reason,
		"limit_at", body.LimitAt, "stamped", changed)
}

// SweepResult tallies one sweep run.
type SweepResult struct {
	Candidates int // open episodes looked at (notice unsent, or 14 days over)
	Blocked    int // owners whose pushed verdict is blocked
	Failed     int // owners whose evaluation or push failed
}

// SweepUnpaidEpisodes is the daily enforcement pass (run from cmd/billing-cycle
// after the cycle). For each open episode whose notice is unsent it retries the
// notice, and for each whose 14 days are over it pushes the now-blocked verdict
// to serving-block; both are one NotifyOwner, whose episode writer sends a
// missing notice and whose verdict read applies the gate. Idempotent, and
// bounded: sweepMaxPages pages of sweepBatch. It does nothing while the flag is
// off. The suspension email on the block transition is billing-engine#331's.
func (n *Notifier) SweepUnpaidEpisodes(ctx context.Context) SweepResult {
	var res SweepResult
	if !n.GraceActive() {
		return res
	}
	after := uuid.Nil
	for page := 0; page < sweepMaxPages; page++ {
		due, err := n.episodes.DueUnpaidEpisodes(ctx, n.clock().UTC(), after, sweepBatch)
		if err != nil {
			n.log.ErrorContext(ctx, "unpaid sweep: episode list failed", "error", err)
			res.Failed++
			return res
		}
		for _, d := range due {
			res.Candidates++
			blocked, err := n.NotifyOwner(ctx, d.Owner.UserID, d.Owner.OrgID)
			switch {
			case err != nil:
				res.Failed++
			case blocked:
				res.Blocked++
			}
		}
		if len(due) < sweepBatch {
			break
		}
		after = due[len(due)-1].ID
	}
	n.log.InfoContext(ctx, "unpaid sweep complete",
		"candidates", res.Candidates, "blocked", res.Blocked, "failed", res.Failed)
	return res
}

// pgEpisodeStore is the production EpisodeStore over ms_billing.unpaid_episodes.
type pgEpisodeStore struct {
	pool *pgxpool.Pool
	bs   billing.Store
}

// NewEpisodeStore returns the EpisodeStore backed by the pool.
func NewEpisodeStore(pool *pgxpool.Pool) EpisodeStore {
	return &pgEpisodeStore{pool: pool, bs: billing.NewStore(pool)}
}

func (s *pgEpisodeStore) AccountForOwner(ctx context.Context, owner Owner) (uuid.UUID, bool, error) {
	if owner.OrgID != uuid.Nil {
		return s.bs.ResolveOrgFundedAccount(ctx, owner.OrgID)
	}
	return s.bs.AccountByUser(ctx, owner.UserID)
}

func (s *pgEpisodeStore) OpenUnpaidEpisodeIfNone(ctx context.Context, accountID uuid.UUID, at time.Time) (billing.UnpaidEpisode, error) {
	return s.bs.OpenUnpaidEpisodeIfNone(ctx, accountID, at)
}

func (s *pgEpisodeStore) CloseUnpaidEpisode(ctx context.Context, accountID uuid.UUID, at time.Time) (bool, error) {
	return s.bs.CloseUnpaidEpisode(ctx, accountID, at)
}

func (s *pgEpisodeStore) SetUnpaidEpisodeNotice(ctx context.Context, accountID uuid.UUID, at time.Time) (bool, error) {
	return s.bs.SetUnpaidEpisodeNotice(ctx, accountID, at)
}

func (s *pgEpisodeStore) RecordNoticeFailure(ctx context.Context, episodeID uuid.UUID) (int, error) {
	var attempts int
	err := s.pool.QueryRow(ctx,
		`UPDATE ms_billing.unpaid_episodes SET notice_attempts = notice_attempts + 1
		  WHERE id = $1 RETURNING notice_attempts`, episodeID).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return attempts, err
}

func (s *pgEpisodeStore) DueUnpaidEpisodes(ctx context.Context, now time.Time, after uuid.UUID, limit int) ([]DueEpisode, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT e.id, a.owner_user_id, a.owner_org_id
		   FROM ms_billing.unpaid_episodes e
		   JOIN ms_billing.accounts a ON a.id = e.account_id
		  WHERE e.closed_at IS NULL
		    AND e.id > $1
		    AND (e.notice_sent_at IS NULL
		         OR e.notice_sent_at + ($3::int * interval '1 day') <= $2)
		  ORDER BY e.id
		  LIMIT $4`, after, now, eligibility.UnpaidNoticeDays, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DueEpisode
	for rows.Next() {
		var id uuid.UUID
		var userID, orgID pgtype.UUID
		if err := rows.Scan(&id, &userID, &orgID); err != nil {
			return nil, fmt.Errorf("scan due episode: %w", err)
		}
		d := DueEpisode{ID: id}
		if userID.Valid {
			d.Owner.UserID = uuid.UUID(userID.Bytes)
		}
		if orgID.Valid {
			d.Owner.OrgID = uuid.UUID(orgID.Bytes)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
