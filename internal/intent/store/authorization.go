package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// SaveAuthorization writes an authorization, and its revocation if it
// has one.
//
// Re-saving is a no-op on the grant and an update on the revocation
// only. An authorization's terms are what the customer accepted, so
// rewriting them under a stored acceptance would make the acceptance
// describe something else — the same reason a sealed intent is
// superseded rather than edited. Revocation is the one thing that
// legitimately arrives later, and it can only ever reduce what is
// permitted.
func (s *Store) SaveAuthorization(ctx context.Context, auth intent.BillingAuthorization) error {
	if auth.ID() == "" {
		return errors.New("store: refusing to save an authorization with no id")
	}
	g := auth.Grant()

	kinds := make([]string, 0, len(g.Kinds))
	for _, k := range g.Kinds {
		kinds = append(kinds, string(k))
	}

	var revokedAt *time.Time
	if at := auth.RevokedAt(); !at.IsZero() {
		revokedAt = &at
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO ms_billing.billing_authorizations
		  (id, scope, subject_kind, subject_id, currency, intent_digest,
		   charge_kinds, per_charge_ceiling_micros, period_ceiling_micros,
		   frequency_ceiling, trigger_below_micros, top_up_amount_micros,
		   provider, mandate_reference, notice_lead_seconds,
		   terms_revision, price_book_revision, notice_policy,
		   effective_from, expires_at, acceptance_digest, revoked_at)
		VALUES ($1,$2,$3,$4,$5,NULLIF($6,''),$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		ON CONFLICT (id) DO UPDATE
		   SET revoked_at = COALESCE(
		         ms_billing.billing_authorizations.revoked_at,
		         EXCLUDED.revoked_at)`,
		g.ID, string(g.Scope), g.Subject.Kind, g.Subject.ID, g.Currency,
		g.IntentDigest, kinds, g.PerChargeCeiling, g.PeriodCeiling,
		g.FrequencyCeiling, g.TriggerBelowMicros, g.TopUpAmountMicros,
		g.Provider, g.MandateReference, int64(g.NoticeLeadTime/time.Second),
		g.TermsRevision, g.PriceBook, g.NoticePolicy,
		g.EffectiveFrom, g.ExpiresAt, g.AcceptanceDigest, revokedAt,
	)
	if err != nil {
		return fmt.Errorf("save authorization: %w", err)
	}
	return nil
}

// LoadAuthorization reads an authorization back through Authorize, so
// every validation runs again on the way out of storage.
//
// A row that no longer satisfies them does not load. That is the right
// failure: an authorization the constructor would refuse is one nothing
// should be charged against, and a stored row is not exempt from the
// rules its own creation had to pass.
func (s *Store) LoadAuthorization(ctx context.Context, id string) (intent.BillingAuthorization, error) {
	var (
		noticeLeadSeconds int64
		g                 intent.AuthorizationGrant
		scope             string
		kinds             []string
		intentDigest      *string
		revokedAt         *time.Time
	)

	err := s.pool.QueryRow(ctx, `
		SELECT scope, subject_kind, subject_id, currency, intent_digest,
		       charge_kinds, per_charge_ceiling_micros, period_ceiling_micros,
		       frequency_ceiling, trigger_below_micros, top_up_amount_micros,
		       provider, mandate_reference, notice_lead_seconds,
		       terms_revision, price_book_revision, notice_policy,
		       effective_from, expires_at, acceptance_digest, revoked_at
		  FROM ms_billing.billing_authorizations
		 WHERE id = $1`, id,
	).Scan(
		&scope, &g.Subject.Kind, &g.Subject.ID, &g.Currency, &intentDigest,
		&kinds, &g.PerChargeCeiling, &g.PeriodCeiling,
		&g.FrequencyCeiling, &g.TriggerBelowMicros, &g.TopUpAmountMicros,
		&g.Provider, &g.MandateReference, &noticeLeadSeconds,
		&g.TermsRevision, &g.PriceBook, &g.NoticePolicy,
		&g.EffectiveFrom, &g.ExpiresAt, &g.AcceptanceDigest, &revokedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return intent.BillingAuthorization{}, fmt.Errorf("%w: authorization %s", ErrNotFound, id)
	}
	if err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf("load authorization: %w", err)
	}

	g.ID = id
	g.Scope = intent.AuthorizationScope(scope)
	g.NoticeLeadTime = time.Duration(noticeLeadSeconds) * time.Second
	if intentDigest != nil {
		g.IntentDigest = *intentDigest
	}
	for _, k := range kinds {
		g.Kinds = append(g.Kinds, intent.ChargeKind(k))
	}

	auth, err := intent.Authorize(g)
	if err != nil {
		return intent.BillingAuthorization{}, fmt.Errorf(
			"stored authorization %s would not be accepted by Authorize: %w", id, err)
	}
	if revokedAt != nil {
		auth = auth.Revoke(*revokedAt)
	}
	return auth, nil
}

// NoticeReceipt is delivery evidence as stored.
//
// INV-005: automatic collection requires durable evidence that the
// sealed intent was delivered byte-for-byte. The schema enforces that
// DeliveredDigest equals the intent's own; this type carries it anyway
// so a caller assembling predicate state reads it rather than assuming.
type NoticeReceipt struct {
	IntentDigest    string
	DeliveredDigest string
	Policy          string
	TerminalStatus  string
	// DeliveredAt is when the bytes arrived. The wait runs from here.
	DeliveredAt          time.Time
	EligibilityNotBefore time.Time
	RevocationPathFresh  bool
}

// RecordNotice stores delivery evidence for an intent.
//
// One receipt per intent. A second delivery of the same document does
// not restart its wait — the clock started when the bytes arrived, and
// re-sending them is not a new arrival.
func (s *Store) RecordNotice(ctx context.Context, receipt NoticeReceipt) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ms_billing.notice_receipts
		  (intent_digest, delivered_digest, policy, terminal_status,
		   delivered_at, eligibility_not_before, revocation_path_fresh)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (intent_digest) DO NOTHING`,
		receipt.IntentDigest, receipt.DeliveredDigest, receipt.Policy,
		receipt.TerminalStatus, receipt.DeliveredAt, receipt.EligibilityNotBefore,
		receipt.RevocationPathFresh,
	)
	if err != nil {
		return fmt.Errorf("record notice: %w", err)
	}
	return nil
}

// LoadNotice reads an intent's delivery evidence.
//
// A missing receipt is returned as the zero value with found=false
// rather than an error, because "no notice yet" is an ordinary state
// for an intent awaiting one — and the predicate refuses on it anyway.
func (s *Store) LoadNotice(ctx context.Context, digest string) (NoticeReceipt, bool, error) {
	var (
		r           NoticeReceipt
		deliveredAt *time.Time
	)
	err := s.pool.QueryRow(ctx, `
		SELECT intent_digest, delivered_digest, policy, terminal_status,
		       delivered_at, eligibility_not_before, revocation_path_fresh
		  FROM ms_billing.notice_receipts
		 WHERE intent_digest = $1`, digest,
	).Scan(&r.IntentDigest, &r.DeliveredDigest, &r.Policy, &r.TerminalStatus,
		&deliveredAt, &r.EligibilityNotBefore, &r.RevocationPathFresh)
	if errors.Is(err, pgx.ErrNoRows) {
		return NoticeReceipt{}, false, nil
	}
	// A NULL delivered_at is a pre-056 row: nothing recorded when its
	// clock started, so it cannot satisfy a lead time and the zero value
	// is the honest answer.
	if deliveredAt != nil {
		r.DeliveredAt = *deliveredAt
	}
	if err != nil {
		return NoticeReceipt{}, false, fmt.Errorf("load notice: %w", err)
	}
	return r, true, nil
}
