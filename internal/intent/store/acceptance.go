package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/predicate"
)

// IssuedAcceptance is a challenge the engine has issued, as it will be stored.
type IssuedAcceptance struct {
	AuthorizationID  string
	DisclosureDigest string
	Payer            intent.Subject
	Nonce            string
	Audience         string
	ReplayIdentity   string
	IssuedAt         time.Time
	ExpiresAt        time.Time
}

// IssueAcceptance records that the engine showed a customer a document.
//
// Issuing is idempotent on (authorization_id, disclosure_digest): showing the
// same terms twice is the same challenge. Re-issuing under DIFFERENT terms
// produces a different digest and therefore a different row, which is exactly
// the distinction that matters — a customer who was shown new ceilings has
// not accepted them by having accepted the old ones.
func (s *Store) IssueAcceptance(ctx context.Context, a IssuedAcceptance) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO ms_billing.authorization_acceptances
		  (authorization_id, disclosure_digest, payer_kind, payer_id,
		   nonce, audience, replay_identity, issued_at, expires_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT ON CONSTRAINT authorization_acceptances_one_per_document DO NOTHING`,
		a.AuthorizationID, a.DisclosureDigest, a.Payer.Kind, a.Payer.ID,
		a.Nonce, a.Audience, a.ReplayIdentity, a.IssuedAt, a.ExpiresAt)
	if err != nil {
		return fmt.Errorf("issue acceptance: %w", err)
	}
	return nil
}

// AcceptIssuedAcceptance records the customer's answer.
//
// It sets accepted_at only when it is NULL, so a replayed answer is a no-op
// rather than a second acceptance with a later instant — and the table's own
// trigger refuses any attempt to change it afterwards. An answer to a
// challenge that was never issued affects nothing, which is the correct
// outcome: the engine cannot accept a document it never showed.
func (s *Store) AcceptIssuedAcceptance(ctx context.Context, authorizationID, disclosureDigest string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE ms_billing.authorization_acceptances
		   SET accepted_at = $3
		 WHERE authorization_id = $1
		   AND disclosure_digest = $2
		   AND accepted_at IS NULL
		   AND revoked_at IS NULL`,
		authorizationID, disclosureDigest, at)
	if err != nil {
		return fmt.Errorf("accept acceptance: %w", err)
	}
	return nil
}

// LoadStandingAcceptance reads the acceptance a standing authorization rests
// on.
//
// 🔴 A missing row returns the ZERO value and no error.
//
// The predicate's zero StandingAcceptance authorises nothing, so "we could not
// find the record" and "the record refuses" reach the same answer — which is
// the only safe reading. Returning an error instead would push the decision up
// to a caller that has no better information, and the one thing it must never
// become is a reason to charge someone.
func (s *Store) LoadStandingAcceptance(ctx context.Context, authorizationID, disclosureDigest string) (predicate.StandingAcceptance, error) {
	var (
		payerKind, payerID string
		expiresAt          time.Time
		acceptedAt         *time.Time
		revokedAt          *time.Time
	)
	err := s.pool.QueryRow(ctx, `
		SELECT payer_kind, payer_id, expires_at, accepted_at, revoked_at
		  FROM ms_billing.authorization_acceptances
		 WHERE authorization_id = $1 AND disclosure_digest = $2`,
		authorizationID, disclosureDigest).Scan(
		&payerKind, &payerID, &expiresAt, &acceptedAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return predicate.StandingAcceptance{}, nil
	}
	if err != nil {
		return predicate.StandingAcceptance{}, fmt.Errorf("load standing acceptance: %w", err)
	}

	return predicate.StandingAcceptance{
		Issued:           true,
		DisclosureDigest: disclosureDigest,
		Payer:            intent.Subject{Kind: payerKind, ID: payerID},
		Accepted:         acceptedAt != nil,
		ExpiresAt:        expiresAt,
		Revoked:          revokedAt != nil,
	}, nil
}
