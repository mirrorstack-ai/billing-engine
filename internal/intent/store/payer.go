package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
)

// ErrNoSuchAccount is returned when nothing owns the account an intent was
// derived from.
var ErrNoSuchAccount = errors.New("store: no billing account with that id")

// ErrNoUsableCard is returned when a payer has no non-deleted default card.
var ErrNoUsableCard = errors.New("store: payer has no usable default card")

// 🔴 THE TWO HALVES OF ONE CONTRACT, DELIBERATELY IN ONE FILE.
//
// A sealed intent names a PAYER, and something later has to turn that payer
// into a Stripe customer and an instrument. Until 2026-08-31 the two halves
// lived in different packages and DISAGREED:
//
//   - the three cut-over legs sealed intent.Subject{Kind: "user", ID:
//     <ms_billing.accounts.id>} (overage.go:825, domain_charges.go:396,
//     autotopup/executor.go:1328);
//   - cmd/intent-executor's resolver matched a.owner_user_id::text = $2.
//
// accounts.id is a gen_random_uuid() primary key and owner_user_id is a soft
// FK to ms_account.users.id (migration 001:18-25). They are never equal. So
// the lookup returned no rows and EVERY intent this tree could produce was
// uncollectable — the routing shipped, and nothing that went through it could
// ever have been charged.
//
// Nothing failed, because no deployment has ever executed an intent: the
// producing side and the consuming side were each internally consistent and
// were never run against each other.
//
// So they are one file now. PayerForAccount produces the subject a leg seals,
// ResolvePayer consumes it, and an integration test drives the round trip —
// account row in, provider identity out — which is the only shape that could
// have caught this.

// PayerForAccount is the subject a charge against this account must seal.
//
// It returns the account's OWNER, not the account id. The owner is who
// accepted the authorization and who the charge is against; the account is
// the internal artifact that happens to hold their card. The vocabulary
// agrees: charge_intents.payer_kind is CHECKed to ('user', 'org', 'app') —
// account is not one of them, which is the schema saying the same thing.
func (s *Store) PayerForAccount(ctx context.Context, accountID string) (intent.Subject, error) {
	var (
		ownerKind string
		ownerUser *string
		ownerOrg  *string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT owner_kind, owner_user_id::text, owner_org_id::text
		  FROM ms_billing.accounts
		 WHERE id = $1`, accountID).Scan(&ownerKind, &ownerUser, &ownerOrg)
	if errors.Is(err, pgx.ErrNoRows) {
		return intent.Subject{}, fmt.Errorf("%w: %s", ErrNoSuchAccount, accountID)
	}
	if err != nil {
		return intent.Subject{}, fmt.Errorf("payer for account: %w", err)
	}

	// Exactly one owner column is non-NULL per the polymorphic-owner CHECK in
	// migrations 001/041. A row that satisfies neither is refused rather than
	// resolved to an empty subject: a charge against nobody must not seal.
	switch {
	case ownerKind == "user" && ownerUser != nil && *ownerUser != "":
		return intent.Subject{Kind: "user", ID: *ownerUser}, nil
	case ownerKind == "org" && ownerOrg != nil && *ownerOrg != "":
		return intent.Subject{Kind: "org", ID: *ownerOrg}, nil
	}
	return intent.Subject{}, fmt.Errorf(
		"%w: account %s is owner_kind %q with no matching owner column",
		ErrNoSuchAccount, accountID, ownerKind)
}

// ResolvePayer returns the Stripe customer and default payment method for a
// sealed payer.
//
// It reads only. The resolution is a lookup of state something else
// established — a customer created when a card was added, a default chosen by
// the customer — and an executor that could CREATE either would be able to
// charge a payer that had never been set up.
//
// Only a non-deleted default is returned. Falling back to any card on file
// would charge an instrument the customer did not choose, and a customer who
// removed a card removed it.
func (s *Store) ResolvePayer(ctx context.Context, payerKind, payerID string) (customerID, paymentMethodID string, err error) {
	const query = `
		SELECT a.stripe_customer_id, pm.stripe_payment_method_id
		  FROM ms_billing.accounts a
		  JOIN ms_billing.payment_methods_mirror pm
		    ON pm.account_id = a.id
		 WHERE a.owner_kind = $1
		   AND ((a.owner_kind = 'user' AND a.owner_user_id::text = $2)
		     OR (a.owner_kind = 'org'  AND a.owner_org_id::text  = $2))
		   AND pm.is_default
		   AND pm.deleted_at IS NULL
		 LIMIT 1`

	err = s.pool.QueryRow(ctx, query, payerKind, payerID).Scan(&customerID, &paymentMethodID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", fmt.Errorf("%w: %s:%s", ErrNoUsableCard, payerKind, payerID)
	}
	if err != nil {
		return "", "", fmt.Errorf("resolve payer: %w", err)
	}
	return customerID, paymentMethodID, nil
}
