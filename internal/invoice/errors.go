package invoice

import "errors"

var (
	// ErrSellerNotFound: no invoice_sellers row matches.
	ErrSellerNotFound = errors.New("invoice: seller not found")

	// ErrInvalidTaxID: the supplied 統編 fails ValidateTaxID.
	ErrInvalidTaxID = errors.New("invoice: tax id fails the checksum")

	// ErrRegisteredRequiresTaxID: registration_status=registered was
	// requested without a tax id.
	ErrRegisteredRequiresTaxID = errors.New("invoice: a registered seller must carry a tax id")

	// ErrOperatorCannotEnterKeys: D525 — only an org_owner actor may put a
	// vendor credential. An operator may revoke but never enter or read one.
	ErrOperatorCannotEnterKeys = errors.New("invoice: an operator actor cannot enter provider keys")

	// ErrDuplicateActiveProvider: the seller already has an active provider
	// account for a DIFFERENT provider. Switching provider is a two-step
	// migration (revoke, then put), never an implicit replace.
	ErrDuplicateActiveProvider = errors.New("invoice: seller already has an active provider account for a different provider")

	// ErrNotConfigured: the seller has no active provider account — nothing
	// to verify, revoke, or issue against.
	ErrNotConfigured = errors.New("invoice: seller has no active provider account")

	// ErrSellerRevoked: the seller's provider account was put and then
	// revoked. Distinct from ErrNotConfigured so a caller can tell "never
	// configured" from "configured, then shut off" apart.
	ErrSellerRevoked = errors.New("invoice: seller's provider credentials are revoked")

	// ErrModeSwitchWithPendingJobs: fix direction #7 — a mode switch is
	// refused while the seller has pending invoice jobs.
	ErrModeSwitchWithPendingJobs = errors.New("invoice: mode cannot change while invoice jobs are pending")

	// ErrVerifyRateLimited: VerifyInvoiceCredentials exceeded its per-seller
	// rate limit (default 6/hour).
	ErrVerifyRateLimited = errors.New("invoice: verify rate limit exceeded")

	// ErrSellerResolutionNotImplemented: ResolveSeller was asked to resolve
	// an origin this package does not yet implement (app_owner_org — "later"
	// per the issue).
	ErrSellerResolutionNotImplemented = errors.New("invoice: seller resolution for this payment-fact origin is not implemented yet")
)
