package cycle

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/mirrorstack-ai/billing-engine/internal/account/db"
)

func (s *pgxStore) FinalizeOrgDeletionBilling(
	ctx context.Context,
	orgID, operationID uuid.UUID,
	finalizedAt time.Time,
) (OrgDeletionFinalizationOutcome, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OrgDeletionFinalized, err
	}
	defer tx.Rollback(ctx) // no-op after Commit

	qtx := s.q.WithTx(tx)
	if _, err := qtx.AcquireOrgBillingLifecycleLock(ctx, orgID.String()); err != nil {
		return OrgDeletionFinalized, err
	}

	existing, err := qtx.GetOrgDeletionFinalizationForUpdate(ctx, orgID.String())
	switch {
	case err == nil:
		existingOperation, parseErr := uuid.Parse(existing.OperationID)
		if parseErr != nil {
			return OrgDeletionFinalized, parseErr
		}
		if existingOperation != operationID {
			return OrgDeletionOperationConflict, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return OrgDeletionFinalized, err
		}
		return OrgDeletionAlreadyFinalized, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return OrgDeletionFinalized, err
	}

	// There is intentionally no database uniqueness constraint on
	// accounts(owner_org_id). Lock and inspect every historical account rather
	// than assuming the ordinary one-account invariant cannot be violated.
	if _, err := qtx.ListOrgAccountsForDeletion(ctx, pgtype.UUID{Bytes: orgID, Valid: true}); err != nil {
		return OrgDeletionFinalized, err
	}

	collectible, err := qtx.FinalCollectibleOrgInvoiceCount(ctx, orgID.String())
	if err != nil {
		return OrgDeletionFinalized, err
	}
	if collectible > 0 {
		return OrgDeletionCollectibleInvoices, nil
	}

	inFlight, err := qtx.OrgBillingInFlightCount(ctx, orgID.String())
	if err != nil {
		return OrgDeletionFinalized, err
	}
	if inFlight > 0 {
		return OrgDeletionMoneyInFlight, nil
	}

	// Retirement writes come before the tombstone because their triggers use
	// that tombstone as the rejection predicate. The lifecycle advisory lock
	// is already held, so no other guarded org writer can interleave here.
	timed := db.RetireOrgDomainsParams{OrgID: orgID.String(), FinalizedAt: finalizedAt}
	if _, err := qtx.RetireOrgDomains(ctx, timed); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.RetireOrgModuleTimers(ctx, db.RetireOrgModuleTimersParams(timed)); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.RetireOrgApps(ctx, db.RetireOrgAppsParams(timed)); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.RetireOrgPaymentMethods(ctx, db.RetireOrgPaymentMethodsParams(timed)); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.DisableOrgAutoTopUp(ctx, orgID.String()); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.FailOrgPendingAddCardRequests(ctx, db.FailOrgPendingAddCardRequestsParams(timed)); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.RetireOrgAccountCollection(ctx, orgID.String()); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.RetainOrgOutboundSponsorships(ctx, db.RetainOrgOutboundSponsorshipsParams{
		OrgID:       orgID.String(),
		OperationID: operationID.String(),
		RetiredAt:   finalizedAt,
	}); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.DeleteOrgOutboundSponsorships(ctx, orgID.String()); err != nil {
		return OrgDeletionFinalized, err
	}
	if _, err := qtx.DeleteOrgDesignationForFinalization(ctx, orgID.String()); err != nil {
		return OrgDeletionFinalized, err
	}

	if err := qtx.InsertOrgDeletionFinalization(ctx, db.InsertOrgDeletionFinalizationParams{
		OrgID:       orgID.String(),
		OperationID: operationID.String(),
		FinalizedAt: finalizedAt,
	}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgErr.Code == "23505" &&
			pgErr.ConstraintName == "org_deletion_finalizations_operation_key" {
			return OrgDeletionOperationConflict, nil
		}
		return OrgDeletionFinalized, err
	}

	if err := tx.Commit(ctx); err != nil {
		return OrgDeletionFinalized, err
	}
	return OrgDeletionFinalized, nil
}
