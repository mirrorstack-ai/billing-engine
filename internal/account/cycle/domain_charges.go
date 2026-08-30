package cycle

import (
	"context"
	"fmt"
	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/proposer"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/mirrorstack-ai/billing-engine/internal/account/billing"
	"github.com/mirrorstack-ai/billing-engine/internal/account/usage"
	billingstripe "github.com/mirrorstack-ai/billing-engine/internal/shared/stripe"
)

// DomainChargeStatus is the terminal classification of one custom-domain
// activation-period charge attempt.
type DomainChargeStatus string

const (
	DomainChargeCharged          DomainChargeStatus = "charged"
	DomainChargeSkippedNoPM      DomainChargeStatus = "skipped_no_pm"
	DomainChargeSkippedPrepaid   DomainChargeStatus = "skipped_prepaid"
	DomainChargeSkippedZeroCents DomainChargeStatus = "zero_cents"
	// DomainChargeProposed: the leg has cut over to the intent path.
	// A sealed intent records the charge; nothing has collected it.
	DomainChargeProposed     DomainChargeStatus = "proposed"
	DomainChargePeriodClosed DomainChargeStatus = "period_closed"
	DomainChargeSkippedStale DomainChargeStatus = "skipped_stale"
)

// DomainChargeResult reports what one ChargeDomain call did.
type DomainChargeResult struct {
	DomainID        uuid.UUID
	Status          DomainChargeStatus
	ChargedCents    int64
	StripeInvoiceID string
	// IntentDigest is set when the leg proposed instead of charging.
	// It is the identity of the sealed document, so a domain row can be
	// walked forward to the intent that replaced its charge.
	IntentDigest string
}

// Policy identifiers a proposed charge is sealed under.
//
// 🔴 These are placeholders, and named so. DESIGN §12 leaves the terms,
// price-book and notice policies for these charge kinds undecided, and
// sealing under an invented revision would put a fiction inside the
// digest — which a customer's charge bundle would then attest to.
//
// A cutover must not be ENABLED in production until they name real,
// published revisions. The shadow reconciliation of §11 step 4 is what
// happens first; this is the second thing that must be true.
const (
	proposedTermsRevision     = "unpublished/pending-decision-12"
	proposedPriceBookRevision = "unpublished/pending-decision-12"
	proposedNoticePolicy      = "unpublished/pending-decision-12"
	proposedTaxRuleRevision   = "not-applicable/pending-decision-12"
)

// domainChargeShape prices only the domain's first, activation-containing
// period. Domains have no grace, included pool, or straddle top-up: the
// boundary leg includes every live domain in each subsequent full period.
func domainChargeShape(cand DomainChargeCandidate) (proratedMicros int64, coverageStart, coverageEnd time.Time, periodClosed bool) {
	periodStart, periodEnd, closed := periodClosedByActivation(cand.ActivatedAt, cand.AccountActivatedAt)
	if closed {
		return 0, time.Time{}, time.Time{}, true
	}
	return usage.ProratedBaseMicros(usage.DomainFeeMicros, cand.ActivatedAt, periodStart, periodEnd),
		usage.ProrationCoverageStart(cand.ActivatedAt, periodStart), periodEnd, false
}

// ChargeDomain charges one live, unresolved custom domain for its activation
// period. The row guard, deterministic Stripe keys, and charge-attempt marker
// make the operation idempotent and recoverable across crashes.
func (s *Service) ChargeDomain(ctx context.Context, cand DomainChargeCandidate, at time.Time) (*DomainChargeResult, error) {
	if cand.ID == uuid.Nil {
		return nil, billing.InvalidInput("domain id required")
	}
	if at.IsZero() {
		return nil, billing.InvalidInput("charge instant required")
	}
	if s.stripe == nil {
		return nil, billing.Internal("ChargeDomain requires a Stripe client", nil)
	}
	res := &DomainChargeResult{DomainID: cand.ID}

	pending, err := s.store.DomainStillPending(ctx, cand.ID)
	if err != nil {
		return nil, billing.Internal("domain pending re-check failed", err)
	}
	if !pending {
		res.Status = DomainChargeSkippedStale
		return res, nil
	}

	if !cand.ChargeAttemptedAt.IsZero() {
		recovered, err := s.recoverDomainCharge(ctx, cand, at, res)
		if err != nil {
			return nil, err
		}
		if recovered {
			return res, nil
		}
	}

	proratedMicros, coverageStart, coverageEnd, periodClosed := domainChargeShape(cand)
	if periodClosed {
		if err := s.store.MarkDomainChargeResolved(ctx, cand.ID); err != nil {
			return nil, billing.Internal("mark domain charge resolved (period closed) failed", err)
		}
		res.Status = DomainChargePeriodClosed
		return res, nil
	}

	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return nil, billing.Internal("micros to cents conversion failed", err)
	}
	if cents == 0 {
		if err := s.store.MarkDomainChargeResolved(ctx, cand.ID); err != nil {
			return nil, billing.Internal("mark domain charge resolved (zero cents) failed", err)
		}
		res.Status = DomainChargeSkippedZeroCents
		return res, nil
	}
	res.ChargedCents = cents

	if permitted, err := s.offSessionChargePermitted(ctx, cand.AccountID); err != nil {
		return nil, err
	} else if !permitted {
		res.Status = DomainChargeSkippedPrepaid
		return res, nil
	}

	claim, err := s.store.ArmDomainStripeCharge(ctx, cand.ID, at.UTC())
	if err != nil {
		return nil, billing.Internal("arm domain Stripe charge failed", err)
	}
	switch claim.Outcome {
	case StripeRailNoPaymentMethod:
		res.Status = DomainChargeSkippedNoPM
		return res, nil
	case StripeRailStale:
		res.Status = DomainChargeSkippedStale
		return res, nil
	}
	custID := claim.StripeCustomerID
	if custID == "" {
		return nil, billing.Internal("domain funding account has a usable PM but no Stripe customer id", nil)
	}

	// 🔴 The cutover point for this leg.
	//
	// Everything above is unchanged: the same staleness re-check, the
	// same proration, the same durable arming claim. What changes is
	// what happens with the derived amount — proposed as a sealed
	// intent, or charged directly.
	//
	// The arming claim is taken FIRST either way, so a cut-over leg
	// keeps the crash-recovery guard the legacy one has. Proposing
	// without it would let a re-run derive the same charge against a
	// row nothing had claimed.
	if s.proposer != nil {
		return s.proposeDomainCharge(ctx, cand, res, proratedMicros, coverageStart, coverageEnd, at)
	}

	draft, err := s.stripe.CreateDraftInvoice(ctx, custID, domainChargeRef(cand.ID), domainInvoiceIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("domain draft invoice failed", err)
	}
	desc := fmt.Sprintf("MirrorStack custom domain (prorated) — %s", cand.Hostname)
	linePeriod := billingstripe.LinePeriod{Start: coverageStart, End: coverageEnd}
	item, err := s.stripe.CreateInvoiceItem(ctx, custID, draft.ID, cents, chargeCurrency, desc, linePeriod, domainItemIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("domain invoice item failed", err)
	}
	inv, err := s.stripe.FinalizeInvoice(ctx, draft.ID, domainFinalizeIdemKey(cand.ID))
	if err != nil {
		return nil, billing.StripeError("domain invoice finalize failed", err)
	}

	acct, err := s.store.AccountCollection(ctx, cand.AccountID)
	if err != nil {
		return nil, billing.Internal("account collection lookup failed", err)
	}
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:               cand.AccountID,
		ChargeFundingAccountID:  claim.FundingAccountID,
		ChargeFundingGeneration: claim.FundingGeneration,
		StripeInvoiceID:         inv.ID,
		Status:                  inv.Status,
		AmountDueCents:          inv.AmountDue,
		AmountPaidCents:         inv.AmountPaid,
		Currency:                chargeCurrency,
		PeriodStart:             coverageStart,
		PeriodEnd:               coverageEnd,
		IsLargeAutoCollect:      flagLargeAutoCollect(proratedMicros, acct),
	}); err != nil {
		return nil, billing.Internal("invoice mirror upsert failed", err)
	}
	if err := s.store.MarkDomainCharged(ctx, cand.ID, at.UTC(), inv.ID, item.ID); err != nil {
		return nil, billing.Internal("mark domain charged failed", err)
	}

	res.Status = DomainChargeCharged
	res.StripeInvoiceID = inv.ID
	return res, nil
}

// SweepDomainChargesResult tallies one custom-domain activation-charge batch.
type SweepDomainChargesResult struct {
	Pending  int
	Charged  int
	Resolved int
	Skipped  int
	Failed   int
}

// SweepDomainCharges processes every live unresolved domain activated by at.
// A per-domain error is counted and left for the next idempotent sweep.
func (s *Service) SweepDomainCharges(ctx context.Context, at time.Time) (*SweepDomainChargesResult, error) {
	if at.IsZero() {
		return nil, billing.InvalidInput("sweep instant required")
	}
	cands, err := s.store.DomainsPendingCharge(ctx, at.UTC())
	if err != nil {
		return nil, billing.Internal("list domains pending charge failed", err)
	}
	res := &SweepDomainChargesResult{Pending: len(cands)}
	for _, cand := range cands {
		r, err := s.ChargeDomain(ctx, cand, at)
		if err != nil {
			slog.ErrorContext(ctx, "domain charge failed", "domain_id", cand.ID,
				"app_id", cand.AppID, "hostname", cand.Hostname, "error", err)
			res.Failed++
			continue
		}
		switch r.Status {
		case DomainChargeCharged:
			res.Charged++
		case DomainChargePeriodClosed, DomainChargeSkippedZeroCents:
			res.Resolved++
		default:
			res.Skipped++
		}
		slog.InfoContext(ctx, "domain charge sweep", "domain_id", cand.ID,
			"app_id", cand.AppID, "hostname", cand.Hostname, "status", string(r.Status),
			"charged_cents", r.ChargedCents, "stripe_invoice_id", r.StripeInvoiceID)
	}
	return res, nil
}

// recoverDomainCharge reconciles a candidate whose charge-attempt marker was
// stamped before a prior process died. A found finalized invoice is mirrored
// and marked; a found draft is completed with the deterministic line and keys.
func (s *Service) recoverDomainCharge(ctx context.Context, cand DomainChargeCandidate, at time.Time, res *DomainChargeResult) (bool, error) {
	if cand.ChargeFundingAccountID == uuid.Nil || cand.ChargeFundingGeneration == uuid.Nil {
		return false, billing.Internal("domain recovery marker has no pinned funding authorization", nil)
	}
	custID, err := s.store.AccountStripeCustomer(ctx, cand.ChargeFundingAccountID)
	if err != nil {
		return false, billing.Internal("stripe customer lookup failed (domain recovery)", err)
	}
	if custID == "" {
		return false, nil
	}
	found, ok, err := s.stripe.FindInvoiceByRef(ctx, custID, domainChargeRef(cand.ID))
	if err != nil {
		return false, billing.StripeError("domain recovery lookup failed", err)
	}
	if !ok {
		return false, nil
	}
	if found.Status == "void" {
		return false, billing.Internal(fmt.Sprintf(
			"domain recovery: invoice %s under %s is VOID — refusing to adopt a canceled charge (domain %s needs ops resolution)",
			found.ID, domainChargeRef(cand.ID), cand.ID), nil)
	}

	proratedMicros, coverageStart, coverageEnd, periodClosed := domainChargeShape(cand)
	if periodClosed {
		return false, nil
	}
	cents, err := centsFromMicros(proratedMicros)
	if err != nil {
		return false, billing.Internal("micros to cents conversion failed", err)
	}

	inv := found
	if found.Status == "draft" {
		if permitted, err := s.offSessionChargePermitted(ctx, cand.AccountID); err != nil {
			return false, err
		} else if !permitted {
			res.Status = DomainChargeSkippedPrepaid
			return true, nil
		}
		switch found.AmountDue {
		case 0:
			desc := fmt.Sprintf("MirrorStack custom domain (prorated) — %s", cand.Hostname)
			linePeriod := billingstripe.LinePeriod{Start: coverageStart, End: coverageEnd}
			if _, err := s.stripe.CreateInvoiceItem(ctx, custID, found.ID, cents, chargeCurrency, desc, linePeriod, domainItemIdemKey(cand.ID)); err != nil {
				return false, billing.StripeError("domain recovery invoice item failed", err)
			}
		case cents:
			// The deterministic line already landed before the crash.
		default:
			return false, billing.Internal(fmt.Sprintf(
				"domain recovery: draft %s carries %d cents but the deterministic amount is %d — refusing to finalize a mismatched draft (domain %s)",
				found.ID, found.AmountDue, cents, cand.ID), nil)
		}
		inv, err = s.stripe.FinalizeInvoice(ctx, found.ID, domainFinalizeIdemKey(cand.ID))
		if err != nil {
			return false, billing.StripeError("domain recovery finalize failed", err)
		}
	}

	acct, err := s.store.AccountCollection(ctx, cand.AccountID)
	if err != nil {
		return false, billing.Internal("account collection lookup failed", err)
	}
	if err := s.store.UpsertInvoice(ctx, InvoiceMirror{
		AccountID:               cand.AccountID,
		ChargeFundingAccountID:  cand.ChargeFundingAccountID,
		ChargeFundingGeneration: cand.ChargeFundingGeneration,
		StripeInvoiceID:         inv.ID,
		Status:                  inv.Status,
		AmountDueCents:          inv.AmountDue,
		AmountPaidCents:         inv.AmountPaid,
		Currency:                chargeCurrency,
		PeriodStart:             coverageStart,
		PeriodEnd:               coverageEnd,
		IsLargeAutoCollect:      flagLargeAutoCollect(proratedMicros, acct),
	}); err != nil {
		return false, billing.Internal("invoice mirror upsert failed (domain recovery)", err)
	}
	if err := s.store.MarkDomainCharged(ctx, cand.ID, at.UTC(), inv.ID, ""); err != nil {
		return false, billing.Internal("mark domain charged failed (domain recovery)", err)
	}

	res.Status = DomainChargeCharged
	res.ChargedCents = inv.AmountDue
	res.StripeInvoiceID = inv.ID
	return true, nil
}

func domainChargeRef(domainID uuid.UUID) string { return "domain:" + domainID.String() }

func domainItemIdemKey(domainID uuid.UUID) string {
	return "domain-fee-ii-" + domainID.String()
}

func domainInvoiceIdemKey(domainID uuid.UUID) string {
	return "domain-fee-inv-" + domainID.String()
}

func domainFinalizeIdemKey(domainID uuid.UUID) string {
	return "domain-fee-fin-" + domainID.String()
}

// proposeDomainCharge seals this leg's derived charge instead of
// collecting it.
//
// The intent records the same figure the legacy path would have
// charged, against the same domain row, inside the same coverage
// window. What it does not do is move money — that waits for something
// holding the write port, and for the predicate to permit it.
//
// The domain row is marked resolved on a successful proposal for the
// same reason the legacy path marks it after a successful finalize:
// the leg is done with it. A row left unresolved would be re-derived
// on the next sweep and proposed again — harmlessly, since the digest
// makes that the same document, but it would also never leave the
// candidate set.
func (s *Service) proposeDomainCharge(
	ctx context.Context,
	cand DomainChargeCandidate,
	res *DomainChargeResult,
	proratedMicros int64,
	coverageStart, coverageEnd time.Time,
	at time.Time,
) (*DomainChargeResult, error) {
	// Seal what a collection would actually take, not the raw derived
	// micros — see collectableMicros. Sealing the unrounded figure would
	// attest to an amount the customer was never charged.
	sealMicros, err := collectableMicros(proratedMicros)
	if err != nil {
		return nil, billing.Internal("micros to collectable micros conversion failed", err)
	}

	sealed, err := s.proposer.Propose(ctx, proposer.Charge{
		Payer:        intent.Subject{Kind: "user", ID: cand.AccountID.String()},
		Kind:         "domain.custom",
		Currency:     chargeCurrency,
		AmountMicros: sealMicros,
		Description:  fmt.Sprintf("MirrorStack custom domain (prorated) — %s", cand.Hostname),
		SourceRef:    domainChargeRef(cand.ID),

		AuthorizationID:   "domain:" + cand.AccountID.String(),
		TermsRevision:     proposedTermsRevision,
		PriceBookRevision: proposedPriceBookRevision,
		NoticePolicy:      proposedNoticePolicy,
		// 🔴 Zero tax, resolved. This leg has never applied tax, so
		// claiming an unresolved determination would quarantine every
		// domain charge, and claiming a computed one would invent a
		// figure. Recording what is actually true — no tax was
		// determined for this charge kind — is the honest option, and
		// docs/DESIGN.md §12's tax decisions are what change it.
		Tax: intent.TaxDetermination{
			Resolved:     true,
			Jurisdiction: "not-applicable",
			RuleRevision: proposedTaxRuleRevision,
		},
		ExecuteNotBefore: coverageStart,
		ExecuteNotAfter:  coverageEnd,
	})
	if err != nil {
		return nil, billing.Internal("propose domain charge intent failed", err)
	}

	if err := s.store.MarkDomainChargeResolved(ctx, cand.ID); err != nil {
		return nil, billing.Internal("mark domain charge resolved (proposed) failed", err)
	}

	res.Status = DomainChargeProposed
	res.IntentDigest = sealed.Digest()
	return res, nil
}
