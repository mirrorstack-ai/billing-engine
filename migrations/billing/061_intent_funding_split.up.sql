-- Migration 061: the sealed funding split.
--
-- Two integers: the credit applied to this charge, and what is then due at
-- the rail. The owner settled the model on 2026-08-31 — a wallet draw is a
-- NEGATIVE LINE ITEM on a Stripe invoice, not a parallel rail:
--
--     usage            20.00
--     credit applied   -6.00
--     due              14.00
--
-- So wallet_allocation_micros is the credit line and provider_remainder_micros
-- is the invoice total actually due. docs/DESIGN.md:1284: the integer handed
-- to an adapter is the sealed providerRemainder, "never grossObligation, and
-- never wallet funding".
--
-- 🔴 SEALED, not execution-time. docs/DESIGN.md:205-206 puts the split in
-- INV-001's "what it may never send" list — "the engine freezes it BEFORE you
-- are shown anything" — and :470 and :1281 say the same. The split the
-- customer saw is the split that settles.
--
-- WHY THIS COLUMN EXISTS AT ALL: predicate.ClauseFundingPlanBalances could not
-- fail. executor.fundingFor synthesised the plan from the intent's own total,
-- so the clause verified a value the same call had just computed — Frozen was
-- a literal, Gross == TotalMicros by construction, and Wallet + Provider ==
-- Gross was 0 + Total == Total. A durable write is what separates the operands
-- and makes the clause capable of disagreeing.
--
-- NOT NULL with no DEFAULT, for migration 060's reason: a default would let a
-- row that never stated a split read as though it had. charge_intents is empty
-- in every environment (production measured 0 on 2026-08-31), so there is no
-- backfill and nothing this can be wrong about. If it is ever non-empty when
-- this runs it fails loudly rather than inventing a split.

ALTER TABLE ms_billing.charge_intents
    ADD COLUMN wallet_allocation_micros  BIGINT NOT NULL,
    ADD COLUMN provider_remainder_micros BIGINT NOT NULL;

ALTER TABLE ms_billing.charge_intents
    ADD CONSTRAINT charge_intents_funding_non_negative
    CHECK (wallet_allocation_micros >= 0 AND provider_remainder_micros >= 0);

-- The balance arithmetic, enforced by an evaluator that is not the Go process.
-- Seal refuses an unbalanced split too; this is the half that holds if a row is
-- ever written by something that skips the sealing path.
ALTER TABLE ms_billing.charge_intents
    ADD CONSTRAINT charge_intents_funding_balances
    CHECK (wallet_allocation_micros + provider_remainder_micros = total_micros);
