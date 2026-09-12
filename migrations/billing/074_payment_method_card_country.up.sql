-- 074: card_country on the payment-method mirror — the payer's tax-jurisdiction
-- signal for the ESTIMATED tax line on the account bill read.
--
-- core-v2#250 (owner, 2026-07-21): every list price is NET (未稅); tax is a
-- SEPARATE itemized line computed per the paying entity's rail and
-- jurisdiction at invoice issuance. Nothing records a payer jurisdiction today
-- (every charge intent carries Jurisdiction "not-applicable"), so the bill
-- read cannot even estimate the line. Until Stripe Tax is wired, the estimate
-- keys on the default card's issuing country (Stripe
-- payment_method.card.country, ISO 3166-1 alpha-2), which the
-- payment_method.attached webhook records here from now on and the
-- payment-method list read backfills for rows attached before this column.
--
-- NULL means UNKNOWN: the bill read then reports tax_status = not_configured
-- and 0, never a silent 0 that reads as "no tax".
ALTER TABLE ms_billing.payment_methods_mirror
    ADD COLUMN IF NOT EXISTS card_country TEXT NULL
        CONSTRAINT payment_methods_mirror_card_country_iso2
        CHECK (card_country IS NULL OR card_country ~ '^[A-Z]{2}$');
