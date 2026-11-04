# ADR 0001 — Taiwan e-invoice is a platform service, not a payment-rail feature

- Status: **Accepted**
- Date: 2026-11-04
- Tracker: [mirrorstack-ai/mirrorstack-core-v2#1850](https://github.com/mirrorstack-ai/mirrorstack-core-v2/issues/1850)
- Supersedes: `docs/DESIGN.md` §12 item 10 ("Invoicing duties, Taiwan and
  NewebPay") as previously worded. That item named one rail; this ADR settles
  the item as rail-neutral.
- Revision constant: `einvoice.EInvoicePolicyRevision` =
  `"einvoice-policy-2026-11-04"` (`internal/account/einvoice/policy.go`)

This ADR accepts [`docs/DESIGN.md`](../DESIGN.md)'s §12 item 10 as an
**immutable policy revision**, per the document's own closing rule ("Until
each of these is accepted as an immutable policy revision and an ADR, they
stay named decisions"). It is deliberately narrow: **data and policy only.**
No issuance code, no vendor call, and no production invoice exist after this
ADR — see [Scope and what this does NOT do](#scope-and-what-this-does-not-do).

## Context

billing-engine has no buyer-invoice data model and no recorded invoicing
policy. The nearest existing text ties invoicing to a specific rail: §12 item
10 was titled "Invoicing duties, Taiwan and NewebPay", and
`internal/account/usage/types.go` described the invoice tax line as "Stripe
Tax on the Stripe rail, ezPay 未稅/稅額/含稅 on the NewebPay rail". Decision
D530 (owner ledger) rejects that coupling: no payment rail owns invoicing.

Billing-engine already seals a provider-neutral `ChargeIntent`
(`internal/intent/chargeintent.go`) and keeps rail selection out of the
charge's identity. Invoicing needs the same property: an invoice must be
issuable regardless of which rail produced the payment.

## Decision

1. **Invoicing is a platform service, separate from every payment rail
   (D530).** One vendor-agnostic invoice-provider port in billing-engine
   (`billing-engine#286`), separate from the money port (`executor.Collector`,
   `internal/intent/executor/executor.go`). Stripe, NewebPay, TapPay and
   manual bank transfer each write a neutral **payment fact**
   (`billing-engine#313`) and nothing else; invoice code never reads which
   rail paid.

2. **Each seller issues under its own 統編 and its own 字軌; MirrorStack
   operates the system (D533).** The invoice goes FROM a seller TO that
   seller's customer (D547). Sellers are tenants: the first-party Taiwan org
   (`mirrorstack-tw`), a distributor, later a customer app. The engine
   resolves the seller; a caller never supplies it.

3. **Sellers configure invoicing in their org distributor settings (D535).**
   MirrorStack signs and pays the vendor centrally by default; a per-seller
   switch to seller-pays is the fallback (D544).

4. **e首發票 (矽聯科技 Systemlead) adapter first, ECPay (綠界) second, both
   behind the one port (D551).** Seller-keys mode (each seller's own vendor
   credentials, sealed) ships first; platform mode (for ECPay: `PlatformID`
   plus a bound `MerchantID`) is later.

5. **Issue immediately and synchronously at the payment fact; no callback
   route (D550).** One constant, `einvoice.IssueTiming = AtPayment`. An
   asynchronous vendor answer is read back with a query call, never with a
   public notify route. Changing this constant is a new policy revision, not
   a code edit — see [Open and UNVERIFIED items](#open-and-unverified-items)
   for why a future revision might have to.

6. **Document kind is a seller-level fact, never a per-sale choice.** Whether
   a seller issues `tax_invoice`, `receipt`, or `none` comes from that
   seller's tax-bureau determination (`billing-engine#312`). A seller
   approved to use 統一發票 may not issue ordinary receipts instead.

7. **One invoice per payment fact, NT$ only, integers end to end.** Net, tax
   and gross are integers from the one NT$ function of
   `billing-engine#293`; charge, receipt and invoice match to the dollar. A
   foreign-currency charge cannot produce a Taiwan invoice in v1 (no FX,
   D499).

8. **Sandbox and vendor stage only until the Taiwan company exists (D504,
   D520).** A sandbox payment gets a test invoice only, never a production
   call (D524). MirrorStack's own statements before the company exists carry
   no 統一發票 (D494, D503).

9. **Retention: five years.** The buyer data and the buyer's consent to a void
   or allowance are kept five years (`billing-engine#270`; MOF rule point 9).

10. **Platform-operator duty (reading UNVERIFIED — 電子發票實施作業要點 point
    21): pass the buyer's carrier request through to the seller.** The
    platform does not choose or override a buyer's carrier preference.

11. **Snapshot rule: the payment fact (#313) and the invoice row (#287) copy
    the buyer invoice profile when they are written, and are immutable
    afterwards.** Changing the profile never changes an issued invoice. This
    is why `ms_billing.buyer_invoice_profiles` (migration 093) stores the
    buyer's CURRENT profile only — it is a source a later write reads from
    and copies, not a reference an invoice follows.

12. **This issue (`billing-engine#285`) ships data only: the buyer invoice
    profile table, its validators, and this ADR.** No vendor is called, no
    invoice is issued, and nothing here changes `internal/intent/catalog.go`'s
    closed seven-kind charge catalog or `LegacyMoneyPaths`.

## The buyer invoice profile (what this issue actually ships)

`ms_billing.buyer_invoice_profiles` (migration 093), one row per billing
account: `account_id`, `kind` (`personal` | `company`), `tax_id`,
`company_name`, `address` (optional), `email`, `carrier_type`, `carrier_num`,
`donation_code`, `updated_at`. CHECK constraints, enforced at the database and
mirrored by `internal/account/einvoice.Validate`:

- `company` requires a checksummed 8-digit 統編 (財政部 check-digit algorithm)
  and a `company_name`, and carries no carrier or donation code.
- `personal` carries no 統編. A carrier and a donation code are mutually
  exclusive (they name two different recipients for the same document).
- `carrier_type` is a neutral enum (`mobile_barcode`, `citizen_cert`,
  `vendor_member`); each adapter (`#286`) maps it to its own vendor codes.
- Default when no profile exists: personal, no carrier, no donation code,
  email when known from the account. What each vendor needs for that default
  case is the adapter's problem, not this table's.

Two verbs only: `GetBuyerInvoiceProfile` / `PutBuyerInvoiceProfile`, internal
control-plane RPCs (`api-platform#1016` proxies them). Online validation
against a vendor is the port's job (`#286`), not this table's.

## Scope and what this does NOT do

- No invoice is issued by this change. No vendor adapter exists yet (`#286`).
- No payment fact exists yet (`#313`) — the profile has nothing to snapshot
  onto until that table ships.
- `internal/intent/catalog.go`'s closed seven-kind charge catalog is
  unchanged; invoicing is not a charge kind.
- `Capabilities.LegacyMoneyPaths` is unaffected — this issue adds no money
  path and removes none.

## Open and UNVERIFIED items (researched defaults, D526)

These are researched defaults, not confirmed law or confirmed vendor
contract. A later revision of this ADR — with its own bumped
`EInvoicePolicyRevision` — is the only place that may change them:

- **Which MOF table row covers SaaS services is UNVERIFIED.** The governing
  rule is that services must be invoiced no later than receipt of payment
  ("以收款時為限"); which numbered row of the 財政部 table applies to a SaaS
  subscription specifically has not been confirmed against an accountant or
  the MOF text itself.
- **Prepaid credit timing is an open accountant question.** "At payment" vs
  "at use" for a prepaid credit purchase is unresolved; `AtPayment` is the
  default until an accountant rules otherwise.
- **The platform-operator carrier pass-through duty (電子發票實施作業要點
  point 21) is an UNVERIFIED reading.** The obligation is believed to require
  the platform to pass a buyer's carrier request through to the issuing
  seller, not to substitute its own.
- **Mobile-barcode and donation-code formats are researched defaults, not a
  single authoritative source.** Mobile barcode: `/` plus 7 characters of
  `0-9 A-Z + - .`. Donation code: 3 to 7 digits. Drawn from the vendors'
  public docs and the MOF rules, not from the ezPay manual alone — the only
  ezPay manual version actually read here is v1.2.1; a "1.2.2" referenced in
  older internal text is itself UNVERIFIED. `citizen_cert` and
  `vendor_member` carrier formats have no verified public shape and are only
  checked for non-emptiness at this layer; the adapter (`#286`) does the
  vendor-specific shape check before issuance.

## Consequences

- `docs/DESIGN.md` §12 item 10 no longer names ezPay or NewebPay as the
  invoicing mechanism; it points here.
- `internal/account/usage/types.go`'s `AccountBillTax` comment no longer
  implies the invoice tax line is a Stripe-or-NewebPay choice.
- `billing-engine#286` (port + e首發票 adapter), `#287` (issuance), `#312`
  (seller identity + document kind), `#313` (payment fact) and `#314` (cost
  event) build on this table and this ADR; none of them is shipped by this
  change.
- A new policy revision (timing, retention, or a newly-verified MOF reading)
  requires a new ADR and a new `EInvoicePolicyRevision` value — never a bare
  constant edit.
