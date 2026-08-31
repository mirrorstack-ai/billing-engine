# What "already implemented" costs

Scoped 2026-08-31 by 42 agents over the 17 unbuilt executor gates, every analysis
adversarially verified. **Verified size tally after refutation: LARGE 26, MEDIUM 4, SMALL 2** across 32 scoped items.

The owner's condition for dropping the legacy collectors was *"if already implemented,
directly drop it."* This document is the answer, and the answer is NOT IMPLEMENTED —
not partially, not mostly.

> Adversarial verification made the estimate WORSE, not better: the first pass scored
> 16 LARGE / 11 MEDIUM / 5 SMALL; refutation moved it to LARGE 26, MEDIUM 4, SMALL 2.

---

MIRRORSTACK BILLING INTENT ENGINE — REMAINING IMPLEMENTATION PLAN
Scoped against /Users/owo/Documents/MirrorStack-AI-V2/be-wt-shadow, verified in code 2026-08-31.

====================================================================
0. THE ANSWER TO THE OWNER'S QUESTION, FIRST
====================================================================

The owner's condition is "if already implemented, directly drop it."

For all eleven legacy money paths the answer is: NOT IMPLEMENTED. Not partially,
not mostly. The intent rail has never sealed an intent, never minted an
authorization, and never settled anything.

The measured facts and the code agree, and the code is more damning than the
numbers:

  - cmd/intent-executor/main.go:61-74 constructs the executor and then writes
    `_ = exec`. There is no work loop, deliberately. Nothing in this tree can
    call executor.Execute in production.
  - cmd/intent-executor/main.go:98-101 (readiness) refuses to start at all while
    capabilities.LegacyMoneyPaths != 0, and internal/account/capabilities/
    capabilities.go:36 pins that at 11.
  - cmd/intent-executor/main.go:112-124 (environment) returns every gate false,
    with a comment saying so. So even if it ran, internal/intent/predicate/
    predicate.go refuses all 29 clauses' worth of intents.
  - BILLING_CYCLE_INTENT_CUTOVER's only armed value is the string
    "propose-do-not-collect" (cmd/billing-cycle/main.go:388). The legacy path
    still collects. The intent is a shadow document.

Dropping the legacy collectors today does not migrate collection. It stops
collection. The 15 invoices, the 2 rolled-up closed periods and the 0
mis-invoiced total all came from the code being dropped.

Worse, only 3 of the 7 deletable legs even PROPOSE an intent today. `grep
proposer.` over non-test code returns exactly three call sites:
internal/account/cycle/overage.go:824, internal/account/cycle/domain_charges.go:393,
internal/account/autotopup/executor.go:1327. The period-boundary collector
(cycle/charge.go — usage arrears plus advance base, the largest single charge in
the system), combined proration (cycle/proration.go), credit purchase
(creditpurchase/executor.go) and unpaid-invoice retry (billing/unpaid.go) have
no intent proposer at all. Four of seven legs have not started.

So "already implemented" costs, in round terms: 17 new subsystems in this repo,
12 of the 16 open §12 decisions, code in 5 sibling repos, one merchant agreement
and one legal-entity decision, plus 4 charge legs written from scratch. That is
quarters for a team, not weeks — and roughly two-thirds of the calendar is
decision latency, not engineering.

There is also a hard circularity that must be broken by an owner decision before
any of this starts. See section 4(E).

====================================================================
1. DEPENDENCY ORDER
====================================================================

The predicate is 29 clauses (internal/intent/predicate/clause.go AllClauses).
Six of them (RailSupportsPlan, ProviderAutonomy, FirstStepMatchesPlan,
InstrumentBinding, EnclaveReady, AttemptFrozen) are skipped when
Funding.ProviderRemainderMicros == 0 (predicate.go:44-51). Today that escape is
unreachable: internal/intent/executor/executor.go:372-377 (fundingFor) hardcodes
WalletAllocationMicros: 0 and ProviderRemainderMicros: sealed.TotalMicros() for
every intent. So all 29 apply to every intent this tree can build.

Of the 29: 12 are satisfiable from records that exist (two of those dishonestly —
see TIER 1 item D), 3 are the top-level Environment bools, 14 are Unbuilt.

--- TIER 0 — DIGEST-SHAPE CHANGES. DO THESE FIRST OR PAY FOREVER. ---

Every one of these adds a field to the canonical encoding
(internal/intent/canonical.go, tag "mirrorstack.charge-intent/v1" at
canonical.go:16) and therefore changes every intent digest. charge_intents = 0
means the cost today is one migration and moving golden-digest tests. After the
first sealed production intent it becomes a migration of live sealed documents
that must pass the INV-003 reject-sealed-update trigger at
migrations/billing/054_intent_core.up.sql:97-121. This is exactly the argument
executor.go:342-346 already makes for the by-kind funding selection: "Writing the
selection now, while they agree, is the only time it can be done without a
migration of live documents."

  T0.1  Seal a digest per named policy revision (terms, price book, notice, tax)
        beside the four id strings at internal/intent/chargeintent.go:378-414.
        Without this, "digest-matching" has nothing to compare against and
        PolicyDigestsMatch can never be more than "the id resolves".
  T0.2  TaxDetermination.VerificationClass, a closed enum whose zero value
        refuses (independently_reproducible | provider_attested | unverified),
        on the type at chargeintent.go:53-65, validated in Seal beside the
        Resolved check at chargeintent.go:254, digested at chargeintent.go:397-399.
        Today provider_attested and independently_reproducible are byte-identical
        once sealed; the only thing preventing substitution is that nobody has
        written a vendor resolver.
  T0.3  Seal the selected rail and the routing-policy revision (DESIGN.md:1033-1037),
        AND add routing_policy to intent.UnpublishedRevisions
        (internal/intent/revision.go:56-71) in the same commit. That list
        enumerates exactly four revisions; a fifth sealed revision missing from
        it would pass ClausePolicyPublished unexamined — the identical hollowness
        predicate.go:190-203 records fixing on 2026-08-30.
  T0.4  Persist and digest the FundingPlan (mode, gross, wallet split, provider
        remainder, credit-policy id) instead of synthesising it at execution time
        in fundingFor. Until it is a record, predicate.go:158-170
        (ClauseFundingPlanBalances) cannot fail: Frozen is a literal true,
        Gross == TotalMicros by construction, and Wallet+Provider == Gross by
        construction. It is a clause named for a check that performs none.
  T0.5  Seal the tax determination's frozen inputs — but ONLY the fields
        something writes truthfully in the same change. A sealed basis field
        nothing computes is a fiction inside the digest.

  Batch T0.1-T0.5 into ONE canonical v2 change. Paying the supersession cost five
  times is the avoidable version of this bill.

--- TIER 1 — MECHANISMS WITH NO POLICY INPUT. ALL BUILDABLE NOW. ---

  A. Policy revision registry.  ms_billing.policy_revisions, append-only, with a
     reject-update trigger in the style of charge_intents_reject_sealed_update
     (054:97-121); kind, artifact bytes or content-addressed pointer, digest,
     effective_from/to, published_at; a loader; a canonical digest over the
     bytes; and an Environment supplier replacing the `false` literal at
     cmd/intent-executor/main.go:119. Absence of a row returns false.
     BUILD IT AND LEAVE IT EMPTY. An empty registry makes PolicyDigestsMatch
     false for a STRUCTURAL reason instead of because someone typed `false`,
     costs no policy decision, and turns the eventual publication of a policy
     into inserting a row rather than building a subsystem under time pressure.
     Feeds: PolicyDigestsMatch, TimeReadinessPolicy, a FK from notice_receipts.policy.

  B. Time readiness, in three separable halves.
     B1 (SMALL, zero dependencies, HIGHEST SAFETY RETURN IN THE WHOLE PLAN):
        make the predicate interval-aware. Put Earliest/Latest on SealedState and
        make every comparison use the conservative end — "has it started/elapsed"
        against Earliest, "has it ended/expired" against Latest — at
        predicate.go:84, :96, :140, :150 and :290. Zero interval refuses,
        mirroring the s.Now.IsZero() guard already at predicate.go:80.
        Why first: today, setting TimeReady = true with a ten-minute skew would
        permit every charge, because no clause anywhere compares an interval to a
        cutoff. The flag is checked at predicate.go:207 and then ignored. Build
        the comparison before the bound arrives, or the flip is decorative.
        Mutation proof: widen the interval across an execution-window edge and
        show a charge that passes today refuses.
     B2 Observation table, append-only, written from cmd/intent-shadow (which by
        construction moves no money — internal/intent/shadow/source.go:34-37 is a
        Querier with no Exec). Records the untrusted clock as an OBSERVATION so
        the real skew distribution is measured before any threshold is chosen. A
        monotonicity constraint on it doubles as rollback detection.
     B3 The bound itself. Not authorable in Go — it is a property of a time
        daemon. See section 3.
     B4 TimeReadinessPolicy content. NOTE, and this is a verified negative worth
        acting on: time readiness is NOT among §12's sixteen items
        (DESIGN.md:1911-2020). It is the one policy in this cluster that is
        engineering-authorable with no owner in the path. §12's prohibition on
        reconstruction does not reach it — but do not mint thresholds from what
        the code happens to tolerate today either; wait for B2 to measure.

  C. Payer proof stream.  ms_billing.customer_proof_streams head row per payer
     (sequence + chain commitment) plus append-only ..._proof_events, gap-free by
     a UNIQUE(payer, seq) and a next-seq CAS inside the durable write, MAC or
     signature over (payer, seq, prior commitment, event digest), and a VERIFYING
     READ whose boolean result becomes Unbuilt.ProofHeadCurrent. Route exactly ONE
     event kind through it to start — revocation, replacing the bare `revoked_at`
     COALESCE upsert at internal/intent/store/authorization.go:47-52 — with a
     schema CHECK enforcing the closed kind vocabulary.
     NOT an honest partial: shipping the tables and setting the flag from an empty
     stream nothing appends to. An empty log is trivially gap-free, so the clause
     would pass while ordering nothing. cmd/intent-executor/main_test.go:97
     currently asserts this field is false and is the guard against exactly that.

  D. The claim transaction rewrite. THIS IS A STRUCTURAL DEFECT, NOT A MISSING
     TABLE, and it is the one place where the current wiring makes an honest
     answer impossible by construction.
       - internal/intent/store/store.go:223-239 (ClaimSettlement) is a single
         pool.Exec with ON CONFLICT DO NOTHING. No pgx.Tx, no head lock, no
         appliedHead comparison, no generation CAS. Compare store.go:71
         (pgx.BeginFunc in SaveIntent) — the repo can do this; the claim path
         does not.
       - executor.go:224 assembles env, :225-241 evaluates, :252 claims. A bool
         computed at :224 cannot report what happened in a transaction that
         starts at :252. Setting Unbuilt.ProofsApplied = true from environment()
         would be false BY CONSTRUCTION even if the stream were fully built.
       - executor.go:238-239 hardcodes `PriorSettlementExists: false,
         ClaimAvailable: true` in the SealedState literal. Both clauses
         (predicate.go:208-211, :212-213) are therefore asserted, not measured.
     Fix all of it in one change: ClaimSettlement becomes a transaction that
     locks the head, checks appliedHead == currentHead, applies pending sequences
     up to the published batch, CASes a generation, inserts the claim, and
     commits — and ProofsApplied stops being a field on the pre-claim Environment
     and becomes the RESULT of that transaction. Mutation coverage already exists
     to point it at: internal/intent/predicate/predicate_test.go:178-179.
     HARD CONSTRAINT (INV-013, DESIGN.md:372-377, :598-604): the metering ingest
     path must never take this lock. internal/account/db/queries/usage.sql:210
     stays a lock-free idempotent insert, and DESIGN.md:603 requires ≥50 admitted
     facts/sec per payer to survive.

  E. payment_attempts table + executor reordering. Columns whose evidence exists
     today: intent_digest FK, claim linkage, rail identity, sealed provider
     remainder and currency, the derived idempotency key already built at
     executor.go:262, a transport digest attesting maxNetworkRetries = 0
     (internal/shared/stripe/client.go:71, :109-114), and the state enum from
     DESIGN.md:695-708. Leave permit_identity / enclave_scope / plan_step_effect_class
     ABSENT rather than defaulted. Insert it in the claim transaction, before the
     dispatch at executor.go:257, and make executor.go:238 read it.
     Useful on its own even with the flag false: it turns the in-memory Ambiguous
     and InProgress branches (executor.go:265-276) into the durable
     execution_unknown latch DESIGN.md:1093-1102 requires, which is what a
     reconciler needs to find an in-flight charge after a crash.
     TRAP: the moment attempts can exist, `PriorSettlementExists: false` becomes
     a lie in the direction that permits, and INV-008 gets WEAKER. These two
     changes must ship together.

  F. Exposure reservations, one-time authorizations only.
     A one-time authorization names exactly one document (054 constraint
     billing_authorizations_one_time_names_its_intent, enforced at
     internal/intent/authorization.go:440), so its window is already a schema
     fact rather than an invented cadence, and the inequality collapses to
     `candidate <= per_charge_ceiling` with a UNIQUE(authorization_id) making the
     uniqueness half a database property. Standing authorizations stay refused,
     which is also honest — their accepted window is precisely what §12 item 1
     has not settled.
     HIGHEST-VALUE BYPRODUCT IN THE PLAN: the same query fills intent.PriorUse,
     which nothing produces today (`grep PriorUse` outside tests returns only its
     definition at authorization.go:363 and the two call sites predicate.go:96
     and :150). Right now the period ceiling (authorization.go:450), the frequency
     ceiling (:456) and the never-stack-on-an-unresolved-attempt refusal (:468)
     all evaluate against zeros. Every intent looks like the first attempt of its
     period.
     Blocked structurally until: executor.Environment becomes per-intent. It is
     `env func(context.Context) Environment` (executor.go:137, called at :225) —
     deployment-scoped. Exposure evidence is per-intent. As written, Environment
     cannot express this gate at all. The same refactor is a prerequisite for
     SourceAllocation and CreditLotsReserved.

  G. Source allocation, non-usage legs only. For custom_domain proration,
     subscription_start and collect_receivable the source IS a single durable row
     and internal/intent/proposer/proposer.go:119 already seals its identity. A
     table over (lineage_root, source_ref) with a uniqueness constraint, inserted
     in the same transaction as SaveIntent (store.go:110-118), is honest.
     Must be LINEAGE-aware: a naive UNIQUE(idempotency_key) breaks INV-003
     supersede, which legitimately re-claims the same leaves
     (charge_intents.supersedes_digest, 054:67).
     Note what today's key cannot do: charge_intent_source_facts' PRIMARY KEY is
     (intent_digest, idempotency_key) — scoped INSIDE one intent, so the same leaf
     may enter two intents and both may settle. intent_settlement_claims cannot
     see it. That is verbatim the reasoning migration 056:118-127 already wrote
     for receivables.
     The usage-derived half is NOT buildable here — see section 3.

  H. The rail read-side, none of which flips a gate.
     H1 Carry MandateReference onto executor.Debit (executor.go:53-69) from
        auth.MandateReference() and refuse in stripeadapter.Collect when the
        resolved instrument differs.
        🔴 LIVE DISCREPANCY, report this to the owner separately: nothing
        compares the accepted mandate to the instrument charged.
        BillingAuthorization.Provider() and .MandateReference()
        (authorization.go:540-542) have ZERO non-test callers.
        cmd/intent-executor/payer.go:38-48 resolves the instrument by
        `pm.is_default AND pm.deleted_at IS NULL` — the customer's CURRENT
        default — and adapter.go:168-169 hands it straight to PayInvoiceWithMethod.
        An intent authorized against mandate M would collect against whatever card
        is default at execution time. authorization.go:136-140 states in its own
        comment why that is wrong, and the code does the thing the comment forbids.
        It is unreachable only because environment() returns all-false.
        This must land as its own refusal, NOT by setting Unbuilt.InstrumentBinding.
     H2 A RailCapabilities read on stripeadapter declaring supported currencies
        WITH THEIR EXPONENTS, and make centsFromMicros
        (internal/provider/stripeadapter/adapter.go:212-217) consult the exponent.
        It is hardcoded to two decimal places, and adapter.go:122 lowercases and
        passes through whatever currency it was handed. A zero-decimal currency is
        currently mis-scaled by 100x on a path that already reaches Stripe.
     H3 ProviderExecutionPlan, DERIVED not invented. internal/architecture/
        allowlist.go:99-102 already names all four intent mutations with reasons,
        and CI fails both when a call site is unlisted and when a listed entry has
        no call site (allowlist.go:11-17). That map is already a CI-enforced closed
        plan-step inventory. Freeze a 4-step plan (three non_adverse_prepare, one
        debit) into the intent at seal time and have the executor compare the step
        it is about to dispatch against index 0.
        Fence this removes: with all four calls behind one Collect method, an
        adapter change adding a fifth mutation is caught by the allowlist and by
        nothing in the predicate.
     H4 ProviderAutonomyPolicy on AuthorizationGrant with the single legal value
        DESIGN.md:442 already fixes (no_autonomous_future_debit), persisted with a
        CHECK in the shape of 056:57-82, and predicate.go:235 becomes
        `s.Unbuilt.ProviderAutonomy && s.Authorization.AutonomyPolicyAccepted()`.
     H5 Permit-aware RoundTripper + durable pre-send egress mark
        (DESIGN.md:983-990). The one EnclaveReady conjunct that is a code property
        rather than a deployment property. Idempotency keys never substitute
        (DESIGN.md:999-1000).

  I. Split UnbuiltEvidence.EnclaveReady into its six named sub-facts (credential
     scope, writer/permit, transport fence, adapter capability, evidence class,
     attestation) and AND them in predicate.go, so each can go true on its own
     evidence and a refusal names which half is missing. Same repair
     ClausePolicyPublished got at predicate.go:203-205.

  J. Honest-refusal tightenings, each SMALL, each needing no decision, each
     strictly narrowing what a future `true` could permit:
     J1 ClauseTaxFinal (predicate.go:172-183) reads Tax().Resolved — but Seal
        already refuses an unresolved determination (chargeintent.go:255,
        ErrTaxUnresolved) and store.LoadIntent force-sets Resolved = true
        (store/store.go:165), so that conjunct is unfalsifiable on the executor
        path. Require non-blank Jurisdiction and non-blank RuleRevision instead.
     J2 Nothing reads Tax().Jurisdiction at all. An intent could seal jurisdiction
        "Atlantis" and no clause objects. Give ClauseCommercialIdentity its
        readable half — refuse a placeholder or an unregistered jurisdiction —
        in the ClauseInstrumentBinding shape (predicate.go:250-262).
     J3 Change `not-applicable/pending-decision-12` to
        `unpublished/pending-decision-12` at internal/account/cycle/
        domain_charges.go:60 and overage.go:839 (autotopup/executor.go:1383
        already says `unpublished/`). RevisionPublished refuses both, so it buys
        no gate progress — but `not-applicable` is a substantive CLAIM that tax
        does not apply, sealed inside the canonical digest a customer's bundle
        attests to, for a Taiwan entity whose duties §12 item 10 leaves undecided.
     J4 Compare the executing rail to BillingAuthorization.Provider() and to
        Disclosure.Rail (internal/intent/disclosure.go:38-39, sealed into the
        disclosure digest at :72). Nothing compares either today.

--- TIER 2 — NEEDS AN ACCEPTED §12 ADR BEFORE A ROW CAN EXIST ---

  Terms revision              ← §12 items 2, 3, 9, 13
  Notice policy revision      ← §12 item 1   (smallest artifact of the four:
                                 a handful of scalars. Reconcile with the
                                 per-authorization lead time, which can disagree
                                 with a published policy — something must say
                                 which governs.)
  Price book revision         ← §12 items 11, 12, 15
  Tax policy revision         ← §12 items 4, 5, 6, 7, 8, 10
  MerchantBindingSet + MoR    ← §12 items 4, 10, 11
  CommercialIdentityBinding
    + TaxProfileReceipt       ← §12 items 5, 7 (and MoR upstream)
  Credit lot class + terms
    + reservations            ← §12 item 13
  FundingMatchesAccepted's
    mode / credit-policy /
    provider-permission
    conjuncts                 ← §12 items 1, 13 (its SPLIT and CAPS conjuncts
                                 are Tier 1; only these three wait)

--- TIER 3 — CROSS-REPO / EXTERNAL ---
  See section 3.

--- THE HARD EDGES, AS A LIST ---

  ProofHeadCurrent  →  ProofsApplied  →  {NoPriorSettlement, ClaimAvailable}
  ProviderExecutionPlan  →  {RailSupportsPlan, FirstStepMatchesPlan, AttemptFrozen}
  EnclaveReady  →  AttemptFrozen (permit identity, enclave scope columns)
  EnclaveReady  →  {ProviderAutonomy, InstrumentBinding}  (read-only credential;
                    executor_assertion_only is never enough, DESIGN.md:1022-1024,
                    and executor.go:280-286 currently appends `succeeded` on it)
  MerchantOfRecord  →  CommercialIdentity  →  TaxIndependentlyReproducible
  Policy registry  →  {PolicyDigestsMatch, TimeReadinessPolicy, notice FK}
  ExposureReservation  →  PriorUse  →  un-vacuums WithinCeilings + AuthorizationValid
  CreditLotsReserved  →  FundingMatchesAccepted (wallet leg only)
  Price book  →  SourceAllocation's per-fact accrual bound
  Per-intent Environment refactor  →  {SourceAllocation, CreditLotsReserved,
                                       ExposureReservation, RailSupportsPlan}
  DESIGN §11 fixes: step 6 (enclave) precedes step 7 (adapters) precedes step 8
  (delete direct charge code and revoke legacy credentials).

====================================================================
2. SMALLEST FIRST SLICE THAT LETS ONE INTENT SETTLE HONESTLY
====================================================================

THERE IS NO SUCH SLICE. Not a small one, not any one. Every path to a single
honest settlement runs through accepted §12 ADRs, and this is provable from the
predicate rather than argued.

Walk the cheapest conceivable intent and watch it die.

  Attempt A — the wallet-only escape (skips 6 provider clauses).
    Requires Funding.ProviderRemainderMicros == 0, i.e. wallet allocation
    implemented. Blocked twice: executor.go:352-357 hardcodes
    WalletAllocationMicros: 0 with the comment "Wallet allocation is not
    implemented"; and the census measures credit_ledger = 0 rows, so no customer
    has any credit to spend. The cheapest predicate path has no production
    instance. It also needs §12 item 13 (legal characterization of stored value).

  Attempt B — a credit purchase (§6 fixes walletFunding = 0, so CreditLotsReserved
    is vacuously satisfiable).
    providerRemainder == gross > 0, so all six provider clauses apply anyway:
    ProviderExecutionPlan, EnclaveReady, AttemptFrozen, InstrumentBinding,
    ProviderAutonomy, RailSupportsPlan. And it needs §12 item 13 regardless.

  Attempt C — a customer-present, one-time-authorized custom-domain charge
    (the kind with a shipped proposer at domain_charges.go:393). This is the
    genuine floor. All 29 clauses apply. It still requires:

      ClauseTaxFinal (predicate.go:172-183) → TaxIndependentlyReproducible.
        There is no shortcut through "we don't charge tax": DESIGN.md:1349-1351
        requires not_applicable to be reproduced from an immutable public rule
        with its inputs. "Tax does not apply" is a CLAIM needing an artifact.
        → §12 items 4, 5, 6, 7, 8 (+10 for a Taiwan entity).

      ClausePolicyPublished (predicate.go:200-205) → PolicyDigestsMatch AND
        len(UnpublishedRevisions) == 0. UnpublishedRevisions
        (revision.go:56-71) checks ALL FOUR revisions unconditionally —
        customer-present mode does not exempt the notice policy from being
        published, it only exempts the notice from being DELIVERED.
        → §12 items 1 (notice), 2/3/9 (terms), 11/12 (price book), 5-8 (tax).

      ClauseMerchantOfRecord → §12 item 4. DESIGN.md:1920-1922 makes it "Blocks
        G1, G3 and G4" and the settlement route and tax determination both bind
        to it.

  Minimum decision set for ONE honest settlement: §12 items 1, 2, 3, 4, 5, 6, 7,
  8, 9, 11, 12 — eleven of sixteen. Add 13 if the first intent is stored-value.
  Add 16 (whose current position is "declined" — that position must be written
  down as an accepted ADR, because DESIGN.md:2018 says item 16 blocks G1).

THE DECISION TO NAME, IF THE OWNER WANTS ONE SENTENCE:
  Nobody has published a tax policy revision, a terms revision, a notice policy
  or a price book. Until those four exist as accepted, immutable, effective-dated
  artifacts, the predicate refuses every intent for a reason no engineering can
  remove — and DESIGN.md:2012-2014 explicitly forbids the shortcut of minting
  them from today's constants, code comments, or the shape of today's
  Stripe-shaped schema. DECISION-12-POLICY-REVISIONS.md already withdrew that
  shortcut once for this exact reason.

WHAT TO BUILD INSTEAD, IN THE MEANTIME, RANKED BY VALUE PER UNIT OF WORK:
  All of these are honest today, none flips a gate, and every one of them only
  ADDS refusals — so none can create the declared-but-unimplemented defect
  SECURITY.md exists to expose.
    1. Tier 0 in one canonical-v2 change. Free exactly once; charge_intents = 0.
    2. B1 — interval-aware predicate. The flag is checked and then ignored today.
    3. F — one-time exposure reservations, which produce PriorUse and un-vacuum
       three ceiling checks currently evaluating against zeros.
    4. D — the claim transaction, which replaces two hardcoded lies
       (executor.go:238-239) with measurements.
    5. H1 — mandate carried onto Debit. Closes a live discrepancy.
    6. A — the empty policy registry, so the refusal becomes structural.
    7. J1-J4 — the four hollow-clause repairs.
  Rough shape: that is one engineer for a quarter, and at the end of it not one
  additional gate is true. That is the correct outcome, and it is the point.

====================================================================
3. BLOCKED OUTSIDE THIS REPO, AND ON WHAT
====================================================================

CODE IN SIBLING REPOS
  api-platform + app-module-sdk + mirrorstack-cli — module_version at usage
    emission. intent.PriceKey requires it (pricebook.go:18-22; ErrPriceKeyIncomplete
    at :62, guard at :84 refuses a version-blind key), production usage is
    entirely version-blind, and SHADOW-PRICING-GAP.md measures
    metric_version_prices = 0 rows. So the key that matches production usage
    cannot be put into a price book AT ALL. Blocks: the price book revision
    covering metered usage, SourceAllocation's per-fact accrual bound, and
    DESIGN §11 step 4's reconciliation (shadow/source.go:186-188 refuses to run:
    "the price catalog is empty; a shadow run against it would quarantine every
    period and report a clean sheet of nothing"). The 38,326 existing events
    cannot be retro-allocated — legacy-shape pricing compatibility is out of
    scope by owner decision.
  api-platform — a record-acceptance / record-revocation RPC. The engine is
    relay-only and cannot mint proof (SECURITY.md:251), and cmd/account-api has
    no such route. Blocks: proof-stream event content, and
    ClauseAuthorityEvidence — executor.go:226-241 omits SealedState.Acceptance
    entirely, so customer-present authority already refuses at predicate.go:276.
  api-platform + web-account — the disclosure bytes and the customer review /
    download surface. REMAINING-LEGS-PLAN.md:177-186 records that AcceptanceDigest
    exists but the disclosure bytes are not built. Blocks: showing a customer the
    wallet/provider split and per-cap ceilings that FundingMatchesAccepted would
    compare against.
  api-platform + web-account — the address / registration / exemption enrollment
    surface. The engine may only receive an engine-issued envelope relayed
    unchanged (DESIGN.md:1378-1380). Blocks: CommercialIdentity.
  mirrorstack-docs — db/ms_billing must be updated in the same cycle as every new
    ms_billing table. That is roughly ten new tables in this plan.

INFRASTRUCTURE
  mirrorstack-infra — a scoped, mutation-capable credential with one exclusive
    owner. Measured today: EIGHT non-test call sites read the same
    STRIPE_SECRET_KEY (cmd/account-api/main.go:521, account-webhook/main.go:95,
    account-webhook-eventbridge/main.go:72, billing-cycle/main.go:334,
    infra-egress-sync/main.go:197, infra-ssr-compute-sync/main.go:144,
    pm-default-backfill/main.go:28, intent-executor/main.go:53), all published as
    Lambda zips. The executor is one of eight holders of the credential it would
    attest exclusivity over. VERIFICATION.md:255-259 requires GENERATED
    inventories from that repo, which this repo cannot produce or verify.
    Blocks: EnclaveReady.
  mirrorstack-infra — a readable clock-discipline error bound. A repo-wide grep
    for chrony/ntp/clockbound/time_sync returns nothing but one unrelated test
    name; the only clock is `func() time.Time { return time.Now().UTC() }` at
    cmd/intent-executor/main.go:58. Every binary here is a short-lived Lambda, so
    Go's process-local monotonic reading cannot detect rollback across
    invocations. Blocks: TimeReady half B. If the answer is "not available on this
    compute form", this becomes a compute-form change, not a billing change.
  Stripe — a restricted read-only key. Blocks: lifting the evidence class above
    executor_assertion_only, the ProviderAutonomy read-back and the
    InstrumentBinding read-back. Note also that Stripe account-level autonomy
    (smart retries, dunning, Revenue Recovery, provider-side subscriptions) is
    configured in the Dashboard and is not readable through the four write
    methods the adapter holds (internal/shared/stripe/types.go:378-383).

COMMERCIAL / LEGAL, NOT ENGINEERING
  Legal entity registration per market; the Stripe account(s) per entity; the
  NewebPay merchant agreement (DESIGN.md:1647-1651 forbids implementing any
  NewebPay feature before the agreement and the official integration exist); and
  a tax rate source WITH REDISTRIBUTION RIGHTS — §12 item 8 is the hard one:
  without redistribution rights the jurisdiction is unsupported for automatic
  collection, and it must not fall back to an attestation. A commercial rate feed
  that cannot be redistributed makes the required public verifier artifact legally
  impossible. That is procurement, not a ticket.

DOES NOT EXIST ANYWHERE
  The offline verifier binary. VERIFICATION.md:167-171: "You cannot run this
  today. There is no verifier in the tree." `ls cmd/` confirms nine binaries,
  none named billing-verify. It is required by VERIFICATION.md:185-186 to
  reproduce tax from the public artifact — and INV-002 (DESIGN.md:70-82) requires
  ONE rating model shared by preview, settlement and the verifier, so building
  the executor-side tax reproduction without it risks creating the second
  implementation INV-002 exists to forbid: "two implementations of one question
  drift, and the looser one is the one that charges."

====================================================================
4. HONEST TOTAL SIZE, AND WHAT MUST BE DECIDED BEFORE ANY OF IT STARTS
====================================================================

(A) SCALE, STATED PLAINLY

  New subsystems required inside billing-engine: 17.
    policy revision registry · canonical v2 · time readiness (interval predicate,
    observation table, policy, bounded source) · payer proof stream + claim
    transaction · payment_attempts + state machine + latch · source allocation +
    ServiceAccrualExposure at ingress · exposure reservations + PriorUse · credit
    lot reservations + lot class + expiry fence · funding plan persistence +
    authorization caps/mode/credit policy · merchant binding set · commercial
    identity binding + tax profile receipt · tax policy artifact + evaluator +
    golden vectors · ProviderExecutionPlan + permits + purpose writers + egress
    journal + transport fence · rail capabilities + conformance suite + currency
    exponent registry · instrument-binding read-back (a PaymentReader port) ·
    offline verifier binary · Capabilities expansion (digests, limits, signing root)

  Roughly ten new ms_billing tables, each paired with a mirrorstack-docs update.
  One canonical digest version bump touching every sealed document.
  §12 decisions on the critical path: 12 of 16.
  Sibling repos with required code changes: 4 (api-platform, app-module-sdk,
  mirrorstack-cli, web-account) plus mirrorstack-infra plus mirrorstack-docs.
  External agreements: merchant agreement(s), legal entity registration, a
  redistributable tax rate source.
  Charge legs with no intent proposer written at all: 4 of 7 — and the largest
  one (the period-boundary invoice: closed-period usage arrears plus the new
  period's advance base, overage and domains, in one charge) is among them.

  This is a payments platform rebuild, not a feature. Multiple engineer-quarters
  for a team, and the long pole is not code — it is eleven accepted ADRs from
  finance, legal, tax and product owners who have not been named. DESIGN.md:1899
  leaves ownership "TBD rather than invented."

(B) WHAT THE OWNER MUST DECIDE BEFORE ANY ENGINEERING STARTS

  D1. Assign an owner to each of §12 items 1-16. Right now every one of them is
      TBD. No amount of engineering advances G1, G2, G3 or G4 without them.
  D2. Publish, or explicitly defer, the four policy artifacts: terms, notice,
      price book, tax. Nothing settles until all four exist. The notice policy is
      the smallest by an order of magnitude — a handful of scalars — and is the
      cheapest first proof that the ADR pipeline works at all.
  D3. Merchant of record (§12 item 4), because it is strictly upstream of the tax
      determination and the settlement route. Build order is MoR → commercial
      identity → tax reproduction. Not parallel.
  D4. §12 item 16, whose current answer is "declined". Either write that down as
      an accepted ADR — with INV-006 and INV-014 explicitly recorded as trust
      assumptions with after-the-fact reproduction rather than independent
      verification — or fund a separate identity product. DESIGN.md:2018 says it
      blocks G1 either way, so silence is not a third option.
  D5. Relaxing intent.PriceKey's version requirement and/or adding a Model
      dimension (SHADOW-PRICING-GAP.md is explicit this is not the implementer's
      call). Six of twelve priced aggregates resolved through metric_model_prices,
      and PriceKey has no Model dimension, so that half cannot be expressed in the
      intent price book under ANY relaxation of the version rule. Without this
      decision, §11 step 4's reconciliation gate stays unmet — not vacuous, UNMET:
      history exists and the rater cannot price it.

(C) WHAT MUST NOT BE DONE, EVER, TO MAKE THIS LOOK FINISHED

  Do not flip any bool in cmd/intent-executor/main.go:112-124 ahead of its record.
  Do not set a gate from an empty scan — an empty proof stream is trivially
  gap-free, an empty reservation table trivially conflict-free. Do not mint a
  policy revision from current constants (DESIGN.md:2012-2014, and
  DECISION-12-POLICY-REVISIONS.md already withdrew that recommendation once). Do
  not lower LegacyMoneyPaths by editing the scanner (LEGACY-DROP-PLAN.md is right:
  the eleventh path is a receiver miscount, and correcting it is safe only once
  the other ten are gone and there is nothing left for it to conceal).

(D) THE ONE THING THAT IS CHEAP TODAY AND EXPENSIVE TOMORROW

  charge_intents = 0. Every canonical-encoding change in Tier 0 costs one
  migration and some golden-digest test churn right now. The moment ONE intent is
  sealed in production, each of them becomes a migration of live sealed documents
  through the INV-003 reject-sealed-update trigger. If exactly one item from this
  entire plan is funded this quarter, fund Tier 0.

(E) THE CIRCULARITY THE OWNER MUST BREAK

  cmd/intent-executor/main.go:98-101 refuses to start while LegacyMoneyPaths != 0.
  LegacyMoneyPaths reaches 0 only by DELETING the collectors
  (capabilities.go:29-34, and LEGACY-DROP-PLAN.md is emphatic: "Deletion is the
  only move"). So the intent rail cannot be exercised in production until the
  legacy rail is deleted, and the legacy rail cannot be safely deleted until the
  intent rail has been exercised.

  A proposer-side dual-run already exists — BILLING_CYCLE_INTENT_CUTOVER's only
  armed value is the string "propose-do-not-collect"
  (cmd/billing-cycle/main.go:388), and the legacy path still collects. There is no
  executor-side equivalent, by design: VERIFICATION.md §2 says "a strong new
  surface beside one weak legacy route is not a strong deployment, it is the weak
  route with better documentation."

  Somebody must decide how the first real settlement happens. The options are a
  per-leg legacy count (weakening a claim that was deliberately made all-or-
  nothing), or a written, time-boxed, security-reviewed dual-run exception. Both
  are owner decisions with a security cost. Neither is an engineering choice, and
  the endgame cannot be planned until one is picked.

(F) THE SENTENCE THE OWNER ASKED FOR

  "Already implemented" is false for all eleven legacy money paths. The intent
  rail is a complete, well-tested, correctly fail-closed SKELETON that has never
  moved a cent and cannot be made to without eleven accepted ADRs and roughly
  seventeen subsystems. The legacy collectors are the only thing collecting money
  in this company today. Dropping them is not a cleanup — it is turning off
  revenue.
