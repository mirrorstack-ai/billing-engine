# The NewebPay rail: open questions, answered in writing

Tracker: `mirrorstack-ai/mirrorstack-core-v2#1851`, plan key N5. This is the
first child; `billing-engine#291` through `#297` (N6–N10, N29) build against
what this note decides. Related, not duplicates:
[`billing-engine#96`](https://github.com/mirrorstack-ai/billing-engine/issues/96)
(the old umbrella this one re-derives rather than extends),
[`billing-engine#273`](https://github.com/mirrorstack-ai/billing-engine/issues/273)
(adds `creditRailFor`, which item 1 below extends), `core-v2#240` (the USD-wallet
decision item 5 cites).

> 🔴 **This is a target, not a build.** Nothing in this file ships code. Every
> type, function and table named `(unbuilt)` does not exist in the tree today —
> `git grep` for it and confirm that yourself rather than trusting the prose.
> Where a default is taken without the owner, it is marked
> **`Assumption (D526):`** so it is easy to find and to reverse. Money-path
> design on this repository is **owner-gated before merge**: the owner reads
> and merges this note, and green CI is a precondition for that review, never a
> substitute for it (see `CLAUDE.md`, "Collection stays off until the owner
> enables it").
>
> No customer name, key, hash or token value appears anywhere below. This
> repository is public.

This note answers twelve questions. Each decision names the package, type and
function it binds, with a file path, so a reviewer can `git grep` it rather
than trust the prose. Items 1–5 are the ones to read first if time is short;
6–12 can follow once the first five are accepted.

---

## 1 · Rail registry (M1)

**Decision:** one registry resolves a rail name to an adapter and refuses an
unknown name at lookup time, not at use time. Suggested home:
`internal/provider/railregistry` (unbuilt), e.g.
`func Lookup(rail string) (Adapter, error)`, returning a sentinel
`ErrUnknownRail` for anything not registered. `creditRailFor` from
`billing-engine#273` already decides `"stripe"` versus `"newebpay"` for a
credit purchase; N9 extends that function rather than replacing it, so the
registry and `creditRailFor` must agree on the same closed vocabulary.

**Decision (no DB CHECK on the value):** `selected_rail` on `charge_intents`
stays `NOT NULL` and `<> ''` with no closed-vocabulary `CHECK`
(`migrations/billing/062_intent_rail.up.sql:23-40` — the comment at lines
23-29 argues this explicitly: the rail a sealed intent names must be allowed to
widen without a migration, and the registry above is the enforcement point
instead). The execution predicate's only use of the rail is the direct string
compare in `ClauseRailSupportsPlan`
(`internal/intent/predicate/predicate.go:250-253`,
`s.Intent.SelectedRail() != s.Authorization.Provider()`), and that comparison
is rail-name-agnostic already — it needs no change to add a second name to
the vocabulary.

**Decision (credit purchase has no rail column):** `credit_ledger` does not
gain a `rail` column. For a Stripe purchase the row's identity already lives
in `stripe_invoice_id`
(`migrations/billing/048_credit_wallet.up.sql:94-102`); for a NewebPay
purchase the proof that the rail was NewebPay is the existence of a row in the
NewebPay payment-record table N9/N10 create, joined by the ledger row's id —
not a string on `credit_ledger` itself. This keeps one wallet schema honest
about "which table proves this happened" rather than letting a free-text
column assert it.

---

## 2 · Redirect-rail port (M2)

**Decision (new port, not an extension of `PaymentReader`):** §5 of
`docs/DESIGN.md` already sketches a narrow-interface shape for Stripe
(`PaymentReader`, `GrantConsumer`, `PaymentStepWriter`,
`docs/DESIGN.md:942-963`) — that shape is **unbuilt** and is a
session-initiated, capture-style model. NewebPay MPG is structurally
different: a browser form POST to a hosted page the customer leaves our
surface for, then an asynchronous server-to-server notice that arrives on our
ingress, not a response we read back synchronously. Modeling that as a variant
of `PaymentReader` would blur "what the engine can read" with "what the
browser must do," so this is a separate port:

```go
// Suggested package: internal/provider/redirect (unbuilt)
type Capabilities struct {
    SupportsBindOnce bool // false in v1
    SupportsBoundCharge bool // false in v1
}

type Port interface {
    Capabilities(context.Context) (Capabilities, error)
    Begin(context.Context, BeginRequest) (FormPost, error)
    VerifyNotice(context.Context, RawNotice) (Observation, error)
    ReadBackTrade(context.Context, TradeReference) (ProviderTrade, error)
}
```

`Begin` returns a `FormPost` (hidden fields + action URL for the browser to
submit, not a redirect the engine follows itself). `VerifyNotice` takes the
raw bytes/headers public ingress queued (item 7) and returns a typed
`Observation` — it does not itself decide settlement. `ReadBackTrade` is the
bounded `QueryTradeInfo`-shaped read-back item 7 requires for the sandbox
rule. The NewebPay adapter implements `Port` by calling the key broker (item
6) for every one of these four calls; it never holds the distributor key
itself.

**Decision (v1 scope and the bind extension point):** v1 is a one-off MPG
purchase only: `Capabilities.SupportsBindOnce` and `SupportsBoundCharge` are
both `false`, and the adapter refuses to construct a bind-once or bound-charge
plan. The extension point for binding is two additions to `Port`,
`BeginBind` and `ChargeBound`, gated by the two capability flags above — not a
new port, because the notice-verification and read-back shape is identical.
D518 says a bound card is in v1 of the *product*; it is explicitly not in v1
of *this port* (plan item N32 builds it).

A bind-by-charge-then-cancel flow — authorize a small amount to prove the card
works, then void it — does not fit "setup never holds funds" (§5 of
`docs/DESIGN.md`, the `non_adverse_prepare` versus `funds_hold` distinction at
lines 998-1002): an authorization is an adverse effect even when it is
reversed, because the hold exists and could fail to release. **A bind must
not look like credit on the ledger**, for the same reason: `BeginBind`
produces no `credit_ledger` row at all. There is nothing to settle, pending,
or refund — a successful bind writes only to wherever the bound-card
reference lives (not designed here; N32), and a failed one writes nothing.
The money-shaped bridge in item 4 below activates only for an MPG purchase
that an `Observation` reports as paid, never for a bind outcome.

---

## 3 · Seller (M3)

**`Assumption (D526):`** leg A (NewebPay/distributor) purchases are recorded
and settled directly by `internal/account/creditledger` and
`internal/account/billing`, and do **not** route through the intent executor.
That executor refuses to start while `capabilities.LegacyMoneyPaths` is
nonzero (`cmd/intent-executor/main.go:162-173`, `readiness`, checking
`caps.LegacyMoneyPaths != 0`; the constant is 3,
`internal/account/capabilities/capabilities.go:52`), so routing a brand-new
rail through the executor today would mean a second rail waiting behind the
same refusal the Stripe legs already wait behind, for no gain. This mirrors
how the existing legs behave: they propose and stop (`docs/DESIGN.md`
banner), and leg A does the same without first detouring through a path that
cannot execute yet.

**Decision (what identifies the seller, in v1):** The seller is the
distributor org (an `api-platform` identifier already carried on the
purchase request) plus `MerchantID` on the NewebPay payment record N9/N10
create. Neither field lives on `charge_intents` or joins the canonical
digest in v1, because leg A never seals an intent.

**What changes when intents switch on.** A seller on the intent is a `v5`
canonical bump (`internal/intent/canonical.go:28` is `v4` today; the comment
at lines 16-22 documents what each prior bump added) and it waits until
`LegacyMoneyPaths` reaches 0 and leg A is cut over to propose-then-execute
like the Stripe legs. At that point `ClauseMerchantOfRecord`
(`docs/DESIGN.md:2004-2022`, the open item 4 of §12) stops being refused by
construction — today "the only production caller leaves false" because there
is no production caller asserting a seller at all — and a NewebPay intent
would need to populate it the same way a future Stripe-marketplace seller
would. This note does not resolve §12 item 4; it narrows what leg A needs
before that resolution exists.

---

## 4 · Settled-payment-to-wallet bridge (M7)

**Decision:** A new function next to the existing settlers, same package:
`internal/account/creditledger`, e.g.
`func (s *Store) SettleNewebPayPayment(ctx context.Context, mode, merchantID, merchantOrderNo string, amtTWD int64, grantMicros int64) (Settlement, error)`
(unbuilt), modeled directly on `settleStripeInvoice`
(`internal/account/creditledger/settlement.go:121-237`): it moves one
`credit_ledger` row from `pending` or `failed` to `settled` **exactly once**,
inside one `pgx.BeginFunc` transaction, locking the same way
`LockCreditAttemptByStripeInvoice` does today, and reuses the balance-mutation
math that already lives in that transaction rather than re-deriving it.

**Checks, before any balance mutation** (mirroring the status, currency and
amount checks in `settlement.go:175-212`):

- the NewebPay payment record's status is `paid`;
- `Amt` on that record equals the frozen NT$ amount recorded when the
  purchase started (item 5 below: the presentment amount is frozen at
  purchase-start time, not re-derived at settlement time);
- the grant equals the frozen grant micros recorded at purchase-start time.

**Decision (idempotency key):** The unique key is the payment record's own
`(mode, merchant_id, merchant_order_no)`, enforced by that record's own table
(N9/N10), not by `credit_ledger`. **No new unique index on `credit_ledger`.**
The existing two —
`credit_ledger_idempotency_key_uidx` and the Stripe-invoice unique index
(`migrations/billing/048_credit_wallet.up.sql:96-98` and `:100-102`) — are
untouched. The ledger purchase row's `stripe_invoice_id` column stays `NULL`
for a NewebPay-settled row; nothing reads that column as "is this a NewebPay
or Stripe purchase" — the join to the NewebPay payment table is what proves
that (item 1).

---

## 5 · Amounts (M8, M9)

**Decision:** The wallet stays one ledger, denominated in USD micros, for
every rail (owner decision 2026-08-02, `core-v2#240`: single USD wallet,
frozen NT$ presentment per purchase). The four existing USD-only assertions
stay exactly as they are and are **not** relaxed for this rail, because none
of them sit on the NewebPay path:

- `internal/account/creditledger/settlement.go:181-183`
  (`strings.ToLower(currency) != "usd"`, inside `SettleStripeInvoice` /
  `SettleManualStripeInvoice`'s shared body);
- `internal/account/creditpurchase/executor.go:618,643,714`;
- `internal/account/autotopup/executor.go:573-581,658`;
- `internal/account/cycle/types.go:233-236` (`const chargeCurrency = "usd"`).

A NewebPay purchase never calls any of those four functions. It calls the new
bridge in item 4 instead, which takes `amtTWD` as its own argument rather than
a `currency` string, so there is no "usd" check to fail or bypass.

**Decision (where NT$ lives):** The NT$ integer amount lives in: the
presentment columns on the NewebPay payment record (N9/N10), the per-SKU or
per-tier top-up configuration that maps a USD grant to a frozen NT$ price
(N8), and the NewebPay adapter's request/response shapes behind the item-2
port. It does not reach `credit_ledger`, `creditpurchase`, `autotopup` or
`cycle/types.go`. `centsFromMicros`
(`internal/provider/stripeadapter/adapter.go:111`, called from `Collect`)
belongs to the Stripe cents conversion and is not reused or generalized for
NT$ — NewebPay's `Amt` is already a whole integer, so there is no sub-unit
rounding step to share. N8 owns the pipeline that produces a frozen NT$
amount from a USD grant; this note only says where the number is allowed to
live once produced.

**`Assumption (D526):`** a distributor priced in USD does not get this rail.
NewebPay/MPG is NT$-only in this design; a USD-priced distributor relationship
stays on Stripe. If that changes, it is a new decision, not an extension of
this one, because it reopens the "which column holds the authoritative price"
question item 5 just closed.

---

## 6 · Where keys live (D516)

**Decision:** The engine never holds a distributor's NewebPay `HashKey` /
`HashIV` or merchant credential. A separate broker Lambda (infra half:
[`mirrorstack-infra#433`](https://github.com/mirrorstack-ai/mirrorstack-infra/issues/433),
filed under `core-v2#1849`) opens the sealed key blob per call, inside its own
attested enclave, and offers a fixed, closed set of verbs — the same shape
`docs/DESIGN.md:1808-1829` already requires for a credential that can also
mutate the merchant account ("a fixed read broker runs inside that same
attested enclave, exposing only operation-bound read procedures"). The public
webhook Lambda (N1/N10) holds no key at all, matching
`docs/DESIGN.md:1820-1829`'s `VerifyCallback`-style requirement: ingress
forwards raw bytes and headers, never a key, to a fixed broker procedure.

**Decision (the verb list):**

| verb | callable by | sandbox-only for now |
|---|---|---|
| `SignMPGRequest` (builds the `TradeInfo`/`TradeSha` form fields for `Begin`) | the NewebPay adapter (item 2), server-side purchase-start path only | no — needed in both modes |
| `VerifyNotice` (checks `TradeSha` on an inbound notice against the distributor's `HashKey`/`HashIV`) | the N10 notice worker only, never public ingress | no |
| `QueryTradeInfo` (bounded read-back of one trade by `MerchantOrderNo`) | the N10 notice worker, as the sandbox-mode read-back item 7 requires | **yes, for now** — live mode needs the owner decision item 7 flags |
| key rotation / provisioning (out of band, operator-invoked, not called by any billing-engine binary) | operator tooling only (mirrorstack-infra#433), never a billing-engine process | n/a |

No verb returns the raw key bytes to any caller; each returns only the
derived artifact (a signed form field, a verified boolean/typed observation,
or a trade snapshot). mirrorstack-infra#433 owns the broker's IAM role, its
KMS key, and the decrypt-rate alarm that watches for a caller exceeding the
shape of normal per-notice/per-purchase traffic.

---

## 7 · Notice path

**Decision:** Public ingress (N1, sandbox-only route for now) does exactly
one thing with an inbound NewebPay notice: queue the raw bytes and headers
unverified, and answer `200`. It calls no broker verb and makes no settlement
decision — the same split `docs/DESIGN.md:1820-1829` already draws for
Stripe callbacks that need a mutation-capable secret to verify.

A worker (N10) dequeues, calls the broker's `VerifyNotice` (item 6) to get a
typed, replay-bound `Observation`, and only then calls the item-4 bridge.
NewebPay does not guarantee a browser ReturnURL call — the customer can close
the tab — so **the ReturnURL path never settles anything**; only a verified
notice does. `SECURITY.md:258-262` is explicit that "executor return values
and provider callbacks are untrusted input... they must never declare an
intent settled. Settlement needs provider-signed evidence the core verifies
itself." NewebPay has no genuinely read-only key (unlike, say, a
Stripe-style restricted API key), so the sandbox rule is two-factor: a
signature-verified notice (`VerifyNotice`) **plus** a bounded
`QueryTradeInfo` read-back through the broker before the bridge in item 4
runs. Either one alone is the caller's self-report; both together is
provider-attested evidence the broker — not the public workload — was in a
position to check.

**Live mode is explicitly not decided here.** Turning on live-mode
`QueryTradeInfo` (or relaxing the double-check above) needs the owner's
decision and is gated the same way `core-v2#1849` gates the broker itself —
this note states the sandbox rule and defers live mode rather than silently
assuming one extends to the other.

---

## 8 · Migration numbers

`main` ends at `092_ai_model_prices_claude5_roster` as of this note (commit
`b1ff705`, 2026-11-02). Two PRs are open against `088`: `#234` and `#236`.
Plan item B37 moves `#236` to `093`. **So the first NewebPay migration is
`094`, unless that changes before a NewebPay PR actually lands** — each PR
takes the next free number at PR-open time, not at design time, and this
table is a plan, not a reservation.

| order | content | number (planned) |
|---|---|---|
| 1 | per-distributor NewebPay settings (merchant id, mode, key-blob reference — no key material) | 094 |
| 2 | TWD amount / presentment columns (N8) | 095 |
| 3 | NewebPay payment record table (N9/N10): `mode`, `merchant_id`, `merchant_order_no`, `amt`, status, unique key | 096 |
| 4 | test-account / sandbox-vs-live marker table (N29) | 097 |

**Two open NewebPay PRs must not claim the same number.** Whoever opens the
second concurrent NewebPay migration PR re-numbers against whatever `main`
actually ends at, the same way `#236` is re-numbering against `#234` today —
this table is the intended order, not a guarantee that these exact numbers
are still free when each PR opens.

---

## 9 · Architecture tests

**Decision:** NewebPay is plain HTTPS called only from inside the broker
Lambda (item 6) — there is no SDK import to confine the way
`internal/architecture/sdk_confinement.go` confines
`providerSDKPath = "github.com/stripe/stripe-go"` (line 13,
`ScanProviderSDKImporters`). The equivalent rule for a host instead of an
import: a new check, same package, same shape as the existing one —
suggested name `internal/architecture/newebpay_host_confinement.go`
(unbuilt) with its companion `_test.go`, scanning for the NewebPay API host
literal (e.g. `core.newebpay.com` / `ccore.newebpay.com`) the same way
`ScanProviderSDKImporters` scans for the import path, and asserting:

1. only the broker's own package (infra side, outside this repo, or a thin
   client package inside `internal/provider/newebpayadapter` if any host
   string must exist here at all) may name a NewebPay host;
2. no package under `internal/account/*` or `cmd/*` other than the broker
   client may import the key-opening/decrypt code path.

This is the same two-sided guarantee `allowlist.go` gives the Stripe mutation
inventory (`internal/architecture/allowlist.go:1-26`): a call site not listed
fails the build, and a listed entry with nothing left to justify it also
fails, so the inventory cannot drift in either direction. `docs/VERIFICATION.md`
§5's table is where this new check gets a row once it exists.

---

## 10 · Mode and test orgs (N29)

**Decision:** The mode vocabulary is exactly two values, `sandbox` and
`live`. Mode sits on the NewebPay payment record created by N9/N10 (item 8,
migration 3) as a column set at insert time and never updated afterward —
immutable per row, the same way a sealed intent's fields do not change after
creation. A row's mode is never inferred from which broker credential
answered; it is written down because the credential (item 6) is itself
sandbox- or live-scoped and the two must never silently disagree.

**Decision (test-org marker):** A small operator-set table (migration 4 in
item 8) marks which org ids are test orgs, defaulting every org to "not
test" until an operator adds a row. The credit-wallet rollout cohort cannot
distinguish a test purchase from a real one by any other signal available
today (there is no separate test wallet), so this table is the only place
that distinction can be made, and it is operator-controlled rather than
self-declared by the purchasing org — consistent with §3's framing in
`SECURITY.md` that a caller-supplied flag about itself grants no authority.

---

## 11 · Leftovers and sellers on lots (D527)

**`Assumption (D526):`** no seller tag on the credit lot itself in v1. The
wallet stays universal — a credit micro does not remember which distributor's
rail funded it, matching how the wallet already treats Stripe-funded and
(today, hypothetically) any other rail's credit identically once settled.

What happens to credits when a customer leaves a distributor is
`core-v2#1848`, not this note: **D527** answers it — leftover credit becomes
ordinary MirrorStack credit rather than being clawed back or frozen. This
note does not re-litigate that; it only confirms the lot schema has nothing
to migrate when D527 takes effect, because no seller tag exists on the lot to
remove.

---

## 12 · Notify topology

One paragraph, because the detail belongs to its own child rather than a
repeat here: the sandbox-only notify route, its queue, and the worker wiring
that consumes it are `billing-engine#295` (N1, infra half
`mirrorstack-infra#435`) and `billing-engine#296` (N10). This note only fixes
the two constraints those children must honor — public ingress never
verifies or holds a key (item 6), and only a verified notice plus a bounded
read-back settles anything (item 7) — and leaves the route, queue and IAM
shape to N1 and N10 themselves.

---

## Where this note differs from assumptions already written in the children

Acceptance for this note requires that any place it diverges from a default
already written into `#291`–`#296`/`#297` gets a comment on that child saying
so. As of this note's merge, the sibling issues (`#291`–`#297`) do not yet
carry their own written defaults to diverge from — they are scoped but not
specced — so there is nothing to reconcile yet. The first child PR that writes
a conflicting default against any `Decision:` above owes this note's
line a comment, not the reverse.

## Platform and alpha, unaffected

Platform is not module (D508): no module cost and no module-sandbox cap
(D441) goes on a NewebPay payment row — the payment record (item 8) carries
only what item 3's seller tuple and item 4's bridge checks need. Alpha is not
touched by anything in this note (D405, D463, D473).
