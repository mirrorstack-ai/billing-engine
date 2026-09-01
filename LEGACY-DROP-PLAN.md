# The legacy drop, made mechanical

`legacy_money_paths` is **11** and cannot fall by cutting legs over. The
constant says so itself: it "reaches zero when the last of them is deleted —
not when an intent surface is added beside them." Deletion is the only move,
and every deletion is gated on a question only production can answer.

`scripts/legacy-drop-preconditions.sql` asks those questions. It is read-only,
enforced by a test, and every query has been executed against a migrated
schema — which is how three wrong table/column names were caught, plus one
(`status IN ('open','past_due')`) that would have returned a confident zero
because no such status exists.

## The map: one precondition, one deletion, one count

| # | precondition (script line) | delete | paths |
|---|---|---|---|
| 1 | no unresolved `billing_runs` with a frozen charge (:29) | `cycle/charge.go` boundary collector | 2 |
| 2 | no `app_module_overage_timers` armed-but-unresolved (:44) | `cycle/overage.go` collector | 2 |
| 3 | no `app_custom_domains` mid-charge (:58) | `cycle/domain_charges.go` collector | 2 |
| 4 | no unresolved `app_combined_proration_attempts` (:68) | `cycle/proration.go` collector | 1 |
| 5 | no pending `credit_ledger` purchase (:81) | `creditpurchase/` executor | 1 |
| 6 | no auto-top-up in `submitted_unknown` (:94) | `autotopup/` executor | 1 |
| 7 | no invoice in `open` awaiting retry (:108) | `billing/unpaid.go` collector | 1 |

Seven deletions, **10 of the 11** counted paths.

## The eleventh is a miscount, and must NOT be "fixed" to reach zero

`cmd/account-api/main.go:233` — `(*dispatcher).dispatch -> PayInvoice` — is the
JSON dispatcher delegating to `d.svc.PayInvoice`, the *service* method already
counted at row 7. The scan matches on method NAME without checking the receiver,
so a call to our own service is counted as a provider collect. The allowlist
knows: its header reads "the ten service call sites that can take money" over
eleven entries, and this row's own reason says "dispatcher delegating to the
service method above".

**Leave it alone until the other ten are gone.** Lowering the number by editing
the scanner is precisely what `LegacyMoneyPaths`' comment forbids ("Do not edit
this without deleting a money path"), and if the delegation analysis is wrong it
hides a real money path in a billing system. Once rows 1–7 are deleted the
count will read 1, the residue will be unambiguous, and correcting it then is
safe because there is nothing left for it to conceal.

## 🔴 DRAINAGE IS NOT THE ONLY GATE — added 2026-09-01

The table above asks one question per path: *is there in-flight state a
deletion would strand?* That is necessary and it is **not sufficient**.

A precondition can be zero — nothing in flight, nothing to strand — while the
intent rail still cannot collect the charge the collector was making. Deleting
it then does not migrate collection. It **stops** collection, silently, for
that charge kind. Every precondition in the table is clear today, and the
intent rail has never settled anything, so the table is currently at its most
misleading.

So each path needs a **second** gate: *can the replacement actually take this
money?* Measured 2026-09-01, that is the real state:

| # | path | drained? | proposes an intent? | can the rail collect it? |
|---|---|---|---|---|
| 1 | `cycle/charge.go` boundary | ✅ | ✅ **routed 2026-09-01** | ❌ every executor gate is false |
| 2 | `cycle/overage.go` | ✅ | ✅ | ❌ every executor gate is false |
| 3 | `cycle/domain_charges.go` | ✅ | ✅ | ❌ every executor gate is false |
| 4 | `cycle/proration.go` | ✅ | ✅ **routed 2026-09-01** | ❌ every executor gate is false |
| 5 | `creditpurchase/` | ✅ | ❌ **owner decision** — it changes a SYNCHRONOUS customer contract | ❌ |
| 6 | `autotopup/` | ✅ | ✅ | ❌ every executor gate is false |
| 7 | `billing/unpaid.go` | ✅ | ❌ **structurally impossible** | ❌ **not a charge** — `InvoicePayParams{}` sends no amount; it re-settles an obligation the provider already holds, and the adapter has no operation that pays an existing invoice |

**Not one row is deletable today**, and only the middle column would have said
so before this note existed.

### Updated 2026-09-01: rows 1 and 4 are routed, and their blocker is answered

Rows 1 and 4 said "blocked — FOUR §6 kinds" and "blocked — two §6 kinds". §12
item 12 answered it: `module_capacity` and `custom_domain` fold into
`platform_base`.

- The **boundary** became TWO kinds — `module_usage` for the closed period's
  arrears, `platform_base` for the next period — and two is what dissolved the
  rounding objection. The three fee components are exact whole-cent multiples,
  so folding them introduces no rounding, and the only sub-cent-fractional
  terms (arrears and the wallet draw) now sit in the SAME intent. A group that
  rounds once over the summed remainders takes exactly the cents the legacy
  path takes.
- **Combined proration** became ONE kind, so it needs no group at all.

Both now propose. Every collecting leg does.

🔴 **The middle column is now the load-bearing one, and rows 5 and 7 are the
whole of step 3's remaining risk.** They are the only paths that will never be
routed by writing more code:

- **Row 5** returns `invoice.ClientSecret` synchronously to the browser
  (`billing/credit.go:624-637`), and that secret exists only after the
  finalize. A proposing version returns a response the browser cannot use.
  Cutting it over changes a customer-facing contract, which is a product
  decision.
- **Row 7** links to a SOURCE INTENT that cannot exist until another leg is
  routed AND enabled and leaves an unpaid intent behind.

Deleting either collector does not strand a charge mid-flight. It makes that
charge kind **uncollectable from then on**, silently. The precondition script
cannot see this — it measures durable state, and every row here is already
clear. `internal/architecture/drop_needs_replacement_test.go` asserts it from
the code instead, and fails if `LegacyMoneyPaths` is ever set to 0 while either
row is unrouted.

### What has to become true, in order

1. ~~**The boundary/proration kind decision**~~ ✅ **ANSWERED 2026-09-01** —
   §12 item 12, fold into base. Rows 1 and 4 propose. What replaces it as the
   first blocker is **row 5's synchronous-contract decision**, because the drop
   is all-or-nothing (the executor refuses to start until the count is zero) and
   row 5 cannot reach zero without it.
2. **A standing authorization per account.** `billing_authorizations` measured
   0 on 2026-08-31, and INV-006 requires one per charge. Its shape is §12 item
   1, which is unanswered. An acceptance can no longer be a string a caller
   invented (migration 065), so minting is a real act with a real document.
3. **An evidence signing key.** INV-014 makes an evidence record a side effect
   of the money moving, and both `cmd/billing-cycle` (when armed) and
   `cmd/intent-executor` now refuse to start without one.
4. **The executor gates.** All seventeen are hardcoded false
   (`cmd/intent-executor/main.go`), and most need a published policy revision
   rather than code. `EXECUTOR-GATE-PLAN.md` scores the remainder LARGE 26 /
   MEDIUM 4 / SMALL 2.
5. **Then arm one leg, watch it settle, and only then delete its collector** —
   one at a time, re-running the precondition for that row in the same window
   as the deletion.

### Row 7 is not on that path at all

Unpaid retry is not a charge to route. It re-attempts settlement of an
obligation Stripe already holds; there is no integer to seal, and
`stripeadapter.Collect` would raise a SECOND invoice beside the unpaid one.
§6's `KindCollectReceivable` is the right model and needs a sealed SOURCE
intent, which the 15 production invoices — all from legacy legs — do not have.

It therefore becomes reachable only **after** the other legs are routed and
the rail itself starts raising invoices that can fail. It should not be
counted among the paths waiting on drainage.

## Order of operations

1. Run the preconditions (needs the ops Lambda, or a read-only DSN).
2. For each row that comes back **zero**, delete that collector and its tests.
3. Rows that come back non-zero: **stop**. Deleting code that owns in-flight
   state strands a charge nobody can finish or prove. Wait for it to drain.
4. Row 7 is flagged **REVIEW** rather than READY in the script itself: its
   collecting call has no deterministic idempotency key, by design, so its
   blast radius on a retry is different from the rest. Read the script's own
   note before touching it.
5. Only after rows 1–7: correct the dispatcher miscount, and `LegacyMoneyPaths`
   reaches 0.

## What is NOT gated on this

Nothing in PR #137. Every leg defaults to legacy, `BILLING_CYCLE_INTENT_CUTOVER`
is unset, and `cmd/intent-executor` refuses to start while any legacy money path
remains. The drop is the last step of the migration, not a prerequisite for
landing the work that precedes it.
