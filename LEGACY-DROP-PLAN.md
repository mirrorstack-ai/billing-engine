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
