# The 2027-01-01 price lock runbook

This is the operational calendar for Annex 1 (附表一) art 3 of the distributor
contract (billing-engine#269, core-v2#1843, billing plan B21): a 12-month price
lock on the App plan list prices, the org plan list prices, the module unit
prices of Annex 2 art 2, and the platform fee, running **2027-01-01 through
2027-12-31**. It does not restate the contract or the invariant it locks —
those live in [`DESIGN.md`](DESIGN.md) and
[`SECURITY.md#known-current-gaps`](../SECURITY.md#known-current-gaps). What
this file owns is the calendar: when to look, when to email, and the dates
that make a late notice a breach instead of a formality.

The machine-checkable half of this (whether a price the engine charges still
matches the printed book) is
[`internal/account/usage/testdata/contract_price_book_2027.json`](../internal/account/usage/testdata/contract_price_book_2027.json),
pinned by `TestContractPriceBook_*` in `internal/account/usage`. This runbook
is the human half: nothing here is enforced by CI, which is why the owner
copies the dates below into an actual calendar (a tracker step on
core-v2#1843).

## The seven cost-linked rows

Only these rows may move during the lock (owner D507), and only under the
rule in the next section:

| row | fixture id | unit |
|---|---|---|
| video storage | `storage-video` | per GiB-month |
| ad asset storage | `storage-ad-asset` | per GiB-month |
| dynamic page requests | `deploy-dynamic-requests` | per 1,000,000 requests |
| dynamic compute | `deploy-dynamic-compute` | per 1,000 GB-seconds |
| dynamic outbound | `deploy-dynamic-outbound` | per GiB |
| static file requests | `deploy-static-requests` | per 1,000,000 requests |
| static file reads | `deploy-static-reads` | per 1,000,000 reads |

Transcode is module usage of the video-transcode module and is **locked**,
not cost-linked (owner D507) — a supplier cost change never moves it.

## Monthly: look for a supplier price change

**On the 1st of every month, 2027-01-01 through 2027-12-01**, check:

1. AWS's published price list for ap-northeast-1 — Lambda ARM64 duration and
   request pricing (`deploy-dynamic-compute`, `deploy-dynamic-requests`), Lambda
   data-transfer-out pricing (`deploy-dynamic-outbound`), and S3 storage
   pricing (`storage-video`, `storage-ad-asset`, billed through the shared
   platform infra storage sentinel, `infra.storage.gib_hours`).
2. Cloudflare's published Workers Paid and R2 pricing — edge request pricing
   (`deploy-static-requests`) and R2 class-B read pricing
   (`deploy-static-reads`).

No change found: nothing to do, check again next month. A change found:
follow the increase or decrease procedure below for that row.

## A supplier cost increase: 45-day advance email

Annex 1 art 3 para 2: an increase on a cost-linked row may not exceed the
supplier's own cost change, and not more than **20% cumulative from the
2027-01-01 baseline in any one year** of the lock.

1. Confirm the supplier change against the published notice or an actual bill
   — that document is the `supplier_evidence` the fixture's `change` record
   must carry (a URL or an invoice id, never a secret, since this repo is
   public).
2. Compute the cumulative rise from the 2027-01-01 baseline (the fixture's
   `charged_micros_per_unit` for that row today) including this change. If it
   exceeds 20%, the increase must be capped at 20% cumulative for this
   contract year — it may not simply pass the full supplier increase through.
3. Send the 45-day email **at least 45 days before** the new price takes
   effect, attaching the supplier notice or bill.
4. Add a `change` object to the row in
   `contract_price_book_2027.json`: `direction: "increase"`,
   `effective_from`, `notice_sent_on`, `supplier_evidence`,
   `cumulative_rise_pct`. `TestContractPriceBook_CostLinkedRowsWithoutAChangeAreUnmodified`
   is the guard: it is red until the notice window and the 20% cap both hold.
5. Update `charged_micros_per_unit` (and `contract_price_usd`) on the row to
   the new value in the same PR as the `change` object — this is the one case
   where a hard-coded price constant in `internal/account/usage` is meant to
   move, and it moves together with the fixture, in one commit, with the
   owner's manual review (the merge gate in `CLAUDE.md`).

## A supplier cost decrease: 45-day pass-through

Annex 1 art 3 para 3: when AWS or Cloudflare's own cost for a cost-linked row
falls, MirrorStack must lower that row's price **within 45 days** of the
supplier's change, by the same mechanism (a `change` record, evidenced,
applied to the fixture and the code in the same PR). There is no 20% cap on a
decrease and no advance-notice requirement — the clock runs from the supplier's
change date to the pass-through's `effective_from`, and it may not exceed 45
days.

## Free plans and free features: never re-priced during the lock

Annex 1 art 3 para 4: every row marked `free` in the fixture (today:
`app-plan-free`'s base fee) must stay at `contract_price_usd: 0` for the whole
lock, no exceptions and no cost-linked path. `TestContractPriceBook_NoFreeRowCarriesAPrice`
enforces this over every row in `entries` and `pending_rows`, not just the app
plans, so a new free row added later is covered automatically.

## A paid prepaid year is never re-priced

Annex 1 art 3 para 6: a distributor (or app owner, if annual billing applies)
who has already paid for a year of a plan or add-on keeps that year's price
for the year they paid for, even if the list price changes under one of the
procedures above mid-year. This is a billing/invoicing rule, not a catalog
rule — it has no row in the fixture, because it is about which invoice an
*existing* payment settles against, not about what the current list price is.

## After the lock: 2028-01-01 onward

Once the 12-month lock ends, every row (cost-linked or not) may rise **at most
once per year**, by **at most 5%, or the national CPI rate for the prior year
if that is higher**, with the same **45-day advance email** rule as a
cost-linked increase. The 20% cumulative cap and the cost-linked restriction
both end with the lock — after 2028-01-01 a non-cost-linked row (an app plan
base fee, a module unit price, the platform fee) may also move, subject only
to the once-a-year / 5-or-CPI ceiling and the 45-day notice.

**The last day to send a notice for a price that takes effect 2028-01-01:**

- **Tuesday 2027-11-16** if "45 days before" is counted as 45 full days
  strictly before the effective date (the stricter reading).
- **Wednesday 2027-11-17** if the 45-day window is counted inclusive of the
  effective date itself (so that notice + 45 days lands exactly on
  2028-01-01).

Both readings are defensible from "a 45-day email"; this runbook does not
pick one. **Send by 2027-11-16** to satisfy either reading, and treat
2027-11-17 as the absolute last day under no circumstances to be relied on if
avoidable.

## A breach

Annex 1 art 3 para 5: a price change outside these rules — no notice, a late
notice, a cost-linked rise over 20% cumulative, a free row charged, or any
rise at all on a locked (non-cost-linked) row during the 2027-2028 lock year —
lets the distributor object and end the agreement. Treat every red
`TestContractPriceBook_*` result as a stop: do not merge past it, and do not
edit the test to make it pass without the underlying notice/evidence actually
existing (the same owner rule that governs the scanner-pinned
`LegacyMoneyPaths` in `CLAUDE.md` applies here — the test exists so a breach
is a visible CI failure, not a silent catalog edit).
