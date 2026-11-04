# NewebPay sandbox notify topology

Owner decision D515: a NewebPay payment notice lands on a **sandbox-only route
of the platform billing webhook**. Not a `dev --tunnel` address, not alpha.
This note names the host, who runs it, and what happens to a notice after it
arrives. It is a topology note, not a design doc — the invariants and the
execution predicate stay in [`../docs/DESIGN.md`](DESIGN.md) and
[`../SECURITY.md`](../SECURITY.md).

## The host

A sandbox notice reaches:

- **host:** the value of SSM parameter `billing-webhook-url` (`mirrorstack-infra`
  `stacks/billing.go:545-550`) — a raw `execute-api` address, HTTPS, port 443
- **path:** `/newebpay/sandbox/notify`
- **who runs it:** MirrorStack. This is the `billing-webhook` Lambda behind a
  plain HTTP API, in the production AWS account. The route is implemented in
  this repo at `cmd/account-webhook` — see the route table in
  `cmd/account-webhook/notify.go` and the handler in the same file. The
  coordinator deploys it; nothing here is agent-built infrastructure.
- **port:** https, port 443. The limit this deployment is built against is
  `mirrorstack-infra#435`'s own statement of it ("https, port 443"), not a
  fresh reading of NewebPay's manual — the official NewebPay manual was not
  reachable to confirm its wording ("the port must be 80 or 443") against
  this deployment's narrower choice. **This is UNVERIFIED against the
  distributor's own documentation.** Port 443 satisfies either wording, so
  nothing here is at risk if the broader reading turns out to be the correct
  one.

Production has no real customers today, and all production use of this route
is testing: sandbox payment notices are for test organizations only (D524).
**Nothing in alpha is used for any part of this flow** (D405, D463, D473) —
alpha is a different environment, and a NewebPay server-to-server notice
cannot reach it: `dev --tunnel` is the agent-built module sandbox's
authenticated WebSocket tunnel (`mirrorstack-cli`
`docs/platform-module-auth.md`), not an HTTP ingress a payment provider can
POST to, and the sandbox words on the decision ledger (D441, D480) describe
that tunnel, not this rail.

## The `ReturnURL` host

`ReturnURL` is a separate, browser-facing address: a `web-account` console
route the customer's browser lands on after leaving NewebPay's hosted page
(plan item N11, tracked separately from this route). It never settles
anything and this binary never serves it — it exists only so a human looking
at this note doesn't confuse the two NewebPay-registered URLs. The NewebPay
manual requires `NotifyURL` and `ReturnURL` to differ; they are different
hosts for different purposes, not two names for the same endpoint.

## The flow

1. NewebPay POSTs the trade notice (`MerchantID`, `TradeInfo`, `TradeSha`,
   `Status`, `Version`, `EncryptType`) to the sandbox notify path above.
2. The ingress (`cmd/account-webhook`) validates only that the required
   fields are present, does **not** decrypt `TradeInfo`, does **not** check
   `TradeSha`, and does **not** hold the HashKey/HashIV. It wraps the raw
   fields in a `Notice{Provider: "newebpay", Mode: "sandbox", ...}` — both
   read from the route table that matched, never from the body — and hands it
   to a `NoticeSink`. The table is provider-generic (`/{provider}/{mode}/notify`)
   so a future provider on the same ingress, such as TapPay, is an added row,
   not a second hard-coded path.
3. The sink sends one message to a queue (the infra child of this tracker,
   `mirrorstack-infra#435` — route, queue and ingress role), named by the
   `NEWEBPAY_NOTICE_QUEUE_URL` environment variable. In tests and local runs
   this route is exercised against a fake sink instead
   (`cmd/account-webhook/sink.go`'s `InMemorySink`). If this Lambda deploys
   before `mirrorstack-infra#435` provisions that variable, it does not crash
   or crash-loop: it logs one WARN at cold start and falls back to a
   not-configured sink, and the sandbox route answers the same 501 every
   other unwired path answers today, until the variable is set. Nothing
   changes in the ingress code when the real queue lands, only that
   environment variable.
4. A separate notice worker (N10, billing-engine#296) drains the queue,
   decrypts and verifies the notice through the signing/verification library
   (N6, billing-engine#291) and the per-distributor key broker (N7,
   billing-engine#292, infra half `mirrorstack-infra#433`).
5. The worker reads back the trade status from NewebPay's own query API before
   treating the notice as authoritative — a callback is evidence to confirm,
   never the sole trigger to settle on (same rule as every other provider
   adapter in this tree, [`DESIGN.md §5`](DESIGN.md#5--paying-and-what-happens-when-the-answer-never-comes)).

The ingress answers **200 only after the queue has accepted the message**. If
the send fails it answers **503**, so NewebPay retries.

## Retries

NewebPay retries a notice only when it does not receive an HTTP 200 back: the
manual describes 3 retries followed by an email to the merchant. The exact
retry timing is undocumented publicly; billing-engine#274 (the first sandbox
notice through this route) is what measures it empirically against this
deployment.

## What is still open

Whether NewebPay's MPG will accept a per-request `NotifyURL` on a host other
than the store's own registered domain is **unproven** — no public NewebPay
document says either way (checked 2026-10-28), and a 2026-09-12 comment on
billing-engine#96 claiming the opposite is not backed by any such document
either. billing-engine#274, group B, is the test that answers it: the first
real notice sent through this exact route.

**Fallback if the probe fails:** a stable MirrorStack-owned hostname in front
of the same API (rather than the raw `execute-api` address), registered as
the `NotifyURL` instead. This changes only what is registered with NewebPay,
not the path, the handler, or anything in this repo.

**The sandbox-only-route rule:** the live path, `/newebpay/notify`, stays
unrouted — answering the same 501 as every other unmatched request — until
core-v2#1849 (a live route and white-label notice hosts). Do not route it
early to unblock a sandbox test; add a distinct path instead, the way this one
was added.
