# `billing-engine` — agent guide

The public-source billing authority for the MirrorStack platform. Its runtime is
private: customers reach `api-platform`, which invokes this engine through an
IAM-gated production RPC (or secret-gated local HTTP). It owns metering,
pricing, customer billing authority, payment-provider execution, reconciliation,
and financial receipts.

> **Target architecture is proposed, not deployed.** Read `README.md` and
> `docs/DESIGN.md` before changing money behavior. Current `main` contains direct
> Stripe paths that do not yet meet the intent/notice/authorization boundary.
> Never describe the target documents as current production guarantees.
>
> **Automatic merge and promotion are paused for this rebuild.** Work on a
> branch, preserve manual billing/security review, and do not enable collection
> merely because CI passes.

> **This repo is v1.** A v0 attempt with a different schema (`ms_billing_account`) lives at `mirrorstack-ai/billing-engine-old` for reference. **Do not import patterns from v0 without re-deriving** — the schema shape changed and the design decisions are different.

## Schema source of truth

The `ms_billing` schema is documented canonically in [`mirrorstack-ai/mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs) under `db/ms_billing/`. The migration files in `migrations/billing/` are the authoritative source; if `mirrorstack-docs/db/ms_billing/tables.md` disagrees, the migration wins and the doc is the bug.

The design discussion that produced this schema lives in the parent workspace at `docs-temp/multi-tenancy/02-billing-schema.md` (forward design, not committed to any single repo).

## Layout

Abbreviated current layout; the command list is security-relevant and deliberately
shows every shipped binary family rather than implying there are only two:

```
billing-engine/
├── cmd/
│   ├── account-api/        Lambda: IAM-gated internal RPC; local HTTP for dev
│   ├── account-webhook/    Lambda: Stripe HTTPS webhook receiver
│   ├── account-webhook-eventbridge/  Stripe partner EventBridge consumer
│   ├── billing-cycle/      scheduled billing-cycle worker
│   ├── infra-egress-sync/  infrastructure egress synchronizer
│   ├── infra-ssr-compute-sync/  infrastructure compute synchronizer
│   └── pm-default-backfill/  payment-method backfill worker
├── internal/               private packages — added per-PR as handlers ship
├── migrations/billing/     ms_billing schema (001_init.up.sql for v1)
├── scripts/                init-db.sql + future helper scripts
├── docker-compose.yml      local Postgres 17
└── Makefile                make db / db-init / test / lint / build
```

## Architecture (trust boundary)

Current topology includes known release blockers:

```
Customer/browser ─► api-platform account/applications API ─IAM lambda.Invoke─► billing-engine/account-api
Module runtime ─► api-platform dispatch ─broad Lambda.Invoke─► full account-api dispatcher
Stripe webhook/EventBridge ─► current callback routers ─► Stripe-writing auto-top-up/credit-purchase executors
billing-cycle / infrastructure sync paths ─► current billing coordinators and provider writers

These callback-to-writer and broad-dispatch edges are not target guarantees.
They must be removed before intent-only readiness.
```

Target topology:

```
Customer/browser ─► api-platform ─private account RPC─► billing-engine core
Module runtime ─► api-platform meter-only ingress ─fact-only role─► billing-engine core
billing-engine isolated permit-gated executor ─► Stripe / future NewebPay
Provider callback ─► bounded callback verifier/ingress ─observation only─► core/read-only reconciliation

Local control-plane RPC uses X-MS-Internal-Secret; RecordUsage uses
X-MS-Meter-Secret.
```

**Hard rules:**

- `api-platform` **never** touches Stripe. All Stripe API calls happen here.
- `billing-engine/account-api` is **never customer-reachable**. Its only public
  HTTP route is a static health response that returns before RPC dispatch;
  provider webhook ingress is a separate binary and capability.
- `api-platform` initiates account RPCs. Asynchronous engine state is pulled
  through authenticated reads or delivered by the notifier; the engine does not
  call the browser or push into the customer-facing account API.
- Do not call the production metering seam dedicated until IAM/resource and
  action dispatch enforce a meter-only capability. The current dispatch role can
  invoke the full account-api Lambda and is a known release blocker.
- A mutation-capable provider secret belongs to exactly one scoped
  `ProviderCredentialEnclave`. Public webhook ingress may hold only a public key
  or provider-enforced verification-only secret; a callback secret that also has
  mutation scope stays inside the enclave's fixed verifier.
- `billing-engine` reads narrow columns from `ms_account.users` (and future `ms_account.orgs`) via soft FK; it never writes outside `ms_billing.*`.

## Target money boundary

The accepted destination is documented publicly in:

- `docs/DESIGN.md` — intent lifecycle and capability separation;
- `docs/CHARGES.md` — exhaustive monetary-effect vocabulary;
- `docs/LEDGER-AND-RECEIPTS.md` — append-only truth and provider trace;
- `docs/TAX.md` — fail-closed versioned tax boundary;
- `docs/THREAT-MODEL.md` — adversaries, assumptions, and limits; and
- `docs/VERIFICATION.md` — deployed-source and receipt verification.

Build toward these invariants:

- The private caller cannot make money authoritative. Usage ingress reports only
  constrained facts. A product route may relay one closed non-authoritative
  engine catalog/template selection; the core validates it, derives every
  financial field, and requires independent exact customer proof.
- Every debit consumes one immutable `ChargeIntent` and live bounded authority,
  plus either fresh exact customer-present acceptance or, for automatic/standing
  collection, terminal destination-delivery evidence and the public wait.
- `api-platform` may relay a signed disclosure but is not the acceptance or
  revocation path. A separate proof-only consent edge appends customer-signed
  envelopes to an inbox consumed by the core. The engine verifies a
  customer-controlled proof over independently rendered canonical fields;
  otherwise automatic execution stays disabled.
- Unknown pre-effect price, tax, notice, authorization, capability, or build
  identity prevents dispatch. An ambiguous provider outcome after dispatch may
  already represent moved cash; it permits no further mutation or ledger-success
  assertion and retains the claim until authoritative same-operation resolution.
- Each actual provider-enforced mutation-credential scope has one exclusive
  attested `ProviderCredentialEnclave` owner; provider/environment/merchant/
  capability scope is preferred, while any broader blast radius is disclosed.
  Isolated instances prevent a global multi-rail secret vault. Its purpose-matched
  writers require exported opaque permit structs with unexported fields/
  constructors and mandatory durable journal authentication. If a
  provider lacks native read-only credentials, its fixed-read broker stays inside
  this enclave and external reconciliation remains credential-free.
- One consumed permit may emit one outbound mutation request. Disable SDK/HTTP
  retries and redirects, fence the actual transport send, and route ambiguity to
  read-only same-operation reconciliation.
- Provider integrations use small consumer-owned Go interfaces and composition.
  Do not pass one universal provider client through the service graph.
- Stripe is an adapter, not the domain model. NewebPay/Taiwan is the next planned
  adapter; provider-specific objects live in `PaymentAttempt`/evidence only.
- A read-only provider port can trace intent → attempt → provider objects → cash
  movement/payout/refund/dispute without moving money.
- One intent settles at most once across every provider and retry.
- Provider invoices/callbacks are evidence; the append-only ledger is monetary
  truth.
- Infrastructure cost is internal and is never a customer charge kind.
- Tax unknown is distinct from final zero and cannot execute.
- Corrections, refunds, and late usage append linked records; they never rewrite
  settled history.

The weakest reachable legacy path defines the actual guarantee. Do not call the
deployment intent-only until `Capabilities` reports `legacyMoneyPaths: 0`, every
caller has migrated, and legacy provider credentials are revoked.

## Commit identity

Commit as **Sheng Kun Chang <nothingchang@mirrorstack.ai>** (or the locally-configured `sheng-kun-chang@mirrorstack.ai`, whichever the local git config holds). Never as `mirrorstack-ops[bot]`. If you find the bot configured, override locally:

```bash
git config --local user.name "Sheng Kun Chang"
git config --local user.email "nothingchang@mirrorstack.ai"
```

## When you edit this repo

1. **Branch off `main`** — `git checkout -b <type>/<slug>` where type is `feat`, `fix`, `chore`, `docs`, `refactor`.
2. **Make the change.** If you touch the schema, coordinate with a matching `mirrorstack-docs/db/ms_billing/` update in the same PR cycle.
3. **Commit prefix**: `feat:` / `fix:` / `chore:` / `docs:` / `refactor:`. Co-author tail: `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
4. **Open a PR against `main`**. Never push directly to `main`.
5. **`Closes #N`** in the PR body for auto-close when a tracking issue exists.

## Core manifest pointer automation

After a PR merges to protected `main`, a successful terminal `Publish` run triggers
`.github/workflows/notify-core-pointer.yml`. The workflow sends the exact
`billing-engine` main SHA to `mirrorstack-ai/mirrorstack-core-v2`, where
`mirrorstack-core-bot` opens or updates the commit-bound pointer PR.

Claude must not manually edit or push the core gitlink during the normal flow.
The automation only opens/updates a reviewable PR; it never reviews, merges,
promotes, or deploys. If the PR is missing, inspect the terminal CI/CD run and
`Notify core pointer` run first; core's scheduled scan remains the fallback.

## Cross-repo coordination

A schema change here typically spans two repos:

1. In `billing-engine/`: write the migration, open a PR.
2. In `mirrorstack-docs/`: update `db/ms_billing/{README,tables,migrations}.md` in the same cycle.
3. In `MirrorStack-AI-V2/` (parent): bump the submodule pointer once both child PRs merge.

## Don't put here

- Stripe API surface mocks or test fixtures with real keys — keep `.env.local` for that.
- Frontend / web-* UI code — lives in `web-account/` or `web-applications/`.
- Schema docs for currently-shipped state — those graduate to `mirrorstack-docs/`.

## Quickstart

```bash
make db         # boot Postgres 17
make db-init    # apply migrations
make test       # unit tests
make lint       # go vet
make build      # go build ./...
```

Local Lambda dev:

- `cd cmd/account-api && go run .` — account-api on `:8091`.
- `make dev-webhook` (or `cd cmd/account-webhook && go run .`) — account-webhook on `:8092`. Pair with `stripe listen --forward-to localhost:8092/webhook` to receive real test-mode events. Override the port via `ACCOUNT_WEBHOOK_PORT`. Both binaries auto-detect Lambda (`AWS_LAMBDA_FUNCTION_NAME`) and fall back to local HTTP otherwise — same code, two transports.
