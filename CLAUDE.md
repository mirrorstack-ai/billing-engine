# `billing-engine` — agent guide

The public-source billing authority for MirrorStack. Its runtime is private:
`cmd/account-api/main.go:647` registers one unauthenticated route, `/__health`,
and every other local route requires `X-MS-Internal-Secret`
(`internal/shared/auth/internal_secret.go:80`). Customers reach `api-platform`.

> **The target architecture is proposed, not deployed.** Read `README.md` and
> `docs/DESIGN.md` before changing money behavior. `main` still carries direct
> Stripe paths outside the intent/notice/authorization boundary. Never describe a
> target document as a current production guarantee.
>
> **Automatic merge and promotion are paused for this rebuild.** Work on a branch,
> preserve manual billing and security review, and do not enable collection
> merely because CI passes.
>
> Current defects are enumerated in exactly one place:
> [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps). Do not start
> a second gap list, here or anywhere else.
>
> **This repo is v1.** A v0 attempt with a different schema (`ms_billing_account`)
> lives at `mirrorstack-ai/billing-engine-old`. Do not import a v0 pattern
> without re-deriving it; the schema shape changed.

## Schema source of truth

`migrations/billing/` is authoritative. The `ms_billing` schema is documented under
`db/ms_billing/` in [`mirrorstack-ai/mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs).
If `tables.md` there disagrees with a migration, the migration wins and the doc is the bug.

## Layout

The command list is security-relevant, so it names every shipped binary.

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
├── internal/{account,billingperiod,shared}/   195 Go files
├── migrations/billing/     ms_billing schema; 001..052 up/down (040 unused)
├── docker-compose.yml      local Postgres 17 (postgres:17-alpine)
└── Makefile                db / db-init / db-reset / test / lint / build / dev-*
```

## Architecture (trust boundary)

Current topology. The broad-dispatch and callback-to-writer edges are release blockers,
not target guarantees; they are filed in [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps).

```
Customer/browser ─► api-platform account/applications API ─IAM lambda.Invoke─► account-api
Module runtime   ─► api-platform dispatch ─broad Lambda.Invoke─► full account-api dispatcher
Stripe webhook / EventBridge ─► callback routers ─► Stripe-writing auto-top-up + credit-purchase executors
billing-cycle / infra sync ─► billing coordinators ─► provider writers
```

Target topology.
```
Customer/browser ─► api-platform ─private account RPC─► billing-engine core
Module runtime   ─► api-platform meter-only ingress ─fact-only role─► billing-engine core
billing-engine isolated permit-gated executor ─► Stripe / future NewebPay
Provider callback ─► callback verifier ─observation only─► read-only reconciliation
```

**Hard rules:**
- `api-platform` must never touch Stripe. Every Stripe API call belongs in this repo (`internal/shared/stripe/client.go`).
- `billing-engine/account-api` must never be customer-reachable. Its only
  unauthenticated route is the static health response at `cmd/account-api/main.go:647`,
  which returns before RPC dispatch. Webhook ingress is a separate binary.
- `api-platform` initiates account RPCs. The engine must not call the browser or push
  into the customer-facing account API. Asynchronous state is pulled by authenticated
  read, or delivered by the notifier (`internal/account/standing/notifier.go:59`).
- Do not treat the metering seam as dedicated. `RecordUsage` is gated on
  `X-MS-Meter-Secret` locally (`cmd/account-api/main.go:776-777`), but the production
  dispatch role can still invoke the whole account-api Lambda.
- A mutation-capable provider secret belongs to exactly one scoped
  `ProviderCredentialEnclave` (unbuilt). Webhook ingress may hold only a public key,
  or a verification-only secret the provider itself restricts.
- One consumed permit may emit one outbound mutation request. Disable SDK and HTTP
  retries and redirects, and fence the transport send. Route an ambiguous outcome to
  same-provider read-only reconciliation, never to a retry.
- Provider integrations use small consumer-owned Go interfaces and composition. Do not
  pass one universal provider client through the service graph.
- Every table this repo owns lives in `ms_billing.*`. Never write outside it.
  `owner_user_id` and `owner_org_id` are soft FKs (`migrations/billing/001_init.up.sql:25-26`),
  and queries do not join across the boundary (`internal/account/db/queries/cycle.sql:375-376`).
- Infrastructure cost must not become a customer charge kind. It already is one,
  so do not widen this path — see [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps).
  - `internal/account/cycle/types.go:59` sets `infraMarkupNum = 12`, a ×1.2 markup applied once in SQL.
  - `internal/account/usage/bill.go:509-530` builds `infra_lines` and `module_infra_lines` for the customer bill.
  - `internal/account/usage/types.go:446-448`: displayed `UnitPriceMicros` is pre-markup COGS while `ChargedMicros` carries the ×1.2. Quantity × displayed unit price does not equal the charge.
- Do not call the deployment intent-only until `Capabilities` (unbuilt) reports
  `legacyMoneyPaths: 0`, every caller has migrated, and legacy provider credentials
  are revoked. The weakest reachable legacy path defines the actual guarantee.

## Target money boundary

The destination: every debit must consume one immutable charge intent plus live
authority, tax must fail closed, and an append-only ledger must outrank any
provider callback. Nothing on `main` meets that yet. Link, never restate.

- Invariants INV-001..INV-014 — [`docs/DESIGN.md#2-the-invariants`](docs/DESIGN.md#2-the-invariants)
- Intent lifecycle and the execution predicate — [`docs/DESIGN.md#executechargeintent`](docs/DESIGN.md#executechargeintent)
- Charge vocabulary, tax, ledger — DESIGN [§8](docs/DESIGN.md#8-what-customers-may-be-charged-for), [§9](docs/DESIGN.md#9-tax), [§10](docs/DESIGN.md#10-ledger-and-receipts)
- Receipt and deployed-source verification — [`docs/VERIFICATION.md`](docs/VERIFICATION.md)
- Adversaries, assumptions, limits — [`SECURITY.md#adversary-model`](SECURITY.md#adversary-model)

## Commit identity

Commit as **Sheng Kun Chang <nothingchang@mirrorstack.ai>**, or the locally-configured
`sheng-kun-chang@mirrorstack.ai`. Never as `mirrorstack-ops[bot]`. If the bot is
configured, override locally:

```bash
git config --local user.name "Sheng Kun Chang"
git config --local user.email "nothingchang@mirrorstack.ai"
```

## When you edit this repo
1. **Branch off `main`** — `git checkout -b <type>/<slug>`, where type is `feat`, `fix`, `chore`, `docs`, or `refactor`.
2. **Make the change.** If you touch the schema, coordinate a matching `mirrorstack-docs/db/ms_billing/` update in the same PR cycle.
3. **Commit prefix**: `feat:` / `fix:` / `chore:` / `docs:` / `refactor:`. Co-author tail: `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
4. **Open a PR against `main`.** Never push directly to `main`.
5. **`Closes #N`** in the PR body for auto-close when a tracking issue exists.

## Core manifest pointer automation

After a PR merges to protected `main`, a successful terminal `Publish` run triggers
`.github/workflows/notify-core-pointer.yml`. It sends the `billing-engine` main SHA to
`mirrorstack-ai/mirrorstack-core-v2`, where `mirrorstack-core-bot` opens or updates the
commit-bound pointer PR. It never reviews, merges, promotes, or deploys. Do not hand-edit
the core gitlink. If the PR is missing, inspect the terminal CI/CD run and the
`Notify core pointer` run; core's scheduled scan is the fallback.

## Cross-repo coordination

A schema change here typically spans two repos.

1. In `billing-engine/`: write the migration, open a PR.
2. In `mirrorstack-docs/`: update `db/ms_billing/{README,tables,migrations}.md` in the same cycle.
3. In `MirrorStack-AI-V2/` (parent): bump the submodule pointer once both child PRs merge.

## Don't put here
- Stripe API surface mocks or fixtures with real keys — keep `.env.local` for that.
- Frontend or `web-*` UI code — that lives in `web-account/` or `web-applications/`.
- Schema docs for currently-shipped state — those graduate to `mirrorstack-docs/`.

## Quickstart
```bash
make db         # boot Postgres 17
make db-init    # apply migrations
make test       # unit tests
make lint       # go vet
make build      # go build ./...
```

Local Lambda dev. Both binaries detect Lambda through `AWS_LAMBDA_FUNCTION_NAME`
and fall back to local HTTP.

- `cd cmd/account-api && go run .` — account-api on `:8091`, overridable via `ACCOUNT_API_PORT` (`cmd/account-api/main.go:860`).
- `make dev-webhook` — account-webhook on `:8092`, overridable via `ACCOUNT_WEBHOOK_PORT` (`cmd/account-webhook/main.go:51`). Pair it with `stripe listen --forward-to localhost:8092/webhook` for real test-mode events.

Two secrets, not one. Local control-plane RPC uses `X-MS-Internal-Secret`; `RecordUsage`
uses the separate `X-MS-Meter-Secret` (`internal/shared/auth/internal_secret.go:80-81`).
An empty configured secret returns 503 rather than opening the route.

