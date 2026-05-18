# `billing-engine` — agent guide

The billing service for the MirrorStack platform. Owns Stripe credentials, the polymorphic billing-entity row, subscription state, invoice mirror, and (later) usage metering.

> **This repo is v1.** A v0 attempt with a different schema (`ms_billing_account`) lives at `mirrorstack-ai/billing-engine-old` for reference. **Do not import patterns from v0 without re-deriving** — the schema shape changed and the design decisions are different.

## Schema source of truth

The `ms_billing` schema is documented canonically in [`mirrorstack-ai/mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs) under `db/ms_billing/`. The migration files in `migrations/billing/` are the authoritative source; if `mirrorstack-docs/db/ms_billing/tables.md` disagrees, the migration wins and the doc is the bug.

The design discussion that produced this schema lives in the parent workspace at `docs-temp/multi-tenancy/02-billing-schema.md` (forward design, not committed to any single repo).

## Layout

```
billing-engine/
├── cmd/
│   ├── account-api/        Lambda: HTTP endpoints for api-platform
│   └── account-webhook/    Lambda: Stripe webhook receiver
├── internal/               private packages — added per-PR as handlers ship
├── migrations/billing/     ms_billing schema (001_init.up.sql for v1)
├── scripts/                init-db.sql + future helper scripts
├── docker-compose.yml      local Postgres 17
└── Makefile                make db / db-init / test / lint / build
```

## Architecture (trust boundary)

```
api-platform/account ─internal HTTP (X-MS-Internal-Secret)─► billing-engine/account-api ─► Stripe API
                                                                     ▲
                                                                     │ webhook (Stripe-Signature verified)
                                                              Stripe → billing-engine/account-webhook ─► ms_billing.*
```

**Hard rules:**

- `api-platform` **never** touches Stripe. All Stripe API calls happen here.
- `billing-engine` is the **only** service with `STRIPE_SECRET_KEY` and `STRIPE_WEBHOOK_SECRET`.
- `billing-engine` reads narrow columns from `ms_account.users` (and future `ms_account.orgs`) via soft FK; it never writes outside `ms_billing.*`.

## Commit identity

Commit as **Sheng Kun Chang <nothingchang@mirrorstack.ai>** (or the locally-configured `sheng-kun-chang@mirrorstack.ai`, whichever the local git config holds). Never as `mirrorstack-ops[bot]`. If you find the bot configured, override locally:

```bash
git config --local user.name "Sheng Kun Chang"
git config --local user.email "nothingchang@mirrorstack.ai"
```

## When you edit this repo

1. **Branch off `main`** — `git checkout -b <type>/<slug>` where type is `feat`, `fix`, `chore`, `docs`, `refactor`.
2. **Make the change.** If you touch the schema, coordinate with a matching `mirrorstack-docs/db/ms_billing/` update in the same PR cycle.
3. **Commit prefix**: `feat:` / `fix:` / `chore:` / `docs:` / `refactor:`. Co-author tail: `Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>`.
4. **Open a PR against `main`**. Never push directly to `main`.
5. **`Closes #N`** in the PR body for auto-close when a tracking issue exists.

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
