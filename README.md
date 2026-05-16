# billing-engine

Billing service for [MirrorStack](https://mirrorstack.ai). Owns Stripe credentials, the polymorphic billing-entity row that anchors user-tier and (future) org-tier customers, subscription state, invoice mirror, and (later) usage metering.

> **Status:** v1 bootstrap — schema-only. Matches the design in [`mirrorstack-docs/db/ms_billing/`](https://github.com/mirrorstack-ai/mirrorstack-docs/tree/main/db/ms_billing). Real subscription / portal / invoice handlers land in subsequent PRs.
>
> This repo is a **clean rewrite** of an earlier v0 attempt that targeted a different schema (`ms_billing_account`). The v0 work is preserved at [`mirrorstack-ai/billing-engine-old`](https://github.com/mirrorstack-ai/billing-engine-old) for reference; nothing from v0 is deployed.

## Layout

```
billing-engine/
├── cmd/
│   ├── account-api/        # Lambda: HTTP endpoints called by api-platform/account (internal-secret-gated)
│   └── account-webhook/    # Lambda: Stripe webhook receiver (signature-verified)
├── internal/               # (private; real handlers land per-PR)
├── migrations/
│   └── billing/            # ms_billing schema: 001_init creates `accounts`
└── scripts/                # init-db.sql for local dev
```

## Architecture

```
api-platform/account ─internal HTTP─► billing-engine/account-api ─► Stripe API
                                                 ▲
                                                 │ webhook (Stripe-Signature verified)
                                            Stripe → billing-engine/account-webhook ─► ms_billing.*
```

`api-platform` never touches Stripe. `billing-engine` is the only service with `STRIPE_SECRET_KEY` and `STRIPE_WEBHOOK_SECRET`.

## Schema (v1)

```
ms_billing
└── accounts                  # polymorphic: owner_kind ∈ {user, org}; lazy stripe_customer_id
```

Canonical reference: [`mirrorstack-docs/db/ms_billing/`](https://github.com/mirrorstack-ai/mirrorstack-docs/tree/main/db/ms_billing). If `tables.md` in mirrorstack-docs disagrees with `migrations/billing/`, the migration wins and the doc is the bug.

## Quickstart

```bash
make db         # boot Postgres 17 in Docker
make db-init    # apply migrations
make lint       # go vet
make build      # go build ./...
make test       # unit tests (no external deps)
```

## License

[FSL-1.1-ALv2](LICENSE) — converts to Apache 2.0 two years after release.
