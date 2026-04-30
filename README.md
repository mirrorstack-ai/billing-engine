# billing-engine

Billing service for [MirrorStack](https://mirrorstack.ai). Owns Stripe credentials, subscription state, invoice mirror, and (later) usage metering.

> **Status:** Phase 1 — SaaS personal MVP. Schema, Stripe integration, and webhook handlers are landing across `#1`–`#8`.

## Layout

```
billing-engine/
├── cmd/
│   ├── account-api/        # Lambda: HTTP endpoints called by api-platform/account
│   └── account-webhook/    # Lambda: Stripe webhook receiver
├── internal/
│   ├── account/            # Subscription, invoice, Stripe sync
│   ├── applications/       # Meter ingest + usage ledger (Phase 3+)
│   └── shared/             # Stripe client, models, errors
├── migrations/
│   ├── account/            # billing_accounts, subscriptions, items, invoices
│   └── applications/       # lambda_attribution, usage_events (Phase 3+)
└── docs/                   # Design notes
```

## Architecture

```
modules ─Record()─► api-applications ─pull at cycle close─► billing-engine ─► Stripe
                                                                ▲
                                                                │ subscribe / portal / invoices
                                                            api-account
```

api-platform never touches Stripe. billing-engine is the only service with `STRIPE_SECRET_KEY`.

## Quickstart

```bash
make db          # boot Postgres in Docker
make test        # unit tests
make lint        # go vet
```

## Documentation

- [Phase 1 design (Stripe SaaS)](docs/early-design.md) — _earlier draft, kept for reference_
- [Billing formula (App Module metering, Phase 4+)](docs/formula.md)
- [Metrics reference](docs/metrics.md)

## License

[FSL-1.1-ALv2](LICENSE) — converts to Apache 2.0 two years after release.
