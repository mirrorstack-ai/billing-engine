# @mirrorstack-ai/billing-engine

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Open-source billing and usage metering for [MirrorStack](https://mirrorstack.ai).

Transparent pricing — developers can verify every line of their bill.

> **Status:** Early stage — documentation and design only. No implementation yet.
>
> Currently scoped to **App Module** billing (database, storage, compute).
> Future: platform API usage, AI agent credits, bandwidth, and other billable resources.

## Scope

### Now: App Module billing

```
Total cost = (compute + storage + io) × 1.2
```

Tracks database usage per (module, app) pair via PostgreSQL roles and `pg_stat_statements`.

### Future: other billable resources

| Resource | Status | Description |
|----------|--------|-------------|
| App Module (DB, compute, I/O) | Designing | Per-module database cost attribution |
| Storage (S3 + R2) | Planned | File storage and CDN bandwidth |
| AI Agent credits | Planned | MCP tool invocations, token usage |
| Platform API | Planned | API call metering |
| Bandwidth | Planned | Data transfer out |
| Background jobs (ECS/SQS) | Planned | Video transcoding, heavy compute |

The billing engine is designed to be **extensible** — each resource type implements a collector + calculator interface. App Module billing is the first implementation.

## Pricing formula (App Modules)

All costs include a **1.2× platform multiplier** covering monitoring, backups, security, and support.

### Compute (ACU)

```
tenant_compute = (tenant_exec_time / total_exec_time) × actual_acu_bill × 1.2
```

Proportional to query execution time. Source: `pg_stat_statements` per role.

### Storage

```
tenant_storage = schema_size_gb × $0.12/GB-month × 1.2
```

Measured per schema via `pg_total_relation_size`.

### I/O

```
tenant_io = ((blks_read + blks_written) × 2) × $0.24/1M × 1.2
```

PostgreSQL block = 8KB, Aurora bills per 4KB page, so blocks × 2.

## Raw AWS costs vs MirrorStack pricing (ap-northeast-1 Tokyo)

| Component | AWS price | × 1.2 | Unit |
|-----------|----------|-------|------|
| ACU-hours | $0.20 | $0.24 | per ACU-hour |
| Storage | $0.12 | $0.144 | per GB-month |
| I/O requests | $0.24 | $0.288 | per 1M requests |

## Developer-facing pricing (simplified)

Internally we calculate compute + storage + I/O precisely. Developers see a simpler model:

| Tier | Rows read | Storage | Price |
|------|-----------|---------|-------|
| Free | 1M/month | 500MB | $0 |
| Pro | 10M/month | 5GB | $10/month |
| Overage | +$0.60/1M rows | +$0.30/GB | metered |

Row-based pricing is a simplification — recalibrated monthly to stay within ±10% of actual costs.

## Collection architecture

```
Every 60s:
  EventBridge → Lambda → polls pg_stat_statements
    → computes deltas from last snapshot
    → writes to _billing.metrics table

Daily:
  EventBridge → Lambda → aggregates daily totals
    → calculates cost per (module, app) pair

Monthly:
  billing service → sums daily → applies tiers → generates invoice
```

## Project structure (planned)

```
billing-engine/
  collector/             — metric collection interface
    pgstat/              — pg_stat_statements poller (App Modules)
    storage/             — S3/R2 usage collector (future)
    agent/               — AI agent credit tracker (future)
  calculator/            — pricing formula per resource type
    module/              — App Module cost calculation
    storage/             — S3/R2 cost calculation (future)
    agent/               — AI agent credit calculation (future)
  estimator/             — CLI tool for cost prediction
  pricing/               — pricing tables per region
  docs/
    formula.md           — detailed billing formula with examples
    metrics.md           — complete metrics reference
```

## Extensibility

Each billable resource implements two interfaces:

```go
// Collector gathers raw usage metrics
type Collector interface {
    Snapshot(ctx context.Context) ([]Metric, error)
    Delta(ctx context.Context, prev, curr []Metric) []Delta
}

// Calculator turns raw metrics into costs
type Calculator interface {
    Calculate(deltas []Delta, pricing PricingTable) []Cost
    Estimate(params EstimateParams) Cost
}
```

Adding a new billable resource = implement Collector + Calculator, register in the billing pipeline.

## Required PostgreSQL configuration

```sql
shared_preload_libraries = 'pg_stat_statements'
track_io_timing = on
pg_stat_statements.track = top
pg_stat_statements.track_planning = on
pg_stat_statements.max = 10000
```

## Documentation

- [Billing formula (detailed)](docs/formula.md) — compute attribution, storage, I/O, calibration, example bill
- [Metrics reference](docs/metrics.md) — every metric from pg_stat_statements, pg_catalog, CloudWatch

## License

Apache 2.0
