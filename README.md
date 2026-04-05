# @mirrorstack-ai/billing-engine

Open-source billing calculation for [MirrorStack](https://mirrorstack.ai) modules.

Transparent pricing — module developers can verify every line of their bill.

## How billing works

MirrorStack charges module usage based on three components:

```
Total cost = (compute + storage + io) × 1.2
```

The **1.2× multiplier** covers platform overhead (monitoring, backups, security, support).

### Compute (ACU)

```
tenant_compute = (tenant_exec_time / total_exec_time) × actual_acu_bill × 1.2
```

Proportional to query execution time. If your module uses 10% of total DB compute time, you pay 10% of the ACU bill.

### Storage

```
tenant_storage = schema_size_gb × $0.12/GB-month × 1.2
```

Measured per schema via `pg_total_relation_size`. Includes tables, indexes, and TOAST data.

### I/O

```
tenant_io = ((blks_read + blks_written) × 2) × $0.24/1M × 1.2
```

Each PostgreSQL block = 8KB. Aurora bills per 4KB page, so blocks × 2 = Aurora I/O requests.

## Pricing table (ap-northeast-1 Tokyo)

### Raw AWS costs

| Component | AWS price | Unit |
|-----------|----------|------|
| ACU-hours | $0.20 | per ACU-hour |
| Storage | $0.12 | per GB-month |
| I/O requests | $0.24 | per 1M requests |

### MirrorStack pricing (× 1.2)

| Component | Price | Unit |
|-----------|-------|------|
| Compute | Proportional to exec_time × 1.2 | of actual ACU bill |
| Storage | $0.144 | per GB-month |
| I/O | $0.288 | per 1M requests |

### Developer-facing pricing (simplified)

| Tier | Rows read | Storage | Price |
|------|-----------|---------|-------|
| Free | 1M/month | 500MB | $0 |
| Pro | 10M/month | 5GB | $10/month |
| Overage | +$0.60/1M rows | +$0.30/GB | metered |

Row-based pricing is a simplification of the formula above. Internally, compute + I/O are calculated precisely; externally, developers see rows + storage.

## Metrics collected

All metrics come from PostgreSQL — no custom instrumentation in the SDK.

### Per-role (per module+app) from pg_stat_statements

| Metric | Column | What | Billing use |
|--------|--------|------|------------|
| Query count | `calls` | Times executed | Volume indicator |
| Execution time | `total_exec_time` | Wall-clock query time (ms) | **Compute attribution** |
| Planning time | `total_plan_time` | Query planning time (ms) | Compute attribution |
| Rows | `rows` | Rows read/written | Developer-facing metric |
| Disk reads | `shared_blks_read` | 8KB blocks from disk | **Read I/O cost** |
| Disk writes | `shared_blks_written` | 8KB blocks to disk | **Write I/O cost** |
| Cache hits | `shared_blks_hit` | 8KB blocks from RAM | Memory pressure |
| WAL bytes | `wal_bytes` | WAL volume generated | Write amplification |
| I/O read time | `shared_blk_read_time` | Disk read wait (ms) | I/O attribution |
| I/O write time | `shared_blk_write_time` | Disk write wait (ms) | I/O attribution |

Attribution key: `pg_stat_statements.userid` → `pg_roles.rolname` → `mod_{moduleId}__app_{appId}`

### Per-schema from pg_catalog

| Metric | Source | What | Billing use |
|--------|--------|------|------------|
| Total size | `pg_total_relation_size` | Table + indexes + TOAST | **Storage cost** |
| Table size | `pg_table_size` | Table + TOAST only | Base storage |
| Index size | `pg_indexes_size` | All indexes | Index overhead |
| Dead tuples | `pg_stat_user_tables.n_dead_tup` | Bloat | Wasted storage |
| Row counts | `n_tup_ins/upd/del` | Write volume per table | Write tracking |

### Cluster-wide from CloudWatch

| Metric | Source | What | Billing use |
|--------|--------|------|------------|
| ACU capacity | `ServerlessDatabaseCapacity` | Current ACU level | **Actual ACU bill** to split proportionally |
| Volume I/O | `VolumeReadIOPs/WriteIOPs` | Billed I/O count | Calibrate pg_stat_statements estimates |
| Volume size | `VolumeBytesUsed` | Total Aurora storage | Verify per-schema totals |
| Connections | `DatabaseConnections` | Open connections | Monitor per-role |

## Collection architecture

```
Every 60s:
  EventBridge → Lambda → polls pg_stat_statements
    → computes deltas from last snapshot
    → writes to _billing.metrics table

Daily:
  EventBridge → Lambda → aggregates daily totals
    → writes to _billing.daily(role, date, metrics)
    → calculates cost per (module, app) pair

Monthly:
  billing service → sums daily → applies tiers → generates invoice
```

## Required PostgreSQL configuration

```sql
-- Aurora parameter group
shared_preload_libraries = 'pg_stat_statements'
track_io_timing = on
pg_stat_statements.track = top
pg_stat_statements.track_planning = on
pg_stat_statements.max = 10000
```

## Project structure

```
billing-engine/
  collector/       — polls pg_stat_statements, stores deltas
  calculator/      — applies pricing formula to raw metrics
  estimator/       — CLI tool for cost prediction
  pricing/         — pricing tables per region
  docs/            — detailed documentation
```

> **Status:** Under development. Documentation first, implementation follows.

## License

Apache 2.0
