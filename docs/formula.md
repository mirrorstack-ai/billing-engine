# Billing Formula

## Overview

```
Total cost = (compute + storage + io) × 1.2
```

The 1.2× multiplier covers platform overhead:
- Monitoring and alerting
- Automated backups
- Security (RLS, role isolation, credential rotation)
- Support infrastructure
- Platform development

## Compute attribution

Aurora Serverless v2 scales ACUs based on total load. Individual tenants don't consume specific ACUs — they share. Attribution is proportional to query execution time.

```
tenant_compute_cost =
    (tenant_total_exec_time + tenant_total_plan_time)
    / (sum_all_tenants_total_exec_time + sum_all_tenants_total_plan_time)
    × actual_acu_bill_for_period
    × 1.2
```

Source: `pg_stat_statements` grouped by `userid`

### Why execution time, not query count?

```
Module A: 1000 queries × 1ms each  = 1000ms total
Module B: 10 queries   × 500ms each = 5000ms total
```

Module B drives 5× more ACU scaling than A, despite 100× fewer queries. Execution time is the fair metric.

### ACU bill source

```
CloudWatch metric: ServerlessDatabaseCapacity
  → sampled every 60s
  → ACU × duration × $0.20/ACU-hour
  → this is the total bill to split proportionally
```

## Storage attribution

Direct measurement per schema.

```
tenant_storage_cost =
    pg_total_relation_size(all tables in schema)
    × $0.12/GB-month
    × 1.2
```

`pg_total_relation_size` includes:
- Heap (table data)
- All indexes
- TOAST (large values)
- Free-space map
- Visibility map

Measured daily. Billed as monthly average.

## I/O attribution

Each PostgreSQL block = 8KB. Aurora bills per 4KB page. Conversion: blocks × 2.

```
tenant_read_io_cost =
    tenant_shared_blks_read × 2
    × $0.24/1M
    × 1.2

tenant_write_io_cost =
    tenant_shared_blks_written × 2
    × $0.24/1M
    × 1.2

tenant_io_cost = tenant_read_io_cost + tenant_write_io_cost
```

Source: `pg_stat_statements.shared_blks_read` and `shared_blks_written` grouped by `userid`

### Calibration

CloudWatch provides actual billed I/O:
- `VolumeReadIOPs` — total read I/O
- `VolumeWriteIOPs` — total write I/O

If `sum(all_tenants_blks_read × 2)` ≠ `VolumeReadIOPs`, apply a correction factor:

```
correction = VolumeReadIOPs / sum(all_tenants_blks_read × 2)
tenant_actual_read_io = tenant_blks_read × 2 × correction
```

This accounts for background I/O (autovacuum, WAL, etc.) that pg_stat_statements doesn't capture.

## Developer-facing pricing (simplified)

Internally we calculate compute + storage + I/O precisely. Externally, developers see a simpler model:

```
Rows read:    $0.60 per 1M rows  (covers compute + read I/O)
Rows written: $1.20 per 1M rows  (covers compute + write I/O + WAL)
Storage:      $0.144 per GB-month (covers storage × 1.2)
```

Row-based pricing is derived from the average cost-per-row across all tenants, updated monthly. This gives developers a predictable, understandable bill.

## Free tier

Per (module, app) pair:
- 1M rows read/month
- 100K rows written/month
- 500MB storage

Below free tier = $0. Above = metered at the rates above.

## Example bill

Module "media" installed on app "abc123", April 2026:

| Metric | Amount | Cost |
|--------|--------|------|
| Rows read | 5.2M | (5.2 - 1.0 free) × $0.60 = $2.52 |
| Rows written | 120K | Free (under 100K threshold... 120K - 100K) × $1.20 = $0.024 |
| Storage | 2.1 GB | (2.1 - 0.5 free) × $0.144 = $0.23 |
| **Total** | | **$2.77** |

Internal cost (compute + I/O proportional):
- Compute share: $1.80
- I/O: $0.45
- Storage: $0.19
- Subtotal: $2.44
- × 1.2 = $2.93

Difference between internal cost ($2.93) and simplified bill ($2.77) is within margin. Row-based pricing is recalibrated monthly to stay within ±10% of actual costs.
