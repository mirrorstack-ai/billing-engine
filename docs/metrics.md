# Metrics Reference

Complete list of metrics collected for billing attribution.

## Data sources

```
pg_stat_statements       — per-role query stats (primary billing source)
pg_stat_user_tables      — per-table write/read stats
pg_catalog               — storage size functions
CloudWatch               — cluster-level AWS billing inputs
```

## pg_stat_statements (per-role)

Attribution key: `userid` → `pg_roles.rolname` → `mod_{moduleId}__app_{appPublicId}`

### Compute metrics

| Column | Type | Description |
|--------|------|-------------|
| `calls` | count | Number of query executions |
| `total_exec_time` | ms | Total execution time (excludes planning) |
| `total_plan_time` | ms | Total planning time |
| `mean_exec_time` | ms | Average execution time |
| `max_exec_time` | ms | Worst-case execution time |
| `rows` | count | Total rows returned or affected |

### I/O metrics

| Column | Type | Description | Cost mapping |
|--------|------|-------------|-------------|
| `shared_blks_read` | blocks (8KB) | Disk reads | × 2 = Aurora read I/O requests |
| `shared_blks_written` | blocks (8KB) | Disk writes | × 2 = Aurora write I/O requests |
| `shared_blks_hit` | blocks (8KB) | Cache hits (RAM) | Free — no I/O charge |
| `shared_blks_dirtied` | blocks (8KB) | Blocks modified in memory | Write amplification indicator |
| `shared_blk_read_time` | ms | Time waiting for disk reads | Requires `track_io_timing = on` |
| `shared_blk_write_time` | ms | Time waiting for disk writes | Requires `track_io_timing = on` |

### WAL metrics

| Column | Type | Description | Cost mapping |
|--------|------|-------------|-------------|
| `wal_records` | count | WAL records generated | Write volume |
| `wal_bytes` | bytes | Total WAL bytes | Storage write cost |
| `wal_fpi` | count | Full-page images in WAL | Write amplification |

### Temp I/O metrics

| Column | Type | Description |
|--------|------|-------------|
| `temp_blks_read` | blocks | Temp file reads (sorts spilling to disk) |
| `temp_blks_written` | blocks | Temp file writes |
| `local_blks_read` | blocks | Local buffer reads (temp tables) |
| `local_blks_written` | blocks | Local buffer writes |

## pg_stat_user_tables (per-table)

Per-table, attributed to tenant via schema name.

| Column | Type | Description |
|--------|------|-------------|
| `seq_scan` | count | Full table scans (expensive) |
| `seq_tup_read` | count | Rows read by full scans |
| `idx_scan` | count | Index scans (efficient) |
| `idx_tup_fetch` | count | Rows read by index scans |
| `n_tup_ins` | count | Rows inserted |
| `n_tup_upd` | count | Rows updated |
| `n_tup_del` | count | Rows deleted |
| `n_live_tup` | count | Estimated live rows |
| `n_dead_tup` | count | Dead rows (bloat) |

## pg_catalog storage functions

| Function | Returns | Description |
|----------|---------|-------------|
| `pg_total_relation_size(oid)` | bytes | Table + indexes + TOAST + FSM + VM |
| `pg_table_size(oid)` | bytes | Table + TOAST (no indexes) |
| `pg_indexes_size(oid)` | bytes | All indexes on a table |

### Per-schema storage query

```sql
SELECT
    schemaname,
    sum(pg_total_relation_size(relid)) AS total_bytes,
    sum(pg_table_size(relid)) AS table_bytes,
    sum(pg_indexes_size(relid)) AS index_bytes
FROM pg_stat_user_tables
WHERE schemaname LIKE 'app_%'
GROUP BY schemaname;
```

## CloudWatch metrics (cluster-wide)

These cannot be attributed per-tenant directly. Used as totals for proportional splitting.

| Metric | Unit | Description | Billing use |
|--------|------|-------------|------------|
| `ServerlessDatabaseCapacity` | ACU | Current ACU level | Actual ACU bill to split |
| `VolumeReadIOPs` | count | Billed read I/O | Calibrate pg_stat_statements |
| `VolumeWriteIOPs` | count | Billed write I/O | Calibrate pg_stat_statements |
| `VolumeBytesUsed` | bytes | Total Aurora storage | Verify per-schema totals |
| `DatabaseConnections` | count | Open connections | Monitor per-role |

## Collection schedule

| Metric source | Poll interval | Method |
|--------------|--------------|--------|
| `pg_stat_statements` | Every 60s | EventBridge → Lambda → snapshot deltas |
| `pg_stat_user_tables` | Every 5min | Same Lambda |
| `pg_total_relation_size` | Daily | Separate cron |
| CloudWatch | Every 60s | CloudWatch API (automatic) |

## Required Aurora parameter group settings

```
shared_preload_libraries = 'pg_stat_statements'
track_io_timing = on
pg_stat_statements.track = top
pg_stat_statements.track_planning = on
pg_stat_statements.max = 10000
```
