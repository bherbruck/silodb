# silodb-bench

`cargo run -p silodb-bench --release -- [out_dir] [rows]`

Three contenders over the same deterministic synthetic time series (1
row/second, 16 sensors, hourly buckets):

- **silodb** — single-name view, all history compacted to parquet
- **plain SQLite** — everything in one hot table with a ts index (the
  "never compact" baseline)
- **DuckDB CLI** — `read_parquet()` over the very same files silodb wrote

## Results (2M rows / ~23 days, 556 hourly files, WSL2, 2026-07-13)

```
hot insert:  5.3s (380k rows/s; ts index maintained)
compaction: 10.9s (183k rows/s) into 556 files
```

| on-disk | size |
|---|---|
| silodb (hot.db + parquet) | **56.4 MB** |
| plain sqlite (+ts index) | 98.6 MB |

| query, median ms (min) | silodb view | plain sqlite | duckdb same parquet |
|---|---|---|---|
| 1h range agg (~0.2%) | 1.3 (0.9) | 0.3 (0.2) | 149 (108) |
| 24h range agg (~4%) | 17.8 (13.8) | 6.8 (5.9) | 151 (104) |
| full-history agg (100%) | 450.9 (406.4) | 167.4 (162.8) | 146 (97) |
| 24h + name filter | 7.9 (7.4) | 6.4 (5.5) | 154 (110) |
| 1h raw rows | 1.4 (1.0) | 0.2 (0.2) | 148 (102) |

## Reading the numbers

- **The target workload (selective ranges) stays in SQLite's league**:
  1.3 ms for "one hour out of 23 days" — file-level pruning touched 2 of
  556 files, 1 of 2 row groups (`ScanStats`). The plain table is ~4x
  faster in absolute terms but pays 75% more disk and its index/table keep
  growing forever; silodb's hot tier stays flat.
- **Full scans are the vtab's worst case** (~2.7x slower than plain
  SQLite): rows cross the vtab boundary one cell at a time. DuckDB wins
  full scans — that's columnar execution, and exactly why the spec calls
  heavy rollups a cloud-side job.
- **DuckDB's ~150 ms floor on everything** is per-query
  `read_parquet` planning + 556 footer parses; it has no equivalent of
  silodb's catalog range pruning or footer cache across one-shot queries.
  A long-lived DuckDB process with its own table copy would do better —
  but then it's a second storage engine, which is the thing this project
  exists to avoid.
- **Compression**: same data, 56 MB parquet vs 99 MB SQLite. Dictionary +
  RLE on repetitive sensor data is the whole story.

## History

The first 2M-row run exposed a real bug: compaction throughput collapsed
10x (152k -> 13k rows/s) because `init_table` didn't index the hot
table's bucket axis, making each `compact_bucket` scan the whole
remaining backlog — quadratic. Fixed (index created by `init_table`);
throughput is now flat with scale. The flat-cost regression test missed
it because it drains buckets as they close (hot table stays small);
the benchmark compacts a 2M-row backlog.
