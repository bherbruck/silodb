# silodb-bench

`cargo run -p silodb-bench --release -- [out_dir] [days]`

The generated dataset is cached under `target/bench-cache/v<N>-<days>d/`
(the expensive part: two 52M-row inserts + 365 compactions at year scale).
First run builds it once; repeat runs stage the mutable silodb side from
the cache in seconds and query the immutable 3 GB plain.db in place —
a year-scale re-run is minutes of queries, not dataset construction.
`SILODB_BENCH_REBUILD=1` forces a rebuild; bump `DATASET_VERSION` in
main.rs when the generator or schema changes. (Detail: staging re-points
`_silodb_catalog` paths at the copy — catalog paths are verbatim, and
tiered GC would otherwise delete the cache's files.)

Three contenders over the same deterministic synthetic telemetry — **one
year at 1-minute interval from 10 devices × 10 sensors** (100 series,
52.56M rows), daily buckets (365 parquet files, 144k rows each):

- **silodb** — single-name view, all history compacted to parquet
- **plain SQLite** — everything in one hot table with a ts index (the
  "never compact" baseline)
- **DuckDB CLI** — `read_parquet()` over the very same files silodb wrote

## Results (52.56M rows, WSL2, 2026-07-13)

```
silodb ingest:  131s (401k rows/s — through the view trigger, ts index maintained)
compaction:     107s (493k rows/s) into 365 daily files
plain ingest:    63s + 30s ts index build
```

| on-disk | size |
|---|---|
| silodb (hot.db + parquet) | **533 MB** |
| plain sqlite (+ts index) | 3,033 MB |

**5.7× smaller** for identical data — dictionary/RLE on device/sensor
strings plus double-encoding do the work.

| query, median ms (min) | silodb view | plain sqlite | duckdb same parquet |
|---|---|---|---|
| 1h, one series (~0.0001%) | **1.5** (1.3) | 0.3 (0.3) | 306 (262) |
| 1 day, all series (~0.27%) | 67.6 (25.1) | 14.2 (11.0) | 246 (197) |
| 1 week, one series (~0.02%) | **170.9** (153.3) | 204.5 (114.9) | 312 (233) |
| 1 week, all series (~2%) | 283.1 (165.1) | 118.7 (70.8) | 247 (241) |
| full year agg (100%) | 5,164 (4,742) | 2,879 (2,477) | **443** (370) |

Pruning observed per query (`ScanStats`): the 1h query touched 1 of 365
files and 1 of 9 row groups within it; the week queries 7 files; only the
full-year scan opened everything (3,285 row groups).

## Reading the numbers

- **The device's actual workload — "show me this sensor's recent window
  out of a year of history" — runs at 1.5 ms** against 533 MB of cold
  storage, and silodb even *beats* the fully-hot indexed table on the
  week/one-series shape (170 ms vs 204 ms): the ts index alone can't help
  SQLite with the device/sensor filter, while parquet's columnar row
  groups make the residual filter cheap.
- **Storage is the headline**: a year of history at 18% of SQLite's
  footprint, with the hot tier permanently at ~0 — the plain table's 3 GB
  (and its 30-second index build) grow forever.
- **Full scans remain the honest worst case** (5.2 s vs DuckDB's 0.44 s):
  row-at-a-time vtab FFI vs columnar execution. The spec's position stands
  — year-wide rollups are a cloud/DuckDB job, and the files are already in
  DuckDB's favorite format.
- **DuckDB's ~250–300 ms floor** on selective queries is per-query
  `read_parquet` planning + 365 footer parses. silodb's catalog range
  pruning + footer cache exist precisely to delete that floor on-device.
- **Ingest through the view trigger costs ~2×** vs raw inserts (401k vs
  832k rows/s equivalent) — trigger dispatch plus ts-index maintenance.
  At 100 rows/minute of real device load, both are ~5 orders of magnitude
  above requirement.

## Tiered maintenance (1d → 7d → 28d)

Same year, after `maintain()` promotes everything due: **365 files → 14
active files** (13×28d + stragglers), 65 merges rewriting the full 52.56M
rows in 31.5 s, 416 files GC'd, size unchanged, view contents identical.

| query, median ms | silodb 365 files | silodb 14 files | duckdb 365 | duckdb 14 |
|---|---|---|---|---|
| 1h, one series | 1.3 | 1.3 | 316 | **175** |
| full year agg | 9,079* | 9,926* | 410 | **217** |

*this run was on a loaded machine (plain SQLite's identical query also ran
1.8× slower than the previous session) — compare within a run, not across.

Honest read: tiering does **not** speed up silodb's own queries — the
footer cache already made 365 files cheap, and per-row vtab costs
dominate. What it buys: (a) bounded file count forever (~130/decade
instead of 3,650), (b) ~2× for ad-hoc DuckDB/external readers of the same
directory (fewer footers to parse), (c) narrow queries stay at 1.3 ms even
though file-level pruning got coarser — row-group pruning inside the 28d
files picks up the slack, which validates the two-layer design.

## History

The first full-scale run (2M-row variant) exposed a real bug: compaction
throughput collapsed 10× because `init_table` didn't index the hot
table's bucket axis, making each `compact_bucket` scan the whole
remaining backlog — quadratic. Fixed (index created by `init_table`);
compacting a full year's backlog now sustains ~493k rows/s, flat. The
flat-cost regression test missed it because it drains buckets as they
close (hot table stays small); the benchmark compacts a huge backlog.
