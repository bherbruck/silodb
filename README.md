# silodb

Time-series storage for edge devices, built **on top of SQLite** (works with
libSQL): hot writes land in a normal SQLite table, closed time buckets
compact into immutable Parquet files, and both stay queryable through one
connection under **one table name**. Think TimescaleDB's
hypertable-plus-compression idea, except the compressed tier is plain
Parquet on disk — readable by DuckDB, pandas, or anything else, with zero
export step and zero vendor lock-in.

```rust
let conn = rusqlite::Connection::open("hot.db")?;
silodb::load_module(&conn)?;                       // every boot
silodb::init_table_tiered(&conn, "readings",
    "ts TIMESTAMP, device TEXT, sensor TEXT, value REAL",
    "cold/", "1d,7d,28d")?;                        // idempotent

// the app's whole world is one name:
conn.execute("INSERT INTO readings VALUES (silodb_ts('2026-07-13T10:00:00Z'), 'boiler', 'temp', 21.5)", [])?;
conn.query_row("SELECT avg(value) FROM readings
                WHERE ts >= silodb_ts('2026-07-06') AND device = 'boiler'", [], |r| r.get::<_, f64>(0))?;

// storage management is one call on a dumb timer:
silodb::maintain(&conn, "readings", "cold/", now_epoch_micros)?;
```

## How the tiers work

![tiered compaction](docs/tiers.svg)

`maintain()` is a convergence function, not a scheduler: from (policy,
catalog, hot table, clock) it derives everything due — compacts closed
buckets, promotes daily files into weekly into monthly windows, and GCs
superseded files. Crash anywhere, run it again: same file names are
recomputed and rewritten byte-identically before the transaction that makes
them real. Full SQL the whole time — joins against your ordinary SQLite
tables included, which no TSDB query DSL gives you.

## Numbers (1 year, 1-min interval, 10 devices × 10 sensors = 52.5M rows)

| | silodb | plain SQLite (+ts index) | DuckDB on the same parquet |
|---|---|---|---|
| on-disk | **133 MB** | 2,988 MB (22×) | (same files) |
| "1h of one sensor" | **1.0 ms** | 0.4 ms | ~120 ms |
| "1 week of one sensor" | **63 ms** | 84 ms | ~120 ms |
| full-year aggregate | 4.7 s | 2.9 s | **0.1 s** |
| ingest (through the view) | ~430k rows/s | — | — |
| compaction | ~540k rows/s | — | — |

Selective time-range queries — the actual edge workload — stay in SQLite's
league while using 4–5 % of its disk; the hot tier stays permanently tiny
instead of growing forever. Full-table scans are the honest worst case:
that's DuckDB's home turf, and it reads silodb's cold files directly when
you want it. Methodology + more numbers: `crates/silodb-bench/`.

## Guarantees

- Hot writes have SQLite's ACID story verbatim (WAL, `synchronous=` knobs).
- Hot→cold migration is atomic: a Parquet file exists **iff** its catalog
  row committed, and the row commits in the same transaction that deletes
  the hot rows. No window where data is in zero places or two.
- Cold files are immutable — new files only, fsync + atomic rename; every
  operation is idempotent under crash-rerun (tested, byte-identical).
- Reads never require anything to exist: no files, no catalog, no
  directories — day zero works, and the vtab does zero file I/O at connect.

## Layout

```
hot.db          # SQLite: hot tables, _silodb_catalog, _silodb_policy
cold/           # one base dir for all tables
  readings/
    bucket-<start>-<end>-<seq>.parquet   # TIMESTAMP(µs, UTC) — real dates in any viewer
```

Canonical design doc: [`docs/spec.md`](docs/spec.md). Crate boundaries:
[`CLAUDE.md`](CLAUDE.md). Status: all spec phases built and tested (~90
tests incl. property tests, fuzzing, crash simulation, concurrency);
next planned: FTS5-style writable vtab so `CREATE VIRTUAL TABLE` is the
entire definition.
