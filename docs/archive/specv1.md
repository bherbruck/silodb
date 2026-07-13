# silodb — SQLite virtual table for querying Parquet cold storage

## What this is

A Rust library that lets a SQLite (or libSQL) connection query Parquet files
directly via `CREATE VIRTUAL TABLE ... USING silodb(...)`, plus a compaction
routine that moves aged-out rows from a live SQLite table into Parquet files.

Purpose: edge supervisory devices are offline-first and hold most of their
data locally, long-term. Hot writes stay in a normal SQLite table. Once a
time bucket is closed, it gets compacted into a Parquet file. Both are then
queryable through one `sqlite3`/libSQL connection — no second query engine,
no ETL step, no ongoing index-maintenance cost on data nobody's actively
writing to anymore.

Not trying to replace DuckDB for heavy analytical rollups — that's a
cloud-side job. This is for lightweight, occasional, filtered local queries
("last week for this sensor") on constrained edge hardware.

## Non-goals

- No write support to Parquet through the vtab (read-only, matches
  `rusqlite`'s current vtab capability and matches the fact that compacted
  data should be immutable anyway).
- No distributed/multi-node anything. Single device, single set of local
  files.
- No general-purpose Parquet feature coverage — support the column types
  and constraint operators this project actually needs, not the whole spec.

## Dependencies (Apache 2.0 / MIT only)

- `rusqlite` (`vtab` feature) — vtab traits, `Connection::create_module`
- `parquet` (arrow-rs) — Parquet read/write, row-group statistics
- `arrow` (arrow-rs) — if needed alongside `parquet` for schema/array types
- avoid pulling in `parquet-cpp`/Arrow C++ or any C++ toolchain dependency —
  the whole point of the rewrite is staying pure-Rust

## Phase 0 — spike (do this first, blocking)

Confirm `rusqlite` actually links against and works with **libSQL**, not
just vanilla `libsqlite3`. libSQL is a fork; the vtab C API surface should
be unmodified core SQLite, but this needs to be verified empirically before
building anything on top of it.

Deliverable: a throwaway test that opens a libSQL-backed connection via
`rusqlite`, registers a trivial `create_module` (e.g. a copy of `rusqlite`'s
own `csvtab` example), and successfully queries it. If this doesn't work
cleanly, stop and report back — don't proceed to Phase 1 with a workaround
that wasn't asked for.

## Phase 1 — naive read path

Implement `VTab` and `VTabCursor` for a module named `silodb` using
`rusqlite::vtab`.

- `xCreate`/`xConnect`: parse the Parquet file path from the
  `CREATE VIRTUAL TABLE ... USING silodb('path/to/file.parquet')` argument.
  Read the Parquet file's schema and declare matching SQLite columns via
  `VTab::connect`'s `sql` return value. Map Parquet logical types to SQLite
  storage classes (INTEGER, REAL, TEXT, BLOB) — keep the mapping simple and
  explicit, don't try to handle every Arrow type up front.
- `xBestIndex`: no-op / accept full scan for this phase.
- `xFilter`/`xNext`/`xColumn`/`xEof`: iterate row groups sequentially,
  materializing rows on demand. Don't load the whole file into memory.
- Acceptance: `sqlite3` (or a small Rust test using `rusqlite` directly)
  can `CREATE VIRTUAL TABLE cold USING silodb('test.parquet')` and
  `SELECT * FROM cold` returns correct rows for a hand-built test Parquet
  file with a handful of rows across at least 3 row groups.

## Phase 2 — constraint pushdown

Implement real `xBestIndex` logic:

- Read row-group statistics (min/max per column, via the `parquet` crate's
  `RowGroupMetaData`/`Statistics` API) at `xConnect` time and cache them.
- In `xBestIndex`, inspect the constraints SQLite offers (particularly on
  the timestamp column, since that's the dominant filter pattern —
  `WHERE ts > ? AND ts < ?`) and mark which row groups can be skipped
  entirely based on statistics, without reading their data pages.
- Acceptance: a query with a selective timestamp range against a
  multi-row-group test file measurably reads fewer row groups than a full
  scan (assert on a counter/log, not just wall-clock time — timing alone is
  a flaky test).

## Phase 3 — compaction (write path)

A separate function/binary target, not part of the vtab itself:

```
fn compact_bucket(sqlite_conn: &Connection, bucket_start: Timestamp, bucket_end: Timestamp, out_path: &Path) -> Result<()>
```

- Select rows from the hot SQLite table within `[bucket_start, bucket_end)`.
- Write them to a Parquet file at a temp path (`out_path.tmp`), using
  reasonable row-group sizing for the expected row count (don't
  over-optimize this — start with something like 10-20k rows per group and
  revisit only if row-group pruning in Phase 2 shows it matters).
- `fsync` the temp file, then atomically rename to `out_path`.
- **Only after the rename succeeds**, `DELETE` the compacted rows from the
  hot SQLite table.
- Must be safe to run twice on the same bucket (idempotent) — if the
  process crashes between the rename and the delete, re-running compaction
  on that bucket should either no-op cleanly or produce the same output
  file, not corrupt or duplicate data.

## Trigger logic (not part of the library — the calling application's job, but document the intended contract)

- Scheduled: whichever of (wall-clock interval) or (hot-table size
  threshold) fires first calls `compact_bucket` for all closed buckets,
  excluding the most recent 1-2 hours (safety margin against compacting a
  bucket that's still receiving writes).
- Manual: a CLI subcommand or signal handler that calls the exact same
  `compact_bucket` function, no separate code path.

## Project layout (proposed, adjust if there's a better fit)

```
silodb/
  Cargo.toml
  src/
    lib.rs          # public API: vtab registration, compact_bucket()
    vtab.rs          # VTab/VTabCursor impl
    schema.rs         # Parquet <-> SQLite type mapping
    compact.rs        # compaction routine
  tests/
    vtab_test.rs
    compact_test.rs
  fixtures/
    *.parquet          # hand-built small test files
```

## Fixtures & test data

Two kinds, not one:

- **Hand-built, small, deterministic** — checked into `fixtures/`. Purpose-
  built for cases that need exact expected output: NULL handling, type
  boundaries, and (for Phase 2) known row-group boundaries, so pruning tests
  can assert *which* row groups got skipped, not just that the right rows
  came back. You can't get that guarantee from realistic-looking data where
  row-group placement isn't something you control by hand.
- **Property-based / generated at test time** (`proptest` or `quickcheck`),
  seeded for reproducibility, not checked in as static files. For Phase 2
  specifically: generate random data plus random query constraints, run both
  a full scan and the pruned path, assert identical results. This is the
  test that actually catches "pruning silently drops a row it shouldn't" —
  a property, not a specific case — and it's the highest-value test in
  `silodb-vtab`.

Deliberately avoid realistic domain-shaped data in the crate-level unit test
suites — it doesn't stress edge cases any better than synthetic data and
tends to quietly bake today's schema assumptions into the tests. Save real
field data for a later end-to-end smoke test, not
`silodb-schema`/`silodb-vtab`/`silodb-compact`'s own suites.

## Ingest path (scope note)

The only path that writes Parquet in this project is `compact_bucket()` in
`silodb-compact`, and it only ever reads from the hot SQLite table — see
Phase 3. There is no path for writing Parquet directly from raw/external
data (a CSV export, a bulk historical backfill) that never passed through
the hot SQLite tier.

**This is deliberately out of scope for v1**, not an oversight. If a bulk
backfill path is needed later (e.g. migrating legacy historian data), it
should be a separate addition that reuses `silodb-schema`'s type mapping,
not something bolted onto `compact_bucket()`, which should stay scoped to
"age hot rows out of SQLite into Parquet" and nothing else.

## Open questions to flag back, don't silently decide

- Whether this links into the supervisory binary directly (in-process,
  preferred — no loadable-extension complexity needed) or should also be
  buildable as a standalone loadable `.so` for ad-hoc inspection with the
  plain `sqlite3` CLI during development/debugging.
- Exact bucket size/granularity for compaction (hourly? daily?) — depends
  on real write volume, not guessable from this spec alone.
