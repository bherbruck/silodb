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

Single file first is deliberate sequencing, not the final argument shape —
see Phase 2.5. Prove row iteration and type mapping against one file before
adding directory globbing and multi-file pruning on top.

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
- **Only after the rename succeeds**, run one SQLite transaction that both
  `DELETE`s the compacted rows from the hot table *and* `INSERT`s the
  corresponding row into `_silodb_catalog` (see Phase 2.5) — same
  transaction, not two separate steps.
- Must be safe to run twice on the same bucket (idempotent) — if the
  process crashes between the rename and the transaction commit,
  re-running compaction on that bucket should either no-op cleanly or
  produce the same output file and catalog row, not corrupt or duplicate
  data. A Parquet file on disk with no matching catalog row is the signal
  that the previous run didn't finish — see Phase 2.5.

**Invariant: compaction cost is bounded by one bucket's worth of hot data,
never by total historical volume.** `compact_bucket()` must never read or
rewrite previously-written Parquet files, and must never scan the full hot
table to decide what's ready to compact — only the newly-closed bucket(s).
This holds as long as the delete-after-compact transaction actually runs on
schedule, which keeps the hot table's size roughly constant over time
rather than growing unbounded. Acceptance: run compaction repeatedly over
synthetic hot data that keeps growing (e.g. 100 buckets' worth fed in over
a test loop) and assert each individual run touches roughly the same number
of rows/does roughly the same amount of I/O — not a test that just checks
correctness once, a test that specifically watches for this cost creeping
up as prior history accumulates.

## Trigger logic (not part of the library — the calling application's job, but document the intended contract)

- Scheduled: whichever of (wall-clock interval) or (hot-table size
  threshold) fires first calls `compact_bucket` for all closed buckets,
  excluding the most recent 1-2 hours (safety margin against compacting a
  bucket that's still receiving writes).
- Manual: a CLI subcommand or signal handler that calls the exact same
  `compact_bucket` function, no separate code path.

## Crate structure

See `CLAUDE.md` for the workspace layout and crate dependency boundaries —
that's the authoritative version, not duplicated here to avoid the two
drifting out of sync. Phase 2.5's catalog table adds a `silodb-catalog`
crate to that structure: depends on `rusqlite` (not `parquet`), owns the
`_silodb_catalog` schema and read/write operations, and sits alongside
`silodb-schema` as something both `silodb-vtab` and `silodb-compact`
depend on without depending on each other.

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

## Phase 2.5 — multiple tables, multiple files (directory support)

**This is required for v1, not a future extension.** `compact_bucket()`
produces one Parquet file per bucket (Phase 3), so without a way to query a
directory of files as one table, compaction's output is never actually
queryable as a single logical table — the read and write paths don't
connect.

**Explicit non-design: cold storage is never a single file appended to or
rewritten over time.** Every `compact_bucket()` call writes a brand-new
file at a unique path and never reopens any previously-written file for
writing again — not the bucket it just wrote, not any older one. If an
implementation ever finds itself reopening an existing Parquet file to add
rows to it, that's a bug against this spec, not an optimization: Parquet's
footer (schema, row-group index, statistics) is written once at file end,
so appending means rewriting the whole file, which reintroduces exactly the
"gets slower as history grows" problem the one-file-per-bucket design
exists to avoid. It also breaks the catalog's per-file cache (keyed on
`(path, mtime, size)`, assumes immutability) and file-level pruning (one
growing file has one statistics range covering all time, so nothing is
ever skippable at the file level).

Decision: `silodb` takes a **directory**, not a single file path —
`CREATE VIRTUAL TABLE cold USING silodb('buckets/')`. "Multiple tables" is
handled by convention, not a separate mechanism: one directory per logical
table (`buckets/sensor_a/`, `buckets/sensor_b/`), each with its own
`CREATE VIRTUAL TABLE`.

Rejected alternatives, and why:

- **One vtab per file + a `UNION ALL` view over them** — works with zero
  code changes today, but doesn't scale on a device meant to run
  indefinitely offline. Hourly buckets over a year is ~8,760 virtual table
  registrations and a `UNION ALL` view with as many arms. Schema bloat and
  planner overhead that grows forever. Fine as a manual smoke test during
  development, wrong as the design.
- **Hive-style partition directories** (`ts_date=2026-07-13/part-0.parquet`)
  — that convention solves multi-reader/multi-tool discovery across a
  shared filesystem (Spark, Athena readers). This is one reader, one writer,
  one device. No audience for the complexity.

Design requirements for directory mode:

- **A thin catalog is the source of truth for what files exist and what
  range each covers — not filename parsing, not a directory glob.** The
  catalog is a table (`_silodb_catalog` or similar) living in the *same*
  hot SQLite database, not a separate manifest file — it inherits that
  database's existing transactional/crash-safety guarantees instead of
  requiring a second consistency mechanism to be designed and trusted.

  Sketch:
  ```sql
  CREATE TABLE _silodb_catalog (
    logical_table TEXT NOT NULL,
    path          TEXT NOT NULL,
    range_start   INTEGER NOT NULL,
    range_end     INTEGER NOT NULL,
    row_count     INTEGER,
    created_at    INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'active', -- reserved for future
                                                    -- retention/eviction use;
                                                    -- present now, unused now
    PRIMARY KEY (logical_table, path)
  );
  CREATE INDEX idx_silodb_catalog_range
    ON _silodb_catalog(logical_table, range_start, range_end);
  ```
- Query-time file-level pruning is an indexed range query against this
  table, not a `readdir` plus string parsing. Filenames should still encode
  the bucket range for human debuggability, but the catalog — not the
  filename — is authoritative.
- Within files the catalog query returns as candidates, apply the existing
  Phase 2 row-group pruning (footer statistics, cached per `(path, mtime,
  size)`) as before — that layer is unchanged.
- **`compact_bucket()` writes the catalog row in the same transaction as
  the hot-row delete** (extending the existing Parquet-rename-then-delete
  sequence from Phase 3: rename Parquet file, then one transaction that
  deletes hot rows *and* inserts the catalog row). This means a crash
  between rename and commit leaves the Parquet file on disk with no
  matching catalog entry — that mismatch is a usable signal that the run
  didn't finish, not just a hazard to guard against.
- **No Hive-style nested partition directories** (`date=2026-07-13/`).
  That convention solves multi-reader/multi-tool discovery across a shared
  filesystem — irrelevant here, one reader, one writer, one device — and
  the catalog already gives better pruning than directory nesting would.
- **File discovery must not require recreating the vtab.** Compaction keeps
  writing new files after `CREATE VIRTUAL TABLE` runs. Re-glob the
  directory on `xFilter` (cheap — `readdir` + `stat`, not re-parsing
  footers) and cache per-file stats keyed by `(path, mtime, size)` so an
  unchanged file's footer is never re-read. New bucket files should become
  visible to queries without any DDL.

Explicitly out of scope for `silodb` itself: nothing here bounds how many
cold files accumulate locally over months of uptime. Retention/eviction of
old local files (e.g. once they've synced to cloud) is the embedding
application's job — same pattern as compaction trigger logic in Phase 3.
Worth tracking as a named gap so it doesn't surface as a surprise later,
not something to solve in this library.

## Open questions to flag back, don't silently decide

- Whether this links into the supervisory binary directly (in-process,
  preferred — no loadable-extension complexity needed) or should also be
  buildable as a standalone loadable `.so` for ad-hoc inspection with the
  plain `sqlite3` CLI during development/debugging.
- Exact bucket size/granularity for compaction (hourly? daily?) — depends
  on real write volume, not guessable from this spec alone.