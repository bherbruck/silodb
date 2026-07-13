# silodb — canonical spec

One SQLite (or libSQL) connection that serves a device's entire time-series
history: recent writes in a normal SQLite table, closed history in immutable
Parquet files, both behind a single table name. This document is the
authoritative spec for the system **as built**; superseded drafts live in
`docs/archive/` and are history, not guidance. Crate boundaries and
dependency rules live in `CLAUDE.md` (authoritative for layout, not
duplicated here).

## What this is

Edge supervisory devices are offline-first and hold most of their data
locally, long-term. Hot writes land in a normal SQLite table. Once a time
bucket is closed, compaction moves its rows into a Parquet file. Both tiers
stay queryable through one connection — no second query engine, no ETL, no
index-maintenance cost on data nobody writes anymore.

Not a DuckDB replacement for heavy analytical rollups (cloud-side job).
This is for lightweight, filtered local queries ("last week for this
sensor") on constrained hardware.

## Non-goals

- No SQL write path into Parquet (the vtab is read-only; compacted data is
  immutable by design).
- No distributed/multi-node anything. One device, one set of local files.
- No general-purpose Parquet coverage — exactly the column types and
  constraint operators this project needs.
- No retention/eviction of old cold files (the embedding application's
  job — a named gap, see Open questions).

## Dependencies (Apache 2.0 / MIT only, pure Rust)

`rusqlite` (`vtab` feature), `parquet` + `arrow` (arrow-rs). No C++
toolchain, no parquet-cpp. Crate test suites dev-depend on rusqlite
`bundled` so `cargo test` is self-contained; the embedding binary links
libSQL via `SQLITE3_LIB_DIR`/`SQLITE3_INCLUDE_DIR`/`SQLITE3_STATIC` —
verified empirically in Phase 0, recipe in `docs/phase0-results.md`.

## The application surface (one name per table)

```rust
let conn = rusqlite::Connection::open("hot.db")?;
silodb::load_module(&conn)?;                 // every boot: per-connection
silodb::init_table(&conn, "readings",        // idempotent, first boot does DDL
    "ts INTEGER, value REAL, name TEXT", "cold/")?;

conn.execute("INSERT INTO readings VALUES (?1, ?2, ?3)", ...)?;   // hot
conn.query_row("SELECT avg(value) FROM readings
                WHERE ts > ?1 AND ts < ?2", ...)?;                // hot ∪ cold

silodb::compact_table(&conn, "readings", start_us, end_us, "cold/")?;
```

`init_table` creates (all `IF NOT EXISTS`, so boots after the first no-op):

- `readings_hot` — real table; writes land here, compaction drains it
- `readings_cold` — silodb vtab; `schema=` is baked into its DDL so it
  reconnects with zero dependencies (works in a cold-only archive database
  where the hot table no longer exists)
- `readings` — view (`hot UNION ALL cold`) with an `INSTEAD OF INSERT`
  trigger forwarding writes to `readings_hot`

The app reads and writes **one name** and never sees the tier split.
`UPDATE`/`DELETE` through the view are deliberately not wired: cold history
is immutable, and mutating only the hot subset silently would lie. Run them
against `readings_hot` explicitly if hot-only mutation is really meant.
`init_table` errors loudly (`SchemaDrift`) if the schema string changed
against an existing table; migration is deliberately unsolved for now.

Two lifetimes to understand: all DDL persists in the database file
(created once, ever); **module registration does not** — every connection
must call `load_module` before touching the vtab, including read-only
consumers of the view.

## On-disk layout (fully lazy)

```
hot.db                  # SQLite: hot tables + _silodb_catalog
cold/                   # ONE base dir, configured once, shared by all tables
  readings/
    bucket-<start>-<end>-<seq>.parquet
  another_table/
    ...
```

Nothing is created up front. The catalog table and `cold/<table>/` appear
when the first compaction for that table actually writes. The read side
never creates, requires, or even stats directories — a vtab over a
nonexistent path is just an empty table until compaction happens.

**Cold files are never modified.** Every compaction writes a brand-new file
at a new path; nothing ever reopens a written Parquet file (Parquet footers
make append = full rewrite, and immutability is what makes the footer cache
and file pruning sound). Filenames encode range and sequence for human
debuggability, but are **not** parsed by the read side.

## The catalog — `_silodb_catalog` (source of truth)

```sql
CREATE TABLE _silodb_catalog (
  logical_table TEXT NOT NULL,
  path          TEXT NOT NULL,      -- stored verbatim as given to compaction
  range_start   INTEGER NOT NULL,   -- [start, end): half-open, epoch µs
  range_end     INTEGER NOT NULL,
  row_count     INTEGER,
  created_at    INTEGER NOT NULL,   -- epoch seconds, stamped by SQLite
  status        TEXT NOT NULL DEFAULT 'active',  -- reserved: supersede/merge
  PRIMARY KEY (logical_table, path)
);
CREATE INDEX idx_silodb_catalog_range
  ON _silodb_catalog(logical_table, range_start, range_end);
```

- Lives in the hot database → inherits its transactional guarantees. A
  compaction is real exactly when the transaction inserting its catalog row
  commits. **A Parquet file with no catalog row does not exist** to the
  read side — that's the crash-recovery signal, not a hazard.
- Ranges are half-open and that exclusivity is load-bearing: the overlap
  query treats `range_end == lo` as a provable non-match.
- Multiple rows may cover the same/overlapping range (late-arrival
  follow-up files); readers handle that natively.
- Readers only see `status = 'active'` — the column is reserved for a
  future merge/supersede flow (see Open questions).

## Read path (`silodb-vtab`)

```sql
CREATE VIRTUAL TABLE sensor_a USING silodb('cold/');
-- overrides: table=<logical>, ts_column=<name> (default ts),
--            schema='col TYPE, ...', hot_table=<name>
```

- **Connect does zero file I/O** and requires nothing to exist. Column
  declaration comes from `schema=` when present, else one PRAGMA against
  the hot table (`hot_table=` or the logical table name) — the
  authoritative schema, mapped through the same
  `silodb_schema::bucket_arrow_schema` compaction writes with, so the two
  cannot drift. The vtab's own name is the default logical table (visible
  in the same SQL statement — explicit, not magic).
- **Every `xFilter`**, two pruning layers:
  1. **File level** — indexed catalog range query using the pushed
     timestamp constraints. This is also how files compacted after
     `CREATE VIRTUAL TABLE` become visible: next query, no DDL. No catalog
     table yet → empty scan.
  2. **Row group level** — footer min/max statistics vs pushed EQ/GT/GE/
     LT/LE constraints on int/timestamp/date/float columns. Footers are
     parsed once and cached per `(path, mtime, size)`; files are immutable
     so entries never go stale.
- Pruning is conservative-only and `omit` is never set: SQLite re-checks
  every constraint on returned rows, so a pruning bug can cost I/O but
  never correctness. Anything un-prunable (TEXT constraints, Real-vs-i64
  domain crossings, missing stats) just keeps the data.
- Candidate files must match the declared column names/order (positional
  `xColumn` mapping); arrow types may differ — cell decoding follows each
  file's own types. A cataloged file missing from disk is a loud error
  (possible data loss), never skipped.
- No vtab trust flag: `DirectOnly` breaks the union-view pattern;
  `Innocuous` asserts things untrue of a module that opens files. Default
  trust + SQLite's trusted-schema mode (schema is self-authored) is
  correct.
- `silodb_vtab::last_scan_stats()` exposes per-scan counters (files
  total/candidate/scanned, row groups total/scanned, cache hits) — the
  acceptance criteria for pruning are counter assertions, never timing.

## Write path (`silodb-compact`)

```rust
compact_bucket(&conn, &BucketSpec { hot_table, logical_table, ts_column,
                                    bucket_start, bucket_end }, base_dir)
// or by convention: compact_table(&conn, "readings", start, end, base_dir)
```

Sequence: select bucket rows ordered by ts → stream into
`<base>/<table>/bucket-<start>-<end>-<seq>.parquet.tmp` in row-group-sized
batches (16k rows; memory bounded by one row group) → fsync file → atomic
rename → fsync dir → **one transaction**: DELETE hot rows + INSERT catalog
row.

`seq` = count of committed files for that exact bucket. That makes every
calling pattern idempotent with no caller-visible failure modes:

| situation | outcome |
|---|---|
| normal run | `Compacted { rows, path }` |
| re-run after success | `AlreadyCompacted`, nothing touched |
| re-run after crash between rename and commit | same `seq` recomputed → byte-identical rewrite → commit; no duplication |
| rows arrive in an already-compacted bucket | next `seq`, follow-up file, separate catalog row |
| bucket empty, never compacted | `EmptyBucket`, nothing written |

Genuine errors stay loud: cataloged file missing on disk; `ts_column`
missing/non-INTEGER; a declared type outside the supported affinity set;
flexible-typing garbage (e.g. TEXT in an INTEGER column) — which aborts
with the temp file cleaned up and nothing deleted.

**Cost invariant:** one call's work is bounded by one bucket's rows, never
by accumulated history. Compaction never opens a previously written Parquet
file and never scans the hot table beyond the bucket's range. Enforced by a
test that runs 100 accumulating buckets and asserts per-run rows and
SQLite change-counts stay exactly flat.

Trigger policy (when to call) is the embedding application's contract, not
the library's: scheduled (interval or hot-size threshold, whichever first)
over closed buckets minus a 1–2h safety margin, plus a manual path calling
the very same function. Bucket granularity is an open question (below).

## Type & timestamp mapping (`silodb-schema`)

Single source of truth for both directions; never depends on `rusqlite`.

- Read (Arrow → SQLite storage class): ints/bool/timestamps/dates →
  INTEGER; floats → REAL; utf8 → TEXT; binary → BLOB. UInt64 and nested
  types are rejected explicitly (don't fit / out of scope).
- Write (declared type → Arrow): SQLite affinity rules minus NUMERIC
  (refuse rather than guess); INTEGER→Int64, REAL→Float64, TEXT→Utf8,
  BLOB→Binary. **One narrow, named exception to the NUMERIC refusal:**
  declared types containing `TIMESTAMP` or `DATETIME` (which SQLite's own
  affinity algorithm files under NUMERIC) map to INTEGER and carry the
  timestamp marker. Nothing is guessed — two literal substrings get a
  deliberate rule; every other NUMERIC-affinity decl stays refused.
- **The TIMESTAMP declared type is the timestamp mechanism.** Declare
  `stamped_at TIMESTAMP` in a hot table / `init_table` schema string and:
  SQLite stores plain INTEGER epoch µs (NUMERIC affinity, zero overhead);
  compaction writes it as Parquet `TIMESTAMP(µs, UTC)` — a real logical
  type, so pandas/DuckDB/any viewer renders actual datetimes and cold
  files are directly exportable with no decoding step; and the bucket axis
  is discovered by type, not name. Secondary TIMESTAMP columns (metadata
  stamps that aren't the bucket axis) also export as real Parquet
  timestamps, nullable.
- **Bucket-axis resolution is a total precedence order**
  (`silodb_schema::resolve_ts_index`): (1) an explicit `ts_column=` always
  wins — type discovery never runs when it's given; (2) else exactly one
  TIMESTAMP/DATETIME column (zero or several → loud error, not a guess);
  (3) else the legacy name convention, an INTEGER column named `ts`.
- Through the vtab the value surfaces as the same raw INTEGER µs as the
  hot table, so SQL comparisons work identically across tiers. µs is
  deliberate: ns overflows i64 in 2262 and buys nothing here.
- **SQL helpers** (registered by the facade's `load_module`, pure logic in
  `silodb-schema`): `silodb_ts(x)` parses ISO 8601 text to epoch µs
  (INTEGER passes through, so `WHERE ts > silodb_ts(?1)` takes either);
  `silodb_datetime(µs)` formats back to ISO 8601 UTC text.

## Testing strategy

- Two kinds of fixtures, not one: hand-built deterministic files in
  `fixtures/` (regenerate via `fixtures/gen`, byte-stable; row-group
  boundaries are load-bearing for skip-count assertions) and
  proptest-generated data at test time. The highest-value test is the
  property: random data split across random bucket files + random
  constraints — doubly-pruned scan must equal an in-memory filter.
- Compaction's crash case is a real test (byte-identical rewrite after a
  simulated crash), as is the flat-cost invariant. The facade's e2e drives
  the single-name surface: day-zero boot, inserts, compaction underneath an
  unchanging view, late rows, second-boot idempotency, cold-only survival
  after dropping the hot table.
- No realistic domain-shaped data in crate suites — synthetic stresses
  edges better and doesn't bake in today's schema. Real field data belongs
  in a later end-to-end smoke test only.

## Open questions (flag back, don't silently decide)

- **Sidecar writer (deliberately open).** A future append-only fast-ingest
  process writing cold files directly, bypassing the hot tier. The
  architecture already has its socket — the contract would be: write a new
  file durably (tmp/fsync/rename), then insert one catalog row; the read
  side picks it up on the next query with zero changes. Parquet physics
  forces roll-a-file-per-threshold rather than true file append, which in
  turn implies a small-file **merge/supersede** job someday — write merged
  file, flip old rows to `status='superseded'` + insert new row in one
  transaction. `status` exists for this. None of it is designed further
  than this paragraph on purpose.
- **Retention/eviction** of old cold files after cloud sync — embedding
  app's job, needs a documented contract eventually (likely
  `status='evicted'` + file delete).
- **Catalog rebuild / adopt** — recovering a database from bare parquet
  files (footer scan → catalog rows). Disaster-recovery tool, cheap to
  build when needed.
- **Schema migration** — `init_table` detects drift and refuses; actual
  migration (hot table + view + trigger + historic files) is unsolved.
- **Bucket size/granularity** — hourly vs daily depends on real write
  volume; not guessable from spec.
- **`silodb-loadable`** — optional `.so` for ad-hoc `sqlite3`-CLI
  inspection; stub until something needs it.
