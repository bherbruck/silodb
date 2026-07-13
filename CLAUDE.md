# CLAUDE.md — silodb

Working notes for Claude Code sessions in this repo. See `docs/spec.md` for
the full phased implementation plan — this file is about how the code is
organized, not what to build first.

## Core rule: low coupling, high cohesion

The read path (querying Parquet through SQLite) and the write path
(compacting SQLite rows into Parquet) are separate concerns and must stay
separate crates. Neither should import the other. If you find yourself
wanting `silodb-compact` to call into `silodb-vtab`, or vice versa, that's a
sign the thing you actually need belongs in `silodb-schema` instead.

Dependency direction is one-way, outward from the two shared base crates:

```
   silodb-schema   silodb-catalog
          \         /      \
        silodb-vtab      silodb-compact
                \           /
       silodb-loadable    silodb  (facades — see below)
```

`silodb-schema` must never depend on `rusqlite`. It's pure Parquet↔SQLite
type-mapping logic and should be testable without a SQLite connection in
scope at all. Symmetrically, `silodb-catalog` must never depend on
`parquet`/`arrow` — it's pure SQLite.

## Crates

- **`silodb-schema`** — Parquet logical type → SQLite storage class mapping.
  No `rusqlite` dependency. Depends only on `parquet`/`arrow`. This is the
  one thing both the read and write paths need to agree on, so it's the
  single source of truth for that mapping, not duplicated in each.

- **`silodb-catalog`** — the `_silodb_catalog` table (specv2 Phase 2.5):
  schema, ensure/insert, and the indexed range-overlap query. Lives in the
  hot database, so a compaction is durable exactly when its transaction
  commits. Depends on `rusqlite` only — never on `parquet`/`arrow`. Like
  `silodb-schema`, it's a base crate both paths share without importing
  each other.

- **`silodb-vtab`** — `VTab`/`VTabCursor` implementation (Phases 1–2.5 of
  the spec). Takes one base directory shared by all cold tables; the vtab's
  own name (or `table=`) selects the logical table. Asks the catalog
  which files can overlap the query's timestamp bounds, then row-group
  prunes within them (footers cached per `(path, mtime, size)`). Day zero
  works: with no cold files yet, the declared schema is borrowed from the
  hot table via `silodb_schema::bucket_arrow_schema` — the same function
  compaction writes files with, so they can't drift. Depends on
  `rusqlite` (`vtab` feature), `parquet`, `silodb-schema`, and
  `silodb-catalog`. Does not know anything about compaction or the hot
  SQLite table's schema.

- **`silodb-compact`** — the compaction routine (Phase 3 of the spec).
  Depends on `rusqlite`, `parquet`, `silodb-schema`, and `silodb-catalog`.
  Does not depend on `silodb-vtab` — it writes Parquet files, it doesn't
  need to read them back through the vtab to do its job. Owns the
  temp-file/fsync/atomic-rename sequencing and the single transaction that
  deletes hot rows *and* inserts the catalog row (specv2), plus the
  idempotency guarantee. Trigger logic (schedule vs. manual) is
  intentionally **not** here — this crate exposes `compact_bucket()`;
  deciding *when* to call it is the embedding application's job, documented
  as a contract in the spec, not enforced by this crate.

- **`silodb-loadable`** — optional `cdylib` wrapper that exposes
  `silodb-vtab` as a standalone loadable SQLite extension (for ad-hoc
  inspection via the plain `sqlite3` CLI during development). Depends on
  `silodb-vtab`. Only crate in the workspace with `crate-type = ["cdylib"]`
  — keep that out of every other crate's `Cargo.toml` so they stay usable
  as plain `rlib` dependencies in-process.

- **`silodb`** — top-level facade crate. Re-exports the vtab module
  registration function from `silodb-vtab` and `compact_bucket` from
  `silodb-compact`. This is the only crate the supervisory binary should
  depend on directly — it should never need to reach into `silodb-vtab` or
  `silodb-compact` internals itself.

## Workspace layout

```
silodb/
  Cargo.toml              # [workspace], members = crates/*
  CLAUDE.md
  docs/
    spec.md
  crates/
    silodb-schema/
    silodb-catalog/
    silodb-vtab/
    silodb-compact/
    silodb-loadable/
    silodb/
  fixtures/
    *.parquet              # shared hand-built test files — regenerate with
                            # `cargo run --manifest-path fixtures/gen/Cargo.toml`,
                            # never edit by hand (row-group boundaries are
                            # load-bearing for pruning tests)
    gen/                   # the deterministic generator for the above
```

## Testing

- `silodb-schema`: pure unit tests, no I/O.
- `silodb-catalog`: unit tests against in-memory SQLite (range-overlap
  boundary semantics live here — entry ranges are half-open).
- `silodb-vtab`: integration tests against `fixtures/*.parquet` plus
  generated multi-file setups — file-level and row-group skip-count
  assertions on `last_scan_stats()`, not wall-clock timing; plus the
  proptest comparing pruned scans to an in-memory filter (the
  highest-value test in the crate, per spec).
- `silodb-compact`: integration tests including the simulated-crash case
  (file renamed, delete+catalog transaction never ran → re-run produces a
  byte-identical file, no duplication) and the specv2 bounded-cost test
  (per-run work flat across 100 accumulating buckets).
- `silodb` (facade): the end-to-end hot→compact→`UNION ALL` view test —
  the only place the read and write paths meet.
- Test suites use a dev-dependency on rusqlite `bundled` so `cargo test`
  is self-contained; linking against libSQL is the embedding binary's
  concern (recipe in `docs/phase0-results.md`).

## Tooling: use the CLI, don't hand-write what a tool can tell you

Don't type dependency versions from memory into `Cargo.toml`. Crate versions,
feature flags, and API shapes in this ecosystem (`rusqlite`, `parquet`,
`arrow`) move fast enough that a remembered version is a guess, not a fact.

- **Adding a dependency:** `cargo add <crate>` (optionally `--features vtab`
  etc.), not a hand-typed `Cargo.toml` line. Let it resolve the current
  version. Same for adding a crate to the workspace — `cargo new
  --lib crates/silodb-schema` from the workspace root, not a hand-built
  directory and manifest.
- **Checking an API before using it:** don't rely on remembered method
  signatures for `rusqlite::vtab` or `parquet`'s row-group/statistics types —
  both have shifted shape across versions. Pull current docs (`docs.rs/<crate>`
  or `cargo doc --open` against what's actually in `Cargo.lock`) before
  writing code against them, especially for `VTab`/`VTabCursor`, which have
  had trait-shape changes across `rusqlite` releases.
- **Workspace/member changes:** `cargo new`/`cargo init` for scaffolding,
  `cargo add --workspace` or per-crate `cargo add -p <crate>` for deps,
  rather than editing the workspace `Cargo.toml` `[workspace.dependencies]`
  table by hand.
- If a version or feature flag genuinely needs to be pinned for a reason
  (e.g. the Phase 0 libSQL-compatibility spike turns up a constraint), note
  *why* next to the pin in `Cargo.toml`, not just the pin itself.

## Open / not yet decided

- Whether `silodb-loadable` is worth building in Phase 1 or deferred until
  something actually needs the plain-`sqlite3`-CLI debugging path — the
  supervisory binary itself will use `silodb` (in-process) either way.
- Conventions for error handling, logging, and lint config aren't set yet —
  fill in here once decided rather than each crate inventing its own.
