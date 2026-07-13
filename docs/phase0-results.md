# Phase 0 spike results — rusqlite on libSQL

**Verdict: works cleanly. Phase 1 unblocked.**

Date: 2026-07-13

## What was tested

- Compiled the libSQL amalgamation (`sqlite3.c` from the `libsql-ffi` 0.9.30
  crate's `bundled/src/`, libsql version 0.2.3, SQLite core 3.45.1) into a
  static `libsqlite3.a` with plain `cc -O2 -DSQLITE_CORE`.
- Built `rusqlite` 0.40.1 with `default-features = false, features =
  ["vtab", "series"]` linked against that static lib via
  `SQLITE3_LIB_DIR`/`SQLITE3_INCLUDE_DIR`/`SQLITE3_STATIC=1`.
- Proved the linked library is really libSQL (not a stray system sqlite3) by
  calling the libSQL-only `libsql_libversion()` symbol → `0.2.3`.
- Registered rusqlite's own `generate_series` vtab module via
  `create_module` and queried it:
  `SELECT value FROM generate_series(1,10,3)` → `[1, 4, 7, 10]`.

## Conclusions

- The vtab C API surface in libSQL is unmodified core SQLite as expected;
  `rusqlite::vtab` (create_module, xBestIndex, xFilter/xNext/xColumn/xEof)
  works without any patching or workarounds.
- Linkage recipe for the supervisory binary: build rusqlite with
  `default-features = false` + `vtab`, point `libsqlite3-sys` at the libSQL
  static lib with the three env vars above. The silodb crates themselves
  stay linkage-agnostic; their test suites use a dev-dependency on
  rusqlite `bundled` so `cargo test` is self-contained.

Spike source lived in the session scratchpad (throwaway per spec); this
file is the durable record.
