# silodb fuzz targets

libFuzzer targets (`cargo-fuzz`, needs a recent nightly — an older default
nightly may fail building `libsqlite3-sys`):

```sh
cargo +nightly fuzz run ts_parse        -- -max_total_time=300
cargo +nightly fuzz run vtab_ddl        -- -max_total_time=300
cargo +nightly fuzz run parquet_footer  -- -fork=4 -ignore_ooms=1 -max_total_time=300
```

- **`ts_parse`** — `silodb_schema::parse_timestamp_micros` on arbitrary
  bytes: never panic; anything accepted must survive a format→parse round
  trip. (Found a real i64 overflow on ~±300k-year inputs on its first run;
  fixed with a year bound + checked math.)
- **`vtab_ddl`** — arbitrary text spliced into
  `CREATE VIRTUAL TABLE ... USING silodb(...)` against a live connection:
  arg parsing, `schema=` parsing, ts resolution, hot-table borrow. Error or
  succeed, never panic (a panic would unwind across the SQLite FFI
  boundary).
- **`parquet_footer`** — the footer/metadata parse path the vtab hits on a
  hostile cold file, in-memory via `bytes::Bytes`. Panics are findings.
  **OOM findings are a known upstream issue, not ours**: arrow-rs's thrift
  decoder pre-allocates `Vec::with_capacity(declared_list_len)` before
  bounds-checking against remaining input, so a ~200-byte file can declare
  a multi-GB schema list. Can't be mitigated from this layer; acceptable
  under silodb's threat model (catalog paths are written by our own
  compaction — a hostile file implies local filesystem write access).
  Hence `-ignore_ooms=1 -fork=4` for panic-hunting.

Corpus and artifacts are not committed; findings that matter get distilled
into deterministic regression tests in the crates' own suites.
