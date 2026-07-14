# Examples

```sh
cargo run -p silodb-examples --bin quickstart    # the full lifecycle, narrated
cargo run -p silodb-examples --bin sql_only      # everything in SQL (create_hypertable-style)
cargo run -p silodb-examples --bin plain_sqlite  # joins with ordinary tables; internals are plain SQL
```

- **quickstart** — ten days of sensor data: ingest, one `maintain()` call
  (compaction + weekly merge + GC), pruned queries with `ScanStats`,
  a declare-anytime rollup, late-data self-heal, and the DuckDB handoff
  (the cold files are plain parquet).
- **sql_only** — after `load_module`, not one more line of Rust: plain
  `CREATE TABLE`, `silodb_create_table()` conversion, CTE ingest,
  `silodb_maintain()`, timestamp helpers both directions, catalog
  inspection with human-readable dates.
- **plain_sqlite** — the point of building on SQLite: a tiered table JOINs
  a normal table in one query (which TSDB query languages can't do), the
  hot tier / catalog / policy are ordinary inspectable tables, and plain
  tables in the same database stay completely untouched.
